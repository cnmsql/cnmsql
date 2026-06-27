/*
Copyright 2026 The CNMSQL - CloudNative for MySQL Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/internal/controller/topology"
	mysqlconfig "github.com/cnmsql/cnmsql/pkg/management/mysql/config"
)

var (
	issuerGVK = schema.GroupVersionKind{
		Group:   "cert-manager.io",
		Version: "v1",
		Kind:    "Issuer",
	}
	certificateGVK = schema.GroupVersionKind{
		Group:   "cert-manager.io",
		Version: "v1",
		Kind:    "Certificate",
	}
)

func (r *ClusterReconciler) ensureCredentials(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) error {
	if cluster.Spec.RootPasswordSecret == nil {
		if err := r.ensurePasswordSecret(ctx, cluster, plan.RootSecretName, map[string]string{"username": "root"}); err != nil {
			return err
		}
	}
	// On recovery the application user comes from the restored data, so no app
	// secret is generated; only initdb provisions one.
	if initdb := cluster.Spec.Bootstrap.InitDB; initdb != nil && initdb.Secret == nil {
		user := initdb.Owner
		if user == "" {
			user = "app"
		}
		if err := r.ensurePasswordSecret(ctx, cluster, plan.AppSecretName, map[string]string{"username": user}); err != nil {
			return err
		}
	}
	if err := r.ensurePasswordSecret(ctx, cluster, plan.ReplicationSecret, map[string]string{"username": "cnmsql_repl"}); err != nil {
		return err
	}
	if err := r.ensurePasswordSecret(ctx, cluster, plan.BackupSecretName, map[string]string{"username": "cnmsql_backup"}); err != nil {
		return err
	}
	return r.ensurePasswordSecret(ctx, cluster, plan.ControlSecretName, map[string]string{"username": "cnmsql_control"})
}

func (r *ClusterReconciler) ensurePasswordSecret(ctx context.Context, cluster *mysqlv1alpha1.Cluster, name string, data map[string]string) error {
	secret := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, secret)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	password, err := randomPassword()
	if err != nil {
		return err
	}
	stringData := map[string]string{"password": password}
	maps.Copy(stringData, data)
	secret = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    labelsFor(cluster, "", ""),
		},
		Type:       corev1.SecretTypeOpaque,
		StringData: stringData,
	}
	if err := controllerutil.SetControllerReference(cluster, secret, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, secret)
}

func randomPassword() (string, error) {
	var buf [24]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}

func (r *ClusterReconciler) ensureConfigMap(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan, inst instancePlan) error {
	rendered, err := r.renderMyCnf(cluster, plan, inst, plan.instanceNames(cluster))
	if err != nil {
		return err
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name:      inst.ConfigMapName,
		Namespace: cluster.Namespace,
	}}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = labelsFor(cluster, inst.Name, roleOf(inst))
		cm.Data = map[string]string{"my.cnf": rendered}
		return controllerutil.SetControllerReference(cluster, cm, r.Scheme)
	})
	return err
}

func (r *ClusterReconciler) renderMyCnf(cluster *mysqlv1alpha1.Cluster, plan clusterPlan, inst instancePlan, memberNames []string) (string, error) {
	role := mysqlconfig.RolePrimary
	if !inst.IsPrimary {
		role = mysqlconfig.RoleReplica
	}
	cfg := &mysqlconfig.ServerConfig{
		ServerID:     inst.ServerID,
		Version:      plan.ServerVersion,
		Role:         role,
		DataDir:      dataDir,
		Socket:       socketPath,
		Port:         3306,
		ReportHost:   inst.ServiceName,
		BinlogFormat: cluster.Spec.MySQL.BinlogFormat,
		AdminAddress: mysqlconfig.DefaultAdminAddress,
		AdminPort:    mysqlconfig.DefaultAdminPort,
		// Configure mysqld transport TLS so replicas and clients can connect over
		// TLS. Whether to require it is left to the user (require_secure_transport
		// is no longer operator-managed).
		TLS: mysqlconfig.TLSPaths{
			CA:   topology.ClientCAPath + "/ca.crt",
			Cert: topology.ServerTLSPath + "/tls.crt",
			Key:  topology.ServerTLSPath + "/tls.key",
		},
		UserParameters: cluster.Spec.MySQL.Parameters,
		Archiving:      archivingConfig(cluster),
	}
	r.topologyReconciler(cluster).ConfigureServer(cluster, topology.ServerConfigInput{
		InstanceName: inst.Name,
		MemberNames:  memberNames,
	}, cfg)
	return cfg.Render()
}

// archivingConfig resolves the my.cnf durability/RPO settings for continuous
// binlog archiving, applying defaults when the API server has not (e.g. in unit
// tests building the spec directly).
func archivingConfig(cluster *mysqlv1alpha1.Cluster) mysqlconfig.Archiving {
	ca := cluster.ContinuousArchiving()
	if ca == nil || !ca.Enabled {
		return mysqlconfig.Archiving{}
	}
	maxSize := int(ca.MaxBinlogSizeMB)
	if maxSize <= 0 {
		maxSize = 16
	}
	expire := int(ca.BinlogExpireSeconds)
	if expire < 0 {
		expire = 0
	} else if ca.BinlogExpireSeconds == 0 {
		expire = 604800
	}
	return mysqlconfig.Archiving{
		Enabled:             true,
		MaxBinlogSizeMB:     maxSize,
		BinlogExpireSeconds: expire,
	}
}

// ensurePVC reconciles the instance's data PVC, growing its storage request when
// spec.storage.size increases (Kubernetes never allows shrinking). It returns
// needsRoll=true when the volume has a pending node-side resize that cannot
// complete while it stays mounted — see ShouldResizeInUseVolumes — so the caller
// must recycle the Pod to finish the expansion.
func (r *ClusterReconciler) ensurePVC(ctx context.Context, cluster *mysqlv1alpha1.Cluster, inst instancePlan) (bool, error) {
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:      inst.PVCName,
		Namespace: cluster.Namespace,
	}}
	if err := r.Get(ctx, client.ObjectKeyFromObject(pvc), pvc); err != nil {
		if !apierrors.IsNotFound(err) {
			return false, err
		}
		spec, err := pvcSpec(cluster.Spec.Storage)
		if err != nil {
			return false, err
		}
		pvc.Labels = labelsFor(cluster, inst.Name, roleOf(inst))
		pvc.Spec = spec
		if err := controllerutil.SetControllerReference(cluster, pvc, r.Scheme); err != nil {
			return false, err
		}
		return false, r.Create(ctx, pvc)
	}

	if cluster.Spec.Storage.Size == "" {
		return false, nil
	}
	desired, err := resource.ParseQuantity(cluster.Spec.Storage.Size)
	if err != nil {
		return false, err
	}
	current := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if current.Cmp(desired) < 0 {
		before := pvc.DeepCopy()
		if pvc.Spec.Resources.Requests == nil {
			pvc.Spec.Resources.Requests = corev1.ResourceList{}
		}
		pvc.Spec.Resources.Requests[corev1.ResourceStorage] = desired
		if err := r.Patch(ctx, pvc, client.MergeFrom(before)); err != nil {
			return false, err
		}
	}

	// With an online-expandable backend the kubelet grows the filesystem in place
	// and no roll is needed. When ResizeInUseVolumes is false the backend cannot,
	// so the resize parks in a pending condition until the volume is detached:
	// signal the caller to recycle the Pod and let the new mount complete it.
	if !cluster.ShouldResizeInUseVolumes() && pvcResizePending(pvc) {
		return true, nil
	}
	return false, nil
}

// rollForResize recycles the instance Pod so a pending volume resize can
// complete on the new mount. It mirrors ensurePod's roll contract: it returns
// rolled=true only when it deletes (or finds already terminating) the Pod, so the
// caller stops the pass and requeues; allowRoll=false defers the action (the
// primary is rolled last). A missing Pod is not an error — ensurePod will create
// it and the fresh mount completes the resize.
func (r *ClusterReconciler) rollForResize(ctx context.Context, cluster *mysqlv1alpha1.Cluster, inst instancePlan, allowRoll bool) (bool, error) {
	if !allowRoll {
		return false, nil
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      inst.Name,
		Namespace: cluster.Namespace,
	}}
	if err := r.Get(ctx, client.ObjectKeyFromObject(pod), pod); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if pod.DeletionTimestamp != nil {
		return true, nil
	}
	if err := r.Delete(ctx, pod); err != nil {
		return false, err
	}
	if r.Recorder != nil {
		r.Recorder.Eventf(cluster, corev1.EventTypeNormal, "VolumeResize",
			"Recreating instance %s to complete an offline volume resize", inst.Name)
	}
	return true, nil
}

// pvcResizePending reports whether a PVC has a resize that Kubernetes has not yet
// finished applying to the underlying filesystem: either the volume-level
// expansion is still in progress (Resizing) or it is done and waiting on a
// node-side filesystem grow that only happens on (re)mount (FileSystemResizePending).
func pvcResizePending(pvc *corev1.PersistentVolumeClaim) bool {
	for _, condition := range pvc.Status.Conditions {
		if condition.Status != corev1.ConditionTrue {
			continue
		}
		switch condition.Type {
		case corev1.PersistentVolumeClaimResizing,
			corev1.PersistentVolumeClaimFileSystemResizePending:
			return true
		}
	}
	return false
}

func pvcSpec(storage mysqlv1alpha1.StorageConfiguration) (corev1.PersistentVolumeClaimSpec, error) {
	spec := corev1.PersistentVolumeClaimSpec{
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
	}
	if storage.PersistentVolumeClaimTemplate != nil {
		spec = *storage.PersistentVolumeClaimTemplate.DeepCopy()
	}
	if storage.StorageClass != nil {
		spec.StorageClassName = storage.StorageClass
	}
	if storage.Size != "" {
		quantity, err := resource.ParseQuantity(storage.Size)
		if err != nil {
			return corev1.PersistentVolumeClaimSpec{}, err
		}
		if spec.Resources.Requests == nil {
			spec.Resources.Requests = corev1.ResourceList{}
		}
		spec.Resources.Requests[corev1.ResourceStorage] = quantity
	}
	if spec.Resources.Requests.Storage().IsZero() {
		return corev1.PersistentVolumeClaimSpec{}, fmt.Errorf("spec.storage.size or spec.storage.pvcTemplate.resources.requests.storage is required")
	}
	return spec, nil
}

// ensureInstanceService reconciles the per-instance headless Service used for
// stable DNS and the instance manager's report_host.
func (r *ClusterReconciler) ensureInstanceService(ctx context.Context, cluster *mysqlv1alpha1.Cluster, inst instancePlan) error {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name:      inst.ServiceName,
		Namespace: cluster.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		service.Labels = labelsFor(cluster, inst.Name, roleOf(inst))
		service.Spec.ClusterIP = corev1.ClusterIPNone
		service.Spec.Selector = map[string]string{instanceLabel: inst.Name}
		service.Spec.Ports = servicePorts()
		service.Spec.PublishNotReadyAddresses = true
		return controllerutil.SetControllerReference(cluster, service, r.Scheme)
	})
	return err
}

func servicePorts() []corev1.ServicePort {
	return []corev1.ServicePort{
		{Name: "mysql", Port: 3306, TargetPort: intstr.FromString("mysql")},
		{Name: "control", Port: 8080, TargetPort: intstr.FromString("control")},
	}
}

// ensurePod reconciles the instance Pod. It returns rolled=true when it deleted
// the Pod to apply a template change (config, seed, scale): that is the
// destructive rolling action the caller must serialise, stopping the pass and
// requeueing so the next ordinal is not rolled until this one is fully ready
// again. Creates and metadata patches are non-destructive and report rolled=false.
//
// allowRoll gates the destructive delete: when false, a Pod that needs a template
// change is left in place (rolled=false) so the caller can defer it — this is how
// the elected primary is rolled last, after every replica.
func (r *ClusterReconciler) ensurePod(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan, inst instancePlan, allowRoll bool) (bool, error) {
	labels := labelsFor(cluster, inst.Name, roleOf(inst))
	spec := r.podSpec(cluster, plan, inst)
	annotations, err := r.podAnnotations(cluster, plan, inst, labels, spec)
	if err != nil {
		return false, err
	}

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      inst.Name,
		Namespace: cluster.Namespace,
	}}
	if err := r.Get(ctx, client.ObjectKeyFromObject(pod), pod); err != nil {
		if !apierrors.IsNotFound(err) {
			return false, err
		}
		pod.Labels = labels
		pod.Annotations = annotations
		pod.Spec = spec
		if err := controllerutil.SetControllerReference(cluster, pod, r.Scheme); err != nil {
			return false, err
		}
		return false, r.Create(ctx, pod)
	}
	if pod.DeletionTimestamp != nil {
		return false, nil
	}
	// The routable label is owned by the fencing reconcile, not the pod template;
	// preserve the live value so this pass does not silently un-fence a Pod.
	if v, ok := pod.Labels[routableLabel]; ok {
		labels[routableLabel] = v
	}
	// The fencing annotation is user-owned. Preserve it so ensurePod does not
	// erase the signal before observe/reconcileFencing can act on it.
	if v, ok := pod.Annotations[fencingAnnotation]; ok {
		annotations[fencingAnnotation] = v
	}
	// The group-observation doorbell is published by the in-Pod reconciler on its
	// own Pod. Preserve it so ensurePod does not erase it between doorbell rings
	// and cause a reconcile storm.
	if v, ok := pod.Annotations[groupObservationAnnotation]; ok {
		annotations[groupObservationAnnotation] = v
	}
	if pod.Annotations[podTemplateHashAnnotation] != annotations[podTemplateHashAnnotation] {
		if !allowRoll {
			// The Pod needs a template change but rolling it is deferred (the primary
			// is rolled last); leave it in place for a later pass.
			return false, nil
		}
		if err := r.Delete(ctx, pod); err != nil {
			return false, err
		}
		return true, nil
	}
	if !maps.Equal(pod.Labels, labels) || !maps.Equal(pod.Annotations, annotations) {
		before := pod.DeepCopy()
		pod.Labels = labels
		pod.Annotations = annotations
		return false, r.Patch(ctx, pod, client.MergeFrom(before))
	}
	return false, nil
}

func (r *ClusterReconciler) podAnnotations(cluster *mysqlv1alpha1.Cluster, plan clusterPlan, inst instancePlan, labels map[string]string, spec corev1.PodSpec) (map[string]string, error) {
	config, err := r.renderMyCnf(cluster, plan, inst, plan.instanceNames(cluster))
	if err != nil {
		return nil, err
	}
	configHash, err := hashObject(config)
	if err != nil {
		return nil, err
	}
	annotations := map[string]string{}
	if cluster.Spec.InheritedMetadata != nil {
		maps.Copy(annotations, cluster.Spec.InheritedMetadata.Annotations)
	}
	annotations[configMapAnnotation] = inst.ConfigMapName
	annotations[configHashAnnotation] = configHash
	stablePlan := plan
	stablePlan.PrimaryName = instanceName(cluster, 1)
	stableInst := stablePlan.instanceFor(cluster, inst.Ordinal)
	// The group seed list is derived from the member set, so it changes on every
	// scale. Seeds are only consulted by a member at join time (and are runtime
	// settable), so rolling existing members just to rewrite them is needless
	// churn. Render the stable hash with a canonical single-member seed list so a
	// scale change does not move the template hash of the existing members; new
	// members are still created with the full, current seed list via ensureConfigMap.
	stableConfig, err := r.renderMyCnf(cluster, stablePlan, stableInst, []string{stableInst.Name})
	if err != nil {
		return nil, err
	}
	stableConfigHash, err := hashObject(stableConfig)
	if err != nil {
		return nil, err
	}
	templateLabels := maps.Clone(labels)
	delete(templateLabels, roleLabel)
	templateAnnotations := maps.Clone(annotations)
	templateAnnotations[configHashAnnotation] = stableConfigHash
	templateHash, err := hashObject(struct {
		Labels      map[string]string
		Annotations map[string]string
		Spec        corev1.PodSpec
		Restart     string
	}{
		Labels:      templateLabels,
		Annotations: templateAnnotations,
		Spec:        restartTriggeringPodSpec(cluster, stablePlan, stableInst, spec),
		// Folding the user-requested restart token into the template hash makes a
		// bump roll every Pod through the existing hash-mismatch path.
		Restart: cluster.Annotations[restartAnnotation],
	})
	if err != nil {
		return nil, err
	}
	annotations[podTemplateHashAnnotation] = templateHash
	return annotations, nil
}

func restartTriggeringPodSpec(cluster *mysqlv1alpha1.Cluster, stablePlan clusterPlan, stableInst instancePlan, actual corev1.PodSpec) corev1.PodSpec {
	stable := actual.DeepCopy()
	stableTemplate := (&ClusterReconciler{}).podSpec(cluster, stablePlan, stableInst)
	if len(stable.InitContainers) == len(stableTemplate.InitContainers) {
		for i := range stable.InitContainers {
			stable.InitContainers[i].Args = stableTemplate.InitContainers[i].Args
		}
	}
	// Normalize the bootstrap-controller init container image to a constant so
	// an operator image bump does not change the pod template hash and trigger a
	// simultaneous Pod restart across every instance. Stale instance detection is
	// handled by the dedicated operator upgrade reconcile path (cluster_upgrade.go).
	if len(stable.InitContainers) > 0 {
		stable.InitContainers[0].Image = "operator"
	}
	if len(stable.Containers) == len(stableTemplate.Containers) {
		for i := range stable.Containers {
			stable.Containers[i].Args = stableTemplate.Containers[i].Args
		}
	}
	return *stable
}

func hashObject(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

// roleOf maps an instance to its role label value.
func roleOf(inst instancePlan) string {
	if inst.IsPrimary {
		return rolePrimary
	}
	return roleReplica
}

func labelsFor(cluster *mysqlv1alpha1.Cluster, instanceName, role string) map[string]string {
	labels := map[string]string{
		"app.kubernetes.io/name":      "cnmsql",
		"app.kubernetes.io/instance":  cluster.Name,
		"app.kubernetes.io/component": "mysql",
		clusterLabel:                  cluster.Name,
		podMonitorClusterLabel:        cluster.Name,
	}
	if cluster.Spec.InheritedMetadata != nil {
		maps.Copy(labels, cluster.Spec.InheritedMetadata.Labels)
	}
	if instanceName != "" {
		labels[instanceLabel] = instanceName
		labels[roleLabel] = role
		// Routable by default; fencing flips this to "false" to drop the Pod from
		// the routing Services. The live value is preserved across pod reconciles.
		labels[routableLabel] = routableTrue
	}
	return labels
}
