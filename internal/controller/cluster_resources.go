/*
Copyright 2026 The CNMySQL Authors.

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

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
	mysqlconfig "github.com/yyewolf/cnmysql/pkg/management/mysql/config"
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
	if initdb := cluster.Spec.Bootstrap.InitDB; initdb.Secret == nil {
		user := initdb.Owner
		if user == "" {
			user = "app"
		}
		if err := r.ensurePasswordSecret(ctx, cluster, plan.AppSecretName, map[string]string{"username": user}); err != nil {
			return err
		}
	}
	if err := r.ensurePasswordSecret(ctx, cluster, plan.ReplicationSecret, map[string]string{"username": "cnmysql_repl"}); err != nil {
		return err
	}
	return r.ensurePasswordSecret(ctx, cluster, plan.ControlSecretName, map[string]string{"username": "cnmysql_control"})
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
			Labels:    labelsFor(cluster, ""),
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

func (r *ClusterReconciler) ensureConfigMap(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) error {
	rendered, err := renderMyCnf(cluster, plan)
	if err != nil {
		return err
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name:      plan.ConfigMapName,
		Namespace: cluster.Namespace,
	}}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = labelsFor(cluster, "")
		cm.Data = map[string]string{"my.cnf": rendered}
		return controllerutil.SetControllerReference(cluster, cm, r.Scheme)
	})
	return err
}

func renderMyCnf(cluster *mysqlv1alpha1.Cluster, plan clusterPlan) (string, error) {
	semiSync := mysqlconfig.SemiSync{}
	if cluster.Spec.MySQL.SemiSync != nil {
		semiSync.Enabled = cluster.Spec.MySQL.SemiSync.Enabled
		semiSync.WaitForReplicaCount = cluster.Spec.MinSyncReplicas
		if cluster.Spec.MySQL.SemiSync.TimeoutMillis != nil {
			semiSync.TimeoutMillis = int(*cluster.Spec.MySQL.SemiSync.TimeoutMillis)
		}
	}
	return (&mysqlconfig.ServerConfig{
		ServerID:       1,
		Version:        plan.ServerVersion,
		Role:           mysqlconfig.RolePrimary,
		DataDir:        dataDir,
		Socket:         socketPath,
		Port:           3306,
		ReportHost:     plan.ServiceName,
		BinlogFormat:   cluster.Spec.MySQL.BinlogFormat,
		AdminAddress:   mysqlconfig.DefaultAdminAddress,
		AdminPort:      mysqlconfig.DefaultAdminPort,
		UserParameters: cluster.Spec.MySQL.Parameters,
		SemiSync:       semiSync,
	}).Render()
}

func (r *ClusterReconciler) ensurePVC(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) error {
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:      plan.DataPVCName,
		Namespace: cluster.Namespace,
	}}
	if err := r.Get(ctx, client.ObjectKeyFromObject(pvc), pvc); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		spec, err := pvcSpec(cluster.Spec.Storage)
		if err != nil {
			return err
		}
		pvc.Labels = labelsFor(cluster, plan.InstanceName)
		pvc.Spec = spec
		if err := controllerutil.SetControllerReference(cluster, pvc, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, pvc)
	}

	if cluster.Spec.Storage.Size == "" {
		return nil
	}
	desired, err := resource.ParseQuantity(cluster.Spec.Storage.Size)
	if err != nil {
		return err
	}
	current := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if current.Cmp(desired) >= 0 {
		return nil
	}
	before := pvc.DeepCopy()
	if pvc.Spec.Resources.Requests == nil {
		pvc.Spec.Resources.Requests = corev1.ResourceList{}
	}
	pvc.Spec.Resources.Requests[corev1.ResourceStorage] = desired
	return r.Patch(ctx, pvc, client.MergeFrom(before))
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

func (r *ClusterReconciler) ensureService(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) error {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name:      plan.ServiceName,
		Namespace: cluster.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		service.Labels = labelsFor(cluster, plan.InstanceName)
		service.Spec.ClusterIP = corev1.ClusterIPNone
		service.Spec.Selector = map[string]string{instanceLabel: plan.InstanceName}
		service.Spec.Ports = []corev1.ServicePort{
			{Name: "mysql", Port: 3306, TargetPort: intstr.FromString("mysql")},
			{Name: "control", Port: 8080, TargetPort: intstr.FromString("control")},
		}
		return controllerutil.SetControllerReference(cluster, service, r.Scheme)
	})
	return err
}

func (r *ClusterReconciler) ensurePod(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) error {
	labels := labelsFor(cluster, plan.InstanceName)
	spec := r.podSpec(cluster, plan)
	annotations, err := podAnnotations(cluster, plan, labels, spec)
	if err != nil {
		return err
	}

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      plan.InstanceName,
		Namespace: cluster.Namespace,
	}}
	if err := r.Get(ctx, client.ObjectKeyFromObject(pod), pod); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		pod.Labels = labels
		pod.Annotations = annotations
		pod.Spec = spec
		if err := controllerutil.SetControllerReference(cluster, pod, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, pod)
	}
	if pod.DeletionTimestamp != nil {
		return nil
	}
	if pod.Annotations[podTemplateHashAnnotation] != annotations[podTemplateHashAnnotation] {
		return r.Delete(ctx, pod)
	}
	return nil
}

func podAnnotations(cluster *mysqlv1alpha1.Cluster, plan clusterPlan, labels map[string]string, spec corev1.PodSpec) (map[string]string, error) {
	config, err := renderMyCnf(cluster, plan)
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
	annotations[configMapAnnotation] = plan.ConfigMapName
	annotations[configHashAnnotation] = configHash
	templateHash, err := hashObject(struct {
		Labels      map[string]string
		Annotations map[string]string
		Spec        corev1.PodSpec
	}{
		Labels:      labels,
		Annotations: annotations,
		Spec:        spec,
	})
	if err != nil {
		return nil, err
	}
	annotations[podTemplateHashAnnotation] = templateHash
	return annotations, nil
}

func hashObject(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func labelsFor(cluster *mysqlv1alpha1.Cluster, instanceName string) map[string]string {
	labels := map[string]string{
		"app.kubernetes.io/name":      "cnmysql",
		"app.kubernetes.io/instance":  cluster.Name,
		"app.kubernetes.io/component": "mysql",
		clusterLabel:                  cluster.Name,
	}
	if cluster.Spec.InheritedMetadata != nil {
		maps.Copy(labels, cluster.Spec.InheritedMetadata.Labels)
	}
	if instanceName != "" {
		labels[instanceLabel] = instanceName
		labels[roleLabel] = "primary"
	}
	return labels
}
