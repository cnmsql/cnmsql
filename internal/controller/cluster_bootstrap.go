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
	"cmp"
	"slices"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

const (
	// pvcStatusAnnotation records whether an instance's data volume holds a
	// bootstrapped data directory (design 031). A new volume starts
	// initializing; the operator marks it ready when the instance's bootstrap
	// Job succeeds, and only then creates the instance Pod.
	pvcStatusAnnotation   = "mysql.cnmsql.co/pvc-status"
	pvcStatusInitializing = "initializing"
	pvcStatusReady        = "ready"
)

// pvcBootstrapped reports whether the volume holds a bootstrapped data
// directory. A volume without the annotation predates bootstrap Jobs: the
// instance Pod that created it bootstrapped it in an init container, so it
// counts as bootstrapped.
func pvcBootstrapped(pvc *corev1.PersistentVolumeClaim) bool {
	status, ok := pvc.Annotations[pvcStatusAnnotation]
	return !ok || status == pvcStatusReady
}

// bootstrapMode is what an instance's bootstrap Job does to its empty volume.
type bootstrapMode string

const (
	bootstrapModeInitDB  bootstrapMode = "initdb"
	bootstrapModeRestore bootstrapMode = "restore"
	bootstrapModeJoin    bootstrapMode = "join"
	bootstrapModeImport  bootstrapMode = "import"
)

const (
	// bootstrapInstanceLabel and bootstrapModeLabel identify a bootstrap Job and
	// its Pod. The Pod carries no cluster or instance label, so Services, PDBs,
	// the PodMonitor and the instance listing never select it.
	bootstrapInstanceLabel = "mysql.cnmsql.co/bootstrap-instance"
	bootstrapModeLabel     = "mysql.cnmsql.co/bootstrap-mode"
	// bootstrapPVCUIDAnnotation binds a Job to the volume it bootstraps: a
	// re-initialised instance gets a new PVC with the same name, and a Job for
	// the old one must never mark it bootstrapped.
	bootstrapPVCUIDAnnotation = "mysql.cnmsql.co/pvc-uid"
	// bootstrapSpecHashAnnotation is the hash of the Job's spec. A failed Job is
	// replaced only when the Job the operator would build now differs.
	bootstrapSpecHashAnnotation = "mysql.cnmsql.co/bootstrap-spec-hash"
)

// bootstrapJobBackoffLimit is the Kubernetes default: with its exponential
// backoff it rides out a primary or an object store that is briefly away.
const bootstrapJobBackoffLimit int32 = 6

const (
	// bootstrapControllerName is the shared init container that copies the
	// manager binary out of the operator image onto the scratch volume.
	bootstrapControllerName = "bootstrap-controller"
	// operatorManagerBinary is the manager binary's path inside the operator
	// image, before it is copied to the scratch volume.
	operatorManagerBinary = "/manager"
	// appLabelKey is the standard Kubernetes application label.
	appLabelKey = "app.kubernetes.io/name"
)

// bootstrapModeFor picks how an instance's empty volume is bootstrapped: the
// primary initialises a fresh data directory, restores a physical backup or
// loads a logical one; an async replica clones the primary; a Group
// Replication member initialises an empty server and provisions itself from a
// group donor via distributed recovery at run time.
func (r *ClusterReconciler) bootstrapModeFor(cluster *mysqlv1alpha1.Cluster, plan clusterPlan, inst instancePlan) bootstrapMode {
	if inst.IsPrimary {
		switch {
		case plan.Recovery != nil:
			return bootstrapModeRestore
		case plan.Import != nil:
			return bootstrapModeImport
		}
		return bootstrapModeInitDB
	}
	if r.topologyReconciler(cluster).PodPolicy(cluster).InitializeReplica {
		return bootstrapModeInitDB
	}
	return bootstrapModeJoin
}

func bootstrapJobName(inst instancePlan, mode bootstrapMode) string {
	return inst.Name + "-" + string(mode)
}

// bootstrapJobTemplate is the cluster-wide worker Job template the bootstrap
// Jobs take their resources, priority, tolerations, deadline and metadata from.
func bootstrapJobTemplate(cluster *mysqlv1alpha1.Cluster) mysqlv1alpha1.BackupJobTemplate {
	if cluster.Spec.Backup == nil {
		return mysqlv1alpha1.BackupJobTemplate{}
	}
	return mergeJobTemplates(cluster.Spec.Backup.JobTemplate)
}

// bootstrapContainers returns the Job's extra init containers and its main
// container for mode. They run the same instance commands the Pod's init
// containers ran before design 031.
func (r *ClusterReconciler) bootstrapContainers(
	cluster *mysqlv1alpha1.Cluster, plan clusterPlan, inst instancePlan, mode bootstrapMode, resources corev1.ResourceRequirements,
) ([]corev1.Container, corev1.Container) {
	step := func(name string, args []string, extraEnv []corev1.EnvVar) corev1.Container {
		return corev1.Container{
			Name:            name,
			Image:           plan.Image,
			ImagePullPolicy: cluster.Spec.ImagePullPolicy,
			Command:         []string{managerBinary},
			Args:            args,
			Env:             append(initEnv(plan), extraEnv...),
			VolumeMounts:    volumeMounts(),
			Resources:       resources,
			SecurityContext: cluster.Spec.SecurityContext,
		}
	}
	switch mode {
	case bootstrapModeRestore:
		return nil, step(string(mode), restoreArgs(plan), plan.Recovery.StoreEnv)
	case bootstrapModeImport:
		initdb := step(string(bootstrapModeInitDB), r.initdbArgs(cluster, cluster.Spec.Bootstrap.InitDB), nil)
		return []corev1.Container{initdb}, step(string(mode), importArgs(plan), plan.Import.StoreEnv)
	case bootstrapModeJoin:
		return nil, step(string(mode), joinArgs(cluster, plan), nil)
	default:
		// A Group Replication secondary initialises an empty server: no
		// application schema, the data comes from a group donor.
		initdb := cluster.Spec.Bootstrap.InitDB
		if !inst.IsPrimary {
			initdb = nil
		}
		return nil, step(string(bootstrapModeInitDB), r.initdbArgs(cluster, initdb), nil)
	}
}

// bootstrapJob builds the one-shot Job that bootstraps inst's volume. It runs
// under the instance's ServiceAccount (design 030's Role lets it read the
// credential Secrets) and follows the instance's scheduling, so a volume that
// binds on first use lands where the instance Pod can run (design 031, B8).
func (r *ClusterReconciler) bootstrapJob(
	cluster *mysqlv1alpha1.Cluster, plan clusterPlan, inst instancePlan, mode bootstrapMode, pvc *corev1.PersistentVolumeClaim,
) (*batchv1.Job, error) {
	tpl := bootstrapJobTemplate(cluster)
	resources := cluster.Spec.Resources
	if hasResourceRequirements(tpl.Resources) {
		resources = tpl.Resources
	}
	operatorImage := cmp.Or(plan.OperatorImage, plan.Image)
	extraInit, main := r.bootstrapContainers(cluster, plan, inst, mode, resources)

	podSpec := corev1.PodSpec{
		RestartPolicy:      corev1.RestartPolicyNever,
		ServiceAccountName: instanceServiceAccountName(inst),
		Volumes:            instanceVolumes(plan, inst),
		InitContainers: append([]corev1.Container{{
			Name:            bootstrapControllerName,
			Image:           operatorImage,
			ImagePullPolicy: cluster.Spec.ImagePullPolicy,
			Command:         []string{operatorManagerBinary},
			Args:            []string{managerBootstrapCmd, managerBinary},
			VolumeMounts:    volumeMounts(),
			Resources:       resources,
			SecurityContext: cluster.Spec.SecurityContext,
		}}, extraInit...),
		Containers:                []corev1.Container{main},
		NodeSelector:              cluster.Spec.Affinity.NodeSelector,
		Affinity:                  affinity(cluster),
		Tolerations:               append(slices.Clone(cluster.Spec.Affinity.Tolerations), tpl.Tolerations...),
		TopologySpreadConstraints: cluster.Spec.TopologySpreadConstraints,
		PriorityClassName:         cmp.Or(tpl.PriorityClassName, cluster.Spec.PriorityClassName),
		SchedulerName:             cluster.Spec.SchedulerName,
		SecurityContext:           podSecurityContext(cluster),
	}
	for _, pullSecret := range cluster.Spec.ImagePullSecrets {
		podSpec.ImagePullSecrets = append(podSpec.ImagePullSecrets, corev1.LocalObjectReference{Name: pullSecret.Name})
	}

	jobLabels := workerJobLabels(cluster.Name, bootstrapInstanceLabel, inst.Name)
	jobLabels[bootstrapModeLabel] = string(mode)
	podLabels := map[string]string{
		appLabelKey:            appLabelValue,
		bootstrapInstanceLabel: inst.Name,
		bootstrapModeLabel:     string(mode),
	}
	backoff := bootstrapJobBackoffLimit
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      bootstrapJobName(inst, mode),
			Namespace: cluster.Namespace,
			// Operator labels win over the template's.
			Labels: combineStringMaps(tpl.Labels, jobLabels),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:          &backoff,
			ActiveDeadlineSeconds: backupJobActiveDeadlineSeconds(tpl),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      combineStringMaps(tpl.Labels, podLabels),
					Annotations: combineStringMaps(tpl.Annotations, nil),
				},
				Spec: podSpec,
			},
		},
	}
	hash, err := hashObject(job.Spec)
	if err != nil {
		return nil, err
	}
	job.Annotations = combineStringMaps(tpl.Annotations, map[string]string{
		bootstrapPVCUIDAnnotation:   string(pvc.UID),
		bootstrapSpecHashAnnotation: hash,
	})
	if err := controllerutil.SetControllerReference(cluster, job, r.Scheme); err != nil {
		return nil, err
	}
	return job, nil
}
