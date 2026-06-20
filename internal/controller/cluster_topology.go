/*
Copyright 2026 The CloudNative MySQL Authors.

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
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
)

// reconcileInstances provisions the desired instances in ordinal order. To bound
// load on the primary, a replica is only created once the previous instance is
// ready; it returns false when it stopped early waiting for that prerequisite.
func (r *ClusterReconciler) reconcileInstances(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan, observed observedCluster) (bool, error) {
	for i := 1; i <= plan.Instances; i++ {
		inst := plan.instanceFor(cluster, i)
		if i > 1 {
			prevReady, err := r.instancePodReady(ctx, cluster, plan.instanceFor(cluster, i-1))
			if err != nil {
				return false, err
			}
			if !prevReady {
				// The previous instance is not ready yet: ramp up later.
				return false, nil
			}
			// A new member is provisioned from a healthy source: async clones the
			// primary over a streamed backup, GR joins a quorate group and recovers
			// from an ONLINE donor (distributed recovery). Never create one while no
			// such source exists — an async clone would fail against an unhealthy
			// primary (and seeding from one about to be failed over risks divergence),
			// and a GR join would stall with no quorum or no donor to recover from.
			// Existing members are left alone (already provisioned); only the first
			// creation of a member Pod is gated, so this never blocks routine
			// reconciliation of a running cluster.
			exists, err := r.instancePodExists(ctx, cluster, inst)
			if err != nil {
				return false, err
			}
			topologyReconciler := r.topologyReconciler(cluster)
			if !exists && !topologyReconciler.DonorAvailable(topology.Observation{
				GroupReplication: observed.GroupReplication,
			}, topologyFailoverState(observed)) {
				logf.FromContext(ctx).Info("Deferring member creation: no healthy provisioning source",
					"instance", inst.Name, "primary", observed.PrimaryName,
					"topology", topologyReconciler.Name())
				return false, nil
			}
		}
		if err := r.ensureInstance(ctx, cluster, plan, inst); err != nil {
			return false, err
		}
	}
	return true, nil
}

// ensureInstance reconciles all per-instance resources.
func (r *ClusterReconciler) ensureInstance(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan, inst instancePlan) error {
	// A pending re-initialisation tears the instance's Pod and PVC down before
	// they are recreated empty. While that teardown is in progress, skip the
	// normal ensure* below so we do not immediately recreate what we are deleting.
	if handled, err := r.reconcileReinit(ctx, cluster, inst); err != nil || handled {
		return err
	}
	if err := r.ensureConfigMap(ctx, cluster, plan, inst); err != nil {
		return err
	}
	if err := r.ensurePVC(ctx, cluster, inst); err != nil {
		return err
	}
	if err := r.ensureInstanceService(ctx, cluster, inst); err != nil {
		return err
	}
	return r.ensurePod(ctx, cluster, plan, inst)
}

// instancePodReady reports whether the instance Pod exists and is Ready.
func (r *ClusterReconciler) instancePodReady(ctx context.Context, cluster *mysqlv1alpha1.Cluster, inst instancePlan) (bool, error) {
	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.Name}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return podReady(pod), nil
}

// instancePodExists reports whether the instance Pod has already been created.
// It distinguishes a brand-new replica (to be cloned from the primary) from one
// whose data is already in place.
func (r *ClusterReconciler) instancePodExists(ctx context.Context, cluster *mysqlv1alpha1.Cluster, inst instancePlan) (bool, error) {
	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.Name}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// scaleDownReplicas removes instances whose ordinal exceeds the desired count.
// Per the M4 retention policy the PVC is left in place for the user to keep or
// delete; the current primary is never removed.
func (r *ClusterReconciler) scaleDownReplicas(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) error {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(cluster.Namespace), client.MatchingLabels{clusterLabel: cluster.Name}); err != nil {
		return err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		ordinal, ok := instanceOrdinal(cluster, pod.Name)
		if !ok || ordinal <= plan.Instances || pod.Name == plan.primaryName(cluster) {
			continue
		}
		logf.FromContext(ctx).Info("Scaling down instance", "instance", pod.Name, "desiredInstances", plan.Instances)
		if err := r.removeInstanceResources(ctx, cluster, plan.instanceFor(cluster, ordinal)); err != nil {
			return err
		}
	}
	return nil
}

// removeInstanceResources deletes the owned Pod, ConfigMap and Service for a
// removed instance. The PVC is intentionally retained.
func (r *ClusterReconciler) removeInstanceResources(ctx context.Context, cluster *mysqlv1alpha1.Cluster, inst instancePlan) error {
	objects := []client.Object{
		&corev1.Pod{},
		&corev1.ConfigMap{},
		&corev1.Service{},
	}
	names := []string{inst.Name, inst.ConfigMapName, inst.ServiceName}
	for i, obj := range objects {
		obj.SetNamespace(cluster.Namespace)
		obj.SetName(names[i])
		if err := r.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// instanceOrdinal parses the 1-based ordinal from an instance name of the form
// "<cluster>-<ordinal>".
func instanceOrdinal(cluster *mysqlv1alpha1.Cluster, name string) (int, bool) {
	suffix, ok := strings.CutPrefix(name, cluster.Name+"-")
	if !ok {
		return 0, false
	}
	ordinal, err := strconv.Atoi(suffix)
	if err != nil || ordinal < 1 {
		return 0, false
	}
	return ordinal, true
}
