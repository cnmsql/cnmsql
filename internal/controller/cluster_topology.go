/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
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
)

// reconcileInstances provisions the desired instances in ordinal order. To bound
// load on the primary, a replica is only created once the previous instance is
// ready; it returns false when it stopped early waiting for that prerequisite.
func (r *ClusterReconciler) reconcileInstances(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) (bool, error) {
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
