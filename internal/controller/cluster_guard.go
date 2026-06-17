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
	"fmt"
	"maps"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

// reconcilePDB keeps the cluster's PodDisruptionBudgets in step with the spec.
//
// When spec.enablePDB is true (the default) the operator maintains two PDBs:
//
//   - {cluster}-primary: maxUnavailable=1, matches the pod holding role=primary.
//   - {cluster}-replicas: maxUnavailable=floor(N/2) for N replicas, matches pods
//     with role=replica. Single-instance clusters (no replicas) skip it.
//
// During a node maintenance window (spec.nodeMaintenanceWindow.inProgress with
// reusePVC) the relevant PDBs are torn down so the kubelet can drain the node;
// they are restored once the window closes. When enablePDB is false (or the
// cluster is deleted) both PDBs are removed.
func (r *ClusterReconciler) reconcilePDB(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) error {
	pdbEnabled := cluster.Spec.EnablePDB == nil || *cluster.Spec.EnablePDB

	// During a node maintenance window we must let nodes drain, so the PDBs that
	// would block eviction are removed for the duration of the window. The
	// replica PDB always goes; the primary PDB only goes for a single-instance
	// cluster, where the lone pod must be allowed to move with its (reused) PVC.
	maintenance := inNodeMaintenance(cluster)
	singleInstance := plan.Instances <= 1

	wantPrimary := pdbEnabled && (!maintenance || !singleInstance)
	wantReplica := pdbEnabled && !singleInstance && !maintenance

	if err := r.reconcileOnePDB(ctx, cluster, primaryPDBName(cluster), wantPrimary, func() *policyv1.PodDisruptionBudget {
		return buildPDB(cluster, primaryPDBName(cluster), rolePrimary, intstr.FromInt32(1))
	}); err != nil {
		return err
	}

	// Replicas tolerate losing up to half their number at once; the rest keep the
	// cluster serving reads and available as failover candidates.
	replicas := plan.Instances - 1
	return r.reconcileOnePDB(ctx, cluster, replicaPDBName(cluster), wantReplica, func() *policyv1.PodDisruptionBudget {
		return buildPDB(cluster, replicaPDBName(cluster), roleReplica, intstr.FromInt32(int32(replicas/2)))
	})
}

// reconcileOnePDB creates/updates the named PDB when want is true, or deletes it
// (if present) otherwise.
func (r *ClusterReconciler) reconcileOnePDB(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	name string,
	want bool,
	build func() *policyv1.PodDisruptionBudget,
) error {
	if !want {
		pdb := &policyv1.PodDisruptionBudget{}
		err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, pdb)
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		return r.Delete(ctx, pdb)
	}

	pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{
		Name:      name,
		Namespace: cluster.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		desired := build()
		pdb.Labels = desired.Labels
		// Selector is immutable once set; CreateOrUpdate re-applies it on create
		// and leaves the (identical) value untouched on update.
		pdb.Spec = desired.Spec
		return controllerutil.SetControllerReference(cluster, pdb, r.Scheme)
	})
	return err
}

// buildPDB returns a PodDisruptionBudget that selects the cluster's pods holding
// the given role, allowing at most maxUnavailable of them to be voluntarily
// disrupted at a time.
func buildPDB(cluster *mysqlv1alpha1.Cluster, name, role string, maxUnavailable intstr.IntOrString) *policyv1.PodDisruptionBudget {
	labels := labelsFor(cluster, "", "")
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &maxUnavailable,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					clusterLabel: cluster.Name,
					roleLabel:    role,
				},
			},
		},
	}
}

// inNodeMaintenance reports whether a node maintenance window is active for the
// cluster. The window only relaxes PDBs when PVCs are reused, otherwise draining
// a node would discard the instance's data.
func inNodeMaintenance(cluster *mysqlv1alpha1.Cluster) bool {
	window := cluster.Spec.NodeMaintenanceWindow
	if window == nil || !window.InProgress {
		return false
	}
	// ReusePVC defaults to true.
	return window.ReusePVC == nil || *window.ReusePVC
}

func primaryPDBName(cluster *mysqlv1alpha1.Cluster) string {
	return cluster.Name + "-primary"
}

func replicaPDBName(cluster *mysqlv1alpha1.Cluster) string {
	return cluster.Name + "-replicas"
}

// isPodFenced reports whether an instance Pod carries the fencing annotation set
// to "true".
func isPodFenced(pod *corev1.Pod) bool {
	return pod.Annotations[fencingAnnotation] == routableTrue
}

// reconcileFencing keeps each instance Pod's routable label in step with its
// fencing annotation and the operator's ability to reach it. A Pod gets
// routable=false — which drops it from every routing Service, whose selectors
// require routable=true — when it is fenced, or when an established replica has
// been unreachable for deRouteGracePeriod (so reads stop being served from a
// partitioned node). routable=true is restored once the Pod is unfenced and
// reachable again. The in-Pod reconciler separately reads status.fencedInstances
// and holds a fenced instance read-only.
func (r *ClusterReconciler) reconcileFencing(ctx context.Context, cluster *mysqlv1alpha1.Cluster, observed observedCluster) error {
	fenced := map[string]bool{}
	for _, name := range observed.FencedInstances {
		fenced[name] = true
	}
	reachable := map[string]bool{}
	for name := range observed.StatusByInstance {
		reachable[name] = true
	}
	// Only pull members out of routing for an already-established cluster; during
	// initial provisioning a not-yet-reachable replica is expected, not degraded.
	deRouteEligible := clusterEstablished(cluster)
	now := time.Now().Truncate(time.Second)

	for _, name := range observed.InstanceNames {
		pod := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, pod); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		if pod.DeletionTimestamp != nil {
			continue
		}
		before := pod.DeepCopy()
		if pod.Labels == nil {
			pod.Labels = map[string]string{}
		}

		// An unreachable established replica (never the primary, whose loss drives
		// failover instead) is de-routed once it has been unreachable past the grace
		// period. Reachable or ineligible Pods clear the marker.
		deRouted := false
		newlyUnreachable := false
		if deRouteEligible && name != observed.PrimaryName && !fenced[name] && !reachable[name] {
			switch since := pod.Annotations[unreachableSinceAnnotation]; since {
			case "":
				if pod.Annotations == nil {
					pod.Annotations = map[string]string{}
				}
				pod.Annotations[unreachableSinceAnnotation] = now.Format(time.RFC3339)
				newlyUnreachable = true
			default:
				if ts, err := time.Parse(time.RFC3339, since); err == nil && now.Sub(ts) >= deRouteGracePeriod {
					deRouted = true
				}
			}
		} else {
			delete(pod.Annotations, unreachableSinceAnnotation)
		}

		desired := routableTrue
		if fenced[name] || deRouted {
			desired = routableFalse
		}
		pod.Labels[routableLabel] = desired

		if maps.Equal(pod.Labels, before.Labels) && maps.Equal(pod.Annotations, before.Annotations) {
			continue
		}
		if err := r.Patch(ctx, pod, client.MergeFrom(before)); err != nil {
			return err
		}
		r.recordRoutingEvent(cluster, name, before.Labels[routableLabel], desired, fenced[name], deRouted, newlyUnreachable)
	}
	return nil
}

// recordRoutingEvent emits an Event describing a routing change for an instance:
// the first time it is seen unreachable, when it is pulled from or restored to
// routing. It is a no-op when nothing actionable changed.
func (r *ClusterReconciler) recordRoutingEvent(
	cluster *mysqlv1alpha1.Cluster,
	name, previousRoutable, desired string,
	fenced, deRouted, newlyUnreachable bool,
) {
	if r.Recorder == nil {
		return
	}
	switch {
	case desired == routableFalse && previousRoutable != routableFalse:
		reason, msg := "Fenced", fmt.Sprintf("Instance %s fenced; removed from routing", name)
		if deRouted {
			reason = "DeRouted"
			msg = fmt.Sprintf("Instance %s unreachable for %s; removed from read routing", name, deRouteGracePeriod)
		}
		r.Recorder.Event(cluster, corev1.EventTypeWarning, reason, msg)
	case desired == routableTrue && previousRoutable == routableFalse:
		verb := "Unfenced"
		if !fenced {
			verb = "Restored"
		}
		r.Recorder.Event(cluster, corev1.EventTypeNormal, verb,
			fmt.Sprintf("Instance %s %s to routing", name, verb))
	case newlyUnreachable:
		r.Recorder.Event(cluster, corev1.EventTypeWarning, "Unreachable",
			fmt.Sprintf("Instance %s control endpoint is unreachable", name))
	}
}
