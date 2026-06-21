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
	"fmt"
	"maps"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
)

// reconcilePDB keeps the cluster's PodDisruptionBudgets in step with the spec.
//
// Under async replication the operator maintains two PDBs:
//
//   - {cluster}-primary: maxUnavailable=1, matches the pod holding role=primary.
//   - {cluster}-replicas: maxUnavailable=floor(N/2) for N replicas, matches pods
//     with role=replica. Single-instance clusters (no replicas) skip it.
//
// Under Group Replication a single quorum-aware PDB replaces the split:
//
//   - {cluster}-group: maxUnavailable = N - quorum (e.g. N=3 → 1, N=5 → 2),
//     matching every cluster Pod by cluster label only, so voluntary disruptions
//     can never break quorum.
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

	// Group Replication uses a single quorum-aware PDB; remove the async split.
	if cluster.IsGroupReplication() {
		if !pdbEnabled || singleInstance {
			_ = r.reconcileOnePDB(ctx, cluster, groupPDBName(cluster), false, nil)
			r.deleteAsyncSplitPDBs(ctx, cluster)
			return nil
		}
		r.deleteAsyncSplitPDBs(ctx, cluster)
		primaryMax, _ := r.topologyReconciler(cluster).PDBMaxUnavailable(cluster)
		wantGroup := pdbEnabled && !maintenance
		return r.reconcileOnePDB(ctx, cluster, groupPDBName(cluster), wantGroup, func() *policyv1.PodDisruptionBudget {
			return buildClusterWidePDB(cluster, groupPDBName(cluster), primaryMax)
		})
	}

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

// deleteAsyncSplitPDBs removes the async-style primary and replica PDBs, ensuring
// a switch from async to GR (or vice versa) cleans up the old topology's PDBs.
func (r *ClusterReconciler) deleteAsyncSplitPDBs(ctx context.Context, cluster *mysqlv1alpha1.Cluster) {
	_ = r.reconcileOnePDB(ctx, cluster, primaryPDBName(cluster), false, nil)
	_ = r.reconcileOnePDB(ctx, cluster, replicaPDBName(cluster), false, nil)
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

// buildClusterWidePDB returns a PDB that selects every cluster Pod without a
// role filter, used for the single quorum-aware GR PDB.
func buildClusterWidePDB(cluster *mysqlv1alpha1.Cluster, name string, maxUnavailable intstr.IntOrString) *policyv1.PodDisruptionBudget {
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
				},
			},
		},
	}
}

func groupPDBName(cluster *mysqlv1alpha1.Cluster) string {
	return cluster.Name + "-group"
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

// checkFenceQuorumGuard ensures that under Group Replication, fencing a member
// (which executes STOP GROUP_REPLICATION) never breaks quorum. It splits the
// observed fenced set into already-persisted (safe, group survived) and new
// additions, then checks whether fencing the new set would drop below quorum.
// If so the new instances are removed from the observed fenced set, the cluster
// is blocked with a quorum-guard reason, and a Warning Event is emitted.
// Returns the blocking reason (empty when all clear).
func (r *ClusterReconciler) checkFenceQuorumGuard(ctx context.Context, cluster *mysqlv1alpha1.Cluster, observed *observedCluster) string {
	if !cluster.IsGroupReplication() || len(observed.FencedInstances) == 0 {
		return ""
	}
	// Separate already-persisted fenced instances (from the last reconcile)
	// from newly observed ones. The persisted set represents fences that
	// already took effect; the group survived them, so they are safe.
	persisted := setFrom(cluster.Status.FencedInstances)
	var newFences []string
	for _, name := range observed.FencedInstances {
		if !persisted[name] {
			newFences = append(newFences, name)
		}
	}
	if len(newFences) == 0 {
		return ""
	}

	// Merge the fresh GR observation so quorum math sees latest member states.
	latest := cluster.DeepCopy()
	r.topologyReconciler(cluster).MergeStatus(latest, topology.Observation{
		GroupReplication: observed.GroupReplication,
	})

	// Persisted fenced members have already been instructed to leave the
	// group; the group view may be stale and still show them ONLINE. Remove
	// them so the quorum math sees the effective member count after all
	// persisted fences take effect.
	if gr := latest.Status.GroupReplication; gr != nil && len(persisted) > 0 {
		filtered := gr.Members[:0]
		for _, m := range gr.Members {
			if !persisted[m.Instance] {
				filtered = append(filtered, m)
			}
		}
		gr.Members = filtered
	}

	// Check whether fencing the new instances (on top of the ones already
	// fenced) would break quorum.
	topo := r.topologyReconciler(cluster)
	if guard := topo.FenceQuorumGuard(latest, newFences); guard != nil && guard.Blocked {
		// Remove the new fences from the observed set so the in-Pod
		// reconciler never executes STOP GROUP_REPLICATION for them.
		for _, name := range newFences {
			observed.FencedInstances = removeString(observed.FencedInstances, name)
		}
		logf.FromContext(ctx).Info("Blocking cluster: fencing would break quorum",
			"newFences", newFences, "reason", guard.Reason)
		if r.Recorder != nil {
			r.Recorder.Event(cluster, corev1.EventTypeWarning, "FenceQuorumGuardBlocked", guard.Reason)
		}
		observed.Phase = topology.PhaseBlocked
		observed.PhaseReason = guard.Reason
		observed.Ready = false
		observed.Progressing = false
		return guard.Reason
	}
	return ""
}

// removeString returns a copy of ss without the first occurrence of s, leaving
// the input slice (and its backing array) untouched.
func removeString(ss []string, s string) []string {
	for i, v := range ss {
		if v == s {
			out := make([]string, 0, len(ss)-1)
			out = append(out, ss[:i]...)
			return append(out, ss[i+1:]...)
		}
	}
	return ss
}

// handleQuorumRecovery is the opt-in, guarded quorum recovery path for Group
// Replication clusters. It triggers only when the Cluster carries the
// force-quorum-recovery annotation and quorum is provably lost. The operator
// computes a safe survivor via ComputeForceQuorumRecovery, stamps the survivor
// Pod with the force_members addresses so its in-Pod reconciler executes
// group_replication_force_members, and clears the Cluster annotation. When
// safety is unprovable the annotation is kept and the cluster stays Blocked.
func (r *ClusterReconciler) handleQuorumRecovery(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	observed observedCluster,
) (ctrl.Result, error, bool) {
	if !cluster.IsGroupReplication() {
		return ctrl.Result{}, nil, false
	}
	// Re-fetch the cluster to see the latest annotations. The passed-in
	// cluster may be stale if this reconcile was triggered by a Pod change
	// rather than the annotation itself.
	latestCluster := &mysqlv1alpha1.Cluster{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
	if err := r.Get(ctx, key, latestCluster); err != nil {
		return ctrl.Result{}, err, true
	}
	if latestCluster.Annotations[forceQuorumRecoveryAnnotation] != "yes" {
		return ctrl.Result{}, nil, false
	}
	gr := latestCluster.Status.GroupReplication
	if gr == nil || !gr.Bootstrapped || gr.HasQuorum {
		logf.FromContext(ctx).Info("Cannot perform quorum recovery: group is not in a recoverable state",
			"hasQuorum", gr != nil && gr.HasQuorum, "bootstrapped", gr != nil && gr.Bootstrapped)
		r.clearAnnotation(ctx, latestCluster, forceQuorumRecoveryAnnotation)
		return ctrl.Result{}, nil, false
	}

	recovery := r.topologyReconciler(latestCluster).ComputeForceQuorumRecovery(latestCluster, observed.GTIDByInstance)
	if recovery == nil {
		logf.FromContext(ctx).Info("Cannot compute safe quorum recovery survivor; cluster stays Blocked")
		r.Recorder.Event(latestCluster, corev1.EventTypeWarning, "QuorumRecoveryUnsafe",
			"No safe survivor set could be proven for quorum recovery")
		return ctrl.Result{RequeueAfter: readyResync}, nil, false
	}

	logf.FromContext(ctx).Info("Executing guarded quorum recovery",
		"survivor", recovery.Survivor, "action", recovery.Action, "forceMembers", recovery.ForceMembers)
	r.Recorder.Eventf(latestCluster, corev1.EventTypeNormal, "QuorumRecovery",
		"Designating %s as the quorum recovery member (%s)", recovery.Survivor, recovery.Action)

	survivorPod := &corev1.Pod{}
	survivorKey := types.NamespacedName{Namespace: latestCluster.Namespace, Name: recovery.Survivor}
	if err := r.Get(ctx, survivorKey, survivorPod); err != nil {
		if apierrors.IsNotFound(err) {
			logf.FromContext(ctx).Info("Survivor Pod not found; cannot proceed with quorum recovery", "survivor", recovery.Survivor)
			return ctrl.Result{RequeueAfter: provisioningRequeue}, nil, false
		}
		return ctrl.Result{}, err, true
	}
	podBefore := survivorPod.DeepCopy()
	if survivorPod.Annotations == nil {
		survivorPod.Annotations = map[string]string{}
	}
	// Stamp the survivor with the action-specific doorbell. force_members resets a
	// surviving (but sub-quorum) view; rebootstrap re-creates the group from
	// scratch after a total outage where no view survived.
	switch recovery.Action {
	case topology.QuorumRecoveryRebootstrap:
		survivorPod.Annotations[forceGroupRebootstrapAnnotation] = "yes"
	default:
		survivorPod.Annotations[forceQuorumMembersAnnotation] = recovery.ForceMembers
	}
	if err := r.Patch(ctx, survivorPod, client.MergeFrom(podBefore)); err != nil {
		return ctrl.Result{}, err, true
	}

	// Reset the sticky ObservedViewMax so the re-formed group reports
	// quorum against its actual (smaller) view size. Otherwise DonorAvailable
	// blocks member provisioning because the pre-crash max (e.g. 3) makes
	// a 1-member post-recovery group falsely read as quorum-lost.
	statusBefore := latestCluster.DeepCopy()
	if latestCluster.Status.GroupReplication != nil {
		latestCluster.Status.GroupReplication.ObservedViewMax = 0
		latestCluster.Status.GroupReplication.ObservedOnlineMax = 0
	}
	if err := r.Status().Patch(ctx, latestCluster, client.MergeFrom(statusBefore)); err != nil {
		logf.FromContext(ctx).Error(err, "Could not reset ObservedViewMax after quorum recovery")
	}

	r.clearAnnotation(ctx, latestCluster, forceQuorumRecoveryAnnotation)
	return ctrl.Result{RequeueAfter: provisioningRequeue}, nil, true
}

// clearAnnotation removes annotation key from the Cluster and persists the change.
func (r *ClusterReconciler) clearAnnotation(ctx context.Context, cluster *mysqlv1alpha1.Cluster, key string) {
	latest := &mysqlv1alpha1.Cluster{}
	nsKey := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
	if err := r.Get(ctx, nsKey, latest); err != nil {
		return
	}
	if latest.Annotations == nil {
		return
	}
	before := latest.DeepCopy()
	delete(latest.Annotations, key)
	if err := r.Patch(ctx, latest, client.MergeFrom(before)); err != nil {
		logf.FromContext(ctx).Error(err, "Could not clear Cluster annotation", "key", key)
		return
	}
	latest.DeepCopyInto(cluster)
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
	deRouteEligible := cluster.IsEstablished()
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
