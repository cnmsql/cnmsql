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
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/internal/controller/topology"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/groupreplication"
)

// reconcileInstances provisions the desired instances in ordinal order. To bound
// load on the primary, a replica is only created once the previous instance is
// ready; it returns false when it stopped early waiting for that prerequisite.
//
// Cluster-wide template changes (config, seed, scale) are rolled one Pod at a
// time: each pass deletes at most one Pod and returns false so the caller
// requeues, and the next ordinal is gated on the previous one being fully ready
// (Kube- and MySQL-wise). The elected primary is always rolled last, after every
// replica is back online.
func (r *ClusterReconciler) reconcileInstances(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan, observed observedCluster) (bool, error) {
	// A total outage (every member down, group view gone) recovers by re-forming
	// the group from the last-seen primary alone — not by racing every member up at
	// once and hoping. Bring only that primary up first; once it re-bootstraps and
	// regains quorum the phase leaves FullOutage and the remaining members rejoin
	// through normal, donor-gated provisioning below. With no recorded primary we
	// fall through to ordinal-order provisioning (the all-GTIDs re-bootstrap bar
	// still guards safety).
	if observed.Phase == topology.PhaseFullOutage && cluster.Status.CurrentPrimary != "" {
		if ordinal, ok := instanceOrdinal(cluster, cluster.Status.CurrentPrimary); ok {
			if _, err := r.ensureInstance(ctx, cluster, plan, plan.instanceFor(cluster, ordinal), false); err != nil {
				return false, err
			}
			// Provisioning is intentionally incomplete: hold here until the primary
			// re-forms the group and quorum returns, then a later pass provisions the rest.
			return false, nil
		}
	}

	// The elected primary is rolled last: a cluster-wide change (config, seed,
	// scale) rolls every replica first, one at a time, and only takes the primary
	// down once the replicas are back online. Fall back to the planned primary when
	// none is observed (initial bootstrap or total outage), where normal
	// ordinal-order provisioning applies and nothing is deferred.
	primaryName := observed.PrimaryName
	if primaryName == "" {
		primaryName = plan.primaryName(cluster)
	}

	for i := 1; i <= plan.Instances; i++ {
		inst := plan.instanceFor(cluster, i)
		if i > 1 {
			proceed, err := r.gateInstance(ctx, cluster, plan, inst, observed)
			if err != nil {
				return false, err
			}
			if !proceed {
				return false, nil
			}
		}
		// Defer the elected primary's roll to the final step below.
		rolled, err := r.ensureInstance(ctx, cluster, plan, inst, inst.Name != primaryName)
		if err != nil {
			return false, err
		}
		if rolled {
			// This instance's Pod was deleted to apply a template change. Stop and
			// requeue (provisioned=false): the next ordinal must not roll until this
			// one comes back fully ready, and the pre-pass observation still shows it
			// healthy, so we cannot rely on the gate to catch it within this pass.
			return false, nil
		}
	}

	// Every replica is provisioned and up to date. Roll the primary last, but only
	// while the whole cluster is ready, so the primary is never taken down with a
	// replica still unavailable.
	if ordinal, ok := instanceOrdinal(cluster, primaryName); ok && observed.Ready {
		rolled, err := r.ensureInstance(ctx, cluster, plan, plan.instanceFor(cluster, ordinal), true)
		if err != nil {
			return false, err
		}
		if rolled {
			return false, nil
		}
	}
	return true, nil
}

// gateInstance decides whether reconciliation may proceed to instance i (i > 1).
// It serialises two distinct actions:
//
//   - Rolling an existing Pod (a template change): gate on the previous member
//     being fully ready so a cluster-wide change rolls one member at a time.
//   - Provisioning a brand-new member (no Pod and no PVC): gate on the previous
//     member being ready AND a healthy donor existing, since the member must be
//     cloned from a live source.
//
// A member whose Pod is missing but whose PVC survives is neither: it already
// holds the group's data, so it is brought up unconditionally — a roll recreate
// or a total-outage restart. Bringing it up needs no donor and is what lets the
// group re-form and every member's GTID become observable; data-loss safety is
// enforced downstream (the re-bootstrap survivor must GTID-dominate all others),
// not here. Gating that restart on "previous ONLINE" would deadlock a total
// outage, where no member can be ONLINE until enough members are back up.
func (r *ClusterReconciler) gateInstance(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	plan clusterPlan,
	inst instancePlan,
	observed observedCluster,
) (bool, error) {
	podExists, err := r.instancePodExists(ctx, cluster, inst)
	if err != nil {
		return false, err
	}
	// During guided recovery from confirmed GR quorum loss, bring every persistent
	// member up before applying the normal previous-member readiness gate. The
	// operator needs each member's live GTID to prove which survivor may safely
	// re-bootstrap the group; none of these Pods can become Ready until that
	// re-bootstrap happens. Without this exception, the first restarted-but-NotReady
	// Pod blocks reconciliation before the remaining missing Pods are recreated.
	quorumRecoveryBlocked := cluster.IsGroupReplication() &&
		cluster.Status.Phase == topology.PhaseBlocked &&
		cluster.Status.GroupReplication != nil &&
		cluster.Status.GroupReplication.Bootstrapped &&
		!cluster.Status.GroupReplication.HasQuorum
	if !podExists || quorumRecoveryBlocked {
		hasData, err := r.instancePVCExists(ctx, cluster, inst)
		if err != nil {
			return false, err
		}
		if hasData {
			// Existing member restarting from its own data: bring it up, no donor.
			return true, nil
		}
	}

	prevReady, err := r.instanceFullyReady(ctx, cluster, observed, plan.instanceFor(cluster, inst.Ordinal-1))
	if err != nil {
		return false, err
	}
	if !prevReady {
		// The previous instance is not fully ready yet: ramp up later.
		return false, nil
	}
	if podExists {
		// The Pod already exists; reconciliation may roll it. The previous member
		// is ready, so the roll may proceed.
		return true, nil
	}

	// Brand-new member (no Pod, no PVC). It is provisioned from a healthy source:
	// async clones the primary over a streamed backup, GR joins a quorate group and
	// recovers from an ONLINE donor (distributed recovery). Never create one while
	// no such source exists — an async clone would fail against an unhealthy primary
	// (and seeding from one about to be failed over risks divergence), and a GR join
	// would stall with no quorum or no donor to recover from.
	topologyReconciler := r.topologyReconciler(cluster)
	if !topologyReconciler.DonorAvailable(topology.Observation{
		GroupReplication: observed.GroupReplication,
	}, topologyFailoverState(observed)) {
		logf.FromContext(ctx).Info("Deferring member creation: no healthy provisioning source",
			"instance", inst.Name, "primary", observed.PrimaryName,
			"topology", topologyReconciler.Name())
		return false, nil
	}
	return true, nil
}

// ensureInstance reconciles all per-instance resources. It returns rolled=true
// when reconciliation deleted the instance's Pod (a re-initialisation teardown
// or a template change applied by ensurePod), signalling the caller to stop and
// requeue rather than advance to the next ordinal. allowRoll is forwarded to
// ensurePod so the caller can defer the primary's template roll.
func (r *ClusterReconciler) ensureInstance(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan, inst instancePlan, allowRoll bool) (bool, error) {
	// A pending re-initialisation tears the instance's Pod and PVC down before
	// they are recreated empty. While that teardown is in progress, skip the
	// normal ensure* below so we do not immediately recreate what we are deleting.
	if handled, err := r.reconcileReinit(ctx, cluster, inst); err != nil || handled {
		return handled, err
	}
	if err := r.ensureConfigMap(ctx, cluster, plan, inst); err != nil {
		return false, err
	}
	needsResizeRoll, err := r.ensurePVC(ctx, cluster, inst)
	if err != nil {
		return false, err
	}
	if err := r.ensureInstanceService(ctx, cluster, inst); err != nil {
		return false, err
	}
	// A volume whose backend cannot expand in use only finishes resizing once the
	// Pod is recycled. Route that through the same serialised roll as a template
	// change: when allowRoll is false (the primary, deferred to last) leave the Pod
	// for a later pass; ensurePod below is then a no-op for the unchanged template.
	if needsResizeRoll {
		if rolled, err := r.rollForResize(ctx, cluster, inst, allowRoll); err != nil || rolled {
			return rolled, err
		}
	}
	return r.ensurePod(ctx, cluster, plan, inst, allowRoll)
}

// instanceFullyReady reports whether an instance is ready to serve as the
// prerequisite for rolling the next ordinal: Kube-wise its Pod exists, is not
// terminating and is Ready; MySQL-wise the instance manager reports mysqld ready
// and, under Group Replication, the member is ONLINE in the group view.
//
// The live Pod re-read (rejecting a DeletionTimestamp) is what serialises a
// rolling change within a single pass: when ensurePod deletes a Pod, the Pod
// keeps its Ready condition through graceful termination, so a gate that trusted
// only that condition — or the pre-pass observation, which still shows the Pod
// healthy — would march on and delete every ordinal at once.
func (r *ClusterReconciler) instanceFullyReady(ctx context.Context, cluster *mysqlv1alpha1.Cluster, observed observedCluster, inst instancePlan) (bool, error) {
	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.Name}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if pod.DeletionTimestamp != nil || !podReady(pod) {
		return false, nil
	}
	status, ok := observed.StatusByInstance[inst.Name]
	if !ok || !status.IsReady {
		return false, nil
	}
	if cluster.IsGroupReplication() {
		return memberOnline(observed.GroupReplication, inst.Name), nil
	}
	return true, nil
}

// memberOnline reports whether the named instance is ONLINE in the group view.
func memberOnline(gr *mysqlv1alpha1.GroupReplicationStatus, instance string) bool {
	if gr == nil {
		return false
	}
	for _, m := range gr.Members {
		if m.Instance == instance {
			return m.State == groupreplication.MemberStateOnline
		}
	}
	return false
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

// instancePVCExists reports whether the instance's data PVC is present. A member
// whose PVC survives holds its own copy of the data and is restarting (a roll
// recreate or a total-outage restart), not being freshly provisioned, so it needs
// no donor to come back up.
func (r *ClusterReconciler) instancePVCExists(ctx context.Context, cluster *mysqlv1alpha1.Cluster, inst instancePlan) (bool, error) {
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.PVCName}, pvc); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// scaleDownReplicas removes instances whose ordinal exceeds the desired count.
// Per the M4 retention policy the PVC is left in place for the user to keep or
// delete; the current primary is never removed. Under Group Replication, a
// member is first fenced (STOP GROUP_REPLICATION) so it gracefully leaves the
// group before the Pod is deleted, and removal is refused when it would drop
// the group below quorum.
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
		topo := r.topologyReconciler(cluster)
		if guard := topo.ScaleDownQuorumGuard(cluster, pod.Name); guard != nil && guard.Blocked {
			logf.FromContext(ctx).Info("Cannot scale down: quorum guard blocked",
				"instance", pod.Name, "reason", guard.Reason)
			continue
		}
		// Under GR, fence the member first so it leaves the group cleanly
		// (STOP GROUP_REPLICATION) before the Pod is deleted. The fencing
		// annotation drives the in-Pod reconciler; on the next reconcile
		// the Pod is removed from the group view and safe to delete.
		if cluster.IsGroupReplication() && !isPodFenced(pod) {
			if err := r.stampFencingAnnotation(ctx, cluster, pod); err != nil {
				return err
			}
			continue
		}
		logf.FromContext(ctx).Info("Scaling down instance", "instance", pod.Name, "desiredInstances", plan.Instances)
		if err := r.removeInstanceResources(ctx, cluster, plan.instanceFor(cluster, ordinal)); err != nil {
			return err
		}
	}
	return nil
}

// stampFencingAnnotation sets the fencing annotation on a Pod, triggering the
// in-Pod reconciler to fence the instance (stop mysqld for async, STOP
// GROUP_REPLICATION for GR). The routing reconciler picks it up and sets
// routable=false on the next pass.
func (r *ClusterReconciler) stampFencingAnnotation(ctx context.Context, _ *mysqlv1alpha1.Cluster, pod *corev1.Pod) error {
	if pod.Annotations[fencingAnnotation] == routableTrue {
		return nil
	}
	before := pod.DeepCopy()
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[fencingAnnotation] = routableTrue
	return r.Patch(ctx, pod, client.MergeFrom(before))
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
