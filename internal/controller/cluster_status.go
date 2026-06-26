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
	"fmt"
	"reflect"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/internal/controller/topology"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/groupreplication"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

type observedCluster struct {
	Phase       string
	PhaseReason string
	Ready       bool
	Progressing bool
	Plan        clusterPlan
	// PrimaryName is the instance acting as primary (fixed to ordinal 1 in M4).
	PrimaryName string
	// ReadyInstances is the number of instances reporting ready.
	ReadyInstances int
	// InstanceNames are the desired instance names, in ordinal order.
	InstanceNames []string
	// GTIDByInstance maps instance name to its gtid_executed set.
	GTIDByInstance map[string]string
	// ExecutableHashByInstance maps instance name to its reported instance-manager hash.
	ExecutableHashByInstance map[string]string
	// StatusByInstance maps instance name to the last successful control status.
	StatusByInstance map[string]*webserver.Status
	// DivergedInstances are reachable replicas whose executed GTID set is not
	// contained in the primary's (errant transactions). They cannot safely rejoin
	// without losing data, so they are surfaced rather than silently re-cloned.
	DivergedInstances []string
	// FencedInstances are instances whose Pod carries the fencing annotation.
	// They are excluded from routing and from failover candidacy and are kept
	// read-only by their in-Pod reconciler.
	FencedInstances []string
	// TerminatingInstances are instances whose Pod carries a DeletionTimestamp
	// (graceful termination, e.g. a node drain/eviction or manual delete) but is
	// still reachable. A terminating primary is the trigger for switchover-on-drain.
	TerminatingInstances []string
	// FailedInstances are instances whose Pod shows positive evidence of being
	// unable to run (Failed phase or a container stuck in CrashLoopBackOff after
	// repeated restarts). They mark a degradation independent of whether the
	// cluster ever finished provisioning.
	FailedInstances []string
	// ReplicationBrokenInstances are reachable replicas whose replication has
	// aborted with a recorded error (a stopped IO or SQL thread, e.g. a
	// duplicate-key conflict). Unlike a diverged replica — which is caught by GTID
	// comparison and reported separately — these are surfaced from the SQL-layer
	// error the in-Pod reconciler reports, so a replica that is Running but cannot
	// replicate is not mistaken for one still finishing provisioning.
	ReplicationBrokenInstances []string
	// ContinuousArchiving holds the primary's archiving frontier/health when
	// continuous archiving is enabled; nil otherwise.
	ContinuousArchiving *mysqlv1alpha1.ContinuousArchivingStatus
	// GroupReplication is the operator's aggregated view of the group under GR
	// mode (primary, members, quorum); nil for async clusters or before any
	// member is observed ONLINE. The sticky groupName/bootstrapped fields are
	// merged in patchStatus, not here.
	GroupReplication *mysqlv1alpha1.GroupReplicationStatus
}

// observe polls every desired instance and aggregates cluster-level readiness.
// The cluster is Ready when all desired instances report ready.
func (r *ClusterReconciler) observe(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) (observedCluster, error) {
	controlClient := r.instanceControlClient()

	observed := observedCluster{
		Plan:                     plan,
		PrimaryName:              plan.primaryName(cluster),
		InstanceNames:            plan.instanceNames(cluster),
		GTIDByInstance:           map[string]string{},
		ExecutableHashByInstance: map[string]string{},
		StatusByInstance:         map[string]*webserver.Status{},
		Progressing:              true,
	}

	for i := 1; i <= plan.Instances; i++ {
		inst := plan.instanceFor(cluster, i)
		pod := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.Name}, pod); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return observedCluster{}, err
		}
		if isPodFenced(pod) {
			observed.FencedInstances = append(observed.FencedInstances, inst.Name)
		}
		if pod.DeletionTimestamp != nil {
			observed.TerminatingInstances = append(observed.TerminatingInstances, inst.Name)
		}
		if podFailed(pod) {
			observed.FailedInstances = append(observed.FailedInstances, inst.Name)
		}
		// Poll the control API for any Pod that is Ready or merely Running. The
		// control endpoint answers independently of the mysqld readiness probe, so a
		// replica whose replication thread has aborted (Running but not Ready) still
		// reports its status. That is exactly when we most need it: it lets us read
		// the diverged GTID and the SQL-layer error instead of going blind on the one
		// instance that is broken.
		if !podReady(pod) && !podRunning(pod) {
			continue
		}
		status, err := controlClient.Status(ctx, cluster, inst.Name)
		if err != nil || status == nil {
			continue
		}
		observed.StatusByInstance[inst.Name] = status
		if status.Role == webserver.RolePrimary {
			observed.PrimaryName = inst.Name
		}
		if status.GTIDExecuted != "" {
			observed.GTIDByInstance[inst.Name] = status.GTIDExecuted
		}
		if status.ExecutableHash != "" {
			observed.ExecutableHashByInstance[inst.Name] = status.ExecutableHash
		}
		if status.IsReady {
			observed.ReadyInstances++
		}
	}

	// Under heavy writes, replicas may apply transactions the primary committed
	// after we read its GTID. Re-read the primary's GTID last so its snapshot
	// reflects the most recent state and replicas are never seen as supersets.
	if observed.PrimaryName != "" {
		if status, err := controlClient.Status(ctx, cluster, observed.PrimaryName); err == nil && status.GTIDExecuted != "" {
			observed.GTIDByInstance[observed.PrimaryName] = status.GTIDExecuted
		}
	}

	if cluster.IsArchivingEnabled() {
		observed.ContinuousArchiving = aggregateArchiving(observed)
	}

	topologyObservation := r.topologyReconciler(cluster).Observe(topologyObservationInput(observed, cluster.Status.GroupReplication, cluster.Status.DivergedInstances))
	if topologyObservation.PrimaryAuthoritative {
		observed.PrimaryName = topologyObservation.PrimaryName
	}
	observed.GroupReplication = topologyObservation.GroupReplication
	observed.DivergedInstances = topologyObservation.DivergedInstances
	observed.ReplicationBrokenInstances = topologyObservation.ReplicationBrokenInstances
	for _, name := range observed.DivergedInstances {
		// A diverged replica may keep its threads running while silently
		// diverging, so do not count it as a healthy ready instance.
		if status, ok := observed.StatusByInstance[name]; ok && status.IsReady {
			observed.ReadyInstances--
		}
	}

	observed.Ready = observed.ReadyInstances == plan.Instances && len(observed.DivergedInstances) == 0
	observed.Progressing = !observed.Ready

	observed.computeClusterPhase(cluster, plan)
	return observed, nil
}

// computeClusterPhase sets the Phase, PhaseReason, Ready, and Progressing
// fields based on the aggregated observation.
func (o *observedCluster) computeClusterPhase(cluster *mysqlv1alpha1.Cluster, plan clusterPlan) {
	if isFullOutage(cluster, *o) {
		o.Phase = topology.PhaseFullOutage
		o.PhaseReason = "all members are down; re-bootstrapping the group from the last-seen primary"
		o.Ready = false
		o.Progressing = false
	} else if isQuorumLost(cluster, *o) {
		o.Phase = topology.PhaseBlocked
		o.PhaseReason = "group has lost quorum; manual recovery required"
		o.Ready = false
		o.Progressing = false
	} else {
		switch {
		case len(o.DivergedInstances) > 0:
			o.Phase = topology.PhaseDegraded
			o.PhaseReason = fmt.Sprintf("replica(s) diverged from primary %s and cannot safely rejoin: %s",
				o.PrimaryName, strings.Join(o.DivergedInstances, ", "))
		case o.Ready:
			o.Phase = topology.PhaseReady
			o.PhaseReason = "All instances are ready"
		case len(o.FailedInstances) > 0 || len(o.ReplicationBrokenInstances) > 0:
			o.Phase = topology.PhaseDegraded
			o.PhaseReason = degradedReason(*o, plan)
		case cluster.IsEstablished():
			o.Phase = topology.PhaseDegraded
			o.PhaseReason = degradedReason(*o, plan)
		case o.ReadyInstances == 0:
			o.Phase = topology.PhasePending
			o.PhaseReason = "Waiting for the primary instance"
		default:
			o.Phase = topology.PhaseProvisioning
			o.PhaseReason = fmt.Sprintf("%d/%d instances ready", o.ReadyInstances, plan.Instances)
		}
	}
}

// establishedPhase reports whether a persisted phase implies the cluster had
// already completed initial provisioning. It exists only to backfill
// EstablishedAt for clusters last reconciled before that field existed; new
// establishment is recorded directly when the cluster first becomes Ready.
func establishedPhase(phase string) bool {
	switch phase {
	case "", topology.PhasePending, topology.PhaseProvisioning:
		return false
	default:
		return true
	}
}

// crashLoopRestartThreshold is how many container restarts must accumulate
// before a CrashLoopBackOff Pod is treated as a failed instance rather than a
// transient restart during normal startup (e.g. a replica briefly restarting
// while it waits for the primary to accept connections).
const crashLoopRestartThreshold = 3

// podFailed reports positive evidence that an instance Pod cannot run: the Pod
// reached the Failed phase, or a container is stuck in CrashLoopBackOff after
// repeated restarts. This is deliberately narrower than "not ready" so that a
// node which simply has not finished coming up is not mistaken for one that
// cannot start at all.
func podFailed(pod *corev1.Pod) bool {
	if pod.Status.Phase == corev1.PodFailed {
		return true
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if w := cs.State.Waiting; w != nil && w.Reason == "CrashLoopBackOff" &&
			cs.RestartCount >= crashLoopRestartThreshold {
			return true
		}
	}
	return false
}

// degradedReason describes which desired instances are keeping the cluster from
// being fully ready, so the Degraded phase points at the problem. Failed
// instances (cannot start) are called out separately from instances that are
// merely unreachable or still not ready.
func degradedReason(observed observedCluster, plan clusterPlan) string {
	base := fmt.Sprintf("%d/%d instances ready", observed.ReadyInstances, plan.Instances)
	// Instances explained by a more specific clause below are excluded from the
	// generic "unreachable or not ready" list so each is named once.
	explained := map[string]bool{}
	for _, name := range observed.FailedInstances {
		explained[name] = true
	}
	for _, name := range observed.ReplicationBrokenInstances {
		explained[name] = true
	}
	var detail []string
	if len(observed.FailedInstances) > 0 {
		detail = append(detail, "failing to start: "+strings.Join(observed.FailedInstances, ", "))
	}
	if len(observed.ReplicationBrokenInstances) > 0 {
		var broken []string
		for _, name := range observed.ReplicationBrokenInstances {
			if status, ok := observed.StatusByInstance[name]; ok && status.Replication != nil && status.Replication.LastError != "" {
				broken = append(broken, fmt.Sprintf("%s (%s)", name, status.Replication.LastError))
				continue
			}
			broken = append(broken, name)
		}
		detail = append(detail, "replication broken: "+strings.Join(broken, ", "))
	}
	var notReady []string
	for _, name := range unreadyInstanceNames(observed) {
		if !explained[name] {
			notReady = append(notReady, name)
		}
	}
	if len(notReady) > 0 {
		detail = append(detail, "unreachable or not ready: "+strings.Join(notReady, ", "))
	}
	if len(detail) == 0 {
		return base
	}
	return base + "; " + strings.Join(detail, "; ")
}

// unreadyInstanceNames returns the desired instances that are not reporting
// ready, in ordinal order: either unreachable (no control status was obtained)
// or reachable but not ready.
func unreadyInstanceNames(observed observedCluster) []string {
	var out []string
	for _, name := range observed.InstanceNames {
		if status, ok := observed.StatusByInstance[name]; !ok || !status.IsReady {
			out = append(out, name)
		}
	}
	return out
}

// aggregateArchiving derives the cluster-level archiving status from the
// primary instance's reported archiver state. Archiving is authoritative only
// on the primary (the single writer), so that is the instance whose frontier
// the cluster surfaces.
func aggregateArchiving(observed observedCluster) *mysqlv1alpha1.ContinuousArchivingStatus {
	out := &mysqlv1alpha1.ContinuousArchivingStatus{Enabled: true}
	status, ok := observed.StatusByInstance[observed.PrimaryName]
	if !ok || status.Archiving == nil {
		return out
	}
	a := status.Archiving
	out.LastArchivedBinlog = a.LastArchivedBinlog
	out.LastArchivedGTID = a.LastArchivedGTID
	out.LastArchivedTime = a.LastArchivedTime
	out.PendingFiles = a.PendingFiles
	out.LastFailureReason = a.LastError
	out.LastFailureTime = a.LastErrorTime
	return out
}

// archivingHealthy reports whether continuous archiving is keeping up: no
// recorded failure on the primary. Archive lag (pending files) alone is
// reported but is not treated as unhealthy unless it stalls with an error.
func archivingHealthy(status *mysqlv1alpha1.ContinuousArchivingStatus) bool {
	return status != nil && status.LastFailureReason == ""
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// podRunning reports whether the Pod has reached the Running phase, meaning its
// containers have started. The control API may answer for a Running Pod even
// when it is not Ready, which is the case we rely on to read status from an
// instance whose mysqld readiness probe is failing.
func podRunning(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodRunning
}

func (r *ClusterReconciler) patchStatus(ctx context.Context, cluster *mysqlv1alpha1.Cluster, observed observedCluster) error {
	latest := &mysqlv1alpha1.Cluster{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
	if err := r.Get(ctx, key, latest); err != nil {
		return err
	}
	before := latest.DeepCopy()
	// currentPrimary is owned by the instance that promotes itself (single
	// writer); the operator only reads it. Everything else is operator-observed.
	if len(observed.InstanceNames) > 0 {
		latest.Status.Instances = observed.Plan.Instances
		latest.Status.InstanceNames = observed.InstanceNames
		latest.Status.LatestGeneratedNode = observed.Plan.Instances
		latest.Status.Image = observed.Plan.Image
	} else {
		latest.Status.Instances = latest.Spec.Instances
		latest.Status.InstanceNames = nil
		latest.Status.LatestGeneratedNode = 0
		latest.Status.Image = ""
	}
	latest.Status.ObservedGeneration = latest.Generation
	latest.Status.Phase = observed.Phase
	latest.Status.PhaseReason = observed.PhaseReason
	latest.Status.ReadyInstances = observed.ReadyInstances
	latest.Status.DivergedInstances = observed.DivergedInstances
	latest.Status.FencedInstances = observed.FencedInstances
	latest.Status.FailedInstances = observed.FailedInstances
	latest.Status.ReplicationBrokenInstances = observed.ReplicationBrokenInstances
	// EstablishedAt is sticky: record it the first time the cluster is fully ready
	// (or backfill it for a cluster whose persisted phase already implies it was
	// operational, for upgrades that predate this field) and never clear it. It is
	// what Cluster.IsEstablished reads, so a transient drop to a provisioning phase no
	// longer erases that the cluster was once established.
	if latest.Status.EstablishedAt == nil && (observed.Ready || establishedPhase(before.Status.Phase)) {
		latest.Status.EstablishedAt = &metav1.Time{Time: time.Now()}
	}
	r.topologyReconciler(latest).MergeStatus(latest, topology.Observation{
		GroupReplication: observed.GroupReplication,
	})
	latest.Status.Certificates = r.certificateStatus(ctx, latest, observed.Plan)
	latest.Status.ContinuousArchiving = observed.ContinuousArchiving
	latest.Status.OperatorExecutableHash = r.OperatorExecutableHash
	if len(observed.ExecutableHashByInstance) > 0 {
		latest.Status.ExecutableHashByInstance = observed.ExecutableHashByInstance
	}
	if observed.ContinuousArchiving != nil {
		healthy := archivingHealthy(observed.ContinuousArchiving)
		reason := "Archiving"
		message := "Continuous binlog archiving is healthy"
		if !healthy {
			reason = "ArchivingDegraded"
			message = "Continuous binlog archiving is degraded: " + observed.ContinuousArchiving.LastFailureReason
		}
		apimeta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type:               conditionContinuousArchiving,
			Status:             conditionStatus(healthy),
			Reason:             reason,
			Message:            message,
			ObservedGeneration: latest.Generation,
		})
	}
	apimeta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             conditionStatus(observed.Ready),
		Reason:             observed.Phase,
		Message:            observed.PhaseReason,
		ObservedGeneration: latest.Generation,
	})
	apimeta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
		Type:               conditionProgressing,
		Status:             conditionStatus(observed.Progressing),
		Reason:             observed.Phase,
		Message:            observed.PhaseReason,
		ObservedGeneration: latest.Generation,
	})
	// gtid_executed advances on every write, so persisting it on every reconcile
	// would patch the Cluster status (an etcd write) continuously under load. It
	// is purely informational — failover and switchover decisions read the live
	// gtid_executed, never this field. So we refresh it only when (a) some other
	// part of the status is already changing and the write is happening anyway, or
	// (b) the last persisted snapshot is older than gtidPersistInterval, so it
	// stays reasonably fresh without writing on every reconcile.
	otherChanged := !reflect.DeepEqual(before.Status, latest.Status)
	if len(observed.GTIDByInstance) > 0 && (otherChanged || gtidSnapshotStale(before.Status.GTIDExecutedUpdatedAt)) {
		latest.Status.GTIDExecutedByInstance = observed.GTIDByInstance
		latest.Status.GTIDExecutedUpdatedAt = &metav1.Time{Time: time.Now()}
	}
	if reflect.DeepEqual(before.Status, latest.Status) {
		return nil
	}
	if before.Status.Phase != observed.Phase {
		logf.FromContext(ctx).Info("Cluster phase changed",
			"from", before.Status.Phase, "to", observed.Phase,
			"reason", observed.PhaseReason, "readyInstances", observed.ReadyInstances)
	}
	r.recordPhaseTransition(latest, before.Status.Phase, observed)
	if observed.Phase == topology.PhaseBlocked && latest.IsGroupReplication() &&
		latest.Status.GroupReplication != nil && !latest.Status.GroupReplication.HasQuorum {
		if r.Recorder != nil {
			r.Recorder.Event(latest, corev1.EventTypeWarning, "QuorumLost",
				"Group has lost quorum; manual recovery required")
		}
	}
	if observed.Phase == topology.PhaseFullOutage && before.Status.Phase != topology.PhaseFullOutage {
		if r.Recorder != nil {
			r.Recorder.Event(latest, corev1.EventTypeWarning, "FullOutage",
				"All members are down; re-bootstrapping the group from the last-seen primary")
		}
	}
	if err := r.Status().Patch(ctx, latest, client.MergeFrom(before)); err != nil {
		return err
	}
	if from, to, ok := r.topologyReconciler(latest).ObservedFailover(before, latest); ok {
		logf.FromContext(ctx).Info("Observed Group Replication failover", "from", from, "to", to)
		if r.Recorder != nil {
			r.Recorder.Eventf(latest, corev1.EventTypeNormal, eventFailoverObserved,
				"Observed Group Replication failover from %s to %s", from, to)
		}
	}
	return nil
}

// gtidPersistInterval bounds how often the operator persists the gtid_executed
// snapshot when nothing else in the status is changing.
const gtidPersistInterval = 5 * time.Minute

// gtidSnapshotStale reports whether the persisted gtid_executed snapshot is old
// enough to be refreshed. A nil timestamp (never persisted) counts as stale.
func gtidSnapshotStale(updatedAt *metav1.Time) bool {
	return updatedAt == nil || time.Since(updatedAt.Time) >= gtidPersistInterval
}

func (r *ClusterReconciler) certificateStatus(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	plan clusterPlan,
) *mysqlv1alpha1.CertificatesStatus {
	if plan.ServerCASecretName == "" && plan.ClientCASecretName == "" && plan.ClientTLSSecret == "" {
		return nil
	}
	serverTLSSecret := plan.UserServerTLSSecret
	if serverTLSSecret == "" && plan.Instances > 0 {
		serverTLSSecret = plan.instanceFor(cluster, 1).ServerTLSSecret
	}
	status := &mysqlv1alpha1.CertificatesStatus{
		CertificatesConfiguration: mysqlv1alpha1.CertificatesConfiguration{
			ServerCASecret:       plan.ServerCASecretName,
			ServerTLSSecret:      serverTLSSecret,
			ClientCASecret:       plan.ClientCASecretName,
			ReplicationTLSSecret: plan.ClientTLSSecret,
		},
		Expirations: r.certificateExpirations(ctx, cluster, plan),
	}
	if len(status.Expirations) == 0 {
		status.Expirations = nil
	}
	return status
}

func (r *ClusterReconciler) certificateExpirations(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	plan clusterPlan,
) map[string]string {
	certNames := []string{plan.CAIssuer, cluster.Name + "-client"}
	for i := 1; i <= plan.Instances; i++ {
		certNames = append(certNames, plan.instanceFor(cluster, i).ServerCertName)
	}
	expirations := map[string]string{}
	for _, certName := range certNames {
		cert := &unstructured.Unstructured{}
		cert.SetGroupVersionKind(certificateGVK)
		if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: certName}, cert); err != nil {
			continue
		}
		notAfter, ok, _ := unstructured.NestedString(cert.Object, "status", "notAfter")
		if !ok || notAfter == "" {
			continue
		}
		secretName, ok, _ := unstructured.NestedString(cert.Object, "spec", "secretName")
		if !ok || secretName == "" {
			secretName = certName
		}
		expirations[secretName] = notAfter
	}
	return expirations
}

// recordPhaseTransition emits an Event only when the phase actually changes, so
// steady-state resyncs do not spam the event stream.
func (r *ClusterReconciler) recordPhaseTransition(cluster *mysqlv1alpha1.Cluster, previousPhase string, observed observedCluster) {
	if r.Recorder == nil || observed.Phase == previousPhase {
		return
	}
	eventType := corev1.EventTypeNormal
	if observed.Phase == topology.PhaseBlocked || observed.Phase == topology.PhaseDegraded ||
		observed.Phase == topology.PhaseFullOutage {
		eventType = corev1.EventTypeWarning
	}
	r.Recorder.Event(cluster, eventType, observed.Phase, observed.PhaseReason)
}

func conditionStatus(ok bool) metav1.ConditionStatus {
	if ok {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

// isQuorumLost reports whether a Group Replication cluster has lost quorum:
// the group was previously bootstrapped but the operator's view (online members
// vs the sticky ObservedViewMax — the largest view ever seen) shows fewer than
// a majority of the observed-max members ONLINE.
func isQuorumLost(cluster *mysqlv1alpha1.Cluster, observed observedCluster) bool {
	if !cluster.IsGroupReplication() {
		return false
	}
	gr := cluster.Status.GroupReplication
	if gr == nil || !gr.Bootstrapped {
		return false
	}
	if observed.GroupReplication == nil {
		// No member reports a group view. Treat as quorum-lost only when the
		// group was previously observed at its full configured size — not during
		// initial bootstrap or scale-up where observation is still converging.
		return gr.ObservedViewMax >= cluster.Spec.Instances
	}
	// Use the sticky ObservedViewMax as the quorum denominator so that a
	// shrunken group (members expelled) is flagged as quorum-lost, while a
	// bootstrapping group that hasn't yet reached its full size uses the
	// actual view size. MergeStatus applies the same logic when persisting.
	denominator := gr.ObservedViewMax
	if denominator <= 0 {
		denominator = len(observed.GroupReplication.Members)
	}
	online := 0
	for _, m := range observed.GroupReplication.Members {
		if m.State == groupreplication.MemberStateOnline {
			online++
		}
	}
	return online*2 <= denominator
}

// isFullOutage reports whether a Group Replication cluster has lost quorum with
// no ONLINE member anywhere: a total outage where the group view is gone. This
// is the subset of quorum loss that force_members cannot fix (there is no live
// view to reset), so the group must be re-bootstrapped from the last-seen
// primary. A partial quorum loss — some members still ONLINE — stays Blocked and
// recovers via force_members instead.
func isFullOutage(cluster *mysqlv1alpha1.Cluster, observed observedCluster) bool {
	if !cluster.IsGroupReplication() {
		return false
	}
	gr := cluster.Status.GroupReplication
	if gr == nil || !gr.Bootstrapped || gr.HasQuorum {
		return false
	}
	// A surviving ONLINE member means a partial quorum loss (force_members can
	// reset the live view), not a total outage.
	if observed.GroupReplication != nil {
		for _, m := range observed.GroupReplication.Members {
			if m.State == groupreplication.MemberStateOnline {
				return false
			}
		}
	}
	// No ONLINE member anywhere. Treat as a total outage only once the cluster was
	// established — during initial bootstrap the group legitimately has not formed
	// yet. This is keyed on establishment rather than the sticky ObservedViewMax so
	// the cluster stays in FullOutage for the whole recovery, including the window
	// after handleQuorumRecovery resets ObservedViewMax to 0 (which it does so the
	// re-formed, initially single-member group reads as quorate). Otherwise the
	// phase would flip out of FullOutage before the survivor re-forms the group,
	// abandoning the auto-recovery retry loop and the in-Pod stand-down.
	return cluster.IsEstablished()
}
