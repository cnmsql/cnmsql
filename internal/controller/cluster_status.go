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

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/replication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
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
	// ContinuousArchiving holds the primary's archiving frontier/health when
	// continuous archiving is enabled; nil otherwise.
	ContinuousArchiving *mysqlv1alpha1.ContinuousArchivingStatus
}

// observe polls every desired instance and aggregates cluster-level readiness.
// The cluster is Ready when all desired instances report ready.
func (r *ClusterReconciler) observe(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) (observedCluster, error) {
	controlClient := r.instanceControlClient()

	observed := observedCluster{
		Plan:             plan,
		PrimaryName:      plan.primaryName(cluster),
		InstanceNames:    plan.instanceNames(cluster),
		GTIDByInstance:   map[string]string{},
		StatusByInstance: map[string]*webserver.Status{},
		Progressing:      true,
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
		if !podReady(pod) {
			continue
		}
		status, err := controlClient.Status(ctx, cluster, inst.Name)
		if err != nil {
			continue
		}
		observed.StatusByInstance[inst.Name] = status
		if status.Role == webserver.RolePrimary {
			observed.PrimaryName = inst.Name
		}
		if status.GTIDExecuted != "" {
			observed.GTIDByInstance[inst.Name] = status.GTIDExecuted
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

	if archivingEnabled(cluster) {
		observed.ContinuousArchiving = aggregateArchiving(observed)
	}

	observed.DivergedInstances = detectDivergedReplicas(observed)
	for _, name := range observed.DivergedInstances {
		// A diverged replica may keep its threads running while silently
		// diverging, so do not count it as a healthy ready instance.
		if status, ok := observed.StatusByInstance[name]; ok && status.IsReady {
			observed.ReadyInstances--
		}
	}

	observed.Ready = observed.ReadyInstances == plan.Instances && len(observed.DivergedInstances) == 0
	observed.Progressing = !observed.Ready
	switch {
	case len(observed.DivergedInstances) > 0:
		observed.Phase = phaseDegraded
		observed.PhaseReason = fmt.Sprintf("replica(s) diverged from primary %s and cannot safely rejoin: %s",
			observed.PrimaryName, strings.Join(observed.DivergedInstances, ", "))
	case observed.Ready:
		observed.Phase = phaseReady
		observed.PhaseReason = "All instances are ready"
	case observed.ReadyInstances == 0:
		observed.Phase = phasePending
		observed.PhaseReason = "Waiting for the primary instance"
	default:
		observed.Phase = phaseProvisioning
		observed.PhaseReason = fmt.Sprintf("%d/%d instances ready", observed.ReadyInstances, plan.Instances)
	}
	return observed, nil
}

// detectDivergedReplicas returns reachable non-primary instances whose executed
// GTID set is not contained in the primary's. This catches a former primary
// that committed transactions the new primary never saw (errant transactions):
// rejoining it as-is would silently diverge the data, so the operator surfaces
// it instead of reconfiguring or re-cloning it.
func detectDivergedReplicas(observed observedCluster) []string {
	primaryGTID := observed.GTIDByInstance[observed.PrimaryName]
	if primaryGTID == "" {
		return nil
	}
	var diverged []string
	for _, name := range observed.InstanceNames {
		if name == observed.PrimaryName {
			continue
		}
		gtid := observed.GTIDByInstance[name]
		if gtid == "" {
			continue
		}
		contained, err := replication.GTIDContains(primaryGTID, gtid)
		if err != nil || contained {
			continue
		}
		diverged = append(diverged, name)
	}
	return diverged
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
	latest.Status.Certificates = r.certificateStatus(ctx, latest, observed.Plan)
	latest.Status.ContinuousArchiving = observed.ContinuousArchiving
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
	return r.Status().Patch(ctx, latest, client.MergeFrom(before))
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
	if observed.Phase == phaseBlocked {
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
