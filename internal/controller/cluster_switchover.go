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
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/replication"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/webserver"
)

func (r *ClusterReconciler) reconcilePrimaryChange(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	plan clusterPlan,
	observed observedCluster,
) (bool, error) {
	target := cluster.Status.TargetPrimary
	if target == "" || target == observed.PrimaryName {
		return false, nil
	}
	if observed.PrimaryName == "" {
		return false, r.patchStatus(ctx, cluster, observedCluster{
			Phase:       phaseBlocked,
			PhaseReason: "Cannot switch primary before the current primary is known",
			Ready:       false,
			Progressing: false,
			Plan:        plan,
		})
	}

	controlClient := r.ControlClient
	if controlClient == nil {
		controlClient = &HTTPControlClient{Client: r.Client}
	}

	if err := validateSwitchoverTarget(observed, target); err != nil {
		return false, r.patchStatus(ctx, cluster, observedCluster{
			Phase:          phaseBlocked,
			PhaseReason:    err.Error(),
			Ready:          false,
			Progressing:    false,
			Plan:           plan,
			PrimaryName:    observed.PrimaryName,
			InstanceNames:  observed.InstanceNames,
			ReadyInstances: observed.ReadyInstances,
			GTIDByInstance: observed.GTIDByInstance,
		})
	}
	if err := validateTargetGTID(observed, observed.PrimaryName, target); err != nil {
		// Bound the catch-up wait by spec.maxSwitchoverDelay (RTO): if the target
		// cannot catch up in time, abort rather than demote the primary forever.
		startedAt, stampErr := r.ensureSwitchoverStarted(ctx, cluster)
		if stampErr != nil {
			return false, stampErr
		}
		maxDelay := time.Duration(cluster.Spec.MaxSwitchoverDelay) * time.Second
		if maxDelay > 0 && time.Since(startedAt) > maxDelay {
			return r.abortSwitchover(ctx, cluster, observed, target)
		}
		return false, r.patchStatus(ctx, cluster, observedCluster{
			Phase:          phaseSwitchover,
			PhaseReason:    err.Error(),
			Ready:          false,
			Progressing:    true,
			Plan:           plan,
			PrimaryName:    observed.PrimaryName,
			InstanceNames:  observed.InstanceNames,
			ReadyInstances: observed.ReadyInstances,
			GTIDByInstance: observed.GTIDByInstance,
		})
	}

	if err := controlClient.Demote(ctx, cluster, observed.PrimaryName); err != nil {
		return false, fmt.Errorf("demote %s: %w", observed.PrimaryName, err)
	}
	if err := controlClient.Promote(ctx, cluster, target); err != nil {
		return false, fmt.Errorf("promote %s: %w", target, err)
	}
	source := sourceOptions(cluster, target)
	for _, name := range observed.InstanceNames {
		if name == target {
			continue
		}
		if _, ok := observed.StatusByInstance[name]; !ok {
			continue
		}
		if err := controlClient.ConfigureReplica(ctx, cluster, name, source); err != nil {
			return false, fmt.Errorf("configure %s as replica of %s: %w", name, target, err)
		}
	}
	if err := r.patchRoleLabels(ctx, cluster, observed.InstanceNames, target); err != nil {
		return false, err
	}
	if err := r.patchPrimaryStatus(ctx, cluster, target); err != nil {
		return false, err
	}
	if r.Recorder != nil {
		r.Recorder.Event(cluster, corev1.EventTypeNormal, phaseSwitchover, fmt.Sprintf("Switched over to %s", target))
	}
	return true, nil
}

// ensureSwitchoverStarted stamps status.targetPrimaryTimestamp on the first
// reconcile of a switchover request and returns the effective start time, so the
// catch-up wait can be bounded by spec.maxSwitchoverDelay.
func (r *ClusterReconciler) ensureSwitchoverStarted(ctx context.Context, cluster *mysqlv1alpha1.Cluster) (time.Time, error) {
	if ts := cluster.Status.TargetPrimaryTimestamp; ts != "" {
		if parsed, err := time.Parse(time.RFC3339, ts); err == nil {
			return parsed, nil
		}
	}
	now := time.Now().Truncate(time.Second)
	if err := r.updateStatus(ctx, cluster, func(s *mysqlv1alpha1.ClusterStatus) {
		s.TargetPrimaryTimestamp = now.Format(time.RFC3339)
	}); err != nil {
		return time.Time{}, err
	}
	return now, nil
}

// abortSwitchover cancels a planned switchover whose target failed to catch up
// within spec.maxSwitchoverDelay, leaving the original primary in place.
func (r *ClusterReconciler) abortSwitchover(ctx context.Context, cluster *mysqlv1alpha1.Cluster, observed observedCluster, target string) (bool, error) {
	reason := fmt.Sprintf("switchover to %s aborted: target did not catch up within maxSwitchoverDelay (%ds)",
		target, cluster.Spec.MaxSwitchoverDelay)
	if err := r.updateStatus(ctx, cluster, func(s *mysqlv1alpha1.ClusterStatus) {
		s.TargetPrimary = observed.PrimaryName
		s.TargetPrimaryTimestamp = ""
		s.Phase = phaseBlocked
		s.PhaseReason = reason
	}); err != nil {
		return false, err
	}
	if r.Recorder != nil {
		r.Recorder.Event(cluster, corev1.EventTypeWarning, phaseBlocked, reason)
	}
	return true, nil
}

func validateSwitchoverTarget(observed observedCluster, target string) error {
	status, ok := observed.StatusByInstance[target]
	if !ok {
		return fmt.Errorf("target primary %s is not reporting status", target)
	}
	if !status.IsReady {
		return fmt.Errorf("target primary %s is not ready", target)
	}
	if status.Role != webserver.RoleReplica {
		return fmt.Errorf("target primary %s has role %s, want replica", target, status.Role)
	}
	if status.Replication == nil || !status.Replication.IORunning || !status.Replication.SQLRunning {
		return fmt.Errorf("target primary %s replication is not healthy", target)
	}
	return nil
}

// validateTargetGTID ensures the promotion target has applied everything the
// old primary had. The target is safe once its executed GTID set contains the
// primary's; a strict equality check would needlessly wait while harmless
// interval coalescing differs.
func validateTargetGTID(observed observedCluster, currentPrimary, target string) error {
	primaryGTID := observed.GTIDByInstance[currentPrimary]
	targetGTID := observed.GTIDByInstance[target]
	if primaryGTID == "" || targetGTID == "" {
		return nil
	}
	contains, err := replication.GTIDContains(targetGTID, primaryGTID)
	if err != nil {
		return fmt.Errorf("comparing gtid sets of %s and %s: %w", target, currentPrimary, err)
	}
	if contains {
		return nil
	}
	return fmt.Errorf("waiting for target primary %s to catch up to %s", target, currentPrimary)
}

func sourceOptions(cluster *mysqlv1alpha1.Cluster, primaryName string) replication.SourceOptions {
	return replication.SourceOptions{
		Host:         primaryName + "." + cluster.Namespace + ".svc",
		Port:         3306,
		User:         replicationUser,
		AutoPosition: true,
		SSL:          true,
		SSLCA:        clientCAPath + "/ca.crt",
		SSLCert:      serverTLSPath + "/tls.crt",
		SSLKey:       serverTLSPath + "/tls.key",
	}
}

func (r *ClusterReconciler) reconcileReplicaSources(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	_ clusterPlan,
	observed observedCluster,
) (bool, error) {
	if observed.PrimaryName == "" {
		return false, nil
	}
	controlClient := r.ControlClient
	if controlClient == nil {
		controlClient = &HTTPControlClient{Client: r.Client}
	}
	diverged := map[string]bool{}
	for _, name := range observed.DivergedInstances {
		diverged[name] = true
	}
	source := sourceOptions(cluster, observed.PrimaryName)
	repaired := false
	for _, name := range observed.InstanceNames {
		if name == observed.PrimaryName {
			continue
		}
		status, ok := observed.StatusByInstance[name]
		if !ok || status.Role == webserver.RolePrimary {
			continue
		}
		// A diverged replica cannot safely follow the primary; surfaced by the
		// status, it must not be silently reconfigured.
		if diverged[name] {
			continue
		}
		if status.Replication != nil && sameSourceHost(status.Replication.SourceHost, source.Host) {
			continue
		}
		if err := controlClient.ConfigureReplica(ctx, cluster, name, source); err != nil {
			return false, fmt.Errorf("configure %s as replica of %s: %w", name, observed.PrimaryName, err)
		}
		repaired = true
	}
	return repaired, nil
}

func sameSourceHost(current, desired string) bool {
	return strings.EqualFold(strings.TrimSuffix(current, "."), strings.TrimSuffix(desired, "."))
}

func (r *ClusterReconciler) patchRoleLabels(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceNames []string, primaryName string) error {
	for _, name := range instanceNames {
		pod := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, pod); err != nil {
			return client.IgnoreNotFound(err)
		}
		if pod.DeletionTimestamp != nil {
			continue
		}
		before := pod.DeepCopy()
		if pod.Labels == nil {
			pod.Labels = map[string]string{}
		}
		pod.Labels[roleLabel] = roleReplica
		if name == primaryName {
			pod.Labels[roleLabel] = rolePrimary
		}
		if err := r.Patch(ctx, pod, client.MergeFrom(before)); err != nil {
			return err
		}
	}
	return nil
}

func (r *ClusterReconciler) patchPrimaryStatus(ctx context.Context, cluster *mysqlv1alpha1.Cluster, primaryName string) error {
	latest := &mysqlv1alpha1.Cluster{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
	if err := r.Get(ctx, key, latest); err != nil {
		return err
	}
	before := latest.DeepCopy()
	now := metav1.Now().Format(time.RFC3339)
	latest.Status.CurrentPrimary = primaryName
	latest.Status.TargetPrimary = primaryName
	latest.Status.CurrentPrimaryTimestamp = now
	latest.Status.TargetPrimaryTimestamp = ""
	latest.Status.Phase = phaseSwitchover
	latest.Status.PhaseReason = fmt.Sprintf("Switched over to %s", primaryName)
	return r.Status().Patch(ctx, latest, client.MergeFrom(before))
}
