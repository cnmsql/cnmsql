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
	"io"
	"os"
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
)

// reconcileUpgrade drives a serialized, primary-last operator upgrade rollout
// when instances report a stale instance manager (executable hash mismatch).
// Only one instance is rolled per reconcile; the caller requeues. Fenced
// instances are skipped. The primary is upgraded last: with >1 instance and an
// unsupervised strategy it triggers a switchover first; with a supervised
// strategy it stops and waits for the user.
func (r *ClusterReconciler) reconcileUpgrade(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	plan clusterPlan,
	observed observedCluster,
) (bool, reconcile.Result, error) {
	if r.OperatorExecutableHash == "" {
		return false, reconcile.Result{}, nil
	}

	targetHash := cluster.Status.OperatorExecutableHash
	if targetHash == "" {
		targetHash = r.OperatorExecutableHash
	}

	candidates := upgradeCandidates(observed, targetHash)
	if len(candidates) == 0 {
		return false, reconcile.Result{}, nil
	}

	// Wait for previously upgraded instances (non-stale, non-candidate) to
	// become Ready before rolling the next one. This serializes the rollout:
	// only one replica is ever down at a time.
	candidateNames := make(map[string]bool, len(candidates))
	for _, c := range candidates {
		candidateNames[c.Name] = true
	}
	for _, name := range observed.InstanceNames {
		if candidateNames[name] {
			continue
		}
		status, ok := observed.StatusByInstance[name]
		if !ok || !status.IsReady {
			logf.FromContext(ctx).V(1).Info("Waiting for a previously upgraded instance to become Ready before continuing the rollout",
				"instance", name)
			return true, reconcile.Result{RequeueAfter: provisioningRequeue}, nil
		}
	}

	// In-place path: stream the operator's own manager binary to the next stale
	// instance, which re-execs without restarting mysqld. The primary is upgraded
	// the same way as a replica — no switchover, no Pod restart — so the
	// supervised-wait and switchover handling below is skipped entirely.
	if cluster.Spec.InPlaceInstanceManagerUpdates {
		return r.upgradeInstanceInPlace(ctx, cluster, plan, observed, candidates, candidates[0], targetHash)
	}

	// When the primary is stale and the strategy is supervised, stop the entire
	// upgrade and wait for the user, even if replicas are also stale. This only
	// applies to multi-instance clusters: a single-instance primary cannot be
	// switched over, so it is always upgraded in place (CNPG does the same).
	if _, ok := candidateByName(candidates, observed.PrimaryName); ok &&
		plan.Instances > 1 &&
		cluster.Spec.PrimaryUpdateStrategy == mysqlv1alpha1.PrimaryUpdateStrategySupervised {
		logf.FromContext(ctx).Info("Primary instance manager is stale, waiting for user",
			"instance", observed.PrimaryName, "strategy", "supervised")
		return true, reconcile.Result{RequeueAfter: readyResync}, r.patchStatus(ctx, cluster, upgradeProgressStatus(
			topology.PhaseWaitingForUser,
			"Primary instance manager is stale (operator upgrade); waiting for user to trigger the update",
			plan, observed))
	}

	instance := candidates[0]

	// Primary upgrade via switchover: promote a healthy replica first. With a
	// single instance, or no healthy replica to switch to, fall through to the
	// in-place restart below. This is the async model where the operator chooses
	// the primary; under Group Replication the operator never promotes (the group
	// elects), and the GR ReconcileSwitchover is a no-op — so setting TargetPrimary
	// here would move nothing and the rollout would re-enter this branch forever.
	// A GR primary is rolled directly instead: deleting its Pod makes the group
	// elect a new primary, and the recreated Pod rejoins as a secondary.
	if !cluster.IsGroupReplication() &&
		instance.Name == observed.PrimaryName && plan.Instances > 1 &&
		cluster.Spec.PrimaryUpdateMethod != mysqlv1alpha1.PrimaryUpdateMethodRestart {
		if handled, result, err := r.upgradePrimaryViaSwitchover(ctx, cluster, plan, observed); handled || err != nil {
			return handled, result, err
		}
	}

	return r.rollInstanceForUpgrade(ctx, cluster, plan, observed, candidates, instance, targetHash)
}

// rollInstanceForUpgrade deletes the instance Pod so the next reconcile recreates
// it from the new spec with the updated operator image in the bootstrap-controller
// init container. This is the rolling-update (non-in-place) upgrade path.
func (r *ClusterReconciler) rollInstanceForUpgrade(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	plan clusterPlan,
	observed observedCluster,
	candidates []upgradeCandidate,
	instance upgradeCandidate,
	targetHash string,
) (bool, reconcile.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Upgrading instance manager",
		"instance", instance.Name, "reportedHash", instance.ReportedHash, "targetHash", targetHash)
	if err := r.Delete(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      instance.Name,
			Namespace: cluster.Namespace,
		},
	}); err != nil {
		log.Error(err, "Could not delete instance Pod for upgrade", "instance", instance.Name)
		return false, reconcile.Result{}, err
	}

	reason := upgradeReason("Upgrading instance manager on", instance.Name, len(candidates))
	return true, reconcile.Result{RequeueAfter: provisioningRequeue}, r.patchStatus(ctx, cluster,
		upgradeProgressStatus(topology.PhaseUpgrading, reason, plan, observed))
}

// upgradeInstanceInPlace streams the operator's own manager binary to the
// instance's control API, which validates it against the target hash and
// re-execs in place — no Pod restart and, for the primary, no switchover. One
// instance is upgraded per reconcile; the caller requeues and the next reconcile
// observes the now-current hash and moves on to the next candidate.
func (r *ClusterReconciler) upgradeInstanceInPlace(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	plan clusterPlan,
	observed observedCluster,
	candidates []upgradeCandidate,
	instance upgradeCandidate,
	targetHash string,
) (bool, reconcile.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Upgrading instance manager in place",
		"instance", instance.Name, "reportedHash", instance.ReportedHash, "targetHash", targetHash)

	binary, err := r.operatorBinary()
	if err != nil {
		log.Error(err, "Could not open operator binary for in-place upgrade", "instance", instance.Name)
		return false, reconcile.Result{}, err
	}
	defer func() {
		_ = binary.Close()
	}()

	if err := r.instanceControlClient().UpgradeInstanceManager(ctx, cluster, instance.Name, binary, targetHash); err != nil {
		log.Error(err, "In-place instance-manager upgrade failed, will retry", "instance", instance.Name)
		return false, reconcile.Result{}, err
	}

	reason := upgradeReason("Upgrading instance manager in place on", instance.Name, len(candidates))
	return true, reconcile.Result{RequeueAfter: provisioningRequeue}, r.patchStatus(ctx, cluster,
		upgradeProgressStatus(topology.PhaseUpgrading, reason, plan, observed))
}

// operatorBinary opens the operator's own manager binary for streaming to an
// instance during an in-place upgrade. The operator and instance manager are the
// same binary (the bootstrap-controller init container copies /manager from the
// operator image), so the operator's own executable is the upgrade target.
func (r *ClusterReconciler) operatorBinary() (io.ReadCloser, error) {
	if r.openOperatorBinary != nil {
		return r.openOperatorBinary()
	}
	exe, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("locating operator binary: %w", err)
	}
	f, err := os.Open(exe) //nolint:gosec // our own executable
	if err != nil {
		return nil, fmt.Errorf("opening operator binary %s: %w", exe, err)
	}
	return f, nil
}

// upgradePrimaryViaSwitchover triggers a switchover from the stale primary to a
// healthy replica so the primary can be upgraded without downtime. It returns
// handled=false (with a nil error) when there is no healthy replica to switch
// to, so the caller falls back to an in-place primary restart.
func (r *ClusterReconciler) upgradePrimaryViaSwitchover(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	plan clusterPlan,
	observed observedCluster,
) (bool, reconcile.Result, error) {
	target := healthyReplicaForSwitchover(observed)
	if target == "" {
		logf.FromContext(ctx).Info("No healthy replica to switch to, falling back to in-place primary restart",
			"primary", observed.PrimaryName)
		return false, reconcile.Result{}, nil
	}

	logf.FromContext(ctx).Info("Upgrading primary via switchover",
		"oldPrimary", observed.PrimaryName, "newPrimary", target)

	if err := r.updateStatus(ctx, cluster, func(s *mysqlv1alpha1.ClusterStatus) {
		s.TargetPrimary = target
		s.TargetPrimaryTimestamp = ""
	}); err != nil {
		return false, reconcile.Result{}, err
	}

	return true, reconcile.Result{RequeueAfter: provisioningRequeue}, r.patchStatus(ctx, cluster, upgradeProgressStatus(
		topology.PhaseUpgrading,
		fmt.Sprintf("Upgrading: switching over to %s before upgrading the primary", target),
		plan, observed))
}

// upgradeProgressStatus builds the in-progress status patch shared by every
// upgrade step (roll, in-place stream, switchover, waiting-for-user): the
// instance facts are carried through unchanged and only the phase and reason
// differ, so the call sites cannot drift apart.
func upgradeProgressStatus(phase, reason string, plan clusterPlan, observed observedCluster) observedCluster {
	return observedCluster{
		Phase:          phase,
		PhaseReason:    reason,
		Ready:          false,
		Progressing:    true,
		Plan:           plan,
		PrimaryName:    observed.PrimaryName,
		InstanceNames:  observed.InstanceNames,
		ReadyInstances: observed.ReadyInstances,
		GTIDByInstance: observed.GTIDByInstance,
	}
}

// upgradeReason describes which instance is being upgraded and, when more than
// one is still stale, how many remain. action is the leading verb phrase, e.g.
// "Upgrading instance manager on" or "Upgrading instance manager in place on".
func upgradeReason(action, instance string, candidates int) string {
	if candidates > 1 {
		return fmt.Sprintf("%s %s (%d/%d remaining)", action, instance, candidates-1, candidates)
	}
	return fmt.Sprintf("%s %s", action, instance)
}

// upgradeCandidate holds enough to determine upgrade order.
type upgradeCandidate struct {
	Name         string
	ReportedHash string
}

// upgradeCandidates returns instances whose manager is stale, ordered replicas
// first then primary last. Fenced instances are excluded.
func upgradeCandidates(observed observedCluster, targetHash string) []upgradeCandidate {
	fenced := setFrom(observed.FencedInstances)
	var candidates []upgradeCandidate
	for _, name := range observed.InstanceNames {
		if fenced[name] {
			continue
		}
		reportedHash := observed.ExecutableHashByInstance[name]
		if reportedHash == "" || reportedHash == targetHash {
			continue
		}
		candidates = append(candidates, upgradeCandidate{
			Name:         name,
			ReportedHash: reportedHash,
		})
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		iPrimary := candidates[i].Name == observed.PrimaryName
		jPrimary := candidates[j].Name == observed.PrimaryName
		if iPrimary != jPrimary {
			return !iPrimary
		}
		return candidates[i].Name < candidates[j].Name
	})
	return candidates
}

func candidateByName(candidates []upgradeCandidate, name string) (upgradeCandidate, bool) {
	for _, c := range candidates {
		if c.Name == name {
			return c, true
		}
	}
	return upgradeCandidate{}, false
}

// healthyReplicaForSwitchover picks a ready, unfenced, reachable replica that is
// not the current primary and is not diverged or replication-broken.
func healthyReplicaForSwitchover(observed observedCluster) string {
	fenced := setFrom(observed.FencedInstances)
	diverged := setFrom(observed.DivergedInstances)
	broken := setFrom(observed.ReplicationBrokenInstances)
	for _, name := range observed.InstanceNames {
		if name == observed.PrimaryName || fenced[name] || diverged[name] || broken[name] {
			continue
		}
		if status, ok := observed.StatusByInstance[name]; ok && status.IsReady {
			return name
		}
	}
	return ""
}

func setFrom(list []string) map[string]bool {
	out := make(map[string]bool, len(list))
	for _, s := range list {
		out[s] = true
	}
	return out
}
