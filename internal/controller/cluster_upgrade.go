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
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
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

	// When the primary is stale and the strategy is supervised, stop the entire
	// upgrade and wait for the user, even if replicas are also stale.
	if primaryStale, ok := candidateByName(candidates, observed.PrimaryName); ok {
		if cluster.Spec.PrimaryUpdateStrategy == mysqlv1alpha1.PrimaryUpdateStrategySupervised {
			logf.FromContext(ctx).Info("Primary instance manager is stale, waiting for user",
				"instance", observed.PrimaryName, "strategy", "supervised")
			return true, reconcile.Result{RequeueAfter: readyResync}, r.patchStatus(ctx, cluster, observedCluster{
				Phase:          phaseWaitingForUser,
				PhaseReason:    "Primary instance manager is stale (operator upgrade); waiting for user to trigger the update",
				Ready:          false,
				Progressing:    true,
				Plan:           plan,
				PrimaryName:    observed.PrimaryName,
				InstanceNames:  observed.InstanceNames,
				ReadyInstances: observed.ReadyInstances,
				GTIDByInstance: observed.GTIDByInstance,
			})
		}
		_ = primaryStale
	}

	instance := candidates[0]
	log := logf.FromContext(ctx)

	// Primary upgrade via switchover: promote a healthy replica first.
	if instance.Name == observed.PrimaryName && plan.Instances > 1 &&
		cluster.Spec.PrimaryUpdateMethod != mysqlv1alpha1.PrimaryUpdateMethodRestart {
		return r.upgradePrimaryViaSwitchover(ctx, cluster, plan, observed)
	}

	// Delete the Pod. The next reconcile recreates it from the new spec with the
	// updated operator image in the bootstrap-controller init container.
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

	reason := fmt.Sprintf("Upgrading instance manager on %s", instance.Name)
	if len(candidates) > 1 {
		reason = fmt.Sprintf("Upgrading instance manager on %s (%d/%d remaining)",
			instance.Name, len(candidates)-1, len(candidates))
	}
	return true, reconcile.Result{RequeueAfter: provisioningRequeue}, r.patchStatus(ctx, cluster, observedCluster{
		Phase:          phaseUpgrading,
		PhaseReason:    reason,
		Ready:          false,
		Progressing:    true,
		Plan:           plan,
		PrimaryName:    observed.PrimaryName,
		InstanceNames:  observed.InstanceNames,
		ReadyInstances: observed.ReadyInstances,
		GTIDByInstance: observed.GTIDByInstance,
	})
}

// upgradePrimaryViaSwitchover triggers a switchover from the stale primary to a
// healthy replica so the primary can be upgraded without downtime.
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

	return true, reconcile.Result{RequeueAfter: provisioningRequeue}, r.patchStatus(ctx, cluster, observedCluster{
		Phase:          phaseUpgrading,
		PhaseReason:    fmt.Sprintf("Upgrading: switching over to %s before upgrading the primary", target),
		Ready:          false,
		Progressing:    true,
		Plan:           plan,
		PrimaryName:    observed.PrimaryName,
		InstanceNames:  observed.InstanceNames,
		ReadyInstances: observed.ReadyInstances,
		GTIDByInstance: observed.GTIDByInstance,
	})
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
