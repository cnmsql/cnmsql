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
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// autoReinitRestartThreshold is the container restart count at which the
// operator automatically re-clones a failed replica. It is higher than
// crashLoopRestartThreshold (which merely flags the instance as failed) to give
// the in-pod force-recovery escalation (1→2→3 across Pod restarts) room to
// succeed before the operator gives up and re-clones. Each force-recovery level
// consumes one Pod restart, so 3 levels + the initial crash = 4 restarts; we
// add a margin so transient crashes are not mistaken for irrecoverable
// corruption.
const autoReinitRestartThreshold = 7

// reconcileAutoReinit scans observed failed instances and automatically adds
// the reinit annotation for replicas that have exhausted crash recovery. This
// re-clones them from a healthy primary, recovering from InnoDB corruption or
// replication metadata damage that force recovery could not fix.
//
// Safety gates:
//   - Only runs on established clusters (not during initial provisioning).
//   - Only replicas are re-initialised (never the primary).
//   - Only instances whose Pod is stuck in CrashLoopBackOff with a restart
//     count at or above autoReinitRestartThreshold are selected.
//   - A healthy primary must exist to re-clone from.
//   - Existing manual reinit requests are preserved (the auto-reinit only
//     adds names not already listed).
func (r *ClusterReconciler) reconcileAutoReinit(ctx context.Context, cluster *mysqlv1alpha1.Cluster, observed observedCluster) error {
	if cluster.ReplicationMode() == mysqlv1alpha1.ReplicationModeGroupReplication {
		return nil
	}
	if !cluster.IsEstablished() {
		return nil
	}
	if len(observed.FailedInstances) == 0 {
		return nil
	}
	if observed.PrimaryName == "" {
		return nil
	}

	log := logf.FromContext(ctx).WithName("auto-reinit")

	// A healthy primary is required to re-clone from.
	primaryStatus, ok := observed.StatusByInstance[observed.PrimaryName]
	if !ok || !primaryStatus.IsReady {
		return nil
	}

	// Collect replicas that need auto-reinit.
	var toReinit []string
	for _, name := range observed.FailedInstances {
		if name == observed.PrimaryName {
			continue
		}
		if reinitRequested(cluster, name) {
			continue
		}
		pod := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, pod); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		if !shouldAutoReinit(pod) {
			continue
		}
		toReinit = append(toReinit, name)
	}

	if len(toReinit) == 0 {
		return nil
	}

	log.Info("Automatically re-initialising failed replicas after crash recovery exhausted",
		"instances", toReinit)

	if r.Recorder != nil {
		r.Recorder.Event(cluster, corev1.EventTypeWarning, "AutoReinitializing",
			fmt.Sprintf("Automatically re-initialising %v: crash recovery exhausted, re-cloning from primary", toReinit))
	}

	return r.addReinitRequests(ctx, cluster, toReinit)
}

// shouldAutoReinit reports whether a Pod is stuck in CrashLoopBackOff with
// enough restarts that the in-pod force recovery has been exhausted and the
// instance should be re-cloned.
func shouldAutoReinit(pod *corev1.Pod) bool {
	if pod.Status.Phase == corev1.PodFailed {
		return true
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if w := cs.State.Waiting; w != nil && w.Reason == "CrashLoopBackOff" &&
			cs.RestartCount >= autoReinitRestartThreshold {
			return true
		}
	}
	return false
}

// addReinitRequests adds the given instance names to the Cluster's reinit
// annotation, preserving any names already listed. It is idempotent: names
// already present are not duplicated.
func (r *ClusterReconciler) addReinitRequests(ctx context.Context, cluster *mysqlv1alpha1.Cluster, names []string) error {
	existing := reinitRequestedInstances(cluster)
	merged := append([]string{}, existing...)
	for _, name := range names {
		if !slices.Contains(merged, name) {
			merged = append(merged, name)
		}
	}
	if len(merged) == len(existing) {
		return nil
	}
	before := cluster.DeepCopy()
	if cluster.Annotations == nil {
		cluster.Annotations = map[string]string{}
	}
	cluster.Annotations[reinitAnnotation] = joinComma(merged)
	return r.Patch(ctx, cluster, client.MergeFrom(before))
}

func joinComma(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	out := parts[0]
	for _, p := range parts[1:] {
		out += "," + p
	}
	return out
}
