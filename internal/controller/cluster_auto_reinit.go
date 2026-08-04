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
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/instance"
)

// autoReinitRestartThreshold is the container restart count at which the
// operator re-clones a replica that keeps failing to start without saying why.
// It is well above crashLoopRestartThreshold (which merely flags the instance as
// failed) because re-cloning discards the instance's data: at this point the
// kubelet has retried often enough that a transient cause — a slow boot, a
// scheduling hiccup, a brief volume problem — would have cleared.
const autoReinitRestartThreshold = 7

// corruptionReinitRestartThreshold is the restart count at which the operator
// re-clones a replica whose instance manager positively diagnosed InnoDB
// corruption. It is far lower than autoReinitRestartThreshold because the
// diagnosis is evidence, not inference: more restarts cannot repair damaged
// InnoDB data, so waiting only extends the outage.
//
// It tracks crashLoopRestartThreshold because an instance is not considered for
// re-init until observeCluster has listed it as failed, which happens at that
// same count; a lower value here would simply never be reached.
const corruptionReinitRestartThreshold = crashLoopRestartThreshold

// reconcileAutoReinit scans observed failed instances and adds the reinit
// annotation for replicas whose mysqld cannot be brought back by restarting it.
// That re-clones them from a healthy primary, recovering from InnoDB corruption
// or other data damage the instance cannot repair in place.
//
// Re-initialising discards the instance's data volume, so the gates are strict:
//   - Only runs on established clusters (not during initial provisioning).
//   - Only replicas are re-initialised, never the primary: a primary has no
//     healthy source to clone from, so corruption there is surfaced for an
//     operator to act on (restore from backup, or salvage with
//     innodb_force_recovery by hand) rather than automated.
//   - Only Pods whose mysqld container is in CrashLoopBackOff qualify, at a
//     restart count that depends on whether corruption was diagnosed. A Pod that
//     merely failed — evicted, preempted, node shut down — never qualifies; its
//     data is intact and it only needs rescheduling. See shouldAutoReinit.
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

	// Collect replicas that need auto-reinit, tracking which were re-cloned on a
	// corruption diagnosis so the event says why.
	var toReinit, corrupt []string
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
		if podReportedCorruption(pod) {
			corrupt = append(corrupt, name)
		}
	}

	if len(toReinit) == 0 {
		return nil
	}

	reason := "mysqld could not be restarted"
	if len(corrupt) > 0 {
		reason = fmt.Sprintf("InnoDB reported corrupt data on %v", corrupt)
	}

	log.Info("Automatically re-initialising failed replicas",
		"instances", toReinit, "reason", reason)

	if r.Recorder != nil {
		r.Recorder.Event(cluster, corev1.EventTypeWarning, "AutoReinitializing",
			fmt.Sprintf("Automatically re-initialising %v from the primary: %s", toReinit, reason))
	}

	return r.addReinitRequests(ctx, cluster, toReinit)
}

// shouldAutoReinit reports whether a Pod has failed in a way that only replacing
// its data can fix, so the instance should be re-cloned from the primary.
//
// Re-cloning is destructive — it discards the instance's data volume — so it
// requires a Pod that is genuinely stuck, never merely gone. A Pod in the Failed
// phase does not qualify on its own: eviction under node pressure, preemption,
// and node shutdown all land there with the data volume intact, and re-cloning
// those would turn a routine reschedule into a full resync. Only a container
// that keeps crashing counts, and how many crashes are needed depends on whether
// the instance manager said why:
//
//   - It diagnosed InnoDB corruption: restarts cannot repair the data, so act at
//     corruptionReinitRestartThreshold.
//   - It failed without a diagnosis: wait for autoReinitRestartThreshold, by
//     which point a transient cause would have cleared.
func shouldAutoReinit(pod *corev1.Pod) bool {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name != instanceContainerName {
			continue
		}
		if w := cs.State.Waiting; w == nil || w.Reason != "CrashLoopBackOff" {
			continue
		}
		threshold := int32(autoReinitRestartThreshold)
		if containerReportedCorruption(cs) {
			threshold = corruptionReinitRestartThreshold
		}
		if cs.RestartCount >= threshold {
			return true
		}
	}
	return false
}

// podReportedCorruption reports whether the Pod's mysqld container published an
// InnoDB corruption diagnosis on its last exit.
func podReportedCorruption(pod *corev1.Pod) bool {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == instanceContainerName && containerReportedCorruption(cs) {
			return true
		}
	}
	return false
}

// containerReportedCorruption reports whether the instance manager's last exit
// published an InnoDB corruption diagnosis through the Pod's termination
// message. Both the last terminated state and the current one are checked: which
// of the two holds the message depends on where the Pod is in its restart cycle.
func containerReportedCorruption(cs corev1.ContainerStatus) bool {
	if t := cs.LastTerminationState.Terminated; t != nil &&
		strings.Contains(t.Message, instance.CorruptionSentinel) {
		return true
	}
	if t := cs.State.Terminated; t != nil &&
		strings.Contains(t.Message, instance.CorruptionSentinel) {
		return true
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
	cluster.Annotations[reinitAnnotation] = strings.Join(merged, ",")
	return r.Patch(ctx, cluster, client.MergeFrom(before))
}
