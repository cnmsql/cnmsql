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
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// archiveTimelineReference is the name the archive's oldest segment position
// goes by among the references that pin the MariaDB timeline.
const archiveTimelineReference = "archive"

// timelineUpdate is the MariaDB primary timeline one observation leads to.
type timelineUpdate struct {
	// Observed is true when the timeline was computed from a full observation,
	// so the status patch may write it.
	Observed bool
	// Timeline is the new status.mariadbTimeline (nil on MySQL and replica
	// clusters).
	Timeline []mysqlv1alpha1.MariaDBEpoch
	// Truncated is true when the ceiling dropped history PinnedBy still
	// referenced.
	Truncated bool
	PinnedBy  []string
}

// engineTimeline returns the verdict view of the timeline.
func (u timelineUpdate) engineTimeline() engine.MariaDBTimeline {
	return toEngineTimeline(u.Timeline)
}

func toEngineTimeline(epochs []mysqlv1alpha1.MariaDBEpoch) engine.MariaDBTimeline {
	if len(epochs) == 0 {
		return nil
	}
	out := make(engine.MariaDBTimeline, len(epochs))
	for i, e := range epochs {
		out[i] = engine.MariaDBEpoch{ServerID: e.ServerID, Handoff: e.Handoff}
	}
	return out
}

// nextMariaDBTimeline records a change of primary and prunes what nothing
// needs any more. It runs in the observation, before divergence is computed,
// so the epoch of a just-promoted primary is there when its predecessor is
// judged.
//
// An epoch is appended when a writable primary is observed whose name differs
// from the last epoch's: a failover, switchover, failback, or the first primary
// of a cluster with no timeline yet. Its handoff is the primary's
// gtid_slave_pos, which stays at what it inherited because a primary's own
// writes do not advance it, so reading it late is harmless.
//
// The first epoch is the exception. Its primary did not necessarily author the
// history it holds: a cluster bootstrapped from a backup holds the source
// cluster's transactions under other server ids, and a cluster upgraded
// mid-life holds history no epoch observed. So the first epoch starts at the
// primary's current position, and everything before it gets no verdict.
//
// Replica clusters do not record a timeline: their designated primary
// replicates from the source cluster, so authorship is not theirs to track.
func nextMariaDBTimeline(cluster *mysqlv1alpha1.Cluster, observed observedCluster, now time.Time) timelineUpdate {
	update := timelineUpdate{Observed: true}
	if cluster.ResolvedFlavor() != mysqlv1alpha1.FlavorMariaDB || cluster.IsReplica() {
		return update
	}
	timeline := slices.Clone(cluster.Status.MariaDBTimeline)
	if len(timeline) == 0 {
		timeline = archivedTimeline(observed, now)
	}
	if status, ok := observed.StatusByInstance[observed.PrimaryName]; ok && writablePrimary(status) &&
		(len(timeline) == 0 || timeline[len(timeline)-1].Instance != observed.PrimaryName) {
		handoff := status.GTIDSlavePos
		if len(timeline) == 0 {
			handoff = status.GTIDExecuted
		}
		timeline = append(timeline, mysqlv1alpha1.MariaDBEpoch{
			Instance: observed.PrimaryName,
			ServerID: status.ServerID,
			Handoff:  canonicalMariaDBPosition(handoff),
			Since:    metav1.NewTime(now),
		})
	}
	if len(timeline) == 0 {
		return update
	}

	pruned := engine.PruneMariaDBTimeline(toEngineTimeline(timeline), timelineReferences(cluster, observed),
		engine.MariaDBTimelineCeiling)
	update.Timeline = timeline[pruned.Dropped:]
	update.Truncated = pruned.Truncated
	update.PinnedBy = pruned.PinnedBy
	return update
}

// archivedTimeline is the timeline the archive carries, as the primary
// reports it: the history a Cluster whose status was lost (recreated from a
// manifest, restored without status) starts from instead of from scratch.
func archivedTimeline(observed observedCluster, now time.Time) []mysqlv1alpha1.MariaDBEpoch {
	status, ok := observed.StatusByInstance[observed.PrimaryName]
	if !ok || status.Archiving == nil || status.Archiving.ForkCheckedAt == "" {
		return nil
	}
	var out []mysqlv1alpha1.MariaDBEpoch
	for _, e := range status.Archiving.MariaDBTimeline {
		out = append(out, mysqlv1alpha1.MariaDBEpoch{
			Instance: e.Instance, ServerID: e.ServerID, Handoff: e.Handoff, Since: metav1.NewTime(now),
		})
	}
	return out
}

// writablePrimary reports whether an instance status shows a primary the
// timeline can record: writable, and new enough to report its server id.
func writablePrimary(status *webserver.Status) bool {
	return status.Role == webserver.RolePrimary && !status.ReadOnly && !status.SuperReadOnly && status.ServerID != 0
}

func canonicalMariaDBPosition(raw string) string {
	eng, err := engine.ForFlavor(engine.FlavorMariaDB)
	if err != nil {
		return raw
	}
	canonical, err := eng.GTID().Canonical(raw)
	if err != nil {
		return raw
	}
	return canonical
}

// timelineReferences are the positions the operator still has to judge against
// the timeline: every instance's position (this pass's, or the last recorded
// one for an unreachable instance), diverged instances included since their
// mark clears only on positive proof, and the archive's oldest segment while
// archiving is on.
func timelineReferences(cluster *mysqlv1alpha1.Cluster, observed observedCluster) map[string]string {
	refs := map[string]string{}
	for _, name := range observed.InstanceNames {
		if position, ok := observed.GTIDByInstance[name]; ok && position != "" {
			refs[name] = position
			continue
		}
		if position, ok := cluster.Status.GTIDExecutedByInstance[name]; ok && position != "" {
			refs[name] = position
		}
	}
	if cluster.IsArchivingEnabled() {
		if oldest := oldestArchivedPosition(cluster, observed); oldest != "" {
			refs[archiveTimelineReference] = oldest
		}
	}
	return refs
}

// oldestArchivedPosition is the lowest position the archive still holds: the
// primary's report once it has fork-checked the index, otherwise the last one
// recorded in status.
func oldestArchivedPosition(cluster *mysqlv1alpha1.Cluster, observed observedCluster) string {
	if status, ok := observed.StatusByInstance[observed.PrimaryName]; ok && status.Archiving != nil &&
		status.Archiving.ForkCheckedAt != "" {
		return status.Archiving.OldestSegmentPosition
	}
	if ca := cluster.Status.ContinuousArchiving; ca != nil {
		return ca.OldestSegmentPosition
	}
	return ""
}

// recordTimelineTruncatedEvent warns when the ceiling forced out timeline
// history something still referenced: that history now gets no verdict.
func (r *ClusterReconciler) recordTimelineTruncatedEvent(cluster *mysqlv1alpha1.Cluster, update timelineUpdate) {
	if r.Recorder == nil || !update.Truncated {
		return
	}
	r.Recorder.Eventf(cluster, corev1.EventTypeWarning, mysqlv1alpha1.EventMariaDBTimelineTruncated,
		"The MariaDB primary timeline reached %d entries and dropped history still referenced by %s; "+
			"divergence and fork checks give no verdict on it",
		engine.MariaDBTimelineCeiling, strings.Join(update.PinnedBy, ", "))
}
