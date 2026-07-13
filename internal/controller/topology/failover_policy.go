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

package topology

import (
	"slices"
	"time"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// FailoverCooldownRemaining reports how much longer
// spec.failoverPolicy.minTimeBetweenFailovers forbids an automatic promotion, or
// zero when one may proceed now.
//
// The cooldown runs from the moment the current primary settled rather than from
// the moment it was promoted: see primarySettledAt. A cluster that has never
// failed over, and whose primary has been healthy for longer than the stability
// window, settled in the past and is not held back at all, which is every cluster
// that is not currently flapping.
func FailoverCooldownRemaining(cluster *mysqlv1alpha1.Cluster) time.Duration {
	cooldown := cluster.MinTimeBetweenFailovers()
	if cooldown <= 0 {
		return 0
	}
	settled := primarySettledAt(cluster)
	if settled.IsZero() {
		return 0
	}
	return max(time.Until(settled.Add(cooldown)), 0)
}

// primarySettledAt is the point the current primary stopped being a fresh
// promotion and became the settled state of the cluster. It is the later of two
// things, and zero when neither is known, which is a cluster that has never
// failed over and whose primary has never been seen healthy.
//
// The first is the last automatic failover. Without a stability window that is
// the whole answer: a promotion settles the instant it happens.
//
// The second is the primary having stayed healthy for primaryStabilityWindow. A
// primary that keeps dropping out has its healthy stretch restarted every time it
// does, so it never reaches the window and never settles, and the cooldown that
// runs from the settling point never begins. That is the anti-flapping guarantee:
// the operator will not walk the primary role around the cluster on behalf of an
// instance that cannot stay up. A primary that never came up healthy at all has no
// healthy stretch to measure, so only the promotion counts and the operator is
// left free to replace it once the plain cooldown expires.
func primarySettledAt(cluster *mysqlv1alpha1.Cluster) time.Time {
	var settled time.Time
	if promoted := cluster.Status.LastFailoverTimestamp; promoted != nil {
		settled = promoted.Time
	}
	window := cluster.PrimaryStabilityWindow()
	if window <= 0 {
		return settled
	}
	if healthy := primaryHealthySince(cluster); !healthy.IsZero() {
		if stable := healthy.Add(window); stable.After(settled) {
			settled = stable
		}
	}
	return settled
}

// primaryHealthySince is the start of the current primary's unbroken healthy
// stretch, or zero when it has never been observed healthy.
//
// It is never earlier than the primary's election. The stamp belongs to whichever
// instance was primary when it was written, and a switchover replaces the primary
// without the operator clearing it, so a stamp older than the election describes
// somebody else's health. The election is the earliest moment this primary's own
// stretch can have started.
func primaryHealthySince(cluster *mysqlv1alpha1.Cluster) time.Time {
	stamp := cluster.Status.PrimaryHealthySince
	if stamp == nil {
		return time.Time{}
	}
	since := stamp.Time
	if elected := cluster.Status.CurrentPrimaryTimestamp; elected != nil && elected.After(since) {
		return elected.Time
	}
	return since
}

// PreferredFailbackTarget returns the instance the primary should be moved back
// to, or empty when it should stay where it is.
//
// It walks spec.failoverPolicy.preferredPrimary in order and returns the first
// instance that promotable accepts. Reaching the current primary first ends the
// walk: nothing left to consider outranks it, so there is nothing to move to. An
// unlisted primary is outranked by any listed instance that is fit to take over,
// which is how the primary comes home after a failover put it somewhere the
// cluster did not ask for.
//
// A preference the topology cannot honour right now is simply not acted on. The
// walk skips a preferred instance that promotable rejects — it may be down, still
// catching up, fenced, or not exist at all in a cluster smaller than the list —
// and settles for the best one that can actually take the role.
func PreferredFailbackTarget(
	cluster *mysqlv1alpha1.Cluster,
	observed FailoverState,
	promotable func(instanceName string) bool,
) string {
	preferred := cluster.PreferredPrimary()
	current := cluster.Status.CurrentPrimary
	if len(preferred) == 0 || current == "" || observed.PrimaryName == "" {
		return ""
	}
	// A switchover is already under way, or a failover has already picked a target:
	// whatever it is, let it land before asking for another move.
	if target := cluster.Status.TargetPrimary; target != "" && target != current {
		return ""
	}
	// The primary must be healthy and be the primary. A failback is an optimisation
	// of a working cluster; when the primary is failing, failover owns the decision
	// and it applies the preference itself when it elects.
	if status, ok := observed.Instances[current]; !ok || !status.Ready || !status.Primary {
		return ""
	}
	// The primary is on its way out, which the drain switchover handles: it picks
	// the safest candidate under the same preference, and it is not worth racing it
	// to hand the role to an instance that will have to give it back.
	if slices.Contains(observed.Terminating, current) {
		return ""
	}
	// A failback is an automatic promotion like any other, so it waits out the
	// anti-flapping cooldown. Without this a preferred instance that keeps dying
	// would drag the primary back onto itself every time it briefly came back.
	if FailoverCooldownRemaining(cluster) > 0 {
		return ""
	}
	for _, name := range preferred {
		if name == current {
			return ""
		}
		if slices.Contains(observed.Fenced, name) || slices.Contains(observed.Diverged, name) {
			continue
		}
		if slices.Contains(cluster.Status.DivergedInstances, name) {
			continue
		}
		if slices.Contains(observed.Terminating, name) {
			continue
		}
		if promotable(name) {
			return name
		}
	}
	return ""
}
