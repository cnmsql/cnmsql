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

package groupreplication

import (
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/util/intstr"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
	mysqlgr "github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/groupreplication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/replication"
)

// quorum returns `floor(N/2) + 1` for N members, the minimum number of
// ONLINE + RECOVERING members required for the group to be writable.
func quorum(members int) int {
	return members/2 + 1
}

// FenceQuorumGuard returns a blocking reason if fencing all members in fenceSet
// would leave fewer than quorum active members (ONLINE or RECOVERING). It
// computes quorum against spec.instances (not the current view size) so that
// the guard remains correct after members have been expelled from the group.
func (r *Reconciler) FenceQuorumGuard(cluster *mysqlv1alpha1.Cluster, fenceSet []string) *topology.QuorumResult {
	gr := cluster.Status.GroupReplication
	n := int32(cluster.Spec.Instances)
	q := quorum(int(n))
	if gr == nil {
		return nil
	}
	online := 0
	for _, member := range gr.Members {
		if member.State == mysqlgr.MemberStateOnline || member.State == mysqlgr.MemberStateRecovering {
			online++
		}
	}
	fencing := 0
	for _, name := range fenceSet {
		for _, member := range gr.Members {
			if member.Instance == name {
				fencing++
				break
			}
		}
	}
	after := online - fencing
	if after < q {
		return &topology.QuorumResult{
			Blocked:     true,
			Reason:      fmt.Sprintf("fencing %d member(s) would drop the group to %d active members, below quorum (%d)", fencing, after, q),
			CurrentSize: online,
			Quorum:      q,
		}
	}
	return nil
}

// PDBMaxUnavailable returns maxUnavailable = N - quorum so voluntary
// disruptions can never break quorum (e.g. N=3 -> 1, N=5 -> 2).
func (r *Reconciler) PDBMaxUnavailable(cluster *mysqlv1alpha1.Cluster) (intstr.IntOrString, intstr.IntOrString) {
	n := int32(cluster.Spec.Instances)
	q := int32(quorum(int(n)))
	mu := max(n-q, 0)
	return intstr.FromInt32(mu), intstr.FromInt32(0)
}

// ScaleDownQuorumGuard returns a blocking reason if removing instanceName (the
// highest-ordinal member during scale-down) would drop the group below quorum.
func (r *Reconciler) ScaleDownQuorumGuard(cluster *mysqlv1alpha1.Cluster, instanceName string) *topology.QuorumResult {
	n := int32(cluster.Spec.Instances)
	if n <= 1 {
		return &topology.QuorumResult{
			Blocked:     true,
			Reason:      "cannot scale below 1 member",
			CurrentSize: int(n),
			Quorum:      1,
		}
	}
	after := n - 1
	q := quorum(int(after))
	if after < int32(q) {
		return &topology.QuorumResult{
			Blocked:     true,
			Reason:      fmt.Sprintf("scaling down to %d members would drop below quorum (%d)", after, q),
			CurrentSize: int(n),
			Quorum:      q,
		}
	}
	return nil
}

// ComputeForceQuorumRecovery selects the safe survivor for a group that has lost
// quorum. It picks the majority partition's most-advanced member by GTID position
// (a surviving PRIMARY is authoritative; otherwise the ONLINE member whose
// gtid_executed is a superset of every other survivor's). When no survivor can be
// proven most-advanced — incomparable or missing GTID sets — it returns nil so
// the cluster stays Blocked rather than re-forming the group from a member that
// might be behind.
func (r *Reconciler) ComputeForceQuorumRecovery(
	cluster *mysqlv1alpha1.Cluster,
	gtidByInstance map[string]string,
) *topology.ForceQuorumRecovery {
	gr := cluster.Status.GroupReplication
	if gr == nil {
		return nil
	}
	if gr.HasQuorum {
		return nil
	}
	online := make([]mysqlv1alpha1.GroupMember, 0, len(gr.Members))
	for _, m := range gr.Members {
		if m.State == mysqlgr.MemberStateOnline {
			online = append(online, m)
		}
	}

	// No ONLINE survivor anywhere: this is a total outage (every member is down
	// or OFFLINE, so the group view is gone). force_members cannot help — there
	// is no live view to reset. Re-form the group by bootstrapping the
	// most-advanced reachable member instead. See computeRebootstrap for the
	// stricter safety bar this requires.
	if len(online) == 0 {
		return r.computeRebootstrap(cluster, gtidByInstance)
	}

	survivor, ok := selectQuorumSurvivor(online, gtidByInstance)
	if !ok {
		return nil
	}
	return &topology.ForceQuorumRecovery{
		Action:   topology.QuorumRecoveryForceMembers,
		Survivor: survivor.Instance,
		// force_members must match the survivor's group_replication_local_address
		// exactly, which is the member's XCom FQDN (see memberAddress), not the
		// bare Pod name.
		ForceMembers: memberAddress(survivor.Instance, cluster.Namespace),
	}
}

// computeRebootstrap selects the member to re-bootstrap a group after a total
// outage — when no member survived ONLINE so there is no group view to reset.
// Because nothing is live, the survivor cannot be cross-checked against a quorate
// view; the only safe guarantee is that the chosen member already holds every
// transaction any other member could replay. So this demands a strictly higher
// bar than force_members: every configured instance must be reachable with a
// known gtid_executed, and the survivor's set must dominate all of them. If any
// instance is unreachable (it might hold transactions the survivor lacks) or no
// member dominates, it returns nil and the cluster stays Blocked.
func (r *Reconciler) computeRebootstrap(
	cluster *mysqlv1alpha1.Cluster,
	gtidByInstance map[string]string,
) *topology.ForceQuorumRecovery {
	survivor, ok := selectRebootstrapSurvivor(gtidByInstance, cluster.Spec.Instances)
	if !ok {
		return nil
	}
	return &topology.ForceQuorumRecovery{
		Action:   topology.QuorumRecoveryRebootstrap,
		Survivor: survivor,
	}
}

// selectRebootstrapSurvivor picks the member to re-bootstrap a group from after a
// total outage. It requires gtid_executed for every configured instance (a
// missing or unreachable member could hold transactions the survivor lacks, and
// re-bootstrapping from a behind member would silently drop them) and returns the
// lexically-first member whose set dominates all others. Iteration is sorted so
// the choice is deterministic across reconciles and ties (identical GTID sets).
// Returns ok=false when any instance is missing or no member dominates.
func selectRebootstrapSurvivor(gtidByInstance map[string]string, configured int) (string, bool) {
	if configured <= 0 || len(gtidByInstance) < configured {
		return "", false
	}
	names := make([]string, 0, len(gtidByInstance))
	for name := range gtidByInstance {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, candidate := range names {
		candidateGTID := gtidByInstance[candidate]
		if candidateGTID == "" {
			continue
		}
		if gtidDominatesAll(candidateGTID, names, candidate, gtidByInstance) {
			return candidate, true
		}
	}
	return "", false
}

// gtidDominatesAll reports whether candidateGTID contains every other named
// member's gtid_executed. An unparseable set or a peer the candidate does not
// contain disproves dominance.
func gtidDominatesAll(candidateGTID string, names []string, candidate string, gtidByInstance map[string]string) bool {
	for _, other := range names {
		if other == candidate {
			continue
		}
		otherGTID := gtidByInstance[other]
		contained, err := replication.GTIDContains(candidateGTID, otherGTID)
		if err != nil || !contained {
			return false
		}
	}
	return true
}

// selectQuorumSurvivor chooses the safe survivor among the ONLINE members of a
// group that has lost quorum. A surviving PRIMARY is authoritative: in
// single-primary GR it holds every committed transaction. A lone survivor is the
// only option and is taken as-is. Otherwise the survivor must be provably
// most-advanced — its gtid_executed a superset of every other survivor's; if no
// member dominates all others (incomparable or missing GTID sets) recovery is
// unsafe and ok is false.
func selectQuorumSurvivor(
	online []mysqlv1alpha1.GroupMember,
	gtidByInstance map[string]string,
) (mysqlv1alpha1.GroupMember, bool) {
	if len(online) == 0 {
		return mysqlv1alpha1.GroupMember{}, false
	}
	if len(online) == 1 {
		return online[0], true
	}
	for _, m := range online {
		if m.Role == mysqlgr.MemberRolePrimary {
			return m, true
		}
	}
	for _, candidate := range online {
		candidateGTID := gtidByInstance[candidate.Instance]
		if candidateGTID == "" {
			continue
		}
		if dominatesAll(candidate, candidateGTID, online, gtidByInstance) {
			return candidate, true
		}
	}
	return mysqlv1alpha1.GroupMember{}, false
}

// dominatesAll reports whether candidateGTID contains every other online
// member's gtid_executed. An empty peer GTID is trivially contained; an
// unparseable set or a peer the candidate does not contain disproves dominance.
func dominatesAll(
	candidate mysqlv1alpha1.GroupMember,
	candidateGTID string,
	online []mysqlv1alpha1.GroupMember,
	gtidByInstance map[string]string,
) bool {
	for _, other := range online {
		if other.Instance == candidate.Instance {
			continue
		}
		otherGTID := gtidByInstance[other.Instance]
		if otherGTID == "" {
			continue
		}
		contained, err := replication.GTIDContains(candidateGTID, otherGTID)
		if err != nil || !contained {
			return false
		}
	}
	return true
}
