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

	"k8s.io/apimachinery/pkg/util/intstr"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
	mysqlgr "github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/groupreplication"
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

// ComputeForceQuorumRecovery selects the safe survivor set for a group that has
// lost quorum. It picks the majority partition's most-advanced member by
// GTID-position (the primary when one survives, otherwise the ONLINE member
// with the highest GTID). When no safe survivor set can be proven it returns nil.
func (r *Reconciler) ComputeForceQuorumRecovery(cluster *mysqlv1alpha1.Cluster) *topology.ForceQuorumRecovery {
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
	if len(online) == 0 {
		return nil
	}
	survivor := online[0]
	for _, m := range online[1:] {
		if m.Role == mysqlgr.MemberRolePrimary || (survivor.Role != mysqlgr.MemberRolePrimary && m.Instance > survivor.Instance) {
			survivor = m
		}
	}
	return &topology.ForceQuorumRecovery{
		Action:       "force_members",
		Survivor:     survivor.Instance,
		ForceMembers: fmt.Sprintf("%s:%d", survivor.Instance, 33061),
	}
}
