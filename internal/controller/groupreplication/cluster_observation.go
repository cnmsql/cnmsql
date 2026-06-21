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
	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
	mysqlgr "github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/groupreplication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
)

// Observe cross-checks the elected primary across ONLINE members' quorate group
// views and produces the operator's authoritative GR status.
func (r *Reconciler) Observe(input topology.ObservationInput) topology.Observation {
	status, primary := observeGroup(input)
	return topology.Observation{
		PrimaryName:          primary,
		PrimaryAuthoritative: true,
		GroupReplication:     status,
	}
}

func observeGroup(input topology.ObservationInput) (*mysqlv1alpha1.GroupReplicationStatus, string) {
	uuidToInstance := map[string]string{}
	for name, status := range input.StatusByInstance {
		if status != nil && status.GroupReplication != nil && status.GroupReplication.MemberID != "" {
			uuidToInstance[status.GroupReplication.MemberID] = name
		}
	}

	var view *webserver.GroupReplicationMemberStatus
	primaryVotes := map[string]int{}
	maxViewMembers := 0
	for _, name := range input.InstanceNames {
		status := input.StatusByInstance[name]
		if status == nil || status.GroupReplication == nil ||
			status.GroupReplication.State != mysqlgr.MemberStateOnline {
			continue
		}
		group := status.GroupReplication
		onlineCount := 0
		primaryID := ""
		for _, member := range group.Members {
			if member.State == mysqlgr.MemberStateOnline {
				onlineCount++
			}
			if member.State == mysqlgr.MemberStateOnline && member.Role == mysqlgr.MemberRolePrimary {
				if primaryID != "" {
					primaryID = ""
					break
				}
				primaryID = member.MemberID
			}
		}
		if onlineCount*2 <= len(group.Members) {
			continue
		}
		if view == nil {
			view = group
		}
		maxViewMembers = max(maxViewMembers, len(group.Members))
		if primaryID != "" {
			primaryVotes[primaryID]++
		}
	}
	if view == nil {
		return nil, ""
	}

	primaryID := ""
	for candidate, votes := range primaryVotes {
		if votes*2 > maxViewMembers {
			primaryID = candidate
			break
		}
	}
	primaryInstance := uuidToInstance[primaryID]
	members := make([]mysqlv1alpha1.GroupMember, 0, len(view.Members))
	onlineCount := 0
	for _, member := range view.Members {
		members = append(members, mysqlv1alpha1.GroupMember{
			Instance:  uuidToInstance[member.MemberID],
			State:     member.State,
			Role:      member.Role,
			Reachable: member.State != mysqlgr.MemberStateUnreachable,
		})
		if member.State == mysqlgr.MemberStateOnline {
			onlineCount++
		}
	}
	return &mysqlv1alpha1.GroupReplicationStatus{
		PrimaryMember:     primaryInstance,
		Members:           members,
		ViewID:            view.ViewID,
		HasQuorum:         onlineCount*2 > quorumDenominator(input.ConfiguredMembers, input.ObservedViewMax, maxViewMembers),
		ObservedViewMax:   maxViewMembers,
		ObservedOnlineMax: onlineCount,
	}, primaryInstance
}

// quorumDenominator returns the group size used for quorum arithmetic.
// During bootstrap/scale-up (maxView < configured), use the actual view size
// so the group reports quorum and new members can join via distributed
// recovery. Once the group reaches its configured size, use the configured
// count so that a shrunken group is detected as quorum-lost.
func quorumDenominator(configured, observedMax, currentMax int) int {
	if observedMax > 0 && observedMax >= configured {
		return observedMax
	}
	return currentMax
}

// MergeStatus preserves sticky GR fields and mirrors the elected primary.
func (r *Reconciler) MergeStatus(cluster *mysqlv1alpha1.Cluster, observed topology.Observation) {
	existing := cluster.Status.GroupReplication
	merged := &mysqlv1alpha1.GroupReplicationStatus{}
	if existing != nil {
		merged.GroupName = existing.GroupName
		merged.Bootstrapped = existing.Bootstrapped
		merged.ObservedViewMax = existing.ObservedViewMax
		merged.ObservedOnlineMax = existing.ObservedOnlineMax
	}
	if status := observed.GroupReplication; status != nil {
		merged.PrimaryMember = status.PrimaryMember
		merged.Members = status.Members
		merged.ViewID = status.ViewID
		merged.ObservedViewMax = max(merged.ObservedViewMax, status.ObservedViewMax)
		merged.ObservedOnlineMax = max(merged.ObservedOnlineMax, status.ObservedOnlineMax)
		if status.PrimaryMember != "" {
			merged.Bootstrapped = true
			cluster.Status.CurrentPrimary = status.PrimaryMember
		}
	}
	// Clamp the sticky maxima to the desired group size. ObservedViewMax only ever
	// grows via max(), so after a deliberate scale-down it would stay pinned at the
	// old, larger size and keep inflating the quorum denominator — making the
	// smaller group brittle (e.g. 5→3 then losing one member reads as quorum-lost).
	// spec.Instances is the authoritative group size, so the denominator must track
	// it down. Failure-driven shrink (spec unchanged) still detects quorum loss.
	if n := cluster.Spec.Instances; n > 0 {
		merged.ObservedViewMax = min(merged.ObservedViewMax, n)
		merged.ObservedOnlineMax = min(merged.ObservedOnlineMax, n)
	}
	// Quorum: use the sticky max view size as the denominator so that a group
	// that shrank (members expelled) is correctly flagged as quorum-lost, while
	// a bootstrapping group that grows uses its actual size until it reaches
	// the max observed.
	if merged.ObservedViewMax > 0 {
		online := 0
		for _, m := range merged.Members {
			if m.State == mysqlgr.MemberStateOnline {
				online++
			}
		}
		merged.HasQuorum = online*2 > merged.ObservedViewMax
	}
	cluster.Status.GroupReplication = merged
}

// ObservedFailover identifies an automatic GR election rather than bootstrap or
// an operator-requested primary handoff.
func (r *Reconciler) ObservedFailover(before, after *mysqlv1alpha1.Cluster) (string, string, bool) {
	from, to := before.Status.CurrentPrimary, after.Status.CurrentPrimary
	if from == "" || to == "" || from == to || before.Status.TargetPrimary == to {
		return "", "", false
	}
	return from, to, true
}
