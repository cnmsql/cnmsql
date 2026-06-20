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

package async

import (
	"fmt"
	"slices"

	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/replication"
)

// PrimaryHealthy reports whether the expected primary is reachable, ready, and
// still acting as primary.
func PrimaryHealthy(observed topology.FailoverState) bool {
	status, ok := observed.Instances[observed.PrimaryName]
	return ok && status.Ready && status.Primary
}

// HasObservedReplica distinguishes an established replica set from one that is
// still being provisioned.
func HasObservedReplica(observed topology.FailoverState) bool {
	for name := range observed.Instances {
		if name != observed.PrimaryName {
			return true
		}
	}
	return false
}

// SelectFailoverCandidate chooses the safest reachable async replica. The SQL
// applier must be running and its GTID set must contain every other candidate's.
// Equal GTID sets resolve to the first instance, preserving ordinal order.
func SelectFailoverCandidate(observed topology.FailoverState, knownDiverged []string) (string, string) {
	var candidates []string
	divergedSkipped := 0
	for _, name := range observed.InstanceNames {
		if name == observed.PrimaryName || slices.Contains(observed.Fenced, name) {
			continue
		}
		if slices.Contains(knownDiverged, name) {
			divergedSkipped++
			continue
		}
		status, ok := observed.Instances[name]
		if !ok || !status.Replica || !status.SQLRunning || status.GTID == "" {
			continue
		}
		candidates = append(candidates, name)
	}
	if len(candidates) == 0 {
		if divergedSkipped > 0 {
			return "", "every replica candidate has diverged from the failed primary (errant transactions); manual recovery required"
		}
		return "", "no healthy replica candidate available"
	}
	for _, candidate := range candidates {
		dominatesAll := true
		for _, other := range candidates {
			if candidate == other {
				continue
			}
			contains, err := replication.GTIDContains(
				observed.Instances[candidate].GTID,
				observed.Instances[other].GTID,
			)
			if err != nil {
				return "", fmt.Sprintf("comparing gtid sets: %v", err)
			}
			if !contains {
				dominatesAll = false
				break
			}
		}
		if dominatesAll {
			return candidate, ""
		}
	}
	return "", "candidate replicas have diverged GTID sets that cannot be proven safe"
}
