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

package async

import (
	"strings"

	"github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/internal/controller/topology"
	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// Observe diagnoses GTID divergence and stopped async replication.
func (r *Reconciler) Observe(input topology.ObservationInput) topology.Observation {
	diverged := detectDivergedReplicas(input)
	return topology.Observation{
		DivergedInstances:          diverged,
		ReplicationBrokenInstances: detectReplicationBroken(input, diverged),
	}
}

// MergeStatus has no async-specific status block to merge.
func (r *Reconciler) MergeStatus(*v1alpha1.Cluster, topology.Observation) {}

// ObservedFailover is always false because async failover is operator-driven.
func (r *Reconciler) ObservedFailover(*v1alpha1.Cluster, *v1alpha1.Cluster) (string, string, bool) {
	return "", "", false
}

// detectDivergedReplicas compares each reachable replica's executed GTID set
// against the primary's and flags any that the primary does not fully contain
// (errant transactions).
//
// On MariaDB containment is blind to forks: a position records only the
// highest sequence per domain, so a forked former primary's dead 0-1-219
// compares as contained in the successor's 0-2-300. The primary timeline names
// who authored each stretch of sequence numbers, so a replica whose position is
// off the timeline is marked as soon as it is reachable, before it tries to
// replicate. Where the timeline has no verdict, a replica whose I/O thread the
// current primary refused with error 1236 is marked too. A prior mark clears
// only once the timeline proves the position canonical.
//
// Divergence is sticky: a previously flagged instance is only cleared once we
// can positively prove against a live primary that it has re-converged. When the
// primary's GTID is unavailable (e.g. the primary just died) we cannot compare,
// so we preserve the prior flags rather than assume everyone is clean — clearing
// here would let a diverged replica be elected primary at exactly the moment the
// guard matters most.
func detectDivergedReplicas(input topology.ObservationInput) []string {
	eng, err := engine.ForFlavor(engine.Flavor(input.EngineFlavor))
	if err != nil {
		return stillPresent(input.PriorDivergedInstances, input.InstanceNames)
	}
	gtidModel := eng.GTID()

	primaryGTID := input.GTIDByInstance[input.PrimaryName]
	if primaryGTID == "" {
		return stillPresent(input.PriorDivergedInstances, input.InstanceNames)
	}
	prior := map[string]bool{}
	for _, name := range input.PriorDivergedInstances {
		prior[name] = true
	}
	mariadb := engine.Flavor(input.EngineFlavor) == engine.FlavorMariaDB
	var diverged []string
	for _, name := range input.InstanceNames {
		if name == input.PrimaryName {
			continue
		}
		if mariadb && refusedByPrimary(input.StatusByInstance[name], input.PrimaryName) {
			diverged = append(diverged, name)
			continue
		}
		gtid := input.GTIDByInstance[name]
		if gtid == "" {
			if prior[name] {
				diverged = append(diverged, name)
			}
			continue
		}
		contained, err := gtidModel.Contains(primaryGTID, gtid)
		if err != nil {
			if prior[name] {
				diverged = append(diverged, name)
			}
			continue
		}
		if !contained {
			diverged = append(diverged, name)
			continue
		}
		if mariadb && len(input.MariaDBTimeline) > 0 {
			on, known, err := input.MariaDBTimeline.Judge(gtid)
			proven := err == nil && known && on
			if (err == nil && known && !on) || (prior[name] && !proven) {
				diverged = append(diverged, name)
			}
		}
	}
	return diverged
}

// mariadbSourceRefused is the I/O error MariaDB reports when the source cannot
// serve a replica's GTID position: it diverged, or the source purged what it
// needs. Either way only a re-clone brings it back.
const mariadbSourceRefused = 1236

// refusedByPrimary reports whether a replica's I/O thread stopped because the
// current primary refused its GTID position.
func refusedByPrimary(status *webserver.Status, primary string) bool {
	if status == nil || status.Replication == nil || primary == "" {
		return false
	}
	host, _, _ := strings.Cut(status.Replication.SourceHost, ".")
	return status.Replication.LastIOErrno == mariadbSourceRefused && host == primary
}

// stillPresent filters names down to those that are still part of the cluster,
// so divergence flags for removed instances are not carried forever.
func stillPresent(names, instanceNames []string) []string {
	if len(names) == 0 {
		return nil
	}
	present := map[string]bool{}
	for _, name := range instanceNames {
		present[name] = true
	}
	var kept []string
	for _, name := range names {
		if present[name] {
			kept = append(kept, name)
		}
	}
	return kept
}

func detectReplicationBroken(input topology.ObservationInput, divergedInstances []string) []string {
	diverged := map[string]bool{}
	for _, name := range divergedInstances {
		diverged[name] = true
	}
	var broken []string
	for _, name := range input.InstanceNames {
		if name == input.PrimaryName || diverged[name] {
			continue
		}
		if status, ok := input.StatusByInstance[name]; ok && replicationBroken(status) {
			broken = append(broken, name)
		}
	}
	return broken
}

func replicationBroken(status *webserver.Status) bool {
	replica := status.Replication
	return replica != nil && replica.LastError != "" && (!replica.SQLRunning || !replica.IORunning)
}
