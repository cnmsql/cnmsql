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
	var diverged []string
	for _, name := range input.InstanceNames {
		if name == input.PrimaryName {
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
		}
	}
	return diverged
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
