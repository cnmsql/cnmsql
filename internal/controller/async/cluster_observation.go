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
	"github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/replication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
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

func detectDivergedReplicas(input topology.ObservationInput) []string {
	primaryGTID := input.GTIDByInstance[input.PrimaryName]
	if primaryGTID == "" {
		return nil
	}
	var diverged []string
	for _, name := range input.InstanceNames {
		if name == input.PrimaryName {
			continue
		}
		gtid := input.GTIDByInstance[name]
		if gtid == "" {
			continue
		}
		contained, err := replication.GTIDContains(primaryGTID, gtid)
		if err == nil && !contained {
			diverged = append(diverged, name)
		}
	}
	return diverged
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
