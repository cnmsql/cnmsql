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
	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/internal/controller/async"
	controllergr "github.com/cnmsql/cnmsql/internal/controller/groupreplication"
	"github.com/cnmsql/cnmsql/internal/controller/topology"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

func (r *ClusterReconciler) topologyReconciler(cluster *mysqlv1alpha1.Cluster) topology.Reconciler {
	if cluster.IsGroupReplication() {
		return controllergr.NewReconciler(r.Client, r.Scheme, r.instanceControlClient(), r.Recorder)
	}
	return async.NewReconciler(
		r.Client,
		r.Scheme,
		r.instanceControlClient(),
		r.Recorder,
		r.OperatorExecutableHash,
	)
}

func topologyFailoverState(observed observedCluster) topology.FailoverState {
	instances := make(map[string]topology.FailoverInstance, len(observed.StatusByInstance))
	for name, status := range observed.StatusByInstance {
		if status == nil {
			continue
		}
		instance := topology.FailoverInstance{
			Ready:            status.IsReady,
			Primary:          status.Role == webserver.RolePrimary,
			Replica:          status.Role == webserver.RoleReplica,
			Role:             string(status.Role),
			GTID:             observed.GTIDByInstance[name],
			InPlaceUpgrading: status.InPlaceUpgrading,
		}
		if status.Replication != nil {
			instance.IORunning = status.Replication.IORunning
			instance.SQLRunning = status.Replication.SQLRunning
		}
		instances[name] = instance
	}
	return topology.FailoverState{
		PrimaryName:   observed.PrimaryName,
		InstanceNames: observed.InstanceNames,
		Instances:     instances,
		Fenced:        observed.FencedInstances,
		Diverged:      observed.DivergedInstances,
		Terminating:   observed.TerminatingInstances,
	}
}

func topologyAvailabilityState(observed observedCluster) topology.AvailabilityState {
	instances := make(map[string]topology.InstanceAvailability, len(observed.StatusByInstance))
	for name, status := range observed.StatusByInstance {
		if status != nil {
			instances[name] = topology.InstanceAvailability{Ready: status.IsReady}
		}
	}
	return topology.AvailabilityState{
		PrimaryName:       observed.PrimaryName,
		Instances:         instances,
		DivergedInstances: observed.DivergedInstances,
		FencedInstances:   observed.FencedInstances,
	}
}

func topologyObservationInput(observed observedCluster, gr *mysqlv1alpha1.GroupReplicationStatus, priorDiverged []string) topology.ObservationInput {
	in := topology.ObservationInput{
		PrimaryName:            observed.PrimaryName,
		InstanceNames:          observed.InstanceNames,
		StatusByInstance:       observed.StatusByInstance,
		GTIDByInstance:         observed.GTIDByInstance,
		ConfiguredMembers:      observed.Plan.Instances,
		PriorDivergedInstances: priorDiverged,
	}
	if gr != nil {
		in.ObservedViewMax = gr.ObservedViewMax
	}
	return in
}
