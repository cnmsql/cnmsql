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
	"context"
	"slices"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
)

// ReconcileAvailability self-heals semi-sync while the cluster is degraded.
func (r *Reconciler) ReconcileAvailability(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	observed topology.AvailabilityState,
) error {
	minSync := cluster.Spec.MinSyncReplicas
	if !cluster.IsSemiSyncEnabled() || minSync <= 0 {
		return nil
	}

	primary := observed.PrimaryName
	if primary == "" {
		return nil
	}
	if _, ok := observed.Instances[primary]; !ok {
		return nil
	}

	desired := minSync
	if cluster.SemiSyncDurabilityPreferred() {
		healthy := healthyReplicaCount(observed)
		if healthy < minSync {
			desired = max(1, healthy)
		}
	}

	if err := r.semiSyncControl.SetSemiSyncWaitForReplicaCount(ctx, cluster, primary, desired); err != nil {
		return err
	}
	if desired != minSync {
		logf.FromContext(ctx).Info("Self-healed semi-sync acknowledgement count",
			"primary", primary, "configured", minSync, "effective", desired)
	}
	return nil
}

func healthyReplicaCount(observed topology.AvailabilityState) int {
	healthy := 0
	for name, status := range observed.Instances {
		if name == observed.PrimaryName {
			continue
		}
		if status.Ready &&
			!slices.Contains(observed.DivergedInstances, name) &&
			!slices.Contains(observed.FencedInstances, name) {
			healthy++
		}
	}
	return healthy
}
