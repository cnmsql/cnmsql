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
	"context"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

func (r *ClusterReconciler) reconcileSwitchover(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	observed observedCluster,
) (bool, error) {
	result, err := r.topologyReconciler(cluster).ReconcileSwitchover(ctx, cluster, topologyFailoverState(observed))
	if err != nil {
		return result.Handled, err
	}
	if result.Phase != nil {
		err = r.patchOperationPhase(ctx, cluster, observed, *result.Phase)
	}
	return result.Handled, err
}
