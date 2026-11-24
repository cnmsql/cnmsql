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

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// reconcileReload applies a pending configuration reload to the cluster's
// instances. A reload is requested by stamping reloadAnnotation on the Cluster
// (e.g. `kubectl cnmsql reload`, or automatically when spec.mysql.parameters
// changed, see reconcileParameters); the SET GLOBAL pass re-applies dynamic
// my.cnf parameters to each running mysqld without a restart.
//
// The request content depends on who stamped the token: a token that matches
// the pending automatic change set sends only the changed parameters, so manual
// SET GLOBAL drift on unchanged parameters survives; any other token is an
// explicit reload and sends the full spec to correct drift.
//
// Idempotency is per-instance: the applied token is recorded on each Pod via
// reloadAppliedAnnotation, so an instance is only reloaded once per request and
// instances that were unreachable on a prior pass are caught up on a later one.
func (r *ClusterReconciler) reconcileReload(ctx context.Context, cluster *mysqlv1alpha1.Cluster) error {
	token := cluster.Annotations[reloadAnnotation]
	if token == "" {
		return nil
	}
	params, state := r.reloadParameters(ctx, cluster)
	if state != nil && state.Pending != nil {
		// Outcomes from any driver classify the pending change set: evidence is
		// a property of the parameter, not of the pass that probed it.
		defer func() {
			if err := r.saveParametersState(ctx, cluster, state); err != nil {
				logf.FromContext(ctx).WithName("reload").Error(err, "Could not persist parameter probe results")
			}
		}()
	}
	return r.applyReload(ctx, cluster, token, params, func(resp *webserver.ReloadResponse) {
		if state != nil && state.Pending != nil {
			classifyReloadResults(state.Pending.Changes, resp)
		}
	})
}

// reloadParameters resolves the parameters for the reload pass named by the
// Cluster's reload token.
func (r *ClusterReconciler) reloadParameters(_ context.Context, cluster *mysqlv1alpha1.Cluster) (map[string]string, *parametersState) {
	state := loadParametersState(cluster)
	if state != nil && state.Pending != nil && state.Pending.Token == cluster.Annotations[reloadAnnotation] {
		return probeParameters(state.Pending), state
	}
	return cluster.Spec.MySQL.Parameters, state
}
