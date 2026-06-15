/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
)

// reconcileReload applies a pending configuration reload to the cluster's
// instances. A reload is requested by stamping reloadAnnotation on the Cluster
// (e.g. `kubectl cloudnative-mysql reload`); the SET GLOBAL pass re-applies the dynamic
// my.cnf parameters to each running mysqld without a restart.
//
// Idempotency is per-instance: the applied token is recorded on each Pod via
// reloadAppliedAnnotation, so an instance is only reloaded once per request and
// instances that were unreachable on a prior pass are caught up on a later one.
func (r *ClusterReconciler) reconcileReload(ctx context.Context, cluster *mysqlv1alpha1.Cluster) error {
	token := cluster.Annotations[reloadAnnotation]
	if token == "" {
		return nil
	}

	log := logf.FromContext(ctx).WithName("reload")
	params := cluster.Spec.MySQL.Parameters
	control := r.instanceControlClient()

	for _, name := range cluster.Status.InstanceNames {
		pod := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, pod); err != nil {
			return err
		}
		if pod.Annotations[reloadAppliedAnnotation] == token {
			continue
		}
		if !podReady(pod) {
			// Skip not-ready instances; they pick up the desired config from the
			// rendered my.cnf on (re)start, and a later pass reloads them once ready.
			continue
		}

		resp, err := control.Reload(ctx, cluster, name, webserver.ReloadRequest{Parameters: params})
		if err != nil {
			return err
		}
		if len(resp.Skipped) > 0 {
			log.Info("Some parameters could not be applied at runtime (restart required)",
				"instance", name, "skipped", resp.Skipped)
		}
		log.Info("Applied configuration reload", "instance", name, "applied", resp.Applied)

		before := pod.DeepCopy()
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}
		pod.Annotations[reloadAppliedAnnotation] = token
		if err := r.Patch(ctx, pod, client.MergeFrom(before)); err != nil {
			return err
		}
	}
	return nil
}
