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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// reconcileReload applies a pending configuration reload to the cluster's
// instances. A reload is requested by stamping reloadAnnotation on the Cluster
// (e.g. `kubectl cnmsql reload`); the SET GLOBAL pass re-applies the dynamic
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
