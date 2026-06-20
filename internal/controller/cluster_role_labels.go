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

package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

// reconcileRoleLabels keeps Pod role labels in step with the current primary so
// the rw Service points only at it and ro/r point at the replicas.
func (r *ClusterReconciler) reconcileRoleLabels(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	observed observedCluster,
) error {
	primary := cluster.Status.CurrentPrimary
	if primary == "" {
		primary = observed.PrimaryName
	}
	if primary == "" {
		return nil
	}
	return r.patchRoleLabels(ctx, cluster, observed.InstanceNames, primary)
}

func (r *ClusterReconciler) patchRoleLabels(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	instanceNames []string,
	primaryName string,
) error {
	for _, name := range instanceNames {
		pod := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, pod); err != nil {
			return client.IgnoreNotFound(err)
		}
		if pod.DeletionTimestamp != nil {
			continue
		}
		desired := roleReplica
		if name == primaryName {
			desired = rolePrimary
		}
		if pod.Labels[roleLabel] == desired {
			continue
		}
		before := pod.DeepCopy()
		if pod.Labels == nil {
			pod.Labels = map[string]string{}
		}
		pod.Labels[roleLabel] = desired
		if err := r.Patch(ctx, pod, client.MergeFrom(before)); err != nil {
			return err
		}
	}
	return nil
}
