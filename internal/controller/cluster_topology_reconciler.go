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
	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/async"
	controllergr "github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/groupreplication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
)

func (r *ClusterReconciler) topologyReconciler(cluster *mysqlv1alpha1.Cluster) topology.Reconciler {
	if cluster.IsGroupReplication() {
		return controllergr.NewReconciler(r.Client, r.Scheme)
	}
	return async.NewReconciler(r.Client, r.Scheme)
}
