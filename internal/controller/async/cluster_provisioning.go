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

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
	mysqlconfig "github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/config"
)

// Name is the user-facing topology name used in reconciliation logs.
func (r *Reconciler) Name() string { return "async" }

// EnsureConfigured has no async topology preflight.
func (r *Reconciler) EnsureConfigured(context.Context, *mysqlv1alpha1.Cluster) error { return nil }

// ConfigureServer keeps the common async/semi-sync server configuration intact.
func (r *Reconciler) ConfigureServer(
	*mysqlv1alpha1.Cluster,
	topology.ServerConfigInput,
	*mysqlconfig.ServerConfig,
) {
}

// DonorAvailable requires a healthy async primary for physical cloning.
func (r *Reconciler) DonorAvailable(_ topology.Observation, observed topology.FailoverState) bool {
	return PrimaryHealthy(observed)
}
