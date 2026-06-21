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
	"k8s.io/apimachinery/pkg/util/intstr"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
)

// FenceQuorumGuard always allows fencing for async topologies; quorum is a GR
// concept and does not apply to the primary-replica model.
func (r *Reconciler) FenceQuorumGuard(_ *mysqlv1alpha1.Cluster, _ []string) *topology.QuorumResult {
	return nil
}

// PDBMaxUnavailable returns the async split values: max 1 primary, floor(N/2)
// replicas. The caller applies these per the existing split.
func (r *Reconciler) PDBMaxUnavailable(cluster *mysqlv1alpha1.Cluster) (intstr.IntOrString, intstr.IntOrString) {
	replicas := int32(cluster.Spec.Instances - 1)
	return intstr.FromInt32(1), intstr.FromInt32(replicas / 2)
}

// ScaleDownQuorumGuard always allows scale-down for async topologies.
func (r *Reconciler) ScaleDownQuorumGuard(_ *mysqlv1alpha1.Cluster, _ string) *topology.QuorumResult {
	return nil
}

// ComputeForceQuorumRecovery returns nil for async — quorum recovery is a GR
// concept.
func (r *Reconciler) ComputeForceQuorumRecovery(_ *mysqlv1alpha1.Cluster, _ map[string]string) *topology.ForceQuorumRecovery {
	return nil
}
