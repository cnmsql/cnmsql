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
	"fmt"
	"strings"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	controllerasync "github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/async"
	mysqlconfig "github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/config"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/groupreplication"
)

// memberAddress is a member's XCom address, the stable per-Pod DNS name plus the
// Group Replication communication port. It matches the host async replicas use
// to reach a source (<pod>.<namespace>.svc), so members resolve each other
// through the same headless DNS.
func memberAddress(name, namespace string) string {
	return fmt.Sprintf("%s.%s.svc:%d", name, namespace, mysqlconfig.DefaultGroupReplicationPort)
}

// groupReplicationConfig builds the fully-resolved GR rendering input for one
// instance, or reports false when the cluster is not in GR mode. The group name
// must already be pinned in status; callers gate config rendering on that.
func groupReplicationConfig(
	cluster *mysqlv1alpha1.Cluster,
	plan clusterPlan,
	inst instancePlan,
) (mysqlconfig.GroupReplication, bool) {
	if !cluster.IsGroupReplication() {
		return mysqlconfig.GroupReplication{}, false
	}

	seeds := make([]string, 0, plan.Instances)
	for _, name := range plan.instanceNames(cluster) {
		seeds = append(seeds, memberAddress(name, cluster.Namespace))
	}

	gr := mysqlconfig.GroupReplication{
		GroupName:    cluster.PinnedGroupName(),
		LocalAddress: memberAddress(inst.Name, cluster.Namespace),
		GroupSeeds:   strings.Join(seeds, ","),
		// Reuse the cluster's server TLS material for the distributed-recovery
		// channel, mirroring the async replication SSL paths.
		RecoverySSL: mysqlconfig.TLSPaths{
			CA:   clientCAPath + "/ca.crt",
			Cert: serverTLSPath + "/tls.crt",
			Key:  serverTLSPath + "/tls.key",
		},
	}

	tunables := cluster.ResolvedGroupReplicationTunables()
	gr.Consistency = tunables.Consistency
	gr.ExitStateAction = tunables.ExitStateAction
	gr.AutoRejoinTries = tunables.AutoRejoinTries
	return gr, true
}

// ensureGroupName pins status.groupReplication.groupName on a GR cluster before
// any instance config is rendered. The name is the user-pinned
// spec.replication.groupReplication.groupName if set, otherwise a generated
// UUID. It is sticky and immutable thereafter (the status webhook enforces this);
// every member renders the same name. It is a no-op for async clusters or once
// the name is already pinned. updateStatus mirrors the patch back onto the
// in-memory cluster, so the rest of the reconcile sees the pinned name.
func (r *ClusterReconciler) ensureGroupName(ctx context.Context, cluster *mysqlv1alpha1.Cluster) error {
	if !cluster.IsGroupReplication() || cluster.PinnedGroupName() != "" {
		return nil
	}
	name := cluster.DesiredGroupName()
	return r.updateStatus(ctx, cluster, func(s *mysqlv1alpha1.ClusterStatus) {
		if s.GroupReplication == nil {
			s.GroupReplication = &mysqlv1alpha1.GroupReplicationStatus{}
		}
		if s.GroupReplication.GroupName == "" {
			s.GroupReplication.GroupName = name
		}
	})
}

// donorAvailable reports whether a new member may be provisioned now: a healthy
// source exists to seed it. Async needs a healthy primary to clone; Group
// Replication needs a quorate group with at least one ONLINE donor for
// distributed recovery.
func donorAvailable(cluster *mysqlv1alpha1.Cluster, observed observedCluster) bool {
	if cluster.IsGroupReplication() {
		return groupHasOnlineDonor(observed)
	}
	return controllerasync.PrimaryHealthy(topologyFailoverState(observed))
}

// groupHasOnlineDonor reports whether the observed group has quorum and at least
// one ONLINE member to recover a joining member from. Until the operator has
// observed an ONLINE member (GroupReplication is nil before then) no donor is
// available, so the first joining member waits for the bootstrap member to come
// ONLINE.
func groupHasOnlineDonor(observed observedCluster) bool {
	gr := observed.GroupReplication
	if gr == nil || !gr.HasQuorum {
		return false
	}
	for _, m := range gr.Members {
		if m.State == groupreplication.MemberStateOnline {
			return true
		}
	}
	return false
}

// reconcileGroupName is the GR pre-step the main loop runs before provisioning,
// pinning the group name. The bool reports whether the caller should stop and
// return the error (mirroring the other guarded pre-steps). For M-GR.2 it only
// pins the name; richer GR provisioning gating lands in later phases.
func (r *ClusterReconciler) reconcileGroupName(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
) (error, bool) {
	if err := r.ensureGroupName(ctx, cluster); err != nil {
		return err, true
	}
	return nil, false
}
