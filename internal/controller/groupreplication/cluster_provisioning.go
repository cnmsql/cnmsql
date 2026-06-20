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

package groupreplication

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
	mysqlconfig "github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/config"
	mysqlgr "github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/groupreplication"
)

const (
	clientCAPath  = "/etc/cloudnative-mysql/tls/client-ca"
	serverTLSPath = "/etc/cloudnative-mysql/tls/server"
)

// Name is the user-facing topology name used in reconciliation logs.
func (r *Reconciler) Name() string { return "groupReplication" }

// EnsureConfigured pins the immutable GR group name before member config is rendered.
func (r *Reconciler) EnsureConfigured(ctx context.Context, cluster *mysqlv1alpha1.Cluster) error {
	if cluster.PinnedGroupName() != "" {
		return nil
	}
	name := cluster.DesiredGroupName()
	latest := &mysqlv1alpha1.Cluster{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
	if err := r.client.Get(ctx, key, latest); err != nil {
		return err
	}
	before := latest.DeepCopy()
	if latest.Status.GroupReplication == nil {
		latest.Status.GroupReplication = &mysqlv1alpha1.GroupReplicationStatus{}
	}
	if latest.Status.GroupReplication.GroupName == "" {
		latest.Status.GroupReplication.GroupName = name
	}
	if err := r.client.Status().Patch(ctx, latest, client.MergeFrom(before)); err != nil {
		return err
	}
	latest.Status.DeepCopyInto(&cluster.Status)
	return nil
}

// ConfigureServer replaces async replication settings with GR member settings.
func (r *Reconciler) ConfigureServer(
	cluster *mysqlv1alpha1.Cluster,
	input topology.ServerConfigInput,
	config *mysqlconfig.ServerConfig,
) {
	seeds := make([]string, 0, len(input.MemberNames))
	for _, name := range input.MemberNames {
		seeds = append(seeds, memberAddress(name, cluster.Namespace))
	}
	tunables := cluster.ResolvedGroupReplicationTunables()
	config.TopologyMode = mysqlconfig.TopologyGroupReplication
	config.GroupReplication = mysqlconfig.GroupReplication{
		GroupName:       cluster.PinnedGroupName(),
		LocalAddress:    memberAddress(input.InstanceName, cluster.Namespace),
		GroupSeeds:      strings.Join(seeds, ","),
		Consistency:     tunables.Consistency,
		ExitStateAction: tunables.ExitStateAction,
		AutoRejoinTries: tunables.AutoRejoinTries,
		RecoverySSL: mysqlconfig.TLSPaths{
			CA:   clientCAPath + "/ca.crt",
			Cert: serverTLSPath + "/tls.crt",
			Key:  serverTLSPath + "/tls.key",
		},
	}
	config.SemiSync = mysqlconfig.SemiSync{}
}

// DonorAvailable requires a quorate group with an ONLINE recovery donor.
func (r *Reconciler) DonorAvailable(observed topology.Observation, _ topology.FailoverState) bool {
	group := observed.GroupReplication
	if group == nil || !group.HasQuorum {
		return false
	}
	for _, member := range group.Members {
		if member.State == mysqlgr.MemberStateOnline {
			return true
		}
	}
	return false
}

// PodPolicy initialises joining members empty and enables the GR instance strategy.
func (r *Reconciler) PodPolicy(*mysqlv1alpha1.Cluster) topology.PodPolicy {
	return topology.PodPolicy{
		InitializeReplica: true,
		InitDBArgs:        []string{"--group-replication"},
		RunArgs:           []string{"--group-replication"},
	}
}

// PublishNotReadyAddresses excludes non-ONLINE GR members from every route.
func (r *Reconciler) PublishNotReadyAddresses(mysqlv1alpha1.ServiceSelectorType) bool {
	return false
}

func memberAddress(name, namespace string) string {
	return fmt.Sprintf("%s.%s.svc:%d", name, namespace, mysqlconfig.DefaultGroupReplicationPort)
}
