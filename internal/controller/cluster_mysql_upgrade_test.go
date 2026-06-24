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
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/groupreplication"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

func versionStatusMap(versions map[string]string) map[string]*webserver.Status {
	out := make(map[string]*webserver.Status, len(versions))
	for name, v := range versions {
		out[name] = &webserver.Status{InstanceName: name, Version: v, IsReady: true}
	}
	return out
}

func TestMajorUpgradePending(t *testing.T) {
	t.Parallel()
	plan := clusterPlan{ServerVersion: "8.4.0"}
	names := []string{"c-1", "c-2"}

	build := func(v1, v2 string) observedCluster {
		return observedCluster{
			InstanceNames:    names,
			StatusByInstance: versionStatusMap(map[string]string{"c-1": v1, "c-2": v2}),
		}
	}

	if _, pending := majorUpgradePending(plan, build("8.0.36", "8.0.36")); !pending {
		t.Error("expected pending when instances run an older series than the target")
	}
	if _, pending := majorUpgradePending(plan, build("8.4.0", "8.0.36")); !pending {
		t.Error("expected pending when one instance still runs the older series")
	}
	if _, pending := majorUpgradePending(plan, build("8.4.3", "8.4.0")); pending {
		t.Error("did not expect pending when all instances are on the target series")
	}
	if _, pending := majorUpgradePending(plan, build("", "")); pending {
		t.Error("did not expect pending when no version is reported")
	}
}

func TestGroupCommunicationProtocolFinalization(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Replication = &mysqlv1alpha1.ReplicationConfiguration{
		Mode: mysqlv1alpha1.ReplicationModeGroupReplication,
	}
	control := &recordingControlClient{}
	scheme := testScheme(t)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).WithObjects(cluster).Build(),
		Scheme: scheme, ControlClient: control,
	}
	plan := clusterPlan{ServerVersion: "8.4.3"}
	primary, replica := cluster.Name+"-1", cluster.Name+"-2"
	status := func(name, serverVersion, state, protocol string) *webserver.Status {
		return &webserver.Status{
			InstanceName: name,
			Version:      serverVersion,
			IsReady:      true,
			GroupReplication: &webserver.GroupReplicationMemberStatus{
				State: state, CommunicationProtocol: protocol,
			},
		}
	}
	observed := observedCluster{
		InstanceNames: []string{primary, replica},
		PrimaryName:   primary,
		StatusByInstance: map[string]*webserver.Status{
			primary: status(primary, "8.4.3", groupreplication.MemberStateOnline, "8.0.36"),
			replica: status(replica, "8.4.3", groupreplication.MemberStateOnline, "8.0.36"),
		},
	}

	_, err, handled := r.reconcileGroupCommunicationProtocol(ctx, cluster, plan, observed)
	if err != nil {
		t.Fatalf("finalize protocol: %v", err)
	}
	if !handled {
		t.Fatal("expected lagging protocol to be finalized")
	}
	if len(control.setCommunicationProtocolInstances) != 1 ||
		control.setCommunicationProtocolInstances[0] != primary ||
		control.setCommunicationProtocolVersions[0] != plan.ServerVersion {
		t.Fatalf("protocol calls = instances %v versions %v", control.setCommunicationProtocolInstances, control.setCommunicationProtocolVersions)
	}
	persisted := &mysqlv1alpha1.Cluster{}
	if err := r.Get(ctx, types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, persisted); err != nil {
		t.Fatalf("get cluster status: %v", err)
	}
	if persisted.Status.GroupReplication == nil ||
		persisted.Status.GroupReplication.CommunicationProtocolTarget != plan.ServerVersion {
		t.Fatalf("persisted protocol target = %#v, want %q", persisted.Status.GroupReplication, plan.ServerVersion)
	}
}

func TestGroupCommunicationProtocolFinalizationWaits(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	cluster.Spec.Replication = &mysqlv1alpha1.ReplicationConfiguration{
		Mode: mysqlv1alpha1.ReplicationModeGroupReplication,
	}
	plan := clusterPlan{ServerVersion: "8.4.3"}
	primary, replica := cluster.Name+"-1", cluster.Name+"-2"
	tests := []struct {
		name            string
		replicaVersion  string
		replicaState    string
		primaryProtocol string
		finalizedTarget string
	}{
		{name: "member still upgrading", replicaVersion: "8.0.36", replicaState: groupreplication.MemberStateOnline, primaryProtocol: "8.0.36"},
		{name: "member not online", replicaVersion: "8.4.3", replicaState: groupreplication.MemberStateRecovering, primaryProtocol: "8.0.36"},
		{name: "target already finalized", replicaVersion: "8.4.3", replicaState: groupreplication.MemberStateOnline, primaryProtocol: "8.0.27", finalizedTarget: "8.4.3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			control := &recordingControlClient{}
			testCluster := cluster.DeepCopy()
			if tt.finalizedTarget != "" {
				testCluster.Status.GroupReplication = &mysqlv1alpha1.GroupReplicationStatus{
					CommunicationProtocolTarget: tt.finalizedTarget,
				}
			}
			r := &ClusterReconciler{ControlClient: control}
			observed := observedCluster{
				InstanceNames: []string{primary, replica}, PrimaryName: primary,
				StatusByInstance: map[string]*webserver.Status{
					primary: {
						Version: "8.4.3",
						GroupReplication: &webserver.GroupReplicationMemberStatus{
							State: groupreplication.MemberStateOnline, CommunicationProtocol: tt.primaryProtocol,
						},
					},
					replica: {
						Version:          tt.replicaVersion,
						GroupReplication: &webserver.GroupReplicationMemberStatus{State: tt.replicaState},
					},
				},
			}
			_, err, handled := r.reconcileGroupCommunicationProtocol(context.Background(), testCluster, plan, observed)
			if err != nil {
				t.Fatalf("finalize protocol: %v", err)
			}
			if handled || len(control.setCommunicationProtocolInstances) != 0 {
				t.Fatalf("unexpected protocol finalization: handled=%v calls=%v", handled, control.setCommunicationProtocolInstances)
			}
		})
	}
}

func upgradePendingObserved(cluster *mysqlv1alpha1.Cluster) observedCluster {
	name := cluster.Name + "-1"
	return observedCluster{
		InstanceNames:    []string{name},
		StatusByInstance: versionStatusMap(map[string]string{name: "8.0.36"}),
	}
}

func TestUpgradeBackupGateBlocksWithoutObjectStore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	scheme := testScheme(t)
	recorder := record.NewFakeRecorder(10)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).WithObjects(cluster).Build(),
		Scheme:   scheme,
		Recorder: recorder,
	}
	plan := clusterPlan{ServerVersion: "8.4.0"}

	_, err, handled := r.reconcileUpgradeBackupGate(ctx, cluster, plan, upgradePendingObserved(cluster))
	if err != nil {
		t.Fatalf("gate: %v", err)
	}
	if !handled {
		t.Fatal("expected the gate to block when no object store is configured")
	}
	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, "BackupRequired") {
			t.Fatalf("event = %q, want BackupRequired", event)
		}
	default:
		t.Fatal("expected a BackupRequired event")
	}
}

func TestUpgradeBackupGateDisabledProceeds(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Upgrade = &mysqlv1alpha1.UpgradeConfiguration{BackupBeforeUpgrade: ptr.To(false)}
	scheme := testScheme(t)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme: scheme,
	}
	plan := clusterPlan{ServerVersion: "8.4.0"}

	_, err, handled := r.reconcileUpgradeBackupGate(ctx, cluster, plan, upgradePendingObserved(cluster))
	if err != nil {
		t.Fatalf("gate: %v", err)
	}
	if handled {
		t.Fatal("expected the gate to proceed when backupBeforeUpgrade is disabled")
	}
}

func TestUpgradeBackupGateCreatesThenProceeds(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Backup = &mysqlv1alpha1.BackupConfiguration{
		ObjectStore: &mysqlv1alpha1.S3ObjectStore{Bucket: "b", Endpoint: "http://s3"},
	}
	scheme := testScheme(t)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}, &mysqlv1alpha1.Backup{}).
			WithObjects(cluster).Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	plan := clusterPlan{ServerVersion: "8.4.0"}
	observed := upgradePendingObserved(cluster)

	// First pass: no backup yet -> create it and block.
	_, err, handled := r.reconcileUpgradeBackupGate(ctx, cluster, plan, observed)
	if err != nil {
		t.Fatalf("gate (create): %v", err)
	}
	if !handled {
		t.Fatal("expected the gate to block while the backup is created")
	}

	backupName := cluster.Name + "-preupgrade-8-4"
	backup := &mysqlv1alpha1.Backup{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: backupName}, backup); err != nil {
		t.Fatalf("expected pre-upgrade backup %q to be created: %v", backupName, err)
	}
	if backup.Spec.Cluster.Name != cluster.Name {
		t.Errorf("backup cluster ref = %q, want %q", backup.Spec.Cluster.Name, cluster.Name)
	}

	// Mark it completed and re-run: the gate should proceed.
	backup.Status.Phase = mysqlv1alpha1.BackupPhaseCompleted
	if err := r.Status().Update(ctx, backup); err != nil {
		t.Fatalf("updating backup status: %v", err)
	}
	_, err, handled = r.reconcileUpgradeBackupGate(ctx, cluster, plan, observed)
	if err != nil {
		t.Fatalf("gate (completed): %v", err)
	}
	if handled {
		t.Fatal("expected the gate to proceed once the backup completed")
	}
}
