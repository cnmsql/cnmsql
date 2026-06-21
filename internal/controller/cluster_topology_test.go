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
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/groupreplication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
)

func TestRoleLabelsPrimaryVsReplica(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	plan := testPlan()
	plan.Instances = 2

	if got := roleOf(plan.instanceFor(cluster, 1)); got != rolePrimary {
		t.Fatalf("instance 1 role = %q, want primary", got)
	}
	if got := roleOf(plan.instanceFor(cluster, 2)); got != roleReplica {
		t.Fatalf("instance 2 role = %q, want replica", got)
	}
	labels := labelsFor(cluster, "demo-2", roleReplica)
	if labels[roleLabel] != roleReplica {
		t.Fatalf("replica label = %q", labels[roleLabel])
	}
}

func TestEnsureDefaultServicesSelectorsAndDisable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	scheme := testScheme(t)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme: scheme,
	}
	plan := testPlan()

	if err := reconciler.ensureDefaultServices(ctx, cluster, plan); err != nil {
		t.Fatal(err)
	}
	rw := getService(t, ctx, reconciler, "demo-rw")
	if rw.Spec.Selector[roleLabel] != rolePrimary {
		t.Fatalf("rw selector = %#v, want role=primary", rw.Spec.Selector)
	}
	if rw.Spec.PublishNotReadyAddresses {
		t.Fatal("rw must not publish not-ready addresses")
	}
	ro := getService(t, ctx, reconciler, "demo-ro")
	if ro.Spec.Selector[roleLabel] != roleReplica {
		t.Fatalf("ro selector = %#v, want role=replica", ro.Spec.Selector)
	}
	r := getService(t, ctx, reconciler, "demo-r")
	if _, hasRole := r.Spec.Selector[roleLabel]; hasRole {
		t.Fatalf("r selector should not pin a role: %#v", r.Spec.Selector)
	}

	// Disabling ro deletes it on the next reconcile.
	plan.DisabledServices = map[mysqlv1alpha1.ServiceSelectorType]bool{mysqlv1alpha1.ServiceSelectorTypeRO: true}
	if err := reconciler.ensureDefaultServices(ctx, cluster, plan); err != nil {
		t.Fatal(err)
	}
	err := reconciler.Get(ctx, types.NamespacedName{Namespace: "default", Name: "demo-ro"}, &corev1.Service{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("disabled ro service get = %v, want not found", err)
	}
}

func TestScaleDownRemovesPodRetainsPVC(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	scheme := testScheme(t)
	// Three instances exist; desired count is 1.
	objects := []*corev1.Pod{}
	for i := 1; i <= 3; i++ {
		objects = append(objects, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:      instanceName(cluster, i),
			Namespace: cluster.Namespace,
			Labels:    map[string]string{clusterLabel: cluster.Name},
		}})
	}
	pvc3 := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "demo-3", Namespace: cluster.Namespace}}
	builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, pvc3)
	for _, p := range objects {
		builder = builder.WithObjects(p)
	}
	reconciler := &ClusterReconciler{Client: builder.Build(), Scheme: scheme}

	plan := testPlan() // Instances == 1
	if err := reconciler.scaleDownReplicas(ctx, cluster, plan); err != nil {
		t.Fatal(err)
	}

	// Replica pods removed, primary kept.
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: "default", Name: "demo-1"}, &corev1.Pod{}); err != nil {
		t.Fatalf("primary pod should be kept: %v", err)
	}
	for _, name := range []string{"demo-2", "demo-3"} {
		err := reconciler.Get(ctx, types.NamespacedName{Namespace: "default", Name: name}, &corev1.Pod{})
		if !apierrors.IsNotFound(err) {
			t.Fatalf("pod %s get = %v, want removed", name, err)
		}
	}
	// PVC retained per the M4 policy.
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: "default", Name: "demo-3"}, &corev1.PersistentVolumeClaim{}); err != nil {
		t.Fatalf("replica PVC should be retained: %v", err)
	}
}

func TestScaleDownKeepsCurrentPrimaryByName(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Status.CurrentPrimary = "demo-3"
	scheme := testScheme(t)
	objects := []client.Object{cluster}
	for i := 1; i <= 3; i++ {
		name := instanceName(cluster, i)
		objects = append(objects, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    map[string]string{clusterLabel: cluster.Name},
		}})
	}
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build(),
		Scheme: scheme,
	}
	plan := testPlan()
	plan.Instances = 2
	plan.PrimaryName = "demo-3"

	if err := reconciler.scaleDownReplicas(ctx, cluster, plan); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: "demo-3"}, &corev1.Pod{}); err != nil {
		t.Fatalf("current primary should be kept: %v", err)
	}
}

// TestReconcileInstancesGuardsReplicaOnUnhealthyPrimary checks that a brand-new
// replica is not created while the primary is not OK: it would be cloned from a
// primary that is unreachable or not acting as primary. Once the primary is OK
// the replica is created.
func TestReconcileInstancesGuardsReplicaOnUnhealthyPrimary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	scheme := testScheme(t)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme: scheme,
	}
	plan := testPlan()
	plan.Instances = 2

	// Bootstrap the primary the way the controller does so its Pod carries the
	// expected template hash; ensureInstance then treats it as stable instead of
	// rolling it. Mark it Ready so the previous-instance ramp-up gate passes.
	primary := plan.instanceFor(cluster, 1)
	if _, err := reconciler.ensureInstance(ctx, cluster, plan, primary, true); err != nil {
		t.Fatal(err)
	}
	markPodReady(t, ctx, reconciler, primary.Name)

	// Primary Pod is Ready, but the control API reports it as a replica (not OK as
	// a primary): primaryHealthy is false, so the new replica must be deferred.
	observed := observedCluster{
		Plan:        plan,
		PrimaryName: primary.Name,
		StatusByInstance: map[string]*webserver.Status{
			primary.Name: {Role: webserver.RoleReplica, IsReady: true},
		},
	}

	provisioned, err := reconciler.reconcileInstances(ctx, cluster, plan, observed)
	if err != nil {
		t.Fatal(err)
	}
	if provisioned {
		t.Fatal("reconcileInstances reported provisioned while deferring replica creation")
	}
	replicaKey := types.NamespacedName{Namespace: cluster.Namespace, Name: instanceName(cluster, 2)}
	if err := reconciler.Get(ctx, replicaKey, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("replica pod get = %v, want not created while primary unhealthy", err)
	}

	// Once the primary reports as a healthy primary, the guard lets the replica
	// through and its Pod is created.
	observed.StatusByInstance[primary.Name] = &webserver.Status{Role: webserver.RolePrimary, IsReady: true}
	if _, err := reconciler.reconcileInstances(ctx, cluster, plan, observed); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.Get(ctx, replicaKey, &corev1.Pod{}); err != nil {
		t.Fatalf("replica pod should be created once primary is healthy: %v", err)
	}
}

// TestReconcileInstancesRollsReplicasBeforePrimary verifies that a cluster-wide
// template change (here a restart-token bump) rolls one replica at a time and
// never the primary in the same pass: it deletes only the first replica's Pod and
// stops, reporting not-provisioned so the caller requeues. The pre-pass
// observation still shows every instance healthy, so without the stop-on-roll
// signal the loop would march on and delete every Pod at once, and without the
// primary-last ordering it would take the primary down first.
func TestReconcileInstancesRollsReplicasBeforePrimary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	scheme := testScheme(t)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme: scheme,
	}
	plan := testPlan()
	plan.Instances = 3
	plan.PrimaryName = instanceName(cluster, 1)

	// Bootstrap all three instances with the current template hash and mark them
	// Ready so they form a fully-provisioned cluster before the roll.
	observed := observedCluster{
		Plan:             plan,
		PrimaryName:      plan.PrimaryName,
		Ready:            true,
		StatusByInstance: map[string]*webserver.Status{},
	}
	for i := 1; i <= plan.Instances; i++ {
		inst := plan.instanceFor(cluster, i)
		if _, err := reconciler.ensureInstance(ctx, cluster, plan, inst, true); err != nil {
			t.Fatal(err)
		}
		markPodReady(t, ctx, reconciler, inst.Name)
		observed.StatusByInstance[inst.Name] = &webserver.Status{Role: webserver.RolePrimary, IsReady: true}
	}

	// A restart-token bump changes every instance's template hash.
	cluster.Annotations = map[string]string{restartAnnotation: "1"}

	provisioned, err := reconciler.reconcileInstances(ctx, cluster, plan, observed)
	if err != nil {
		t.Fatal(err)
	}
	if provisioned {
		t.Fatal("reconcileInstances reported provisioned while rolling a template change")
	}
	// The first replica (ordinal 2) is rolled; the primary (ordinal 1) and the
	// other replica must be untouched in this pass.
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: instanceName(cluster, 2)}, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("demo-2 (first replica) get = %v, want deleted (rolled)", err)
	}
	for _, ord := range []int{1, 3} {
		name := instanceName(cluster, ord)
		if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, &corev1.Pod{}); err != nil {
			t.Fatalf("%s should not be rolled before the replicas: %v", name, err)
		}
	}
}

// TestReconcileInstancesRollsPrimaryLast verifies that once every replica is
// up to date and the cluster is ready, the primary is the one that rolls.
func TestReconcileInstancesRollsPrimaryLast(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	scheme := testScheme(t)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme: scheme,
	}
	plan := testPlan()
	plan.Instances = 3
	plan.PrimaryName = instanceName(cluster, 1)

	observed := observedCluster{
		Plan:             plan,
		PrimaryName:      plan.PrimaryName,
		Ready:            true,
		StatusByInstance: map[string]*webserver.Status{},
	}
	markReady := func(name string) {
		markPodReady(t, ctx, reconciler, name)
		observed.StatusByInstance[name] = &webserver.Status{Role: webserver.RolePrimary, IsReady: true}
	}
	// Bootstrap the primary with the pre-change template hash, then bootstrap the
	// replicas with the post-change hash (restart token set). With the token set as
	// the desired state, only the primary is left needing a roll.
	primary := plan.instanceFor(cluster, 1)
	if _, err := reconciler.ensureInstance(ctx, cluster, plan, primary, true); err != nil {
		t.Fatal(err)
	}
	markReady(primary.Name)
	cluster.Annotations = map[string]string{restartAnnotation: "1"}
	for i := 2; i <= plan.Instances; i++ {
		inst := plan.instanceFor(cluster, i)
		if _, err := reconciler.ensureInstance(ctx, cluster, plan, inst, true); err != nil {
			t.Fatal(err)
		}
		markReady(inst.Name)
	}

	provisioned, err := reconciler.reconcileInstances(ctx, cluster, plan, observed)
	if err != nil {
		t.Fatal(err)
	}
	if provisioned {
		t.Fatal("reconcileInstances reported provisioned while rolling the primary")
	}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: instanceName(cluster, 1)}, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("demo-1 (primary) get = %v, want deleted (rolled last)", err)
	}
	for _, ord := range []int{2, 3} {
		name := instanceName(cluster, ord)
		if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, &corev1.Pod{}); err != nil {
			t.Fatalf("replica %s must stay up while the primary rolls: %v", name, err)
		}
	}
}

// TestReconcileInstancesRecreatesAllMembersAfterTotalOutage verifies that when
// every Pod is gone but the PVCs survive (a total outage), all members are
// recreated even though none is ONLINE — so the operator can observe every
// member's GTID and pick a re-bootstrap survivor. Gating the restart on the
// previous member being ONLINE would deadlock: no member can be ONLINE until
// enough members are back up.
func TestReconcileInstancesRecreatesAllMembersAfterTotalOutage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := grCluster(&mysqlv1alpha1.GroupReplicationStatus{GroupName: "group-uuid", Bootstrapped: true})
	scheme := testScheme(t)
	plan := testPlan()
	plan.Instances = 3
	plan.PrimaryName = instanceName(cluster, 1)

	// The PVCs survive the outage; all Pods are gone and the group has no quorum.
	objs := []client.Object{cluster}
	for i := 1; i <= plan.Instances; i++ {
		objs = append(objs, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Name:      plan.instanceFor(cluster, i).PVCName,
			Namespace: cluster.Namespace,
		}})
	}
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(),
		Scheme: scheme,
	}

	observed := observedCluster{Plan: plan, PrimaryName: plan.PrimaryName}

	if _, err := reconciler.reconcileInstances(ctx, cluster, plan, observed); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= plan.Instances; i++ {
		name := instanceName(cluster, i)
		if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, &corev1.Pod{}); err != nil {
			t.Fatalf("pod %s should be recreated after a total outage: %v", name, err)
		}
	}
}

// TestReconcileInstancesFinishesPartialTotalOutageRestart covers the real
// multi-reconcile sequence: members 1 and 2 have already been recreated but
// cannot become Ready before re-bootstrap, while member 3 is still missing.
// The NotReady member 2 must not prevent member 3 from coming up and reporting
// the final GTID required for safe survivor selection.
func TestReconcileInstancesFinishesPartialTotalOutageRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := grCluster(&mysqlv1alpha1.GroupReplicationStatus{
		GroupName:    "group-uuid",
		Bootstrapped: true,
		HasQuorum:    false,
	})
	cluster.Status.Phase = topology.PhaseBlocked
	scheme := testScheme(t)
	plan := testPlan()
	plan.Instances = 3
	plan.PrimaryName = instanceName(cluster, 1)

	objects := []client.Object{cluster}
	for i := 1; i <= plan.Instances; i++ {
		inst := plan.instanceFor(cluster, i)
		objects = append(objects, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Name:      inst.PVCName,
			Namespace: cluster.Namespace,
		}})
	}
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build(),
		Scheme: scheme,
	}
	for i := 1; i < plan.Instances; i++ {
		if _, err := reconciler.ensureInstance(ctx, cluster, plan, plan.instanceFor(cluster, i), true); err != nil {
			t.Fatal(err)
		}
	}

	observed := observedCluster{Plan: plan, PrimaryName: plan.PrimaryName}
	if _, err := reconciler.reconcileInstances(ctx, cluster, plan, observed); err != nil {
		t.Fatal(err)
	}
	member3 := types.NamespacedName{Namespace: cluster.Namespace, Name: instanceName(cluster, 3)}
	if err := reconciler.Get(ctx, member3, &corev1.Pod{}); err != nil {
		t.Fatalf("third persistent member should be recreated during guided recovery: %v", err)
	}
}

// TestReconcileInstancesGatesBrandNewMemberWithoutDonor verifies the gate still
// holds for a genuinely new member (no Pod, no PVC) when no donor is available:
// it must not be created until there is a healthy source to clone from.
func TestReconcileInstancesGatesBrandNewMemberWithoutDonor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := grCluster(&mysqlv1alpha1.GroupReplicationStatus{GroupName: "group-uuid", Bootstrapped: true})
	scheme := testScheme(t)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme: scheme,
	}
	plan := testPlan()
	plan.Instances = 2
	plan.PrimaryName = instanceName(cluster, 1)

	// Bootstrap the primary so it is fully ready, then drive reconcile with no
	// quorum so no donor is available for the brand-new replica (no PVC).
	primary := plan.instanceFor(cluster, 1)
	if _, err := reconciler.ensureInstance(ctx, cluster, plan, primary, true); err != nil {
		t.Fatal(err)
	}
	markPodReady(t, ctx, reconciler, primary.Name)
	observed := observedCluster{
		Plan:        plan,
		PrimaryName: primary.Name,
		StatusByInstance: map[string]*webserver.Status{
			primary.Name: onlineMemberStatus(primary.Name, "uuid-1", groupreplication.MemberRolePrimary),
		},
		GroupReplication: &mysqlv1alpha1.GroupReplicationStatus{HasQuorum: false},
	}

	if _, err := reconciler.reconcileInstances(ctx, cluster, plan, observed); err != nil {
		t.Fatal(err)
	}
	replicaKey := types.NamespacedName{Namespace: cluster.Namespace, Name: instanceName(cluster, 2)}
	if err := reconciler.Get(ctx, replicaKey, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("brand-new replica pod get = %v, want not created without a donor", err)
	}
}

// markPodReady flips the named Pod's Ready condition to True.
func markPodReady(t *testing.T, ctx context.Context, reconciler *ClusterReconciler, name string) {
	t.Helper()
	pod := &corev1.Pod{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: "default", Name: name}, pod); err != nil {
		t.Fatalf("get pod %s: %v", name, err)
	}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if err := reconciler.Status().Update(ctx, pod); err != nil {
		t.Fatalf("mark pod %s ready: %v", name, err)
	}
}

func getService(t *testing.T, ctx context.Context, reconciler *ClusterReconciler, name string) *corev1.Service {
	t.Helper()
	svc := &corev1.Service{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: "default", Name: name}, svc); err != nil {
		t.Fatalf("get service %s: %v", name, err)
	}
	return svc
}
