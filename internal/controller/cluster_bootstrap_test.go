package controller

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

func TestPVCBootstrapped(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		annotations map[string]string
		want        bool
	}{
		{"legacy volume without annotation", nil, true},
		{"initializing", map[string]string{pvcStatusAnnotation: pvcStatusInitializing}, false},
		{"ready", map[string]string{pvcStatusAnnotation: pvcStatusReady}, true},
		{"unknown value", map[string]string{pvcStatusAnnotation: "detached"}, false},
	} {
		pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Annotations: tc.annotations}}
		if got := pvcBootstrapped(pvc); got != tc.want {
			t.Errorf("%s: pvcBootstrapped = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestEnsurePVCMarksNewVolumeInitializing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	inst := testPlan().instanceFor(cluster, 1)

	if _, err := r.ensurePVC(ctx, cluster, inst); err != nil {
		t.Fatal(err)
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.PVCName}, pvc); err != nil {
		t.Fatal(err)
	}
	if got := pvc.Annotations[pvcStatusAnnotation]; got != pvcStatusInitializing {
		t.Fatalf("new PVC %s = %q, want %q", pvcStatusAnnotation, got, pvcStatusInitializing)
	}
}

func TestEnsurePVCBackfillsLegacyVolume(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	scheme := testScheme(t)
	inst := testPlan().instanceFor(cluster, 1)
	legacy := instancePVC(cluster, inst.PVCName)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, legacy).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	if _, err := r.ensurePVC(ctx, cluster, inst); err != nil {
		t.Fatal(err)
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.PVCName}, pvc); err != nil {
		t.Fatal(err)
	}
	if got := pvc.Annotations[pvcStatusAnnotation]; got != pvcStatusReady {
		t.Fatalf("legacy PVC %s = %q, want %q", pvcStatusAnnotation, got, pvcStatusReady)
	}
}

func TestEnsurePVCKeepsInitializingVolume(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	scheme := testScheme(t)
	inst := testPlan().instanceFor(cluster, 1)
	pvc := instancePVC(cluster, inst.PVCName)
	pvc.Annotations = map[string]string{pvcStatusAnnotation: pvcStatusInitializing}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, pvc).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	if _, err := r.ensurePVC(ctx, cluster, inst); err != nil {
		t.Fatal(err)
	}
	got := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.PVCName}, got); err != nil {
		t.Fatal(err)
	}
	if got.Annotations[pvcStatusAnnotation] != pvcStatusInitializing {
		t.Fatalf("ensurePVC rewrote an initializing volume to %q", got.Annotations[pvcStatusAnnotation])
	}
}

func TestBootstrapModeFor(t *testing.T) {
	t.Parallel()
	r := &ClusterReconciler{}
	async := baseCluster()
	async.Spec.Instances = 3
	gr := grCluster(&mysqlv1alpha1.GroupReplicationStatus{GroupName: "g"})
	gr.Spec.Instances = 3
	plain := testPlan()
	plain.Instances = 3
	recovery := plain
	recovery.Recovery = &recoveryPlan{Bucket: "bkt"}
	imported := plain
	imported.Import = &importPlan{Bucket: "bkt"}

	for _, tc := range []struct {
		name    string
		cluster *mysqlv1alpha1.Cluster
		plan    clusterPlan
		ordinal int
		want    bootstrapMode
	}{
		{"primary initdb", async, plain, 1, bootstrapModeInitDB},
		{"primary restore", async, recovery, 1, bootstrapModeRestore},
		{"primary import", async, imported, 1, bootstrapModeImport},
		{"async replica clones", async, recovery, 2, bootstrapModeJoin},
		{"group replication secondary initialises", gr, recovery, 2, bootstrapModeInitDB},
	} {
		inst := tc.plan.instanceFor(tc.cluster, tc.ordinal)
		if got := r.bootstrapModeFor(tc.cluster, tc.plan, inst); got != tc.want {
			t.Errorf("%s: mode = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func restoreTestFixture(t *testing.T) (*ClusterReconciler, *mysqlv1alpha1.Cluster, clusterPlan, instancePlan, *corev1.PersistentVolumeClaim) {
	t.Helper()
	cluster := baseBackupCluster()
	cluster.Status.CurrentPrimary = ""
	cluster.Spec.Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
	}
	cluster.Spec.Affinity.NodeSelector = map[string]string{"pool": "db"}
	cluster.Spec.Affinity.Tolerations = []corev1.Toleration{{Key: "db"}}
	plan := testPlan()
	plan.ClusterName = cluster.Name
	plan.Recovery = &recoveryPlan{
		Bucket: "bkt", ArchiveKey: "a/backup.xbstream", MetadataKey: "a/metadata.json",
		StoreEnv: []corev1.EnvVar{{Name: objectstore.EnvBucket, Value: "bkt"}},
	}
	inst := plan.instanceFor(cluster, 1)
	pvc := instancePVC(cluster, inst.PVCName)
	pvc.UID = "uid-1"
	return &ClusterReconciler{Scheme: testScheme(t)}, cluster, plan, inst, pvc
}

func containerNames(cs []corev1.Container) []string {
	names := make([]string, 0, len(cs))
	for _, c := range cs {
		names = append(names, c.Name)
	}
	return names
}

func TestBootstrapJobRestore(t *testing.T) {
	t.Parallel()
	r, cluster, plan, inst, pvc := restoreTestFixture(t)
	deadline := metav1.Duration{Duration: 2 * time.Hour}
	cluster.Spec.Backup.JobTemplate = &mysqlv1alpha1.BackupJobTemplate{
		ActiveDeadline:    &deadline,
		Resources:         corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("4Gi")}},
		PriorityClassName: "restore",
		Tolerations:       []corev1.Toleration{{Key: "restore"}},
		NodeSelector:      map[string]string{"pool": "backup"},
		Labels:            map[string]string{"team": "db"},
	}

	job, err := r.bootstrapJob(cluster, plan, inst, bootstrapModeRestore, pvc)
	if err != nil {
		t.Fatal(err)
	}
	if job.Name != inst.Name+"-restore" {
		t.Fatalf("job name = %q", job.Name)
	}
	if job.Annotations[bootstrapPVCUIDAnnotation] != "uid-1" || job.Annotations[bootstrapSpecHashAnnotation] == "" {
		t.Fatalf("job annotations = %v", job.Annotations)
	}
	if job.Labels[clusterLabel] != cluster.Name || job.Labels["team"] != "db" {
		t.Fatalf("job labels = %v", job.Labels)
	}
	if ref := metav1.GetControllerOf(job); ref == nil || ref.Name != cluster.Name {
		t.Fatalf("job controller = %v, want the Cluster", ref)
	}
	if *job.Spec.BackoffLimit != bootstrapJobBackoffLimit || *job.Spec.ActiveDeadlineSeconds != 7200 {
		t.Fatalf("backoff/deadline = %d/%d", *job.Spec.BackoffLimit, *job.Spec.ActiveDeadlineSeconds)
	}

	pod := job.Spec.Template
	for _, key := range []string{clusterLabel, instanceLabel, roleLabel, routableLabel, podMonitorClusterLabel} {
		if _, ok := pod.Labels[key]; ok {
			t.Fatalf("bootstrap Pod carries instance label %q: %v", key, pod.Labels)
		}
	}
	if pod.Labels[bootstrapInstanceLabel] != inst.Name || pod.Labels[bootstrapModeLabel] != "restore" {
		t.Fatalf("pod labels = %v", pod.Labels)
	}
	spec := pod.Spec
	if spec.RestartPolicy != corev1.RestartPolicyNever || spec.ServiceAccountName != inst.Name+"-instance" {
		t.Fatalf("restartPolicy/serviceAccount = %s/%s", spec.RestartPolicy, spec.ServiceAccountName)
	}
	if got := containerNames(spec.InitContainers); !slices.Equal(got, []string{"bootstrap-controller"}) {
		t.Fatalf("init containers = %v", got)
	}
	main := spec.Containers[0]
	args := strings.Join(main.Args, " ")
	if main.Name != "restore" || !strings.Contains(args, "instance restore") || !strings.Contains(args, "--bucket=bkt") {
		t.Fatalf("main container %q args %q", main.Name, args)
	}
	if !slices.ContainsFunc(main.Env, func(e corev1.EnvVar) bool { return e.Name == objectstore.EnvBucket }) {
		t.Fatal("restore container has no object-store env")
	}
	if !slices.ContainsFunc(spec.Volumes, func(v corev1.Volume) bool {
		return v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == inst.PVCName
	}) {
		t.Fatal("job does not mount the instance PVC")
	}
	// Scheduling follows the instance (B8): the template's nodeSelector must not
	// pull the volume onto a node the instance Pod cannot use.
	if spec.NodeSelector["pool"] != "db" {
		t.Fatalf("nodeSelector = %v, want the instance's", spec.NodeSelector)
	}
	keys := make([]string, 0, len(spec.Tolerations))
	for _, tol := range spec.Tolerations {
		keys = append(keys, tol.Key)
	}
	if !slices.Equal(keys, []string{"db", "restore"}) {
		t.Fatalf("tolerations = %v, want instance then template", keys)
	}
	if spec.PriorityClassName != "restore" {
		t.Fatalf("priorityClassName = %q", spec.PriorityClassName)
	}
	if got := main.Resources.Limits.Memory().String(); got != "4Gi" {
		t.Fatalf("restore memory limit = %s, want the template's 4Gi", got)
	}
}

func TestBootstrapJobDefaultsToInstanceResources(t *testing.T) {
	t.Parallel()
	r, cluster, plan, inst, pvc := restoreTestFixture(t)
	job, err := r.bootstrapJob(cluster, plan, inst, bootstrapModeRestore, pvc)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range append(job.Spec.Template.Spec.InitContainers, job.Spec.Template.Spec.Containers...) {
		if got := c.Resources.Limits.Memory().String(); got != "1Gi" {
			t.Fatalf("container %s memory limit = %s, want the instance's 1Gi", c.Name, got)
		}
	}
}

func TestBootstrapJobImportRunsInitDBFirst(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	plan := testPlan()
	plan.ClusterName = cluster.Name
	plan.Import = &importPlan{Bucket: "bkt", DumpKey: "d/dump.sql.zst", ManifestKey: "d/logical.json"}
	inst := plan.instanceFor(cluster, 1)
	pvc := instancePVC(cluster, inst.PVCName)
	r := &ClusterReconciler{Scheme: testScheme(t)}

	job, err := r.bootstrapJob(cluster, plan, inst, bootstrapModeImport, pvc)
	if err != nil {
		t.Fatal(err)
	}
	spec := job.Spec.Template.Spec
	if got := containerNames(spec.InitContainers); !slices.Equal(got, []string{"bootstrap-controller", "initdb"}) {
		t.Fatalf("init containers = %v", got)
	}
	if got := strings.Join(spec.InitContainers[1].Args, " "); !strings.Contains(got, "instance initdb") || !strings.Contains(got, "--database="+appName) {
		t.Fatalf("initdb args = %q", got)
	}
	if got := strings.Join(spec.Containers[0].Args, " "); spec.Containers[0].Name != "import" || !strings.Contains(got, "--dump-key=d/dump.sql.zst") {
		t.Fatalf("import container %q args %q", spec.Containers[0].Name, got)
	}
}

func TestBootstrapJobGroupReplicationSecondaryHasNoSchema(t *testing.T) {
	t.Parallel()
	cluster := grCluster(&mysqlv1alpha1.GroupReplicationStatus{GroupName: "g"})
	cluster.Spec.Instances = 3
	plan := testPlan()
	plan.Instances = 3
	inst := plan.instanceFor(cluster, 2)
	r := &ClusterReconciler{Scheme: testScheme(t)}

	job, err := r.bootstrapJob(cluster, plan, inst, bootstrapModeInitDB, instancePVC(cluster, inst.PVCName))
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Join(job.Spec.Template.Spec.Containers[0].Args, " ")
	if !strings.Contains(args, "--group-replication") || strings.Contains(args, "--database=") {
		t.Fatalf("GR secondary initdb args = %q", args)
	}
}

func TestBootstrapJobHashFollowsSpec(t *testing.T) {
	t.Parallel()
	r, cluster, plan, inst, pvc := restoreTestFixture(t)
	first, err := r.bootstrapJob(cluster, plan, inst, bootstrapModeRestore, pvc)
	if err != nil {
		t.Fatal(err)
	}
	again, err := r.bootstrapJob(cluster, plan, inst, bootstrapModeRestore, pvc)
	if err != nil {
		t.Fatal(err)
	}
	if first.Annotations[bootstrapSpecHashAnnotation] != again.Annotations[bootstrapSpecHashAnnotation] {
		t.Fatal("spec hash is not stable across identical builds")
	}
	plan.Recovery.ArchiveKey = "b/backup.xbstream"
	changed, err := r.bootstrapJob(cluster, plan, inst, bootstrapModeRestore, pvc)
	if err != nil {
		t.Fatal(err)
	}
	if changed.Annotations[bootstrapSpecHashAnnotation] == first.Annotations[bootstrapSpecHashAnnotation] {
		t.Fatal("spec hash did not change with the restore source")
	}
}

// bootstrapFixture returns a reconciler over a fake client holding cluster and
// objs. The fake client treats Jobs as a status subresource, so finishJob
// writes their status through the status writer.
func bootstrapFixture(t *testing.T, cluster *mysqlv1alpha1.Cluster, objs ...client.Object) (*ClusterReconciler, client.Client) {
	t.Helper()
	scheme := testScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
		WithObjects(append([]client.Object{cluster}, objs...)...).
		Build()
	return &ClusterReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(20)}, c
}

func initializingPVC(cluster *mysqlv1alpha1.Cluster, name, uid string) *corev1.PersistentVolumeClaim {
	pvc := instancePVC(cluster, name)
	pvc.UID = types.UID(uid)
	pvc.Annotations = map[string]string{pvcStatusAnnotation: pvcStatusInitializing}
	return pvc
}

func finishJob(t *testing.T, ctx context.Context, c client.Client, cluster *mysqlv1alpha1.Cluster, name string, cond batchv1.JobConditionType) {
	t.Helper()
	job := &batchv1.Job{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, job); err != nil {
		t.Fatal(err)
	}
	job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{
		Type: cond, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded", Message: "Job was active longer than specified deadline",
	})
	if err := c.Status().Update(ctx, job); err != nil {
		t.Fatal(err)
	}
}

func jobExists(t *testing.T, ctx context.Context, c client.Client, cluster *mysqlv1alpha1.Cluster, name string) bool {
	t.Helper()
	err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, &batchv1.Job{})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
	return err == nil
}

func TestEnsureBootstrappedCreatesJobForNewVolume(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	plan := testPlan()
	inst := plan.instanceFor(cluster, 1)
	r, c := bootstrapFixture(t, cluster, initializingPVC(cluster, inst.PVCName, "uid-1"))

	done, err := r.ensureBootstrapped(ctx, cluster, plan, inst)
	if err != nil || done {
		t.Fatalf("ensureBootstrapped = %v, %v; want false, nil", done, err)
	}
	job := &batchv1.Job{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.Name + "-initdb"}, job); err != nil {
		t.Fatalf("initdb Job not created: %v", err)
	}
	if job.Annotations[bootstrapPVCUIDAnnotation] != "uid-1" {
		t.Fatalf("job pvc-uid = %q", job.Annotations[bootstrapPVCUIDAnnotation])
	}
}

func TestEnsureBootstrappedMarksVolumeAndDeletesJobOnSuccess(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	plan := testPlan()
	inst := plan.instanceFor(cluster, 1)
	r, c := bootstrapFixture(t, cluster, initializingPVC(cluster, inst.PVCName, "uid-1"))

	if _, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil {
		t.Fatal(err)
	}
	finishJob(t, ctx, c, cluster, inst.Name+"-initdb", batchv1.JobComplete)

	done, err := r.ensureBootstrapped(ctx, cluster, plan, inst)
	if err != nil || done {
		t.Fatalf("pass after success = %v, %v; want false (the Job is still being deleted)", done, err)
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.PVCName}, pvc); err != nil {
		t.Fatal(err)
	}
	if !pvcBootstrapped(pvc) {
		t.Fatal("volume not marked bootstrapped after the Job succeeded")
	}
	if jobExists(t, ctx, c, cluster, inst.Name+"-initdb") {
		t.Fatal("succeeded Job not deleted")
	}
	if done, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil || !done {
		t.Fatalf("pass after deletion = %v, %v; want true", done, err)
	}
}

func TestEnsureBootstrappedKeepsFailedJobWithSameSpec(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	plan := testPlan()
	inst := plan.instanceFor(cluster, 1)
	r, c := bootstrapFixture(t, cluster, initializingPVC(cluster, inst.PVCName, "uid-1"))

	if _, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil {
		t.Fatal(err)
	}
	finishJob(t, ctx, c, cluster, inst.Name+"-initdb", batchv1.JobFailed)
	for range 3 {
		if done, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil || done {
			t.Fatalf("ensureBootstrapped = %v, %v; want false, nil", done, err)
		}
	}
	if !jobExists(t, ctx, c, cluster, inst.Name+"-initdb") {
		t.Fatal("failed Job with an unchanged spec was deleted")
	}
}

func TestEnsureBootstrappedReplacesFailedJobWhenSpecChanges(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	plan := testPlan()
	inst := plan.instanceFor(cluster, 1)
	r, c := bootstrapFixture(t, cluster, initializingPVC(cluster, inst.PVCName, "uid-1"))

	if _, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil {
		t.Fatal(err)
	}
	old := &batchv1.Job{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.Name + "-initdb"}, old); err != nil {
		t.Fatal(err)
	}
	finishJob(t, ctx, c, cluster, inst.Name+"-initdb", batchv1.JobFailed)

	plan.Image = "ghcr.io/cnmsql/cnmsql-instance:8.0.99"
	if _, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil {
		t.Fatal(err)
	}
	if jobExists(t, ctx, c, cluster, inst.Name+"-initdb") {
		t.Fatal("failed Job not deleted after the spec changed")
	}
	if _, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil {
		t.Fatal(err)
	}
	fresh := &batchv1.Job{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.Name + "-initdb"}, fresh); err != nil {
		t.Fatalf("replacement Job not created: %v", err)
	}
	if fresh.Annotations[bootstrapSpecHashAnnotation] == old.Annotations[bootstrapSpecHashAnnotation] {
		t.Fatal("replacement Job carries the old spec hash")
	}
}

func TestEnsureBootstrappedDeletesJobForOtherVolume(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 2
	plan := testPlan()
	plan.Instances = 2
	inst := plan.instanceFor(cluster, 2)
	r, c := bootstrapFixture(t, cluster, initializingPVC(cluster, inst.PVCName, "uid-old"))

	if _, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil {
		t.Fatal(err)
	}
	finishJob(t, ctx, c, cluster, inst.Name+"-join", batchv1.JobComplete)

	// The instance is re-initialised: a new, empty PVC with the same name.
	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.PVCName}, pvc); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, pvc); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, initializingPVC(cluster, inst.PVCName, "uid-new")); err != nil {
		t.Fatal(err)
	}

	if _, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.PVCName}, pvc); err != nil {
		t.Fatal(err)
	}
	if pvcBootstrapped(pvc) {
		t.Fatal("a Job for the previous volume marked the new volume bootstrapped")
	}
	if jobExists(t, ctx, c, cluster, inst.Name+"-join") {
		t.Fatal("Job for the previous volume not deleted")
	}
}

func TestEnsureBootstrappedWaitsForInstancePod(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	plan := testPlan()
	inst := plan.instanceFor(cluster, 1)
	r, c := bootstrapFixture(t, cluster,
		initializingPVC(cluster, inst.PVCName, "uid-1"),
		readyPod(cluster, inst.Name, rolePrimary))

	if done, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil || done {
		t.Fatalf("ensureBootstrapped = %v, %v; want false, nil", done, err)
	}
	if jobExists(t, ctx, c, cluster, inst.Name+"-initdb") {
		t.Fatal("bootstrap Job created while the instance Pod exists")
	}
}

func TestEnsureBootstrappedRemovesLeftoverJobBeforePod(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	plan := testPlan()
	inst := plan.instanceFor(cluster, 1)
	pvc := initializingPVC(cluster, inst.PVCName, "uid-1")
	pvc.Annotations[pvcStatusAnnotation] = pvcStatusReady
	leftover := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: inst.Name + "-initdb", Namespace: cluster.Namespace,
		Labels: map[string]string{clusterLabel: cluster.Name, bootstrapInstanceLabel: inst.Name},
	}}
	r, c := bootstrapFixture(t, cluster, pvc, leftover)

	if done, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil || done {
		t.Fatalf("first pass = %v, %v; want false while the leftover Job exists", done, err)
	}
	if jobExists(t, ctx, c, cluster, inst.Name+"-initdb") {
		t.Fatal("leftover Job not deleted")
	}
	if done, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil || !done {
		t.Fatalf("second pass = %v, %v; want true", done, err)
	}
}

func TestEnsureBootstrappedRefusesPrimaryOnEstablishedCluster(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	now := metav1.Now()
	cluster.Status.EstablishedAt = &now
	plan := testPlan()
	inst := plan.instanceFor(cluster, 1)
	r, c := bootstrapFixture(t, cluster, initializingPVC(cluster, inst.PVCName, "uid-1"))

	if done, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil || done {
		t.Fatalf("ensureBootstrapped = %v, %v; want false, nil", done, err)
	}
	if jobExists(t, ctx, c, cluster, inst.Name+"-initdb") {
		t.Fatal("established cluster got a Job that would re-initialise its primary")
	}
}

func recoveryCluster() *mysqlv1alpha1.Cluster {
	cluster := baseBackupCluster()
	cluster.Status.CurrentPrimary = ""
	cluster.Spec.Bootstrap = &mysqlv1alpha1.BootstrapConfiguration{
		Recovery: &mysqlv1alpha1.BootstrapRecovery{
			Backup: &mysqlv1alpha1.LocalObjectReference{Name: "backup-sample"},
		},
	}
	return cluster
}

func TestBuildPlanSkipsRecoveryOnceBootstrapped(t *testing.T) {
	t.Parallel()
	cluster := recoveryCluster()
	pvc := instancePVC(cluster, instanceName(cluster, 1))
	pvc.Annotations = map[string]string{pvcStatusAnnotation: pvcStatusReady}
	// No Backup object: it was deleted after the restore.
	r, _ := bootstrapFixture(t, cluster, pvc)

	plan, err := r.buildPlan(context.Background(), cluster)
	if err != nil {
		t.Fatalf("buildPlan failed although the primary is bootstrapped: %v", err)
	}
	if plan.Recovery != nil {
		t.Fatal("plan.Recovery resolved after the primary was bootstrapped")
	}
}

func TestBuildPlanSkipsRecoveryForLegacyVolume(t *testing.T) {
	t.Parallel()
	cluster := recoveryCluster()
	r, _ := bootstrapFixture(t, cluster, instancePVC(cluster, instanceName(cluster, 1)))

	if _, err := r.buildPlan(context.Background(), cluster); err != nil {
		t.Fatalf("buildPlan failed on a pre-031 volume whose Backup is gone: %v", err)
	}
}

func TestBuildPlanSkipsRecoveryOnEstablishedCluster(t *testing.T) {
	t.Parallel()
	cluster := recoveryCluster()
	now := metav1.Now()
	cluster.Status.EstablishedAt = &now
	r, _ := bootstrapFixture(t, cluster)

	if _, err := r.buildPlan(context.Background(), cluster); err != nil {
		t.Fatalf("buildPlan failed on an established cluster whose Backup is gone: %v", err)
	}
}

func TestBuildPlanResolvesRecoveryWhileInitializing(t *testing.T) {
	t.Parallel()
	cluster := recoveryCluster()
	backup := baseBackup()
	backup.Status = mysqlv1alpha1.BackupStatus{Phase: mysqlv1alpha1.BackupPhaseCompleted, BackupID: testBackupID}
	r, _ := bootstrapFixture(t, cluster, backup, initializingPVC(cluster, instanceName(cluster, 1), "uid-1"))

	plan, err := r.buildPlan(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Recovery == nil {
		t.Fatal("plan.Recovery not resolved while the primary volume is initializing")
	}
}

func TestObserveBootstrapJobs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 2
	plan := testPlan()
	plan.Instances = 2
	primary, replica := plan.instanceFor(cluster, 1), plan.instanceFor(cluster, 2)
	r, c := bootstrapFixture(t, cluster,
		initializingPVC(cluster, primary.PVCName, "uid-1"),
		initializingPVC(cluster, replica.PVCName, "uid-2"))
	for _, inst := range []instancePlan{primary, replica} {
		if _, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil {
			t.Fatal(err)
		}
	}
	finishJob(t, ctx, c, cluster, primary.Name+"-initdb", batchv1.JobFailed)
	finishJob(t, ctx, c, cluster, replica.Name+"-join", batchv1.JobComplete)

	states, err := r.observeBootstrapJobs(ctx, cluster)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 1 {
		t.Fatalf("states = %+v, want only the failed initdb (completed Jobs are skipped)", states)
	}
	got := states[0]
	if got.Instance != primary.Name || got.Mode != bootstrapModeInitDB || !got.Failed || got.Reason != "DeadlineExceeded" {
		t.Fatalf("state = %+v", got)
	}
}

// markVolumeBootstrapped creates or marks inst's PVC as bootstrapped, so a test
// about Pod handling skips the bootstrap Job.
func markVolumeBootstrapped(t *testing.T, ctx context.Context, c client.Client, cluster *mysqlv1alpha1.Cluster, inst instancePlan) {
	t.Helper()
	pvc := &corev1.PersistentVolumeClaim{}
	err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.PVCName}, pvc)
	if apierrors.IsNotFound(err) {
		pvc = instancePVC(cluster, inst.PVCName)
		pvc.Annotations = map[string]string{pvcStatusAnnotation: pvcStatusReady}
		if err := c.Create(ctx, pvc); err != nil {
			t.Fatal(err)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	before := pvc.DeepCopy()
	if pvc.Annotations == nil {
		pvc.Annotations = map[string]string{}
	}
	pvc.Annotations[pvcStatusAnnotation] = pvcStatusReady
	if err := c.Patch(ctx, pvc, client.MergeFrom(before)); err != nil {
		t.Fatal(err)
	}
}
