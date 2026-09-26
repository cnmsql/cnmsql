package controller

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
