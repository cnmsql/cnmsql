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
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/version"
)

// importContainerName is the init container that loads a logical backup into
// the bootstrap primary, after initdb.
const importContainerName = "import"

const (
	// reasonImportSourceNotReady prefixes the phase reason of a cluster whose
	// import source is not usable yet: the Backup is still running, or the
	// object store could not be read. The reconcile is retried.
	reasonImportSourceNotReady = "ImportSourceNotReady"
	// reasonImportIncompatible prefixes the Blocked reason of a cluster that
	// cannot load the selected dump.
	reasonImportIncompatible = "ImportIncompatible"
	// reasonPhysicalBackupNotImportable prefixes the Blocked reason of a
	// cluster whose initdb.import.backup names a physical Backup.
	reasonPhysicalBackupNotImportable = "PhysicalBackupNotImportable"
)

// importPlan locates the logical backup the bootstrap primary loads.
type importPlan struct {
	Bucket      string
	DumpKey     string
	ManifestKey string
	// StoreEnv carries the cnmsql_S3_* environment the import command reads
	// the object store with.
	StoreEnv      []corev1.EnvVar
	Databases     []string
	PostImportSQL []string
}

// importNotReadyError marks an import source that can become usable without
// a spec change. The caller keeps the cluster provisioning and requeues,
// instead of blocking it until the next resync.
type importNotReadyError struct{ msg string }

func (e *importNotReadyError) Error() string { return reasonImportSourceNotReady + ": " + e.msg }

func importNotReady(format string, args ...any) error {
	return &importNotReadyError{msg: fmt.Sprintf(format, args...)}
}

// resolveImport locates the dump the bootstrap primary loads when
// spec.bootstrap.initdb.import is set, and checks that this cluster can load
// it. It returns nil when there is no import, and once the cluster is
// established: the import has run by then, and its container is kept out of
// the Pod template hash, so dropping it rolls nothing. Resolving it again
// would read a Backup and an object store the cluster no longer needs.
func (r *ClusterReconciler) resolveImport(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	serverVersion string,
) (*importPlan, error) {
	if cluster.Spec.Bootstrap == nil || cluster.Spec.Bootstrap.InitDB == nil ||
		cluster.Spec.Bootstrap.InitDB.Import == nil || cluster.Status.EstablishedAt != nil {
		return nil, nil
	}
	imp := cluster.Spec.Bootstrap.InitDB.Import

	var (
		store       *mysqlv1alpha1.S3ObjectStore
		dumpKey     string
		manifestKey string
		meta        *objectstore.LogicalBackupMetadata
		err         error
	)
	if imp.Source != "" {
		store, dumpKey, manifestKey, meta, err = r.resolveImportSource(ctx, cluster, imp)
	} else {
		store, dumpKey, manifestKey, meta, err = r.resolveImportBackup(ctx, cluster, imp)
	}
	if err != nil {
		return nil, err
	}
	if err := meta.CheckImportable(string(cluster.ResolvedFlavor()), imp.Databases); err != nil {
		return nil, fmt.Errorf("%s: %w", reasonImportIncompatible, err)
	}
	r.warnImportFromNewerSeries(cluster, meta.ServerVersion, serverVersion)

	return &importPlan{
		Bucket:      store.Bucket,
		DumpKey:     dumpKey,
		ManifestKey: manifestKey,
		StoreEnv: append(backupObjectStoreEnv(*store),
			corev1.EnvVar{Name: objectstore.EnvBucket, Value: store.Bucket},
			corev1.EnvVar{Name: objectstore.EnvPath, Value: store.Path},
		),
		Databases:     slices.Clone(imp.Databases),
		PostImportSQL: slices.Clone(imp.PostImportSQL),
	}, nil
}

// resolveImportBackup locates the dump of a logical Backup in this namespace.
func (r *ClusterReconciler) resolveImportBackup(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	imp *mysqlv1alpha1.BootstrapImport,
) (*mysqlv1alpha1.S3ObjectStore, string, string, *objectstore.LogicalBackupMetadata, error) {
	backup := &mysqlv1alpha1.Backup{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: imp.Backup.Name}, backup); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, "", "", nil, importNotReady("import backup %q does not exist", imp.Backup.Name)
		}
		return nil, "", "", nil, importNotReady("reading import backup %q: %v", imp.Backup.Name, err)
	}
	if backup.Spec.Method != mysqlv1alpha1.BackupMethodLogical {
		return nil, "", "", nil, fmt.Errorf("%s: import backup %q is a %s backup, and initdb.import loads a "+
			"logical backup; restore a physical backup with bootstrap.recovery instead",
			reasonPhysicalBackupNotImportable, backup.Name, backup.Spec.Method)
	}
	switch backup.Status.Phase {
	case mysqlv1alpha1.BackupPhaseCompleted:
	case mysqlv1alpha1.BackupPhaseFailed:
		return nil, "", "", nil, fmt.Errorf("%s: import backup %q failed", reasonImportIncompatible, backup.Name)
	default:
		return nil, "", "", nil, importNotReady("import backup %q is not completed yet", backup.Name)
	}
	if backup.Status.BackupID == "" {
		return nil, "", "", nil, fmt.Errorf("%s: import backup %q has no backupID", reasonImportIncompatible, backup.Name)
	}

	store, err := r.importObjectStore(ctx, cluster, backup)
	if err != nil {
		return nil, "", "", nil, err
	}
	keys, err := objectstore.BuildLogicalBackupKeys(*store, backup.Spec.Cluster.Name, backup.Name, backup.Status.BackupID)
	if err != nil {
		return nil, "", "", nil, fmt.Errorf("%s: %w", reasonImportIncompatible, err)
	}
	osClient, err := r.importStoreClient(ctx, cluster.Namespace, store)
	if err != nil {
		return nil, "", "", nil, err
	}
	var meta objectstore.LogicalBackupMetadata
	if err := osClient.GetJSON(ctx, store.Bucket, keys.MetadataKey, &meta); err != nil {
		if objectstore.IsNotFound(err) {
			return nil, "", "", nil, fmt.Errorf("%s: import backup %q has no manifest at s3://%s/%s",
				reasonImportIncompatible, backup.Name, store.Bucket, keys.MetadataKey)
		}
		return nil, "", "", nil, importNotReady("reading the manifest of import backup %q: %v", backup.Name, err)
	}
	return store, keys.ArchiveKey, keys.MetadataKey, &meta, nil
}

// importObjectStore picks the object store an import Backup was written to:
// the Backup's own override, else the store of the cluster it was taken from,
// else this cluster's store (the source cluster was deleted and this one was
// created in its place).
func (r *ClusterReconciler) importObjectStore(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	backup *mysqlv1alpha1.Backup,
) (*mysqlv1alpha1.S3ObjectStore, error) {
	var store *mysqlv1alpha1.S3ObjectStore
	if backup.Spec.ObjectStore != nil {
		store = backup.Spec.ObjectStore.DeepCopy()
	} else {
		source := &mysqlv1alpha1.Cluster{}
		err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: backup.Spec.Cluster.Name}, source)
		switch {
		case err == nil && source.Spec.Backup != nil && source.Spec.Backup.ObjectStore != nil:
			store = source.Spec.Backup.ObjectStore.DeepCopy()
		case err != nil && !apierrors.IsNotFound(err):
			return nil, importNotReady("reading cluster %q of import backup %q: %v",
				backup.Spec.Cluster.Name, backup.Name, err)
		case cluster.Spec.Backup != nil && cluster.Spec.Backup.ObjectStore != nil:
			store = cluster.Spec.Backup.ObjectStore.DeepCopy()
		default:
			return nil, fmt.Errorf("%s: import backup %q has no object store, and neither its cluster %q nor "+
				"this cluster has spec.backup.objectStore", reasonImportIncompatible, backup.Name, backup.Spec.Cluster.Name)
		}
	}
	store.SetDefaults()
	return store, nil
}

// resolveImportSource locates a dump under an externalClusters entry's object
// store: the one with the requested backupID, or the latest.
func (r *ClusterReconciler) resolveImportSource(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	imp *mysqlv1alpha1.BootstrapImport,
) (*mysqlv1alpha1.S3ObjectStore, string, string, *objectstore.LogicalBackupMetadata, error) {
	ext := cluster.Spec.FindExternalCluster(imp.Source)
	if ext == nil || ext.ObjectStore == nil {
		// Admission rejects both; this covers objects admitted before it did.
		return nil, "", "", nil, fmt.Errorf("%s: import source %q is not an externalClusters entry with an objectStore",
			reasonImportIncompatible, imp.Source)
	}
	store := ext.ObjectStore.DeepCopy()
	store.SetDefaults()

	osClient, err := r.importStoreClient(ctx, cluster.Namespace, store)
	if err != nil {
		return nil, "", "", nil, err
	}
	// The external cluster name is the key prefix the dumps were stored under.
	entries, err := objectstore.ListLogicalBackups(ctx, osClient, *store, ext.Name)
	if err != nil {
		return nil, "", "", nil, importNotReady("listing logical backups of source %q: %v", imp.Source, err)
	}
	var entry objectstore.LogicalBackupEntry
	if imp.BackupID != "" {
		entry, err = objectstore.FindLogicalBackupByID(entries, imp.BackupID)
	} else {
		entry, err = objectstore.SelectLatestLogicalBackup(entries)
	}
	if err != nil {
		// A dump can still land there (a schedule, a Backup running now).
		return nil, "", "", nil, importNotReady("source %q: %v", imp.Source, err)
	}
	// entry.Prefix already ends with a slash.
	return store, entry.Prefix + objectstore.LogicalArchiveName, entry.Prefix + objectstore.LogicalMetadataName,
		&entry.Meta, nil
}

// importStoreClient builds an object-store client for resolving an import.
// Failures are retried: a Secret can be created after the Cluster.
func (r *ClusterReconciler) importStoreClient(
	ctx context.Context, namespace string, store *mysqlv1alpha1.S3ObjectStore,
) (*objectstore.Client, error) {
	cfg, err := r.objectStoreConfig(ctx, namespace, store)
	if err != nil {
		return nil, importNotReady("%v", err)
	}
	osClient, err := objectstore.NewClient(cfg)
	if err != nil {
		return nil, importNotReady("%v", err)
	}
	return osClient, nil
}

// warnImportFromNewerSeries emits a Warning when the dump comes from a newer
// server series than this cluster runs. Loading into an older series through
// SQL usually works, but it is not tested.
func (r *ClusterReconciler) warnImportFromNewerSeries(cluster *mysqlv1alpha1.Cluster, source, target string) {
	if r.Recorder == nil || source == "" || target == "" {
		return
	}
	src, err := version.Parse(source)
	if err != nil {
		return
	}
	tgt, err := version.Parse(target)
	if err != nil {
		return
	}
	s, t := src.Series(), tgt.Series()
	if s.Major > t.Major || (s.Major == t.Major && s.Minor > t.Minor) {
		r.Recorder.Eventf(cluster, corev1.EventTypeWarning, "ImportFromNewerServer",
			"The dump was taken on %s, a newer series than this cluster's %s. Loading it is allowed but not tested",
			source, target)
	}
}

// importArgs builds the import init container's command.
func importArgs(plan clusterPlan) []string {
	args := make([]string, 0, 9+len(plan.Import.Databases)+len(plan.Import.PostImportSQL))
	args = append(args,
		managerInstanceCmd, "import",
		"--mysqld="+mysqldBinary,
		"--config="+configPath,
		"--data-dir="+dataDir,
		"--socket="+socketPath,
		"--bucket="+plan.Import.Bucket,
		"--dump-key="+plan.Import.DumpKey,
		"--manifest-key="+plan.Import.ManifestKey,
	)
	for _, db := range plan.Import.Databases {
		args = append(args, "--database="+escapeArgVars(db))
	}
	for _, stmt := range plan.Import.PostImportSQL {
		args = append(args, "--post-import-sql="+escapeArgVars(stmt))
	}
	return args
}

// escapeArgVars keeps the kubelet from expanding $(VAR) references in a
// user-supplied container argument: "$$" is a literal "$".
func escapeArgVars(s string) string {
	return strings.ReplaceAll(s, "$", "$$")
}

// importContainer is the init container that loads the dump. It runs after
// "bootstrap" (initdb) and starts a temporary mysqld over the same data
// directory, with the same my.cnf, so it gets the instance's resources, not
// the backup Job's: the server's buffer pool is sized for them.
func importContainer(cluster *mysqlv1alpha1.Cluster, plan clusterPlan) corev1.Container {
	return corev1.Container{
		Name:            importContainerName,
		Image:           plan.Image,
		ImagePullPolicy: cluster.Spec.ImagePullPolicy,
		Command:         []string{managerBinary},
		Args:            importArgs(plan),
		Env:             append(initEnv(plan), plan.Import.StoreEnv...),
		VolumeMounts:    volumeMounts(),
		Resources:       cluster.Spec.Resources,
		SecurityContext: cluster.Spec.SecurityContext,
	}
}

// withoutImportContainer drops the import init container. The container only
// exists until the cluster is established, and its arguments follow the
// resolved dump, so it must never move the Pod template hash: dropping it, or
// a newer dump appearing under the source, would otherwise roll the primary.
func withoutImportContainer(spec *corev1.PodSpec) {
	spec.InitContainers = slices.DeleteFunc(spec.InitContainers, func(c corev1.Container) bool {
		return c.Name == importContainerName
	})
}
