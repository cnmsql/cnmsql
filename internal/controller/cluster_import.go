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
	"errors"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"

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

	resolver := logicalDumpResolver{
		client:     r.Client,
		backupNoun: "import backup",
		sourceNoun: "import source",
		target:     "this cluster",
	}
	var (
		dump *logicalDump
		err  error
	)
	if imp.Source != "" {
		dump, err = resolver.fromSource(ctx, cluster.Namespace, imp.Source,
			cluster.Spec.FindExternalCluster(imp.Source), imp.BackupID)
	} else {
		var fallback *mysqlv1alpha1.S3ObjectStore
		if cluster.Spec.Backup != nil {
			fallback = cluster.Spec.Backup.ObjectStore
		}
		dump, err = resolver.fromBackup(ctx, cluster.Namespace, imp.Backup.Name, fallback)
	}
	if err != nil {
		return nil, importDumpError(err)
	}
	if err := dump.Meta.CheckImportable(string(cluster.ResolvedFlavor()), imp.Databases); err != nil {
		return nil, fmt.Errorf("%s: %w", reasonImportIncompatible, err)
	}
	r.warnImportFromNewerSeries(cluster, dump.Meta.ServerVersion, serverVersion)

	store := dump.Store
	return &importPlan{
		Bucket:      store.Bucket,
		DumpKey:     dump.DumpKey,
		ManifestKey: dump.ManifestKey,
		StoreEnv: append(backupObjectStoreEnv(*store),
			corev1.EnvVar{Name: objectstore.EnvBucket, Value: store.Bucket},
			corev1.EnvVar{Name: objectstore.EnvPath, Value: store.Path},
		),
		Databases:     slices.Clone(imp.Databases),
		PostImportSQL: slices.Clone(imp.PostImportSQL),
	}, nil
}

// importDumpError gives a resolver error the reason prefix of the cluster
// phase it ends in.
func importDumpError(err error) error {
	var de *dumpError
	if !errors.As(err, &de) {
		return err
	}
	switch de.kind {
	case dumpNotReady:
		return &importNotReadyError{msg: de.msg}
	case dumpPhysical:
		return fmt.Errorf("%s: %s, and initdb.import loads a logical backup; restore a physical backup "+
			"with bootstrap.recovery instead", reasonPhysicalBackupNotImportable, de.msg)
	default:
		return fmt.Errorf("%s: %s", reasonImportIncompatible, de.msg)
	}
}

// warnImportFromNewerSeries emits a Warning when the dump comes from a newer
// server series than this cluster runs. Loading into an older series through
// SQL usually works, but it is not tested.
func (r *ClusterReconciler) warnImportFromNewerSeries(cluster *mysqlv1alpha1.Cluster, source, target string) {
	if r.Recorder == nil {
		return
	}
	if dumpFromNewerSeries(source, target) {
		r.Recorder.Eventf(cluster, corev1.EventTypeWarning, "ImportFromNewerServer",
			"The dump was taken on %s, a newer series than this cluster's %s. Loading it is allowed but not tested",
			source, target)
	}
}

// dumpFromNewerSeries reports whether a dump taken on source comes from a
// newer server series than target. Unparsable versions report false.
func dumpFromNewerSeries(source, target string) bool {
	if source == "" || target == "" {
		return false
	}
	src, err := version.Parse(source)
	if err != nil {
		return false
	}
	tgt, err := version.Parse(target)
	if err != nil {
		return false
	}
	s, t := src.Series(), tgt.Series()
	return s.Major > t.Major || (s.Major == t.Major && s.Minor > t.Minor)
}

// importArgs builds the import init container's command.
func importArgs(plan clusterPlan) []string {
	args := make([]string, 0, 10+len(plan.Import.Databases)+len(plan.Import.PostImportSQL))
	args = append(args,
		managerInstanceCmd, "import",
		"--mysqld="+mysqldBinary,
		"--config="+configPath,
		"--data-dir="+dataDir,
		"--socket="+socketPath,
		"--bucket="+plan.Import.Bucket,
		"--dump-key="+plan.Import.DumpKey,
		"--manifest-key="+plan.Import.ManifestKey,
		// import reads the temporary server's root password from the cluster's
		// credential Secrets through the Kubernetes API.
		"--cluster-name="+plan.ClusterName,
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
