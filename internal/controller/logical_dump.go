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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// logicalDump locates a logical backup's dump and manifest in an object store.
type logicalDump struct {
	Store       *mysqlv1alpha1.S3ObjectStore
	DumpKey     string
	ManifestKey string
	Meta        *objectstore.LogicalBackupMetadata
}

// dumpErrorKind says what a caller does about a dump it cannot use.
type dumpErrorKind int

const (
	// dumpNotReady can clear without a spec change: the Backup is still
	// running, or the object store could not be read. The caller retries.
	dumpNotReady dumpErrorKind = iota
	// dumpUnusable needs a spec change: the Backup failed, the manifest is
	// missing, the source is not configured.
	dumpUnusable
	// dumpPhysical: the referenced Backup is a physical one.
	dumpPhysical
)

// dumpError is why a logical dump cannot be located. Its message names the
// dump the way the caller's spec does ("import backup", "backup").
type dumpError struct {
	kind dumpErrorKind
	msg  string
}

func (e *dumpError) Error() string { return e.msg }

func dumpErrorf(kind dumpErrorKind, format string, args ...any) error {
	return &dumpError{kind: kind, msg: fmt.Sprintf(format, args...)}
}

// logicalDumpResolver locates the logical backup that a bootstrap import or a
// LogicalRestore loads: a logical Backup in the namespace, or a dump under an
// externalClusters entry's object store. Both callers share its rules.
type logicalDumpResolver struct {
	client client.Client
	// backupNoun and sourceNoun name the spec's reference in messages, e.g.
	// "import backup" and "import source".
	backupNoun string
	sourceNoun string
	// target names the cluster the dump is loaded into, e.g. "this cluster".
	target string
}

// fromBackup locates the dump of a logical Backup in namespace. fallback is
// the target cluster's own object store, used when neither the Backup nor its
// cluster names one (the source cluster was deleted and replaced).
func (d logicalDumpResolver) fromBackup(
	ctx context.Context,
	namespace, name string,
	fallback *mysqlv1alpha1.S3ObjectStore,
) (*logicalDump, error) {
	backup := &mysqlv1alpha1.Backup{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, backup); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, dumpErrorf(dumpNotReady, "%s %q does not exist", d.backupNoun, name)
		}
		return nil, dumpErrorf(dumpNotReady, "reading %s %q: %v", d.backupNoun, name, err)
	}
	if backup.Spec.Method != mysqlv1alpha1.BackupMethodLogical {
		return nil, dumpErrorf(dumpPhysical, "%s %q is a %s backup", d.backupNoun, backup.Name, backup.Spec.Method)
	}
	switch backup.Status.Phase {
	case mysqlv1alpha1.BackupPhaseCompleted:
	case mysqlv1alpha1.BackupPhaseFailed:
		return nil, dumpErrorf(dumpUnusable, "%s %q failed", d.backupNoun, backup.Name)
	default:
		return nil, dumpErrorf(dumpNotReady, "%s %q is not completed yet", d.backupNoun, backup.Name)
	}
	if backup.Status.BackupID == "" {
		return nil, dumpErrorf(dumpUnusable, "%s %q has no backupID", d.backupNoun, backup.Name)
	}

	store, err := d.backupStore(ctx, backup, fallback)
	if err != nil {
		return nil, err
	}
	keys, err := objectstore.BuildLogicalBackupKeys(*store, backup.Spec.Cluster.Name, backup.Name, backup.Status.BackupID)
	if err != nil {
		return nil, dumpErrorf(dumpUnusable, "%v", err)
	}
	osClient, err := d.storeClient(ctx, namespace, store)
	if err != nil {
		return nil, err
	}
	var meta objectstore.LogicalBackupMetadata
	if err := osClient.GetJSON(ctx, store.Bucket, keys.MetadataKey, &meta); err != nil {
		if objectstore.IsNotFound(err) {
			return nil, dumpErrorf(dumpUnusable, "%s %q has no manifest at s3://%s/%s",
				d.backupNoun, backup.Name, store.Bucket, keys.MetadataKey)
		}
		return nil, dumpErrorf(dumpNotReady, "reading the manifest of %s %q: %v", d.backupNoun, backup.Name, err)
	}
	return &logicalDump{Store: store, DumpKey: keys.ArchiveKey, ManifestKey: keys.MetadataKey, Meta: &meta}, nil
}

// backupStore picks the object store a Backup was written to: the Backup's own
// override, else the store of the cluster it was taken from, else fallback.
func (d logicalDumpResolver) backupStore(
	ctx context.Context,
	backup *mysqlv1alpha1.Backup,
	fallback *mysqlv1alpha1.S3ObjectStore,
) (*mysqlv1alpha1.S3ObjectStore, error) {
	var store *mysqlv1alpha1.S3ObjectStore
	if backup.Spec.ObjectStore != nil {
		store = backup.Spec.ObjectStore.DeepCopy()
	} else {
		source := &mysqlv1alpha1.Cluster{}
		err := d.client.Get(ctx, types.NamespacedName{Namespace: backup.Namespace, Name: backup.Spec.Cluster.Name}, source)
		switch {
		case err == nil && source.Spec.Backup != nil && source.Spec.Backup.ObjectStore != nil:
			store = source.Spec.Backup.ObjectStore.DeepCopy()
		case err != nil && !apierrors.IsNotFound(err):
			return nil, dumpErrorf(dumpNotReady, "reading cluster %q of %s %q: %v",
				backup.Spec.Cluster.Name, d.backupNoun, backup.Name, err)
		case fallback != nil:
			store = fallback.DeepCopy()
		default:
			return nil, dumpErrorf(dumpUnusable, "%s %q has no object store, and neither its cluster %q nor "+
				"%s has spec.backup.objectStore", d.backupNoun, backup.Name, backup.Spec.Cluster.Name, d.target)
		}
	}
	store.SetDefaults()
	return store, nil
}

// fromSource locates a dump under an externalClusters entry's object store:
// the one with backupID, or the latest when backupID is empty. ext is nil when
// the entry does not exist.
func (d logicalDumpResolver) fromSource(
	ctx context.Context,
	namespace, sourceName string,
	ext *mysqlv1alpha1.ExternalCluster,
	backupID string,
) (*logicalDump, error) {
	if ext == nil || ext.ObjectStore == nil {
		// Admission rejects this for imports; a LogicalRestore names an entry
		// of another object, which admission cannot see.
		return nil, dumpErrorf(dumpUnusable, "%s %q is not an externalClusters entry with an objectStore",
			d.sourceNoun, sourceName)
	}
	store := ext.ObjectStore.DeepCopy()
	store.SetDefaults()

	osClient, err := d.storeClient(ctx, namespace, store)
	if err != nil {
		return nil, err
	}
	// The external cluster name is the key prefix the dumps were stored under.
	entries, err := objectstore.ListLogicalBackups(ctx, osClient, *store, ext.Name)
	if err != nil {
		return nil, dumpErrorf(dumpNotReady, "listing logical backups of source %q: %v", sourceName, err)
	}
	var entry objectstore.LogicalBackupEntry
	if backupID != "" {
		entry, err = objectstore.FindLogicalBackupByID(entries, backupID)
	} else {
		entry, err = objectstore.SelectLatestLogicalBackup(entries)
	}
	if err != nil {
		// A dump can still land there (a schedule, a Backup running now).
		return nil, dumpErrorf(dumpNotReady, "source %q: %v", sourceName, err)
	}
	// entry.Prefix already ends with a slash.
	return &logicalDump{
		Store:       store,
		DumpKey:     entry.Prefix + objectstore.LogicalArchiveName,
		ManifestKey: entry.Prefix + objectstore.LogicalMetadataName,
		Meta:        &entry.Meta,
	}, nil
}

// storeClient builds an object-store client. Failures are retried: a Secret
// can be created after the object that references it.
func (d logicalDumpResolver) storeClient(
	ctx context.Context, namespace string, store *mysqlv1alpha1.S3ObjectStore,
) (*objectstore.Client, error) {
	cfg, err := objectstore.ResolveConfig(ctx, d.client, namespace, store)
	if err != nil {
		return nil, dumpErrorf(dumpNotReady, "%v", err)
	}
	osClient, err := objectstore.NewClient(cfg)
	if err != nil {
		return nil, dumpErrorf(dumpNotReady, "%v", err)
	}
	return osClient, nil
}
