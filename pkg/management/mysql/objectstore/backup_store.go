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

package objectstore

import (
	"context"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// ErrNoBackupStore reports that nothing names the object store a Backup was
// written to.
var ErrNoBackupStore = errors.New("no object store")

// BackupStore returns the object store a Backup was written to, defaulted. It
// takes, in order: the store recorded in the Backup's status when it ran,
// which survives its cluster being deleted or moving to another store; the
// Backup's own override; its cluster's store; then fallback, for a cluster
// that is gone or no longer names a store. A cluster that cannot be read for
// another reason is returned as an error; when nothing names a store the error
// wraps ErrNoBackupStore.
func BackupStore(
	ctx context.Context,
	c client.Reader,
	backup *mysqlv1alpha1.Backup,
	fallback *mysqlv1alpha1.S3ObjectStore,
) (*mysqlv1alpha1.S3ObjectStore, error) {
	store, err := backupStore(ctx, c, backup, fallback)
	if err != nil {
		return nil, err
	}
	store = store.DeepCopy()
	store.SetDefaults()
	return store, nil
}

func backupStore(
	ctx context.Context,
	c client.Reader,
	backup *mysqlv1alpha1.Backup,
	fallback *mysqlv1alpha1.S3ObjectStore,
) (*mysqlv1alpha1.S3ObjectStore, error) {
	if backup.Status.ObjectStore != nil {
		return backup.Status.ObjectStore, nil
	}
	if backup.Spec.ObjectStore != nil {
		return backup.Spec.ObjectStore, nil
	}
	cluster := &mysqlv1alpha1.Cluster{}
	err := c.Get(ctx, types.NamespacedName{Namespace: backup.Namespace, Name: backup.Spec.Cluster.Name}, cluster)
	switch {
	case err == nil && cluster.Spec.Backup != nil && cluster.Spec.Backup.ObjectStore != nil:
		return cluster.Spec.Backup.ObjectStore, nil
	case err != nil && !apierrors.IsNotFound(err):
		return nil, fmt.Errorf("reading cluster %q of backup %q: %w", backup.Spec.Cluster.Name, backup.Name, err)
	case fallback != nil:
		return fallback, nil
	}
	return nil, fmt.Errorf("%w: backup %q names none and neither does its cluster %q",
		ErrNoBackupStore, backup.Name, backup.Spec.Cluster.Name)
}
