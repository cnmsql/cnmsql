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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
)

// backupDestinationCheck reports the outcome of the empty-archive safety check.
type backupDestinationCheck struct {
	// Blocked is a non-empty human-readable reason when the destination already
	// holds data and the fresh cluster must not adopt it.
	Blocked string
	// Retry is set when the destination could not be verified (e.g. the object
	// store is unreachable); the caller should requeue rather than block.
	Retry error
}

// checkBackupDestination guards a freshly bootstrapping cluster from adopting an
// object-store destination that already contains another cluster's backups.
//
// It mirrors CloudNativePG's empty-archive check: a fresh cluster pointed at a
// non-empty destination is kept out of service (Blocked) instead of silently
// overwriting existing data. The check only applies before the primary is
// established (status.currentPrimary unset) and never to a recovery bootstrap,
// whose destination is expected to already hold the backups it restores from.
func (r *ClusterReconciler) checkBackupDestination(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
) backupDestinationCheck {
	// Only fresh, archiving clusters that have never established a primary.
	if cluster.Status.CurrentPrimary != "" {
		return backupDestinationCheck{}
	}
	if cluster.Spec.Bootstrap != nil && cluster.Spec.Bootstrap.Recovery != nil {
		return backupDestinationCheck{}
	}
	if cluster.Spec.Backup == nil || cluster.Spec.Backup.ObjectStore == nil {
		return backupDestinationCheck{}
	}

	store := cluster.Spec.Backup.ObjectStore
	cfg, err := r.objectStoreConfig(ctx, cluster.Namespace, store)
	if err != nil {
		return backupDestinationCheck{Retry: err}
	}
	osClient, err := objectstore.NewClient(cfg)
	if err != nil {
		return backupDestinationCheck{Retry: err}
	}

	prefix := objectstore.ClusterPrefix(*store, cluster.Name)
	empty, err := osClient.IsEmptyPrefix(ctx, store.Bucket, prefix)
	if err != nil {
		return backupDestinationCheck{Retry: err}
	}
	if !empty {
		return backupDestinationCheck{Blocked: fmt.Sprintf(
			"Backup destination s3://%s/%s is not empty; refusing to overwrite an existing archive. "+
				"Use a different cluster name or object-store path, or bootstrap with spec.bootstrap.recovery to restore it",
			store.Bucket, prefix)}
	}
	return backupDestinationCheck{}
}

// recoveryTargetCheck reports the outcome of the up-front point-in-time recovery
// satisfiability check.
type recoveryTargetCheck struct {
	// Blocked is a non-empty reason when the recovery target cannot be satisfied
	// by the archive (e.g. a targetGTID beyond the archived coverage).
	Blocked string
	// Retry is set when satisfiability could not be verified (the object store is
	// unreachable or the archive index is not present yet); the caller requeues.
	Retry error
}

// checkRecoveryTarget validates, before any instance is provisioned, that a
// point-in-time recovery target is reachable from the archive. It is the
// operator-side complement to the init-container's PlanReplay guard: it gives
// fast, clear feedback (a Blocked condition) instead of a CrashLooping Pod when
// the target is obviously unsatisfiable.
//
// It only does work the operator can cheaply check from the cluster-level
// archive index — chiefly that a targetGTID is within the cumulative coverage.
// The precise "older than the base backup" / timeline checks need the base
// backup's anchor GTID and run in the init container.
func (r *ClusterReconciler) checkRecoveryTarget(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	plan clusterPlan,
) recoveryTargetCheck {
	if plan.Recovery == nil || !plan.Recovery.HasTarget {
		return recoveryTargetCheck{}
	}
	if cluster.Status.CurrentPrimary != "" {
		// The primary already recovered; don't re-validate on every resync.
		return recoveryTargetCheck{}
	}

	store := plan.Recovery.Store
	cfg, err := r.objectStoreConfig(ctx, cluster.Namespace, &store)
	if err != nil {
		return recoveryTargetCheck{Retry: err}
	}
	osClient, err := objectstore.NewClient(cfg)
	if err != nil {
		return recoveryTargetCheck{Retry: err}
	}

	indexKey := objectstore.ArchiveIndexKey(store, plan.Recovery.SourceCluster)
	var index objectstore.ArchiveIndex
	if err := osClient.GetJSON(ctx, store.Bucket, indexKey, &index); err != nil {
		// The archive index is missing or unreadable. A recovery target needs a
		// binlog archive; requeue so a still-initialising archive can appear.
		return recoveryTargetCheck{Retry: fmt.Errorf("reading archive index %q: %w", indexKey, err)}
	}

	// The only cheap, anchor-free check: a targetGTID must be within the
	// archive's cumulative coverage.
	if plan.Recovery.TargetGTID != "" {
		contained, err := replication.GTIDContains(index.CoveredGTIDSet, plan.Recovery.TargetGTID)
		if err != nil {
			return recoveryTargetCheck{Blocked: fmt.Sprintf(
				"Invalid recovery targetGTID or archive coverage: %v", err)}
		}
		if !contained {
			return recoveryTargetCheck{Blocked: fmt.Sprintf(
				"Recovery targetGTID %q is beyond the archived binlog coverage %q; the archive cannot replay to it",
				plan.Recovery.TargetGTID, index.CoveredGTIDSet)}
		}
	}
	return recoveryTargetCheck{}
}

// objectStoreConfig resolves an object store plus its secret-backed credentials
// into a client Config the operator can use directly.
func (r *ClusterReconciler) objectStoreConfig(
	ctx context.Context,
	namespace string,
	store *mysqlv1alpha1.S3ObjectStore,
) (objectstore.Config, error) {
	return resolveObjectStoreConfig(ctx, r.Client, namespace, store)
}

// resolveObjectStoreConfig resolves an object store plus its secret-backed
// credentials into a client Config, using c to read the referenced Secrets. It
// is shared by every reconciler that needs its own object-store access.
func resolveObjectStoreConfig(
	ctx context.Context,
	c client.Client,
	namespace string,
	store *mysqlv1alpha1.S3ObjectStore,
) (objectstore.Config, error) {
	var accessKeyID, secretAccessKey, sessionToken string
	creds := store.Credentials
	if creds.AccessKeyID != nil {
		value, err := resolveSecretValue(ctx, c, namespace, *creds.AccessKeyID)
		if err != nil {
			return objectstore.Config{}, err
		}
		accessKeyID = value
	}
	if creds.SecretAccessKey != nil {
		value, err := resolveSecretValue(ctx, c, namespace, *creds.SecretAccessKey)
		if err != nil {
			return objectstore.Config{}, err
		}
		secretAccessKey = value
	}
	if creds.SessionToken != nil {
		value, err := resolveSecretValue(ctx, c, namespace, *creds.SessionToken)
		if err != nil {
			return objectstore.Config{}, err
		}
		sessionToken = value
	}
	return objectstore.ConfigFromStore(*store, accessKeyID, secretAccessKey, sessionToken), nil
}

func resolveSecretValue(
	ctx context.Context,
	c client.Client,
	namespace string,
	selector mysqlv1alpha1.SecretKeySelector,
) (string, error) {
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: namespace, Name: selector.Name}
	if err := c.Get(ctx, key, secret); err != nil {
		return "", fmt.Errorf("reading secret %s/%s: %w", namespace, selector.Name, err)
	}
	value, ok := secret.Data[selector.Key]
	if !ok {
		return "", fmt.Errorf("secret %s/%s has no key %q", namespace, selector.Name, selector.Key)
	}
	return string(value), nil
}
