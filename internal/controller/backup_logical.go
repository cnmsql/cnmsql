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
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/backupworker"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// backupWorkerContainer is the name of the worker container in a backup Job.
const backupWorkerContainer = "backup"

// backupKeysForMethod checks the Backup's method and returns the key builder
// for it, or the reason and message to fail the Backup with. Admission already
// rejects the invalid combinations; this covers objects admitted before the
// CRD carried the rules.
func backupKeysForMethod(backup *mysqlv1alpha1.Backup) (
	func(mysqlv1alpha1.S3ObjectStore, string, string, string) (objectstore.BackupKeys, error), string, string,
) {
	switch method := backup.Spec.Method; method {
	case mysqlv1alpha1.BackupMethodXtrabackup:
		if backup.Spec.Logical != nil {
			return nil, "InvalidSpec", "spec.logical is only valid with method: logical"
		}
		return objectstore.BuildBackupKeys, "", ""
	case mysqlv1alpha1.BackupMethodLogical:
		if backup.Spec.Online != nil && !*backup.Spec.Online {
			return nil, "OnlineRequired", "A logical backup is always online; remove online: false"
		}
		return objectstore.BuildLogicalBackupKeys, "", ""
	default:
		return nil, "UnsupportedMethod", fmt.Sprintf("Backup method %q is not supported", method)
	}
}

// logicalWorkerArgs are the worker flags specific to a logical backup: the
// method, the databases, and the dump client's extra arguments. A Backup's
// spec.logical.extraArgs replaces the cluster's spec.backup.logicalOptions.
func logicalWorkerArgs(backup *mysqlv1alpha1.Backup, cluster *mysqlv1alpha1.Cluster) []string {
	args := []string{"--method=logical"}
	var extra []string
	if cluster.Spec.Backup != nil {
		extra = cluster.Spec.Backup.LogicalOptions
	}
	if opts := backup.Spec.Logical; opts != nil {
		for _, db := range opts.Databases {
			args = append(args, "--database="+db)
		}
		if opts.ExtraArgs != nil {
			extra = opts.ExtraArgs
		}
	}
	for _, a := range extra {
		args = append(args, "--dump-arg="+a)
	}
	return args
}

// markBackupPending records why a Backup has not started yet.
func (r *BackupReconciler) markBackupPending(ctx context.Context, backup *mysqlv1alpha1.Backup, reason, message string) error {
	if backup.Status.Phase == mysqlv1alpha1.BackupPhasePending && backup.Status.Error == "" {
		if c := findBackupCondition(backup.Status.Conditions, mysqlv1alpha1.ConditionProgressing); c != nil &&
			c.Reason == reason && c.Message == message {
			return nil
		}
	}
	return r.patchBackupStatus(ctx, backup, func(status *mysqlv1alpha1.BackupStatus) {
		status.Phase = mysqlv1alpha1.BackupPhasePending
		status.Method = backup.Spec.Method
		setBackupCondition(status, mysqlv1alpha1.ConditionProgressing, metav1.ConditionFalse, reason, message, backup.Generation)
		setBackupCondition(status, mysqlv1alpha1.ConditionReady, metav1.ConditionFalse, reason, message, backup.Generation)
	})
}

func findBackupCondition(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}

// readLogicalManifest reads a finished logical backup's logical.json.
func (r *BackupReconciler) readLogicalManifest(
	ctx context.Context,
	namespace string,
	store *mysqlv1alpha1.S3ObjectStore,
	keys objectstore.BackupKeys,
) (*objectstore.LogicalBackupMetadata, error) {
	cfg, err := objectstore.ResolveConfig(ctx, r.Client, namespace, store)
	if err != nil {
		return nil, err
	}
	osClient, err := objectstore.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	var meta objectstore.LogicalBackupMetadata
	if err := osClient.GetJSON(ctx, store.Bucket, keys.MetadataKey, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// manifestUnrecoverable reports whether a manifest read failed in a way no
// retry fixes: the object is not there, or it is not a manifest.
func manifestUnrecoverable(err error) bool {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	return objectstore.IsNotFound(err) || errors.As(err, &syntaxErr) || errors.As(err, &typeErr)
}

// applyLogicalManifest copies a logical backup's results onto its status. The
// snapshot position is for reference only: a dump is never a recovery base.
func applyLogicalManifest(status *mysqlv1alpha1.BackupStatus, meta *objectstore.LogicalBackupMetadata) {
	status.SHA256 = meta.SHA256
	status.Databases = meta.Databases
	status.BeginGTID = meta.SnapshotGTID
	status.EndGTID = meta.SnapshotGTID
	status.BeginBinlog = meta.SnapshotBinlog
	status.EndBinlog = meta.SnapshotBinlog
}

// workerFailure reads the reason and message a failed worker left in its
// termination message. It reports false when there is none (the worker was
// killed, or failed without a known reason), so the caller keeps the Job's own
// failure reason.
func (r *BackupReconciler) workerFailure(ctx context.Context, job *batchv1.Job) (string, string, bool) {
	msg, ok := readWorkerTermination(ctx, r.Client, job, backupWorkerContainer)
	return msg.Reason, msg.Message, ok
}

// readWorkerTermination reads the termination message a failed worker
// container left in one of the Job's Pods. It reports false when there is none.
func readWorkerTermination(
	ctx context.Context, c client.Client, job *batchv1.Job, container string,
) (backupworker.TerminationMessage, bool) {
	var pods corev1.PodList
	if err := c.List(ctx, &pods,
		client.InNamespace(job.Namespace),
		client.MatchingLabels{batchv1.JobNameLabel: job.Name},
	); err != nil {
		// Without the pods the caller falls back to the Job's generic reason;
		// losing the worker's precise one is not worth failing the reconcile.
		logf.FromContext(ctx).Info("Could not list worker Pods", "job", job.Name, "error", err.Error())
		return backupworker.TerminationMessage{}, false
	}
	// Newest attempt first, names breaking same-second ties, so the reported
	// attempt does not depend on the List order. The newest attempt that left a
	// termination message wins: when the final attempt died without one (OOM,
	// deadline kill), an older attempt's diagnosis is still more useful than
	// the Job's generic reason.
	sort.SliceStable(pods.Items, func(i, j int) bool {
		ti, tj := pods.Items[i].CreationTimestamp, pods.Items[j].CreationTimestamp
		if !ti.Equal(&tj) {
			return tj.Before(&ti)
		}
		return pods.Items[i].Name < pods.Items[j].Name
	})
	for i := range pods.Items {
		for _, cs := range pods.Items[i].Status.ContainerStatuses {
			if cs.Name != container || cs.State.Terminated == nil || cs.State.Terminated.Message == "" {
				continue
			}
			var msg backupworker.TerminationMessage
			if err := json.Unmarshal([]byte(cs.State.Terminated.Message), &msg); err != nil || msg.Reason == "" {
				continue
			}
			return msg, true
		}
	}
	return backupworker.TerminationMessage{}, false
}
