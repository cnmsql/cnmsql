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

package v1alpha1

import "k8s.io/apimachinery/pkg/util/validation/field"

// SetDefaults fills in unset fields of the Backup spec with their default
// values, mirroring the kubebuilder markers so the in-memory object is
// consistent before the reconciler reads it. It is idempotent.
func (b *Backup) SetDefaults() {
	spec := &b.Spec
	if spec.Method == "" {
		spec.Method = BackupMethodXtrabackup
	}
	if spec.Target == "" {
		spec.Target = BackupTargetPreferStandby
	}
	if spec.ReclaimPolicy == "" {
		spec.ReclaimPolicy = BackupReclaimRetain
	}
}

// Validate checks the durations of a volumeSnapshot configuration: a zero or
// negative deadline or lock timeout would fail every snapshot Backup at once.
// A nil configuration is valid.
func (c *VolumeSnapshotConfiguration) Validate(path *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if c == nil {
		return allErrs
	}
	if c.Deadline != nil && c.Deadline.Duration <= 0 {
		allErrs = append(allErrs, field.Invalid(path.Child("deadline"), c.Deadline.Duration.String(),
			"must be a positive duration"))
	}
	if c.LockTimeout != nil && c.LockTimeout.Duration <= 0 {
		allErrs = append(allErrs, field.Invalid(path.Child("lockTimeout"), c.LockTimeout.Duration.String(),
			"must be a positive duration"))
	}
	return allErrs
}

// IsVolumeSnapshot reports whether the Backup is, or was taken as, a
// volumeSnapshot backup.
func (b *Backup) IsVolumeSnapshot() bool {
	return b.Spec.Method == BackupMethodVolumeSnapshot || b.Status.Method == BackupMethodVolumeSnapshot
}

// WantsObjectStoreCleanup reports whether this Backup's archive should be
// removed from the object store when the Backup is deleted.
func (b *Backup) WantsObjectStoreCleanup() bool {
	return b.Spec.ReclaimPolicy == BackupReclaimDelete
}
