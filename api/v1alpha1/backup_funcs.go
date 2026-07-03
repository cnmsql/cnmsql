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

// WantsObjectStoreCleanup reports whether this Backup's archive should be
// removed from the object store when the Backup is deleted.
func (b *Backup) WantsObjectStoreCleanup() bool {
	return b.Spec.ReclaimPolicy == BackupReclaimDelete
}
