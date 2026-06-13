/*
Copyright 2026 The CNMySQL Authors.

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

import (
	"regexp"
	"time"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// gtidSetSyntaxRe matches a MySQL GTID set: one or more comma-separated
// "<uuid>:<interval[:interval...]>" terms, each interval being "n" or "m-n".
// This is a syntactic gate for admission; semantic containment is checked in
// the controller/instance recovery paths via replication.ParseGTIDSet.
var gtidSetSyntaxRe = regexp.MustCompile(
	`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}:[0-9]+(-[0-9]+)?(:[0-9]+(-[0-9]+)?)*` +
		`(,[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}:[0-9]+(-[0-9]+)?(:[0-9]+(-[0-9]+)?)*)*$`)

// SetDefaults fills in the unset fields of the Cluster spec with their default
// values. It is idempotent. Defaults declared via kubebuilder markers are
// applied by the API server; this mirrors them so the in-memory object is
// consistent in unit tests and controller code paths that run before
// admission.
func (cluster *Cluster) SetDefaults() {
	spec := &cluster.Spec

	if spec.Instances == 0 {
		spec.Instances = DefaultInstances
	}

	if spec.MySQL.BinlogFormat == "" {
		spec.MySQL.BinlogFormat = DefaultBinlogFormat
	}

	if spec.PrimaryUpdateStrategy == "" {
		spec.PrimaryUpdateStrategy = DefaultPrimaryUpdateStrategy
	}

	if spec.PrimaryUpdateMethod == "" {
		spec.PrimaryUpdateMethod = DefaultPrimaryUpdateMethod
	}

	if spec.MaxStartDelay == 0 {
		spec.MaxStartDelay = DefaultStartupDelay
	}

	if spec.MaxStopDelay == 0 {
		spec.MaxStopDelay = DefaultShutdownDelay
	}

	if spec.MaxSwitchoverDelay == 0 {
		spec.MaxSwitchoverDelay = DefaultSwitchoverDelay
	}

	if spec.EnablePDB == nil {
		spec.EnablePDB = ptrTo(true)
	}

	if spec.EnableSuperuserAccess == nil {
		spec.EnableSuperuserAccess = ptrTo(false)
	}

	if spec.Storage.ResizeInUseVolumes == nil {
		spec.Storage.ResizeInUseVolumes = ptrTo(true)
	}

	if spec.Backup != nil && spec.Backup.ObjectStore != nil {
		spec.Backup.ObjectStore.SetDefaults()
	}
	for i := range spec.ExternalClusters {
		if spec.ExternalClusters[i].ObjectStore != nil {
			spec.ExternalClusters[i].ObjectStore.SetDefaults()
		}
	}
}

// SetDefaults fills in the object store's optional fields with their defaults.
func (store *S3ObjectStore) SetDefaults() {
	if store.ForcePathStyle == nil {
		store.ForcePathStyle = ptrTo(true)
	}
	if store.SignatureVersion == "" {
		store.SignatureVersion = SignatureVersionV4
	}
}

// Validate returns the list of validation errors for the Cluster spec. An empty
// list means the spec is valid. This is used both by unit tests and (later) by
// the validating webhook.
func (cluster *Cluster) Validate() field.ErrorList {
	var allErrs field.ErrorList
	spec := &cluster.Spec
	specPath := field.NewPath("spec")

	// Image source must be unambiguous: exactly one of imageName/imageCatalogRef.
	switch {
	case spec.ImageName != "" && spec.ImageCatalogRef != nil:
		allErrs = append(allErrs, field.Invalid(
			specPath.Child("imageCatalogRef"), spec.ImageCatalogRef,
			"imageName and imageCatalogRef are mutually exclusive"))
	}

	if spec.Instances < 1 {
		allErrs = append(allErrs, field.Invalid(
			specPath.Child("instances"), spec.Instances,
			"instances must be at least 1"))
	}

	if spec.MaxSyncReplicas < 0 {
		allErrs = append(allErrs, field.Invalid(
			specPath.Child("maxSyncReplicas"), spec.MaxSyncReplicas,
			"maxSyncReplicas cannot be negative"))
	}

	if spec.MinSyncReplicas < 0 {
		allErrs = append(allErrs, field.Invalid(
			specPath.Child("minSyncReplicas"), spec.MinSyncReplicas,
			"minSyncReplicas cannot be negative"))
	}

	if spec.MaxSyncReplicas < spec.MinSyncReplicas {
		allErrs = append(allErrs, field.Invalid(
			specPath.Child("maxSyncReplicas"), spec.MaxSyncReplicas,
			"maxSyncReplicas cannot be lower than minSyncReplicas"))
	}

	// Synchronous replicas must be acknowledgeable by the available standbys.
	if spec.Instances > 0 && spec.MaxSyncReplicas >= spec.Instances {
		allErrs = append(allErrs, field.Invalid(
			specPath.Child("maxSyncReplicas"), spec.MaxSyncReplicas,
			"maxSyncReplicas must be lower than the number of instances"))
	}

	allErrs = append(allErrs, spec.validateBootstrap(specPath.Child("bootstrap"))...)
	allErrs = append(allErrs, spec.validateReplica(specPath.Child("replica"))...)
	allErrs = append(allErrs, spec.validateBackup(specPath.Child("backup"))...)

	return allErrs
}

// validateBackup checks the backup/continuous-archiving configuration is
// coherent: continuous archiving needs an object store to ship binlogs to.
func (spec *ClusterSpec) validateBackup(path *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if spec.Backup == nil || spec.Backup.ContinuousArchiving == nil {
		return allErrs
	}
	if spec.Backup.ContinuousArchiving.Enabled && spec.Backup.ObjectStore == nil {
		allErrs = append(allErrs, field.Invalid(
			path.Child("continuousArchiving", "enabled"), true,
			"continuous archiving requires backup.objectStore to be configured"))
	}
	return allErrs
}

func (spec *ClusterSpec) validateBootstrap(path *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if spec.Bootstrap == nil {
		return allErrs
	}
	if spec.Bootstrap.InitDB != nil && spec.Bootstrap.Recovery != nil {
		allErrs = append(allErrs, field.Invalid(
			path, spec.Bootstrap,
			"only one of initdb or recovery can be specified"))
	}
	if spec.Bootstrap.Recovery != nil {
		allErrs = append(allErrs, spec.validateRecovery(path.Child("recovery"))...)
	}
	return allErrs
}

// validateRecovery checks the recovery bootstrap, in particular the
// point-in-time recovery target. PG-only targets (targetName/targetLSN) are
// rejected by construction: RecoveryTarget has no such fields.
func (spec *ClusterSpec) validateRecovery(path *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	rec := spec.Bootstrap.Recovery
	if rec.Backup == nil || rec.Backup.Name == "" {
		allErrs = append(allErrs, field.Required(
			path.Child("backup"), "recovery requires a backup reference"))
	}

	target := rec.RecoveryTarget
	if target == nil {
		return allErrs
	}
	tPath := path.Child("recoveryTarget")

	// A point-in-time target is replayed from the binlog archive, which only
	// exists when continuous archiving is configured against an object store.
	if spec.Backup == nil || spec.Backup.ObjectStore == nil {
		allErrs = append(allErrs, field.Invalid(
			tPath, target,
			"recoveryTarget requires backup.objectStore to be configured for binlog replay"))
	}

	// At most one of targetTime / targetGTID / targetImmediate may be set.
	set := 0
	if target.TargetTime != "" {
		set++
	}
	if target.TargetGTID != "" {
		set++
	}
	if target.TargetImmediate != nil && *target.TargetImmediate {
		set++
	}
	if set > 1 {
		allErrs = append(allErrs, field.Invalid(
			tPath, target,
			"at most one of targetTime, targetGTID or targetImmediate may be specified"))
	}

	if target.TargetTime != "" {
		if _, err := time.Parse(time.RFC3339, target.TargetTime); err != nil {
			allErrs = append(allErrs, field.Invalid(
				tPath.Child("targetTime"), target.TargetTime,
				"must be an RFC3339 timestamp"))
		}
	}
	if target.TargetGTID != "" && !gtidSetSyntaxRe.MatchString(target.TargetGTID) {
		allErrs = append(allErrs, field.Invalid(
			tPath.Child("targetGTID"), target.TargetGTID,
			"must be a valid GTID set (e.g. \"uuid:1-100\")"))
	}
	return allErrs
}

func (spec *ClusterSpec) validateReplica(path *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if spec.Replica == nil {
		return allErrs
	}
	if spec.Replica.Source == "" {
		allErrs = append(allErrs, field.Required(
			path.Child("source"), "replica.source is required when replica is set"))
		return allErrs
	}
	if !spec.hasExternalCluster(spec.Replica.Source) {
		allErrs = append(allErrs, field.Invalid(
			path.Child("source"), spec.Replica.Source,
			"replica.source must reference an entry in externalClusters"))
	}
	return allErrs
}

func (spec *ClusterSpec) hasExternalCluster(name string) bool {
	for i := range spec.ExternalClusters {
		if spec.ExternalClusters[i].Name == name {
			return true
		}
	}
	return false
}

// GetEnableSuperuserAccess returns whether superuser (root) access is enabled,
// resolving the default.
func (cluster *Cluster) GetEnableSuperuserAccess() bool {
	if cluster.Spec.EnableSuperuserAccess == nil {
		return false
	}
	return *cluster.Spec.EnableSuperuserAccess
}

// IsReplica returns whether the cluster is configured as a replica cluster.
func (cluster *Cluster) IsReplica() bool {
	return cluster.Spec.Replica != nil &&
		(cluster.Spec.Replica.Enabled == nil || *cluster.Spec.Replica.Enabled)
}

// ptrTo returns a pointer to the given value.
func ptrTo[T any](v T) *T {
	return &v
}
