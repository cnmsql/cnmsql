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
	"k8s.io/apimachinery/pkg/util/validation/field"
)

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
		spec.Backup.ObjectStore.setDefaults()
	}
	for i := range spec.ExternalClusters {
		if spec.ExternalClusters[i].ObjectStore != nil {
			spec.ExternalClusters[i].ObjectStore.setDefaults()
		}
	}
}

func (store *S3ObjectStore) setDefaults() {
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
