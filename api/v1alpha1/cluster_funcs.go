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

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/version"
)

// ClusterLabelName is the label the operator stamps on every resource owned by a
// Cluster, carrying the Cluster's name as its value. It is the canonical way to
// select the instance Pods belonging to a Cluster and is published through the
// scale sub-resource so autoscalers (HPA, VPA) can discover them.
const ClusterLabelName = "mysql.cnmsql.co/cluster"

// GetInstancesSelector returns the serialized label selector that matches all
// the instance Pods managed by this Cluster. It is published in the status and
// exposed through the scale sub-resource so that autoscalers (such as HPA or
// VPA) can discover the managed Pods.
func (cluster *Cluster) GetInstancesSelector() string {
	return labels.SelectorFromSet(labels.Set{
		ClusterLabelName: cluster.Name,
	}).String()
}

// retentionPolicyRe matches a retention-policy duration string: a positive
// integer followed by a unit (d=day, w=week, m=month). It mirrors the
// kubebuilder pattern on BackupConfiguration.RetentionPolicy.
var retentionPolicyRe = regexp.MustCompile(`^([1-9][0-9]*)([dwm])$`)

// retentionUnitDuration maps a retention-policy unit to its duration. A month is
// approximated as 30 days, matching CloudNativePG's barman retention semantics.
var retentionUnitDuration = map[string]time.Duration{
	"d": 24 * time.Hour,
	"w": 7 * 24 * time.Hour,
	"m": 30 * 24 * time.Hour,
}

// ParseRetentionPolicy parses a retention-policy string (e.g. "30d", "8w",
// "3m") into the duration a backup may live before it is eligible for GC. A
// month is treated as 30 days.
func ParseRetentionPolicy(policy string) (time.Duration, error) {
	match := retentionPolicyRe.FindStringSubmatch(policy)
	if match == nil {
		return 0, fmt.Errorf("invalid retention policy %q: want <n>{d|w|m} (e.g. 30d, 8w, 3m)", policy)
	}
	n, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, fmt.Errorf("invalid retention policy %q: %w", policy, err)
	}
	return time.Duration(n) * retentionUnitDuration[match[2]], nil
}

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

	if spec.SmartShutdownTimeout == nil {
		spec.SmartShutdownTimeout = ptr.To(int32(DefaultSmartShutdownTimeout))
	}
	// The smart shutdown must finish before the hard stop delay so there is
	// headroom for the forced fallback; clamp it if it was set too high.
	if *spec.SmartShutdownTimeout >= spec.MaxStopDelay {
		spec.SmartShutdownTimeout = ptr.To(spec.MaxStopDelay / 2)
	}

	if spec.MaxSwitchoverDelay == 0 {
		spec.MaxSwitchoverDelay = DefaultSwitchoverDelay
	}

	if spec.EnablePDB == nil {
		spec.EnablePDB = ptr.To(true)
	}

	if spec.EnablePrimaryLease == nil {
		spec.EnablePrimaryLease = ptr.To(true)
	}

	if spec.EnableSuperuserAccess == nil {
		spec.EnableSuperuserAccess = ptr.To(false)
	}

	if spec.Storage.ResizeInUseVolumes == nil {
		spec.Storage.ResizeInUseVolumes = ptr.To(true)
	}

	if spec.Backup != nil {
		if spec.Backup.ReclaimPolicy == "" {
			spec.Backup.ReclaimPolicy = BackupReclaimRetain
		}
		if spec.Backup.ObjectStore != nil {
			spec.Backup.ObjectStore.SetDefaults()
		}
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
		store.ForcePathStyle = ptr.To(true)
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
	allErrs = append(allErrs, spec.validateManagedServices(specPath.Child("managed", "services"))...)
	allErrs = append(allErrs, spec.validateManagedRoles(specPath.Child("managed", "roles"))...)
	allErrs = append(allErrs, spec.validateReplication(specPath.Child("replication"))...)

	return allErrs
}

// ValidateUpdate returns the validation errors specific to updating an existing
// Cluster: the fields that are immutable once set. It is additive to Validate,
// which the caller still runs for field-level checks.
func (cluster *Cluster) ValidateUpdate(old *Cluster) field.ErrorList {
	var allErrs field.ErrorList
	path := field.NewPath("spec", "replication")

	// replication.mode is immutable: switching topology on a live cluster is a
	// data-path change that cannot be done safely in place.
	if old.ReplicationMode() != cluster.ReplicationMode() {
		allErrs = append(allErrs, field.Invalid(
			path.Child("mode"), cluster.ReplicationMode(),
			"replication.mode is immutable"))
	}

	// A pinned group name is immutable: a changed group_replication_group_name
	// fractures the group.
	oldName := old.groupName()
	newName := cluster.groupName()
	if oldName != "" && newName != "" && oldName != newName {
		allErrs = append(allErrs, field.Invalid(
			path.Child("groupReplication", "groupName"), newName,
			"groupReplication.groupName is immutable once set"))
	}

	allErrs = append(allErrs, cluster.validateSeriesUpgrade(old)...)

	return allErrs
}

// validateSeriesUpgrade guards MySQL server major-version transitions: a series
// change must be expressed through imageCatalogRef (not imageName), and must be
// a single supported hop forward along the upgrade chain — no skips, no
// downgrades. It is best-effort at admission: a series that cannot be determined
// from the spec (e.g. a digest-pinned imageName) is left to the instance-manager
// guard, which knows the actual running server version.
func (cluster *Cluster) validateSeriesUpgrade(old *Cluster) field.ErrorList {
	var allErrs field.ErrorList

	oldSeries, oldOK := old.Spec.targetSeries()
	newSeries, newOK := cluster.Spec.targetSeries()
	if !oldOK || !newOK || oldSeries == newSeries {
		return allErrs
	}

	specPath := field.NewPath("spec")
	if cluster.Spec.ImageCatalogRef == nil {
		allErrs = append(allErrs, field.Invalid(
			specPath.Child("imageName"), cluster.Spec.ImageName,
			fmt.Sprintf("changing the MySQL series (from %d.%d to %d.%d) must be done through imageCatalogRef, not imageName",
				oldSeries.Major, oldSeries.Minor, newSeries.Major, newSeries.Minor)))
		return allErrs
	}
	if err := version.CheckUpgrade(oldSeries, newSeries); err != nil {
		allErrs = append(allErrs, field.Invalid(
			specPath.Child("imageCatalogRef", "series"), cluster.Spec.ImageCatalogRef.Series, err.Error()))
	}
	return allErrs
}

// targetSeries returns the MySQL series the spec targets and whether it could be
// determined. imageCatalogRef.series is authoritative; an imageName is parsed
// best-effort from its tag, so a digest-pinned or non-version tag yields false.
func (spec *ClusterSpec) targetSeries() (version.Version, bool) {
	if spec.ImageCatalogRef != nil {
		if v, err := version.Parse(spec.ImageCatalogRef.Series); err == nil {
			return v.Series(), true
		}
		return version.Version{}, false
	}
	if spec.ImageName != "" {
		if v, err := version.Parse(imageTag(spec.ImageName)); err == nil {
			return v.Series(), true
		}
	}
	return version.Version{}, false
}

// imageTag extracts the tag from a container image reference, dropping any
// digest and registry/repository path. It mirrors the resolver in the
// controller so admission can read a series from imageName without a lookup.
func imageTag(image string) string {
	imageWithoutDigest := strings.SplitN(image, "@", 2)[0]
	lastSlash := strings.LastIndexByte(imageWithoutDigest, '/')
	lastColon := strings.LastIndexByte(imageWithoutDigest, ':')
	if lastColon < 0 || lastColon < lastSlash {
		return ""
	}
	return imageWithoutDigest[lastColon+1:]
}

// ReplicationMode returns the effective replication mode, defaulting to async.
func (cluster *Cluster) ReplicationMode() string {
	if cluster.Spec.Replication == nil || cluster.Spec.Replication.Mode == "" {
		return ReplicationModeAsync
	}
	return cluster.Spec.Replication.Mode
}

// groupName returns the group name pinned in the spec, if any.
func (cluster *Cluster) groupName() string {
	if cluster.Spec.Replication == nil || cluster.Spec.Replication.GroupReplication == nil {
		return ""
	}
	return cluster.Spec.Replication.GroupReplication.GroupName
}

// IsGroupReplication reports whether the cluster runs MySQL Group Replication.
func (cluster *Cluster) IsGroupReplication() bool {
	return cluster.ReplicationMode() == ReplicationModeGroupReplication
}

// PinnedGroupName returns the group_replication_group_name pinned in status, or
// the empty string when it has not been pinned yet.
func (cluster *Cluster) PinnedGroupName() string {
	if cluster.Status.GroupReplication == nil {
		return ""
	}
	return cluster.Status.GroupReplication.GroupName
}

// DesiredGroupName is the Group Replication group name to pin: the user-pinned
// spec.replication.groupReplication.groupName when set, otherwise a freshly
// generated UUID.
func (cluster *Cluster) DesiredGroupName() string {
	name := cluster.groupName()
	if name != "" {
		return name
	}
	return uuid.NewString()
}

// ResolvedGroupReplicationTunables resolves the spec Group Replication tunables
// with their defaults applied, so rendering is correct even when the optional
// spec.replication.groupReplication block (or a field) is omitted.
type ResolvedGroupReplicationTunables struct {
	Consistency     string
	ExitStateAction string
	AutoRejoinTries int
}

// ResolvedGroupReplicationTunables returns the Group Replication tunable values
// with defaults applied.
func (cluster *Cluster) ResolvedGroupReplicationTunables() ResolvedGroupReplicationTunables {
	t := ResolvedGroupReplicationTunables{
		Consistency:     "BEFORE_ON_PRIMARY_FAILOVER",
		ExitStateAction: "READ_ONLY",
		AutoRejoinTries: 3,
	}
	if cluster.Spec.Replication == nil || cluster.Spec.Replication.GroupReplication == nil {
		return t
	}
	cfg := cluster.Spec.Replication.GroupReplication
	if cfg.Consistency != "" {
		t.Consistency = cfg.Consistency
	}
	if cfg.ExitStateAction != "" {
		t.ExitStateAction = cfg.ExitStateAction
	}
	if cfg.AutoRejoinTries != nil {
		t.AutoRejoinTries = int(*cfg.AutoRejoinTries)
	}
	return t
}

// groupNameRe matches a MySQL group_replication_group_name: a canonical UUID.
var groupNameRe = regexp.MustCompile(
	`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// validateReplication checks the replication topology selection: Group
// Replication is incompatible with semi-synchronous replication, a pinned group
// name must be a UUID, and the groupReplication tuning block is only meaningful
// when the mode selects it.
func (spec *ClusterSpec) validateReplication(path *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if spec.Replication == nil {
		return allErrs
	}

	mode := spec.Replication.Mode
	if mode == "" {
		mode = ReplicationModeAsync
	}

	if mode != ReplicationModeGroupReplication {
		// The tuning block is only honoured under Group Replication; flag it rather
		// than silently ignoring a setting the user expected to take effect.
		if spec.Replication.GroupReplication != nil {
			allErrs = append(allErrs, field.Invalid(
				path.Child("groupReplication"), spec.Replication.GroupReplication,
				"groupReplication settings require replication.mode=groupReplication"))
		}
		return allErrs
	}

	// Group Replication has its own group-wide consistency model; the operator's
	// semi-synchronous replication path does not apply and the two must not be
	// combined.
	if spec.MySQL.SemiSync != nil && spec.MySQL.SemiSync.Enabled {
		allErrs = append(allErrs, field.Invalid(
			path.Child("mode"), mode,
			"group replication is incompatible with semi-synchronous replication (spec.mysql.semiSync.enabled)"))
	}

	if gr := spec.Replication.GroupReplication; gr != nil && gr.GroupName != "" {
		if !groupNameRe.MatchString(gr.GroupName) {
			allErrs = append(allErrs, field.Invalid(
				path.Child("groupReplication", "groupName"), gr.GroupName,
				"groupName must be a valid UUID"))
		}
	}

	// MySQL Group Replication requires at least 8.0; reject obviously-too-old
	// catalogs at admission. The authoritative version floor (8.0.22) is enforced
	// by the instance manager before it starts the group, where the full server
	// version is known.
	if spec.ImageCatalogRef != nil {
		if v, err := version.Parse(spec.ImageCatalogRef.Series); err == nil && v.Major < 8 {
			allErrs = append(allErrs, field.Invalid(
				path.Child("mode"), mode,
				fmt.Sprintf("group replication requires MySQL 8.0+, but the image catalog targets series %s",
					spec.ImageCatalogRef.Series)))
		}
	}

	return allErrs
}

// validateManagedRoles checks the declarative managed roles: names must be
// unique per host, must not collide with reserved accounts, must specify a
// host, must not mix superuser with explicit privileges, and must use a valid
// RequireTLS value.
func (spec *ClusterSpec) validateManagedRoles(path *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if spec.Managed == nil || len(spec.Managed.Roles) == 0 {
		return allErrs
	}

	seen := map[string]bool{}
	for i := range spec.Managed.Roles {
		role := &spec.Managed.Roles[i]
		rolePath := path.Index(i)

		if role.Name == "" {
			allErrs = append(allErrs, field.Required(rolePath.Child("name"), "role name is required"))
			continue
		}
		if isReservedRoleName(role.Name) {
			allErrs = append(allErrs, field.Invalid(
				rolePath.Child("name"), role.Name,
				"role name is reserved (root, mysql.*, cnmsql_*)"))
		}
		if role.Host == "" {
			allErrs = append(allErrs, field.Required(rolePath.Child("host"), "role host is required"))
		}
		key := role.Name + "@" + role.Host
		if seen[key] {
			allErrs = append(allErrs, field.Duplicate(rolePath, key))
		}
		seen[key] = true

		if role.Superuser && len(role.Privileges) > 0 {
			allErrs = append(allErrs, field.Invalid(
				rolePath.Child("privileges"), role.Privileges,
				"privileges cannot be set when superuser is true"))
		}
		switch role.RequireTLS {
		case "", "none", "ssl", "x509":
		default:
			allErrs = append(allErrs, field.Invalid(
				rolePath.Child("requireTLS"), role.RequireTLS,
				"requireTLS must be one of none, ssl, x509"))
		}
	}
	return allErrs
}

// isReservedRoleName reports whether a MySQL user name is reserved by MySQL or
// the operator and must not be declared as a managed role.
func isReservedRoleName(name string) bool {
	if name == "root" {
		return true
	}
	return strings.HasPrefix(name, "mysql.") || strings.HasPrefix(name, "cnmsql_")
}

// validateManagedServices checks the user-defined service exposition: the rw
// service cannot be disabled, additional service names must be unique, and
// additional services must not collide with the default service name suffixes.
func (spec *ClusterSpec) validateManagedServices(path *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if spec.Managed == nil || spec.Managed.Services == nil {
		return allErrs
	}
	services := spec.Managed.Services

	for i, disabled := range services.DisabledDefaultServices {
		if disabled == ServiceSelectorTypeRW {
			allErrs = append(allErrs, field.Invalid(
				path.Child("disabledDefaultServices").Index(i), disabled,
				"the rw service cannot be disabled"))
		}
	}

	reserved := map[string]bool{"rw": true, "ro": true, "r": true}
	seen := map[string]bool{}
	for i := range services.Additional {
		svc := &services.Additional[i]
		svcPath := path.Child("additional").Index(i)
		if svc.Name == "" {
			allErrs = append(allErrs, field.Required(
				svcPath.Child("name"), "additional service name is required"))
			continue
		}
		if seen[svc.Name] {
			allErrs = append(allErrs, field.Duplicate(svcPath.Child("name"), svc.Name))
		}
		seen[svc.Name] = true
		if reserved[svc.Name] {
			allErrs = append(allErrs, field.Invalid(
				svcPath.Child("name"), svc.Name,
				"additional service name collides with a default service name (rw, ro, r)"))
		}
	}
	return allErrs
}

// validateBackup checks the backup/continuous-archiving configuration is
// coherent: continuous archiving needs an object store to ship binlogs to.
func (spec *ClusterSpec) validateBackup(path *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if spec.Backup == nil {
		return allErrs
	}
	if spec.Backup.RetentionPolicy != "" {
		if _, err := ParseRetentionPolicy(spec.Backup.RetentionPolicy); err != nil {
			allErrs = append(allErrs, field.Invalid(
				path.Child("retentionPolicy"), spec.Backup.RetentionPolicy, err.Error()))
		} else if spec.Backup.ObjectStore == nil {
			allErrs = append(allErrs, field.Invalid(
				path.Child("retentionPolicy"), spec.Backup.RetentionPolicy,
				"retentionPolicy requires backup.objectStore to be configured"))
		}
	}
	if spec.Backup.ContinuousArchiving == nil {
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
	hasBackup := rec.Backup != nil && rec.Backup.Name != ""
	switch {
	case rec.Source == "" && !hasBackup:
		allErrs = append(allErrs, field.Required(
			path.Child("backup"), "recovery requires a backup reference or source"))
	case rec.Source != "" && hasBackup:
		allErrs = append(allErrs, field.Invalid(
			path.Child("source"), rec.Source,
			"source and backup are mutually exclusive"))
	case rec.Source != "":
		if !spec.hasExternalCluster(rec.Source) {
			allErrs = append(allErrs, field.Invalid(
				path.Child("source"), rec.Source,
				"source must reference an entry in externalClusters"))
		} else if ext := spec.FindExternalCluster(rec.Source); ext != nil && ext.ObjectStore == nil {
			allErrs = append(allErrs, field.Invalid(
				path.Child("source"), rec.Source,
				"external cluster referenced by source must have objectStore configured"))
		}
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
	return spec.FindExternalCluster(name) != nil
}

// FindExternalCluster returns the externalClusters entry with the given name, or
// nil when none matches.
func (spec *ClusterSpec) FindExternalCluster(name string) *ExternalCluster {
	for i := range spec.ExternalClusters {
		if spec.ExternalClusters[i].Name == name {
			return &spec.ExternalClusters[i]
		}
	}
	return nil
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

// IsSwitchoverOnDrainEnabled reports whether the operator should perform a
// planned switchover when the primary Pod is gracefully terminated. It defaults
// to true when unset.
func (cluster *Cluster) IsSwitchoverOnDrainEnabled() bool {
	return cluster.Spec.EnableSwitchoverOnDrain == nil || *cluster.Spec.EnableSwitchoverOnDrain
}

// ShouldResizeInUseVolumes reports whether the operator may grow PVCs that are
// still mounted by a running instance (online expansion). It defaults to true.
//
// When false, the storage backend cannot expand a volume while it is in use, so
// the node-side filesystem resize stays pending until the volume is detached and
// remounted. In that case the operator completes the resize by recycling the
// instance Pod (serialised replica-by-replica, primary last) rather than leaving
// the PVC stuck.
func (cluster *Cluster) ShouldResizeInUseVolumes() bool {
	return cluster.Spec.Storage.ResizeInUseVolumes == nil || *cluster.Spec.Storage.ResizeInUseVolumes
}

// GetMaxStopDelay returns the amount of time in seconds MySQL has to stop.
func (cluster *Cluster) GetMaxStopDelay() int32 {
	if cluster.Spec.MaxStopDelay > 0 {
		return cluster.Spec.MaxStopDelay
	}
	return DefaultShutdownDelay
}

// GetSmartShutdownTimeout returns the timeout reserved for a smart (graceful)
// shutdown attempt before falling back to fast shutdown.
func (cluster *Cluster) GetSmartShutdownTimeout() int32 {
	if cluster.Spec.SmartShutdownTimeout != nil {
		return *cluster.Spec.SmartShutdownTimeout
	}
	return int32(DefaultSmartShutdownTimeout)
}

// IsPrimaryLeaseEnabled reports whether the primary Lease fencing layer is
// active, resolving the default (enabled).
func (cluster *Cluster) IsPrimaryLeaseEnabled() bool {
	return cluster.Spec.EnablePrimaryLease == nil || *cluster.Spec.EnablePrimaryLease
}

// IsEstablished reports whether the cluster has completed initial provisioning
// at least once. It is anchored on status.EstablishedAt (set the first time the
// cluster reaches Ready) rather than on phase, so a cluster that was once
// operational stays established even when an intermediate reconcile re-stamps
// its phase back to Provisioning.
func (cluster *Cluster) IsEstablished() bool {
	return cluster.Status.EstablishedAt != nil
}

// SemiSyncDurabilityPreferred reports whether semi-synchronous data durability
// is "preferred" (the default when unset), under which the operator self-heals
// the acknowledgement count instead of letting writes block.
func (cluster *Cluster) SemiSyncDurabilityPreferred() bool {
	if cluster.Spec.MySQL.SemiSync == nil {
		return true
	}
	return cluster.Spec.MySQL.SemiSync.DataDurability != DataDurabilityRequired
}

// IsSemiSyncEnabled reports whether semi-synchronous replication is configured.
func (cluster *Cluster) IsSemiSyncEnabled() bool {
	return cluster.Spec.MySQL.SemiSync != nil && cluster.Spec.MySQL.SemiSync.Enabled
}

// ContinuousArchiving returns the cluster's continuous-archiving configuration,
// or nil when it is not configured.
func (cluster *Cluster) ContinuousArchiving() *ContinuousArchivingConfiguration {
	if cluster.Spec.Backup == nil {
		return nil
	}
	return cluster.Spec.Backup.ContinuousArchiving
}

// IsArchivingEnabled reports whether continuous binlog archiving is turned on
// and has a destination object store to ship to.
func (cluster *Cluster) IsArchivingEnabled() bool {
	ca := cluster.ContinuousArchiving()
	return ca != nil && ca.Enabled &&
		cluster.Spec.Backup != nil && cluster.Spec.Backup.ObjectStore != nil
}

// ArchiveRPOSeconds returns the configured RPO bound in seconds, defaulting to
// 300 (5 minutes).
func (cluster *Cluster) ArchiveRPOSeconds() int {
	ca := cluster.ContinuousArchiving()
	if ca == nil || ca.TargetRPOSeconds <= 0 {
		return 300
	}
	return int(ca.TargetRPOSeconds)
}
