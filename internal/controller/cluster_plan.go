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

package controller

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/objectstore"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/version"
)

type clusterPlan struct {
	Image         string
	ServerVersion string
	// Instances is the desired number of MySQL instances (1 primary + replicas).
	Instances int
	// PrimaryName is the instance currently expected to be primary.
	PrimaryName string

	// Cluster-wide secret names.
	RootSecretName    string
	AppSecretName     string
	ReplicationSecret string
	ControlSecretName string
	BackupSecretName  string

	// Cluster-wide cert-manager material.
	SelfSignedIssuer string
	CAIssuer         string
	CASecretName     string
	// ClientTLSSecret holds the operator's client certificate used to call each
	// instance's control API.
	ClientTLSSecret string
	// UserServerTLSSecret, when set, is a user-provided server certificate used
	// for every instance instead of generating per-instance certs.
	UserServerTLSSecret string

	// Default traffic-routing services.
	RWServiceName    string
	ROServiceName    string
	RServiceName     string
	DisabledServices map[mysqlv1alpha1.ServiceSelectorType]bool

	// InstanceServiceAccount is the ServiceAccount instance Pods run as so their
	// in-Pod reconciler can watch this Cluster and patch its status.
	InstanceServiceAccount string

	// Recovery, when set, makes the bootstrap primary restore from an object
	// store instead of running initdb. Replicas always clone from the primary.
	Recovery *recoveryPlan
}

// recoveryPlan locates the physical backup the bootstrap primary restores from.
type recoveryPlan struct {
	Bucket      string
	ArchiveKey  string
	MetadataKey string
	// StoreEnv carries the CNMYSQL_S3_* environment (endpoint, region, signing,
	// credentials, bucket, path) the restore worker needs to reach the object
	// store and reconstruct binlog archive keys.
	StoreEnv []corev1.EnvVar

	// The fields below drive point-in-time recovery (M7.2). HasTarget is set when
	// spec.bootstrap.recovery.recoveryTarget is present; the restore worker then
	// replays archived binlogs from SourceCluster's archive up to the target.
	HasTarget       bool
	SourceCluster   string
	TargetTime      string
	TargetGTID      string
	TargetImmediate bool
	// Store is the resolved (defaulted) recovery object store, used by the
	// operator's up-front recovery-target satisfiability check.
	Store mysqlv1alpha1.S3ObjectStore
}

// instancePlan holds the per-instance derived names and identity.
type instancePlan struct {
	Name            string
	Ordinal         int
	ServerID        int
	IsPrimary       bool
	PVCName         string
	ConfigMapName   string
	ServiceName     string
	ServerCertName  string
	ServerTLSSecret string
}

// primaryName is the expected primary, falling back to the bootstrap instance.
func (p clusterPlan) primaryName(cluster *mysqlv1alpha1.Cluster) string {
	if p.PrimaryName != "" {
		return p.PrimaryName
	}
	return instanceName(cluster, 1)
}

// instanceNames lists the desired instance names in ordinal order.
func (p clusterPlan) instanceNames(cluster *mysqlv1alpha1.Cluster) []string {
	names := make([]string, 0, p.Instances)
	for i := 1; i <= p.Instances; i++ {
		names = append(names, instanceName(cluster, i))
	}
	return names
}

// instanceFor derives the per-instance plan for the given 1-based ordinal.
func (p clusterPlan) instanceFor(cluster *mysqlv1alpha1.Cluster, ordinal int) instancePlan {
	name := instanceName(cluster, ordinal)
	inst := instancePlan{
		Name:            name,
		Ordinal:         ordinal,
		ServerID:        ordinal,
		IsPrimary:       name == p.primaryName(cluster),
		PVCName:         name,
		ConfigMapName:   name + "-config",
		ServiceName:     name,
		ServerCertName:  name + "-server",
		ServerTLSSecret: name + "-server-tls",
	}
	if p.UserServerTLSSecret != "" {
		inst.ServerTLSSecret = p.UserServerTLSSecret
	}
	return inst
}

func instanceName(cluster *mysqlv1alpha1.Cluster, ordinal int) string {
	return fmt.Sprintf("%s-%d", cluster.Name, ordinal)
}

const (
	defaultMySQL80ServerVersion = "8.0.46"
	defaultMySQL84ServerVersion = "8.4.0"
	defaultMySQL9xServerVersion = "9.6.0"
)

func unsupportedReason(cluster *mysqlv1alpha1.Cluster) string {
	switch {
	case cluster.Spec.Instances < 1:
		return "spec.instances must be at least 1"
	case cluster.Spec.Bootstrap == nil:
		return "spec.bootstrap.initdb or spec.bootstrap.recovery is required"
	case cluster.Spec.Bootstrap.InitDB == nil && cluster.Spec.Bootstrap.Recovery == nil:
		return "spec.bootstrap.initdb or spec.bootstrap.recovery is required"
	case cluster.Spec.Bootstrap.Recovery != nil && cluster.Spec.Bootstrap.Recovery.Backup == nil:
		return "spec.bootstrap.recovery requires a backup reference"
	case cluster.Spec.Replica != nil:
		return "replica clusters (following an external source) are kept for a later milestone"
	case cluster.Spec.BinlogStorage != nil:
		return "separate binlog storage is kept for a later milestone"
	}
	return ""
}

func (r *ClusterReconciler) buildPlan(ctx context.Context, cluster *mysqlv1alpha1.Cluster) (clusterPlan, error) {
	image, err := r.resolveImage(ctx, cluster)
	if err != nil {
		return clusterPlan{}, err
	}
	serverVersion, err := resolveServerVersion(image)
	if err != nil {
		return clusterPlan{}, err
	}

	certs := cluster.Spec.Certificates
	plan := clusterPlan{
		Image:             image,
		ServerVersion:     serverVersion,
		Instances:         cluster.Spec.Instances,
		PrimaryName:       cluster.Status.CurrentPrimary,
		RootSecretName:    cluster.Name + "-root",
		AppSecretName:     cluster.Name + "-app",
		ReplicationSecret: cluster.Name + "-replication",
		ControlSecretName: cluster.Name + "-control",
		BackupSecretName:  cluster.Name + "-backup",
		SelfSignedIssuer:  cluster.Name + "-selfsigned",
		CAIssuer:          cluster.Name + "-ca",
		CASecretName:      cluster.Name + "-ca",
		ClientTLSSecret:   cluster.Name + "-client-tls",
		RWServiceName:     cluster.Name + "-rw",
		ROServiceName:     cluster.Name + "-ro",
		RServiceName:      cluster.Name + "-r",
		DisabledServices:  disabledServices(cluster),

		InstanceServiceAccount: cluster.Name + "-instance",
	}
	if plan.Instances == 0 {
		plan.Instances = 1
	}
	if plan.PrimaryName == "" {
		plan.PrimaryName = instanceName(cluster, 1)
	}
	if cluster.Spec.RootPasswordSecret != nil && cluster.Spec.RootPasswordSecret.Name != "" {
		plan.RootSecretName = cluster.Spec.RootPasswordSecret.Name
	}
	if initdb := cluster.Spec.Bootstrap.InitDB; initdb != nil && initdb.Secret != nil && initdb.Secret.Name != "" {
		plan.AppSecretName = initdb.Secret.Name
	}
	if certs != nil {
		if certs.ServerCASecret != "" {
			plan.CASecretName = certs.ServerCASecret
		}
		if certs.ClientCASecret != "" {
			plan.CASecretName = certs.ClientCASecret
		}
		if certs.ServerTLSSecret != "" {
			plan.UserServerTLSSecret = certs.ServerTLSSecret
		}
		if certs.ReplicationTLSSecret != "" {
			plan.ClientTLSSecret = certs.ReplicationTLSSecret
		}
	}

	recovery, err := r.resolveRecovery(ctx, cluster)
	if err != nil {
		return clusterPlan{}, err
	}
	plan.Recovery = recovery
	return plan, nil
}

// resolveRecovery locates the object store and archive keys the bootstrap
// primary restores from when spec.bootstrap.recovery is set. It returns nil when
// recovery is not configured.
//
// The referenced Backup must stay present and completed for as long as the
// Cluster references it: its status carries the backupID the archive keys are
// derived from, and the recovery init-container's spec depends on those keys.
func (r *ClusterReconciler) resolveRecovery(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
) (*recoveryPlan, error) {
	if cluster.Spec.Bootstrap == nil || cluster.Spec.Bootstrap.Recovery == nil {
		return nil, nil
	}
	rec := cluster.Spec.Bootstrap.Recovery
	if rec.Backup == nil || rec.Backup.Name == "" {
		return nil, fmt.Errorf("bootstrap.recovery requires a backup reference")
	}

	backup := &mysqlv1alpha1.Backup{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: rec.Backup.Name}
	if err := r.Get(ctx, key, backup); err != nil {
		return nil, fmt.Errorf("resolving recovery backup %q: %w", rec.Backup.Name, err)
	}
	if backup.Status.Phase != mysqlv1alpha1.BackupPhaseCompleted {
		return nil, fmt.Errorf("recovery backup %q is not completed (phase %q)", backup.Name, backup.Status.Phase)
	}
	if backup.Status.BackupID == "" {
		return nil, fmt.Errorf("recovery backup %q has no backupID", backup.Name)
	}

	store, err := recoveryObjectStore(cluster, backup)
	if err != nil {
		return nil, err
	}
	store.SetDefaults()

	keys, err := objectstore.BuildBackupKeys(*store, backup.Spec.Cluster.Name, backup.Name, backup.Status.BackupID)
	if err != nil {
		return nil, err
	}

	// The binlog archive is partitioned under the source cluster's prefix; the
	// restore worker reconstructs its keys from the bucket/path env plus this name.
	sourceCluster := backup.Spec.Cluster.Name
	storeEnv := append(backupObjectStoreEnv(*store),
		corev1.EnvVar{Name: objectstore.EnvBucket, Value: store.Bucket},
		corev1.EnvVar{Name: objectstore.EnvPath, Value: store.Path},
	)

	plan := &recoveryPlan{
		Bucket:        store.Bucket,
		ArchiveKey:    keys.ArchiveKey,
		MetadataKey:   keys.MetadataKey,
		StoreEnv:      storeEnv,
		SourceCluster: sourceCluster,
		Store:         *store,
	}
	if target := rec.RecoveryTarget; target != nil {
		plan.HasTarget = true
		plan.TargetTime = target.TargetTime
		plan.TargetGTID = target.TargetGTID
		plan.TargetImmediate = target.TargetImmediate != nil && *target.TargetImmediate
	}
	return plan, nil
}

// recoveryObjectStore picks the object store to recover from: the Backup's own
// override if set, otherwise the recovering cluster's backup object store
// (same-cluster disaster recovery).
func recoveryObjectStore(cluster *mysqlv1alpha1.Cluster, backup *mysqlv1alpha1.Backup) (*mysqlv1alpha1.S3ObjectStore, error) {
	if backup.Spec.ObjectStore != nil {
		return backup.Spec.ObjectStore.DeepCopy(), nil
	}
	if cluster.Spec.Backup != nil && cluster.Spec.Backup.ObjectStore != nil {
		return cluster.Spec.Backup.ObjectStore.DeepCopy(), nil
	}
	return nil, fmt.Errorf(
		"recovery backup %q has no object store and cluster has no spec.backup.objectStore", backup.Name)
}

// disabledServices indexes the default services the user turned off.
func disabledServices(cluster *mysqlv1alpha1.Cluster) map[mysqlv1alpha1.ServiceSelectorType]bool {
	disabled := map[mysqlv1alpha1.ServiceSelectorType]bool{}
	if cluster.Spec.Managed == nil || cluster.Spec.Managed.Services == nil {
		return disabled
	}
	for _, s := range cluster.Spec.Managed.Services.DisabledDefaultServices {
		disabled[s] = true
	}
	return disabled
}

func (r *ClusterReconciler) resolveImage(ctx context.Context, cluster *mysqlv1alpha1.Cluster) (string, error) {
	if cluster.Spec.ImageName != "" {
		return cluster.Spec.ImageName, nil
	}
	if ref := cluster.Spec.ImageCatalogRef; ref != nil {
		switch ref.Kind {
		case "ImageCatalog", "":
			catalog := &mysqlv1alpha1.ImageCatalog{}
			if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: ref.Name}, catalog); err != nil {
				return "", err
			}
			if image, ok := catalog.Spec.FindImageForMajor(ref.Major); ok {
				return image, nil
			}
		case "ClusterImageCatalog":
			catalog := &mysqlv1alpha1.ClusterImageCatalog{}
			if err := r.Get(ctx, types.NamespacedName{Name: ref.Name}, catalog); err != nil {
				return "", err
			}
			if image, ok := catalog.Spec.FindImageForMajor(ref.Major); ok {
				return image, nil
			}
		default:
			return "", fmt.Errorf("unsupported imageCatalogRef kind %q", ref.Kind)
		}
		return "", fmt.Errorf("no image for MySQL major %d in catalog %s", ref.Major, ref.Name)
	}
	return defaultInstanceImage, nil
}

func resolveServerVersion(image string) (string, error) {
	tag := imageTag(image)
	switch tag {
	case "8.0":
		return defaultMySQL80ServerVersion, nil
	case "8.4":
		return defaultMySQL84ServerVersion, nil
	case "9.x":
		return defaultMySQL9xServerVersion, nil
	}
	parsed, err := version.Parse(tag)
	if err != nil {
		return "", fmt.Errorf("cannot resolve MySQL server version from image %q: %w", image, err)
	}
	if parsed.Major == 5 && parsed.Minor == 6 {
		return "", fmt.Errorf("MySQL 5.6 is not supported")
	}
	return tag, nil
}

func imageTag(image string) string {
	imageWithoutDigest := strings.SplitN(image, "@", 2)[0]
	lastSlash := strings.LastIndexByte(imageWithoutDigest, '/')
	lastColon := strings.LastIndexByte(imageWithoutDigest, ':')
	if lastColon <= lastSlash {
		return ""
	}
	return imageWithoutDigest[lastColon+1:]
}
