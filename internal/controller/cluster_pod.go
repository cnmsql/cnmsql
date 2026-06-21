/*
Copyright 2026 The CloudNative MySQL Authors.

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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
	mysqlconfig "github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/config"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/objectstore"
)

func (r *ClusterReconciler) podSpec(cluster *mysqlv1alpha1.Cluster, plan clusterPlan, inst instancePlan) corev1.PodSpec {
	gracePeriod := int64(cluster.GetMaxStopDelay())

	operatorImage := plan.OperatorImage
	if operatorImage == "" {
		operatorImage = plan.Image
	}

	podSpec := corev1.PodSpec{
		RestartPolicy:                 corev1.RestartPolicyAlways,
		TerminationGracePeriodSeconds: &gracePeriod,
		ServiceAccountName:            instanceServiceAccountName(inst),
		Volumes: []corev1.Volume{
			{Name: "scratch-data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: inst.PVCName}}},
			{Name: "run", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			{Name: "backup", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: inst.ConfigMapName}}}},
			{Name: "server-tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: inst.ServerTLSSecret}}},
			{Name: "client-ca", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: plan.ClientCASecretName}}},
		},
		InitContainers: []corev1.Container{
			{
				Name:            "bootstrap-controller",
				Image:           operatorImage,
				ImagePullPolicy: cluster.Spec.ImagePullPolicy,
				Command:         []string{"/manager"},
				Args:            []string{"bootstrap", "/controller/manager"},
				VolumeMounts:    volumeMounts(),
				Resources:       cluster.Spec.Resources,
				SecurityContext: cluster.Spec.SecurityContext,
			},
			{
				Name:            "bootstrap",
				Image:           plan.Image,
				ImagePullPolicy: cluster.Spec.ImagePullPolicy,
				Command:         []string{"/controller/manager"},
				Args:            r.bootstrapArgs(cluster, plan, inst),
				Env:             bootstrapEnv(plan, inst),
				VolumeMounts:    volumeMounts(),
				Resources:       cluster.Spec.Resources,
				SecurityContext: cluster.Spec.SecurityContext,
			},
		},
		Containers: []corev1.Container{{
			Name:            "mysql",
			Image:           plan.Image,
			ImagePullPolicy: cluster.Spec.ImagePullPolicy,
			Command:         []string{"/controller/manager"},
			Args:            r.runArgs(cluster, plan, inst),
			Env:             runEnv(cluster, plan),
			EnvFrom:         cluster.Spec.EnvFrom,
			Ports: []corev1.ContainerPort{
				{Name: "mysql", ContainerPort: 3306},
				{Name: "control", ContainerPort: 8080},
				{Name: "health", ContainerPort: 8081},
				{Name: metricsPortName, ContainerPort: 9187},
			},
			VolumeMounts:    volumeMounts(),
			Resources:       cluster.Spec.Resources,
			SecurityContext: cluster.Spec.SecurityContext,
			// /readyz and /livez run a live MySQL ping plus SHOW REPLICA STATUS, so
			// under CPU/IO pressure (e.g. several instances bootstrapping on a small
			// CI node) a single probe can take longer than the 1s Kubernetes default
			// TimeoutSeconds. With the default 3-failure threshold that briefly flips
			// the primary NotReady, which makes the operator treat it as failed and
			// enter failover handling before replicas are even provisioned, so a
			// cluster can stall at "1/3 ready" indefinitely. Set explicit, generous
			// timeouts and failure thresholds so transient slowness does not flap
			// readiness or restart a busy-but-healthy mysqld.
			ReadinessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
					Path: "/readyz",
					Port: intstr.FromString("health"),
				}},
				PeriodSeconds:    2,
				TimeoutSeconds:   5,
				FailureThreshold: 3,
			},
			LivenessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
					Path: "/livez",
					Port: intstr.FromString("health"),
				}},
				PeriodSeconds:    10,
				TimeoutSeconds:   5,
				FailureThreshold: 6,
			},
			StartupProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
					Path: "/startupz",
					Port: intstr.FromString("health"),
				}},
				PeriodSeconds:    2,
				TimeoutSeconds:   5,
				FailureThreshold: 90,
			},
		}},
		NodeSelector:              cluster.Spec.Affinity.NodeSelector,
		Affinity:                  affinity(cluster),
		Tolerations:               cluster.Spec.Affinity.Tolerations,
		TopologySpreadConstraints: cluster.Spec.TopologySpreadConstraints,
		PriorityClassName:         cluster.Spec.PriorityClassName,
		SchedulerName:             cluster.Spec.SchedulerName,
		SecurityContext:           podSecurityContext(cluster),
	}
	for _, pullSecret := range cluster.Spec.ImagePullSecrets {
		podSpec.ImagePullSecrets = append(podSpec.ImagePullSecrets, corev1.LocalObjectReference{Name: pullSecret.Name})
	}
	podSpec.Containers[0].Env = append(podSpec.Containers[0].Env, cluster.Spec.Env...)
	return podSpec
}

// bootstrapArgs returns the init-container command: the primary initialises a
// fresh data dir (initdb) or restores a physical backup from object storage
// (recovery); an async replica clones the primary over the streamed backup,
// while a Group Replication member initialises an empty server and provisions
// itself from a group donor via distributed recovery at run time.
func (r *ClusterReconciler) bootstrapArgs(cluster *mysqlv1alpha1.Cluster, plan clusterPlan, inst instancePlan) []string {
	if inst.IsPrimary {
		if plan.Recovery != nil {
			return restoreArgs(plan)
		}
		return r.initdbArgs(cluster, cluster.Spec.Bootstrap.InitDB)
	}
	if r.topologyReconciler(cluster).PodPolicy(cluster).InitializeReplica {
		// A GR member is not seeded by an XtraBackup clone of the primary (which
		// would configure an async replication channel and copy the donor's
		// server_uuid, both of which break START GROUP_REPLICATION). It initialises
		// an empty server with the cluster's internal accounts but no application
		// schema, then the in-Pod role strategy clears the initdb GTIDs and clones
		// from a group donor via distributed recovery.
		return r.initdbArgs(cluster, nil)
	}
	return joinArgs(cluster, plan)
}

// restoreArgs builds the recovering primary's init-container command: download
// and restore a physical backup from object storage into the data directory,
// then (for point-in-time recovery) replay archived binlogs up to the target.
func restoreArgs(plan clusterPlan) []string {
	args := []string{
		"instance", "restore",
		"--data-dir=" + dataDir,
		"--backup-dir=" + joinBackupDir,
		"--bucket=" + plan.Recovery.Bucket,
		"--archive-key=" + plan.Recovery.ArchiveKey,
		"--metadata-key=" + plan.Recovery.MetadataKey,
		// Reset the restored internal accounts to this cluster's generated
		// credentials so the instance manager can authenticate post-recovery.
		"--mysqld=" + mysqldBinary,
		"--config=" + configPath,
		"--socket=" + socketPath,
		"--server-version=$(MYSQL_VERSION)",
		"--control-user=" + controlUser,
		"--backup-user=" + backupUser,
	}
	// Point-in-time recovery: replay archived binlogs after the base restore.
	// --source-cluster enables the replay; bucket/path come from cloudnative-mysql_S3_* env.
	if plan.Recovery.HasTarget {
		args = append(args, "--source-cluster="+plan.Recovery.SourceCluster)
		if plan.Recovery.TargetTime != "" {
			args = append(args, "--target-time="+plan.Recovery.TargetTime)
		}
		if plan.Recovery.TargetGTID != "" {
			args = append(args, "--target-gtid="+plan.Recovery.TargetGTID)
		}
		if plan.Recovery.TargetImmediate {
			args = append(args, "--target-immediate")
		}
	}
	return args
}

func (r *ClusterReconciler) initdbArgs(cluster *mysqlv1alpha1.Cluster, initdb *mysqlv1alpha1.BootstrapInitDB) []string {
	args := []string{
		"instance", "initdb",
		"--mysqld=" + mysqldBinary,
		"--config=" + configPath,
		"--data-dir=" + dataDir,
		"--socket=" + socketPath,
		"--server-version=$(MYSQL_VERSION)",
		"--replication-user=" + replicationUser,
		"--replication-require-x509",
		"--backup-user=" + backupUser,
		"--control-user=" + controlUser,
		"--metrics-user=" + metricsUser,
	}
	args = append(args, r.topologyReconciler(cluster).PodPolicy(cluster).InitDBArgs...)
	// initdb is nil for a GR joining member: it initialises an empty server (no
	// application schema) and recovers the data from a group donor.
	if initdb != nil {
		if initdb.Database != "" {
			args = append(args, "--database="+initdb.Database)
		}
		if initdb.Owner != "" {
			args = append(args, "--owner="+initdb.Owner)
		}
		if initdb.CharacterSet != "" {
			args = append(args, "--character-set="+initdb.CharacterSet)
		}
		if initdb.Collation != "" {
			args = append(args, "--collation="+initdb.Collation)
		}
	}
	return args
}

// joinArgs builds the replica's init-container command: pull and restore a
// streamed backup from the primary over mTLS, then configure GTID replication.
func joinArgs(cluster *mysqlv1alpha1.Cluster, plan clusterPlan) []string {
	primaryFQDN := plan.primaryName(cluster) + "." + cluster.Namespace + ".svc"
	return []string{
		"instance", "join",
		"--mysqld=" + mysqldBinary,
		"--config=" + configPath,
		"--data-dir=" + dataDir,
		"--socket=" + socketPath,
		"--server-version=$(MYSQL_VERSION)",
		"--backup-dir=" + joinBackupDir,
		"--source-host=" + primaryFQDN,
		"--source-port=3306",
		"--replication-user=" + replicationUser,
		"--source-ssl",
		"--source-ssl-ca=" + topology.ClientCAPath + "/ca.crt",
		"--source-ssl-cert=" + topology.ServerTLSPath + "/tls.crt",
		"--source-ssl-key=" + topology.ServerTLSPath + "/tls.key",
		"--source-manager-url=https://" + primaryFQDN + ":8080/cluster/backup",
		"--source-manager-server-name=" + primaryFQDN,
	}
}

func (r *ClusterReconciler) runArgs(cluster *mysqlv1alpha1.Cluster, _ clusterPlan, _ instancePlan) []string {
	// Role is dynamic: the in-Pod reconciler watches the Cluster and drives the
	// local mysqld to match status.targetPrimary / currentPrimary. The run
	// command therefore carries no --role/--source-host; it gets the owning
	// Cluster identity and the static replication connection parameters (the
	// source host is derived from currentPrimary at runtime).
	args := []string{
		"instance", "run",
		"--mysqld=" + mysqldBinary,
		"--config=" + configPath,
		"--data-dir=" + dataDir,
		"--socket=" + socketPath,
		"--server-version=$(MYSQL_VERSION)",
		"--instance-name=$(POD_NAME)",
		"--cluster-name=" + cluster.Name,
		"--namespace=$(POD_NAMESPACE)",
		"--control-user=" + controlUser,
		"--backup-user=" + backupUser,
		"--admin-address=" + mysqlconfig.DefaultAdminAddress,
		fmt.Sprintf("--admin-port=%d", mysqlconfig.DefaultAdminPort),
		"--web-addr=:8080",
		"--health-addr=:8081",
		"--tls-cert=" + topology.ServerTLSPath + "/tls.crt",
		"--tls-key=" + topology.ServerTLSPath + "/tls.key",
		"--tls-client-ca=" + topology.ClientCAPath + "/ca.crt",
		"--source-port=3306",
		"--replication-user=" + replicationUser,
		"--source-ssl",
		"--source-ssl-ca=" + topology.ClientCAPath + "/ca.crt",
		"--source-ssl-cert=" + topology.ServerTLSPath + "/tls.crt",
		"--source-ssl-key=" + topology.ServerTLSPath + "/tls.key",
	}
	args = append(args, r.topologyReconciler(cluster).PodPolicy(cluster).RunArgs...)
	if monitoringTLSEnabled(cluster) {
		// Serve metrics over the same mutual TLS as the control API: the pod
		// already mounts server-tls and client-ca, so no extra key material is
		// needed. Prometheus authenticates with the operator's client cert.
		args = append(args, "--metrics-tls")
	}
	if cluster.IsArchivingEnabled() {
		args = append(args,
			"--continuous-archiving",
			fmt.Sprintf("--archive-rpo-seconds=%d", cluster.ArchiveRPOSeconds()),
		)
	}
	args = append(args,
		fmt.Sprintf("--stop-delay=%d", cluster.GetMaxStopDelay()),
		fmt.Sprintf("--smart-shutdown-timeout=%d", cluster.GetSmartShutdownTimeout()),
	)
	return args
}

// bootstrapEnv is the init-container environment. The recovering primary also
// gets the object-store credentials its restore worker needs.
func bootstrapEnv(plan clusterPlan, inst instancePlan) []corev1.EnvVar {
	env := initEnv(plan)
	if inst.IsPrimary && plan.Recovery != nil {
		env = append(env, plan.Recovery.StoreEnv...)
	}
	return env
}

// initEnv is the environment for the init container, which may run initdb (on
// the primary) or join (on a replica). Replication uses mTLS-only auth, so the
// generated replication password is deliberately not exposed to pods.
func initEnv(plan clusterPlan) []corev1.EnvVar {
	env := runEnv(nil, plan)
	env = append(env, secretEnv("MYSQL_ROOT_PASSWORD", plan.RootSecretName))
	// On recovery the application user comes from the restored data, so no app
	// secret is generated (see ensureCredentials) and the non-optional secret
	// reference would otherwise wedge the Pod in CreateContainerConfigError.
	if plan.Recovery == nil {
		env = append(env, secretEnv("MYSQL_APP_PASSWORD", plan.AppSecretName))
	}
	return env
}

// runEnv is the environment for the run container. When cluster has continuous
// archiving enabled, the object-store credentials and destination (bucket/path)
// are appended so the in-Pod archiver can ship binlogs. cluster may be nil for
// the init container, which never archives.
func runEnv(cluster *mysqlv1alpha1.Cluster, plan clusterPlan) []corev1.EnvVar {
	env := []corev1.EnvVar{
		{Name: "MYSQL_VERSION", Value: plan.ServerVersion},
		{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
		secretEnv("MYSQL_CONTROL_PASSWORD", plan.ControlSecretName),
		secretEnv("MYSQL_BACKUP_PASSWORD", plan.BackupSecretName),
	}
	if cluster != nil && cluster.IsArchivingEnabled() {
		store := *cluster.Spec.Backup.ObjectStore
		env = append(env, backupObjectStoreEnv(store)...)
		env = append(env,
			corev1.EnvVar{Name: objectstore.EnvBucket, Value: store.Bucket},
			corev1.EnvVar{Name: objectstore.EnvPath, Value: store.Path},
		)
	}
	return env
}

func secretEnv(name, secretName string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: name,
		ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
			Key:                  "password",
		}},
	}
}

func volumeMounts() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{Name: "scratch-data", MountPath: "/controller"},
		{Name: "data", MountPath: dataDir},
		{Name: "run", MountPath: "/var/run/mysqld"},
		{Name: "backup", MountPath: joinBackupDir},
		{Name: "config", MountPath: configPath, SubPath: "my.cnf", ReadOnly: true},
		{Name: "server-tls", MountPath: topology.ServerTLSPath, ReadOnly: true},
		{Name: "client-ca", MountPath: topology.ClientCAPath, ReadOnly: true},
	}
}

func affinity(cluster *mysqlv1alpha1.Cluster) *corev1.Affinity {
	if cluster.Spec.Affinity.NodeAffinity == nil &&
		cluster.Spec.Affinity.AdditionalPodAffinity == nil &&
		cluster.Spec.Affinity.AdditionalPodAntiAffinity == nil {
		return nil
	}
	return &corev1.Affinity{
		NodeAffinity:    cluster.Spec.Affinity.NodeAffinity,
		PodAffinity:     cluster.Spec.Affinity.AdditionalPodAffinity,
		PodAntiAffinity: cluster.Spec.Affinity.AdditionalPodAntiAffinity,
	}
}

func podSecurityContext(cluster *mysqlv1alpha1.Cluster) *corev1.PodSecurityContext {
	if cluster.Spec.PodSecurityContext != nil {
		return cluster.Spec.PodSecurityContext
	}
	runAsNonRoot := true
	runAsUser := int64(1001)
	fsGroup := int64(0)
	return &corev1.PodSecurityContext{
		RunAsNonRoot: &runAsNonRoot,
		RunAsUser:    &runAsUser,
		FSGroup:      &fsGroup,
	}
}
