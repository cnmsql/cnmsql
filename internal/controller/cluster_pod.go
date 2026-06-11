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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
	mysqlconfig "github.com/yyewolf/cnmysql/pkg/management/mysql/config"
)

func (r *ClusterReconciler) podSpec(cluster *mysqlv1alpha1.Cluster, plan clusterPlan) corev1.PodSpec {
	initdb := cluster.Spec.Bootstrap.InitDB
	podSpec := corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyAlways,
		Volumes: []corev1.Volume{
			{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: plan.DataPVCName}}},
			{Name: "run", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: plan.ConfigMapName}}}},
			{Name: "server-tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: plan.ServerTLSSecret}}},
			{Name: "client-ca", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: plan.CASecretName}}},
		},
		InitContainers: []corev1.Container{{
			Name:            "initdb",
			Image:           plan.Image,
			ImagePullPolicy: cluster.Spec.ImagePullPolicy,
			Args:            initdbArgs(initdb),
			Env:             initdbEnv(plan),
			VolumeMounts:    volumeMounts(),
			Resources:       cluster.Spec.Resources,
			SecurityContext: cluster.Spec.SecurityContext,
		}},
		Containers: []corev1.Container{{
			Name:            "mysql",
			Image:           plan.Image,
			ImagePullPolicy: cluster.Spec.ImagePullPolicy,
			Args:            runArgs(),
			Env:             runEnv(plan),
			EnvFrom:         cluster.Spec.EnvFrom,
			Ports: []corev1.ContainerPort{
				{Name: "mysql", ContainerPort: 3306},
				{Name: "control", ContainerPort: 8080},
			},
			VolumeMounts:    volumeMounts(),
			Resources:       cluster.Spec.Resources,
			SecurityContext: cluster.Spec.SecurityContext,
			ReadinessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{
					Port: intstr.FromString("control"),
				}},
				PeriodSeconds: 10,
			},
			LivenessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{
					Port: intstr.FromString("control"),
				}},
				PeriodSeconds: 30,
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

func initdbArgs(initdb *mysqlv1alpha1.BootstrapInitDB) []string {
	args := []string{
		"instance", "initdb",
		"--mysqld=/usr/sbin/mysqld",
		"--config=" + configPath,
		"--data-dir=" + dataDir,
		"--socket=" + socketPath,
		"--server-version=$(MYSQL_VERSION)",
		"--replication-user=cnmysql_repl",
		"--replication-require-x509",
		"--control-user=cnmysql_control",
	}
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
	return args
}

func runArgs() []string {
	return []string{
		"instance", "run",
		"--mysqld=/usr/sbin/mysqld",
		"--config=" + configPath,
		"--data-dir=" + dataDir,
		"--socket=" + socketPath,
		"--server-version=$(MYSQL_VERSION)",
		"--instance-name=$(POD_NAME)",
		"--control-user=cnmysql_control",
		"--admin-address=" + mysqlconfig.DefaultAdminAddress,
		fmt.Sprintf("--admin-port=%d", mysqlconfig.DefaultAdminPort),
		"--web-addr=:8080",
		"--tls-cert=" + serverTLSPath + "/tls.crt",
		"--tls-key=" + serverTLSPath + "/tls.key",
		"--tls-client-ca=" + clientCAPath + "/ca.crt",
	}
}

func initdbEnv(plan clusterPlan) []corev1.EnvVar {
	env := runEnv(plan)
	env = append(env,
		secretEnv("MYSQL_ROOT_PASSWORD", plan.RootSecretName),
		secretEnv("MYSQL_APP_PASSWORD", plan.AppSecretName),
		secretEnv("MYSQL_REPLICATION_PASSWORD", plan.ReplicationSecret),
	)
	return env
}

func runEnv(plan clusterPlan) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "MYSQL_VERSION", Value: plan.ServerVersion},
		{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		secretEnv("MYSQL_CONTROL_PASSWORD", plan.ControlSecretName),
	}
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
		{Name: "data", MountPath: dataDir},
		{Name: "run", MountPath: "/var/run/mysqld"},
		{Name: "config", MountPath: configPath, SubPath: "my.cnf", ReadOnly: true},
		{Name: "server-tls", MountPath: serverTLSPath, ReadOnly: true},
		{Name: "client-ca", MountPath: clientCAPath, ReadOnly: true},
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
