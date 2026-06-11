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

	"k8s.io/apimachinery/pkg/types"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/version"
)

type clusterPlan struct {
	Image         string
	ServerVersion string
	// Instances is the desired number of MySQL instances (1 primary + replicas).
	Instances int

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

// primaryName is the bootstrap instance and, throughout M4, the fixed primary.
func (p clusterPlan) primaryName(cluster *mysqlv1alpha1.Cluster) string {
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
		IsPrimary:       ordinal == 1,
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
	defaultMySQL56ServerVersion = "5.6.51"
	defaultMySQL80ServerVersion = "8.0.46"
	defaultMySQL84ServerVersion = "8.4.0"
	defaultMySQL9xServerVersion = "9.6.0"
)

func unsupportedReason(cluster *mysqlv1alpha1.Cluster) string {
	switch {
	case cluster.Spec.Instances < 1:
		return "spec.instances must be at least 1"
	case cluster.Spec.Bootstrap == nil || cluster.Spec.Bootstrap.InitDB == nil:
		return "M4 supports only bootstrap.initdb; recovery is kept for M6"
	case cluster.Spec.Bootstrap.Recovery != nil:
		return "bootstrap.recovery is kept for M6"
	case cluster.Spec.Replica != nil:
		return "replica clusters (following an external source) are kept for a later milestone"
	case cluster.Spec.BinlogStorage != nil:
		return "separate binlog storage is kept for M6"
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
	}
	if plan.Instances == 0 {
		plan.Instances = 1
	}
	if cluster.Spec.RootPasswordSecret != nil && cluster.Spec.RootPasswordSecret.Name != "" {
		plan.RootSecretName = cluster.Spec.RootPasswordSecret.Name
	}
	if initdb := cluster.Spec.Bootstrap.InitDB; initdb.Secret != nil && initdb.Secret.Name != "" {
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
	return plan, nil
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
	case "5.6":
		return defaultMySQL56ServerVersion, nil
	case "8.0":
		return defaultMySQL80ServerVersion, nil
	case "8.4":
		return defaultMySQL84ServerVersion, nil
	case "9.x":
		return defaultMySQL9xServerVersion, nil
	}
	if _, err := version.Parse(tag); err != nil {
		return "", fmt.Errorf("cannot resolve MySQL server version from image %q: %w", image, err)
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
