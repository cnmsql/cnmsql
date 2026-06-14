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

package plugin

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
)

// Default mTLS secret names, overridable via cluster.Spec.Certificates.
const (
	caSecretSuffix        = "-ca"
	clientTLSSecretSuffix = "-client-tls"
)

// MonitoringTLSEnabled reports whether the cluster serves its metrics endpoint
// over (mutual) TLS. It mirrors the operator's monitoringTLSEnabled check; when
// false the metrics port is plain HTTP.
func MonitoringTLSEnabled(cluster *mysqlv1alpha1.Cluster) bool {
	return cluster.Spec.Monitoring != nil &&
		cluster.Spec.Monitoring.TLSConfig != nil &&
		cluster.Spec.Monitoring.TLSConfig.Enabled
}

// controlTLSConfig builds the mTLS client config for talking to an instance's
// control API. The connection is made to a local port-forward, so the
// certificate's SAN (the per-instance Service DNS name) is pinned via
// ServerName rather than derived from the dial address.
func (e *Env) controlTLSConfig(
	ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string,
) (*tls.Config, error) {
	caSecretName := cluster.Name + caSecretSuffix
	clientSecretName := cluster.Name + clientTLSSecretSuffix
	if certs := cluster.Spec.Certificates; certs != nil {
		if certs.ServerCASecret != "" {
			caSecretName = certs.ServerCASecret
		}
		if certs.ReplicationTLSSecret != "" {
			clientSecretName = certs.ReplicationTLSSecret
		}
	}

	caSecret := &corev1.Secret{}
	caKey := types.NamespacedName{Namespace: cluster.Namespace, Name: caSecretName}
	if err := e.Client.Get(ctx, caKey, caSecret); err != nil {
		return nil, fmt.Errorf("reading CA secret %q: %w", caSecretName, err)
	}
	clientSecret := &corev1.Secret{}
	clientKey := types.NamespacedName{Namespace: cluster.Namespace, Name: clientSecretName}
	if err := e.Client.Get(ctx, clientKey, clientSecret); err != nil {
		return nil, fmt.Errorf("reading client TLS secret %q: %w", clientSecretName, err)
	}

	cert, err := tls.X509KeyPair(clientSecret.Data[corev1.TLSCertKey], clientSecret.Data[corev1.TLSPrivateKeyKey])
	if err != nil {
		return nil, fmt.Errorf("loading client certificate: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caSecret.Data["ca.crt"]) {
		return nil, fmt.Errorf("secret %q does not contain a valid ca.crt", caSecretName)
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		ServerName:   fmt.Sprintf("%s.%s.svc", instanceName, cluster.Namespace),
		Certificates: []tls.Certificate{cert},
		RootCAs:      roots,
	}, nil
}
