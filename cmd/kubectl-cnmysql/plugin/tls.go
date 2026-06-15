/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package plugin

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
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
