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

package controller

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

func (r *ClusterReconciler) validateUserCertificates(ctx context.Context, cluster *mysqlv1alpha1.Cluster) error {
	certs := cluster.Spec.Certificates
	if certs == nil {
		return nil
	}
	if certs.ServerCASecret != "" {
		requiresPrivateKey := certs.ServerTLSSecret == "" || certs.ReplicationTLSSecret == ""
		if err := r.validateUserCASecret(ctx, cluster, certs.ServerCASecret, "server CA", requiresPrivateKey); err != nil {
			return err
		}
	}
	if certs.ClientCASecret != "" {
		if err := r.validateUserCASecret(ctx, cluster, certs.ClientCASecret, "client CA", false); err != nil {
			return err
		}
	}
	if certs.ServerTLSSecret != "" {
		if err := r.validateUserTLSSecret(ctx, cluster, certs.ServerTLSSecret, "server TLS"); err != nil {
			return err
		}
	}
	if certs.ReplicationTLSSecret != "" {
		if err := r.validateUserTLSSecret(ctx, cluster, certs.ReplicationTLSSecret, "replication TLS"); err != nil {
			return err
		}
	}
	return nil
}

func (r *ClusterReconciler) validateUserTLSSecret(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	name string,
	role string,
) error {
	secret, err := r.userCertificateSecret(ctx, cluster, name, role)
	if err != nil {
		return err
	}
	if secret.Type != corev1.SecretTypeTLS {
		return fmt.Errorf("user-provided %s secret %q must be type %q", role, name, corev1.SecretTypeTLS)
	}
	for _, key := range []string{corev1.TLSCertKey, corev1.TLSPrivateKeyKey} {
		if len(secret.Data[key]) == 0 {
			return fmt.Errorf("user-provided %s secret %q is missing key %q", role, name, key)
		}
	}
	if _, err := tls.X509KeyPair(secret.Data[corev1.TLSCertKey], secret.Data[corev1.TLSPrivateKeyKey]); err != nil {
		return fmt.Errorf("user-provided %s secret %q contains invalid TLS material: %w", role, name, err)
	}
	return nil
}

func (r *ClusterReconciler) validateUserCASecret(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	name string,
	role string,
	requiresPrivateKey bool,
) error {
	secret, err := r.userCertificateSecret(ctx, cluster, name, role)
	if err != nil {
		return err
	}
	if secret.Type != corev1.SecretTypeTLS && secret.Type != corev1.SecretTypeOpaque {
		return fmt.Errorf("user-provided %s secret %q must be type %q or %q",
			role, name, corev1.SecretTypeTLS, corev1.SecretTypeOpaque)
	}
	if len(secret.Data["ca.crt"]) == 0 {
		return fmt.Errorf("user-provided %s secret %q is missing key %q", role, name, "ca.crt")
	}
	if requiresPrivateKey {
		for _, key := range []string{corev1.TLSCertKey, corev1.TLSPrivateKeyKey} {
			if len(secret.Data[key]) == 0 {
				return fmt.Errorf("user-provided %s secret %q is missing key %q", role, name, key)
			}
		}
		if _, err := tls.X509KeyPair(secret.Data[corev1.TLSCertKey], secret.Data[corev1.TLSPrivateKeyKey]); err != nil {
			return fmt.Errorf("user-provided %s secret %q contains invalid signing material: %w", role, name, err)
		}
	}
	certs, err := parseCertificatesPEM(secret.Data["ca.crt"])
	if err != nil {
		return fmt.Errorf("user-provided %s secret %q contains invalid ca.crt: %w", role, name, err)
	}
	for _, cert := range certs {
		if cert.IsCA {
			return nil
		}
	}
	return fmt.Errorf("user-provided %s secret %q does not contain a CA certificate", role, name)
}

func (r *ClusterReconciler) userCertificateSecret(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	name string,
	role string,
) (*corev1.Secret, error) {
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: name}
	if err := r.Get(ctx, key, secret); err != nil {
		return nil, fmt.Errorf("user-provided %s secret %q is not readable: %w", role, name, err)
	}
	return secret, nil
}

func parseCertificatesPEM(data []byte) ([]*x509.Certificate, error) {
	var certs []*x509.Certificate
	for {
		block, rest := pem.Decode(data)
		if block == nil {
			break
		}
		data = rest
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, err
		}
		certs = append(certs, cert)
	}
	if len(certs) == 0 {
		return nil, fmt.Errorf("no PEM certificates found")
	}
	return certs, nil
}
