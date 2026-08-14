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

package cmd

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/cmd/kubectl-cnmsql/plugin"
)

func newCertificateCommand() *cobra.Command {
	var (
		user    string
		cluster string
		output  string
		dryRun  bool
	)
	cmd := &cobra.Command{
		Use:   "certificate [secretName]",
		Short: "Create a client certificate for mTLS access to a cluster",
		Long: `Generate a client certificate signed by the cluster's CA and store it in a
Kubernetes Secret of type kubernetes.io/tls, suitable for configuring an
application to connect to the cluster with TLS and certificate authentication.

The CA certificate and key are read from the cluster's CA secret (default
<cluster>-ca); the new keypair is generated locally as ECDSA P-256.`,
		Args: cobra.ExactArgs(1),
		Example: `  # Create a client cert for user 'app' signed by cluster-sample's CA
  kubectl cnmsql certificate app-client \
      --cnmsql-cluster cluster-sample --cnmsql-user app

  # Print the Secret manifest without creating it
  kubectl cnmsql certificate app-client \
      --cnmsql-cluster cluster-sample --cnmsql-user app --dry-run -o yaml`,
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return completeCluster(cmd.Context(), toComplete)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCertificate(cmd.Context(), args[0], user, cluster, output, dryRun)
		},
	}
	cmd.Flags().StringVar(&user, "cnmsql-user", "", "The MySQL user (certificate Common Name)")
	_ = cmd.MarkFlagRequired("cnmsql-user")
	cmd.Flags().StringVar(&cluster, "cnmsql-cluster", "", "The name of the cnmsql cluster")
	_ = cmd.MarkFlagRequired("cnmsql-cluster")
	cmd.Flags().StringVarP(&output, "output", "o", "", "Output format: json or yaml (with --dry-run)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Print the Secret manifest instead of creating it")
	return cmd
}

func runCertificate(
	ctx context.Context, secretName, user, clusterName, output string, dryRun bool,
) error {
	if user == "" {
		return fmt.Errorf("--cnmsql-user is required")
	}
	if clusterName == "" {
		return fmt.Errorf("--cnmsql-cluster is required")
	}
	env, err := newEnv()
	if err != nil {
		return err
	}
	clusterObj, err := env.GetCluster(ctx, clusterName)
	if err != nil {
		return err
	}
	caCertPEM, caCert, caKey, err := loadCAFromSecret(ctx, env, clusterObj)
	if err != nil {
		return err
	}
	clientCert, clientKey, err := signClientCertificate(caCert, caKey, user)
	if err != nil {
		return fmt.Errorf("signing client certificate: %w", err)
	}
	secret := buildClientSecret(clusterObj.Namespace, secretName, user,
		caCertPEM, clientCert, clientKey)

	if dryRun || output != "" {
		return plugin.PrintObject(secret, output)
	}
	if err := env.Client.Create(ctx, secret); err != nil {
		return fmt.Errorf("creating secret %q: %w", secretName, err)
	}
	_, _ = fmt.Fprintf(plugin.Out, "secret/%s created\n", secret.Name)
	return nil
}

// loadCAFromSecret reads the cluster's CA certificate and private key from the
// CA secret (default <cluster>-ca, overridable via spec.certificates). It
// returns the CA certificate PEM, the parsed CA certificate, and the CA key.
func loadCAFromSecret(
	ctx context.Context, env *plugin.Env, cluster *mysqlv1alpha1.Cluster,
) ([]byte, *x509.Certificate, any, error) {
	caSecretName := cluster.Name + "-ca"
	if certs := cluster.Spec.Certificates; certs != nil && certs.ServerCASecret != "" {
		caSecretName = certs.ServerCASecret
	}
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: caSecretName}
	if err := env.Client.Get(ctx, key, secret); err != nil {
		return nil, nil, nil, fmt.Errorf("reading CA secret %q: %w", caSecretName, err)
	}
	caCertPEM := secret.Data["ca.crt"]
	if len(caCertPEM) == 0 {
		caCertPEM = secret.Data[corev1.TLSCertKey]
	}
	caCert, err := parseCertificatePEM(caCertPEM)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("parsing CA certificate from secret %q: %w", caSecretName, err)
	}
	caKeyPEM := secret.Data[corev1.TLSPrivateKeyKey]
	caKey, err := parsePrivateKeyPEM(caKeyPEM)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("parsing CA private key from secret %q: %w", caSecretName, err)
	}
	return caCertPEM, caCert, caKey, nil
}

func parseCertificatePEM(data []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("no PEM block found in certificate")
	}
	return x509.ParseCertificate(block.Bytes)
}

func parsePrivateKeyPEM(data []byte) (any, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("no PEM block found in private key")
	}
	if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	if key, err := x509.ParseECPrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	return nil, fmt.Errorf("unsupported CA private key type (expected ECDSA or RSA PKCS8/SEC1)")
}

// signClientCertificate generates an ECDSA P-256 keypair and signs a client
// certificate with the CA, valid for one year. The CN and SAN are set to user.
func signClientCertificate(caCert *x509.Certificate, caKey any, user string) ([]byte, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generating client key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, fmt.Errorf("generating serial: %w", err)
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: user},
		DNSNames:     []string{user},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.AddDate(1, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, key.Public(), caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("creating certificate: %w", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, fmt.Errorf("marshalling client key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM, nil
}

func buildClientSecret(namespace, name, user string, caCert, clientCert, clientKey []byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Annotations: map[string]string{
				"mysql.cnmsql.co/user": user,
			},
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			corev1.TLSCertKey:       clientCert,
			corev1.TLSPrivateKeyKey: clientKey,
			"ca.crt":                caCert,
		},
	}
}
