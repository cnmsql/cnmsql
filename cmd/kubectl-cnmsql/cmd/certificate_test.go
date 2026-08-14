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
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	"github.com/cnmsql/cnmsql/cmd/kubectl-cnmsql/plugin"
)

// makeCASecret builds a self-signed ECDSA P-256 CA and returns the secret data
// (ca.crt, tls.crt, tls.key) plus the parsed CA cert for verification.
func makeCASecret(t *testing.T) (corev1.Secret, *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating CA key: %v", err)
	}
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		t.Fatalf("creating CA cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, _ := x509.MarshalPKCS8PrivateKey(key)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	caCert, _ := parseCertificatePEM(certPEM)
	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName + "-ca", Namespace: "test"},
		Data: map[string][]byte{
			"ca.crt":                certPEM,
			corev1.TLSCertKey:       certPEM,
			corev1.TLSPrivateKeyKey: keyPEM,
		},
	}
	return secret, caCert
}

func TestRunCertificateDryRunEmitsSignedSecret(t *testing.T) {
	var buf bytes.Buffer
	origOut := plugin.Out
	plugin.Out = &buf
	t.Cleanup(func() { plugin.Out = origOut })

	cluster := testCluster()
	caSecret, caCert := makeCASecret(t)
	objects := []any{cluster.DeepCopy(), caSecret.DeepCopy()}
	env := &plugin.Env{
		Namespace: "test",
		Client:    fakeClientWith(objects),
	}
	previous := newEnv
	newEnv = func() (*plugin.Env, error) { return env, nil }
	t.Cleanup(func() { newEnv = previous })

	if err := runCertificate(context.Background(), "app-client", "app",
		testClusterName, "yaml", true); err != nil {
		t.Fatalf("runCertificate() error = %v", err)
	}

	var secret corev1.Secret
	if err := yaml.Unmarshal(buf.Bytes(), &secret); err != nil {
		t.Fatalf("decoding secret manifest: %v\nbody: %s", err, buf.String())
	}
	if secret.Name != "app-client" || secret.Type != corev1.SecretTypeTLS {
		t.Errorf("secret = %q/%q, want app-client/tls", secret.Name, secret.Type)
	}
	if secret.Annotations["mysql.cnmsql.co/user"] != "app" {
		t.Errorf("user annotation = %q", secret.Annotations["mysql.cnmsql.co/user"])
	}

	// Verify the client cert is signed by the CA and has CN=app.
	certBlock, _ := pem.Decode(secret.Data[corev1.TLSCertKey])
	if certBlock == nil {
		t.Fatalf("client cert not PEM-encoded")
	}
	clientCert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		t.Fatalf("parsing client cert: %v", err)
	}
	if clientCert.Subject.CommonName != "app" {
		t.Errorf("client cert CN = %q, want app", clientCert.Subject.CommonName)
	}
	if err := clientCert.CheckSignatureFrom(caCert); err != nil {
		t.Errorf("client cert not signed by CA: %v", err)
	}
	if _, ok := secret.Data["ca.crt"]; !ok {
		t.Error("secret missing ca.crt")
	}
}

func TestRunCertificateRequiresUserAndCluster(t *testing.T) {
	t.Parallel()
	if err := runCertificate(context.Background(), "s", "", "c", "yaml", true); err == nil ||
		!strings.Contains(err.Error(), "cnmsql-user") {
		t.Errorf("missing user error = %v", err)
	}
	if err := runCertificate(context.Background(), "s", "u", "", "yaml", true); err == nil ||
		!strings.Contains(err.Error(), "cnmsql-cluster") {
		t.Errorf("missing cluster error = %v", err)
	}
}

func TestRunCertificateRejectsUnsupportedOutput(t *testing.T) {
	var buf bytes.Buffer
	origOut := plugin.Out
	plugin.Out = &buf
	t.Cleanup(func() { plugin.Out = origOut })

	cluster := testCluster()
	caSecret, _ := makeCASecret(t)
	env := &plugin.Env{
		Namespace: "test",
		Client:    fakeClientWith([]any{cluster.DeepCopy(), caSecret.DeepCopy()}),
	}
	previous := newEnv
	newEnv = func() (*plugin.Env, error) { return env, nil }
	t.Cleanup(func() { newEnv = previous })

	if err := runCertificate(context.Background(), "s", "u", testClusterName, "toml", true); err == nil ||
		!strings.Contains(err.Error(), "unsupported output format") {
		t.Errorf("expected unsupported format error, got %v", err)
	}
}
