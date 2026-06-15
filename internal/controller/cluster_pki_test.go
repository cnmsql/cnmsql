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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

func TestBuildPlanCertificateOverridesAreIndependent(t *testing.T) {
	t.Parallel()
	const serverCASecretName = "server-ca"
	const clientCASecretName = "client-ca"
	cluster := baseCluster()
	cluster.Spec.Certificates = &mysqlv1alpha1.CertificatesConfiguration{
		ServerCASecret:       serverCASecretName,
		ClientCASecret:       clientCASecretName,
		ServerTLSSecret:      "server-tls",
		ReplicationTLSSecret: "replication-tls",
	}
	reconciler := &ClusterReconciler{Client: fake.NewClientBuilder().WithScheme(testScheme(t)).Build()}

	plan, err := reconciler.buildPlan(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ServerCASecretName != serverCASecretName {
		t.Fatalf("server CA = %q, want %s", plan.ServerCASecretName, serverCASecretName)
	}
	if plan.ClientCASecretName != clientCASecretName {
		t.Fatalf("client CA = %q, want %s", plan.ClientCASecretName, clientCASecretName)
	}
	if plan.UserServerTLSSecret != "server-tls" {
		t.Fatalf("server TLS = %q, want server-tls", plan.UserServerTLSSecret)
	}
	if plan.ClientTLSSecret != "replication-tls" {
		t.Fatalf("replication TLS = %q, want replication-tls", plan.ClientTLSSecret)
	}
}

func TestBuildPlanServerCAOverrideBecomesDefaultClientCA(t *testing.T) {
	t.Parallel()
	const serverCASecretName = "server-ca"
	cluster := baseCluster()
	cluster.Spec.Certificates = &mysqlv1alpha1.CertificatesConfiguration{
		ServerCASecret: serverCASecretName,
	}
	reconciler := &ClusterReconciler{Client: fake.NewClientBuilder().WithScheme(testScheme(t)).Build()}

	plan, err := reconciler.buildPlan(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ServerCASecretName != serverCASecretName {
		t.Fatalf("server CA = %q, want %s", plan.ServerCASecretName, serverCASecretName)
	}
	if plan.ClientCASecretName != serverCASecretName {
		t.Fatalf("client CA = %q, want %s", plan.ClientCASecretName, serverCASecretName)
	}
}

func TestPodSpecUsesClientCASecret(t *testing.T) {
	t.Parallel()
	const clientCASecretName = "client-ca"
	cluster := baseCluster()
	reconciler := &ClusterReconciler{}
	plan := testPlan()
	plan.ClientCASecretName = clientCASecretName

	podSpec := reconciler.podSpec(cluster, plan, plan.instanceFor(cluster, 1))
	for _, volume := range podSpec.Volumes {
		if volume.Name == "client-ca" {
			if volume.Secret == nil || volume.Secret.SecretName != clientCASecretName {
				t.Fatalf("client-ca volume = %#v, want %s secret", volume.Secret, clientCASecretName)
			}
			return
		}
	}
	t.Fatal("client-ca volume not found")
}

func TestEnsureCertificatesSkipsOnlyUserProvidedServerTLS(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Certificates = &mysqlv1alpha1.CertificatesConfiguration{
		ServerTLSSecret: "user-server-tls",
	}
	scheme := testScheme(t)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme: scheme,
	}
	plan, err := reconciler.buildPlan(ctx, cluster)
	if err != nil {
		t.Fatal(err)
	}

	if err := reconciler.ensureCertificates(ctx, cluster, plan); err != nil {
		t.Fatal(err)
	}

	assertUnstructuredExists(t, ctx, reconciler, issuerGVK, "demo-selfsigned")
	assertUnstructuredExists(t, ctx, reconciler, certificateGVK, "demo-ca")
	assertUnstructuredExists(t, ctx, reconciler, issuerGVK, "demo-ca")
	assertUnstructuredExists(t, ctx, reconciler, certificateGVK, "demo-client")
	assertUnstructuredNotFound(t, ctx, reconciler, certificateGVK, "demo-1-server")
}

func TestServerCAOnlyOverrideSignsGeneratedCertificatesAndIsReady(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const serverCASecretName = "server-ca"
	caCRT, caKey := testCertificate(t, true)
	cluster := baseCluster()
	cluster.Spec.Certificates = &mysqlv1alpha1.CertificatesConfiguration{
		ServerCASecret: serverCASecretName,
	}
	scheme := testScheme(t)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(
			cluster,
			caSecret(serverCASecretName, caCRT, caKey),
			tlsSecret("demo-client-tls", caCRT, caKey),
			tlsSecret("demo-1-server-tls", caCRT, caKey),
		).Build(),
		Scheme: scheme,
	}
	plan, err := reconciler.buildPlan(ctx, cluster)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ClientCASecretName != serverCASecretName {
		t.Fatalf("client CA = %q, want %s", plan.ClientCASecretName, serverCASecretName)
	}

	if err := reconciler.validateUserCertificates(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ensureCertificates(ctx, cluster, plan); err != nil {
		t.Fatal(err)
	}

	assertUnstructuredNotFound(t, ctx, reconciler, issuerGVK, "demo-selfsigned")
	assertUnstructuredNotFound(t, ctx, reconciler, certificateGVK, "demo-ca")
	issuer := assertUnstructuredExists(t, ctx, reconciler, issuerGVK, "demo-ca")
	secretName, _, _ := unstructured.NestedString(issuer.Object, "spec", "ca", "secretName")
	if secretName != serverCASecretName {
		t.Fatalf("CA issuer secretName = %q, want %s", secretName, serverCASecretName)
	}
	assertUnstructuredExists(t, ctx, reconciler, certificateGVK, "demo-client")
	assertUnstructuredExists(t, ctx, reconciler, certificateGVK, "demo-1-server")
	ready, err := reconciler.certSecretsReady(ctx, cluster, plan)
	if err != nil {
		t.Fatal(err)
	}
	if !ready {
		t.Fatal("certSecretsReady() = false, want true")
	}
}

func TestClientCAOnlyOverrideKeepsGeneratedSigningCA(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	caCRT, caKey := testCertificate(t, true)
	cluster := baseCluster()
	cluster.Spec.Certificates = &mysqlv1alpha1.CertificatesConfiguration{
		ClientCASecret: "client-ca",
	}
	scheme := testScheme(t)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(
			cluster,
			caSecret("client-ca", caCRT, nil),
			caSecret("demo-ca", caCRT, caKey),
			tlsSecret("demo-client-tls", caCRT, caKey),
			tlsSecret("demo-1-server-tls", caCRT, caKey),
		).Build(),
		Scheme: scheme,
	}
	plan, err := reconciler.buildPlan(ctx, cluster)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ServerCASecretName != "demo-ca" {
		t.Fatalf("server CA = %q, want demo-ca", plan.ServerCASecretName)
	}
	if plan.ClientCASecretName != "client-ca" {
		t.Fatalf("client CA = %q, want client-ca", plan.ClientCASecretName)
	}

	if err := reconciler.validateUserCertificates(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ensureCertificates(ctx, cluster, plan); err != nil {
		t.Fatal(err)
	}

	assertUnstructuredExists(t, ctx, reconciler, issuerGVK, "demo-selfsigned")
	assertUnstructuredExists(t, ctx, reconciler, certificateGVK, "demo-ca")
	ready, err := reconciler.certSecretsReady(ctx, cluster, plan)
	if err != nil {
		t.Fatal(err)
	}
	if !ready {
		t.Fatal("certSecretsReady() = false, want true")
	}
}

func TestEnsureCertificatesSkipsAllCertManagerResourcesForFullOverride(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Certificates = &mysqlv1alpha1.CertificatesConfiguration{
		ServerCASecret:       "server-ca",
		ClientCASecret:       "client-ca",
		ServerTLSSecret:      "server-tls",
		ReplicationTLSSecret: "replication-tls",
	}
	scheme := testScheme(t)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme: scheme,
	}
	plan, err := reconciler.buildPlan(ctx, cluster)
	if err != nil {
		t.Fatal(err)
	}

	if err := reconciler.ensureCertificates(ctx, cluster, plan); err != nil {
		t.Fatal(err)
	}

	assertUnstructuredNotFound(t, ctx, reconciler, issuerGVK, "demo-selfsigned")
	assertUnstructuredNotFound(t, ctx, reconciler, certificateGVK, "demo-ca")
	assertUnstructuredNotFound(t, ctx, reconciler, issuerGVK, "demo-ca")
	assertUnstructuredNotFound(t, ctx, reconciler, certificateGVK, "demo-client")
	assertUnstructuredNotFound(t, ctx, reconciler, certificateGVK, "demo-1-server")
}

func TestServerDNSNamesAppendUserAltNames(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	cluster.Spec.Certificates = &mysqlv1alpha1.CertificatesConfiguration{
		ServerAltDNSNames: []string{"mysql.example.com", "mysql.internal"},
	}
	plan := testPlan()

	names := serverDNSNames(cluster, plan, plan.instanceFor(cluster, 1))
	if got := names[len(names)-2:]; got[0] != "mysql.example.com" || got[1] != "mysql.internal" {
		t.Fatalf("trailing alt names = %#v", got)
	}
}

func TestValidateUserCertificates(t *testing.T) {
	t.Parallel()
	caCRT, caKey := testCertificate(t, true)
	leafCRT, leafKey := testCertificate(t, false)

	tests := []struct {
		name      string
		secrets   []*corev1.Secret
		certs     *mysqlv1alpha1.CertificatesConfiguration
		wantError string
	}{
		{
			name:      "missing secret",
			certs:     &mysqlv1alpha1.CertificatesConfiguration{ServerTLSSecret: "missing"},
			wantError: "is not readable",
		},
		{
			name: "wrong tls type",
			secrets: []*corev1.Secret{{
				ObjectMeta: metav1.ObjectMeta{Name: "server-tls", Namespace: "default"},
				Type:       corev1.SecretTypeOpaque,
				Data:       map[string][]byte{corev1.TLSCertKey: leafCRT, corev1.TLSPrivateKeyKey: leafKey},
			}},
			certs:     &mysqlv1alpha1.CertificatesConfiguration{ServerTLSSecret: "server-tls"},
			wantError: "must be type",
		},
		{
			name: "missing tls key",
			secrets: []*corev1.Secret{{
				ObjectMeta: metav1.ObjectMeta{Name: "server-tls", Namespace: "default"},
				Type:       corev1.SecretTypeTLS,
				Data:       map[string][]byte{corev1.TLSCertKey: leafCRT},
			}},
			certs:     &mysqlv1alpha1.CertificatesConfiguration{ServerTLSSecret: "server-tls"},
			wantError: "tls.key",
		},
		{
			name: "server ca used for signing requires tls key",
			secrets: []*corev1.Secret{{
				ObjectMeta: metav1.ObjectMeta{Name: "server-ca", Namespace: "default"},
				Type:       corev1.SecretTypeOpaque,
				Data:       map[string][]byte{"ca.crt": caCRT},
			}},
			certs:     &mysqlv1alpha1.CertificatesConfiguration{ServerCASecret: "server-ca"},
			wantError: corev1.TLSCertKey,
		},
		{
			name: "valid server ca signing secret",
			secrets: []*corev1.Secret{{
				ObjectMeta: metav1.ObjectMeta{Name: "server-ca", Namespace: "default"},
				Type:       corev1.SecretTypeOpaque,
				Data:       map[string][]byte{"ca.crt": caCRT, corev1.TLSCertKey: caCRT, corev1.TLSPrivateKeyKey: caKey},
			}},
			certs: &mysqlv1alpha1.CertificatesConfiguration{ServerCASecret: "server-ca"},
		},
		{
			name: "server ca without key is valid for full override",
			secrets: []*corev1.Secret{{
				ObjectMeta: metav1.ObjectMeta{Name: "server-ca", Namespace: "default"},
				Type:       corev1.SecretTypeOpaque,
				Data:       map[string][]byte{"ca.crt": caCRT},
			}, {
				ObjectMeta: metav1.ObjectMeta{Name: "server-tls", Namespace: "default"},
				Type:       corev1.SecretTypeTLS,
				Data:       map[string][]byte{corev1.TLSCertKey: leafCRT, corev1.TLSPrivateKeyKey: leafKey},
			}, {
				ObjectMeta: metav1.ObjectMeta{Name: "replication-tls", Namespace: "default"},
				Type:       corev1.SecretTypeTLS,
				Data:       map[string][]byte{corev1.TLSCertKey: leafCRT, corev1.TLSPrivateKeyKey: leafKey},
			}},
			certs: &mysqlv1alpha1.CertificatesConfiguration{
				ServerCASecret:       "server-ca",
				ServerTLSSecret:      "server-tls",
				ReplicationTLSSecret: "replication-tls",
			},
		},
		{
			name: "client ca is verify only",
			secrets: []*corev1.Secret{{
				ObjectMeta: metav1.ObjectMeta{Name: "client-ca", Namespace: "default"},
				Type:       corev1.SecretTypeOpaque,
				Data:       map[string][]byte{"ca.crt": caCRT},
			}},
			certs: &mysqlv1alpha1.CertificatesConfiguration{ClientCASecret: "client-ca"},
		},
		{
			name: "ca secret is not a ca",
			secrets: []*corev1.Secret{{
				ObjectMeta: metav1.ObjectMeta{Name: "server-ca", Namespace: "default"},
				Type:       corev1.SecretTypeOpaque,
				Data:       map[string][]byte{"ca.crt": leafCRT, corev1.TLSCertKey: caCRT, corev1.TLSPrivateKeyKey: caKey},
			}},
			certs:     &mysqlv1alpha1.CertificatesConfiguration{ServerCASecret: "server-ca"},
			wantError: "does not contain a CA certificate",
		},
		{
			name: "valid tls secret",
			secrets: []*corev1.Secret{{
				ObjectMeta: metav1.ObjectMeta{Name: "server-tls", Namespace: "default"},
				Type:       corev1.SecretTypeTLS,
				Data:       map[string][]byte{corev1.TLSCertKey: leafCRT, corev1.TLSPrivateKeyKey: leafKey},
			}},
			certs: &mysqlv1alpha1.CertificatesConfiguration{ServerTLSSecret: "server-tls"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cluster := baseCluster()
			cluster.Spec.Certificates = tt.certs
			builder := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(cluster)
			for _, secret := range tt.secrets {
				builder = builder.WithObjects(secret)
			}
			reconciler := &ClusterReconciler{Client: builder.Build()}

			err := reconciler.validateUserCertificates(context.Background(), cluster)
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("validateUserCertificates() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("validateUserCertificates() error = %v, want containing %q", err, tt.wantError)
			}
		})
	}
}

func assertUnstructuredExists(
	t *testing.T,
	ctx context.Context,
	reconciler *ClusterReconciler,
	gvk schema.GroupVersionKind,
	name string,
) *unstructured.Unstructured {
	t.Helper()
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: "default", Name: name}, obj); err != nil {
		t.Fatalf("%s %s not found: %v", gvk.Kind, name, err)
	}
	return obj
}

func assertUnstructuredNotFound(
	t *testing.T,
	ctx context.Context,
	reconciler *ClusterReconciler,
	gvk schema.GroupVersionKind,
	name string,
) {
	t.Helper()
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	err := reconciler.Get(ctx, types.NamespacedName{Namespace: "default", Name: name}, obj)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("%s %s error = %v, want not found", gvk.Kind, name, err)
	}
}

func testCertificate(t *testing.T, isCA bool) ([]byte, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1_000_000))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "cloudnative-mysql-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  isCA,
	}
	if isCA {
		template.KeyUsage |= x509.KeyUsageCertSign
	} else {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return certPEM, keyPEM
}

func caSecret(name string, caCRT []byte, caKey []byte) *corev1.Secret {
	data := map[string][]byte{"ca.crt": caCRT}
	if len(caKey) > 0 {
		data[corev1.TLSCertKey] = caCRT
		data[corev1.TLSPrivateKeyKey] = caKey
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Type:       corev1.SecretTypeOpaque,
		Data:       data,
	}
}

func tlsSecret(name string, crt []byte, key []byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Type:       corev1.SecretTypeTLS,
		Data: map[string][]byte{
			corev1.TLSCertKey:              crt,
			corev1.TLSPrivateKeyKey:        key,
			corev1.ServiceAccountRootCAKey: crt,
		},
	}
}
