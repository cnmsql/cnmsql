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

package objectstore

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

func credentialsFixture(t *testing.T) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	optedIn := map[string]string{CredentialsSecretLabel: "true"}
	data := map[string][]byte{"access": []byte("key"), "secret": []byte("secret"), "password": []byte("root-pw")}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s3", Namespace: "ns", Labels: optedIn}, Data: data},
		// The operator's own Secret, as the advisory's reproduction names it.
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "c-root", Namespace: "ns"}, Data: data},
		// Labelled, but owned by a Cluster: still refused.
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{
			Name: "c-control", Namespace: "ns", Labels: optedIn,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: mysqlv1alpha1.GroupVersion.String(), Kind: "Cluster", Name: "c", UID: "u",
			}},
		}, Data: data},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "ca", Namespace: "ns"},
			Data: map[string][]byte{"ca.crt": []byte("-----BEGIN CERTIFICATE-----")}},
	).Build()
}

// GHSA-h4q2-3gj8-wfwf: a credential selector naming a Secret its owner did not
// opt in, or one a cnmsql object owns, is refused before its value is read.
func TestResolveConfigRefusesSecretsNotOptedIn(t *testing.T) {
	t.Parallel()
	c := credentialsFixture(t)
	sel := func(name, key string) *mysqlv1alpha1.SecretKeySelector {
		return &mysqlv1alpha1.SecretKeySelector{Name: name, Key: key}
	}

	for _, tc := range []struct {
		name        string
		creds       mysqlv1alpha1.S3Credentials
		wantRefused bool
	}{
		{name: "opted-in secret", creds: mysqlv1alpha1.S3Credentials{
			AccessKeyID: sel("s3", "access"), SecretAccessKey: sel("s3", "secret")}},
		{name: "access key from an unlabelled secret", creds: mysqlv1alpha1.S3Credentials{
			AccessKeyID: sel("c-root", "password"), SecretAccessKey: sel("s3", "secret")}, wantRefused: true},
		{name: "session token from an unlabelled secret", creds: mysqlv1alpha1.S3Credentials{
			AccessKeyID: sel("s3", "access"), SecretAccessKey: sel("s3", "secret"),
			SessionToken: sel("c-root", "password")}, wantRefused: true},
		{name: "labelled but owned by a Cluster", creds: mysqlv1alpha1.S3Credentials{
			AccessKeyID: sel("c-control", "password"), SecretAccessKey: sel("s3", "secret")}, wantRefused: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := &mysqlv1alpha1.S3ObjectStore{Bucket: "b", Endpoint: "http://attacker.example", Credentials: tc.creds}
			cfg, err := ResolveConfig(context.Background(), c, "ns", store, false)
			if refused := errors.Is(err, ErrCredentialsSecretRefused); refused != tc.wantRefused {
				t.Fatalf("refused = %v (err %v), want %v", refused, err, tc.wantRefused)
			}
			if tc.wantRefused && (cfg.AccessKeyID != "" || cfg.SessionToken != "") {
				t.Fatalf("a refused config still carries values: %+v", cfg)
			}
			err = CheckStoreSecrets(context.Background(), c, "ns", store)
			if errors.Is(err, ErrCredentialsSecretRefused) != tc.wantRefused {
				t.Fatalf("CheckStoreSecrets = %v, want refused %v", err, tc.wantRefused)
			}
		})
	}
}

// The CA bundle only verifies the endpoint and is never sent to it, so its
// Secret needs no label.
func TestCheckStoreSecretsLeavesTheCABundleAlone(t *testing.T) {
	t.Parallel()
	store := &mysqlv1alpha1.S3ObjectStore{Bucket: "b", TLS: &mysqlv1alpha1.S3TLSConfig{
		CABundleSecret: &mysqlv1alpha1.SecretKeySelector{Name: "ca", Key: "ca.crt"},
	}}
	if err := CheckStoreSecrets(context.Background(), credentialsFixture(t), "ns", store); err != nil {
		t.Fatal(err)
	}
}

func TestCheckStoreSecretsReportsAMissingSecret(t *testing.T) {
	t.Parallel()
	store := &mysqlv1alpha1.S3ObjectStore{Bucket: "b", Credentials: mysqlv1alpha1.S3Credentials{
		AccessKeyID: &mysqlv1alpha1.SecretKeySelector{Name: "absent", Key: "k"},
	}}
	err := CheckStoreSecrets(context.Background(), credentialsFixture(t), "ns", store)
	if err == nil || errors.Is(err, ErrCredentialsSecretRefused) {
		t.Fatalf("err = %v, want a read error that is retried, not a refusal", err)
	}
}

// The operator must not sign with its own identity for a store a Cluster
// author chose (GHSA-qxwf-59cm-cj4x) unless its administrator opted in.
func TestResolveConfigAmbientCredentials(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: "s3", Namespace: "ns", Labels: map[string]string{CredentialsSecretLabel: "true"},
		},
		Data: map[string][]byte{"access": []byte("key"), "secret": []byte("secret"), "empty": nil},
	}).Build()
	selector := func(key string) *mysqlv1alpha1.SecretKeySelector {
		return &mysqlv1alpha1.SecretKeySelector{Name: "s3", Key: key}
	}

	for _, tc := range []struct {
		name         string
		creds        mysqlv1alpha1.S3Credentials
		allowAmbient bool
		wantRefused  bool
	}{
		{name: "inheritFromIAMRole", creds: mysqlv1alpha1.S3Credentials{InheritFromIAMRole: true}, wantRefused: true},
		{name: "no credentials", wantRefused: true},
		{name: "session token alone", creds: mysqlv1alpha1.S3Credentials{SessionToken: selector("secret")},
			wantRefused: true},
		{name: "selectors naming empty values",
			creds:       mysqlv1alpha1.S3Credentials{AccessKeyID: selector("empty"), SecretAccessKey: selector("empty")},
			wantRefused: true},
		{name: "static keys", creds: mysqlv1alpha1.S3Credentials{
			AccessKeyID: selector("access"), SecretAccessKey: selector("secret")}},
		{name: "operator opted in", creds: mysqlv1alpha1.S3Credentials{InheritFromIAMRole: true}, allowAmbient: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := &mysqlv1alpha1.S3ObjectStore{Bucket: "b", Credentials: tc.creds}
			cfg, err := ResolveConfig(context.Background(), c, "ns", store, tc.allowAmbient)
			if refused := errors.Is(err, ErrAmbientCredentialsDisabled); refused != tc.wantRefused {
				t.Fatalf("refused = %v (err %v), want %v", refused, err, tc.wantRefused)
			}
			if err == nil && cfg.AllowAmbient != tc.allowAmbient {
				t.Fatalf("cfg.AllowAmbient = %v, want %v", cfg.AllowAmbient, tc.allowAmbient)
			}
		})
	}
}

// NewClient enforces the same rule, so a Config built some other way cannot
// reach the ambient chain without asking for it.
func TestNewClientRefusesAmbientUnlessAllowed(t *testing.T) {
	t.Parallel()

	if _, err := NewClient(Config{Endpoint: "http://127.0.0.1:1"}); !errors.Is(err, ErrAmbientCredentialsDisabled) {
		t.Fatalf("NewClient without keys = %v, want ErrAmbientCredentialsDisabled", err)
	}
	if _, err := NewClient(Config{Endpoint: "http://127.0.0.1:1", AllowAmbient: true}); err != nil {
		t.Fatalf("NewClient with AllowAmbient = %v", err)
	}
	if !ConfigFromEnv().AllowAmbient || !BinlogConfigFromEnv().AllowAmbient {
		t.Fatal("worker configs must allow the ambient chain: workers run as the tenant")
	}
}
