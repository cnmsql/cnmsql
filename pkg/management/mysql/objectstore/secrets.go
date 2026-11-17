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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// CredentialsSecretLabel marks a Secret its owner allows object stores to use
// as credentials. It must be set to "true".
const CredentialsSecretLabel = "mysql.cnmsql.co/object-store-credentials"

// ErrCredentialsSecretRefused is returned for an object-store credential
// selector naming a Secret that may not be used as credentials.
var ErrCredentialsSecretRefused = errors.New("secret may not be used as object-store credentials")

// CheckCredentialsSecret reports whether secret may be used as object-store
// credentials.
//
// SigV4 sends the access key ID in clear in the Authorization header and the
// session token verbatim, to an endpoint the Backup or Cluster author picks.
// Anyone who may edit either could otherwise name any Secret in the namespace
// (such as <cluster>-root) and read it off the wire, without being allowed to
// read Secrets (GHSA-h4q2-3gj8-wfwf). So the Secret's own author has to opt it
// in with CredentialsSecretLabel, and a Secret a cnmsql object owns (the
// cluster's account passwords, certificates) is refused even then.
func CheckCredentialsSecret(secret *corev1.Secret) error {
	for _, ref := range secret.OwnerReferences {
		if gv, err := schema.ParseGroupVersion(ref.APIVersion); err == nil &&
			gv.Group == mysqlv1alpha1.GroupVersion.Group {
			return fmt.Errorf("secret %s/%s is owned by %s %q: %w",
				secret.Namespace, secret.Name, ref.Kind, ref.Name, ErrCredentialsSecretRefused)
		}
	}
	if secret.Labels[CredentialsSecretLabel] != "true" {
		return fmt.Errorf("secret %s/%s does not carry the label %s=true: %w",
			secret.Namespace, secret.Name, CredentialsSecretLabel, ErrCredentialsSecretRefused)
	}
	return nil
}

// credentialSelectors returns the selectors whose values are sent to the
// endpoint. The CA bundle is not: it only verifies the endpoint.
func credentialSelectors(store *mysqlv1alpha1.S3ObjectStore) []*mysqlv1alpha1.SecretKeySelector {
	creds := store.Credentials
	return []*mysqlv1alpha1.SecretKeySelector{creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken}
}

// CheckStoreSecrets checks every credential Secret store names in namespace
// with CheckCredentialsSecret. Run it before rendering the store into a Pod:
// the kubelet resolves the Secrets there, and nothing checks them on the way.
func CheckStoreSecrets(
	ctx context.Context,
	c client.Reader,
	namespace string,
	store *mysqlv1alpha1.S3ObjectStore,
) error {
	for _, selector := range credentialSelectors(store) {
		if selector == nil {
			continue
		}
		if _, err := credentialSecret(ctx, c, namespace, selector.Name); err != nil {
			return err
		}
	}
	return nil
}

// credentialSecret reads the named Secret and checks that it may be used as
// object-store credentials.
func credentialSecret(ctx context.Context, c client.Reader, namespace, name string) (*corev1.Secret, error) {
	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, secret); err != nil {
		return nil, fmt.Errorf("reading secret %s/%s: %w", namespace, name, err)
	}
	if err := CheckCredentialsSecret(secret); err != nil {
		return nil, err
	}
	return secret, nil
}

// ResolveConfig resolves an object store plus its Secret-backed credentials
// into a client Config, reading the referenced Secrets in namespace with c. It
// serves every caller with API access of its own: the reconcilers and the
// kubectl plugin.
//
// allowAmbient says whether a store with no static credentials may fall back to
// the caller's own identity. The kubectl plugin passes true: that identity is
// the user's. The operator passes its --object-store-ambient-credentials flag,
// off by default: its identity reaches every bucket it was granted, so lending
// it to any Cluster author would let one tenant read and delete another's
// backups. A refused store returns an error wrapping
// ErrAmbientCredentialsDisabled.
func ResolveConfig(
	ctx context.Context,
	c client.Reader,
	namespace string,
	store *mysqlv1alpha1.S3ObjectStore,
	allowAmbient bool,
) (Config, error) {
	var secrets StoreSecrets
	selectors := credentialSelectors(store)
	for i, into := range []*string{&secrets.AccessKeyID, &secrets.SecretAccessKey, &secrets.SessionToken} {
		if selectors[i] == nil {
			continue
		}
		secret, err := credentialSecret(ctx, c, namespace, selectors[i].Name)
		if err != nil {
			return Config{}, err
		}
		value, err := keyValue(secret, selectors[i].Key)
		if err != nil {
			return Config{}, err
		}
		*into = value
	}
	if store.TLS != nil && store.TLS.CABundleSecret != nil {
		value, err := secretValue(ctx, c, namespace, *store.TLS.CABundleSecret)
		if err != nil {
			return Config{}, err
		}
		secrets.CABundle = value
	}
	if !allowAmbient && secrets.AccessKeyID == "" && secrets.SecretAccessKey == "" {
		return Config{}, fmt.Errorf("object store %s has no static credentials: %w",
			store.Location(), ErrAmbientCredentialsDisabled)
	}
	cfg := ConfigFromStore(*store, secrets)
	cfg.AllowAmbient = allowAmbient
	return cfg, nil
}

func secretValue(
	ctx context.Context,
	c client.Reader,
	namespace string,
	selector mysqlv1alpha1.SecretKeySelector,
) (string, error) {
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: namespace, Name: selector.Name}
	if err := c.Get(ctx, key, secret); err != nil {
		return "", fmt.Errorf("reading secret %s/%s: %w", namespace, selector.Name, err)
	}
	return keyValue(secret, selector.Key)
}

func keyValue(secret *corev1.Secret, key string) (string, error) {
	value, ok := secret.Data[key]
	if !ok {
		return "", fmt.Errorf("secret %s/%s has no key %q", secret.Namespace, secret.Name, key)
	}
	return string(value), nil
}
