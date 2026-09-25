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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// ResolveConfig resolves an object store plus its Secret-backed credentials
// into a client Config, reading the referenced Secrets in namespace with c. It
// serves every caller with API access of its own: the reconcilers and the
// kubectl plugin.
func ResolveConfig(
	ctx context.Context,
	c client.Reader,
	namespace string,
	store *mysqlv1alpha1.S3ObjectStore,
) (Config, error) {
	var secrets StoreSecrets
	creds := store.Credentials
	for _, ref := range []struct {
		selector *mysqlv1alpha1.SecretKeySelector
		into     *string
	}{
		{creds.AccessKeyID, &secrets.AccessKeyID},
		{creds.SecretAccessKey, &secrets.SecretAccessKey},
		{creds.SessionToken, &secrets.SessionToken},
	} {
		if ref.selector == nil {
			continue
		}
		value, err := secretValue(ctx, c, namespace, *ref.selector)
		if err != nil {
			return Config{}, err
		}
		*ref.into = value
	}
	if store.TLS != nil && store.TLS.CABundleSecret != nil {
		value, err := secretValue(ctx, c, namespace, *store.TLS.CABundleSecret)
		if err != nil {
			return Config{}, err
		}
		secrets.CABundle = value
	}
	return ConfigFromStore(*store, secrets), nil
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
	value, ok := secret.Data[selector.Key]
	if !ok {
		return "", fmt.Errorf("secret %s/%s has no key %q", namespace, selector.Name, selector.Key)
	}
	return string(value), nil
}
