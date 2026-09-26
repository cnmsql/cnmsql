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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

func TestBackupStorePrecedence(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := mysqlv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	store := func(bucket string) *mysqlv1alpha1.S3ObjectStore {
		return &mysqlv1alpha1.S3ObjectStore{Bucket: bucket}
	}
	cluster := func(s *mysqlv1alpha1.S3ObjectStore) *mysqlv1alpha1.Cluster {
		c := &mysqlv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "src", Namespace: "ns"}}
		if s != nil {
			c.Spec.Backup = &mysqlv1alpha1.BackupConfiguration{ObjectStore: s}
		}
		return c
	}
	backup := func(status, spec *mysqlv1alpha1.S3ObjectStore) *mysqlv1alpha1.Backup {
		b := &mysqlv1alpha1.Backup{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns"}}
		b.Spec.Cluster.Name = "src"
		b.Spec.ObjectStore = spec
		b.Status.ObjectStore = status
		return b
	}

	for _, tc := range []struct {
		name     string
		backup   *mysqlv1alpha1.Backup
		objects  []client.Object
		fallback *mysqlv1alpha1.S3ObjectStore
		want     string
	}{
		{"status wins over a rotated cluster store", backup(store("ran"), store("spec")),
			[]client.Object{cluster(store("rotated"))}, nil, "ran"},
		{"spec before the cluster", backup(nil, store("spec")),
			[]client.Object{cluster(store("cluster"))}, nil, "spec"},
		{"the cluster's store", backup(nil, nil), []client.Object{cluster(store("cluster"))}, store("fb"), "cluster"},
		{"fallback for a deleted cluster", backup(nil, nil), nil, store("fb"), "fb"},
		{"fallback for a cluster with no store", backup(nil, nil), []client.Object{cluster(nil)}, store("fb"), "fb"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.objects...).Build()
			got, err := BackupStore(context.Background(), c, tc.backup, tc.fallback)
			if err != nil {
				t.Fatal(err)
			}
			if got.Bucket != tc.want {
				t.Errorf("bucket = %q, want %q", got.Bucket, tc.want)
			}
			if tc.backup.Status.ObjectStore != nil && got == tc.backup.Status.ObjectStore {
				t.Error("the Backup's own store was returned, not a copy")
			}
		})
	}

	t.Run("nothing names a store", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		if _, err := BackupStore(context.Background(), c, backup(nil, nil), nil); !errors.Is(err, ErrNoBackupStore) {
			t.Errorf("err = %v, want ErrNoBackupStore", err)
		}
	})

	t.Run("an unreadable cluster is not a missing store", func(t *testing.T) {
		boom := errors.New("apiserver down")
		c := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
				return boom
			},
		}).Build()
		_, err := BackupStore(context.Background(), c, backup(nil, nil), store("fb"))
		if !errors.Is(err, boom) || errors.Is(err, ErrNoBackupStore) {
			t.Errorf("err = %v, want the read error", err)
		}
	})
}
