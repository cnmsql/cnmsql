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
	"net/http"
	"net/http/httptest"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

const listNonEmpty = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>cluster-backups</Name><Prefix>clusters/demo/</Prefix><KeyCount>1</KeyCount>
  <MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated>
  <Contents><Key>clusters/demo/old/id/backup.xbstream</Key><Size>42</Size></Contents>
</ListBucketResult>`

const listEmpty = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>cluster-backups</Name><Prefix>clusters/demo/</Prefix><KeyCount>0</KeyCount>
  <MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated>
</ListBucketResult>`

func s3CredentialsSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-s3", Namespace: "default"},
		Data:       map[string][]byte{"access": []byte("key"), "secret": []byte("secret")},
	}
}

func freshArchivingCluster(endpoint string) *mysqlv1alpha1.Cluster {
	cluster := baseBackupCluster()
	cluster.Status.CurrentPrimary = "" // fresh: no primary established yet
	cluster.SetDefaults()              // path-style + signature defaults on the store
	cluster.Spec.Backup.ObjectStore.Endpoint = endpoint
	return cluster
}

func guardReconciler(t *testing.T) *ClusterReconciler {
	t.Helper()
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(s3CredentialsSecret()).Build()
	return &ClusterReconciler{Client: client, Scheme: scheme}
}

func TestCheckBackupDestinationBlocksNonEmpty(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(listNonEmpty))
	}))
	defer server.Close()

	cluster := freshArchivingCluster(server.URL)
	reconciler := guardReconciler(t)

	check := reconciler.checkBackupDestination(context.Background(), cluster)
	if check.Retry != nil {
		t.Fatalf("unexpected retry: %v", check.Retry)
	}
	if check.Blocked == "" {
		t.Fatal("expected a non-empty destination to block the cluster")
	}
}

func TestCheckBackupDestinationAllowsEmpty(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(listEmpty))
	}))
	defer server.Close()

	cluster := freshArchivingCluster(server.URL)
	reconciler := guardReconciler(t)

	check := reconciler.checkBackupDestination(context.Background(), cluster)
	if check.Retry != nil || check.Blocked != "" {
		t.Fatalf("empty destination should pass, got blocked=%q retry=%v", check.Blocked, check.Retry)
	}
}

func TestCheckBackupDestinationSkipsEstablishedCluster(t *testing.T) {
	t.Parallel()

	// A reachable primary means the cluster already owns its archive; the check
	// must not run (and must not need the object store at all).
	cluster := baseBackupCluster()                                  // CurrentPrimary = "demo-1"
	cluster.Spec.Backup.ObjectStore.Endpoint = "http://127.0.0.1:1" // would fail if dialed
	reconciler := guardReconciler(t)

	check := reconciler.checkBackupDestination(context.Background(), cluster)
	if check.Blocked != "" || check.Retry != nil {
		t.Fatalf("established cluster should skip the check, got blocked=%q retry=%v", check.Blocked, check.Retry)
	}
}

func TestCheckBackupDestinationSkipsRecovery(t *testing.T) {
	t.Parallel()

	cluster := freshArchivingCluster("http://127.0.0.1:1") // would fail if dialed
	cluster.Spec.Bootstrap = &mysqlv1alpha1.BootstrapConfiguration{
		Recovery: &mysqlv1alpha1.BootstrapRecovery{
			Backup: &mysqlv1alpha1.LocalObjectReference{Name: "backup-sample"},
		},
	}
	reconciler := guardReconciler(t)

	check := reconciler.checkBackupDestination(context.Background(), cluster)
	if check.Blocked != "" || check.Retry != nil {
		t.Fatalf("recovery bootstrap should skip the check, got blocked=%q retry=%v", check.Blocked, check.Retry)
	}
}

func TestCheckBackupDestinationSkipsWithoutObjectStore(t *testing.T) {
	t.Parallel()

	cluster := baseCluster()
	cluster.Status.CurrentPrimary = ""
	reconciler := guardReconciler(t)

	check := reconciler.checkBackupDestination(context.Background(), cluster)
	if check.Blocked != "" || check.Retry != nil {
		t.Fatalf("cluster without a backup store should skip the check, got blocked=%q retry=%v", check.Blocked, check.Retry)
	}
}
