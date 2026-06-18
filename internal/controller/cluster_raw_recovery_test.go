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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

// rawS3Server serves an S3 ListObjects response listing the given metadata keys
// and a metadata.json body keyed by object key.
func rawS3Server(t *testing.T, list string, metadata map[string]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/"+"metadata.json") {
			for key, body := range metadata {
				if strings.HasSuffix(r.URL.Path, key) {
					w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
					w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
					_, _ = w.Write([]byte(body))
					return
				}
			}
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(list))
	}))
}

func rawRecoveryCluster(endpoint, backupID string) *mysqlv1alpha1.Cluster {
	cluster := baseCluster()
	cluster.Status.CurrentPrimary = ""
	cluster.Spec.Bootstrap = &mysqlv1alpha1.BootstrapConfiguration{
		Recovery: &mysqlv1alpha1.BootstrapRecovery{Source: "prod", BackupID: backupID},
	}
	cluster.Spec.ExternalClusters = []mysqlv1alpha1.ExternalCluster{{
		Name: "prod",
		ObjectStore: &mysqlv1alpha1.S3ObjectStore{
			Bucket:   "cluster-backups",
			Path:     "clusters",
			Endpoint: endpoint,
			Credentials: mysqlv1alpha1.S3Credentials{
				AccessKeyID:     &mysqlv1alpha1.SecretKeySelector{Name: "cluster-s3", Key: "access"},
				SecretAccessKey: &mysqlv1alpha1.SecretKeySelector{Name: "cluster-s3", Key: "secret"},
			},
		},
	}}
	cluster.SetDefaults()
	return cluster
}

const rawList = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>cluster-backups</Name><Prefix>clusters/prod/</Prefix><KeyCount>2</KeyCount>
  <MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>
  <Contents><Key>clusters/prod/old/metadata.json</Key><Size>10</Size></Contents>
  <Contents><Key>clusters/prod/new/metadata.json</Key><Size>10</Size></Contents>
</ListBucketResult>`

func rawMetadata() map[string]string {
	return map[string]string{
		"clusters/prod/old/metadata.json": `{"backupID":"old","completedAt":"2026-06-10T10:00:00Z"}`,
		"clusters/prod/new/metadata.json": `{"backupID":"new","completedAt":"2026-06-12T10:00:00Z"}`,
	}
}

func rawRecoveryReconciler(t *testing.T) *ClusterReconciler {
	t.Helper()
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(s3CredentialsSecret()).Build()
	return &ClusterReconciler{Client: client, Scheme: scheme}
}

func TestResolveRawS3RecoveryLatest(t *testing.T) {
	t.Parallel()

	server := rawS3Server(t, rawList, rawMetadata())
	defer server.Close()

	cluster := rawRecoveryCluster(server.URL, "")
	reconciler := rawRecoveryReconciler(t)

	plan, err := reconciler.resolveRecovery(context.Background(), cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan == nil {
		t.Fatal("expected a recovery plan")
	}
	if plan.SourceCluster != "prod" {
		t.Fatalf("expected sourceCluster %q, got %q", "prod", plan.SourceCluster)
	}
	if want := "clusters/prod/new/backup.xbstream"; plan.ArchiveKey != want {
		t.Fatalf("expected latest archive key %q, got %q", want, plan.ArchiveKey)
	}
	if want := "clusters/prod/new/metadata.json"; plan.MetadataKey != want {
		t.Fatalf("expected metadata key %q, got %q", want, plan.MetadataKey)
	}
}

func TestResolveRawS3RecoveryByID(t *testing.T) {
	t.Parallel()

	server := rawS3Server(t, rawList, rawMetadata())
	defer server.Close()

	cluster := rawRecoveryCluster(server.URL, oldHash)
	reconciler := rawRecoveryReconciler(t)

	plan, err := reconciler.resolveRecovery(context.Background(), cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := "clusters/prod/old/backup.xbstream"; plan.ArchiveKey != want {
		t.Fatalf("expected pinned archive key %q, got %q", want, plan.ArchiveKey)
	}
}

func TestResolveRawS3RecoveryUnknownID(t *testing.T) {
	t.Parallel()

	server := rawS3Server(t, rawList, rawMetadata())
	defer server.Close()

	cluster := rawRecoveryCluster(server.URL, "missing")
	reconciler := rawRecoveryReconciler(t)

	if _, err := reconciler.resolveRecovery(context.Background(), cluster); err == nil {
		t.Fatal("expected an error for an unknown backupID")
	}
}

func TestResolveRawS3RecoveryEmptyDestination(t *testing.T) {
	t.Parallel()

	empty := `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>cluster-backups</Name><Prefix>clusters/prod/</Prefix><KeyCount>0</KeyCount>
  <MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>
</ListBucketResult>`
	server := rawS3Server(t, empty, nil)
	defer server.Close()

	cluster := rawRecoveryCluster(server.URL, "")
	reconciler := rawRecoveryReconciler(t)

	if _, err := reconciler.resolveRecovery(context.Background(), cluster); err == nil {
		t.Fatal("expected an error when no backups exist")
	}
}

func TestResolveRawS3RecoveryTarget(t *testing.T) {
	t.Parallel()

	server := rawS3Server(t, rawList, rawMetadata())
	defer server.Close()

	cluster := rawRecoveryCluster(server.URL, "")
	cluster.Spec.Bootstrap.Recovery.RecoveryTarget = &mysqlv1alpha1.RecoveryTarget{
		TargetGTID: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100",
	}
	reconciler := rawRecoveryReconciler(t)

	plan, err := reconciler.resolveRecovery(context.Background(), cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !plan.HasTarget || plan.TargetGTID == "" {
		t.Fatalf("expected PITR target to flow through, got %+v", plan)
	}
}
