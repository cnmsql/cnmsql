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

package backup

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/objectstore"
)

// uploadOptions configures the backup worker that streams a physical backup
// from a source instance to object storage.
type uploadOptions struct {
	SourceManagerURL        string
	SourceManagerServerName string
	Bucket                  string
	ArchiveKey              string
	MetadataKey             string
	BackupID                string
	BackupName              string
	ClusterName             string
	InstanceName            string
	TLSCert                 string
	TLSKey                  string
	TLSCA                   string
	Compress                bool
	SHA256                  bool
}

func (o uploadOptions) validate() error {
	missing := map[string]string{
		"--source-manager-url": o.SourceManagerURL,
		"--bucket":             o.Bucket,
		"--archive-key":        o.ArchiveKey,
		"--metadata-key":       o.MetadataKey,
		"--backup-id":          o.BackupID,
		"--cluster-name":       o.ClusterName,
		"--tls-cert":           o.TLSCert,
		"--tls-key":            o.TLSKey,
		"--tls-ca":             o.TLSCA,
	}
	for flag, value := range missing {
		if value == "" {
			return fmt.Errorf("backup upload: %s is required", flag)
		}
	}
	return nil
}

// runUpload streams the source instance's XtraBackup archive over mTLS straight
// into object storage, checksumming it in flight, then writes an inspectable
// metadata manifest alongside it.
func runUpload(ctx context.Context, opts uploadOptions) error {
	if err := opts.validate(); err != nil {
		return err
	}
	log := logf.FromContext(ctx).WithName("backup-upload").WithValues(
		"sourceURL", opts.SourceManagerURL,
		"bucket", opts.Bucket,
		"archiveKey", opts.ArchiveKey,
		"backupID", opts.BackupID,
	)

	store, err := objectstore.NewClientFromEnv()
	if err != nil {
		return err
	}

	client, err := mtlsClient(opts)
	if err != nil {
		return err
	}

	startedAt := time.Now().UTC()
	log.Info("Requesting backup stream from source instance")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, opts.SourceManagerURL, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("requesting backup stream: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("backup stream returned %s", resp.Status)
	}

	// Stream the archive straight to the object store, checksumming in flight. A
	// negative size lets the SDK use multipart uploads of an unknown length.
	reader := objectstore.NewSHA256Reader(resp.Body)
	log.Info("Uploading backup archive to object store")
	if err := store.Upload(ctx, opts.Bucket, opts.ArchiveKey, reader, -1, "application/octet-stream"); err != nil {
		return err
	}
	completedAt := time.Now().UTC()

	checksum := ""
	if opts.SHA256 {
		checksum = reader.SumHex()
	}
	log.Info("Backup archive uploaded", "bytes", reader.Count(), "sha256", checksum)

	metadata := objectstore.BackupMetadata{
		BackupID:     opts.BackupID,
		ClusterName:  opts.ClusterName,
		BackupName:   opts.BackupName,
		InstanceName: opts.InstanceName,
		Method:       "xtrabackup",
		ArchiveKey:   opts.ArchiveKey,
		Compressed:   opts.Compress,
		SizeBytes:    reader.Count(),
		SHA256:       checksum,
		StartedAt:    startedAt,
		CompletedAt:  completedAt,
	}
	log.Info("Writing backup metadata")
	if err := store.PutJSON(ctx, opts.Bucket, opts.MetadataKey, metadata); err != nil {
		return err
	}
	log.Info("Backup upload complete")
	return nil
}

// mtlsClient builds an HTTP client that mutually authenticates to the source
// instance manager. The transfer is unbounded: large datasets can take a long
// time to stream.
func mtlsClient(opts uploadOptions) (*http.Client, error) {
	cert, err := tls.LoadX509KeyPair(opts.TLSCert, opts.TLSKey)
	if err != nil {
		return nil, fmt.Errorf("loading client certificate: %w", err)
	}
	caPEM, err := os.ReadFile(opts.TLSCA)
	if err != nil {
		return nil, fmt.Errorf("reading CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("CA file %s contains no certificates", opts.TLSCA)
	}
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				MinVersion:   tls.VersionTLS12,
				ServerName:   opts.SourceManagerServerName,
				Certificates: []tls.Certificate{cert},
				RootCAs:      roots,
			},
		},
	}, nil
}
