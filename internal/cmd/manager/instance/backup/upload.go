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

package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/backupworker"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// Backup methods the worker understands.
const (
	methodXtrabackup = "xtrabackup"
	methodLogical    = "logical"
)

// uploadStallTimeout bounds how long an upload may see no bytes move before it
// is failed as stalled. Tests shorten it; production uses the objectstore
// default (5 minutes).
var uploadStallTimeout = objectstore.DefaultStallTimeout

// stallFailure turns a stall-detected upload error into the failure reason the
// operator surfaces on the Backup, distinct from BackoffLimitExceeded: which
// side stopped decides between SourceStalled and ObjectStoreStalled.
func stallFailure(side objectstore.StallSide, err error) error {
	if side == objectstore.StallSource {
		return &backupworker.Failure{
			Reason: backupworker.ReasonSourceStalled,
			Err:    fmt.Errorf("backup: the source instance sent nothing for %s: %w", uploadStallTimeout, err),
		}
	}
	return &backupworker.Failure{
		Reason: backupworker.ReasonObjectStoreStalled,
		Err:    fmt.Errorf("backup: upload to the object store made no progress for %s: %w", uploadStallTimeout, err),
	}
}

// uploadFailure classifies an upload error: a stall by the side that stopped,
// a stream that outgrew its part size as ArchiveTooLarge, anything else as
// itself.
func uploadFailure(watch *objectstore.StallWatchReader, err error) error {
	switch {
	case watch.Stalled() || errors.Is(err, objectstore.ErrStalled):
		return stallFailure(watch.StalledSide(), err)
	case errors.Is(err, objectstore.ErrUploadTooLarge):
		return &backupworker.Failure{
			Reason: backupworker.ReasonArchiveTooLarge,
			Err: fmt.Errorf("backup: %w; the stream is larger than twice the data volume size "+
				"the part size was chosen for", err),
		}
	}
	return err
}

// uploadOptions configures the backup worker that streams a backup from a
// source instance to object storage.
type uploadOptions struct {
	// Method selects the stream: "xtrabackup" (the default) pulls a physical
	// archive from GET /cluster/backup, "logical" a SQL dump from
	// POST /cluster/dump.
	Method                  string
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
	// ExpectedSizeBytes estimates the stream size (the data volume size); it
	// picks the multipart part size so the upload fits the 10000-part limit.
	// Zero keeps the default part size.
	ExpectedSizeBytes int64
	// Databases and DumpArgs configure a logical dump.
	Databases []string
	DumpArgs  []string
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
	switch o.Method {
	case "", methodXtrabackup:
	case methodLogical:
		// The worker compresses and checksums a dump itself, always.
		if o.Compress {
			return errors.New("backup upload: --compress applies to xtrabackup; a logical dump is always compressed")
		}
	default:
		return fmt.Errorf("backup upload: unknown --method %q", o.Method)
	}
	return nil
}

// archiveStore is the part of the object-store client a physical upload uses.
type archiveStore interface {
	Upload(ctx context.Context, bucket, key string, reader io.Reader, size int64, contentType string) error
	PutJSON(ctx context.Context, bucket, key string, v any) error
	Remove(ctx context.Context, bucket, key string) error
}

// runUpload streams the source instance's XtraBackup archive over mTLS straight
// into object storage, checksumming it in flight, then writes an inspectable
// metadata manifest alongside it.
func runUpload(ctx context.Context, opts uploadOptions) error {
	if err := opts.validate(); err != nil {
		return err
	}

	store, err := objectstore.NewClientFromEnv()
	if err != nil {
		return err
	}
	// Size the multipart parts for the data volume: the default 64MiB parts
	// cap a stream at ~625GiB, and the part size is also the upload buffer.
	store.SetUploadPartSize(objectstore.UploadPartSizeFor(opts.ExpectedSizeBytes))

	client, err := mtlsClient(opts)
	if err != nil {
		return err
	}

	if opts.Method == methodLogical {
		// The dump account's password is not the worker's business (design
		// 030): the source instance manager reads the dump Secret itself.
		return runLogicalUpload(ctx, opts, store, client)
	}
	return runPhysicalUpload(ctx, opts, store, client)
}

// runPhysicalUpload pulls the archive stream from the source instance manager
// and uploads it. It is split from runUpload so tests can run it against a
// plain HTTP server and a fake store.
func runPhysicalUpload(ctx context.Context, opts uploadOptions, store archiveStore, client *http.Client) error {
	log := logf.FromContext(ctx).WithName("backup-upload").WithValues(
		"sourceURL", opts.SourceManagerURL,
		"bucket", opts.Bucket,
		"archiveKey", opts.ArchiveKey,
		"backupID", opts.BackupID,
	)

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
	// negative size lets the SDK use multipart uploads of an unknown length. The
	// stall watchdog cancels the upload context when bytes stop moving, so a
	// hung store or source fails the backup instead of riding out the Job's
	// active deadline.
	reader := objectstore.NewSHA256Reader(resp.Body)
	uploadCtx, cancelUpload := context.WithCancel(ctx)
	defer cancelUpload()
	watch := objectstore.NewStallWatchReader(reader, uploadStallTimeout, cancelUpload)
	defer func() { _ = watch.Close() }()
	log.Info("Uploading backup archive to object store")
	if err := store.Upload(uploadCtx, opts.Bucket, opts.ArchiveKey, watch, -1, "application/octet-stream"); err != nil {
		return uploadFailure(watch, err)
	}
	completedAt := time.Now().UTC()

	// The archive body is fully drained, so the source's post-stream trailers are
	// now populated. A resolution error means the source could not produce a
	// well-specified anchor; fail the backup so it is retried rather than shipping a
	// backup that would replay from genesis at recovery time.
	if anchorErr := resp.Trailer.Get(webserver.BackupAnchorErrorTrailer); anchorErr != "" {
		return fmt.Errorf("backup: source failed to resolve anchor GTID: %s", anchorErr)
	}
	anchorGTID := resp.Trailer.Get(webserver.BackupAnchorGTIDTrailer)
	anchorServer := resp.Trailer.Get(webserver.BackupAnchorServerTrailer)

	checksum := ""
	if opts.SHA256 {
		checksum = reader.SumHex()
	}
	log.Info("Backup archive uploaded", "bytes", reader.Count(), "sha256", checksum, "anchorGTID", anchorGTID)

	metadata := objectstore.BackupMetadata{
		BackupID:         opts.BackupID,
		ClusterName:      opts.ClusterName,
		BackupName:       opts.BackupName,
		InstanceName:     opts.InstanceName,
		Method:           methodXtrabackup,
		ArchiveKey:       opts.ArchiveKey,
		Compressed:       opts.Compress,
		SizeBytes:        reader.Count(),
		SHA256:           checksum,
		AnchorGTID:       anchorGTID,
		AnchorServerUUID: anchorServer,
		StartedAt:        startedAt,
		CompletedAt:      completedAt,
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
	cfg, err := webserver.ClientTLSConfig(webserver.ClientTLSOptions{
		CertFile: opts.TLSCert, KeyFile: opts.TLSKey, CAFile: opts.TLSCA, ServerName: opts.SourceManagerServerName,
	})
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}}, nil
}
