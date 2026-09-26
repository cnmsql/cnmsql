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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/backupworker"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/sqldump"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/tail"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// errUploadClosed is the error the copy sees when the upload has ended the
// pipe. It is a routing marker, never a reported failure. It relies on the
// store contract that Upload returns nil only after draining its reader to
// EOF: if a store ever returned early, the manifest would describe a prefix
// of the stream while the footer check still validates the source's tail.
var errUploadClosed = errors.New("upload finished")

var (
	// dumpRetryInterval and dumpRetryTimeout bound the retries of refusals that
	// clear on their own: a replica that has not applied the dump account yet,
	// or a dump from an earlier attempt still being torn down.
	dumpRetryInterval = 5 * time.Second
	dumpRetryTimeout  = 2 * time.Minute
)

// logicalStore is the part of the object-store client a logical upload uses.
type logicalStore interface {
	Upload(ctx context.Context, bucket, key string, reader io.Reader, size int64, contentType string) error
	PutJSON(ctx context.Context, bucket, key string, v any) error
	Remove(ctx context.Context, bucket, key string) error
}

// runLogicalUpload asks the source instance for a dump, compresses it with
// zstd, checksums and uploads it, checks it is complete, and writes the
// logical.json manifest.
func runLogicalUpload(
	ctx context.Context,
	opts uploadOptions,
	store logicalStore,
	client *http.Client,
	password string,
) error {
	log := logf.FromContext(ctx).WithName("backup-upload").WithValues(
		"sourceURL", opts.SourceManagerURL,
		"bucket", opts.Bucket,
		"archiveKey", opts.ArchiveKey,
		"backupID", opts.BackupID,
		"method", methodLogical,
	)

	startedAt := time.Now().UTC()
	resp, err := requestDump(logf.IntoContext(ctx, log), client, opts, password)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	databases, err := webserver.DecodeDumpDatabases(resp.Header.Get(webserver.DumpDatabasesHeader))
	if err != nil {
		return fmt.Errorf("backup: decoding the dumped databases: %w", err)
	}
	log.Info("Uploading logical dump to object store",
		"tool", resp.Header.Get(webserver.DumpToolHeader), "databases", databases)

	// Source → footer window + byte count → zstd → pipe → SHA256 → object store.
	pr, pw := io.Pipe()
	footer := tail.NewWriter(sqldump.FooterWindow)
	var uncompressed int64
	copyDone := make(chan error, 1)
	go func() {
		zw, err := objectstore.NewZstdWriter(pw)
		if err != nil {
			pw.CloseWithError(err)
			copyDone <- err
			return
		}
		n, err := io.Copy(io.MultiWriter(zw, footer), resp.Body)
		uncompressed = n
		if err == nil {
			err = zw.Close()
		} else {
			err = fmt.Errorf("reading the dump stream: %w", err)
		}
		pw.CloseWithError(err)
		copyDone <- err
	}()
	archive := objectstore.NewSHA256Reader(pr)
	uploadErr := store.Upload(ctx, opts.Bucket, opts.ArchiveKey, archive, -1, "application/zstd")
	// Unblock the copy if the upload gave up first.
	pr.CloseWithError(errUploadClosed)
	copyErr := <-copyDone
	completedAt := time.Now().UTC()

	// Past this point something was uploaded; a failed dump must not leave a
	// half-written archive behind.
	fail := func(reason string, err error) error {
		if rmErr := store.Remove(context.WithoutCancel(ctx), opts.Bucket, opts.ArchiveKey); rmErr != nil {
			log.Info("Could not remove the incomplete dump", "error", rmErr.Error())
		}
		return &backupworker.Failure{Reason: reason, Err: err}
	}
	switch {
	case copyErr != nil && !errors.Is(copyErr, errUploadClosed):
		return fail(backupworker.ReasonDumpFailed, fmt.Errorf("backup: %w", copyErr))
	case uploadErr != nil:
		return fail("", uploadErr)
	}
	// The body is drained, so the source's trailers are in.
	if dumpErr := resp.Trailer.Get(webserver.DumpErrorTrailer); dumpErr != "" {
		return fail(backupworker.ReasonDumpFailed, fmt.Errorf("backup: the source failed the dump: %s", dumpErr))
	}
	if !sqldump.HasFooter(footer.Bytes()) {
		return fail(backupworker.ReasonDumpFailed, errors.New("backup: the dump has no completion footer; it was cut short"))
	}

	metadata := objectstore.LogicalBackupMetadata{
		FormatVersion:     objectstore.LogicalFormatVersion,
		BackupID:          opts.BackupID,
		ClusterName:       opts.ClusterName,
		BackupName:        opts.BackupName,
		InstanceName:      opts.InstanceName,
		Method:            methodLogical,
		Tool:              resp.Header.Get(webserver.DumpToolHeader),
		Flavor:            resp.Header.Get(webserver.DumpFlavorHeader),
		ServerVersion:     resp.Header.Get(webserver.DumpServerVersionHeader),
		Compression:       objectstore.LogicalCompressionZstd,
		ArchiveKey:        opts.ArchiveKey,
		SizeBytes:         archive.Count(),
		UncompressedBytes: uncompressed,
		SHA256:            archive.SumHex(),
		Databases:         databases,
		SnapshotGTID:      resp.Trailer.Get(webserver.DumpSnapshotGTIDTrailer),
		SnapshotBinlog:    resp.Trailer.Get(webserver.DumpSnapshotBinlogTrailer),
		StartedAt:         startedAt,
		CompletedAt:       completedAt,
	}
	log.Info("Logical dump uploaded", "bytes", metadata.SizeBytes, "uncompressedBytes", uncompressed,
		"sha256", metadata.SHA256, "snapshotBinlog", metadata.SnapshotBinlog)
	if err := store.PutJSON(ctx, opts.Bucket, opts.MetadataKey, metadata); err != nil {
		return fail("", err)
	}
	log.Info("Backup upload complete")
	return nil
}

// requestDump posts the dump request, retrying the refusals that clear on their
// own, and turns every other refusal into a failure with its reason.
func requestDump(
	ctx context.Context,
	client *http.Client,
	opts uploadOptions,
	password string,
) (*http.Response, error) {
	log := logf.FromContext(ctx)
	payload, err := json.Marshal(webserver.DumpRequest{
		Password:  password,
		Databases: opts.Databases,
		ExtraArgs: opts.DumpArgs,
	})
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(dumpRetryTimeout)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, opts.SourceManagerURL, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		log.Info("Requesting logical dump from source instance")
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("requesting dump: %w", err)
		}
		if resp.StatusCode == http.StatusOK {
			return resp, nil
		}
		refusal := backupworker.ReadRefusal(resp)
		_ = resp.Body.Close()
		retryable := resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusConflict
		if retryable && time.Now().Before(deadline) {
			log.Info("Source refused the dump, will retry", "status", resp.Status, "reason", refusal.Reason,
				"error", refusal.Error)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(dumpRetryInterval):
			}
			continue
		}
		return nil, refusalFailure(resp.StatusCode, opts.InstanceName, refusal)
	}
}

func refusalFailure(status int, instance string, refusal webserver.ReasonErrorBody) error {
	if backupworker.ManagerPredatesEndpoint(status) {
		return &backupworker.Failure{
			Reason: backupworker.ReasonInstanceManagerOutdated,
			Err: fmt.Errorf("instance %s does not serve logical dumps yet: its instance manager predates them; "+
				"retry once the operator upgrade has reached it", instance),
		}
	}
	reason := refusal.Reason
	switch {
	case reason != "":
	case status == http.StatusNotImplemented:
		// Only a missing dump client answers 501, with or without a body.
		reason = webserver.DumpReasonToolUnavailable
	default:
		reason = backupworker.ReasonDumpFailed
	}
	return &backupworker.Failure{Reason: reason,
		Err: fmt.Errorf("instance %s refused the dump (%d): %s", instance, status, refusal.Error)}
}
