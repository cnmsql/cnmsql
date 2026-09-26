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

package logicalrestore

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sync/atomic"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/backupworker"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/sqldump"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/tail"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// options configures the restore worker.
type options struct {
	TargetURL    string
	ServerName   string
	InstanceName string
	TLSCert      string
	TLSKey       string
	TLSCA        string
	Bucket       string
	DumpKey      string
	ManifestKey  string
	Databases    []string
	Policy       string
	Flavor       string
}

func (o options) validate() error {
	for flag, value := range map[string]string{
		"--target-manager-url": o.TargetURL,
		"--tls-cert":           o.TLSCert,
		"--tls-key":            o.TLSKey,
		"--tls-ca":             o.TLSCA,
		"--bucket":             o.Bucket,
		"--dump-key":           o.DumpKey,
		"--manifest-key":       o.ManifestKey,
		"--policy":             o.Policy,
	} {
		if value == "" {
			return fmt.Errorf("logical-restore: %s is required", flag)
		}
	}
	if len(o.Databases) == 0 {
		return errors.New("logical-restore: at least one --database is required")
	}
	return nil
}

// store is the part of the object-store client a restore uses.
type store interface {
	GetJSON(ctx context.Context, bucket, key string, v any) error
	Download(ctx context.Context, bucket, key string, writer io.Writer) (int64, error)
}

// progressInterval is how often the worker logs how far the restore got.
var progressInterval = 30 * time.Second

// refusalReasons are the instance's answers that mean it changed nothing.
var refusalReasons = []string{
	webserver.LoadReasonToolUnavailable,
	webserver.LoadReasonInProgress,
	webserver.LoadReasonNotPrimary,
	webserver.LoadReasonInvalidRequest,
	webserver.LoadReasonDatabaseNotEmpty,
}

// run reads and checks the manifest, then streams the dump through its
// checksum, the zstd decoder, a footer window and the database filter into
// POST /cluster/load on the target. The target loads it and answers once the
// load is over.
func run(ctx context.Context, opts options, st store, client *http.Client) error {
	log := logf.FromContext(ctx).WithName("logical-restore").WithValues(
		"target", opts.InstanceName, "dumpKey", opts.DumpKey, "databases", opts.Databases)
	ctx = logf.IntoContext(ctx, log)

	var meta objectstore.LogicalBackupMetadata
	if err := st.GetJSON(ctx, opts.Bucket, opts.ManifestKey, &meta); err != nil {
		return unchanged(backupworker.ReasonDownloadFailed,
			fmt.Errorf("reading manifest s3://%s/%s: %w", opts.Bucket, opts.ManifestKey, err))
	}
	if err := meta.CheckImportable(opts.Flavor, opts.Databases); err != nil {
		return unchanged(backupworker.ReasonIncompatible, err)
	}

	stream := newRestoreStream(opts, st, meta)
	body, pw := io.Pipe()
	streamDone := make(chan struct{})
	go func() {
		defer close(streamDone)
		stream.run(ctx, pw)
	}()
	stopProgress := stream.logProgress(ctx, meta.SizeBytes)
	defer stopProgress()

	target, err := loadURL(opts)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/sql")
	// The instance checks the request and applies the policy before it asks
	// for the body, so a refused restore never streams the dump.
	req.Header.Set("Expect", "100-continue")
	log.Info("Restoring logical backup", "backupID", meta.BackupID, "sourceCluster", meta.ClusterName,
		"sourceVersion", meta.ServerVersion, "policy", opts.Policy)
	resp, doErr := client.Do(req)
	// The transport closes the body once it is done with it, which stops the
	// stream if the instance answered before reading all of it.
	_ = body.CloseWithError(errBodyDone)
	<-streamDone
	stopProgress()

	if doErr != nil {
		// A stream failure is the root cause of whatever the transport saw.
		if streamErr := stream.failure(); streamErr != nil {
			return streamErr
		}
		if targetUnreachable(doErr) {
			return unchanged(backupworker.ReasonTargetUnreachable,
				fmt.Errorf("connecting to instance %s: %w", opts.InstanceName, doErr))
		}
		return changed(webserver.LoadReasonFailed,
			fmt.Errorf("the connection to instance %s failed during the load: %w", opts.InstanceName, doErr))
	}
	defer func() { _ = resp.Body.Close() }()

	// A refusal decides the outcome even when the stream failed too: the
	// instance changed nothing, whatever happened to the download meanwhile.
	switch {
	case resp.StatusCode == http.StatusOK:
		if streamErr := stream.failure(); streamErr != nil {
			return streamErr
		}
	case backupworker.ManagerPredatesEndpoint(resp.StatusCode):
		return unchanged(backupworker.ReasonInstanceManagerOutdated,
			fmt.Errorf("instance %s does not serve logical loads yet: its instance manager predates them; "+
				"retry once the operator upgrade has reached it", opts.InstanceName))
	default:
		refusal := backupworker.ReadRefusal(resp)
		reason := refusal.Reason
		if reason == "" {
			reason = webserver.LoadReasonFailed
		}
		err := fmt.Errorf("instance %s refused or failed the load (%d): %s", opts.InstanceName, resp.StatusCode, refusal.Error)
		if slices.Contains(refusalReasons, reason) {
			return unchanged(reason, err)
		}
		if streamErr := stream.failure(); streamErr != nil {
			return streamErr
		}
		return changed(reason, err)
	}

	var result webserver.LoadResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return changed(webserver.LoadReasonFailed, fmt.Errorf("reading the load result: %w", err))
	}
	log.Info("Logical backup restored", "databases", result.Databases, "bytes", result.Bytes,
		"compressedBytes", stream.downloaded.Load())
	return nil
}

// targetUnreachable reports whether a request failed before it reached the
// instance's handler: the connection or the TLS handshake failed, so no check
// ran and nothing was changed.
func targetUnreachable(err error) bool {
	var (
		opErr       *net.OpError
		verifyErr   *tls.CertificateVerificationError
		alertErr    tls.AlertError
		headerErr   tls.RecordHeaderError
		authorityEr x509.UnknownAuthorityError
		hostnameErr x509.HostnameError
	)
	return (errors.As(err, &opErr) && opErr.Op == "dial") ||
		errors.As(err, &verifyErr) || errors.As(err, &alertErr) || errors.As(err, &headerErr) ||
		errors.As(err, &authorityEr) || errors.As(err, &hostnameErr)
}

// loadURL is the target URL with the databases and the policy in its query.
func loadURL(opts options) (string, error) {
	u, err := url.Parse(opts.TargetURL)
	if err != nil {
		return "", fmt.Errorf("logical-restore: parsing --target-manager-url: %w", err)
	}
	q := u.Query()
	for _, db := range opts.Databases {
		q.Add(webserver.LoadDatabaseParam, db)
	}
	q.Set(webserver.LoadPolicyParam, opts.Policy)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// errBodyDone ends the request body once the transport returned. It is a
// routing marker, never a reported failure.
var errBodyDone = errors.New("request finished")

// restoreStream produces the request body: object store → SHA256 → zstd →
// footer window → database filter.
type restoreStream struct {
	opts       options
	store      store
	meta       objectstore.LogicalBackupMetadata
	downloaded atomic.Int64
	err        atomic.Pointer[error]
}

func newRestoreStream(opts options, st store, meta objectstore.LogicalBackupMetadata) *restoreStream {
	return &restoreStream{opts: opts, store: st, meta: meta}
}

// run writes the stream to w and closes it. It ends w cleanly only when the
// whole dump went through with a matching checksum and its footer; otherwise it
// aborts w, so the instance kills its client before the statement it holds
// runs.
func (s *restoreStream) run(ctx context.Context, w *io.PipeWriter) {
	dr, dw := io.Pipe()
	hash := objectstore.NewSHA256Writer(dw)
	downloadDone := make(chan error, 1)
	go func() {
		_, err := s.store.Download(ctx, s.opts.Bucket, s.opts.DumpKey, &countingWriter{w: hash, n: &s.downloaded})
		_ = dw.CloseWithError(err)
		downloadDone <- err
	}()

	footer := tail.NewWriter(sqldump.FooterWindow)
	copyErr := func() error {
		dec, err := objectstore.NewZstdReader(dr)
		if err != nil {
			return err
		}
		defer func() { _ = dec.Close() }()
		_, err = sqldump.FilterDatabases(w, io.TeeReader(dec, footer), s.opts.Databases)
		return err
	}()
	// Unblock the download if the copy stopped early; a no-op once it is done.
	_ = dr.CloseWithError(errBodyDone)
	downloadErr := <-downloadDone
	if errors.Is(downloadErr, errBodyDone) {
		downloadErr = nil
	}

	var fail error
	switch {
	case errors.Is(copyErr, io.ErrClosedPipe), errors.Is(copyErr, errBodyDone):
		// The request ended first: the instance answered (a refusal, or a
		// failed load). Its answer is the outcome. This relies on
		// FilterDatabases returning the body writer's error as it came.
	case downloadErr != nil:
		fail = changed(backupworker.ReasonDownloadFailed,
			fmt.Errorf("downloading s3://%s/%s: %w", s.opts.Bucket, s.opts.DumpKey, downloadErr))
	case copyErr != nil:
		fail = changed(backupworker.ReasonDumpCorrupt, fmt.Errorf("reading the dump: %w", copyErr))
	case s.meta.SHA256 != "" && hash.SumHex() != s.meta.SHA256:
		fail = changed(backupworker.ReasonDumpCorrupt, fmt.Errorf("the dump's checksum is %s and the manifest has %s",
			hash.SumHex(), s.meta.SHA256))
	case !sqldump.HasFooter(footer.Bytes()):
		fail = changed(backupworker.ReasonDumpCorrupt, errors.New("the dump has no completion footer; it was cut short"))
	}
	if fail != nil {
		s.err.Store(&fail)
		_ = w.CloseWithError(fail)
		return
	}
	_ = w.Close()
}

func (s *restoreStream) failure() error {
	if p := s.err.Load(); p != nil {
		return *p
	}
	return nil
}

// logProgress logs how much of the dump was read every progressInterval,
// against the manifest's size when it has one. It returns a stop function that
// is safe to call twice.
func (s *restoreStream) logProgress(ctx context.Context, total int64) func() {
	log := logf.FromContext(ctx)
	done := make(chan struct{})
	var once atomic.Bool
	go func() {
		ticker := time.NewTicker(progressInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				read := s.downloaded.Load()
				if total > 0 {
					log.Info("Restore in progress", "compressedBytesRead", read, "compressedBytesTotal", total,
						"percent", read*100/total)
				} else {
					log.Info("Restore in progress", "compressedBytesRead", read)
				}
			}
		}
	}()
	return func() {
		if once.CompareAndSwap(false, true) {
			close(done)
		}
	}
}

// countingWriter counts the bytes written through it, for progress logging.
type countingWriter struct {
	w io.Writer
	n *atomic.Int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n.Add(int64(n))
	return n, err
}
