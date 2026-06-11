/*
Copyright 2026 The CNMySQL Authors.

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

package instance

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"time"

	"github.com/yyewolf/cnmysql/pkg/management/mysql/xtrabackup"
)

// FetchOptions configures pulling a streamed backup from a source instance's
// control API and extracting it into a local directory.
type FetchOptions struct {
	// SourceURL is the source instance's streaming backup endpoint, e.g.
	// https://<cluster>-1.<ns>.svc:8080/cluster/backup.
	SourceURL string
	// BackupDir is where the extracted backup is written.
	BackupDir string
	// XBStreamPath is the xbstream binary (default "xbstream").
	XBStreamPath string
	// XtrabackupPath is used to decompress when Compress is set (default
	// "xtrabackup").
	XtrabackupPath string
	// Compress indicates the stream is compressed and must be decompressed after
	// extraction.
	Compress bool
	// TLS material for the mutually-authenticated pull. ServerName must match the
	// source server certificate.
	CAFile     string
	CertFile   string
	KeyFile    string
	ServerName string
	// Timeout bounds the whole transfer (0 = no client timeout; the backup can
	// take a long time on large datasets).
	Timeout time.Duration
}

func (o *FetchOptions) applyDefaults() {
	if o.XBStreamPath == "" {
		o.XBStreamPath = "xbstream"
	}
	if o.XtrabackupPath == "" {
		o.XtrabackupPath = defaultXtrabackupBinary
	}
}

// FetchBackup streams the source backup over mTLS and extracts it into
// BackupDir, leaving it ready for Join to prepare and restore.
func FetchBackup(ctx context.Context, opts FetchOptions) error {
	opts.applyDefaults()
	if opts.SourceURL == "" || opts.BackupDir == "" {
		return fmt.Errorf("fetch: source URL and backup dir are required")
	}
	if err := os.MkdirAll(opts.BackupDir, 0o750); err != nil {
		return fmt.Errorf("creating backup dir: %w", err)
	}

	transport, err := opts.transport()
	if err != nil {
		return err
	}
	client := &http.Client{Transport: transport, Timeout: opts.Timeout}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, opts.SourceURL, nil)
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

	// Pipe the response body straight into `xbstream -x`.
	extractArgs, err := xtrabackup.ExtractArgs(opts.BackupDir)
	if err != nil {
		return err
	}
	extract := exec.CommandContext(ctx, opts.XBStreamPath, extractArgs...)
	extract.Stdin = resp.Body
	extract.Stdout = os.Stdout
	extract.Stderr = os.Stderr
	if err := extract.Run(); err != nil {
		return fmt.Errorf("xbstream extract: %w", err)
	}

	if opts.Compress {
		decompressArgs, err := xtrabackup.DecompressArgs(opts.BackupDir)
		if err != nil {
			return err
		}
		decompress := exec.CommandContext(ctx, opts.XtrabackupPath, decompressArgs...)
		decompress.Stdout = os.Stdout
		decompress.Stderr = os.Stderr
		if err := decompress.Run(); err != nil {
			return fmt.Errorf("xtrabackup decompress: %w", err)
		}
	}
	return nil
}

func (o *FetchOptions) transport() (*http.Transport, error) {
	cert, err := tls.LoadX509KeyPair(o.CertFile, o.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("loading client certificate: %w", err)
	}
	caPEM, err := os.ReadFile(o.CAFile)
	if err != nil {
		return nil, fmt.Errorf("reading CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("CA file %s contains no certificates", o.CAFile)
	}
	return &http.Transport{
		TLSClientConfig: &tls.Config{
			MinVersion:   tls.VersionTLS12,
			ServerName:   o.ServerName,
			Certificates: []tls.Certificate{cert},
			RootCAs:      roots,
		},
	}, nil
}
