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
	"database/sql"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/binlog"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/objectstore"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/webserver"
)

// ArchivingConfig configures the in-Pod continuous binlog archiver.
type ArchivingConfig struct {
	// Enabled turns the archiver on. The loop still only ships from the primary.
	Enabled bool
	// ObjectStore is the destination bucket + key prefix. Credentials/endpoint
	// come from the CNMYSQL_S3_* environment.
	ObjectStore mysqlv1alpha1.S3ObjectStore
	// ClusterName and InstanceName identify this segment of the archive.
	ClusterName  string
	InstanceName string
	// BinlogDir is the directory holding the local binary-log files (the
	// datadir, where log_bin writes them).
	BinlogDir string
	// MysqlbinlogPath is the mysqlbinlog binary (defaults to PATH lookup).
	MysqlbinlogPath string
	// FlushInterval bounds the time-based RPO via forced FLUSH BINARY LOGS.
	FlushInterval time.Duration
	// Purge enables the active purge gate.
	Purge bool
}

// startArchiver builds and runs the continuous binlog archiver loop against the
// given control connection. It returns the loop (so its state can be surfaced in
// status) and a channel that receives the loop's terminal error. It blocks only
// long enough to read server_uuid; the loop itself runs in a goroutine.
func startArchiver(
	ctx context.Context,
	cfg ArchivingConfig,
	db *sql.DB,
) (*binlog.Loop, <-chan error, error) {
	log := logf.FromContext(ctx).WithName("archiver")
	store, err := objectstore.NewClientFromEnv()
	if err != nil {
		return nil, nil, err
	}
	reader := binlog.NewReader(db)
	serverUUID, err := reader.ServerUUID(ctx)
	if err != nil {
		return nil, nil, err
	}

	archiver, err := binlog.NewArchiver(binlog.ArchiverOptions{
		Store:        store,
		ObjectStore:  cfg.ObjectStore,
		ClusterName:  cfg.ClusterName,
		InstanceName: cfg.InstanceName,
		ServerUUID:   serverUUID,
		BinlogDir:    cfg.BinlogDir,
		Scan:         binlog.MysqlbinlogScanner(cfg.MysqlbinlogPath, log.WithName("mysqlbinlog")),
	})
	if err != nil {
		return nil, nil, err
	}

	loop := binlog.NewLoop(binlog.LoopOptions{
		Reader:        reader,
		Archiver:      archiver,
		Logger:        log,
		FlushInterval: cfg.FlushInterval,
		Purge:         cfg.Purge,
	})

	errCh := make(chan error, 1)
	go func() { errCh <- loop.Run(ctx) }()

	log.Info("Started continuous binlog archiver",
		"serverUUID", serverUUID,
		"bucket", cfg.ObjectStore.Bucket,
		"binlogDir", cfg.BinlogDir,
		"purgeGate", cfg.Purge)
	return loop, errCh, nil
}

// archivingStatusProvider adapts a Loop's State to the webserver status shape.
func archivingStatusProvider(loop *binlog.Loop) func() *webserver.ArchivingStatus {
	return func() *webserver.ArchivingStatus {
		s := loop.State()
		out := &webserver.ArchivingStatus{
			Active:             s.Active,
			LastArchivedBinlog: s.LastArchivedBinlog,
			LastArchivedGTID:   s.LastArchivedGTID,
			PendingFiles:       s.PendingFiles,
			LastError:          s.LastError,
		}
		if !s.LastArchivedTime.IsZero() {
			out.LastArchivedTime = s.LastArchivedTime.UTC().Format(time.RFC3339)
		}
		if !s.LastErrorTime.IsZero() {
			out.LastErrorTime = s.LastErrorTime.UTC().Format(time.RFC3339)
		}
		return out
	}
}
