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

// Package restore implements `manager instance restore`: bootstrap a primary's
// data directory from a physical backup stored in object storage.
package restore

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/yyewolf/cnmysql/pkg/management/mysql/binlog"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/instance"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/objectstore"
)

// NewCommand builds the `instance restore` command.
func NewCommand() *cobra.Command {
	var (
		xtrabackupPath string
		xbstreamPath   string
		backupDir      string
		dataDir        string
		bucket         string
		archiveKey     string
		metadataKey    string
		compress       bool
		verifyChecksum bool
		mysqldPath     string
		configFile     string
		socket         string
		serverVersion  string
		controlUser    string
		backupUser     string

		// Point-in-time recovery (M7.2).
		sourceCluster   string
		targetTime      string
		targetGTID      string
		targetImmediate bool
		mysqlbinlogPath string
		mysqlPath       string
	)

	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore a physical backup from object storage into the data directory",
		Long: "Download an XtraBackup archive from S3-compatible object storage, " +
			"extract, prepare and restore it into the data directory. Idempotent: " +
			"a no-op when the data directory is already initialised. Object-store " +
			"credentials are read from the CNMYSQL_S3_* environment variables.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			store, err := objectstore.NewClientFromEnv()
			if err != nil {
				return err
			}
			if serverVersion == "" {
				serverVersion = os.Getenv("MYSQL_VERSION")
			}

			// Resolve the optional point-in-time recovery target. Replay is enabled
			// only when --source-cluster is set; the bucket/path come from the same
			// CNMYSQL_S3_* env the run container receives.
			var target binlog.RecoveryTarget
			if targetTime != "" {
				ts, err := time.Parse(time.RFC3339, targetTime)
				if err != nil {
					return fmt.Errorf("invalid --target-time %q: %w", targetTime, err)
				}
				target.Time = &ts
			}
			target.GTID = targetGTID
			target.Immediate = targetImmediate

			return instance.Restore(cmd.Context(), instance.RestoreOptions{
				Store:          store,
				Bucket:         bucket,
				ArchiveKey:     archiveKey,
				MetadataKey:    metadataKey,
				BackupDir:      backupDir,
				DataDir:        dataDir,
				XBStreamPath:   xbstreamPath,
				XtrabackupPath: xtrabackupPath,
				Compress:       compress,
				VerifyChecksum: verifyChecksum,
				// Post-restore credential reconcile. Passwords come from the same
				// environment the run container uses; root drives the reconcile.
				MysqldPath:      mysqldPath,
				ConfigFile:      configFile,
				Socket:          socket,
				Version:         serverVersion,
				RootPassword:    os.Getenv("MYSQL_ROOT_PASSWORD"),
				ControlUser:     controlUser,
				ControlPassword: os.Getenv("MYSQL_CONTROL_PASSWORD"),
				BackupUser:      backupUser,
				BackupPassword:  os.Getenv("MYSQL_BACKUP_PASSWORD"),
				// Point-in-time recovery: object-store layout from env, archive
				// cluster + target from flags.
				ObjectStore:     objectstore.StoreFromEnv(),
				SourceCluster:   sourceCluster,
				Target:          target,
				MysqlbinlogPath: mysqlbinlogPath,
				MysqlPath:       mysqlPath,
			})
		},
	}

	cmd.Flags().StringVar(&xtrabackupPath, "xtrabackup", "xtrabackup", "Path to the xtrabackup binary")
	cmd.Flags().StringVar(&xbstreamPath, "xbstream", "xbstream", "Path to the xbstream binary")
	cmd.Flags().StringVar(&backupDir, "backup-dir", "", "Scratch directory to extract the archive into")
	cmd.Flags().StringVar(&dataDir, "data-dir", "/var/lib/mysql", "MySQL data directory")
	cmd.Flags().StringVar(&bucket, "bucket", "", "Source object-store bucket")
	cmd.Flags().StringVar(&archiveKey, "archive-key", "", "Object key of the xbstream archive")
	cmd.Flags().StringVar(&metadataKey, "metadata-key", "", "Object key of the backup metadata; when set, drives decompression and checksum verification")
	cmd.Flags().BoolVar(&compress, "compress", false, "The archive is compressed and must be decompressed after extraction (overridden by metadata when present)")
	cmd.Flags().BoolVar(&verifyChecksum, "verify-checksum", true, "Verify the downloaded archive against the metadata SHA256")
	cmd.Flags().StringVar(&mysqldPath, "mysqld", "mysqld", "Path to the mysqld binary, used to reconcile restored credentials")
	cmd.Flags().StringVar(&configFile, "config", "/etc/mysql/my.cnf", "Path to the rendered my.cnf for the reconcile server")
	cmd.Flags().StringVar(&socket, "socket", "/var/run/mysqld/mysqld.sock", "Unix socket for the temporary reconcile server")
	cmd.Flags().StringVar(&serverVersion, "server-version", "", "MySQL server version; gates ALTER USER vs SET PASSWORD syntax")
	cmd.Flags().StringVar(&controlUser, "control-user", "", "Control account to reset to MYSQL_CONTROL_PASSWORD after restore")
	cmd.Flags().StringVar(&backupUser, "backup-user", "", "XtraBackup account to reset to MYSQL_BACKUP_PASSWORD after restore")

	// Point-in-time recovery (M7.2): replay archived binlogs after the base
	// restore. Enabled by --source-cluster; bucket/path come from CNMYSQL_S3_*.
	cmd.Flags().StringVar(&sourceCluster, "source-cluster", "", "Name of the cluster whose binlog archive to replay; enables point-in-time recovery")
	cmd.Flags().StringVar(&targetTime, "target-time", "", "Replay archived binlogs up to this RFC3339 timestamp")
	cmd.Flags().StringVar(&targetGTID, "target-gtid", "", "Replay archived binlogs up to this GTID set")
	cmd.Flags().BoolVar(&targetImmediate, "target-immediate", false, "Stop replay as soon as a consistent state is reached")
	cmd.Flags().StringVar(&mysqlbinlogPath, "mysqlbinlog", "mysqlbinlog", "Path to the mysqlbinlog binary used to decode archived binlogs")
	cmd.Flags().StringVar(&mysqlPath, "mysql", "mysql", "Path to the mysql client binary used to apply the replay")

	return cmd
}
