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

// Package join implements `manager instance join`: provision a replica.
package join

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/yyewolf/cnmysql/pkg/management/mysql/instance"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/replication"
)

// NewCommand builds the `instance join` command.
func NewCommand() *cobra.Command {
	var (
		xtrabackupPath string
		mysqldPath     string
		backupDir      string
		dataDir        string
		configFile     string
		socket         string
		serverVersion  string
		sourceHost     string
		sourcePort     int
		replUser       string
		useTLS         bool
		sslCA          string
		sslCert        string
		sslKey         string
		getPublicKey   bool
		managerURL     string
		managerName    string
		streamCompress bool
	)

	cmd := &cobra.Command{
		Use:   "join",
		Short: "Provision a replica from a source backup via XtraBackup",
		Long: "Restore a streamed XtraBackup into the data directory and configure " +
			"GTID replication so the replica resumes from the backup point when it " +
			"starts. The temporary server's root password is read from " +
			"MYSQL_ROOT_PASSWORD and the replication password from " +
			"MYSQL_REPLICATION_PASSWORD.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if serverVersion == "" {
				serverVersion = os.Getenv("MYSQL_VERSION")
			}
			if serverVersion == "" {
				return fmt.Errorf("--server-version or MYSQL_VERSION must be set")
			}

			// When a source manager URL is given, pull and extract the backup
			// stream over mTLS before restoring it. Idempotent: skip when the
			// data directory is already initialised.
			if managerURL != "" && !instance.IsInitialized(dataDir) {
				if err := instance.FetchBackup(cmd.Context(), instance.FetchOptions{
					SourceURL:      managerURL,
					BackupDir:      backupDir,
					XtrabackupPath: xtrabackupPath,
					Compress:       streamCompress,
					CAFile:         sslCA,
					CertFile:       sslCert,
					KeyFile:        sslKey,
					ServerName:     managerName,
				}); err != nil {
					return err
				}
			}

			return instance.Join(cmd.Context(), instance.JoinOptions{
				XtrabackupPath: xtrabackupPath,
				MysqldPath:     mysqldPath,
				BackupDir:      backupDir,
				DataDir:        dataDir,
				ConfigFile:     configFile,
				Socket:         socket,
				Version:        serverVersion,
				RootPassword:   os.Getenv("MYSQL_ROOT_PASSWORD"),
				Source: replication.SourceOptions{
					Host:         sourceHost,
					Port:         sourcePort,
					User:         replUser,
					Password:     os.Getenv("MYSQL_REPLICATION_PASSWORD"),
					AutoPosition: true,
					SSL:          useTLS,
					SSLCA:        sslCA,
					SSLCert:      sslCert,
					SSLKey:       sslKey,
					GetPublicKey: getPublicKey,
				},
			})
		},
	}

	cmd.Flags().StringVar(&xtrabackupPath, "xtrabackup", "xtrabackup", "Path to the xtrabackup binary")
	cmd.Flags().StringVar(&mysqldPath, "mysqld", "mysqld", "Path to the mysqld binary")
	cmd.Flags().StringVar(&backupDir, "backup-dir", "", "Directory holding the streamed backup")
	cmd.Flags().StringVar(&dataDir, "data-dir", "/var/lib/mysql", "MySQL data directory")
	cmd.Flags().StringVar(&configFile, "config", "/etc/mysql/my.cnf", "Path to the rendered my.cnf")
	cmd.Flags().StringVar(&socket, "socket", "/var/run/mysqld/mysqld.sock", "Unix socket for the temporary server")
	cmd.Flags().StringVar(&serverVersion, "server-version", "", "MySQL server version (e.g. 8.0.36)")
	cmd.Flags().StringVar(&sourceHost, "source-host", "", "Replication source host")
	cmd.Flags().IntVar(&sourcePort, "source-port", 3306, "Replication source port")
	cmd.Flags().StringVar(&replUser, "replication-user", "", "Replication user")
	cmd.Flags().BoolVar(&useTLS, "source-ssl", false, "Use TLS for the replication connection")
	cmd.Flags().StringVar(&sslCA, "source-ssl-ca", "", "Replication source CA certificate")
	cmd.Flags().StringVar(&sslCert, "source-ssl-cert", "", "Replication client certificate")
	cmd.Flags().StringVar(&sslKey, "source-ssl-key", "", "Replication client key")
	cmd.Flags().BoolVar(&getPublicKey, "source-get-public-key", false, "Request the source's public key for caching_sha2_password over a non-TLS connection")
	cmd.Flags().StringVar(&managerURL, "source-manager-url", "", "Source instance-manager backup stream URL; when set, the backup is pulled and extracted into --backup-dir over mTLS (reusing --source-ssl-* material)")
	cmd.Flags().StringVar(&managerName, "source-manager-server-name", "", "TLS server name to verify on the source manager certificate")
	cmd.Flags().BoolVar(&streamCompress, "source-stream-compress", false, "The backup stream is compressed and must be decompressed after extraction")

	return cmd
}
