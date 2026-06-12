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

// Package run implements `manager instance run`: the PID1 supervisor.
package run

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/yyewolf/cnmysql/pkg/management/mysql/instance"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/objectstore"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/pool"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/replication"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/webserver"
)

// NewCommand builds the `instance run` command.
func NewCommand() *cobra.Command {
	var (
		mysqldPath     string
		dataDir        string
		configFile     string
		socket         string
		serverVersion  string
		instanceName   string
		controlUser    string
		adminAddress   string
		adminPort      int
		webAddr        string
		healthAddr     string
		serverCert     string
		serverKey      string
		clientCA       string
		role           string
		sourceHost     string
		sourcePort     int
		replUser       string
		useSourceTLS   bool
		sourceSSLCA    string
		sourceSSLCert  string
		sourceSSLKey   string
		backupUser     string
		xtrabackupPath string
		clusterName    string
		namespace      string

		archiving         bool
		archiveRPOSeconds int
		archivePurge      bool
		mysqlbinlogPath   string
	)

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run as PID1, supervise mysqld and serve the control API",
		Long: "Run mysqld under supervision and expose the control API. The control " +
			"user's password is read from MYSQL_CONTROL_PASSWORD; the server version " +
			"from --server-version or MYSQL_VERSION.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if serverVersion == "" {
				serverVersion = os.Getenv("MYSQL_VERSION")
			}
			if serverVersion == "" {
				return fmt.Errorf("--server-version or MYSQL_VERSION must be set")
			}
			if instanceName == "" {
				instanceName = os.Getenv("POD_NAME")
			}
			expectedRole := webserver.Role(role)
			if expectedRole == "" {
				expectedRole = webserver.RolePrimary
			}

			if namespace == "" {
				namespace = os.Getenv("POD_NAMESPACE")
			}

			// Static replication connection parameters. The role reconciler fills
			// the source host from status.currentPrimary; the legacy fallback uses
			// the explicit --source-host.
			sourceTemplate := replication.SourceOptions{
				Port:         sourcePort,
				User:         replUser,
				Password:     os.Getenv("MYSQL_REPLICATION_PASSWORD"),
				AutoPosition: true,
				SSL:          useSourceTLS,
				SSLCA:        sourceSSLCA,
				SSLCert:      sourceSSLCert,
				SSLKey:       sourceSSLKey,
			}

			roleManaged := clusterName != "" && namespace != ""

			var source *replication.SourceOptions
			if !roleManaged && expectedRole == webserver.RoleReplica {
				if sourceHost == "" {
					return fmt.Errorf("--source-host must be set when --role=replica")
				}
				s := sourceTemplate
				s.Host = sourceHost
				source = &s
			}

			// Enable the streaming backup endpoint when a backup user is set, so
			// this instance can clone replicas.
			var backup *instance.BackupConfig
			if backupUser != "" {
				backup = &instance.BackupConfig{
					XtrabackupPath: xtrabackupPath,
					DataDir:        dataDir,
					Socket:         socket,
					User:           backupUser,
					Password:       os.Getenv("MYSQL_BACKUP_PASSWORD"),
				}
			}

			// Enable continuous binlog archiving when requested; the destination
			// bucket/path come from the environment alongside the S3 credentials.
			var archive *instance.ArchivingConfig
			if archiving {
				flush := time.Duration(archiveRPOSeconds) * time.Second
				archive = &instance.ArchivingConfig{
					Enabled:         true,
					ObjectStore:     objectstore.StoreFromEnv(),
					ClusterName:     clusterName,
					InstanceName:    instanceName,
					BinlogDir:       dataDir,
					MysqlbinlogPath: mysqlbinlogPath,
					FlushInterval:   flush,
					Purge:           archivePurge,
				}
			}

			return instance.Run(cmd.Context(), instance.RunOptions{
				MysqldPath:     mysqldPath,
				ConfigFile:     configFile,
				DataDir:        dataDir,
				Socket:         socket,
				Version:        serverVersion,
				InstanceName:   instanceName,
				Role:           expectedRole,
				Source:         source,
				ClusterName:    clusterName,
				Namespace:      namespace,
				SourceTemplate: sourceTemplate,
				WebserverAddr:  webAddr,
				HealthAddr:     healthAddr,
				Backup:         backup,
				Archiving:      archive,
				Control: pool.ControlParams{
					User:         controlUser,
					Password:     os.Getenv("MYSQL_CONTROL_PASSWORD"),
					Socket:       socket,
					AdminAddress: adminAddress,
					AdminPort:    adminPort,
				},
				TLS: webserver.TLSOptions{
					ServerCertFile: serverCert,
					ServerKeyFile:  serverKey,
					ClientCAFile:   clientCA,
				},
			})
		},
	}

	cmd.Flags().StringVar(&mysqldPath, "mysqld", "mysqld", "Path to the mysqld binary")
	cmd.Flags().StringVar(&dataDir, "data-dir", "/var/lib/mysql", "MySQL data directory")
	cmd.Flags().StringVar(&configFile, "config", "/etc/mysql/my.cnf", "Path to the rendered my.cnf")
	cmd.Flags().StringVar(&socket, "socket", "/var/run/mysqld/mysqld.sock", "Unix socket path")
	cmd.Flags().StringVar(&serverVersion, "server-version", "", "MySQL server version (e.g. 8.0.36)")
	cmd.Flags().StringVar(&instanceName, "instance-name", "", "Instance name reported in status")
	cmd.Flags().StringVar(&controlUser, "control-user", "root", "Privileged user for the control connection")
	cmd.Flags().StringVar(&adminAddress, "admin-address", "", "Administrative interface address (8.0.14+)")
	cmd.Flags().IntVar(&adminPort, "admin-port", 0, "Administrative interface port (8.0.14+)")
	cmd.Flags().StringVar(&webAddr, "web-addr", ":8080", "Control API listen address")
	cmd.Flags().StringVar(&healthAddr, "health-addr", ":8081", "Plain HTTP health probe listen address")
	cmd.Flags().StringVar(&serverCert, "tls-cert", "", "Control API server certificate (enables mTLS)")
	cmd.Flags().StringVar(&serverKey, "tls-key", "", "Control API server key")
	cmd.Flags().StringVar(&clientCA, "tls-client-ca", "", "Control API client CA bundle")
	cmd.Flags().StringVar(&role, "role", "primary", "Expected instance role: primary or replica")
	cmd.Flags().StringVar(&sourceHost, "source-host", "", "Replication source host when --role=replica")
	cmd.Flags().IntVar(&sourcePort, "source-port", 3306, "Replication source port when --role=replica")
	cmd.Flags().StringVar(&replUser, "replication-user", "", "Replication user when --role=replica")
	cmd.Flags().BoolVar(&useSourceTLS, "source-ssl", false, "Use TLS for the replication connection")
	cmd.Flags().StringVar(&sourceSSLCA, "source-ssl-ca", "", "Replication source CA certificate")
	cmd.Flags().StringVar(&sourceSSLCert, "source-ssl-cert", "", "Replication client certificate")
	cmd.Flags().StringVar(&sourceSSLKey, "source-ssl-key", "", "Replication client key")
	cmd.Flags().StringVar(&backupUser, "backup-user", "", "Backup user for streaming clones (password from MYSQL_BACKUP_PASSWORD); enables GET /cluster/backup")
	cmd.Flags().StringVar(&xtrabackupPath, "xtrabackup", "xtrabackup", "Path to the xtrabackup binary")
	cmd.Flags().StringVar(&clusterName, "cluster-name", "", "Owning Cluster name; enables the in-Pod role reconciler (dynamic role)")
	cmd.Flags().StringVar(&namespace, "namespace", "", "Cluster namespace (defaults to POD_NAMESPACE)")
	cmd.Flags().BoolVar(&archiving, "continuous-archiving", false, "Run the continuous binlog archiver (destination from CNMYSQL_S3_* env)")
	cmd.Flags().IntVar(&archiveRPOSeconds, "archive-rpo-seconds", 300, "Force a binlog rotation at least this often to bound RPO")
	cmd.Flags().BoolVar(&archivePurge, "archive-purge", true, "Purge binary logs once archived (the active purge gate)")
	cmd.Flags().StringVar(&mysqlbinlogPath, "mysqlbinlog", "mysqlbinlog", "Path to the mysqlbinlog binary")

	return cmd
}
