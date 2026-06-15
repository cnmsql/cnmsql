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

// Package initdb implements `manager instance initdb`: fresh data-dir bootstrap.
package initdb

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/instance"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/version"
)

// NewCommand builds the `instance initdb` command.
func NewCommand() *cobra.Command {
	var (
		mysqldPath    string
		dataDir       string
		configFile    string
		socket        string
		database      string
		owner         string
		replUser      string
		requireTLS    bool
		charset       string
		collation     string
		controlUser   string
		backupUser    string
		metricsUser   string
		serverVersion string
	)

	cmd := &cobra.Command{
		Use:   "initdb",
		Short: "Initialise a fresh MySQL data directory",
		Long: "Initialise a fresh MySQL data directory and bootstrap the application " +
			"and replication accounts. Passwords are read from the environment " +
			"(MYSQL_ROOT_PASSWORD, MYSQL_APP_PASSWORD, MYSQL_REPLICATION_PASSWORD). " +
			"This command is idempotent: it is a no-op on an already initialised directory.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			rootPassword := os.Getenv("MYSQL_ROOT_PASSWORD")
			if rootPassword == "" {
				return fmt.Errorf("MYSQL_ROOT_PASSWORD must be set")
			}

			if serverVersion == "" {
				serverVersion = os.Getenv("MYSQL_VERSION")
			}
			// Dynamic privileges (admin interface, super_read_only) exist on
			// MySQL 8.0+. When the version is unknown, assume modern.
			dynamicPrivileges := true
			if serverVersion != "" {
				ver, err := version.Parse(serverVersion)
				if err != nil {
					return err
				}
				dynamicPrivileges = ver.AtLeast(8, 0, 0)
			}

			return instance.Initialize(cmd.Context(), instance.InitOptions{
				MysqldPath: mysqldPath,
				Version:    serverVersion,
				DataDir:    dataDir,
				ConfigFile: configFile,
				Socket:     socket,
				Bootstrap: instance.BootstrapParams{
					RootPassword:              rootPassword,
					Database:                  database,
					AppUser:                   owner,
					AppPassword:               os.Getenv("MYSQL_APP_PASSWORD"),
					CharacterSet:              charset,
					Collation:                 collation,
					ReplicationUser:           replUser,
					ReplicationPassword:       os.Getenv("MYSQL_REPLICATION_PASSWORD"),
					ReplicationRequireX509:    requireTLS,
					BackupUser:                backupUser,
					BackupPassword:            os.Getenv("MYSQL_BACKUP_PASSWORD"),
					ControlUser:               controlUser,
					ControlPassword:           os.Getenv("MYSQL_CONTROL_PASSWORD"),
					MetricsUser:               metricsUser,
					SupportsDynamicPrivileges: dynamicPrivileges,
				},
			})
		},
	}

	cmd.Flags().StringVar(&mysqldPath, "mysqld", "mysqld", "Path to the mysqld binary")
	cmd.Flags().StringVar(&dataDir, "data-dir", "/var/lib/mysql", "MySQL data directory")
	cmd.Flags().StringVar(&configFile, "config", "/etc/mysql/my.cnf", "Path to the rendered my.cnf")
	cmd.Flags().StringVar(&socket, "socket", "/var/run/mysqld/mysqld.sock", "Unix socket for the temporary server")
	cmd.Flags().StringVar(&database, "database", "", "Application database to create")
	cmd.Flags().StringVar(&owner, "owner", "", "Owner user of the application database")
	cmd.Flags().StringVar(&replUser, "replication-user", "", "Replication user to create")
	cmd.Flags().BoolVar(&requireTLS, "replication-require-x509", false, "Require a client certificate (mTLS) for the replication user")
	cmd.Flags().StringVar(&charset, "character-set", "", "Character set for the application database")
	cmd.Flags().StringVar(&collation, "collation", "", "Collation for the application database")
	cmd.Flags().StringVar(&controlUser, "control-user", "", "Privileged control user for the instance manager (password from MYSQL_CONTROL_PASSWORD)")
	cmd.Flags().StringVar(&backupUser, "backup-user", "", "XtraBackup user for cloning replicas (password from MYSQL_BACKUP_PASSWORD)")
	cmd.Flags().StringVar(&metricsUser, "metrics-user", "", "Local metrics exporter user to create")
	cmd.Flags().StringVar(&serverVersion, "server-version", "", "MySQL server version (e.g. 8.0.36); gates dynamic privilege grants")

	return cmd
}
