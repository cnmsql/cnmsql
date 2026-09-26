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

// Package importdump implements `manager instance import`: load a logical
// backup into a freshly initialised data directory.
package importdump

import (
	"cmp"
	"os"

	"github.com/spf13/cobra"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/credentials"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/instance"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// NewCommand builds the `instance import` command.
func NewCommand() *cobra.Command {
	var (
		mysqldPath    string
		mysqlPath     string
		configFile    string
		dataDir       string
		socket        string
		workDir       string
		bucket        string
		dumpKey       string
		manifestKey   string
		databases     []string
		postImportSQL []string

		creds credentials.Options
	)

	cmd := &cobra.Command{
		Use:   "import",
		Short: "Load a logical backup from object storage into the data directory",
		Long: "Load a logical backup (a SQL dump) from S3-compatible object storage into " +
			"a freshly initialised data directory, through a temporary socket-only " +
			"server with binary logging off. Idempotent: a no-op once an import has " +
			"finished. Object-store credentials are read from the cnmsql_S3_* " +
			"environment variables, the root password from the cluster's " +
			"credential Secrets.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			store, err := objectstore.NewClientFromEnv()
			if err != nil {
				return err
			}
			creds.Namespace = os.Getenv("POD_NAMESPACE")
			src, err := credentials.Open(cmd.Context(), creds, credentials.Root)
			if err != nil {
				return err
			}
			rootPassword, _ := src.Password(credentials.Root)
			return instance.Import(cmd.Context(), instance.ImportOptions{
				Store:         store,
				Bucket:        bucket,
				DumpKey:       dumpKey,
				ManifestKey:   manifestKey,
				Databases:     databases,
				PostImportSQL: postImportSQL,
				// The engine (selected from CNMSQL_FLAVOR, set by the controller)
				// picks the SQL client; it falls back to MySQL when unset.
				Engine:       engine.MustForFlavor(engine.Flavor(os.Getenv("CNMSQL_FLAVOR"))),
				MysqldPath:   mysqldPath,
				LoadPath:     mysqlPath,
				ConfigFile:   configFile,
				DataDir:      dataDir,
				Socket:       socket,
				WorkDir:      cmp.Or(workDir, instance.ScratchWorkDir()),
				RootPassword: rootPassword,
			})
		},
	}

	cmd.Flags().StringVar(&mysqldPath, "mysqld", "mysqld", "Path to the mysqld binary for the temporary server")
	cmd.Flags().StringVar(&mysqlPath, "mysql", "", "Override the SQL client that loads the dump (defaults to the engine's client: mysql / mariadb)")
	cmd.Flags().StringVar(&configFile, "config", "/etc/mysql/my.cnf", "Path to the rendered my.cnf for the temporary server")
	cmd.Flags().StringVar(&dataDir, "data-dir", "/var/lib/mysql", "MySQL data directory")
	cmd.Flags().StringVar(&socket, "socket", "/var/run/mysqld/mysqld.sock", "Unix socket for the temporary server")
	cmd.Flags().StringVar(&workDir, "work-dir", "", "Directory for the SQL client's credentials file (defaults to the Pod's scratch volume, else the system temp dir)")
	cmd.Flags().StringVar(&bucket, "bucket", "", "Source object-store bucket")
	cmd.Flags().StringVar(&dumpKey, "dump-key", "", "Object key of the dump.sql.zst dump")
	cmd.Flags().StringVar(&manifestKey, "manifest-key", "", "Object key of the dump's logical.json manifest")
	// StringArray, not StringSlice: a database name or a statement may hold a
	// comma, and each flag is one value.
	cmd.Flags().StringArrayVar(&databases, "database", nil, "Load only this database from the dump (repeatable; default: every database in it)")
	cmd.Flags().StringArrayVar(&postImportSQL, "post-import-sql", nil, "SQL statement run as root after the load (repeatable, in order)")
	cmd.Flags().StringVar(&creds.ClusterName, "cluster-name", "", "Owning Cluster name; locates the credential Secrets")
	credentials.AddFlags(cmd.Flags(), &creds)

	return cmd
}
