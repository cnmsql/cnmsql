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

// Package logicalrestore implements `manager instance logical-restore`: the
// worker a LogicalRestore runs. It streams a logical backup from object
// storage to the target primary's instance manager, which loads it.
package logicalrestore

import (
	"net/http"
	"time"

	"github.com/spf13/cobra"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// NewCommand builds the `instance logical-restore` command.
func NewCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "logical-restore",
		Short: "Load selected databases from a logical backup into a running primary",
		Long: "Stream a logical backup (a SQL dump) from S3-compatible object storage, " +
			"verify it and keep the selected databases, and post it over mTLS to the " +
			"target primary's instance manager, which loads it. Object-store " +
			"credentials are read from the cnmsql_S3_* environment variables.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			err := runWithEnv(cmd, opts)
			reportFailure(err)
			return err
		},
	}

	cmd.Flags().StringVar(&opts.TargetURL, "target-manager-url", "", "Target instance-manager load URL (/cluster/load)")
	cmd.Flags().StringVar(&opts.ServerName, "target-manager-server-name", "", "TLS server name for the target manager")
	cmd.Flags().StringVar(&opts.InstanceName, "instance-name", "", "Target instance name, for messages")
	cmd.Flags().StringVar(&opts.TLSCert, "tls-cert", "", "Client TLS certificate")
	cmd.Flags().StringVar(&opts.TLSKey, "tls-key", "", "Client TLS key")
	cmd.Flags().StringVar(&opts.TLSCA, "tls-ca", "", "Client TLS CA bundle")
	cmd.Flags().StringVar(&opts.Bucket, "bucket", "", "Source object-store bucket")
	cmd.Flags().StringVar(&opts.DumpKey, "dump-key", "", "Object key of the dump.sql.zst dump")
	cmd.Flags().StringVar(&opts.ManifestKey, "manifest-key", "", "Object key of the dump's logical.json manifest")
	// StringArray, not StringSlice: a database name may hold a comma.
	cmd.Flags().StringArrayVar(&opts.Databases, "database", nil, "Database to restore (repeatable, at least one)")
	cmd.Flags().StringVar(&opts.Policy, "policy", "", "FailIfExists or DropAndRecreate")
	cmd.Flags().StringVar(&opts.Flavor, "flavor", "mysql", "Engine flavor of the target cluster: mysql or mariadb")
	return cmd
}

func runWithEnv(cmd *cobra.Command, opts options) error {
	if err := opts.validate(); err != nil {
		return err
	}
	store, err := objectstore.NewClientFromEnv()
	if err != nil {
		return err
	}
	client, err := mtlsClient(opts)
	if err != nil {
		return err
	}
	return run(cmd.Context(), opts, store, client)
}

// expectContinueTimeout bounds the wait for the instance's "100 Continue".
// The instance sends it after its checks and the DropAndRecreate drops, which
// can take a while on a large database; past the timeout the client sends the
// body anyway, which is harmless.
const expectContinueTimeout = 5 * time.Minute

// mtlsClient builds an HTTP client that mutually authenticates to the target
// instance manager. The transfer is unbounded: a large restore can take hours.
func mtlsClient(opts options) (*http.Client, error) {
	cfg, err := webserver.ClientTLSConfig(webserver.ClientTLSOptions{
		CertFile: opts.TLSCert, KeyFile: opts.TLSKey, CAFile: opts.TLSCA, ServerName: opts.ServerName,
	})
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: &http.Transport{
		ExpectContinueTimeout: expectContinueTimeout,
		TLSClientConfig:       cfg,
	}}, nil
}
