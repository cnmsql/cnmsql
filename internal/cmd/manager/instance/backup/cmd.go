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

// Package backup implements `manager instance backup`: one-shot backup worker
// commands used by the operator.
package backup

import (
	"github.com/spf13/cobra"
)

// NewCommand builds the `instance backup` command tree.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Run physical backup worker commands",
	}
	cmd.AddCommand(newUploadCommand())
	return cmd
}

func newUploadCommand() *cobra.Command {
	opts := uploadOptions{}

	cmd := &cobra.Command{
		Use:   "upload",
		Short: "Upload a streamed XtraBackup archive to object storage",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runUpload(cmd.Context(), opts)
		},
	}

	cmd.Flags().StringVar(&opts.SourceManagerURL, "source-manager-url", "", "Source instance-manager backup stream URL")
	cmd.Flags().StringVar(&opts.SourceManagerServerName, "source-manager-server-name", "", "TLS server name for the source manager")
	cmd.Flags().StringVar(&opts.Bucket, "bucket", "", "Destination object-store bucket")
	cmd.Flags().StringVar(&opts.ArchiveKey, "archive-key", "", "Destination object key for the xbstream archive")
	cmd.Flags().StringVar(&opts.MetadataKey, "metadata-key", "", "Destination object key for backup metadata")
	cmd.Flags().StringVar(&opts.BackupID, "backup-id", "", "Backup identifier")
	cmd.Flags().StringVar(&opts.BackupName, "backup-name", "", "Backup object name")
	cmd.Flags().StringVar(&opts.ClusterName, "cluster-name", "", "Cluster object name")
	cmd.Flags().StringVar(&opts.InstanceName, "instance-name", "", "Source instance name")
	cmd.Flags().StringVar(&opts.TLSCert, "tls-cert", "", "Client TLS certificate")
	cmd.Flags().StringVar(&opts.TLSKey, "tls-key", "", "Client TLS key")
	cmd.Flags().StringVar(&opts.TLSCA, "tls-ca", "", "Client TLS CA bundle")
	cmd.Flags().BoolVar(&opts.Compress, "compress", false, "The stream is compressed and recovery must decompress it")
	cmd.Flags().BoolVar(&opts.SHA256, "sha256", true, "Compute SHA256 while uploading")

	return cmd
}
