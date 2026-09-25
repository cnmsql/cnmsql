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

package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// downloadOptions configures `backup download`.
type downloadOptions struct {
	// Output is the file to write, or "-" for stdout. Empty picks
	// <backup>.sql.zst, or <backup>.sql with Decompress.
	Output string
	// Decompress writes plain SQL instead of the zstd archive.
	Decompress bool
	// Endpoint replaces the object store's endpoint, for a store only
	// reachable inside the cluster (through a port-forward).
	Endpoint string
}

func newBackupDownloadCommand() *cobra.Command {
	var opts downloadOptions
	cmd := &cobra.Command{
		Use:   "download BACKUP",
		Short: "Download the SQL dump of a logical backup",
		Long: "Download the dump of a completed logical Backup from its object store and " +
			"verify its checksum. The plugin reads the store's credentials from the Secrets " +
			"the Backup's store references, and connects to the store from this machine. " +
			"When the store's endpoint is only reachable inside the cluster, port-forward " +
			"to it and pass --endpoint.",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeLogicalBackupArg,
		Example: `  # Download a logical backup as dump.sql.zst (named after the Backup)
  kubectl cnmsql backup download cluster-sample-20260925120000

  # Write plain SQL instead
  kubectl cnmsql backup download cluster-sample-20260925120000 --decompress -o dump.sql

  # Load it somewhere else without keeping a file
  kubectl cnmsql backup download cluster-sample-20260925120000 --decompress -o - | mysql -h db.example

  # Reach an in-cluster object store through a port-forward
  kubectl -n storage port-forward svc/minio 9000:9000 &
  kubectl cnmsql backup download cluster-sample-20260925120000 --endpoint http://127.0.0.1:9000`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			env, err := newEnv()
			if err != nil {
				return err
			}
			backup := &mysqlv1alpha1.Backup{}
			if err := env.Client.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: args[0]}, backup); err != nil {
				return fmt.Errorf("reading backup %q: %w", args[0], err)
			}
			written, err := downloadLogicalBackup(ctx, env.Client, backup, opts, cmd.OutOrStdout())
			if err != nil {
				return err
			}
			if written != "-" {
				_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "downloaded backup %q to %s\n", backup.Name, written)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&opts.Output, "output", "o", "",
		`file to write, or "-" for stdout (default: <backup>.sql.zst, or <backup>.sql with --decompress)`)
	cmd.Flags().BoolVar(&opts.Decompress, "decompress", false, "write plain SQL instead of the zstd archive")
	cmd.Flags().StringVar(&opts.Endpoint, "endpoint", "",
		"object-store endpoint to use instead of the one in the store's spec (e.g. a port-forward)")
	return cmd
}

// downloadLogicalBackup writes a logical backup's dump to opts.Output (stdout
// when it is "-") and verifies its checksum against the manifest. A file is
// written next to its final name and renamed only once the checksum matches,
// so a failed download never leaves a file that looks complete. It returns
// where the dump went.
func downloadLogicalBackup(
	ctx context.Context,
	c client.Reader,
	backup *mysqlv1alpha1.Backup,
	opts downloadOptions,
	stdout io.Writer,
) (string, error) {
	if backup.Spec.Method != mysqlv1alpha1.BackupMethodLogical {
		return "", fmt.Errorf("backup %q is a %s backup; only logical backups can be downloaded",
			backup.Name, backup.Spec.Method)
	}
	if backup.Status.Phase != mysqlv1alpha1.BackupPhaseCompleted || backup.Status.BackupID == "" {
		return "", fmt.Errorf("backup %q is not completed (phase %q)", backup.Name, backup.Status.Phase)
	}
	store, err := downloadObjectStore(ctx, c, backup)
	if err != nil {
		return "", err
	}
	cfg, err := objectstore.ResolveConfig(ctx, c, backup.Namespace, store)
	if err != nil {
		return "", err
	}
	if opts.Endpoint != "" {
		cfg.Endpoint = opts.Endpoint
	}
	osClient, err := objectstore.NewClient(cfg)
	if err != nil {
		return "", err
	}
	keys, err := objectstore.BuildLogicalBackupKeys(*store, backup.Spec.Cluster.Name, backup.Name, backup.Status.BackupID)
	if err != nil {
		return "", err
	}
	var meta objectstore.LogicalBackupMetadata
	if err := osClient.GetJSON(ctx, store.Bucket, keys.MetadataKey, &meta); err != nil {
		return "", fmt.Errorf("reading manifest s3://%s/%s: %w", store.Bucket, keys.MetadataKey, err)
	}
	want := meta.SHA256
	if want == "" {
		want = backup.Status.SHA256
	}

	output := opts.Output
	if output == "" {
		output = backup.Name + ".sql.zst"
		if opts.Decompress {
			output = backup.Name + ".sql"
		}
	}
	if output == "-" {
		return output, streamDump(ctx, osClient, store.Bucket, keys.ArchiveKey, want, opts.Decompress, stdout)
	}

	// O_EXCL on the final name refuses to overwrite a file the user already
	// has; the dump itself goes to a partial file renamed at the end.
	final, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", err
	}
	_ = final.Close()
	partial := output + ".partial"
	f, err := os.OpenFile(partial, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		_ = os.Remove(output)
		return "", err
	}
	err = streamDump(ctx, osClient, store.Bucket, keys.ArchiveKey, want, opts.Decompress, f)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(partial, output)
	}
	if err != nil {
		_ = os.Remove(partial)
		_ = os.Remove(output)
		return "", err
	}
	return output, nil
}

// streamDump copies the dump object to w, decompressing it on the way when
// asked, and checks the compressed bytes against the expected SHA256.
func streamDump(
	ctx context.Context,
	osClient *objectstore.Client,
	bucket, key, wantSHA256 string,
	decompress bool,
	w io.Writer,
) error {
	var sum string
	if !decompress {
		hash := objectstore.NewSHA256Writer(w)
		if _, err := osClient.Download(ctx, bucket, key, hash); err != nil {
			return err
		}
		sum = hash.SumHex()
	} else {
		pr, pw := io.Pipe()
		hash := objectstore.NewSHA256Writer(pw)
		downloaded := make(chan error, 1)
		go func() {
			_, err := osClient.Download(ctx, bucket, key, hash)
			_ = pw.CloseWithError(err)
			downloaded <- err
		}()
		dec, err := objectstore.NewZstdReader(pr)
		if err == nil {
			_, err = io.Copy(w, dec)
			_ = dec.Close()
		}
		_ = pr.CloseWithError(errDownloadStopped)
		if downloadErr := <-downloaded; downloadErr != nil && !errors.Is(downloadErr, errDownloadStopped) {
			return downloadErr
		}
		if err != nil {
			return fmt.Errorf("decompressing s3://%s/%s: %w", bucket, key, err)
		}
		sum = hash.SumHex()
	}
	if wantSHA256 != "" && !strings.EqualFold(sum, wantSHA256) {
		return fmt.Errorf("checksum mismatch for s3://%s/%s: got %s, want %s", bucket, key, sum, wantSHA256)
	}
	return nil
}

// errDownloadStopped closes the download pipe when decompression stopped
// reading it, so the download's own error can be told apart from it.
var errDownloadStopped = errors.New("download stopped")

// downloadObjectStore picks the store a Backup was written to: its own
// override, else its cluster's.
func downloadObjectStore(
	ctx context.Context, c client.Reader, backup *mysqlv1alpha1.Backup,
) (*mysqlv1alpha1.S3ObjectStore, error) {
	var store *mysqlv1alpha1.S3ObjectStore
	if backup.Spec.ObjectStore != nil {
		store = backup.Spec.ObjectStore.DeepCopy()
	} else {
		cluster := &mysqlv1alpha1.Cluster{}
		key := types.NamespacedName{Namespace: backup.Namespace, Name: backup.Spec.Cluster.Name}
		if err := c.Get(ctx, key, cluster); err != nil {
			return nil, fmt.Errorf("backup %q has no object store of its own and its cluster could not be read: %w",
				backup.Name, err)
		}
		if cluster.Spec.Backup == nil || cluster.Spec.Backup.ObjectStore == nil {
			return nil, fmt.Errorf("neither backup %q nor cluster %q has an object store", backup.Name, cluster.Name)
		}
		store = cluster.Spec.Backup.ObjectStore.DeepCopy()
	}
	store.SetDefaults()
	return store, nil
}
