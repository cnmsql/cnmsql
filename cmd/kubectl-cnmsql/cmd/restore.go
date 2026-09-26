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
	"time"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// restoreOptions are the flags of `kubectl cnmsql restore`.
type restoreOptions struct {
	name      string
	backup    string
	source    string
	backupID  string
	databases []string
	policy    string
}

func newRestoreCommand() *cobra.Command {
	var opts restoreOptions
	cmd := &cobra.Command{
		Use:   "restore [CLUSTER]",
		Short: "Load databases from a logical backup into a running cluster",
		Long: "Create a LogicalRestore that loads the selected databases from a logical " +
			"backup into the cluster's primary. The load goes through the binary log, so " +
			"replicas follow it. It is not atomic: a failure can leave the selected " +
			"databases partly loaded, and the LogicalRestore status says so.",
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completeClusterArg,
		Example: `  # Restore one database from a logical Backup, refusing if it holds objects
  kubectl cnmsql restore cluster-sample --backup=nightly --databases=billing

  # Replace two databases with their copies in the dump
  kubectl cnmsql restore cluster-sample --backup=nightly --databases=billing,shop --policy=DropAndRecreate

  # Restore from a dump of another cluster, listed in spec.externalClusters
  kubectl cnmsql restore cluster-sample --source=prod --backup-id=nightly-1760000000 --databases=billing`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			env, err := newEnv()
			if err != nil {
				return err
			}
			cluster, err := env.ResolveClusterToModify(ctx, firstArg(args))
			if err != nil {
				return err
			}
			if opts.name == "" {
				opts.name = fmt.Sprintf("%s-restore-%s", cluster.Name, time.Now().Format("20060102150405"))
			}
			restore, err := buildLogicalRestore(cluster, opts)
			if err != nil {
				return err
			}
			if opts.backup != "" {
				note, err := checkRestoreBackup(ctx, env.Client, cluster.Namespace, opts.backup)
				if err != nil {
					return err
				}
				if note != "" {
					_, _ = fmt.Fprintln(cmd.ErrOrStderr(), note)
				}
			}
			if err := env.Client.Create(ctx, restore); err != nil {
				return fmt.Errorf("creating logical restore: %w", err)
			}
			fmt.Printf("created logical restore %q for cluster %q\n", restore.Name, cluster.Name)
			return nil
		},
	}
	cmd.Flags().StringVar(&opts.name, "name", "", "LogicalRestore name (default: <cluster>-restore-<timestamp>)")
	cmd.Flags().StringVar(&opts.backup, "backup", "", "completed logical Backup to restore from")
	cmd.Flags().StringVar(&opts.source, "source", "",
		"spec.externalClusters entry whose object store holds the dump (instead of --backup)")
	cmd.Flags().StringVar(&opts.backupID, "backup-id", "", "with --source: the dump to restore (default: the latest)")
	cmd.Flags().StringSliceVar(&opts.databases, "databases", nil, "comma-separated databases to restore (required)")
	cmd.Flags().StringVar(&opts.policy, "policy", string(mysqlv1alpha1.LogicalRestoreFailIfExists),
		"what to do with a database that already holds objects: FailIfExists|DropAndRecreate")
	_ = cmd.RegisterFlagCompletionFunc("backup",
		func(cmd *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return completeLogicalBackupArg(cmd, nil, toComplete)
		})
	_ = cmd.RegisterFlagCompletionFunc("policy",
		func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
			return []string{
				string(mysqlv1alpha1.LogicalRestoreFailIfExists),
				string(mysqlv1alpha1.LogicalRestoreDropAndRecreate),
			}, cobra.ShellCompDirectiveNoFileComp
		})
	return cmd
}

// buildLogicalRestore renders the LogicalRestore the command creates. It runs
// the same checks as admission, so a mistake fails before the API call.
func buildLogicalRestore(cluster *mysqlv1alpha1.Cluster, opts restoreOptions) (*mysqlv1alpha1.LogicalRestore, error) {
	switch {
	case (opts.backup == "") == (opts.source == ""):
		return nil, errors.New("set exactly one of --backup or --source")
	case opts.backupID != "" && opts.source == "":
		return nil, errors.New("--backup-id needs --source")
	}
	databases := cleanDatabases(opts.databases)
	if len(databases) == 0 {
		return nil, errors.New("--databases is required: a restore never loads a whole dump implicitly")
	}
	policy := mysqlv1alpha1.LogicalRestorePolicy(opts.policy)
	switch policy {
	case mysqlv1alpha1.LogicalRestoreFailIfExists, mysqlv1alpha1.LogicalRestoreDropAndRecreate:
	default:
		return nil, fmt.Errorf("--policy must be %s or %s", mysqlv1alpha1.LogicalRestoreFailIfExists,
			mysqlv1alpha1.LogicalRestoreDropAndRecreate)
	}
	restore := &mysqlv1alpha1.LogicalRestore{
		ObjectMeta: metav1.ObjectMeta{Name: opts.name, Namespace: cluster.Namespace},
		Spec: mysqlv1alpha1.LogicalRestoreSpec{
			Cluster:   mysqlv1alpha1.LocalObjectReference{Name: cluster.Name},
			Source:    opts.source,
			BackupID:  opts.backupID,
			Databases: databases,
			Policy:    policy,
		},
	}
	if opts.backup != "" {
		restore.Spec.Backup = &mysqlv1alpha1.LocalObjectReference{Name: opts.backup}
	}
	return restore, nil
}

// checkRestoreBackup catches a --backup the restore could never load before
// creating it: the controller keeps a restore of a missing Backup pending, in
// case it is created later. A Backup still running is fine, with a note.
func checkRestoreBackup(ctx context.Context, c client.Reader, namespace, name string) (string, error) {
	backup := &mysqlv1alpha1.Backup{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, backup); err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Errorf("backup %q not found in namespace %q", name, namespace)
		}
		return "", fmt.Errorf("reading backup %q: %w", name, err)
	}
	switch {
	case backup.Spec.Method != mysqlv1alpha1.BackupMethodLogical:
		return "", fmt.Errorf("backup %q is a %s backup; a restore loads a logical backup", name, backup.Spec.Method)
	case backup.Status.Phase == mysqlv1alpha1.BackupPhaseFailed:
		return "", fmt.Errorf("backup %q failed", name)
	case backup.Status.Phase != mysqlv1alpha1.BackupPhaseCompleted:
		return fmt.Sprintf("backup %q is not completed yet; the restore waits for it", name), nil
	}
	return "", nil
}
