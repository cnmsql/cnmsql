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
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

func newBackupCommand() *cobra.Command {
	var (
		name      string
		method    string
		target    string
		databases []string
	)
	cmd := &cobra.Command{
		Use:   "backup [CLUSTER]",
		Short: "Take an on-demand backup of a cluster",
		Long: "Create a Backup resource referencing the cluster. The operator runs " +
			"the backup Job and reports progress in the Backup's status.",
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completeClusterArg,
		Example: `  # Take an on-demand backup (xtrabackup, prefer-standby target)
  kubectl cnmsql backup cluster-sample

  # Take a backup from the primary
  kubectl cnmsql backup cluster-sample --target=primary

  # Name the backup explicitly
  kubectl cnmsql backup cluster-sample --name=pre-upgrade

  # Take a logical backup (SQL dump) of two databases
  kubectl cnmsql backup cluster-sample --method=logical --databases=billing,catalog`,
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
			if name == "" {
				name = fmt.Sprintf("%s-%s", cluster.Name, time.Now().Format("20060102150405"))
			}
			backup, err := buildBackup(cluster, name, method, target, databases)
			if err != nil {
				return err
			}
			if err := env.Client.Create(ctx, backup); err != nil {
				return fmt.Errorf("creating backup: %w", err)
			}
			fmt.Printf("created backup %q for cluster %q\n", name, cluster.Name)
			return nil
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "backup name (default: <cluster>-<timestamp>)")
	cmd.Flags().StringVar(&method, "method", string(mysqlv1alpha1.BackupMethodXtrabackup),
		"backup method: xtrabackup|volumeSnapshot|logical")
	cmd.Flags().StringVar(&target, "target", string(mysqlv1alpha1.BackupTargetPreferStandby),
		"backup target: primary|prefer-standby")
	cmd.Flags().StringSliceVar(&databases, "databases", nil,
		"logical backup only: comma-separated databases to dump (default: every application database)")
	_ = cmd.RegisterFlagCompletionFunc("method",
		func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
			return []string{
				string(mysqlv1alpha1.BackupMethodXtrabackup),
				string(mysqlv1alpha1.BackupMethodVolumeSnapshot),
				string(mysqlv1alpha1.BackupMethodLogical),
			}, cobra.ShellCompDirectiveNoFileComp
		})
	_ = cmd.RegisterFlagCompletionFunc("target",
		func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
			return []string{
				string(mysqlv1alpha1.BackupTargetPrimary),
				string(mysqlv1alpha1.BackupTargetPreferStandby),
			}, cobra.ShellCompDirectiveNoFileComp
		})
	return cmd
}

// buildBackup renders the Backup the command creates.
func buildBackup(
	cluster *mysqlv1alpha1.Cluster, name, method, target string, databases []string,
) (*mysqlv1alpha1.Backup, error) {
	backup := &mysqlv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace},
		Spec: mysqlv1alpha1.BackupSpec{
			Cluster: mysqlv1alpha1.LocalObjectReference{Name: cluster.Name},
			Method:  mysqlv1alpha1.BackupMethod(method),
			Target:  mysqlv1alpha1.BackupTarget(target),
		},
	}
	databases = cleanDatabases(databases)
	if len(databases) > 0 {
		if backup.Spec.Method != mysqlv1alpha1.BackupMethodLogical {
			return nil, fmt.Errorf("--databases needs --method=%s", mysqlv1alpha1.BackupMethodLogical)
		}
		backup.Spec.Logical = &mysqlv1alpha1.LogicalBackupOptions{Databases: databases}
	}
	return backup, nil
}

// cleanDatabases trims whitespace from each --databases entry, drops empty
// entries and removes duplicates, preserving order.
func cleanDatabases(databases []string) []string {
	cleaned := make([]string, 0, len(databases))
	seen := make(map[string]struct{}, len(databases))
	for _, database := range databases {
		database = strings.TrimSpace(database)
		if database == "" {
			continue
		}
		if _, ok := seen[database]; ok {
			continue
		}
		seen[database] = struct{}{}
		cleaned = append(cleaned, database)
	}
	return cleaned
}
