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

package cmd

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

func newBackupCommand() *cobra.Command {
	var (
		name   string
		method string
		target string
	)
	cmd := &cobra.Command{
		Use:   "backup [CLUSTER]",
		Short: "Take an on-demand backup of a cluster",
		Long: "Create a Backup resource referencing the cluster. The operator runs " +
			"the backup Job and reports progress in the Backup's status.",
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completeClusterArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			env, err := newEnv()
			if err != nil {
				return err
			}
			cluster, err := env.ResolveCluster(ctx, firstArg(args))
			if err != nil {
				return err
			}
			if name == "" {
				name = fmt.Sprintf("%s-%s", cluster.Name, time.Now().Format("20060102150405"))
			}
			backup := &mysqlv1alpha1.Backup{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace},
				Spec: mysqlv1alpha1.BackupSpec{
					Cluster: mysqlv1alpha1.LocalObjectReference{Name: cluster.Name},
					Method:  mysqlv1alpha1.BackupMethod(method),
					Target:  mysqlv1alpha1.BackupTarget(target),
				},
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
		"backup method: xtrabackup|volumeSnapshot")
	cmd.Flags().StringVar(&target, "target", string(mysqlv1alpha1.BackupTargetPreferStandby),
		"backup target: primary|prefer-standby")
	return cmd
}
