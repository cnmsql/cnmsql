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
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/CloudNative-MySQL/cloudnative-mysql/cmd/kubectl-cnmysql/plugin"
)

func newReinitCommand() *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "reinit CLUSTER INSTANCE",
		Short: "Re-initialise an instance from scratch (destroys its data, re-clones from a backup)",
		Long: "Request that the operator re-initialise an instance from the ground up: " +
			"it deletes the instance's Pod and PVC and recreates them empty, so the " +
			"bootstrap re-clones a fresh copy from a backup and rejoins replication. " +
			"The instance keeps its name and ordinal (hence its server_id); only its " +
			"data is discarded.\n\n" +
			"This is the remediation for a diverged or irrecoverably broken replica " +
			"(MySQL has no pg_rewind to surgically realign one). It is DESTRUCTIVE and " +
			"IRREVERSIBLE: any data only present on this instance — e.g. errant " +
			"transactions — is lost. The current primary cannot be re-initialised this way.",
		Args: cobra.ExactArgs(2),
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			switch len(args) {
			case 0:
				return completeCluster(cmd.Context(), toComplete)
			case 1:
				return completeInstance(cmd.Context(), args[0], toComplete)
			default:
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runReinit(cmd.Context(), args[0], args[1], yes)
		},
	}
	cmd.Flags().BoolVarP(&yes, "yes", "y", false, "skip confirmation prompts")
	return cmd
}

func runReinit(ctx context.Context, clusterName, instance string, yes bool) error {
	env, err := newEnv()
	if err != nil {
		return err
	}
	cluster, err := env.GetCluster(ctx, clusterName)
	if err != nil {
		return err
	}

	if instance == plugin.PrimaryInstance(cluster) {
		return fmt.Errorf("%q is the current primary and cannot be re-initialised (it is the replication source)", instance)
	}

	pods, err := env.ListPods(ctx, cluster)
	if err != nil {
		return err
	}
	found := false
	for i := range pods {
		if pods[i].Name == instance {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("instance %q not found in cluster %q", instance, cluster.Name)
	}

	if !plugin.Confirm(
		fmt.Sprintf("Re-initialise %q? This DESTROYS its data and re-clones from a backup; "+
			"any data only on this instance is lost.", instance),
		yes,
	) {
		fmt.Println("aborted")
		return nil
	}

	// Append the instance to the Cluster's reinit annotation (the operator-consumed
	// contract), preserving any other instances already queued.
	requested := splitReinit(cluster.Annotations[plugin.ReinitAnnotation])
	if !plugin.Contains(requested, instance) {
		requested = append(requested, instance)
	}
	before := cluster.DeepCopy()
	if cluster.Annotations == nil {
		cluster.Annotations = map[string]string{}
	}
	cluster.Annotations[plugin.ReinitAnnotation] = strings.Join(requested, ",")
	if err := env.Client.Patch(ctx, cluster, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("requesting re-initialisation of %q: %w", instance, err)
	}
	fmt.Printf("requested re-initialisation of %q (operator will delete its Pod and PVC and re-clone)\n", instance)
	return nil
}

// splitReinit parses the comma-separated reinit annotation value into instance
// names, dropping blanks and surrounding whitespace.
func splitReinit(raw string) []string {
	if raw == "" {
		return nil
	}
	var out []string
	for part := range strings.SplitSeq(raw, ",") {
		if name := strings.TrimSpace(part); name != "" {
			out = append(out, name)
		}
	}
	return out
}
