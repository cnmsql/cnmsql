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
	"fmt"
	"sort"

	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/cmd/kubectl-cnmsql/plugin"
)

// newGroupCommand builds the `group` subtree, which inspects and operates on a
// MySQL Group Replication cluster's quorum and membership. Async clusters have
// no group, so every subcommand refuses to run against them.
func newGroupCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "group",
		Short: "Inspect and recover a Group Replication cluster's quorum",
		Long: "Commands for MySQL Group Replication clusters: inspect the live " +
			"group view (membership, roles, quorum) and, as a last resort, request " +
			"a guarded quorum recovery after a group has lost majority.\n\n" +
			"These commands only apply to clusters with " +
			"spec.replication.mode: groupReplication.",
		Example: `  # Show the group view for a cluster
  kubectl cnmsql group status cluster-gr

  # Watch the group view continuously
  kubectl cnmsql group status -w cluster-gr

  # Request guarded quorum recovery (DANGER: see --help for risks)
  kubectl cnmsql group recover cluster-gr`,
	}
	cmd.AddCommand(newGroupStatusCommand(), newGroupRecoverCommand())
	return cmd
}

func newGroupStatusCommand() *cobra.Command {
	return newWatchingCommand("status [CLUSTER]",
		"Show the live group view: members, roles and quorum",
		`Display the operator's cross-validated Group Replication group view: group
name, bootstrapped status, quorum health, the elected primary, online member
count, and a per-member table with state, role and reachability.

This command only works with Group Replication clusters
(spec.replication.mode: groupReplication).`,
		`  # Show the group view
  kubectl cnmsql group status cluster-gr

  # Watch the group view every 2 seconds
  kubectl cnmsql group status -w cluster-gr

  # Output the group view as JSON
  kubectl cnmsql group status -o json cluster-gr`,
		"group status ", runGroupStatus)
}

func runGroupStatus(ctx context.Context, clusterName, output string) error {
	env, err := newEnv()
	if err != nil {
		return err
	}
	cluster, err := env.ResolveCluster(ctx, clusterName)
	if err != nil {
		return err
	}
	if !cluster.IsGroupReplication() {
		return fmt.Errorf("cluster %q is not a Group Replication cluster", cluster.Name)
	}

	gr := cluster.Status.GroupReplication
	if output != "" {
		return plugin.PrintObject(gr, output)
	}
	if gr == nil {
		fmt.Printf("cluster %q has not reported a group view yet\n", cluster.Name)
		return nil
	}

	printGroupSummary(cluster, gr)
	printGroupMembers(gr)
	return nil
}

func printGroupSummary(c *mysqlv1alpha1.Cluster, gr *mysqlv1alpha1.GroupReplicationStatus) {
	plugin.Section("Group Replication")
	plugin.KeyVal("Cluster", c.Name)
	plugin.KeyVal("Group Name", orNone(gr.GroupName))
	plugin.KeyVal("Bootstrapped", yesNo(gr.Bootstrapped))
	quorum := yesNo(gr.HasQuorum)
	if !gr.HasQuorum {
		quorum += "  (group has lost majority — writes are blocked)"
	}
	plugin.KeyVal("Quorum", quorum)
	plugin.KeyVal("Primary", orNone(gr.PrimaryMember))
	plugin.KeyVal("Online Members", fmt.Sprintf("%d/%d", countOnline(gr), gr.ObservedViewMax))
	if gr.ViewID != "" {
		plugin.KeyVal("View ID", gr.ViewID)
	}
}

func printGroupMembers(gr *mysqlv1alpha1.GroupReplicationStatus) {
	plugin.Section("Members")
	rows := groupMemberRows(gr)
	if len(rows) == 0 {
		fmt.Println("  <no members reported>")
		return
	}
	plugin.Table([]string{"INSTANCE", "STATE", "ROLE", "REACHABLE"}, rows)
}

// groupMemberRows renders the per-member table, sorted by instance name so the
// output is stable across scrapes. Extracted for unit testing.
func groupMemberRows(gr *mysqlv1alpha1.GroupReplicationStatus) [][]string {
	if gr == nil {
		return nil
	}
	members := append([]mysqlv1alpha1.GroupMember(nil), gr.Members...)
	sort.Slice(members, func(i, j int) bool { return members[i].Instance < members[j].Instance })
	rows := make([][]string, 0, len(members))
	for _, m := range members {
		rows = append(rows, []string{m.Instance, m.State, m.Role, yesNo(m.Reachable)})
	}
	return rows
}

func newGroupRecoverCommand() *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "recover [CLUSTER]",
		Short: "Request a guarded quorum recovery for a group that lost majority",
		Long: "Request a guarded quorum recovery for a Group Replication cluster " +
			"that has lost majority.\n\n" +
			"DANGER: quorum recovery forces a new group membership with " +
			"group_replication_force_members, overriding Paxos consensus. If a " +
			"partitioned-away member is still running, this can cause split-brain " +
			"and permanent data loss. Only run this when you have confirmed the " +
			"lost members are truly down and will not come back on their own.\n\n" +
			"This command only stamps the force-quorum-recovery annotation on the " +
			"Cluster — it is a request. The operator still independently verifies " +
			"that quorum is provably lost and that a single safe survivor (the " +
			"most-advanced member, dominating every other reachable member's GTID " +
			"set) exists. If it cannot prove safety, it refuses and the cluster " +
			"stays Blocked.",
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completeClusterArg,
		Example: `  # Request guarded quorum recovery for a cluster that has lost majority
  kubectl cnmsql group recover cluster-gr

  # Skip the confirmation prompt (use with caution)
  kubectl cnmsql group recover cluster-gr --yes`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runGroupRecover(cmd.Context(), firstArg(args), yes)
		},
	}
	cmd.Flags().BoolVarP(&yes, "yes", "y", false, "skip the confirmation prompt")
	return cmd
}

func runGroupRecover(ctx context.Context, clusterName string, yes bool) error {
	env, err := newEnv()
	if err != nil {
		return err
	}
	cluster, err := env.ResolveCluster(ctx, clusterName)
	if err != nil {
		return err
	}
	if err := checkRecoverable(cluster); err != nil {
		return err
	}

	fmt.Printf("About to request guarded quorum recovery for cluster %q.\n\n", cluster.Name)
	fmt.Println("  - This overrides Paxos consensus via group_replication_force_members.")
	fmt.Println("  - If any lost member is still running elsewhere, this risks SPLIT-BRAIN")
	fmt.Println("    and permanent data loss.")
	fmt.Println("  - The operator will still refuse unless it can prove a single safe survivor.")
	fmt.Println()
	if !plugin.Confirm(fmt.Sprintf("Proceed with quorum recovery of %q?", cluster.Name), yes) {
		fmt.Println("aborted")
		return nil
	}

	before := cluster.DeepCopy()
	if cluster.Annotations == nil {
		cluster.Annotations = map[string]string{}
	}
	cluster.Annotations[plugin.ForceQuorumRecoveryAnnotation] = "yes"
	if err := env.Client.Patch(ctx, cluster, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("annotating cluster %q for quorum recovery: %w", cluster.Name, err)
	}
	fmt.Printf("requested quorum recovery for %q; watch `kubectl cnmsql group status %s` for progress\n",
		cluster.Name, cluster.Name)
	return nil
}

// checkRecoverable validates that a quorum-recovery request makes sense for the
// cluster, mirroring the operator's own gate (Group Replication, a bootstrapped
// group, and quorum provably lost). It is pure so the guard is unit-testable.
func checkRecoverable(cluster *mysqlv1alpha1.Cluster) error {
	if !cluster.IsGroupReplication() {
		return fmt.Errorf("cluster %q is not a Group Replication cluster", cluster.Name)
	}
	gr := cluster.Status.GroupReplication
	if gr == nil || !gr.Bootstrapped {
		return fmt.Errorf("cluster %q has no bootstrapped group to recover", cluster.Name)
	}
	if gr.HasQuorum {
		return fmt.Errorf("cluster %q still has quorum; recovery is unnecessary and unsafe", cluster.Name)
	}
	return nil
}

func countOnline(gr *mysqlv1alpha1.GroupReplicationStatus) int {
	n := 0
	for _, m := range gr.Members {
		if m.State == "ONLINE" {
			n++
		}
	}
	return n
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}
