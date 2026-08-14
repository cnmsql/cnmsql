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
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/cmd/kubectl-cnmsql/plugin"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

const (
	readyYes = "yes"
	readyNo  = "no"

	none        = "<none>"
	unreachable = "<unreachable>"
	phaseReady  = "Ready"

	defaultStatusTimeout = 10 * time.Second
)

// statusVerbose and statusTimeout carry the -v/--verbose and --timeout flag
// values for the status command. They are package-level so runStatus (the
// newWatchingCommand runFn) can read them without widening its signature.
var (
	statusVerbose int
	statusTimeout time.Duration
)

// statusDialer opens a control connection to an instance for the T2 /status
// fetch. It defaults to env.DialControl and is overridable in tests to inject a
// fake control client without opening real port-forwards.
var statusDialer plugin.ControlDialer

func newStatusCommand() *cobra.Command {
	cmd := newWatchingCommand("status [CLUSTER]",
		"Show the status of a cluster and its instances",
		`Display a human-readable summary of a cnmsql cluster: the current
phase, primary, ready instance count, conditions, and a per-instance table with
role, readiness, and flags.

CLUSTER defaults to the sole cluster in the current namespace.`,
		`  # Show the status of the default cluster in the current namespace
  kubectl cnmsql status

  # Show the status of a specific cluster
  kubectl cnmsql status cluster-sample

  # Watch status refresh every 2 seconds
  kubectl cnmsql status -w

  # Output the full enriched status as YAML
  kubectl cnmsql status -o yaml

  # Verbose instance detail (container restarts, storage)
  kubectl cnmsql status -v`,
		"status ", runStatus)
	cmd.Flags().CountVarP(&statusVerbose, "verbose", "v", "increase instance detail (repeat for more)")
	cmd.Flags().DurationVar(&statusTimeout, "timeout", defaultStatusTimeout,
		"per-instance control API dial timeout")
	return cmd
}

// instanceStatus pairs an instance name with the live status fetched from its
// instance manager (T2). Error is non-nil when the instance was unreachable, in
// which case Status is nil.
type instanceStatus struct {
	Instance string
	Status   *webserver.Status
	Error    error
}

// clusterStatusReport is the enriched snapshot emitted by `status -o json|yaml`:
// the Cluster CR plus the per-instance live statuses (and any dial errors).
type clusterStatusReport struct {
	Cluster   *mysqlv1alpha1.Cluster         `json:"cluster"`
	Instances []instanceStatus               `json:"instances"`
	Services  []corev1.Service               `json:"services,omitempty"`
	PDBs      []policyv1.PodDisruptionBudget `json:"pdbs,omitempty"`
}

func runStatus(ctx context.Context, clusterName, output string) error {
	env, err := newEnv()
	if err != nil {
		return err
	}
	cluster, err := env.ResolveCluster(ctx, clusterName)
	if err != nil {
		return err
	}

	pods, err := env.ListPods(ctx, cluster)
	if err != nil {
		return err
	}

	live := fetchInstanceStatuses(ctx, cluster, pods)

	if output != "" {
		services, pdbs := listServicesAndPDBs(ctx, env, cluster)
		return plugin.PrintObject(&clusterStatusReport{
			Cluster:   cluster,
			Instances: live,
			Services:  services,
			PDBs:      pdbs,
		}, output)
	}

	printSummary(cluster)
	printConditions(cluster)
	printInstances(cluster, pods, live)
	printContinuousArchiving(cluster, live)
	printBackups(ctx, env, cluster)
	printManagedRoles(cluster)
	printCertificates(cluster)
	printServicesAndPDBs(ctx, env, cluster)
	return nil
}

// fetchInstanceStatuses dials each instance concurrently and collects its T2
// /status payload. An unreachable instance yields a degraded entry carrying its
// error; the function never fails because of a per-instance dial failure,
// mirroring how --watch tolerates per-frame errors.
func fetchInstanceStatuses(ctx context.Context, cluster *mysqlv1alpha1.Cluster, pods []corev1.Pod) []instanceStatus {
	results := make([]instanceStatus, len(pods))
	var wg sync.WaitGroup
	for i := range pods {
		wg.Add(1)
		go func(idx int, pod corev1.Pod) {
			defer wg.Done()
			results[idx] = fetchOneInstance(ctx, cluster, pod.Name)
		}(i, pods[i])
	}
	wg.Wait()
	sort.Slice(results, func(i, j int) bool { return results[i].Instance < results[j].Instance })
	return results
}

func fetchOneInstance(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instance string) instanceStatus {
	dial := statusDialer
	if dial == nil {
		dial = func(ctx context.Context, c *mysqlv1alpha1.Cluster, name string) (*plugin.ControlClient, error) {
			return envDialControl(ctx, c, name)
		}
	}
	cc, err := dial(ctx, cluster, instance)
	if err != nil {
		return instanceStatus{Instance: instance, Error: err}
	}
	defer cc.Close()
	st := &webserver.Status{}
	if err := cc.Get(ctx, "/status", st); err != nil {
		return instanceStatus{Instance: instance, Error: err}
	}
	return instanceStatus{Instance: instance, Status: st}
}

// envDialControl resolves the environment and dials an instance's control API.
// It is split out so the default dialer closure can be constructed lazily.
func envDialControl(
	ctx context.Context, cluster *mysqlv1alpha1.Cluster, instance string,
) (*plugin.ControlClient, error) {
	env, err := newEnv()
	if err != nil {
		return nil, err
	}
	dialCtx := ctx
	if statusTimeout > 0 {
		var cancel context.CancelFunc
		dialCtx, cancel = context.WithTimeout(ctx, statusTimeout)
		defer cancel()
	}
	return env.DialControl(dialCtx, cluster, instance)
}

func printSummary(c *mysqlv1alpha1.Cluster) {
	plugin.Section("Cluster Summary")
	plugin.KeyVal("Name", c.Name)
	plugin.KeyVal("Namespace", c.Namespace)
	plugin.KeyVal("Flavor", string(c.ResolvedFlavor()))
	plugin.KeyVal("Phase", plugin.Badge(c.Status.Phase,
		isHealthyPhase(c.Status.Phase), isFailedPhase(c.Status.Phase)).String())
	if c.Status.PhaseReason != "" {
		plugin.KeyVal("Phase Reason", c.Status.PhaseReason)
	}
	ready := c.Status.ReadyInstances
	total := c.Status.Instances
	plugin.KeyValColor("Instances", plugin.Badge(
		fmt.Sprintf("%d/%d ready", ready, total), ready == total && total > 0, ready == 0 && total > 0))
	plugin.KeyVal("Primary", orNone(c.Status.CurrentPrimary))
	if c.Status.TargetPrimary != "" && c.Status.TargetPrimary != c.Status.CurrentPrimary {
		plugin.KeyVal("Target Primary", c.Status.TargetPrimary)
	}
	plugin.KeyVal("Image", orNone(c.Status.Image))
	if len(c.Status.FencedInstances) > 0 {
		plugin.KeyVal("Fenced", fmt.Sprintf("%v", c.Status.FencedInstances))
	}
	if len(c.Status.DivergedInstances) > 0 {
		plugin.KeyVal("Diverged", fmt.Sprintf("%v", c.Status.DivergedInstances))
	}
}

func printConditions(c *mysqlv1alpha1.Cluster) {
	if len(c.Status.Conditions) == 0 {
		return
	}
	plugin.Section("Conditions")
	rows := make([][]string, 0, len(c.Status.Conditions))
	for _, cond := range c.Status.Conditions {
		rows = append(rows, []string{cond.Type, string(cond.Status), cond.Reason, cond.Message})
	}
	plugin.Table([]string{"TYPE", "STATUS", "REASON", "MESSAGE"}, rows)
}

func printInstances(c *mysqlv1alpha1.Cluster, pods []corev1.Pod, live []instanceStatus) {
	plugin.Section("Instances")
	sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })
	primary := plugin.PrimaryInstance(c)
	liveByInstance := make(map[string]*webserver.Status, len(live))
	for i := range live {
		liveByInstance[live[i].Instance] = live[i].Status
	}
	header := []string{"NAME", "ROLE", "READY", "PHASE", "NODE", "GTID", "LAG", "UPTIME"}
	if statusVerbose > 0 {
		header = append(header, "STORAGE", "RESTARTS")
	}
	header = append(header, "FLAGS")
	rows := make([][]string, 0, len(pods))
	for i := range pods {
		pod := &pods[i]
		role := "replica"
		if pod.Name == primary {
			role = "primary"
		}
		ready := readyNo
		if plugin.PodReady(pod) {
			ready = readyYes
		}
		gtid := orNone(c.Status.GTIDExecutedByInstance[pod.Name])
		lag := unreachable
		uptime := unreachable
		storage := unreachable
		if st := liveByInstance[pod.Name]; st != nil {
			lag = lagString(c, pod.Name, st)
			uptime = uptimeString(st.UptimeSeconds)
			if statusVerbose > 0 {
				storage = storageString(st.Storage)
			}
			if gtid == none && st.GTIDExecuted != "" {
				gtid = st.GTIDExecuted
			}
		}
		gtid = truncateGTID(gtid)
		restarts := ""
		if statusVerbose > 0 {
			restarts = fmt.Sprintf("%d", containerRestarts(pod))
		}
		flags := instanceFlags(c, pod.Name)
		row := []string{pod.Name, role, ready, string(pod.Status.Phase), pod.Spec.NodeName, gtid, lag, uptime}
		if statusVerbose > 0 {
			row = append(row, storage, restarts)
		}
		row = append(row, flags)
		rows = append(rows, row)
	}
	plugin.Table(header, rows)
}

func lagString(c *mysqlv1alpha1.Cluster, instance string, st *webserver.Status) string {
	if lag, ok := c.Status.ReplicationLagByInstance[instance]; ok {
		return fmt.Sprintf("%ds", lag)
	}
	if st.Replication != nil && st.Replication.SecondsBehindSource != nil {
		return fmt.Sprintf("%ds", *st.Replication.SecondsBehindSource)
	}
	return "0s"
}

func uptimeString(seconds int64) string {
	if seconds <= 0 {
		return "0s"
	}
	d := time.Duration(seconds) * time.Second
	days := int(d.Hours()) / 24
	if days > 0 {
		return fmt.Sprintf("%dd%dh", days, int(d.Hours())%24)
	}
	if d.Hours() >= 1 {
		return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
	}
	return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
}

func storageString(st *webserver.StorageStatus) string {
	if st == nil || st.CapacityBytes == 0 {
		return "<none>"
	}
	pct := float64(st.UsedBytes) / float64(st.CapacityBytes) * 100
	return fmt.Sprintf("%.1f%%", pct)
}

func containerRestarts(pod *corev1.Pod) int {
	var restarts int32
	for i := range pod.Status.ContainerStatuses {
		restarts += pod.Status.ContainerStatuses[i].RestartCount
	}
	return int(restarts)
}

func instanceFlags(c *mysqlv1alpha1.Cluster, name string) string {
	var flags []string
	if plugin.Contains(c.Status.FencedInstances, name) {
		flags = append(flags, "fenced")
	}
	if plugin.Contains(c.Status.DivergedInstances, name) {
		flags = append(flags, "diverged")
	}
	return strings.Join(flags, ",")
}

// truncateGTID collapses a multi-UUID GTID set to a single line and caps its
// length so the Instances table stays one row per instance. With verbose mode
// off it shows the first set and an ellipsis when more exist; the full value
// is available via `status -o json`.
func truncateGTID(gtid string) string {
	if gtid == none || gtid == "" {
		return none
	}
	oneLine := strings.ReplaceAll(gtid, "\n", "")
	if statusVerbose > 0 {
		return oneLine
	}
	const max = 20
	if len(oneLine) <= max {
		return oneLine
	}
	if i := strings.Index(oneLine, ","); i >= 0 && i < max {
		return oneLine[:i] + ",…"
	}
	return oneLine[:max-1] + "…"
}

func printContinuousArchiving(c *mysqlv1alpha1.Cluster, live []instanceStatus) {
	ca := c.Status.ContinuousArchiving
	if ca == nil && !hasArchiving(live) {
		return
	}
	plugin.Section("Continuous Archiving")
	if ca != nil {
		plugin.KeyVal("Enabled", boolStr(ca.Enabled))
		plugin.KeyVal("Last Binlog", orNone(ca.LastArchivedBinlog))
		plugin.KeyVal("Last GTID", orNone(ca.LastArchivedGTID))
		if ca.LastArchivedTime != nil {
			plugin.KeyVal("Last Archived", ca.LastArchivedTime.Format(time.RFC3339))
		}
		pending := plugin.Badge(fmt.Sprintf("%d", ca.PendingFiles),
			ca.PendingFiles == 0, false)
		plugin.KeyValColor("Pending Files", pending)
		if ca.LastFailureReason != "" {
			plugin.KeyValColor("Last Failure", plugin.Red(ca.LastFailureReason))
		}
		if ca.LastFailureTime != nil {
			plugin.KeyVal("Last Failure Time", ca.LastFailureTime.Format(time.RFC3339))
		}
	}
}

func hasArchiving(live []instanceStatus) bool {
	for i := range live {
		if live[i].Status != nil && live[i].Status.Archiving != nil && live[i].Status.Archiving.Active {
			return true
		}
	}
	return false
}

func printBackups(ctx context.Context, env *plugin.Env, cluster *mysqlv1alpha1.Cluster) {
	list := &mysqlv1alpha1.BackupList{}
	if err := env.Client.List(ctx, list, client.InNamespace(cluster.Namespace)); err != nil {
		return
	}
	var rows [][]string
	for i := range list.Items {
		b := &list.Items[i]
		if b.Spec.Cluster.Name != cluster.Name {
			continue
		}
		completed := none
		if b.Status.StoppedAt != nil {
			completed = b.Status.StoppedAt.Format(time.RFC3339)
		}
		rows = append(rows, []string{
			b.Name,
			string(b.Status.Phase),
			orNone(b.Status.BackupID),
			completed,
		})
	}
	if cluster.Status.LastRetentionRunTime != nil {
		plugin.Section("Backups")
		plugin.KeyVal("Last Retention Run", cluster.Status.LastRetentionRunTime.Format(time.RFC3339))
	}
	if len(rows) == 0 {
		return
	}
	if cluster.Status.LastRetentionRunTime == nil {
		plugin.Section("Backups")
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i][3] > rows[j][3] })
	plugin.Table([]string{"NAME", "PHASE", "BACKUP ID", "COMPLETED"}, rows)
}

func printManagedRoles(c *mysqlv1alpha1.Cluster) {
	mrs := c.Status.ManagedRolesStatus
	if mrs == nil {
		return
	}
	if len(mrs.ByStatus) == 0 && len(mrs.CannotReconcile) == 0 {
		return
	}
	plugin.Section("Managed Roles")
	for status, names := range mrs.ByStatus {
		sort.Strings(names)
		plugin.KeyVal(string(status), strings.Join(names, ", "))
	}
	if len(mrs.CannotReconcile) > 0 {
		for reason, names := range mrs.CannotReconcile {
			sort.Strings(names)
			plugin.KeyValColor("Cannot Reconcile ("+reason+")", plugin.Yellow(strings.Join(names, ", ")))
		}
	}
}

func printCertificates(c *mysqlv1alpha1.Cluster) {
	certs := c.Status.Certificates
	if certs == nil || len(certs.Expirations) == 0 {
		return
	}
	plugin.Section("Certificates")
	type certRow struct {
		name string
		exp  string
	}
	rows := make([]certRow, 0, len(certs.Expirations))
	for name, exp := range certs.Expirations {
		rows = append(rows, certRow{name: name, exp: exp})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].exp < rows[j].exp })
	for _, r := range rows {
		plugin.KeyValColor(r.name, certExpiryColor(r.exp))
	}
}

func certExpiryColor(exp string) any {
	t, err := time.Parse(time.RFC3339, exp)
	if err != nil {
		return exp
	}
	days := time.Until(t).Hours() / 24
	switch {
	case days < 0:
		return plugin.Red(exp + " (expired)")
	case days < 7:
		return plugin.Red(exp)
	case days < 30:
		return plugin.Yellow(exp)
	default:
		return plugin.Green(exp)
	}
}

func printServicesAndPDBs(ctx context.Context, env *plugin.Env, cluster *mysqlv1alpha1.Cluster) {
	services, pdbs := listServicesAndPDBs(ctx, env, cluster)
	if len(services) > 0 {
		plugin.Section("Services")
		sort.Slice(services, func(i, j int) bool { return services[i].Name < services[j].Name })
		rows := make([][]string, 0, len(services))
		for i := range services {
			svc := &services[i]
			role := svc.Labels[plugin.RoleLabel]
			rows = append(rows, []string{svc.Name, role, string(svc.Spec.Type), primaryIP(svc)})
		}
		plugin.Table([]string{"NAME", "ROLE", "TYPE", "CLUSTER IP"}, rows)
	}
	if len(pdbs) > 0 {
		plugin.Section("Pod Disruption Budgets")
		sort.Slice(pdbs, func(i, j int) bool { return pdbs[i].Name < pdbs[j].Name })
		rows := make([][]string, 0, len(pdbs))
		for i := range pdbs {
			pdb := &pdbs[i]
			budget := pdbBudget(pdb)
			rows = append(rows, []string{pdb.Name, budget})
		}
		plugin.Table([]string{"NAME", "BUDGET"}, rows)
	}
}

func listServicesAndPDBs(
	ctx context.Context, env *plugin.Env, cluster *mysqlv1alpha1.Cluster,
) ([]corev1.Service, []policyv1.PodDisruptionBudget) {
	svcList, err := env.Clientset.CoreV1().
		Services(cluster.Namespace).List(ctx, plugin.MetaListByCluster(cluster.Name))
	if err != nil {
		return nil, nil
	}
	pdbList, err := env.Clientset.PolicyV1().
		PodDisruptionBudgets(cluster.Namespace).List(ctx, plugin.MetaListByCluster(cluster.Name))
	if err != nil {
		return svcList.Items, nil
	}
	return svcList.Items, pdbList.Items
}

func primaryIP(svc *corev1.Service) string {
	if len(svc.Spec.ClusterIPs) > 0 && svc.Spec.ClusterIPs[0] != "" {
		return svc.Spec.ClusterIPs[0]
	}
	return orNone(svc.Spec.ClusterIP)
}

func pdbBudget(pdb *policyv1.PodDisruptionBudget) string {
	if pdb.Spec.MaxUnavailable != nil {
		return "maxUnavailable=" + pdb.Spec.MaxUnavailable.String()
	}
	if pdb.Spec.MinAvailable != nil {
		return "minAvailable=" + pdb.Spec.MinAvailable.String()
	}
	return none
}

func boolStr(b bool) string {
	if b {
		return readyYes
	}
	return readyNo
}

func orNone(s string) string {
	if s == "" {
		return none
	}
	return s
}

// isHealthyPhase reports whether a cluster phase string indicates a healthy
// state (Ready). The operator writes these phase strings from
// internal/controller/topology; the CLI compares them as plain strings to avoid
// importing the controller internals.
func isHealthyPhase(phase string) bool {
	return phase == phaseReady
}

// isFailedPhase reports whether a cluster phase string indicates a critical
// state (Blocked or FullOutage).
func isFailedPhase(phase string) bool {
	return phase == "Blocked" || phase == "FullOutage"
}
