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

	"github.com/logrusorgru/aurora/v4"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/cmd/kubectl-cnmsql/plugin"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

const (
	readyYes = "yes"
	readyNo  = "no"

	phaseReady = "Ready"

	defaultStatusTimeout = 10 * time.Second

	// recentBackups is how many backups status lists without -v.
	recentBackups = 5
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
		`Display a summary of a cnmsql cluster: its health, primary, continuous
backup and archiving state, replication streams, and a per-instance table.

Live figures (replication threads, lag, uptime, storage) come from each
instance manager; an instance that cannot be reached is shown as such rather
than failing the command.

CLUSTER defaults to the sole cluster in the current namespace.`,
		`  # Show the status of the default cluster in the current namespace
  kubectl cnmsql status

  # Show the status of a specific cluster
  kubectl cnmsql status cluster-sample

  # Watch status refresh every 2 seconds
  kubectl cnmsql status -w

  # Output the full enriched status as YAML
  kubectl cnmsql status -o yaml

  # Verbose: all conditions and backups, full GTID sets, services and PDBs
  kubectl cnmsql status -v`,
		"status ", runStatus)
	cmd.Flags().CountVarP(&statusVerbose, "verbose", "v", "increase detail (repeat for more)")
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

// statusView is everything the human-readable status renders, gathered once.
type statusView struct {
	cluster   *mysqlv1alpha1.Cluster
	primary   string
	pods      []corev1.Pod
	live      map[string]*instanceStatus
	backups   []mysqlv1alpha1.Backup
	scheduled []mysqlv1alpha1.ScheduledBackup
	restores  []mysqlv1alpha1.LogicalRestore
}

func (v *statusView) liveStatus(instance string) *webserver.Status {
	if l := v.live[instance]; l != nil {
		return l.Status
	}
	return nil
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

	live := fetchInstanceStatuses(ctx, env, cluster, pods)

	if output != "" {
		services, pdbs := listServicesAndPDBs(ctx, env, cluster)
		return plugin.PrintObject(&clusterStatusReport{
			Cluster:   cluster,
			Instances: live,
			Services:  services,
			PDBs:      pdbs,
		}, output)
	}

	v := &statusView{
		cluster: cluster,
		primary: plugin.PrimaryInstance(cluster),
		pods:    pods,
		live:    make(map[string]*instanceStatus, len(live)),
	}
	for i := range live {
		v.live[live[i].Instance] = &live[i]
	}
	v.backups, v.scheduled = listBackups(ctx, env, cluster)
	v.restores = listLogicalRestores(ctx, env, cluster)
	sortInstances(v.pods, v.primary)

	printSummary(v)
	printConditions(cluster)
	printContinuousBackup(v)
	if cluster.IsGroupReplication() {
		printGroupReplication(cluster)
	} else {
		printStreamingReplication(v)
	}
	printInstances(v)
	printBackups(v)
	printLogicalRestores(v)
	printManagedRoles(cluster)
	printCertificates(cluster)
	if statusVerbose > 0 {
		printServicesAndPDBs(ctx, env, cluster)
	}
	return nil
}

// fetchInstanceStatuses dials each instance concurrently and collects its T2
// /status payload. An unreachable instance yields a degraded entry carrying its
// error; the function never fails because of a per-instance dial failure,
// mirroring how --watch tolerates per-frame errors.
func fetchInstanceStatuses(
	ctx context.Context, env *plugin.Env, cluster *mysqlv1alpha1.Cluster, pods []corev1.Pod,
) []instanceStatus {
	results := make([]instanceStatus, len(pods))
	var wg sync.WaitGroup
	for i := range pods {
		wg.Add(1)
		go func(idx int, pod corev1.Pod) {
			defer wg.Done()
			results[idx] = fetchOneInstance(ctx, env, cluster, pod.Name)
		}(i, pods[i])
	}
	wg.Wait()
	sort.Slice(results, func(i, j int) bool { return results[i].Instance < results[j].Instance })
	return results
}

func fetchOneInstance(
	ctx context.Context, env *plugin.Env, cluster *mysqlv1alpha1.Cluster, instance string,
) instanceStatus {
	dial := statusDialer
	if dial == nil {
		dial = env.DialControl
	}
	if statusTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, statusTimeout)
		defer cancel()
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

// sortInstances orders pods primary first, then by name, like kubectl cnpg.
func sortInstances(pods []corev1.Pod, primary string) {
	sort.SliceStable(pods, func(i, j int) bool {
		if (pods[i].Name == primary) != (pods[j].Name == primary) {
			return pods[i].Name == primary
		}
		return pods[i].Name < pods[j].Name
	})
}

func printSummary(v *statusView) {
	c := v.cluster
	plugin.Section("Cluster Summary")
	f := plugin.Fields{}
	f.Add("Name", c.Namespace+"/"+c.Name)
	f.Add("Server", serverDescription(c, v.liveStatus(v.primary)))
	f.Add("Image", c.Status.Image)
	f.Add("Replication", replicationDescription(c))

	f.Add("Primary instance", plugin.Or(c.Status.CurrentPrimary))
	if c.Status.CurrentPrimaryTimestamp != nil {
		f.Add("Primary promotion time", plugin.Timestamp(c.Status.CurrentPrimaryTimestamp.Time))
	}
	if c.Status.TargetPrimary != "" && c.Status.TargetPrimary != c.Status.CurrentPrimary {
		switchover := fmt.Sprintf("%s → %s", plugin.Or(c.Status.CurrentPrimary), c.Status.TargetPrimary)
		if c.Status.TargetPrimaryTimestamp != nil {
			switchover += fmt.Sprintf(" (requested %s ago)",
				plugin.HumanDuration(time.Since(c.Status.TargetPrimaryTimestamp.Time)))
		}
		f.Add("Switchover in progress", plugin.Yellow(switchover))
	}

	status := phaseBadge(c.Status.Phase).String()
	if c.Status.PhaseReason != "" {
		status += " " + plugin.Faint("("+c.Status.PhaseReason+")").String()
	}
	f.Add("Status", status)
	ready, total := c.Status.ReadyInstances, c.Status.Instances
	f.Add("Instances", total)
	f.Add("Ready instances", plugin.Badge(fmt.Sprintf("%d/%d", ready, total),
		ready == total && total > 0, ready == 0 && total > 0))
	if st := v.liveStatus(v.primary); st != nil && st.Storage != nil {
		f.Add("Size", storageSummary(st.Storage))
	}
	if gtid := primaryGTID(v); gtid != "" {
		f.Add("Current GTID executed", shortGTID(gtid))
	}
	if c.Status.LastFailoverTimestamp != nil {
		f.Add("Last failover", plugin.Timestamp(c.Status.LastFailoverTimestamp.Time))
	}

	for _, l := range []struct {
		label string
		names []string
		bad   bool
	}{
		{"Fenced instances", c.Status.FencedInstances, false},
		{"Diverged instances", c.Status.DivergedInstances, true},
		{"Failed instances", c.Status.FailedInstances, true},
		{"Replication broken", c.Status.ReplicationBrokenInstances, true},
		{"Resizing PVCs", c.Status.ResizingPVC, false},
	} {
		if len(l.names) > 0 {
			f.Add(l.label, plugin.Badge(strings.Join(l.names, ", "), false, l.bad))
		}
	}
	f.Print()
}

// serverDescription names the engine and, when an instance reported it, the
// live server version: "MySQL 8.4.3".
func serverDescription(c *mysqlv1alpha1.Cluster, primary *webserver.Status) string {
	name := "MySQL"
	if c.ResolvedFlavor() == mysqlv1alpha1.FlavorMariaDB {
		name = "MariaDB"
	}
	if primary != nil && primary.Version != "" {
		return name + " " + primary.Version
	}
	return name
}

func replicationDescription(c *mysqlv1alpha1.Cluster) string {
	switch {
	case c.IsGroupReplication():
		return "Group Replication"
	case c.IsSemiSyncEnabled():
		return "asynchronous, semi-synchronous acknowledgement"
	default:
		return "asynchronous"
	}
}

func phaseBadge(phase string) aurora.Value {
	return plugin.Badge(plugin.Or(phase), phase == phaseReady, phase == "Blocked" || phase == "FullOutage")
}

func primaryGTID(v *statusView) string {
	if st := v.liveStatus(v.primary); st != nil && st.GTIDExecuted != "" {
		return st.GTIDExecuted
	}
	return v.cluster.Status.GTIDExecutedByInstance[v.primary]
}

// storageSummary renders a data volume's usage, colored as it fills up.
func storageSummary(st *webserver.StorageStatus) string {
	if st == nil || st.CapacityBytes == 0 {
		return plugin.NoValue
	}
	pct := float64(st.UsedBytes) / float64(st.CapacityBytes) * 100
	label := fmt.Sprintf("%s of %s (%.0f%%)", plugin.HumanBytes(st.UsedBytes), plugin.HumanBytes(st.CapacityBytes), pct)
	return plugin.Badge(label, pct < 80, pct >= 90).String()
}

// storageShort is storageSummary for a table cell: "45% of 10.0 GiB".
func storageShort(st *webserver.StorageStatus) string {
	if st == nil || st.CapacityBytes == 0 {
		return plugin.NoValue
	}
	pct := float64(st.UsedBytes) / float64(st.CapacityBytes) * 100
	label := fmt.Sprintf("%.0f%% of %s", pct, plugin.HumanBytes(st.CapacityBytes))
	return plugin.Badge(label, pct < 80, pct >= 90).String()
}

// shortGTID collapses a GTID set to one line. Without -v a set spanning several
// server UUIDs keeps only the first and counts the rest; the full value is in
// `status -o json`.
func shortGTID(gtid string) string {
	oneLine := strings.Join(strings.Fields(gtid), "")
	if oneLine == "" {
		return plugin.NoValue
	}
	if statusVerbose > 0 {
		return oneLine
	}
	parts := strings.Split(oneLine, ",")
	if len(parts) == 1 {
		return oneLine
	}
	return fmt.Sprintf("%s %s", parts[0], plugin.Faint(fmt.Sprintf("(+%d more)", len(parts)-1)))
}

// conditionHealthy reports whether a condition is in the state a healthy
// cluster shows. Ready and ContinuousArchiving are healthy when true; the
// others (Progressing, Degraded, StoragePressure) when false.
func conditionHealthy(cond metav1.Condition) bool {
	switch cond.Type {
	case "Ready", "ContinuousArchiving":
		return cond.Status == "True"
	default:
		return cond.Status != "True"
	}
}

// printConditions lists the conditions that need attention, or all of them
// with -v. A healthy cluster prints nothing here.
func printConditions(c *mysqlv1alpha1.Cluster) {
	var rows [][]string
	for _, cond := range c.Status.Conditions {
		healthy := conditionHealthy(cond)
		if healthy && statusVerbose == 0 {
			continue
		}
		status := plugin.Badge(string(cond.Status), healthy, cond.Type != "Progressing").String()
		since := plugin.HumanDuration(time.Since(cond.LastTransitionTime.Time))
		rows = append(rows, []string{cond.Type, status, cond.Reason, since, cond.Message})
	}
	if len(rows) == 0 {
		return
	}
	plugin.Section("Conditions")
	plugin.Table([]string{"Type", "Status", "Reason", "Since", "Message"}, rows)
}

func printContinuousBackup(v *statusView) {
	c := v.cluster
	ca := c.Status.ContinuousArchiving
	if c.Spec.Backup == nil && ca == nil && len(v.backups) == 0 {
		return
	}
	plugin.Section("Continuous Backup status")
	f := plugin.Fields{}
	if c.Spec.Backup != nil && c.Spec.Backup.ObjectStore != nil {
		f.Add("Object store", objectStoreURL(c.Spec.Backup.ObjectStore))
	}

	firstRecoverable, lastSuccess, lastFailed := summarizeBackups(v.backups)
	if firstRecoverable != nil {
		f.Add("First point of recoverability", plugin.Timestamp(firstRecoverable.Status.StoppedAt.Time))
	} else {
		f.Add("First point of recoverability", plugin.Yellow("none (no completed backup)"))
	}
	if lastSuccess != nil {
		f.Add("Last successful backup", fmt.Sprintf("%s  %s",
			plugin.Timestamp(lastSuccess.Status.StoppedAt.Time), plugin.Faint(lastSuccess.Name)))
	} else {
		f.Add("Last successful backup", plugin.NoValue)
	}
	// A failure only matters until a later backup succeeds.
	if lastFailed != nil && (lastSuccess == nil || lastSuccess.Status.StoppedAt.Before(ptrTime(backupTime(lastFailed)))) {
		f.Add("Last failed backup", plugin.Red(fmt.Sprintf("%s  %s: %s",
			backupTime(lastFailed).Local().Format(time.DateTime), lastFailed.Name, plugin.Or(lastFailed.Status.Error))))
	} else {
		f.Add("Last failed backup", plugin.NoValue)
	}
	if next := nextScheduledBackup(v.scheduled); next != nil {
		f.Add("Next scheduled backup", plugin.Timestamp(*next))
	}
	if c.Status.LastRetentionRunTime != nil {
		f.Add("Last retention run", plugin.Timestamp(c.Status.LastRetentionRunTime.Time))
	}

	if ca != nil && ca.Enabled {
		f.Add("Working binlog archiving", archivingHealth(ca))
		f.Add("Binlogs waiting to be archived", plugin.Badge(fmt.Sprint(ca.PendingFiles), ca.PendingFiles == 0, false))
		last := plugin.Or(ca.LastArchivedBinlog)
		if ca.LastArchivedTime != nil {
			last += "  @  " + plugin.Timestamp(ca.LastArchivedTime.Time)
		}
		f.Add("Last archived binlog", last)
		if ca.LastArchivedGTID != "" {
			f.Add("Last archived GTID", shortGTID(ca.LastArchivedGTID))
		}
		if ca.LastFailureReason != "" {
			failure := ca.LastFailureReason
			if ca.LastFailureTime != nil {
				failure += "  @  " + ca.LastFailureTime.Local().Format(time.DateTime)
			}
			if archivingFailing(ca) {
				f.Add("Last failed archiving", plugin.Red(failure))
			} else {
				f.Add("Last failed archiving", failure)
			}
		}
	} else {
		f.Add("Working binlog archiving", plugin.Faint("disabled"))
	}
	f.Print()
}

// summarizeBackups picks the oldest completed physical backup (the first point
// of recoverability), the newest completed backup of any method, and the newest
// failed one. A logical backup is never a recovery base, so it does not move
// the first point of recoverability.
func summarizeBackups(
	backups []mysqlv1alpha1.Backup,
) (firstRecoverable, lastSuccess, lastFailed *mysqlv1alpha1.Backup) {
	for i := range backups {
		b := &backups[i]
		switch b.Status.Phase {
		case mysqlv1alpha1.BackupPhaseCompleted:
			if b.Status.StoppedAt == nil {
				continue
			}
			if b.Status.Method != mysqlv1alpha1.BackupMethodLogical &&
				(firstRecoverable == nil || b.Status.StoppedAt.Before(firstRecoverable.Status.StoppedAt)) {
				firstRecoverable = b
			}
			if lastSuccess == nil || lastSuccess.Status.StoppedAt.Before(b.Status.StoppedAt) {
				lastSuccess = b
			}
		case mysqlv1alpha1.BackupPhaseFailed:
			if lastFailed == nil || backupTime(lastFailed).Before(backupTime(b)) {
				lastFailed = b
			}
		}
	}
	return firstRecoverable, lastSuccess, lastFailed
}

// archivingFailing reports whether archiving is currently broken: its most
// recent failure is newer than its most recent success.
func archivingFailing(ca *mysqlv1alpha1.ContinuousArchivingStatus) bool {
	return ca.LastFailureTime != nil &&
		(ca.LastArchivedTime == nil || ca.LastArchivedTime.Before(ca.LastFailureTime))
}

func archivingHealth(ca *mysqlv1alpha1.ContinuousArchivingStatus) aurora.Value {
	if archivingFailing(ca) {
		return plugin.Red("Failing")
	}
	return plugin.Green("OK")
}

func objectStoreURL(store *mysqlv1alpha1.S3ObjectStore) string {
	url := "s3://" + store.Bucket
	if p := strings.Trim(store.Path, "/"); p != "" {
		url += "/" + p
	}
	if store.Endpoint != "" {
		url += " " + plugin.Faint("("+store.Endpoint+")").String()
	}
	return url
}

func backupTime(b *mysqlv1alpha1.Backup) time.Time {
	switch {
	case b.Status.StoppedAt != nil:
		return b.Status.StoppedAt.Time
	case b.Status.StartedAt != nil:
		return b.Status.StartedAt.Time
	default:
		return b.CreationTimestamp.Time
	}
}

func ptrTime(t time.Time) *metav1.Time {
	return &metav1.Time{Time: t}
}

func nextScheduledBackup(scheduled []mysqlv1alpha1.ScheduledBackup) *time.Time {
	var next *time.Time
	for i := range scheduled {
		s := &scheduled[i]
		if s.Spec.Suspend != nil && *s.Spec.Suspend {
			continue
		}
		if t := s.Status.NextScheduleTime; t != nil && (next == nil || t.Time.Before(*next)) {
			next = &t.Time
		}
	}
	return next
}

// printStreamingReplication shows each replica's stream: the state of its IO
// and SQL threads, its lag and whether it acknowledges semi-synchronously.
func printStreamingReplication(v *statusView) {
	var rows [][]string
	for i := range v.pods {
		name := v.pods[i].Name
		if name == v.primary {
			continue
		}
		st := v.liveStatus(name)
		if st == nil {
			rows = append(rows, []string{name, plugin.NoValue, plugin.NoValue, plugin.NoValue,
				lagCell(v.cluster, name, nil), plugin.NoValue, plugin.Red("instance unreachable").String()})
			continue
		}
		source, ioThread, sqlThread, lastError := plugin.NoValue, plugin.NoValue, plugin.NoValue, ""
		if r := st.Replication; r != nil {
			source = plugin.Or(r.SourceHost)
			ioThread = threadCell(r.IORunning)
			sqlThread = threadCell(r.SQLRunning)
			lastError = r.LastError
		}
		mode := "async"
		if st.SemiSync.ReplicaEnabled {
			mode = "semi-sync"
		}
		row := []string{name, source, ioThread, sqlThread, lagCell(v.cluster, name, st), mode,
			plugin.Red(lastError).String()}
		if lastError == "" {
			row[6] = plugin.NoValue
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return
	}
	plugin.Section("Streaming Replication status")
	plugin.Table([]string{"Name", "Source", "IO Thread", "SQL Thread", "Lag", "Mode", "Last Error"}, rows)
}

func threadCell(running bool) string {
	if running {
		return plugin.Green("Running").String()
	}
	return plugin.Red("Stopped").String()
}

// lagCell renders a replica's lag, preferring the live heartbeat reading, then
// the operator's last persisted one, then Seconds_Behind_Source. An instance
// with no reading shows "-": no reading is not the same as no lag.
func lagCell(c *mysqlv1alpha1.Cluster, instance string, st *webserver.Status) string {
	var lag time.Duration
	switch {
	case st != nil && st.ReplicationLag != nil && st.ReplicationLag.LagMillis != nil:
		lag = time.Duration(*st.ReplicationLag.LagMillis) * time.Millisecond
	case hasKey(c.Status.ReplicationLagByInstance, instance):
		lag = time.Duration(c.Status.ReplicationLagByInstance[instance]) * time.Millisecond
	case st != nil && st.Replication != nil && st.Replication.SecondsBehindSource != nil:
		lag = time.Duration(*st.Replication.SecondsBehindSource) * time.Second
	default:
		return plugin.NoValue
	}
	label := plugin.HumanDuration(lag)
	if limit := c.MaxReplicationLag(); limit != nil && lag > *limit {
		return plugin.Red(label).String()
	}
	switch {
	case lag >= time.Minute:
		return plugin.Red(label).String()
	case lag >= time.Second:
		return plugin.Yellow(label).String()
	default:
		return label
	}
}

func hasKey(m map[string]int64, k string) bool {
	_, ok := m[k]
	return ok
}

func printGroupReplication(c *mysqlv1alpha1.Cluster) {
	gr := c.Status.GroupReplication
	plugin.Section("Group Replication status")
	if gr == nil {
		_, _ = fmt.Fprintln(plugin.Out, plugin.Yellow("The operator has not reported a group view yet"))
		return
	}
	f := plugin.Fields{}
	f.Add("Group name", gr.GroupName)
	f.Add("Quorum", quorumCell(gr))
	f.Add("Online members", fmt.Sprintf("%d/%d", countOnline(gr), max(gr.ObservedViewMax, len(gr.Members))))
	if gr.CommunicationProtocol != "" {
		protocol := gr.CommunicationProtocol
		if gr.CommunicationProtocolTarget != "" && gr.CommunicationProtocolTarget != protocol {
			protocol += plugin.Yellow(" → " + gr.CommunicationProtocolTarget).String()
		}
		f.Add("Communication protocol", protocol)
	}
	f.Print()
	if rows := groupMemberRows(gr); len(rows) > 0 {
		_, _ = fmt.Fprintln(plugin.Out)
		plugin.Table([]string{"Member", "State", "Role", "Reachable"}, rows)
	}
}

func quorumCell(gr *mysqlv1alpha1.GroupReplicationStatus) aurora.Value {
	switch {
	case !gr.Bootstrapped:
		return plugin.Yellow("not bootstrapped")
	case gr.HasQuorum:
		return plugin.Green("yes")
	default:
		return plugin.Red("LOST (writes are blocked)")
	}
}

func printInstances(v *statusView) {
	c := v.cluster
	header := []string{"Name", "GTID Executed", "Role", "Status", "Uptime", "Storage", "QoS", "Manager", "Node"}
	if statusVerbose > 0 {
		header = append(header, "Version", "Restarts")
	}
	rows := make([][]string, 0, len(v.pods))
	for i := range v.pods {
		pod := &v.pods[i]
		st := v.liveStatus(pod.Name)
		gtid := c.Status.GTIDExecutedByInstance[pod.Name]
		uptime, storage, version := plugin.NoValue, plugin.NoValue, plugin.NoValue
		if st != nil {
			if st.GTIDExecuted != "" {
				gtid = st.GTIDExecuted
			}
			if st.UptimeSeconds > 0 {
				uptime = plugin.HumanDuration(time.Duration(st.UptimeSeconds) * time.Second)
			}
			storage = storageShort(st.Storage)
			version = plugin.Or(st.Version)
		}
		row := []string{
			pod.Name,
			shortGTID(gtid),
			instanceRole(c, pod.Name, v.primary, st),
			instanceHealth(c, pod, v.live[pod.Name]),
			uptime,
			storage,
			plugin.Or(string(pod.Status.QOSClass)),
			managerCell(c, pod.Name),
			plugin.Or(pod.Spec.NodeName),
		}
		if statusVerbose > 0 {
			row = append(row, version, fmt.Sprint(containerRestarts(pod)))
		}
		rows = append(rows, row)
	}
	plugin.Section("Instances status")
	if len(rows) == 0 {
		_, _ = fmt.Fprintln(plugin.Out, plugin.Yellow("No instance Pods found"))
		return
	}
	plugin.Table(header, rows)
}

func instanceRole(c *mysqlv1alpha1.Cluster, name, primary string, st *webserver.Status) string {
	if c.IsGroupReplication() {
		for _, m := range memberList(c) {
			if m.Instance == name && m.Role != "" {
				return titleCase(m.Role)
			}
		}
	}
	if name == primary {
		return plugin.Bold("Primary").String()
	}
	switch {
	case st == nil:
		return "Replica"
	case st.SemiSync.ReplicaEnabled:
		return "Replica (semi-sync)"
	default:
		return "Replica (async)"
	}
}

func memberList(c *mysqlv1alpha1.Cluster) []mysqlv1alpha1.GroupMember {
	if c.Status.GroupReplication == nil {
		return nil
	}
	return c.Status.GroupReplication.Members
}

func titleCase(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + strings.ToLower(s[1:])
}

// instanceHealth condenses an instance's state into one colored word, most
// severe first: what the operator flagged, then reachability, then readiness.
func instanceHealth(c *mysqlv1alpha1.Cluster, pod *corev1.Pod, live *instanceStatus) string {
	name := pod.Name
	switch {
	case plugin.Contains(c.Status.DivergedInstances, name):
		return plugin.Red("Diverged").String()
	case plugin.Contains(c.Status.FailedInstances, name):
		return plugin.Red("Failed").String()
	case plugin.Contains(c.Status.ReplicationBrokenInstances, name):
		return plugin.Red("Replication broken").String()
	case plugin.Contains(c.Status.FencedInstances, name):
		return plugin.Yellow("Fenced").String()
	case pod.DeletionTimestamp != nil:
		return plugin.Yellow("Terminating").String()
	case live == nil || live.Status == nil:
		return plugin.Red("Unreachable").String()
	case live.Status.InPlaceUpgrading:
		return plugin.Yellow("Upgrading").String()
	case !plugin.PodReady(pod) || !live.Status.IsReady:
		return plugin.Yellow("Not ready").String()
	default:
		return plugin.Green("OK").String()
	}
}

// managerCell compares the instance manager an instance runs with the
// operator's: an outdated one is awaiting its in-place or rolling upgrade.
func managerCell(c *mysqlv1alpha1.Cluster, instance string) string {
	hash := c.Status.ExecutableHashByInstance[instance]
	switch {
	case hash == "" || c.Status.OperatorExecutableHash == "":
		return plugin.NoValue
	case hash == c.Status.OperatorExecutableHash:
		return "up to date"
	default:
		return plugin.Yellow("outdated").String()
	}
}

func containerRestarts(pod *corev1.Pod) int {
	var restarts int32
	for i := range pod.Status.ContainerStatuses {
		restarts += pod.Status.ContainerStatuses[i].RestartCount
	}
	return int(restarts)
}

// printBackups lists the most recent backups, all of them with -v.
func printBackups(v *statusView) {
	if len(v.backups) == 0 {
		return
	}
	backups := append([]mysqlv1alpha1.Backup(nil), v.backups...)
	sort.Slice(backups, func(i, j int) bool { return backupTime(&backups[j]).Before(backupTime(&backups[i])) })
	title := "Backups"
	if statusVerbose == 0 && len(backups) > recentBackups {
		title = fmt.Sprintf("Backups (latest %d of %d, -v for all)", recentBackups, len(backups))
		backups = backups[:recentBackups]
	}
	rows := make([][]string, 0, len(backups))
	for i := range backups {
		b := &backups[i]
		started, duration := plugin.NoValue, plugin.NoValue
		if b.Status.StartedAt != nil {
			started = b.Status.StartedAt.Local().Format(time.DateTime)
			end := time.Now()
			if b.Status.StoppedAt != nil {
				end = b.Status.StoppedAt.Time
			}
			duration = plugin.HumanDuration(end.Sub(b.Status.StartedAt.Time))
		}
		rows = append(rows, []string{
			b.Name, backupPhase(b.Status.Phase), plugin.Or(string(b.Status.Method)),
			plugin.Or(b.Status.InstanceName), started, duration,
		})
	}
	plugin.Section(title)
	plugin.Table([]string{"Name", "Phase", "Method", "Instance", "Started", "Duration"}, rows)
}

func backupPhase(phase mysqlv1alpha1.BackupPhase) string {
	switch phase {
	case mysqlv1alpha1.BackupPhaseCompleted:
		return plugin.Green(phase).String()
	case mysqlv1alpha1.BackupPhaseFailed:
		return plugin.Red(phase).String()
	case "":
		return plugin.Yellow("new").String()
	default:
		return plugin.Yellow(phase).String()
	}
}

func listBackups(
	ctx context.Context, env *plugin.Env, cluster *mysqlv1alpha1.Cluster,
) ([]mysqlv1alpha1.Backup, []mysqlv1alpha1.ScheduledBackup) {
	var backups []mysqlv1alpha1.Backup
	list := &mysqlv1alpha1.BackupList{}
	if err := env.Client.List(ctx, list, client.InNamespace(cluster.Namespace)); err == nil {
		for i := range list.Items {
			if list.Items[i].Spec.Cluster.Name == cluster.Name {
				backups = append(backups, list.Items[i])
			}
		}
	}
	var scheduled []mysqlv1alpha1.ScheduledBackup
	slist := &mysqlv1alpha1.ScheduledBackupList{}
	if err := env.Client.List(ctx, slist, client.InNamespace(cluster.Namespace)); err == nil {
		for i := range slist.Items {
			if slist.Items[i].Spec.Cluster.Name == cluster.Name {
				scheduled = append(scheduled, slist.Items[i])
			}
		}
	}
	return backups, scheduled
}

// printLogicalRestores lists the cluster's LogicalRestores, newest first.
// Whether a failed one changed the data is in its status error, which
// `kubectl describe logicalrestore` shows.
func printLogicalRestores(v *statusView) {
	if len(v.restores) == 0 {
		return
	}
	restores := append([]mysqlv1alpha1.LogicalRestore(nil), v.restores...)
	sort.Slice(restores, func(i, j int) bool {
		return restores[j].CreationTimestamp.Before(&restores[i].CreationTimestamp)
	})
	title := "Logical restores"
	if statusVerbose == 0 && len(restores) > recentBackups {
		title = fmt.Sprintf("Logical restores (latest %d of %d, -v for all)", recentBackups, len(restores))
		restores = restores[:recentBackups]
	}
	rows := make([][]string, 0, len(restores))
	for i := range restores {
		r := &restores[i]
		started, duration := plugin.NoValue, plugin.NoValue
		if r.Status.StartedAt != nil {
			started = r.Status.StartedAt.Local().Format(time.DateTime)
			end := time.Now()
			if r.Status.StoppedAt != nil {
				end = r.Status.StoppedAt.Time
			}
			duration = plugin.HumanDuration(end.Sub(r.Status.StartedAt.Time))
		}
		rows = append(rows, []string{
			r.Name, backupPhase(mysqlv1alpha1.BackupPhase(r.Status.Phase)), string(r.Spec.Policy),
			strings.Join(r.Spec.Databases, ","), plugin.Or(r.Status.TargetInstance), started, duration,
		})
	}
	plugin.Section(title)
	plugin.Table([]string{"Name", "Phase", "Policy", "Databases", "Target", "Started", "Duration"}, rows)
}

func listLogicalRestores(
	ctx context.Context, env *plugin.Env, cluster *mysqlv1alpha1.Cluster,
) []mysqlv1alpha1.LogicalRestore {
	list := &mysqlv1alpha1.LogicalRestoreList{}
	// An operator without the LogicalRestore CRD answers with an error; the
	// section is then simply left out.
	if err := env.Client.List(ctx, list, client.InNamespace(cluster.Namespace)); err != nil {
		return nil
	}
	var restores []mysqlv1alpha1.LogicalRestore
	for i := range list.Items {
		if list.Items[i].Spec.Cluster.Name == cluster.Name {
			restores = append(restores, list.Items[i])
		}
	}
	return restores
}

func printManagedRoles(c *mysqlv1alpha1.Cluster) {
	mrs := c.Status.ManagedRolesStatus
	if mrs == nil || (len(mrs.ByStatus) == 0 && len(mrs.CannotReconcile) == 0) {
		return
	}
	plugin.Section("Managed Roles")
	f := plugin.Fields{}
	statuses := make([]string, 0, len(mrs.ByStatus))
	for status := range mrs.ByStatus {
		statuses = append(statuses, string(status))
	}
	sort.Strings(statuses)
	for _, status := range statuses {
		names := append([]string(nil), mrs.ByStatus[mysqlv1alpha1.ManagedRoleStatus(status)]...)
		sort.Strings(names)
		f.Add(status, strings.Join(names, ", "))
	}
	roles := make([]string, 0, len(mrs.CannotReconcile))
	for role := range mrs.CannotReconcile {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	for _, role := range roles {
		f.Add("Cannot reconcile "+role, plugin.Red(strings.Join(mrs.CannotReconcile[role], "; ")))
	}
	f.Print()
}

func printCertificates(c *mysqlv1alpha1.Cluster) {
	certs := c.Status.Certificates
	if certs == nil || len(certs.Expirations) == 0 {
		return
	}
	names := make([]string, 0, len(certs.Expirations))
	for name := range certs.Expirations {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		return certs.Expirations[names[i]] < certs.Expirations[names[j]]
	})
	rows := make([][]string, 0, len(names))
	for _, name := range names {
		exp := certs.Expirations[name]
		t, err := time.Parse(time.RFC3339, exp)
		if err != nil {
			rows = append(rows, []string{name, exp, plugin.NoValue})
			continue
		}
		left := time.Until(t)
		remaining := plugin.HumanDuration(left)
		if left < 0 {
			remaining = "expired " + remaining + " ago"
		}
		days := left.Hours() / 24
		rows = append(rows, []string{
			name, t.Local().Format(time.DateTime),
			plugin.Badge(remaining, days >= 30, days < 7).String(),
		})
	}
	plugin.Section("Certificates")
	plugin.Table([]string{"Name", "Expires", "Remaining"}, rows)
}

func printServicesAndPDBs(ctx context.Context, env *plugin.Env, cluster *mysqlv1alpha1.Cluster) {
	services, pdbs := listServicesAndPDBs(ctx, env, cluster)
	if len(services) > 0 {
		plugin.Section("Services")
		sort.Slice(services, func(i, j int) bool { return services[i].Name < services[j].Name })
		rows := make([][]string, 0, len(services))
		for i := range services {
			svc := &services[i]
			rows = append(rows, []string{svc.Name, plugin.Or(svc.Labels[plugin.RoleLabel]),
				string(svc.Spec.Type), primaryIP(svc)})
		}
		plugin.Table([]string{"Name", "Role", "Type", "Cluster IP"}, rows)
	}
	if len(pdbs) > 0 {
		plugin.Section("Pod Disruption Budgets")
		sort.Slice(pdbs, func(i, j int) bool { return pdbs[i].Name < pdbs[j].Name })
		rows := make([][]string, 0, len(pdbs))
		for i := range pdbs {
			pdb := &pdbs[i]
			allowed := fmt.Sprint(pdb.Status.DisruptionsAllowed)
			rows = append(rows, []string{pdb.Name, pdbBudget(pdb),
				plugin.Badge(allowed, pdb.Status.DisruptionsAllowed > 0, false).String()})
		}
		plugin.Table([]string{"Name", "Budget", "Disruptions Allowed"}, rows)
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
	return plugin.Or(svc.Spec.ClusterIP)
}

func pdbBudget(pdb *policyv1.PodDisruptionBudget) string {
	if pdb.Spec.MaxUnavailable != nil {
		return "maxUnavailable=" + pdb.Spec.MaxUnavailable.String()
	}
	if pdb.Spec.MinAvailable != nil {
		return "minAvailable=" + pdb.Spec.MinAvailable.String()
	}
	return plugin.NoValue
}
