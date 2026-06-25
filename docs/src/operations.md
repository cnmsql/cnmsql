---
title: "Operations Runbooks"
description: "Common cnmsql operational tasks with the kubectl cnmsql plugin: status, scaling, switchovers, failover, restart, backup, user and database management."
sidebar_position: 8
---

# Operations runbooks

cnmsql ships a kubectl plugin, `kubectl-cnmsql`, that wraps common day-two
operations. Install it once:

```bash
curl -sSfL https://github.com/cnmsql/cnmsql/raw/main/hack/install-cnmsql-plugin.sh | sh -s -- -b ~/.local/bin
```

The script downloads the latest release, verifies its checksum, and installs the
plugin along with a `kubectl_complete-cnmsql` shim for shell tab completion.

Most commands accept an optional `CLUSTER` argument. When you omit it, the
plugin picks the only cluster in the current namespace and warns if there are
several.

Commands in this guide use `cluster-sample` as the Cluster name.

## Inspect cluster state

```bash
kubectl cnmsql status
kubectl cnmsql status cluster-sample
```

Add `-w` or `--watch` to refresh every 2s, like watch(1):

```bash
kubectl cnmsql status -w
kubectl cnmsql status -w --watch-interval=5s
```

The status command shows instance topology, phase, conditions, and health. For
raw Kubernetes output, `kubectl describe cluster` and `kubectl get events` still
work and give you more detail when you need it.

Key status fields on the Cluster resource:

- `status.readyInstances`
- `status.currentPrimary`
- `status.targetPrimary`
- `status.gtidExecutedByInstance`
- `status.divergedInstances`
- `status.replicationBrokenInstances`
- `status.continuousArchiving`
- `status.phase` and `status.phaseReason`

## Stream logs

```bash
kubectl cnmsql logs cluster-sample          # all instances, merged with a prefix
kubectl cnmsql logs cluster-sample cluster-sample-2  # single instance
```

## Scale up

```bash
kubectl patch cluster cluster-sample --type merge -p '{"spec":{"instances":4}}'
kubectl wait --for=condition=Ready cluster/cluster-sample --timeout=15m
```

Scale-up is ordered. cnmsql creates one replica at a time and waits for it to
be healthy before creating the next one.

## Scale down

```bash
kubectl patch cluster cluster-sample --type merge -p '{"spec":{"instances":1}}'
```

Scale-down removes highest-ordinal replicas first. cnmsql deletes replica Pods
but retains PVCs. It never scales below one instance and does not remove the
current primary during normal scale-down.

List retained PVCs:

```bash
kubectl get pvc -l mysql.cnmsql.co/cluster=cluster-sample
```

Delete retained PVCs only after confirming the data is no longer needed.

## Planned switchover

cnmsql follows the CNPG-style status transition model. A planned switchover
promotes a named healthy replica. Use the plugin:

```bash
kubectl cnmsql promote cluster-sample cluster-sample-2
```

Watch progress:

```bash
kubectl cnmsql status -w
```

The operator validates the target, waits for GTID containment, bounds the
operation by `spec.maxSwitchoverDelay`, and lets the selected instance promote
itself. Role Services move after the database role is safe.

## Fence an instance

Fencing takes an instance out of service without deleting it or its data. The
Pod stays and the PVC stays, but the instance drops out of all routing Services
and mysqld is stopped:

```bash
kubectl cnmsql fence on cluster-sample cluster-sample-2
```

Unfence it to restart mysqld and restore normal routing and role
reconciliation:

```bash
kubectl cnmsql fence off cluster-sample cluster-sample-2
```

The in-Pod manager stops mysqld while
staying alive as PID 1, so the Pod keeps answering its control and liveness
endpoints. The liveness probe does not depend on mysqld being up, so a fenced
instance is not restarted by the kubelet. Because mysqld is down, the Pod
reports NotReady and shows as `0/1 Running`, which is expected. The data
directory is untouched, so you can mount the PVC elsewhere or restart the
instance by unfencing.

The operator tracks fenced instances in `status.fencedInstances`. A fenced
instance is skipped as a failover candidate. Fencing the primary stops writes
for the cluster because the rw Service has no endpoint. That is deliberate: use
fencing to freeze an instance for inspection or maintenance, not as a failover
trigger.

## Automatic failover

Automatic failover is driven by primary health, Pod readiness, and GTID safety.
`spec.failoverDelay` controls how long cnmsql waits after detecting the
primary as failed. `0` means immediate failover.

```yaml
spec:
  failoverDelay: 30
```

During failover cnmsql:

1. chooses a ready replica with healthy replication SQL state;
2. excludes any replica already known to be diverged (see below);
3. checks that candidate GTID sets are comparable;
4. fences the old primary Pod while retaining its PVC;
5. sets `targetPrimary` to the safe candidate;
6. updates role labels and Services after promotion.

If GTID sets are divergent or no safe candidate exists, failover is blocked
instead of risking data loss.

Known-diverged replicas are excluded explicitly, before the GTID comparison.
This matters because a diverged replica's GTID set is a *superset*: it carries
errant transactions the others never saw. The candidate selection would otherwise
pick it as the most up to date, and promoting it makes those errant transactions
canonical. The primary is unreachable during a failover, so divergence cannot be
computed live; instead the operator relies on `status.divergedInstances` recorded
earlier, while the primary was still reachable. If every surviving candidate is
known-diverged, failover blocks with "every replica candidate has diverged ...
manual recovery required" rather than promoting one. Re-initialise a survivor
(see [Re-initialise an instance](#re-initialise-an-instance-from-scratch)) to
recover.

## Former primary rejoin

A former primary that returns after failover starts read-only and follows the
current primary if its GTID set is compatible.

If it contains errant transactions, cnmsql marks it diverged and keeps it out
of service. Do not delete the retained PVC until you have decided whether manual
recovery is required.

Check:

```bash
kubectl cnmsql status cluster-sample
```

Look for entries under `divergedInstances`. To bring a diverged instance back
into service, re-initialise it (see
[Re-initialise an instance](#re-initialise-an-instance-from-scratch)), which
discards its data and re-clones from a backup.

A replica whose replication has aborted at the SQL layer (a stopped IO or SQL
thread with a recorded error, such as a duplicate-key conflict) is reported under
`replicationBrokenInstances` and marks the cluster `Degraded`, even while the Pod
is still Running. This catches a replica that is up but silently no longer
replicating, which would otherwise sit unnoticed. Re-initialise it when the break
is not transient.

## Restart an instance

Restart all instances in a rolling fashion, or a single instance:

```bash
kubectl cnmsql restart cluster-sample          # rolling restart
kubectl cnmsql restart cluster-sample cluster-sample-2  # single instance
```

The command prompts for confirmation. Skip the prompt with `--yes` or `-y`.

Every instance boots read only. The in-pod role reconciler observes Cluster
status and only clears read-only mode when the instance is the confirmed
primary.

## Destroy an instance

Delete a single instance Pod and its PVC:

```bash
kubectl cnmsql destroy cluster-sample cluster-sample-3
```

This command also prompts for confirmation. Use it to clean up a failed or
diverged instance you have decided to discard. The remaining instances keep
running unaffected.

## Re-initialise an instance from scratch

When a replica is diverged (errant GTIDs) or its replication is irrecoverably
broken, you can re-initialise it instead of destroying it. MySQL has no
`pg_rewind` to surgically realign a divergent replica, so the remediation is the
same as CloudNativePG's destroy-and-rebootstrap fallback: discard the local data
and re-clone a fresh copy from a backup.

```bash
kubectl cnmsql reinit cluster-sample cluster-sample-2
```

The operator deletes the instance's Pod and PVC and recreates them empty, so the
bootstrap re-clones from a backup and rejoins replication. The instance keeps its
name and ordinal, so it keeps its `server_id`; only its data is discarded.

This is destructive and irreversible. Any data that exists only on that instance,
such as errant transactions, is lost. It prompts for confirmation; skip the
prompt with `--yes`/`-y`.

The current primary cannot be re-initialised this way. It is the replication
source, so the command refuses it. To replace a primary, switch over first, then
re-initialise the former primary as a replica.

Under the hood the command appends the instance to the Cluster's `cnmsql.cnmsql.co/reinit` annotation, a comma-separated list the operator consumes. Re-initialisation is always human-triggered; the operator never re-clones an instance over its retained PVC on its own. The operator clears the entry once the teardown completes and the instance has been recreated.

## Reload MySQL parameters

After you change `spec.mysql.parameters`, apply dynamic parameters without
restarting:

```bash
kubectl cnmsql reload cluster-sample
```

This connects to each instance over mTLS and issues the equivalent of reloading
the running configuration. Parameters that require a restart are noted and need a
follow-up rolling restart.

Update parameters:

```bash
kubectl patch cluster cluster-sample --type merge -p \
  '{"spec":{"mysql":{"parameters":{"require_secure_transport":"ON"}}}}'
```

cnmsql owns replication, backup, PITR, identity, and lifecycle-critical
settings. User parameters that conflict with managed keys are rejected by the
configuration layer.

## Take an on-demand backup

Instead of crafting a Backup YAML by hand, use the plugin:

```bash
kubectl cnmsql backup cluster-sample
```

This creates a `Backup` object with sensible defaults: `xtrabackup` method,
`prefer-standby` target, online mode. The Backup reconciler then runs the actual
XtraBackup job. Track it:

```bash
kubectl cnmsql status cluster-sample
kubectl get backup -l mysql.cnmsql.co/cluster=cluster-sample
```

For recurring backups, create a `ScheduledBackup` resource. See the [Scheduled
Backups](./scheduled-backups.md) page for the schedule format and options.

Deleting the `Backup` Kubernetes object does not delete the remote object-store
artifacts today. Remote cleanup is a planned finalizer/retention feature.

## User management

cnmsql manages MySQL users through the control-tier API, reached over mTLS
port-forwarding inside the cluster:

```bash
kubectl cnmsql user create cluster-sample --name=app --password-stdin < secret.txt
kubectl cnmsql user alter cluster-sample --name=app        # prompt for new password
kubectl cnmsql user list cluster-sample
kubectl cnmsql user drop cluster-sample --name=old-user
```

Passwords are never accepted as flags. Use `--password-stdin` for piping from a
secret, or let the plugin prompt on the terminal with echo disabled.

Users can be created with optional grants (`--superuser`), TLS requirements
(`--require-x509`), and named privileges.

## Database management

Manage MySQL databases the same way:

```bash
kubectl cnmsql database create cluster-sample --name=analytics
kubectl cnmsql database list cluster-sample
kubectl cnmsql database drop cluster-sample --name=analytics
```

You can specify character set and collation on create:

```bash
kubectl cnmsql database create cluster-sample --name=utf8db --charset=utf8mb4 --collation=utf8mb4_unicode_ci
```

## Node maintenance window

Toggle the maintenance window before draining a node or performing Kubernetes
node maintenance:

```bash
kubectl cnmsql maintenance set cluster-sample
kubectl cnmsql maintenance unset cluster-sample
```

Use `--reuse-pvc` to retain the existing PVC across node restarts. This is
useful when the underlying storage is durable and you want to avoid a full clone.

## Scrape Prometheus metrics

```bash
kubectl cnmsql metrics cluster-sample              # primary
kubectl cnmsql metrics cluster-sample cluster-sample-2  # specific instance
kubectl cnmsql metrics -w --filter=mysql_global_status_threads  # watch mode, filtered
```

Add `-w` for continuous refresh. Use `--filter` with a pattern to narrow the
output to matching metric names (grep-style substring match).

## Continuous archiving operations

When continuous archiving is enabled, inspect:

```bash
kubectl cnmsql status cluster-sample
```

Look for `continuousArchiving` in the output. Growing pending files or a
degraded condition usually means an object-store, credential, network, or
throughput issue.

## Safe maintenance habits

- Prefer planned switchover before node or primary maintenance.
- Keep at least three instances for meaningful automatic failover.
- Use semi-sync when acknowledged-write durability matters.
- Keep object-store lifecycle rules aligned with backup and PITR retention.
- Treat retained PVCs and remote backups as recovery assets.

## Group Replication operations

These commands apply only to clusters with `spec.replication.mode: groupReplication`.
They refuse to run against async clusters.

### Inspect the group view

```bash
kubectl cnmsql group status cluster-gr
kubectl cnmsql group status cluster-gr -w
```

Shows the group name, whether it is bootstrapped, quorum status, the current
primary, online member count, view ID, and a per-member table with state, role,
and reachability.

### Recover from quorum loss

When a Group Replication cluster loses quorum (fewer than a majority of members
are ONLINE), writes are blocked and the cluster reports `Phase=Blocked`.
Recovery is a deliberate, confirmed human action through the plugin:

```bash
kubectl cnmsql group recover cluster-gr
```

Before acting, read the full command help and the [Group Replication](./group-replication.md#quorum-loss-and-recovery)
page. Quorum recovery overrides Paxos consensus with
`group_replication_force_members` and can cause split-brain and permanent data
loss if a lost member is still running elsewhere. The command prints a
consequence summary and requires confirmation. The operator independently
verifies quorum is lost and a safe survivor exists before acting; it refuses if
safety is unprovable.

### Fencing under Group Replication

Fencing works the same way (`kubectl cnmsql fence on/off`) but acts differently
under the hood:

- Instead of stopping mysqld, the fenced member runs `STOP GROUP_REPLICATION`,
  gracefully leaving the group. mysqld stays up and reachable for inspection.
- Fencing the primary triggers a group re-election. Fencing a secondary shrinks
  the group, and the operator refuses if it would drop the group below quorum.

### Switchover under Group Replication

Switchover works the same way (`kubectl cnmsql promote <cluster> <target>`) but
uses `group_replication_set_as_primary` on the group instead of the async
stop/promote/demote dance. The operator validates that the target is an ONLINE
SECONDARY, invokes the UDF, and observes the result. Bounded by
`spec.maxSwitchoverDelay` as with async.
