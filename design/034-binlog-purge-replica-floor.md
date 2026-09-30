# 034 — Binlog purge gate: replica floor

Status: accepted (2026-09-30)

Issue #126. Related: #102 (a replica could not rejoin after the primary's
binlogs expired).

## Problem

With `backup.continuousArchiving.purgeAfterArchive: true` the primary's archive
loop (`pkg/management/mysql/binlog/loop.go`) runs
`PURGE BINARY LOGS TO <file before the last archived one>` after every pass. The
only condition is that the file is in the object store.

mysqld refuses to purge a file a *connected* replica is still reading, but a
replica that is disconnected, restarting, fenced or rejoining has no such
protection. Once the primary purged what it needs, it must be re-cloned. That is
why the field defaults to off and its doc comment warns against it.

## Goal

Purge file `F` only when both hold:

1. `F` is archived (unchanged).
2. Every other expected instance has applied every GTID in `F`.

`binlogExpireSeconds` keeps working on its own as the hard upper bound: a replica
that stays down past it still has to be re-cloned, as today.

## Design

### Who computes the floor

The issue sketches the operator computing a floor and pushing it to the primary
through a new endpoint or a Pod annotation. Neither is needed: the instance
manager already runs a controller-runtime manager with a cached, watched copy of
its own Cluster (the role reconciler, D17/M5.5). The Cluster status already
carries everything the floor needs:

- `status.instanceNames`: the expected instance set. The operator rewrites it
  on scale-up and scale-down.
- `status.gtidExecutedByInstance`: each instance's last observed
  `gtid_executed` (MariaDB: `gtid_current_pos`), in the engine's canonical
  form. The operator keeps only names in `instanceNames`, and a scale-down
  changes `instanceNames` in the same status patch, so removed instances leave
  the map at once.
- `status.divergedInstances` and `status.currentPrimary`.

So the primary evaluates the floor itself from the Cluster it already watches.
No new endpoint, no Pod annotation, no new RBAC, no Pod spec change (no roll).

The role reconciler hands each Cluster it reads to a small in-process holder
(`clusterFloor`, an atomic pointer set through a new `StartOptions.OnCluster`
callback). The archive loop reads the latest snapshot
through a new `binlog.ReplicaFloor` interface:

```go
// ReplicaFloor reports what the other expected instances have applied.
type ReplicaFloor interface {
	// Positions returns the applied GTID set of every instance the purge must
	// wait for, and the names of expected instances whose position is unknown.
	// ok is false when no Cluster has been observed yet.
	Positions() (positions map[string]string, unknown []string, ok bool)
}
```

Which instances count:

- every name in `status.instanceNames`, except this instance itself;
- minus `status.divergedInstances`: a diverged instance cannot catch up from
  binlogs whatever we keep (the source refuses it), it has to be re-cloned, so
  waiting for it would only stop purging for good.
- Fenced instances **do** count: fencing is temporary and the instance is
  expected to catch up when unfenced.

### Purge target

`ArchivePending` already walks every archivable file and holds its manifest
(fresh, or from the `verified` cache, or re-read from the store). It now also
returns the ordered per-file GTID sets (`ArchiveResult.Files`).

The loop keeps today's upper bound (`fileBefore(logs, lastArchived)`) and
lowers it to the first file that is not covered:

```
limit := fileBefore(logs, res.LastArchivedBinlog)
for each archived file F in order, before limit:
    if some counted instance's position does not Contain(F.GTIDSet):
        limit = F; heldBy = those instances; break
PURGE BINARY LOGS TO limit
```

`Contains` goes through the archiver's `gtidOps` (MySQL `replication.GTIDSet`;
MariaDB the engine `GTIDModel`, per-domain), so GTID strings are never compared
directly. A file with an empty GTID set (no transactions) is always covered.

### Fail closed

- No Cluster observed yet (manager just started, API server unreachable before
  the first sync): purge nothing.
- A counted instance has no entry in `gtidExecutedByInstance` (just added,
  still bootstrapping or joining, never answered): purge nothing, it is
  reported as holding the purge.
- An entry that fails to parse: purge nothing, loop error.
- Role reconciler not running (`roleManaged` false): the floor never observes a
  Cluster, purge nothing.
- No floor wired into the loop at all: purge nothing.

A stale snapshot is safe. `gtid_executed` only grows, and a newly provisioned
or re-cloned instance starts from a copy of an existing instance (primary or
replica) whose set already contains any earlier floor. So an older snapshot can
only understate what the replicas have, which purges less, never more. The
operator persists the map at least every 5 minutes (`gtidPersistInterval`), so
purging lags writes by about that much. That is the price of reusing the
throttled map instead of adding a write path, and it is what the issue asks.

This also covers scale-up ordering. The operator creates a join Job in the
same reconcile that adds the instance to `instanceNames`, but before the status
patch, so for a moment the primary does not count it. Nothing unsafe can be
purged in that window: the join streams a backup from a live instance that the
floor already counts (or from the primary), so every GTID in a purged file is
already in the source's position when the backup ends, and the new instance
restores to that position. Once the primary sees the instance, it has no
position yet and holds every file until it reports one.

One exception to "a new instance starts from a copy": scale-down keeps a
bootstrapped volume (M4 retention), and a later scale-up reuses it, bringing
back the instance's old data. Once it left `instanceNames` it stopped holding
the purge, so the primary may have purged what it is missing, and it then needs
a re-clone. That was already the case before this change (and with the gate
off once `binlogExpireSeconds` passes); the docs tell users to delete the
volume before scaling back up.

### Stuck floor: surfacing

A few recent files being held is the normal steady state (the snapshot is up to
5 minutes old), so "held" alone is not worth a condition. What matters is the
floor not moving.

The loop records, for the oldest file held by the floor, which instances hold it
and since when (reset when that file is purged or no longer held):

- `ArchivingStatus.PurgeHeldBy []string` and `PurgeHeldSince` (instance
  manager `/status`)
- `status.continuousArchiving.purgeHeldBy` / `purgeHeldSince` (Cluster)

The operator sets a new `BinlogPurgeHeld` condition to `True` when purging is on
and `purgeHeldSince` is older than 15 minutes (three snapshot intervals), with
the holding instances in the message, and emits a `Warning` event
`BinlogPurgeHeld` on the False→True transition. Otherwise the condition is
`False`. It is absent when `purgeAfterArchive` is off.

### Group Replication

Under GR a member that falls behind recovers through distributed recovery,
reading binlogs from a donor. The expected member set is still
`status.instanceNames` (the operator maintains the same list and the same GTID
map in both modes), so the same rule applies without a GR-specific path. The
archive loop only runs on the writable primary, as today. Other members' own
binlogs are not purged by us (only by `binlogExpireSeconds`).

### MariaDB

Positions and file sets are compared with the MariaDB `GTIDModel` through
`binlog.NewMariadbGTIDSet`, per domain by sequence, the same path the archiver
already uses to accumulate coverage.

## API changes

- `ContinuousArchivingStatus`: `purgeHeldBy []string`, `purgeHeldSince *metav1.Time`.
- New condition type `BinlogPurgeHeld`.
- `purgeAfterArchive` doc comment: purging now waits for every expected replica
  to have applied a file; a replica down longer than `binlogExpireSeconds` still
  needs a re-clone. The default stays `false` in this change; flipping it is a
  separate decision once this has run in the field.

## Code changes

| Area | Change |
|------|--------|
| `binlog/archiver.go` | `ArchiveResult.Files` (name + GTID set, in order) |
| `binlog/loop.go` | `ReplicaFloor` option; purge target lowered to the floor; held-by/since state |
| `instance/archiving.go` | `clusterFloor` implementing `ReplicaFloor`; wiring |
| `rolereconciler` | `StartOptions.OnCluster`, called with every Cluster read |
| `instance/runner.go` | share one floor between role reconciler and archiver |
| `webserver/status.go` | `ArchivingStatus.PurgeHeldBy/Since` |
| `api/v1alpha1` | status fields, doc comment; `make manifests generate` |
| `internal/controller/cluster_status.go` | copy fields, `BinlogPurgeHeld` condition + event |
| `docs/src/pitr.md`, `operator-upgrades.md` | describe the gate and the condition; 0.9.0 upgrade note |

## Tests

- `binlog/purge_test.go`: purge bounded by frontier (unchanged behaviour when all
  replicas are caught up); bounded by the first uncovered file; nothing purged
  when a position is unknown, when no Cluster was observed, or with no floor
  provider; empty-GTID files are covered; held-by/since set, kept for the same
  file and reset once it is purged; MariaDB multi-domain sets.
- `instance/archiving_test.go`: self, diverged and removed instances excluded;
  missing entries reported unknown; nothing allowed before a Cluster is seen.
- operator: condition True only past the threshold, False otherwise, absent with
  the gate off; event on transition.
- e2e (`binlog_purge_gate_test.go`), for async MySQL, async MariaDB and Group
  Replication, on one three-instance cluster in order:
  - with every replica caught up, archived files are purged;
  - a fenced instance holds the files written since the fence (`purgeHeldBy`
    names it, the files stay for a full minute), catches up from the primary
    once unfenced with no broken replication, and the purge then moves on;
  - (async) a fenced instance removed by a scale-down stops holding at once; a
    fresh instance added back joins and the purge moves on;
  - (async) after a switchover the new primary purges on the same rules;
  - PITR from a base backup taken before any purge, to a GTID inside files the
    first primary purged and to one after the switchover, checked by exact row
    count and id sum, with later rows written past the target.
  `BinlogPurgeHeld` needs 15 minutes, too slow for e2e; it is unit-tested.
