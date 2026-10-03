# 036 — Separate binlog archive storage and replica clusters

Status: proposed (2026-10-03). Phase 1 accepted for implementation; phases 2
and 3 are recorded here and wait for their own go-ahead.

Issue #65.

## Problem

Two limits are listed as future work in `docs/src/pitr.md`:

1. **Binlogs share the base-backup store.** `spec.backup.objectStore` holds the
   base backups (`<path>/<cluster>/<backupID>/...`) and the continuous binlog
   archive (`<path>/<cluster>/binlogs/<server_uuid>/...` plus `_index.json`).
   The archive cannot go to its own bucket, credentials, storage class,
   retention rules or object-lock settings.
2. **No cluster can follow another one.** `spec.replica` exists in the API
   since 001 but `unsupportedReason` rejects it. A cluster can be recovered once
   from an archive (011), but it cannot keep applying the source's changes as a
   standby (CNPG's "replica cluster", fed by streaming and/or the archive).

## Phases

| Phase | Scope | Status |
|-------|-------|--------|
| 1 | Separate binlog archive object store | accepted, implement now |
| 2 | MySQL replica clusters: live channel + archive follow | proposed |
| 3 | MariaDB replica clusters | proposed, sketch only |

Each phase is its own PR (phase 2 is split in three). Phase 1 touches no replica
code and is useful on its own.

---

## Phase 1 — Separate binlog archive store

### Goal

- The binlog archive can live in an object store of its own.
- Unset, nothing changes: the archive stays in `spec.backup.objectStore`.
- PITR from a `Backup` CR and raw-S3 recovery find the binlogs in that store.
- Retention and reclaim act on each store for what it holds.

### Non-goals

- Moving an existing archive between stores. The operator never copies
  objects.
- A path-only override inside the same store. A separate `S3ObjectStore` covers
  it (same bucket, other path) and also covers other buckets and credentials.
- `spec.binlogStorage` (a local PVC for binlogs). It stays blocked; it is about
  the instance's disk, not the archive.

### Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| S1 | New `spec.backup.continuousArchiving.objectStore`, defaulting to `spec.backup.objectStore` | Lives next to the other archiving knobs; no change for existing clusters |
| S2 | New `ExternalCluster.binlogObjectStore`, defaulting to `objectStore` | Raw-S3 recovery (and later replica clusters) need the archive's location and credentials |
| S3 | New `Backup.status.binlogObjectStore`, recorded when the backup runs | A `Backup` must keep pointing at the archive it was anchored to after the cluster spec changes, like `status.objectStore` does for the base backup |
| S4 | Same key layout in the binlog store: `<path>/<cluster>/binlogs/...` | One key builder, one planner, one retention code path; a dedicated store just has nothing else beside `binlogs/` |
| S5 | No immutability rule. The operator records the effective destination in `status.continuousArchiving.destination` and warns when it changes | Changing `spec.backup.objectStore` already moves the archive today without a guard; a rule on the new field only would be inconsistent. The warning tells the user what the move costs |
| S6 | The instance Pod's `cnmsql_S3_*` env describes the binlog store | The archiver is the only consumer of object-store credentials in the instance Pod; base backups are uploaded by the worker Job |
| S7 | The restore Job gets a second env set, `cnmsql_BINLOG_S3_*`, only when the binlog store differs from the base-backup store | The restore Job reads both; absent means "same store", so the common case renders exactly what it renders today |

### API

```go
type ContinuousArchivingConfiguration struct {
    // ... existing fields ...

    // ObjectStore is where the binary-log archive is written. When unset, the
    // archive goes to spec.backup.objectStore next to the base backups. The
    // archive keeps the same layout in either store:
    // <path>/<cluster>/binlogs/.
    // +optional
    ObjectStore *S3ObjectStore `json:"objectStore,omitempty"`
}

type ExternalCluster struct {
    // ... existing fields ...

    // BinlogObjectStore is where the external cluster's binary-log archive
    // lives, when it is not in ObjectStore. Recovery reads base backups from
    // ObjectStore and binlogs from here.
    // +optional
    BinlogObjectStore *S3ObjectStore `json:"binlogObjectStore,omitempty"`
}

type BackupStatus struct {
    // ... existing fields ...

    // BinlogObjectStore records the binary-log archive store of the cluster
    // when the backup ran. Point-in-time recovery from this backup replays
    // binlogs from it. Unset on backups taken before this field existed, which
    // means the archive is in ObjectStore.
    // +optional
    BinlogObjectStore *S3ObjectStore `json:"binlogObjectStore,omitempty"`
}

type ContinuousArchivingStatus struct {
    // ... existing fields ...

    // Destination is the archive location in use, "<endpoint>/<bucket>/<path>"
    // (endpoint empty for AWS). A change is reported with an ArchiveMoved
    // Warning event.
    // +optional
    Destination string `json:"destination,omitempty"`
}
```

Helpers in `api/v1alpha1`:

- `(*Cluster).BinlogObjectStore() *S3ObjectStore` returns
  `continuousArchiving.objectStore` when set, otherwise `backup.objectStore`.
- `(*ExternalCluster).GetBinlogObjectStore() *S3ObjectStore` returns
  `binlogObjectStore` when set, otherwise `objectStore`.

### Validation

- `IsArchivingEnabled()` keeps requiring `spec.backup.objectStore`: base
  backups anchor every recovery, so an archive without a base store is not
  useful. This stays a runtime check, as today, not a CEL rule: a rule would
  reject updates to existing clusters that set `enabled: true` without a store,
  which are accepted (and do not archive) today.
- `continuousArchiving.objectStore` without `enabled: true` is accepted and
  ignored, like the other archiving fields.
- A recovery that needs binlogs (`recoveryTarget` set) from an external cluster
  needs `binlogObjectStore` or `objectStore`; the existing "has no objectStore"
  check already covers the second, so nothing new is needed.

### Resolution of the binlog store per consumer

| Consumer | Today | Phase 1 |
|----------|-------|---------|
| Archiver (instance Pod env, `runEnv`) | `backup.objectStore` | `cluster.BinlogObjectStore()` |
| `Backup` controller status | `status.objectStore` | also `status.binlogObjectStore = cluster.BinlogObjectStore()` when archiving is enabled, even when `backup.spec.objectStore` overrides the base store |
| Recovery from `Backup` CR (`resolveRecovery`) | backup store | `backup.status.binlogObjectStore`, falling back to the backup store |
| Raw-S3 recovery (`resolveRawS3Recovery`) | `ext.objectStore` | `ext.GetBinlogObjectStore()` |
| Recovery target guard (`cluster_backup_guard.go`) | `plan.Recovery.Store` | `plan.Recovery.BinlogStore` |
| PITR replay (`restore_pitr.go`) | `o.ObjectStore` / `o.Store` | new `o.BinlogObjectStore` / `o.BinlogStore`, defaulting to the base ones |
| Retention (`cluster_retention.go`) | one store | base backups listed and expired in the base store; binlogs, index read and binlog GC in the binlog store |
| Reclaim `Delete` (`cluster_backup_reclaim.go`) | `RemovePrefix` on the base store | `RemovePrefix` on both cluster prefixes, once when they are the same store and path |

A per-Backup `spec.objectStore` override moves only the base backup; the
archive is the cluster's. Today PITR from such a backup looks for binlogs in the
override, where they never were. Recording the cluster's binlog store in
`status.binlogObjectStore` fixes that as a side effect.

`recoveryPlan` gains `BinlogStore S3ObjectStore` and `BinlogStoreEnv
[]corev1.EnvVar`. When the binlog store equals the base store (same endpoint,
bucket and path, credentials ignored), `BinlogStoreEnv` is empty.

### Retention across two stores

`PlanRetention` already takes the base-backup list, the binlog list and the
index as separate inputs, so the planner does not change. `ApplyRetention` is
split in two calls (or takes two clients): expired base backups are deleted
from the base store, expired binlogs and the rewritten index go to the binlog
store. The order stays: base backups first, then binlogs, so a failure on the
binlog side never keeps an expired anchor alive and never deletes a binlog an
unexpired anchor still needs.

### Moving the archive

When `status.continuousArchiving.destination` differs from the effective
destination, the operator emits a Warning `ArchiveMoved` event naming both and
updates the status. In the new store the archiver finds no `_archive_status.json`
and ships every binlog still on the primary's disk, so the new archive starts at
the oldest local binlog. Base backups whose anchor is older than that can only
be recovered with the old store (raw-S3 recovery with `binlogObjectStore`
pointing at it). The docs recommend a new base backup right after a move.

### Credentials

- Instance Pod: `cnmsql_S3_*` and `cnmsql_S3_BUCKET`/`cnmsql_S3_PATH` render the
  binlog store. Setting `continuousArchiving.objectStore` (or changing its
  Secret references) changes the Pod template and rolls the instances, like any
  object-store change does today. Called out in the upgrade notes.
- Restore Job: the base store in `cnmsql_S3_*`, and the binlog store in
  `cnmsql_BINLOG_S3_*` when it differs. `objectstore` gains
  `ConfigFromEnvPrefix(prefix)`; the restore command reads the binlog config
  when `cnmsql_BINLOG_S3_BUCKET` is set and uses the base config otherwise.
- Operator: reads both stores' Secrets through `objectStoreConfig`, as today.

### Testing

Unit:

- `BinlogObjectStore()` / `GetBinlogObjectStore()` defaults.
- `IsArchivingEnabled()` unchanged when only `continuousArchiving.objectStore`
  is set (no base store means no archiving).
- `runEnv` renders the binlog store; unchanged output when it is unset.
- Recovery plans (Backup CR and raw S3): `BinlogStore` resolution, empty
  `BinlogStoreEnv` when the stores match, populated when they differ, and the
  fallback for a `Backup` without `status.binlogObjectStore`.
- Backup controller records `status.binlogObjectStore`, including for a
  Backup whose `spec.objectStore` overrides the base store.
- Retention with two fake stores: base backups deleted in one, binlogs and
  index rewritten in the other.
- Reclaim: two prefixes removed, one when they match.
- `ArchiveMoved` event and `destination` status on a change.
- Restore command picks the binlog env set when present.

E2E (existing SeaweedFS setup, a second bucket):

- Archive to a separate bucket, check that no `binlogs/` key lands in the base
  bucket, PITR to a `targetGTID` from the `Backup` CR with exact data
  assertions.
- Raw-S3 recovery with `objectStore` and `binlogObjectStore` on the external
  cluster entry.

### Docs

- `docs/src/pitr.md`: separate store in the diagram, components and integrator
  responsibilities; remove the "future work" line for storage (replica clusters
  stay listed until phase 2).
- `docs/src/backup-recovery.md`: configuring `continuousArchiving.objectStore`,
  raw-S3 recovery with `binlogObjectStore`, moving the archive.
- API reference regenerated.
- `INSTRUCTION.md`: decision D22.

---

## Phase 2 — MySQL replica clusters (proposed)

### Goal

A Cluster with `spec.replica.enabled` restores a cnmsql base backup of a source
and then keeps applying the source's changes, from a live replication channel to
any GTID MySQL server, from the source's binlog archive, or both. It stays
read-only until it is promoted with `spec.replica.enabled: false`.

### Scope decisions

| # | Decision |
|---|----------|
| R1 | MySQL flavor and async replication mode only. MariaDB is phase 3; Group Replication is rejected |
| R2 | Bootstrap only through `bootstrap.recovery` (`backup` or `source`) without `recoveryTarget`; `initdb` with `spec.replica` is rejected |
| R3 | The live path works with any GTID-enabled MySQL/Percona server; the archive path needs a cnmsql archive layout |
| R4 | Source account changes replicate (app users included). Each replica-cluster instance repairs its own internal accounts locally |
| R5 | Archived binlogs are applied through relay-log injection on a dedicated channel, gated by a spike; `mysqlbinlog \| mysql` replay is the fallback |
| R6 | No demotion of a promoted cluster back to replica |

### API

- `spec.replica` keeps `enabled` and `source`; `unsupportedReason` stops
  rejecting it.
- `ExternalCluster.connectionParameters` documented keys: `host`, `port`
  (default 3306), `user`. Authentication uses `password`, `sslCert`, `sslKey`,
  `sslRootCert`. Without `host` the cluster follows the archive only.
- The external entry referenced by `spec.replica.source` needs `host` or an
  object store (`binlogObjectStore` or `objectStore`).
- `status.replicaCluster`: `following` (`live` | `archive` | `none`),
  `lastAppliedGTID`, `archiveHead`, `lagSeconds`, `lastTransition`.
- Condition `ReplicaClusterFollowing` with reasons including `ArchiveGap`,
  `ArchiveDiverged`, `ApplierError`, `ObjectStoreError`.
- CEL: `enabled` cannot go from false back to true; `spec.replica` with
  `flavor: mariadb` is rejected with a message pointing at phase 3.

### Operator behaviour while `IsReplica()`

- The topology picks a designated primary the same way it picks a primary
  (`targetPrimary`/`currentPrimary`, primary Lease included). Other instances
  replicate from it on the default channel.
- Reconcilers that write SQL are skipped with a `Skipped: replica cluster`
  reason: managed roles, `Database`, `DatabaseUser`, dump account, metrics
  account, semi-sync. `LogicalRestore` is rejected.
- Physical and logical backups, and continuous archiving to the replica
  cluster's own store, stay allowed. Its binlogs carry the source's GTIDs, so
  its archive is a valid PITR archive.
- Services are unchanged; `rw` points at the read-only designated primary.
- Failover inside the replica cluster works as today; the new designated
  primary takes over the external follow.
- Major upgrades: the target series must be at least the source's series when a
  live channel reports it; documented otherwise.
- Promotion (`enabled: false`): the designated primary stops both external
  channels, runs `RESET REPLICA ALL` on them, and promotes on the normal path.
  The skipped reconcilers run afterwards. Fencing the old source is the user's
  job, as in CNPG.

### In-Pod follow engine (designated primary)

Channels:

- `cnmsql_external`: `CHANGE REPLICATION SOURCE TO ... SOURCE_AUTO_POSITION=1
  FOR CHANNEL 'cnmsql_external'`. The password goes through `START REPLICA ...
  USER=, PASSWORD=` so it is never stored in the connection metadata. TLS files
  are written by the manager to a 0600 scratch directory. The manager reads the
  external Secrets by name through the API (D17); the instance Role gains those
  `resourceNames`.
- `cnmsql_archive`: dummy source host, IO thread never started. A follower loop
  reads the source's `_index.json` from the external binlog store (credentials
  in a `cnmsql_SOURCE_S3_*` env set, rendered while `spec.replica` exists so
  promotion does not roll the Pods), plans with `binlog.PlanReplay` from the
  local `gtid_executed` to the latest point, downloads the missing files into
  the channel's relay directory as relay logs (byte cap per batch) and feeds the
  SQL thread. `relay_log_purge` reclaims applied files.

Both channels apply through the replication applier, which works under
`super_read_only=ON`, and a GTID applied on one channel is skipped on the other.

Switching policy:

- No `host`: archive only.
- With `host`: the live channel always runs. The archive channel turns on when
  the live IO thread has been in error for more than `archiveFallbackSeconds`
  (default 60), or at once on error 1236 (source purged needed binlogs). It
  turns off once live is healthy and `gtid_executed` contains the archive head.

Credential repair (every instance of a replica cluster):

- Every 30s the manager opens a fresh connection for each internal account
  (control, backup, dump, metrics). Open connections survive a password change,
  so this is how drift shows up.
- Online repair when an account fails and the control pool still has a
  connection: `SQL_LOG_BIN=0`, `CREATE USER IF NOT EXISTS` / `ALTER USER`
  (password and `REQUIRE`), with `super_read_only` lowered for those statements
  only; `read_only` stays on.
- Offline repair when control itself is locked out: stop mysqld, run the
  restore-time `--skip-grant-tables --skip-log-bin --skip-replica-start`
  credential reconcile, restart.
- Each repair emits an Event naming the account. App and metrics grants follow
  the source until promotion.

Errors (never auto-skip a transaction):

- SQL-thread error on either channel: the channel stops,
  `ReplicaClusterFollowing=False`, reason `ApplierError` with the MySQL error.
- Archive gap (source retention removed files still needed): `ArchiveGap`.
  Following continues on a healthy live channel; otherwise the documented fix is
  a re-bootstrap.
- Index fork that does not contain the local `gtid_executed`:
  `ArchiveDiverged`, following stops.
- Object-store errors: backoff and retry, `ObjectStoreError`.

Lag comes from
`performance_schema.replication_applier_status_by_worker.LAST_APPLIED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP`,
the same for both channels; the manager reports it in `/status` and the operator
aggregates it into `status.replicaCluster`.

### Delivery

- **2a, spike kept as integration tests** (Docker, 8.0, 8.4 and 9.7):
  continuous multi-batch relay-log injection on `cnmsql_archive`, duplicate GTID
  skipping across two channels, apply under `super_read_only=ON`. It settles
  whether new files can be appended while the SQL thread runs or whether each
  batch repositions it (`STOP SQL_THREAD`, `RELAY_LOG_FILE=`, `START`; the safe
  baseline). If a version fails, phase 2 switches to the replay fallback and
  this document is revised before going further.
- **2b, in-Pod engine**: channels, follower, switching policy, credential
  repair, `/status` fields. Table tests for the policy state machine, repair
  detection and online/offline selection.
- **2c, operator and API**: unblock `spec.replica`, CEL, reconciler gating,
  status and condition, Role `resourceNames`, env set, promotion.
- **E2E lane `replica-cluster`** (two clusters, one SeaweedFS): archive-only
  follow with data assertions; live follow; live outage (NetworkPolicy) falls
  back to the archive and returns to live; the source re-applies its dump
  password and the replica cluster survives through repair; failover inside the
  replica cluster keeps following; promotion makes it writable and runs the
  gated reconcilers.
- Docs: new `docs/src/replica-clusters.md`; `pitr.md` future-work line removed.
  `INSTRUCTION.md`: decision D23.

---

## Phase 3 — MariaDB replica clusters (sketch)

- Named connections (`CHANGE MASTER 'cnmsql_external' TO ...`,
  `'cnmsql_archive'`) instead of channels.
- `gtid_ignore_duplicates=ON` so the same domain can arrive on two connections;
  check its interaction with `gtid_strict_mode`.
- Archive segments are keyed by `server_id`; following the head hits the
  positional edge cases of 10.11 anchors on every file, not once at restore.
- Its own spike over 10.11, 11.4 and 12.3 before any code.
- Credential repair is the same as phase 2.

## Upgrade

Phase 1: no change for clusters that do not set the new field. Setting it rolls
the instances once (Pod env). `Backup` objects created before the upgrade have no
`status.binlogObjectStore` and recover from their base store, which is where
their archive was.
