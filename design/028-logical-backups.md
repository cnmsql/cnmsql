# 028 — Logical Backups

- **Status:** accepted
- **Milestone:** M-LB
- **Issue:** [#47](https://github.com/cnmsql/cnmsql/issues/47)
- **Supersedes:** none

Add a logical backup path next to the XtraBackup one: a `Backup` with
`method: logical` produces a SQL dump in the same object store, and a new cluster
can be bootstrapped by importing it (`bootstrap.initdb.import`). A later phase
adds a one-shot restore into a running cluster for partial restores.

## 1. Why

cnmsql only takes physical backups (XtraBackup/MariaBackup, design 008). A
physical backup is the right tool for disaster recovery and PITR, but it cannot:

- **Restore part of a cluster.** An xbstream archive restores a whole data
  directory. You cannot pull one schema out of it without restoring the whole
  thing somewhere else first.
- **Cross server series.** A physical backup restores only onto the same server
  series it was taken on. Moving data `8.0 → 9.x` without walking the in-place
  upgrade chain (design 024), downgrading, or moving between flavors
  (design 026 says cross-flavor migration "is a dump/restore exercise") needs a
  logical format.
- **Export a schema** for a developer, a test environment, or another tool.

A SQL dump does all three. The issue asks for logical backups "stored in the
object store alongside physical backups, with a matching restore path".

## 2. Scope

### In scope

- `Backup.spec.method: logical` (and on `ScheduledBackup`), producing one
  consistent, compressed SQL dump of the application schemas.
- Engine-native dump tooling: `mysqldump` (Percona) and `mariadb-dump`
  (MariaDB), behind a new `pkg/engine` facet.
- **Shipping those tools in the instance images**, which are built in
  [cnmsql/containers](https://github.com/cnmsql/containers). Today's images
  don't have them (§5.9).
- Storage next to physical backups under the same cluster prefix, with a
  **separate manifest name**, so nothing that discovers physical backups
  (raw-S3 recovery, retention) can pick a dump by mistake.
- Reclaim policy, retention GC, scheduled backups, and `kubectl cnmsql`
  support for logical backups.
- **Bootstrap import:** `spec.bootstrap.initdb.import` creates a fresh cluster
  (any supported series, same flavor) and loads a dump into it, optionally only a
  subset of databases.
- **Restore into a running cluster** (phase 3): a one-shot `LogicalRestore` CR
  that loads selected databases from a dump into an existing cluster, with
  overwrite guards.

### Out of scope

- **Users and grants in the dump.** Accounts are managed through
  `spec.managed.roles`, `Database` and `DatabaseUser` (designs 014/023). A dump
  carries application schemas only. System schemas (`mysql`, `sys`,
  `performance_schema`, `information_schema`) are always excluded. This is also
  what keeps cross-series imports safe: auth plugins and privilege tables change
  between series. Operator-owned schemas (the replication `heartbeat` schema,
  configurable via the heartbeat settings) are excluded too, so an import never
  collides with what the target cluster creates for itself.
- **Logical backups as PITR anchors.** A dump is never a base for binlog
  replay, and retention never uses it to compute the binlog horizon.
- **Parallel dump/load** (mydumper/myloader, MySQL Shell `util.dumpInstance`).
  Deferred to a follow-up; see §11.
- **Renaming a database on restore** (restore `shop` as `shop_restored`). The
  dump references the database name inside views, routines and triggers, and
  rewriting those is unreliable.
- **Supported cross-flavor import** (MySQL dump into MariaDB or the reverse). It
  may work for simple schemas, but it is not tested or guaranteed in this design
  (collations like `utf8mb4_0900_ai_ci` do not exist on every MariaDB version).
- **Automatic first physical backup after an import.** An imported cluster has
  no base backup and no PITR until one is taken. Taking one is left to the user
  (a `Backup` or `ScheduledBackup`), and the docs say so. If the operator ever
  does this, it is a general "backup after bootstrap" feature, not part of
  logical backups.
- **Rewriting `DEFINER` clauses** (a `stripDefiners` option). Views, routines,
  triggers and events keep the definer from the dump (LB16).
- **Import from a live external server** (CNPG's `initdb.import` from a running
  PostgreSQL). Only object-store dumps are importable here.

## 3. Existing building blocks

- `Backup`/`ScheduledBackup` CRDs, a `BackupReconciler` that selects a source
  instance (`prefer-standby` / `primary`), renders a worker Job, and mirrors Job
  state into `Backup.status` (`internal/controller/backup_controller.go`).
- The worker (`manager instance backup upload`) pulls a stream from the source
  instance manager over mTLS (`GET /cluster/backup`), checksums it in flight,
  uploads it with a multipart upload of unknown length, and writes a manifest.
  Mid-stream source failures are reported through HTTP trailers
  (`webserver.BackupAnchorErrorTrailer`).
- `pkg/management/mysql/objectstore`: key building, SHA256 reader/writer,
  streaming `Upload`/`Download`, `ListBaseBackups`, `PlanRetention`.
- `pkg/engine.BackupTool` already names the SQL client (`mysql`/`mariadb`) used
  for PITR replay.
- `instance restore` shows the pattern for an init container that starts a
  temporary `mysqld` on a socket to run SQL before `instance run` takes over.
- Backup `reclaimPolicy: Delete` removes the whole backup directory
  (`<cluster>/<backup>/<id>/`), so it covers any file layout inside it.

**Missing piece: the dump tools.** The instance images install the server
client packages and then delete the dump binaries on purpose, to stay slim
(§5.9). No published tag of `cnmsql-instance` or `cnmsql-mariadb-instance`
contains `mysqldump` or `mariadb-dump`. The SQL clients (`mysql`, `mariadb`) are
kept, so loading a dump works with today's images; taking one does not.

### Places that assume every Backup is physical

These must learn about `method: logical` or they will break:

| Place | Assumption | Change |
|---|---|---|
| `BackupReconciler.Reconcile` | rejects any method but `xtrabackup` | branch on method |
| `objectstore.ListBaseBackups` | every `metadata.json` under the cluster prefix is a base backup | dumps use a different manifest name (§5.3) |
| `cluster_plan.go` `resolveRecovery` | `bootstrap.recovery.backup` points at a physical backup | reject logical Backups with a pointer to `initdb.import` |
| `cluster_retention.go` | only base backups + binlogs | also expire dumps (§5.6) |
| `cluster_mysql_upgrade.go` pre-upgrade gate | the gate Backup is physical | create it with an explicit `method: xtrabackup` |
| `scheduledbackup_retention.go` floor | newest completed Backup is a recovery point | unchanged: a schedule has one method, so the floor stays correct |
| `kubectl cnmsql status` / `backup` | method is always xtrabackup | show method, add `--method logical` |

## 4. Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| LB1 | `method: logical` on the existing `Backup` CRD, not a new CRD | Reuses target selection, Job template, reclaim policy, schedules, status, and kubectl. CNPG models methods the same way. |
| LB2 | Tool is chosen by the engine: `mysqldump` for Percona, `mariadb-dump` for MariaDB. The instance images in cnmsql/containers are changed to keep them (§5.9) | Both come from the client packages the images already install, so it means removing one entry from each strip list, not adding a new dependency. They match the server series and flavor, and the engine facet keeps the tool swappable (§11). |
| LB3 | The dump runs **inside the source instance manager** and is streamed over the existing mTLS channel (`POST /cluster/dump`) to the worker | Same data path as physical backups (008 decision 2): the dump connects over the local socket, and object-store credentials never enter the instance Pod. |
| LB4 | One consistent snapshot per backup: `--single-transaction` across all selected databases in a single dump invocation | Cross-schema consistency. Several per-database invocations would each see a different point in time. |
| LB5 | Store a single `dump.sql.zst` object; compress with zstd in the worker (Go, `klauspost/compress`) | One object, one checksum, one upload. Compressing in the worker keeps CPU out of the database Pod and needs no binary in the image. |
| LB6 | Partial restore is a **filter at load time**, not a per-database file layout | mysqldump output with `--databases` is split by `-- Current Database: \`x\`` markers on their own line, which cannot appear inside data (mysqldump escapes newlines in values). The loader streams and skips unselected sections. |
| LB7 | Dumps are **GTID-neutral** (`--set-gtid-purged=OFF`; MariaDB: no `--gtid`); the snapshot GTID is recorded in the manifest for information only | Loading must not rewrite the target's `gtid_purged`. The snapshot position is useful for humans but a dump is never a PITR anchor. |
| LB8 | Application schemas only, no users/grants, system schemas always excluded | Accounts are declarative CRs, and privilege tables are the least portable part of a dump across series. |
| LB9 | Manifest is `logical.json`, not `metadata.json` | `ListBaseBackups` matches `metadata.json`. A distinct name keeps raw-S3 recovery and retention from treating a dump as a base backup, even on an older operator after a rollback. |
| LB10 | Restore into a new cluster goes through `bootstrap.initdb.import`, named after CNPG's `initdb.import` | A dump loads into an initialised server; it does not replace a data directory. This is how cross-series moves work: initdb on the target series, then load. |
| LB11 | The import runs in an init container on the first instance, against a temporary `mysqld` with binary logging off | Object-store credentials stay out of the run container (as in `instance restore`). No binlog means a faster load and no huge first binlog to archive. Replicas then clone the loaded primary physically, as usual. |
| LB12 | Restore into a running cluster is a separate one-shot CR, `LogicalRestore`, in a later phase | It has different safety rules (it writes to a live primary) and should not hold up the backup and bootstrap work. |
| LB13 | The instance manager checks that the dump tool exists before it starts, and the Backup fails with reason `LogicalToolUnavailable` when it doesn't | Clusters pinned to an older image tag (`8.0-1`, or an `ImageCatalog` entry) will not have the tool. They need a clear failure that says which image to move to, not a crash in the middle of the stream. |
| LB14 | Dumps run as a dedicated read-only system account, `cnmsql_dump@localhost`, not the control account | Least privilege: the account can read application data and take the brief read lock the dump needs, and nothing else. `localhost` means it only works over the instance's Unix socket, so it can't be used from another Pod even if the password leaks. `cnmsql_*` names are already reserved (`isReservedRoleName`), so no user can declare a clashing role. |
| LB15 | The operator creates and migrates the dump account from its reconcile loop, on new and existing clusters alike, by calling the primary's instance manager (the managed-roles path). The worker Job, not the instance Pod, carries the password and sends it with the dump request | One code path for new clusters and for the migration. Putting the password in the instance Pod's env would change the Pod template hash and restart every instance on operator upgrade, which the upgrade design avoids on purpose. The account replicates to replicas through the binlog, so no replica needs the password in advance. Carrying the password in the Job is temporary until [#128](https://github.com/cnmsql/cnmsql/issues/128) (§5.10). |
| LB16 | `DEFINER` clauses are kept as dumped; no rewriting option | Rewriting means editing SQL text, which LB6 avoids. The missing definer accounts are declared as CRs, like any other user. |
| LB17 | `LogicalRestore` loads through the binlog, with `sql_log_bin` left on | Replicas and continuous archiving follow the restore with no extra step, and PITR stays correct across it. The cost, a burst of binlog the size of the restore, is documented. |

## 5. Design

### 5.1 API

`BackupMethod` gains a value:

```go
// +kubebuilder:validation:Enum=xtrabackup;volumeSnapshot;logical
type BackupMethod string

// BackupMethodLogical takes a SQL dump of the application schemas with the
// engine's dump client (mysqldump / mariadb-dump).
BackupMethodLogical BackupMethod = "logical"
```

`BackupSpec` and `ScheduledBackupSpec` gain an optional block, only valid with
`method: logical`:

```go
// LogicalBackupOptions configures a logical (SQL dump) backup.
type LogicalBackupOptions struct {
    // Databases limits the dump to these schemas. Empty means every
    // application schema (system schemas are always excluded).
    // +optional
    Databases []string `json:"databases,omitempty"`

    // ExtraArgs are appended to the dump command. The operator does not
    // validate them; flags that change the output format or GTID handling
    // break restore.
    // +optional
    ExtraArgs []string `json:"extraArgs,omitempty"`
}

// Logical configures the dump when method is "logical".
// +optional
Logical *LogicalBackupOptions `json:"logical,omitempty"`
```

`Cluster.spec.backup` gets `logicalOptions []string` next to `xtrabackupOptions`,
used as the cluster-wide default for `extraArgs`.

`BackupStatus` reuses its fields for logical backups: `destinationPath` points
at `dump.sql.zst`, `sha256` is the compressed object's checksum,
`beginBinlog`/`endBinlog` both hold the snapshot position (`file:position`), and
`beginGTID`/`endGTID` both hold the snapshot GTID on MariaDB. MySQL has no
snapshot GTID (see §5.2). A new optional `databases []string` records what was
dumped (so a user can see what a restore can select).

`ClusterStatus` gains `dumpAccountSecretVersion string` and a
`DumpAccountReady` condition, both written only by the operator (§5.10).

Bootstrap import (phase 2), next to CNPG's shape:

```go
// BootstrapInitDB
// Import loads a logical backup into the freshly initialised cluster.
// +optional
Import *BootstrapImport `json:"import,omitempty"`

type BootstrapImport struct {
    // Backup references a completed logical Backup in this namespace.
    // +optional
    Backup *LocalObjectReference `json:"backup,omitempty"`

    // Source names an ExternalClusters entry whose object store holds the
    // dump. Mutually exclusive with Backup.
    // +optional
    Source string `json:"source,omitempty"`

    // BackupID selects a dump under Source; empty picks the latest.
    // +optional
    BackupID string `json:"backupID,omitempty"`

    // Databases imports only these schemas from the dump. Empty imports all.
    // +optional
    Databases []string `json:"databases,omitempty"`

    // PostImportSQL runs as root after the load.
    // +optional
    PostImportSQL []string `json:"postImportSQL,omitempty"`
}
```

Admission:

- `initdb.import` and `bootstrap.recovery` are mutually exclusive (already true
  for `initdb` vs `recovery`). Cluster webhook, phase 2.
- Exactly one of `import.backup` / `import.source`. Cluster webhook, phase 2.
- `Backup.spec.logical` set with a non-`logical` method, `online: false` on a
  logical Backup, and system schemas in `logical.databases` are rejected. There
  is no Backup webhook, so these are CEL rules on the Backup and ScheduledBackup
  CRDs (`x-kubernetes-validations`). `logical.databases` has `maxItems: 256`:
  without a bound the rule's estimated cost exceeds the API server's budget and
  the CRD itself is rejected. The Backup reconciler repeats the first two checks
  for objects admitted before the rules existed.
- `bootstrap.recovery.backup` pointing at a logical Backup is rejected at
  reconcile time with reason `LogicalBackupNotRecoverable` and a message pointing
  at `initdb.import`. The webhook cannot see the referenced Backup's method
  reliably, so this is a reconcile check.

### 5.2 Engine facet

New interface in `pkg/engine`, next to `BackupTool`:

```go
type LogicalTool interface {
    DumpBinary() string               // mysqldump | mariadb-dump
    LoadBinary() string               // mysql | mariadb (== SQLClientBinary)
    DumpArgs(opts DumpOpts) ([]string, error)
    // ParseSnapshotPosition reads the snapshot position from the dump's
    // comment lines.
    ParseSnapshotPosition(comments string) (BinlogInfo, error)
    DumpAccountGrants(v version.Version) []AccountGrant
    DumpAccountRevokes(v version.Version) []AccountGrant
}
```

`LoadArgs(defaultsFile)` joined it in phase 2: the SQL client reads root's
credentials from an option file, reads the stream as utf8mb4, and sends
statements up to the protocol's 1 GiB packet limit, since a single large row
is dumped as one statement.

Base arguments (MySQL; MariaDB swaps the ones it names differently):

```
--single-transaction --quick --skip-lock-tables
--routines --events --triggers --hex-blob
--default-character-set=utf8mb4
--set-gtid-purged=OFF --source-data=2   (MariaDB: --master-data=2)
--no-tablespaces
--databases <db>...
```

- `--source-data` exists from 8.0.26 and is the only spelling on 8.4/9.x. The
  facet gates on version and falls back to `--master-data` where needed.
- `--source-data=2` takes a brief `FLUSH TABLES WITH READ LOCK` to read a
  consistent position. On `prefer-standby` this lands on a replica, and it
  happens once at the start, not for the whole dump.
- `--no-tablespaces` avoids needing `PROCESS` and tablespace DDL that does not
  port across series.
- `--databases` with an explicit list, even for "all": the instance manager
  resolves the list from `information_schema.SCHEMATA` minus system and
  operator-owned schemas, so
  the output always has `CREATE DATABASE` / `USE` statements and the section
  markers LB6 relies on. A database's marker can appear twice: both clients
  write views in a second pass at the end, under a repeated
  `-- Current Database:` line. The phase 2 filter keys on every marker, not on
  the first one.
- The snapshot position comes from the `--source-data=2` / `--master-data=2`
  comment near the top: `-- CHANGE MASTER TO MASTER_LOG_FILE=…` on Percona 8.0
  (even with `--source-data`), `-- CHANGE REPLICATION SOURCE TO SOURCE_LOG_FILE=…`
  on 8.4 and later. `mariadb-dump` also writes the matching GTID as a comment
  near the end (`-- SET GLOBAL gtid_slave_pos='…';`), so the instance manager
  keeps the first and last comment lines of the stream, not only the header.
- MySQL has no GTID-neutral way to record the snapshot GTID.
  `--set-gtid-purged=COMMENTED` comments out the `GTID_PURGED` line but still
  writes an uncommented `SET @@SESSION.SQL_LOG_BIN=0`, which would keep a load
  out of the binlog (against LB17). So MySQL dumps record the binlog position
  only.

### 5.3 Object-store layout

Same directory as a physical backup, different files:

```text
<path>/<cluster>/<backup-name>/<backup-id>/dump.sql.zst
<path>/<cluster>/<backup-name>/<backup-id>/logical.json
```

`objectstore.BuildLogicalBackupKeys` builds them. `logical.json`
(`objectstore.LogicalBackupMetadata`):

```json
{
  "formatVersion": 1,
  "backupID": "nightly-1760000000",
  "clusterName": "shop",
  "backupName": "nightly",
  "instanceName": "shop-2",
  "method": "logical",
  "tool": "mysqldump",
  "flavor": "mysql",
  "serverVersion": "8.4.6-6",
  "compression": "zstd",
  "archiveKey": ".../dump.sql.zst",
  "sizeBytes": 123456789,
  "uncompressedBytes": 987654321,
  "sha256": "…",
  "databases": ["shop", "billing"],
  "snapshotBinlog": "mysql-bin.000042:1234",
  "startedAt": "…",
  "completedAt": "…"
}
```

`snapshotGTID` is only set on MariaDB (§5.2); a MySQL manifest has
`snapshotBinlog` only.

`formatVersion` lets a future dump format (e.g. per-table mydumper files) live
under the same manifest name. `serverVersion` and `flavor` let the import path
warn on a downgrade or a cross-flavor load.

New `objectstore.ListLogicalBackups` mirrors `ListBaseBackups` but matches
`/logical.json`. `ListBaseBackups` is unchanged.

### 5.4 Backup data path

```
Backup (method: logical)
  └─ BackupReconciler: select source, render worker Job
       └─ worker: manager instance backup upload --method=logical
            ├─ POST https://<source>:8080/cluster/dump   (mTLS)
            │     body: {databases, dump password from <cluster>-dump Secret}
            │     └─ instance manager: resolve schema list,
            │        exec mysqldump over the local socket as cnmsql_dump,
            │        stream stdout, send trailers
            ├─ zstd encode → SHA256 → S3 multipart upload (dump.sql.zst)
            ├─ check trailers + "-- Dump completed on" footer
            └─ PutJSON logical.json
```

**Instance manager endpoint** `POST /cluster/dump`:

- Same mTLS client-cert check as `/cluster/backup`. It is a `POST` because the
  request body carries the dump account password (§5.10), which must not go in a
  URL or a header that proxies and access logs record.
- Runs the dump client from the engine facet over the local Unix socket, as
  `cnmsql_dump`. The password from the request is written to a
  `--defaults-extra-file` on the scratch volume with mode `0600` and removed
  afterwards. It never appears on the command line.
- Returns `503` with reason `DumpAccountMissing` if `cnmsql_dump@localhost` does
  not exist yet on this instance (a replica that hasn't applied the account
  yet). The worker retries this (and `409`) every 5 seconds for 2 minutes
  before failing.
- Returns `422` with reason `InvalidDumpRequest` when a requested database does
  not exist or is a system or operator schema, or when "all" resolves to no
  database.
- Starts the dump client and waits for its first byte of output before it sends
  the status. A client that exits before writing anything (a wrong password, an
  unknown `extraArgs` flag) is a `500` carrying the client's stderr, not a `200`
  with an error trailer. The client has read its credentials file by then, so
  the file is removed at that point.
- Streams stdout as the response body. stderr is wrapped into structured log
  entries (D13).
- Sends the tool, flavor, server version and resolved databases as headers
  (`X-Cnmsql-Dump-Tool`, `-Flavor`, `-Server-Version`, `-Databases`), since they
  are known before the stream starts. Database names are path-escaped and
  comma-separated: names may contain commas and non-ASCII characters.
- Reads the comment lines as they pass to extract the snapshot position (§5.2)
  and sends it as trailers (`X-Cnmsql-Dump-Snapshot-GTID`,
  `X-Cnmsql-Dump-Snapshot-Binlog`).
- If the dump process exits non-zero after the 200 was committed, sends
  `X-Cnmsql-Dump-Error` so the worker fails the Backup (same pattern as
  `BackupAnchorErrorTrailer`).
- Allows one dump per instance at a time (409 otherwise), so a retried Job
  cannot stack dumps on the source.
- Before anything else, runs `exec.LookPath` on the engine's dump binary. If it
  is missing, returns `501 Not Implemented` with a body naming the binary and the
  running image (LB13). The worker turns this into a failed Backup with reason
  `LogicalToolUnavailable` and a message saying which instance image tag has the
  tool.

**Worker** reuses `backup upload` with `--method=logical`, which reads the dump
password from the `<cluster>-dump` Secret (mounted in the Job as an env var),
switches the source path, adds the zstd stage, verifies the trailers and the
`-- Dump completed on` footer, and writes `logical.json` instead of
`metadata.json`. A dump without the footer is treated as truncated and fails.
A failed dump removes the partial `dump.sql.zst`, so only complete dumps (with
a manifest) stay in the store. Databases and extra arguments are repeatable
flags (`--database`, `--dump-arg`), again because names may contain commas.

A worker that fails for a known reason writes
`{"reason": …, "message": …}` to its container's termination message. When the
Job fails, the Backup reconciler reads it from the newest worker Pod and fails
the Backup with that reason (`LogicalToolUnavailable`,
`InstanceManagerOutdated`, `DumpAccountMissing`, `DumpInProgress`,
`InvalidDumpRequest`, `DumpFailed`) instead of the Job's generic
`BackoffLimitExceeded`. The contract (the env var name, the message type) lives
in `pkg/management/mysql/backupworker`, shared by the worker and the operator.

**Controller** changes are small: branch on method for the worker args, key
builder and status. A logical Backup stays `Pending` with reason
`DumpAccountNotReady` until the Cluster's `DumpAccountReady` condition is true
(§5.10). A `404` from the source (its instance manager predates `/cluster/dump`,
which happens mid-upgrade) fails the Backup with reason
`InstanceManagerOutdated`. Source selection, Job template, TTL/deadline, and reclaim
stay as they are. `online: false` is rejected for logical backups, since a dump
is always online.

### 5.5 Bootstrap import (phase 2)

On ordinal 1 of a cluster with `initdb.import`:

> This follows today's init-container pattern for bootstrap (as `instance
> restore` does). [#127](https://github.com/cnmsql/cnmsql/issues/127) moves
> bootstrap to one-shot Jobs that run before the instance Pod exists. If #127
> lands first, the import is an `<instance>-import` Job running the same
> command, and it no longer needs its own "done" marker file.

1. The existing `initdb` init container initialises the data directory on the
   target series.
2. A new `import` init container (`manager instance import`), with the
   object-store env and the cluster's own `resources` (not the backup Job's:
   the temporary server has the instance's buffer pool):
   - skips if `<datadir>/.cnmsql-import-done` exists;
   - starts a temporary `mysqld --skip-networking --skip-log-bin
     --event-scheduler=OFF` on the socket (the same helper `instance restore`
     uses for credential reconcile), so imported events do not fire against a
     half-loaded schema;
   - downloads `dump.sql.zst`, verifies the SHA256 against `logical.json` while
     streaming, zstd-decodes, filters to `import.databases` (LB6), and pipes into
     the SQL client as root;
   - runs `postImportSQL`;
   - shuts the temporary server down cleanly and writes the marker file.
3. `instance run` starts as usual. Replicas clone the loaded primary through the
   normal XtraBackup/MariaBackup path.

The operator resolves the import (Backup or `externalClusters` entry, then its
`logical.json`) only until the cluster is established (`status.establishedAt`).
The import container is left out of the Pod template hash, so neither dropping
it once the cluster is established nor a newer dump appearing under a `source`
rolls the primary. A Backup that is still running, a missing Backup, or an
object store that cannot be read keeps the cluster `Provisioning` with reason
`ImportSourceNotReady` and requeues. A flavor mismatch, a missing database, a
failed Backup or an unreadable manifest blocks it with `ImportIncompatible`; a
physical Backup blocks it with `PhysicalBackupNotImportable`. The import command
repeats the manifest checks before it starts the temporary server.

An import Backup's dump is read from the Backup's own `objectStore`, else from
the store of the cluster it was taken from, else from the new cluster's (the
source cluster was deleted and replaced). `postImportSQL` and `databases` are
passed as container arguments with `$` escaped, so the kubelet does not expand
`$(VAR)` in them.

Re-running after a crash half-way is safe: the dump's `DROP TABLE IF EXISTS`
(mysqldump default) and `CREATE DATABASE IF NOT EXISTS` make a second load
overwrite the first. The marker only makes a finished import a no-op.

`Database` and `DatabaseUser` CRs work as usual after import. The `Database`
path issues `CREATE DATABASE IF NOT EXISTS`, so an imported schema is simply
taken over. Views, routines, triggers and events keep their `DEFINER` (LB16).
If the definer account does not exist yet, MySQL creates the object with a
warning and it fails at call time until the account is declared. The docs must
say to declare those users.

Guards:

- `logical.json.flavor` must match the cluster flavor (cross-flavor is out of
  scope, §2).
- `serverVersion` newer than the target series: allowed, with a Warning event
  (downgrades through SQL usually work but are not tested).
- Selected databases that are not in `logical.json.databases` fail the import
  before loading anything.

### 5.6 Retention and reclaim

- **Per-Backup reclaim**: unchanged. The finalizer removes the backup directory
  prefix, which contains `dump.sql.zst` + `logical.json`.
- **ScheduledBackup history limits**: unchanged (per schedule, one method).
- **Cluster `retentionPolicy`**: `cluster_retention.go` also lists logical
  backups and expires those whose `completedAt` is before the cutoff, always
  keeping the newest one. This is a separate `PlanLogicalRetention` pure
  function. It never touches base backups, binlogs, or the recovery horizon, and
  `PlanRetention` never sees dumps.

### 5.7 Restore into a running cluster (phase 3)

A new CRD, scaffolded with `kubebuilder create api --kind LogicalRestore`:

```yaml
apiVersion: mysql.cnmsql.co/v1alpha1
kind: LogicalRestore
metadata:
  name: restore-billing
spec:
  cluster:
    name: shop
  backup:
    name: nightly-20260924     # or source + backupID
  databases: [billing]         # required: no implicit "restore everything"
  policy: FailIfExists         # FailIfExists | DropAndRecreate
```

- The operator runs a Job (same worker image) that streams, filters and loads
  into the **current primary** through the `rw` service, as a dedicated
  short-lived account created through the instance manager SQL API and dropped
  afterwards.
- `FailIfExists` refuses if any selected database exists and has tables.
  `DropAndRecreate` drops them first. The policy is required so an overwrite is
  always an explicit choice.
- The load goes through the binlog (LB17), unlike bootstrap, so replicas and
  the continuous archive follow it. A large restore produces a binlog burst of
  about the same size: replica lag during the restore, and more archive and
  disk use until the binlogs expire. Docs and the CR status say so.
- The restore is not atomic. A failure leaves the selected databases partly
  loaded, and the status says so.

This phase gets its own sub-design review before implementation: the load
account's exact grants, how `DropAndRecreate` interacts with `Database` CRs that
own the dropped schema, and progress reporting.

### 5.8 kubectl plugin

- `kubectl cnmsql backup <cluster> --method logical [--databases a,b]`.
- `status` shows the method column for backups.
- `kubectl cnmsql backup download <backup> [--databases a] -o file.sql`
  streams, verifies, decodes and filters a dump to a local file. This covers
  "export a single schema" without a cluster. It needs object-store credentials
  on the user's side, the same way `aws s3 cp` would.

### 5.9 Instance images (cnmsql/containers)

The instance images are built in
[cnmsql/containers](https://github.com/cnmsql/containers), not in this repo.
Their current state on `main`:

| Image | Installs | Then strips | Result |
|---|---|---|---|
| `Dockerfile.instance` (Percona 8.0 / 8.4 / 9.x) | `percona-server-server`, which pulls `percona-server-client` | `mysqldump`, `mysqlpump`, and other client tools, in the `for b in …; do rm -f` loop | no dump tool; `mysql` and `mysqlbinlog` kept |
| `Dockerfile.mariadb-instance` (MariaDB 10.11 / 11.4 / 11.8 / 12.3) | `mariadb-server`, which pulls `mariadb-client` | `mariadb-dump` and its `mysqldump` alias, among others | no dump tool; `mariadb` and `mariadb-binlog` kept |

**Changes in cnmsql/containers** (one PR, before phase 1 merges here):

1. `Dockerfile.instance`: drop `mysqldump` from the strip loop. `mysqlpump`
   stays stripped (deprecated in 8.0.34, removed in 8.4). Fix the header
   comment, which says the mysql client is stripped, and list `mysqldump` in the
   "we keep" comment.
2. `Dockerfile.mariadb-instance`: drop `mariadb-dump` from the strip loop. Keep
   stripping the `mysqldump` alias: the engine calls `mariadb-dump`, which has
   been the real binary since MariaDB 10.5 on every supported series.
3. A tool contract. Add `images/required-tools.txt` and
   `images/mariadb-required-tools.txt`, listing every binary the instance manager
   executes (server, backup tool, stream tool, binlog client, SQL client, dump
   tool, install-db for MariaDB). `images/check-tools.sh` runs each built image
   and fails if a listed binary is missing or its probe fails (`--version` by
   default; `mariadb-install-db` and `mariadb-upgrade` only have a presence
   check, because they have no probe that exits 0 without side effects). The build
   drivers run it before tagging or pushing, and `build.yml` also builds and
   checks every image on pull requests, without pushing. That stops a later
   slimming pass from quietly removing a tool the operator needs.
4. README: update the "The build keeps…" paragraph and both flavor sections.
5. Record the size difference per image in the PR. Expected to be small: the
   dump tools link against client libraries the kept `mysql`/`mariadb` client
   already loads.
6. Publish. The build computes the next patch tag (`8.0-N`, `11.4-N`, …) and
   moves the bare `8.0` / `11.4` tags to it.

Done in [cnmsql/containers#1](https://github.com/cnmsql/containers/pull/1),
released as `v1.5.0`. The first tags with the dump tool are `8.0-5`, `8.4-5`
and `9.x-5` (`cnmsql-instance`), and `10.11-4`, `11.4-4`, `11.8-4` and `12.3-4`
(`cnmsql-mariadb-instance`). The images grew by 0.3 to 1.5 MB compressed.

**Effect in this repo**:

- The operator defaults (`pkg/engine/engine.go`) and the integration and e2e
  images (`test/integration/flavors_test.go`, `test/e2e/images.go`) use the bare
  series tags, so they pick up the new images with no code change once the
  containers PR is published. Phase 1 integration and e2e tests depend on it.
- Clusters on a pinned patch tag, or on an `ImageCatalog` that points at one,
  keep working for everything except logical backups, which fail with
  `LogicalToolUnavailable` (LB13) until they move to a newer tag. The docs list
  the first tag of each series that has the tool.
- Phase 2 import and phase 3 restore only need the SQL client, so they work on
  every image, old or new.

**Rejected alternatives**:

- *Copy the dump tool from the operator image*, like the manager binary (D10).
  The dump tool must match the server's flavor and series, and it links against
  that series' client libraries. D11 keeps backup tooling version-aligned with
  the instance image for this reason.
- *A separate "tools" image for a dump sidecar or Job.* The dump would then
  connect to MySQL over the network, which breaks LB3 and LB14 (a socket-only account; database credentials
  leave the Pod). It would also add an image matrix to maintain for every
  series.

### 5.10 Dump account and migration

**Account.** `cnmsql_dump@'localhost'`, with the password in a Secret
`<cluster>-dump` (key `password`, `username: cnmsql_dump`), created by the
operator with `ensurePasswordSecret`, like the other system accounts.

Grants come from the engine facet (`LogicalTool.DumpAccountGrants(version)`):

| Flavor | Grants on `*.*` | Why |
|---|---|---|
| MySQL 8.0 / 8.4 / 9.x | `SELECT, SHOW VIEW, TRIGGER, EVENT, RELOAD, REPLICATION CLIENT` | read data and definitions; `RELOAD` for the read lock `--source-data` takes; `REPLICATION CLIENT` for the binlog position |
| MariaDB 10.11 | `SELECT, SHOW VIEW, TRIGGER, EVENT, RELOAD, BINLOG MONITOR` | `BINLOG MONITOR` replaced `REPLICATION CLIENT` in 10.5 |
| MariaDB 11.4 / 11.8 / 12.3 | the above plus `SHOW CREATE ROUTINE` | added in 11.3 to read routine bodies without `SELECT` on `mysql.proc` |

`PROCESS` isn't needed because of `--no-tablespaces`, and `LOCK TABLES` isn't
needed because of `--single-transaction`. The list is confirmed per series by
the integration round trip (`test/integration/logical_integration_test.go`),
which dumps tables, views, routines, triggers and events as `cnmsql_dump` on
every published image and fails on any privilege error or
`insufficient privileges` comment.

Global `SELECT` also covers the `mysql` schema, including password hashes.
Where `partial_revokes` is on (MySQL), the account also gets
`REVOKE SELECT ON mysql.*`, using the same `Revokes` mechanism managed roles use.
The dump never reads `mysql.*` (LB8), and routines still dump with the revoke
in place. Where `partial_revokes` is off, `REVOKE IF EXISTS` only warns, so the
same statements work on every MySQL cluster. MariaDB has no partial revokes, so
there the `localhost` host limit is what contains the account.

The instance manager's user API refuses to touch its reserved accounts
(`cnmsql_control`, `cnmsql_repl`, …). `cnmsql_dump` is deliberately not on that
list: the operator creates it through that API, including on instance managers
that predate logical backups. Users still cannot declare it, because the Cluster
and DatabaseUser webhooks reserve every `cnmsql_` name.

**Who creates it.** Not `initdb`. The `ClusterReconciler` ensures the account in
its normal loop, next to `reconcileManagedRoles`, which already creates users on
the primary through the instance manager (`/user/list`, `/user/create`,
`/user/alter`):

1. Ensure the `<cluster>-dump` Secret exists.
2. When `status.currentPrimary` is set and ready, list users on it. If
   `cnmsql_dump@localhost` is missing, create it with the Secret's password and
   the facet's grants. If it exists, set its password and grants to what the
   Secret and facet say.
3. Record the result in status: condition `DumpAccountReady` and
   `status.dumpAccountSecretVersion` (the Secret's `resourceVersion` that was
   applied). Step 2 runs again only when that version changes or the condition
   isn't true, so the steady state costs one Secret read per reconcile and no SQL.
4. Replicas (async and Group Replication) get the account through replication,
   so nothing talks to them directly.

**Migration for existing clusters.** This is the same loop. After the operator
upgrade, the first reconcile of each existing Cluster creates the Secret and the
account; there is no separate migration step or flag. Specific cases:

- **No Pod restart.** The instance Pod spec doesn't change: the password goes to
  the worker Job (LB15), not the instance env, so the Pod template hash stays
  the same.
  This is a workaround. [#128](https://github.com/cnmsql/cnmsql/issues/128)
  has the instance manager read its credential Secrets through the Kubernetes
  API instead of env vars. Once it lands, the manager reads `<cluster>-dump`
  itself, the worker Job no longer carries the password, and
  `POST /cluster/dump` stops taking it in the request body.
- **Instances still on the old manager** (rolling or in-place manager upgrade
  not finished). Creating the account only needs `/user/create`, which old
  managers have. Dumping needs `/cluster/dump`, so a logical Backup against an
  old source fails with `InstanceManagerOutdated` (§5.4) until that instance is
  upgraded.
- **No primary available** (fenced, failing over, hibernated). The condition
  stays false with a reason, and logical Backups wait in `Pending`. Physical
  backups are unaffected.
- **Replica not caught up.** A replica may not have the account yet when a
  dump lands on it. The endpoint returns `503 DumpAccountMissing` and the worker
  retries (§5.4).
- **Read-only replica cluster** (`spec.replica`, if and when it exists). The
  account can't be created locally; it has to come from the source cluster. Out
  of scope here, called out so the replica-cluster design picks it up.
- **Recovered or imported clusters.** A cluster recovered from a physical backup
  may carry the source cluster's `cnmsql_dump` with a different password. Its
  status starts empty, so step 2 resets the password to its own Secret.
- **Password rotation.** Changing the Secret's `password` changes its
  `resourceVersion`, so step 2 re-applies it. No extra API.
- **Operator rollback.** The account and Secret stay behind. They are harmless:
  the name is reserved, the account is read-only and socket-only, and the
  Secret is owned by the Cluster, so it is garbage-collected with it.

## 6. Security

- The dump runs as `cnmsql_dump@localhost` (LB14), read-only and socket-only,
  through a `0600` defaults file, never through argv (argv is visible in
  `/proc`). The control, root and backup passwords are not involved.
- The dump password is in the `<cluster>-dump` Secret and in logical backup
  worker Jobs. It travels to the instance in the body of an mTLS request. On its
  own it gives nothing outside the instance Pod, because the account only
  accepts socket connections.
- Object-store credentials stay in worker Jobs and the import init container,
  never in the run container.
- `/cluster/dump` is mTLS-gated like `/cluster/backup`. It exposes all
  application data to any holder of a client certificate signed by the cluster
  client CA. That is the same trust as the physical stream, which already exposes
  the full data directory.
- `extraArgs` are passed to the dump tool as-is. They go through argv (not a
  shell) so there is no injection, but a user can break the output format. That
  is documented, as in CNPG.
- Phase 3 load account: least privilege on the selected schemas only, dropped
  when the Job finishes.

## 7. Implementation plan

Each phase ships on its own and updates `docs/`.

### Phase 0 — instance images (cnmsql/containers)

The §5.9 changes: keep the dump tools, add the required-tools check to CI,
update the README, publish new patch tags. Phase 1 code can be written in
parallel, but its integration and e2e tests need these images.

Done: [cnmsql/containers#1](https://github.com/cnmsql/containers/pull/1),
released as `v1.5.0` (first tags in §5.9).

### Phase 1 — logical Backup (M-LB.1)

1. `pkg/engine`: `LogicalTool` facet for MySQL and MariaDB, version-gated args,
   snapshot-header parser, unit tests per flavor/series.
2. `pkg/management/mysql/objectstore`: `BuildLogicalBackupKeys`,
   `LogicalBackupMetadata`, `ListLogicalBackups`, zstd stream helpers; tests
   that `ListBaseBackups` ignores `logical.json`.
3. Dump account (§5.10): `DumpAccountGrants` in the facet, `<cluster>-dump`
   Secret, ensure step in the `ClusterReconciler` via the instance-manager user
   API, `DumpAccountReady` condition and `status.dumpAccountSecretVersion`.
4. Instance manager: `POST /cluster/dump` handler (schema resolution, account
   check, defaults file, trailers, single-flight), unit tests with a fake process.
5. Worker: `backup upload --method=logical` (zstd, footer check, trailers,
   manifest).
6. API: `BackupMethodLogical`, `LogicalBackupOptions` on Backup/ScheduledBackup,
   `spec.backup.logicalOptions`, `status.databases`; webhook validation;
   `make manifests generate`.
7. `BackupReconciler`: method branch; wait on `DumpAccountReady`; pre-upgrade
   gate pins `xtrabackup`; `resolveRecovery` rejects logical Backups.
   `status.dumpAccountSecretVersion` is operator-owned, so the instance status
   webhook (D14) must keep rejecting instance writes to it.
8. Retention: `PlanLogicalRetention` + wiring in `cluster_retention.go`.
9. kubectl: `--method logical`, method column.
10. Docs: `docs/src/logical-backups.md` (drafted, see §9), links from
    `backup-recovery.md`, `scheduled-backups.md`, `backup-retention-deletion.md`.
    `instance-images.md` lists the first image tag of each series that carries
    the dump tool.

Done: [#132](https://github.com/cnmsql/cnmsql/pull/132).

### Phase 2 — bootstrap import (M-LB.2)

1. API: `BootstrapImport` under `initdb`, webhook rules, samples.
2. `manager instance import` command: temp server, stream/verify/decode/filter,
   load, marker, `postImportSQL`.
3. Cluster reconciler: resolve the import source (Backup CR or external
   cluster + `ListLogicalBackups`), render the init container on ordinal 1,
   flavor/version guards, events.
4. `kubectl cnmsql backup download`: reads the store's credentials from its
   Secrets, connects from the user's machine (`--endpoint` for an in-cluster
   store behind a port-forward), verifies the checksum, and writes through a
   partial file so a failed download leaves nothing behind.
5. Docs: import section, cross-series walkthrough (`8.0 → 9.x`).

### Phase 3 — LogicalRestore (M-LB.3)

Sub-design first (§10), then scaffold with `kubebuilder create api`,
controller, Job, docs.

## 8. Testing

**Unit**

- Dump args per flavor and series (8.0 < 8.0.26, 8.0.26+, 8.4, 9.x, MariaDB
  10.11/11.x).
- Snapshot parser on real header fixtures from each tool.
- Section filter: select one, several, none; database names with backticks and
  unicode; a value containing `-- Current Database:` inside an INSERT stays put.
- `ListBaseBackups` ignores dumps; `ListLogicalBackups` ignores base backups.
- `PlanLogicalRetention` floor and cutoff.
- Controller: method branching, `online: false` rejected, recovery rejects a
  logical Backup, pre-upgrade Backup pinned to xtrabackup.
- Handler: error trailer on non-zero exit, 409 on a concurrent dump, no password
  in argv, 501 + `LogicalToolUnavailable` when the binary is not in `PATH`,
  503 + `DumpAccountMissing` when the account is absent.
- Dump account: create when missing, reset password and grants when present,
  no SQL when `dumpAccountSecretVersion` matches, re-apply after a Secret
  change, condition false and Backup `Pending` without a primary; grants per
  flavor and series.

**Integration** (testcontainers, version matrix D12)

- Dump/load round trip per series with tables, views, routines, triggers,
  events, generated columns, BLOBs, utf8mb4 data.
- Cross-series: dump on 8.0 → load on 8.4; 8.4 → 9.x.
- MariaDB round trip on the supported MariaDB series.
- Every round trip dumps as `cnmsql_dump` with the facet's grants, so a missing
  privilege shows up as a failure per series.
- The dump binary exists and runs in every image in the version matrix. This is
  the same check as the containers CI, repeated here to catch skew.

**E2E** (Kind + MinIO, `feature` tier per design 025)

- Logical Backup on a 3-instance cluster → `Completed`, objects in MinIO,
  status fields set.
- New cluster with `initdb.import` from that Backup → data present, replicas
  join.
- Import with `databases: [one]` → only that schema exists.
- Raw-S3 recovery on a prefix holding both kinds picks the physical backup.
- Logical `ScheduledBackup` + retention expires old dumps and leaves base
  backups alone.
- Migration: create a cluster with the previous operator release, upgrade the
  operator, check that no instance Pod restarts, that `DumpAccountReady`
  becomes true, and that a logical Backup from a replica completes.
- The same specs also run in the MariaDB `flavor` lanes, so each MariaDB series
  checks its own dump grants, `mariadb-dump`, and the recorded snapshot GTID.
  Schedules and retention are operator logic, so they only run on MySQL.

## 9. Documentation

- New user page `docs/src/logical-backups.md`. It is committed now with
  `draft: true`, which keeps it out of production builds. Remove the flag and add
  it to `docs/sidebars.js` under "Backup and Recovery" when phase 1 ships.
- `backup-recovery.md`: a "physical vs logical" section and a link.
- `scheduled-backups.md`: `method: logical`.
- `backup-retention-deletion.md`: how dumps are expired.
- `major-version-upgrade.md` and `mariadb.md`: point at dump/import for moves
  the in-place path does not cover.
- `api-reference.md`: regenerated.

## 10. Open questions

None. The review answers are recorded as LB5 (compression in the worker), LB14
and LB15 (dedicated dump account, with migration), LB16 (keep definers) and LB17
(phase 3 loads through the binlog). The automatic backup after import is out of
scope (§2).

## 11. Future work

- **Parallel dump/load.** mydumper/myloader (MySQL + MariaDB, per-table files,
  regex filters) or MySQL Shell dump utilities (MySQL only). Needs the tool in
  the instance images. mydumper is not in the Percona or MariaDB apt repos, so
  the containers repo would install it from the upstream GitHub release `.deb`,
  pinned per Debian base. It would be selected with `logical.tool` and stored as
  `formatVersion: 2` (a directory of objects plus the same manifest name).
- `schemaOnly` import.
- Database rename on restore, if a reliable approach turns up.
- Supported cross-flavor migration (MySQL → MariaDB), with a compatibility check
  of the dump before loading.
