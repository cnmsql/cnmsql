# 029 — Logical Restore into a Running Cluster

- **Status:** accepted
- **Milestone:** M-LB.3
- **Issue:** [#47](https://github.com/cnmsql/cnmsql/issues/47)
- **Parent:** [028 — Logical Backups](028-logical-backups.md), §5.7 and phase 3

This is the phase 3 sub-design that 028 §5.7 asked for. It settles the three
questions left open there: which account the load runs as, how
`DropAndRecreate` interacts with `Database` CRs, and how progress is reported.
One of those answers changes 028: the load does **not** run as a dedicated
least-privilege account (LR2). 028 §5.7 and §6 now point here.

## 1. What it does

A `LogicalRestore` loads selected databases from a logical backup into a
**running** cluster's primary. It is the partial-restore path (028 §1): pull
`billing` back out of last night's dump without touching `shop`, or copy one
schema from another cluster's dump.

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

Not in scope, same as 028: renaming a database on restore, cross-flavor loads,
stripping definers, loading users and grants, making the restore atomic.

## 2. Findings that shaped the design

### 2.1 A least-privilege load account cannot load a dump

028 §5.7 planned a short-lived account with privileges on the selected schemas
only. That does not work, because of two decisions already taken:

- **LB16** keeps `DEFINER` clauses. Creating a view, routine, trigger or event
  for another account needs `SET_USER_ID` (8.0) or `SET_ANY_DEFINER` plus
  `ALLOW_NONEXISTENT_DEFINER` (8.2+), and `SYSTEM_USER` when the definer holds
  `SYSTEM_USER` (for example `root@localhost`).
- **LB17** loads through the binlog. With binary logging on and
  `log_bin_trust_function_creators=OFF` (the default), creating a trigger or a
  stored function needs **`SUPER`**, whatever else the account holds.

Checked on Percona 8.4 with a dump holding a table, a view, a function, a
procedure, a trigger and an event, all with `DEFINER=app@%`, plus a view with
`DEFINER=root@localhost`:

| Load account | Result |
|---|---|
| `ALL ON shop.*` | `ERROR 1419 … You do not have the SUPER privilege and binary logging is enabled` at the trigger |
| + `SET_ANY_DEFINER, ALLOW_NONEXISTENT_DEFINER` | same error 1419 |
| + `SYSTEM_USER` | same error 1419 |
| + `SUPER` | loads everything |
| `ALL ON *.*` (the control account's shape) | loads everything |

An account holding `SUPER` and definer impersonation can do everything the
operator's control account can, so a "scoped" account would only add a
credential to create, pass around and drop. Setting
`log_bin_trust_function_creators=ON` for the duration of the restore was
rejected too: it is a server-wide safety setting on a live primary, it races
with every other session, and the definer rules would still need
`SET_ANY_DEFINER` and `SYSTEM_USER`.

### 2.2 The dump is trusted input already

A restore runs whatever SQL the dump holds. That is the same trust the
operator already places in the bucket: a tampered `backup.xbstream` owns every
cluster recovered from it, and bootstrap import (028 §5.5) loads dumps as
root. The checksum in `logical.json` catches corruption, not tampering (the
manifest lives next to the dump). The docs say so; the design does not pretend
the load account narrows it.

### 2.3 Schema grants survive `DROP DATABASE`

MySQL and MariaDB keep schema-level grants (`mysql.db`) when the schema is
dropped. A `DropAndRecreate` restore therefore keeps the grants that
`Database` and `DatabaseUser` CRs applied; nothing has to re-grant after it.

## 3. Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| LR1 | The load runs **inside the target primary's instance manager** (`POST /cluster/load`), fed by a worker Job that reads the object store. The Job does not connect to MySQL through the `rw` Service. | Symmetric with the dump (LB3). Object-store credentials stay in the Job; no MySQL credential leaves the instance Pod; the instance manager checks it is a writable primary at the moment the load starts. |
| LR2 | The load runs as the instance manager's **control account**, over the local socket. No dedicated load account. Supersedes the "dedicated short-lived account" of 028 §5.7 and §6. | §2.1: any account that can load a dump with definers through the binlog needs `SUPER`, so a scoped account buys nothing. The control account is already in the instance Pod, so there is no credential to create, send or drop. |
| LR3 | The worker sends the databases and the policy in the query string and the SQL as the body, with `Expect: 100-continue`. Every refusal is a real status sent before any byte of the dump. | Nothing in the request is secret (the account lives in the Pod), and a refused restore never streams the dump for nothing. |
| LR4 | The **instance manager applies the database filter again** on the incoming stream (the section filter from phase 2), and fails the load if a selected database has no section. | The instance manager is the authority on what reaches the client. The worker also filters, but only to save bandwidth. |
| LR5 | The instance manager checks and applies the policy right before it starts the client: `FailIfExists` refuses (`409 DatabaseNotEmpty`) when a selected database holds a table, view, routine or event; `DropAndRecreate` runs `DROP DATABASE IF EXISTS` on each selected database. | No gap between the check and the load, which an operator-side check would have. An empty schema, such as one a `Database` CR created, is not in the way: that is the usual "declare the database, then restore into it" flow. |
| LR6 | `Database` CRs need no special handling (§2.3). The restore never deletes a `Database` CR, and the `Database` controller only issues `CREATE DATABASE IF NOT EXISTS`, so neither blocks the other. | Grants survive the drop. If the `Database` controller recreates the schema between the drop and the dump's own `CREATE DATABASE IF NOT EXISTS`, the schema keeps the CR's default character set, and each table keeps the one in the dump. |
| LR7 | **One attempt.** The worker Job has `backoffLimit: 0`; a failed restore is not retried. | After a partial load, a retry would hit `FailIfExists` or silently redo a `DropAndRecreate`. The user decides and creates a new `LogicalRestore`. |
| LR8 | **Progress is in the worker's logs**, not in status: every 30 s it logs the compressed bytes read against the manifest's `sizeBytes`, as a percentage. Status holds the phase, target, timing and outcome. | A live figure in status needs the worker Pod to write the API (a ServiceAccount and RBAC for Jobs) or the operator to poll the instance mid-load. Deferred (§9). |
| LR9 | A failed restore says whether data was touched. The worker decides and writes `unchanged: true` in its termination message when it failed before the instance accepted the load: a refusal (`NotPrimary`, `DatabaseNotEmpty`, `LoadInProgress`, `InvalidLoadRequest`, `LoadToolUnavailable`), `InstanceManagerOutdated`, a manifest that does not fit (`Incompatible`), or a target it could not reach (`TargetUnreachable`: dial or TLS handshake failure). A refusal wins over a download that failed at the same time. Failures the controller finds before the Job exists are unchanged too. Everything else, including a worker that died without a message, says the selected databases may be partly restored. | The load is not atomic (each dumped statement commits on its own), and the status must be honest about it. Only the worker knows whether its request got past the checks. |
| LR10 | The operator targets `status.currentPrimary` when it creates the Job, and waits in `Pending` (`PrimaryNotReady`) while there is no current primary, a switchover or failover is in flight (`targetPrimary != currentPrimary`), or the primary Pod is not ready. It does not block switchovers during a restore. | The instance manager's own writable check (LR1) catches a primary that moved between Job creation and the load. A switchover during the load makes the old primary read-only, and the restore fails part-way (LR9). |
| LR11 | The spec is **immutable**, and a `LogicalRestore` is one-shot like a `Backup`. Deleting a running one deletes its Job (owner reference), which cuts the stream; the instance manager kills the client. | A restore is an action, not a desired state. |
| LR12 | The dump is located with the **same resolver as bootstrap import**: `backup` (a completed logical `Backup` in the namespace) or `source` (an `externalClusters` entry on the target Cluster) plus an optional `backupID`. The manifest is checked (format, flavor, databases) before the Job is created, and again by the worker. | One code path, one set of rules. A dump from a newer series gets the same Warning event as an import. |

## 4. API

Scaffolded with `kubebuilder create api --group mysql --version v1alpha1
--kind LogicalRestore`.

```go
// LogicalRestorePolicy says what a restore does with a selected database that
// already holds objects.
// +kubebuilder:validation:Enum=FailIfExists;DropAndRecreate
type LogicalRestorePolicy string

const (
    // Refuse the whole restore when a selected database holds a table, view,
    // routine or event. Nothing is changed.
    LogicalRestoreFailIfExists LogicalRestorePolicy = "FailIfExists"
    // Drop each selected database, then load it from the dump.
    LogicalRestoreDropAndRecreate LogicalRestorePolicy = "DropAndRecreate"
)

// +kubebuilder:validation:XValidation:rule="has(self.backup) != (has(self.source) && size(self.source) > 0)",message="set exactly one of backup or source"
// +kubebuilder:validation:XValidation:rule="!has(self.backupID) || size(self.backupID) == 0 || (has(self.source) && size(self.source) > 0)",message="backupID is only valid with source"
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec is immutable; create a new LogicalRestore"
type LogicalRestoreSpec struct {
    Cluster     LocalObjectReference  `json:"cluster"`
    Backup      *LocalObjectReference `json:"backup,omitempty"`
    Source      string                `json:"source,omitempty"`
    BackupID    string                `json:"backupID,omitempty"`
    // MinItems=1, MaxItems=256, items 1..64 chars, a set, no system schemas
    // (same CEL rule as Backup.spec.logical.databases).
    Databases   []string              `json:"databases"`
    Policy      LogicalRestorePolicy  `json:"policy"`
    // Shapes the worker Job; merged field by field over the cluster's
    // spec.backup.jobTemplate, like a Backup's.
    JobTemplate *BackupJobTemplate    `json:"jobTemplate,omitempty"`
}

type LogicalRestoreStatus struct {
    Phase          LogicalRestorePhase `json:"phase,omitempty"` // pending|running|completed|failed
    TargetInstance string              `json:"targetInstance,omitempty"`
    JobName        string              `json:"jobName,omitempty"`
    BackupID       string              `json:"backupID,omitempty"`
    SourcePath     string              `json:"sourcePath,omitempty"`  // s3://bucket/key of dump.sql.zst
    Databases      []string            `json:"databases,omitempty"`   // what was restored
    StartedAt      *metav1.Time        `json:"startedAt,omitempty"`
    StoppedAt      *metav1.Time        `json:"stoppedAt,omitempty"`
    Error          string              `json:"error,omitempty"`
    Conditions     []metav1.Condition  `json:"conditions,omitempty"` // Ready, Progressing, Degraded
}
```

Short name `mylogicalrestore`. Print columns: cluster, policy, phase, age.

There is no webhook for this kind. The CEL rules cover the spec; the
controller repeats the "exactly one of backup/source" check. The configured
heartbeat schema cannot be checked by CEL; the instance manager refuses it
(`InvalidLoadRequest`), and a dump never holds it anyway.

## 5. Data path

```
LogicalRestore
  └─ LogicalRestoreReconciler
       ├─ resolve dump (Backup or source) + read logical.json, check it
       ├─ wait for a ready primary (LR10)
       └─ worker Job: manager instance logical-restore
            ├─ read logical.json, check it again
            ├─ POST https://<primary>:8080/cluster/load?database=…&policy=…   (mTLS, Expect: 100-continue)
            │     instance manager, before 100 Continue:
            │       tool present? one load at a time? writable primary?
            │       databases valid? policy (FailIfExists check / DropAndRecreate drop)
            │       start the SQL client as the control account over the socket
            │     after 100 Continue: body → section filter → client stdin
            ├─ object store → SHA256 → zstd decode → footer window → section filter → body
            ├─ at end of stream: checksum + footer; on a mismatch, abort the
            │  request instead of ending it, so the last partial statement never runs
            └─ response: 200 {databases, bytes} | error {reason, error}
```

**Instance manager, `POST /cluster/load`:**

- mTLS-gated like `/cluster/dump`. Query: `database` (repeatable, path-safe
  through the query encoding), `policy`.
- `StartLoad` runs every check before the body is read, so each refusal is a
  real status and the worker never sends the dump:
  - `501 LoadToolUnavailable`: the SQL client (`mysql`/`mariadb`) is not in
    the image. Every published image has it; the check keeps the failure clear.
  - `409 LoadInProgress`: one load per instance at a time.
  - `409 NotPrimary`: `read_only` or `super_read_only` is on, read through the
    replication reader, which also parses MariaDB 12's `OFF`/`ON` enum. This
    covers async replicas, Group Replication secondaries and a demoted primary.
  - `422 InvalidLoadRequest`: no database, an unknown policy, or a system or
    operator schema.
  - `409 DatabaseNotEmpty` (`FailIfExists`): names the non-empty databases.
  - `DropAndRecreate`: `DROP DATABASE IF EXISTS` for each selected database, on
    the control connection.
  - Starts the SQL client with the control account's credentials in a `0600`
    defaults file on the scratch volume, removed once the client exits.
- Reading the body sends `100 Continue`. The body goes through
  `sqldump.FilterDatabases` into the client's stdin.
- A body that ends with an error (the worker aborted, the connection dropped)
  kills the client rather than closing its stdin: at EOF the client would run
  the partial statement it holds.
- The response comes after the client exits: `200` with the loaded databases
  and the byte count, or `500 LoadFailed` with the tail of the client's stderr,
  or `500 LoadFailed` when a selected database had no section in the stream.
- No request timeout: the server has no `ReadTimeout`, same as the dump.

**Worker, `manager instance logical-restore`:**

- Flags: `--target-manager-url`, `--target-manager-server-name`,
  `--instance-name`, the TLS files, `--bucket`, `--dump-key`, `--manifest-key`,
  `--database` (repeatable), `--policy`, `--flavor`.
- Reads and checks `logical.json` (`CheckImportable`) before it contacts the
  instance.
- The worker's own filter runs after a footer window that sees the whole,
  unfiltered stream, so the `-- Dump completed on` check works even when the
  last section is not selected.
- On a failure with a known reason it writes the termination message
  (`backupworker.TerminationMessage`, which gains `unchanged`), as the backup
  worker does. Its own reasons: `Incompatible`, `DownloadFailed` (store error),
  `DumpCorrupt` (checksum, footer, zstd), `InstanceManagerOutdated`,
  `TargetUnreachable`, and the instance's `LoadReason*`.
- `404`/`405` from the instance: `InstanceManagerOutdated`.

**Controller:**

- Fails at once, and changes nothing, on `ClusterNotFound`, an invalid spec
  (`InvalidSpec`), a physical `Backup` (`PhysicalBackupNotRestorable`), a
  failed `Backup`, an unknown `source`, or a dump that does not fit the cluster
  (`Incompatible`: flavor, format, databases).
- Stays `Pending` and requeues while the source is not ready
  (`SourceNotReady`: Backup not completed yet, store unreachable) or there is no
  ready primary (`PrimaryNotReady`).
- Renders the Job like the backup worker's (D11: the cluster's instance image
  with the manager copied in from the operator image; the client TLS Secret and
  CA), with the object-store env of the resolved store, `backoffLimit: 0`, and
  the merged job template. Owner: the `LogicalRestore`.
- Mirrors the Job into status. On failure, the reason comes from the worker's
  termination message when it left one (LR9 decides the wording), else from the
  Job's condition. A Job that disappears under a running restore fails it with
  `JobMissing`, after the API server (not the cache) confirms it is gone. A
  Job of the same name that the restore does not control is never adopted
  (`JobConflict`).

## 6. Behaviour on a live primary

- **Binlog and replicas.** The load goes through the binlog (LB17): replicas,
  Group Replication members and continuous archiving follow it. A large restore
  writes about as much binlog as the dump holds, so expect replica lag and more
  archive and disk use until the binlogs expire.
- **GTIDs.** Each statement gets a GTID of the target cluster. The dump's
  snapshot GTID is ignored (LB7).
- **PITR.** Stays valid across the restore, since the restore is in the binlog.
- **Server settings.** None are changed. `max_allowed_packet` on the target must
  fit the dump's largest row, as it did on the source. Events are created with
  the status they had in the dump, and an enabled event starts firing once it
  is created.
- **Group Replication.** The usual Group Replication limits apply to the load
  like any other write: every table needs a primary key, and a transaction must
  fit `group_replication_transaction_size_limit`. mysqldump writes extended
  inserts of about 1 MB, each its own transaction.
- **Not atomic.** A failure part-way leaves the selected databases partly
  loaded (LR9).

## 7. Security

- The load runs as the control account (LR2), inside the instance Pod, over the
  socket, with its password in a `0600` defaults file, never in argv.
- `/cluster/load` accepts SQL from any holder of a client certificate signed by
  the cluster's client CA. That is the same trust as the rest of the control
  API (`/user/create`, `/instance/manager/upgrade`), which already grants full
  control of the instance.
- The dump is trusted input (§2.2). Anyone who can write to the backup bucket
  can make a restore run arbitrary SQL, as they can make a recovery restore
  arbitrary data today.
- Object-store credentials stay in the worker Job.

## 8. Implementation plan (M-LB.3)

1. `design/`: this document; 028 §5.7 and §6 point here; INDEX; D16 in
   INSTRUCTION.md.
2. API: `kubebuilder create api --kind LogicalRestore`, types, CEL rules,
   `make manifests generate`, sample.
3. `pkg/engine`: nothing new; `LoadBinary` and `LoadArgs` from phase 2 are
   reused.
4. Instance manager: `LoadStreamer` in `webserver` (`POST /cluster/load`,
   refusal mapping), `Controller.StartLoad` / `loadSession.Load` in
   `pkg/management/mysql/instance/load.go`, wired in the runner with the
   control credentials.
5. Worker: `manager instance logical-restore` in
   `internal/cmd/manager/instance/logicalrestore`.
6. Controller: shared dump resolver (refactored out of `cluster_import.go`),
   `LogicalRestoreReconciler`, registration in `cmd/main.go`.
7. kubectl: `kubectl cnmsql restore <cluster> --backup … | --source … [--backup-id …] --databases … --policy …`,
   and restores in `status`.
8. Docs: "Restore into a running cluster" in `logical-backups.md`,
   `api-reference.md`, sample.
9. Tests (§10).

## 9. Future work

- Progress in status (LR8): bytes loaded and the current database, written by
  the operator from a `/status` field the instance manager fills while a load
  runs.
- An opt-in "restore to a new schema name", if 028's rename problem is solved.
- An opt-in `stripDefiners`, if LB16 is ever revisited; it would not remove the
  `SUPER` need for triggers and functions under binlog (§2.1).

## 10. Testing

**Unit**

- Handler: every refusal maps to its status and reason; refusals happen before
  the body is read (the test body fails if read); `Expect: 100-continue`
  round trip; an aborted body kills the client and returns an error.
- Load session: `FailIfExists` refuses a database with a table, a view, a
  routine or an event, and accepts an empty one; `DropAndRecreate` drops each
  selected database; read-only refusal; the filter drops unselected sections;
  a missing section fails; the credentials file is gone after the load;
  single-flight.
- Worker: manifest checked before any request; refusal reasons written to the
  termination message; checksum mismatch and missing footer abort the request;
  `404` gives `InstanceManagerOutdated`.
- Controller: resolution failures (not found, running, failed, physical,
  flavor, missing database) end in the right phase and reason; pending without
  a primary and during a switchover; Job shape (target URL, args, env, backoff
  0, owner, template merge); Job success and failure mirrored, with the LR9
  wording.
- CEL rules: exactly one source, backupID only with source, immutable spec,
  system schemas refused (envtest).

**Integration** (testcontainers, version matrix)

- Load through `POST /cluster/load` on a running instance manager, per series:
  a dump with tables, views, routines, triggers and events with a foreign
  definer loads as the control account with binlog on; `FailIfExists` refuses a
  non-empty database; `DropAndRecreate` replaces it; an unselected database in
  the same dump is untouched.

**E2E** (Kind + MinIO, `feature` tier)

- Logical Backup of two databases on a 3-instance cluster; change and drop
  data; `LogicalRestore` of one database with `DropAndRecreate` → data back,
  the other database untouched, replicas have it.
- `FailIfExists` on a non-empty database → `Failed`, reason `DatabaseNotEmpty`,
  nothing changed.
- The same specs in the MariaDB lane.
