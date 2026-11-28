# 045 — VolumeSnapshot backups and recovery

Status: accepted (2026-11-28)

Related: issue #46, 008 (physical backup and recovery), 009 (binlog
archiving and PITR), 011 (raw S3 recovery), 031 (bootstrap Jobs), 036
(separate binlog storage, deferred), 044 (bounded fences in the instance
manager).

## Problem

`BackupMethod` already accepts `volumeSnapshot`, and the kubectl plugin and
ScheduledBackup forward it, but nothing implements it. A Backup with
`method: volumeSnapshot` fails in one of two ways before any work happens:

1. with `ObjectStoreNotConfigured` when the cluster has no
   `spec.backup.objectStore`, because the reconciler resolves the store before
   it looks at the method;
2. otherwise with `UnsupportedMethod` from `backupKeysForMethod`.

Everything downstream of the Backup assumes an xbstream archive in S3: the
worker Job, `metadata.json`, the anchor read on completion, the cleanup
finalizer, retention GC, cluster reclaim and `resolveRecovery`.

A CSI snapshot is the only backup method that does not stream the whole data
directory over the network. For large volumes it is the difference between
minutes and hours, and it is the natural base for "clone this cluster" and for
replica provisioning without a donor.

## Scope

In scope:

- taking a Backup with `method: volumeSnapshot`, online (default) or cold;
- recording the snapshot and its GTID anchor on the Backup and on the
  VolumeSnapshot itself;
- bootstrapping a new cluster from such a Backup through
  `spec.bootstrap.recovery.backup`, with or without point-in-time recovery.

Deferred to follow-up issues (named here so the design leaves room for them):

- retention policy for snapshot Backups;
- cluster reclaim of snapshots on Cluster deletion;
- seeding new replicas from the latest snapshot Backup instead of cloning;
- recovery from a VolumeSnapshot without a Backup object
  (a `recovery.volumeSnapshots` source naming the snapshot directly);
- multi-volume snapshots, once separate binlog storage (036) lands.

## Facts the design rests on

- **One PVC per instance.** `spec.binlogStorage` is refused by
  `validateClusterSpec` ("kept for a later milestone"), so the data directory
  and the binary logs share the instance volume. A single VolumeSnapshot
  captures both.
- **InnoDB is crash-safe and the binlog recovery is on the same volume.** A
  crash-consistent snapshot of that volume restores: InnoDB replays its redo
  log and MySQL reconciles prepared transactions against the last binlog. That
  is what makes a snapshot usable at all. It is not what makes it a PITR base:
  for that we need the GTID position of the consistent point.
- **MySQL has no non-blocking backup mode.** `LOCK INSTANCE FOR BACKUP` blocks
  only DDL and leaves DML running, so it gives no consistent position. The
  choices for an exact anchor are `FLUSH TABLES WITH READ LOCK` (writes blocked
  for the hold) or a stopped server. An online snapshot therefore cannot keep a
  primary writable, so the design is built around keeping the hold short and
  bounded.
- **Fencing stops mysqld and keeps the manager alive** (`Controller.Fence`),
  requested through the `cnmsql.cnmsql.co/fencing` Pod annotation. That is the
  cold path for free.
- **Restore already knows how to adopt a foreign data directory.**
  `reconcileRestoredServer` runs a temporary `--skip-grant-tables --skip-log-bin`
  server to `RESET REPLICA ALL` and reset the internal account passwords;
  `resetArchiveIdentity` gives the restored instance a fresh archive identity;
  `maybeReplay` replays archived binlogs from an anchor. The snapshot restore
  reuses all three and skips only the download, prepare and copy-back.
- **VolumeSnapshot is namespaced** and a PVC `dataSource` can only name a
  snapshot in its own namespace (cross-namespace data sources are still alpha
  behind `ReferenceGrant`). Recovery from a snapshot Backup is therefore
  same-namespace only.

## Decision

### 1. API

#### Cluster

```go
// BackupConfiguration
// VolumeSnapshot configures Backups taken with method volumeSnapshot.
// Required for that method: a snapshot Backup on a cluster without it fails
// with ClusterHasNoVolumeSnapshotSection.
// +optional
VolumeSnapshot *VolumeSnapshotConfiguration `json:"volumeSnapshot,omitempty"`

type VolumeSnapshotConfiguration struct {
	// ClassName is the VolumeSnapshotClass used for the instance data volume.
	// Empty uses the cluster's default class for the volume's CSI driver.
	// +optional
	ClassName string `json:"className,omitempty"`
	// Labels and Annotations are added to every VolumeSnapshot. Operator keys win.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
	// SnapshotOwnerReference sets who owns the VolumeSnapshot objects:
	// none (default; they outlive Backup and Cluster), backup, or cluster.
	// +kubebuilder:validation:Enum=none;backup;cluster
	// +kubebuilder:default:=none
	// +optional
	SnapshotOwnerReference SnapshotOwnerReference `json:"snapshotOwnerReference,omitempty"`
	// Online is the default for Backups that do not set spec.online. True (default)
	// holds a global read lock on the source for the cut; false fences the source
	// instance (mysqld stopped) for the cut.
	// +kubebuilder:default:=true
	// +optional
	Online *bool `json:"online,omitempty"`
	// Deadline bounds how long a snapshot Backup retries CSI errors and waits
	// for the snapshot to be provisioned before failing. Default 10m.
	// +optional
	Deadline *metav1.Duration `json:"deadline,omitempty"`
	// LockTimeout bounds how long the source instance holds the global read lock
	// (online) or stays fenced (cold) waiting for the snapshot to be cut. When it
	// elapses the instance releases on its own and the Backup fails. Default 2m.
	// +optional
	LockTimeout *metav1.Duration `json:"lockTimeout,omitempty"`
}
```

`deadline` and `lockTimeout` are fields rather than annotations so they are
validated and documented with the rest of the spec. There is no per-volume
class yet because there is no second volume; when 036 lands it adds
`binlogClassName` here.

#### Backup

```go
// BackupSpec
// VolumeSnapshot overrides the cluster's spec.backup.volumeSnapshot field by
// field for this Backup. Only valid with method: volumeSnapshot.
// +optional
VolumeSnapshot *VolumeSnapshotConfiguration `json:"volumeSnapshot,omitempty"`
```

CEL on `BackupSpec`, next to the existing `logical` rule:

```
!has(self.volumeSnapshot) || (has(self.method) && self.method == 'volumeSnapshot')
```

`spec.online` already exists on Backup (default true). For this method it
means hot versus cold as above. For xtrabackup it keeps its current meaning.
`spec.objectStore` is ignored for this method and `spec.jobTemplate` too (no
Job runs); both are documented, not rejected, so a ScheduledBackup can carry a
template shared with other methods.

#### Backup status

```go
// VolumeSnapshots lists the snapshots this Backup produced, one per instance
// volume. Empty for other methods.
// +listType=map
// +listMapKey=name
// +optional
VolumeSnapshots []VolumeSnapshotStatus `json:"volumeSnapshots,omitempty"`

type VolumeSnapshotStatus struct {
	// Name of the VolumeSnapshot, in the Backup's namespace.
	Name string `json:"name"`
	// Volume is the role of the snapshotted volume: data (and later binlog).
	Volume string `json:"volume"`
	// PVCName is the source claim.
	PVCName string `json:"pvcName,omitempty"`
	// ClassName is the VolumeSnapshotClass used, resolved at creation.
	ClassName string `json:"className,omitempty"`
	// ReadyToUse mirrors the snapshot's status.readyToUse.
	ReadyToUse bool `json:"readyToUse,omitempty"`
	// RestoreSize mirrors status.restoreSize: the minimum PVC request to restore it.
	RestoreSize *resource.Quantity `json:"restoreSize,omitempty"`
	// CreationTime mirrors status.creationTime: when the driver cut it.
	CreationTime *metav1.Time `json:"creationTime,omitempty"`
}

// Online records whether the snapshot was taken under a read lock (true) or
// on a fenced instance (false). Only set for method volumeSnapshot.
// +optional
Online *bool `json:"online,omitempty"`
```

Existing fields keep their meaning: `method`, `instanceName`, `startedAt`,
`stoppedAt`, `endGTID` (the anchor), `beginGTID` (equal to `endGTID`: the
consistent point is a single position), `endBinlog` (file:position at the
cut), `binlogObjectStore` (the archive the anchor belongs to, when archiving
is on). `jobName`, `destinationPath`, `objectStore`, `sha256` and `backupId`
stay empty. The `Method` printcolumn already shows `volumeSnapshot`.

### 2. Taking the snapshot

The Backup reconciler branches on the method before resolving any object
store. The snapshot branch is a step machine driven by requeues (no worker
Job), in six steps:

| Step | Operator | Source instance | Fails with |
|---|---|---|---|
| 0 preflight | VolumeSnapshot CRD present; `spec.backup.volumeSnapshot` set; no fenced instance on the cluster; target PVC bound; class exists and its `driver` equals the PV's CSI driver | | `ClusterHasNoVolumeSnapshotCRD`, `ClusterHasNoVolumeSnapshotSection`, `FencedInstancesPresent`, `VolumeSnapshotClassMismatch` |
| 1 prepare | online: `POST /cluster/snapshot/begin` ; cold: set the fencing annotation on the source Pod and wait for it to be not ready | online: see below; cold: mysqld stops | `SnapshotPrepareFailed` |
| 2 create | create the VolumeSnapshot (name = Backup name, source = the instance PVC, class from config), labels and annotations below; requeue 10 s | holding | `SnapshotCreateFailed` (non-AlreadyExists) |
| 3 provisioned | wait for `status.boundVolumeSnapshotContentName` and `status.creationTime`; `status.error` is retried every 10 s until `deadline` | holding, bounded by `lockTimeout` | `SnapshotDeadlineExceeded`, `SnapshotLockExpired` |
| 4 finalize | online: `POST /cluster/snapshot/end`; cold: clear the fencing annotation; patch `status.volumeSnapshots`, anchor and `Online`; condition `Progressing/SnapshotProvisioned` | releases / mysqld restarts | |
| 5 ready | wait for `status.readyToUse`, same retry and deadline | running normally | `SnapshotDeadlineExceeded` |
| 6 complete | phase Completed, `stoppedAt`, annotate the VolumeSnapshot with the final status, event `SnapshotCompleted` | | |

Notes on the steps:

- Steps 1 and 2 run only when no VolumeSnapshot labelled with this Backup
  exists yet, so a reconcile that lost its status patch does not cut twice.
  An `AlreadyExists` on create for a snapshot carrying our labels is harmless
  (cache lag) and the next pass picks the snapshot up by label.
- Step 3 waits for `creationTime`, not `readyToUse`. CSI semantics: the
  content is bound and `creationTime` set when the driver's `CreateSnapshot`
  returned, which is when the data is frozen; `readyToUse` can lag by an
  upload. The hold ends at step 4.
- A failure at any step after 1 runs step 4's release first (unfence or
  `/cluster/snapshot/end`) and then fails the Backup. The created
  VolumeSnapshot is left in place for inspection unless
  `snapshotOwnerReference: backup` removes it with the Backup.
- CSI reports errors as free text with no reliable permanent/transient
  distinction, so every error is retried until `deadline`. The
  class-to-driver preflight catches the one permanent case that is cheap to
  detect up front.
- Target selection reuses `selectBackupSource` (prefer-standby default). On a
  primary target the online hold is a write stall and the cold hold is a
  read-only cluster; both are documented as warnings, and an event
  `SnapshotOnPrimary` is emitted.

#### Labels and annotations on the VolumeSnapshot

Labels: `mysql.cnmsql.co/cluster`, `mysql.cnmsql.co/backup`,
`mysql.cnmsql.co/instance`, `mysql.cnmsql.co/volume: data`, plus the cluster's
and Backup's configured labels. Annotations, written at step 4 and completed at
step 6: `mysql.cnmsql.co/anchor-gtid`, `mysql.cnmsql.co/anchor-binlog`,
`mysql.cnmsql.co/online`, `mysql.cnmsql.co/flavor`,
`mysql.cnmsql.co/server-version`, `mysql.cnmsql.co/backup-start`,
`mysql.cnmsql.co/backup-end`, `mysql.cnmsql.co/binlog-object-store` (JSON of
the resolved archive store, no credentials). They make a snapshot
self-describing so the deferred `recovery.volumeSnapshots` source needs no
Backup object, and so a snapshot copied to another cluster still says what it
holds.

### 3. The instance manager's quiesce

Two operator-only routes (the `AuthorizeClients` default; not peer routes):

`POST /cluster/snapshot/begin` with body `{ "backupName": "...", "timeout": "2m" }`

1. Refuse with 409 if a hold for another backup name is active; return the
   existing anchor if the same name asks again (idempotent across operator
   retries).
2. On a replica: `STOP REPLICA SQL_THREAD` (MariaDB: `STOP SLAVE SQL_THREAD`)
   and wait for the applier to report stopped. The I/O thread keeps fetching.
   On a Group Replication secondary, skip this step: stopping the applier is
   not available without leaving the group, and FTWRL alone blocks the applier
   for the hold, which flow control tolerates for the bounded time.
3. Open a dedicated connection and run `FLUSH TABLES WITH READ LOCK`. The lock
   lives on that connection only, so a dropped connection releases it.
4. Read the position on the same connection: MySQL `@@global.gtid_executed`
   and `SHOW BINARY LOG STATUS` (`SHOW MASTER STATUS` before 8.2); MariaDB
   `@@gtid_binlog_pos` and `SHOW MASTER STATUS`.
5. Write `<datadir>/cnmsql-snapshot.json`: `{backupName, anchorGTID,
   binlogFile, binlogPos, flavor, serverVersion, instance, takenAt, online: true}`,
   fsync it. It travels inside the snapshot and is what the restore reads, the
   same role `xtrabackup_binlog_info` plays for xbstream.
6. Arm a timer for `timeout`. When it fires, release as in `end` and record
   `expired: true`; a later `end` for that name returns 410 so the operator
   fails the Backup with `SnapshotLockExpired` instead of completing an
   anchor that may not match the cut.
7. Return `{anchorGTID, binlogFile, binlogPos, flavor, serverVersion}`.

`POST /cluster/snapshot/end` with body `{ "backupName": "..." }`

1. `UNLOCK TABLES` on the held connection and close it.
2. `START REPLICA SQL_THREAD` if step 2 stopped it.
3. Leave `cnmsql-snapshot.json` in place; the next `begin` overwrites it. A
   stale file on a running instance is harmless: the restore reads it only on
   a data directory it is adopting.
4. Return 200, or 404 when nothing is held, or 410 when the hold expired.

`GET /status` gains `snapshotHold: {backupName, since, expiresAt}` so the
operator and `kubectl cnmsql status` can see a hold.

The timer is the design 044 lesson applied here: a lock that depends on the
operator coming back is a lock that can hold the cluster hostage. The
instance always releases itself.

For the cold path the manager does nothing new. Fencing stops mysqld cleanly
(`SHUTDOWN` through the supervisor), which flushes InnoDB and closes the
binlog; the anchor is read by the restore from the data directory itself
(`mysql.gtid_executed` is complete after a clean shutdown, and the binlog
index names the last file). The operator records the anchor on the Backup
after the fact by asking the instance for it once it is back: `begin`/`end`
are not used, and `Online` is false.

### 4. Ownership and cleanup

`snapshotOwnerReference` has three values: `none` sets no owner,
`backup` sets the Backup as controller owner (snapshot goes with the Backup),
`cluster` sets the Cluster (snapshot goes with the Cluster). Default `none`,
consistent with the project's "deleting a Kubernetes object never destroys
the only copy of a recovery point unless the user opts in".

`reclaimPolicy: Delete` on the Backup additionally makes the existing cleanup
finalizer delete the VolumeSnapshot (new RBAC verb `delete` on
`volumesnapshots`). The project already has the reclaim opt-in and users
expect `reclaimPolicy` to mean the same thing for every method. The two
compose:
`snapshotOwnerReference: backup` is garbage collection, `reclaimPolicy:
Delete` is an explicit delete that also works with `none`.

`VolumeSnapshotContent` retention is the class's `deletionPolicy`, standard
Kubernetes behaviour; the docs say so and recommend backing up VolumeSnapshot objects
with a Kubernetes-level tool, since the content carries none of our
annotations.

Retention GC (`reconcileRetention`) and cluster reclaim ignore snapshot
Backups in this design; both are listed as follow-ups above.

### 5. Recovery from a snapshot Backup

`resolveRecovery` accepts a Backup whose `status.method` is `volumeSnapshot`
when it is Completed, has one `volumeSnapshots` entry with `readyToUse`, and
lives in the recovering cluster's namespace. Otherwise it fails the recovery
check with `SnapshotNotReady` or `SnapshotCrossNamespace`.

`recoveryPlan` gains:

```go
// Snapshot, when set, bootstraps the primary's volume from a VolumeSnapshot
// instead of an object-store archive. Bucket/ArchiveKey/MetadataKey are empty.
Snapshot *snapshotRecoveryPlan

type snapshotRecoveryPlan struct {
	Name        string            // VolumeSnapshot name
	RestoreSize resource.Quantity // from the Backup status
	AnchorGTID  string            // from the Backup status, for the target check
	Online      bool
}
```

`ensurePVC` for the bootstrap primary, when the plan has a snapshot and the
PVC does not exist yet, sets `spec.dataSource` to
`{apiGroup: snapshot.storage.k8s.io, kind: VolumeSnapshot, name}` and raises
the storage request to `max(spec.storage.size, restoreSize)`. The PVC still
starts with `pvc-status: initializing`. An existing PVC without that data
source is left alone: the plan is only consulted while the primary is not
bootstrapped (design 031); a pre-existing volume whose data source disagrees
with the plan is reported as `RecoverySourceMismatch` and left untouched.

`bootstrapModeFor` returns a new `bootstrapModeAdopt` for the primary when
the plan has a snapshot. Its Job runs `manager instance restore
--from-volume` with the same credential and config flags as today and no
object-store flags unless PITR is requested. The restore path:

1. Expect the data directory to be initialized (it is a copy of one). If it
   is not, fail: the snapshot did not hold a data directory.
2. Gate on a new sentinel `.cnmsql-adopted`, since `.cnmsql-bootstrapped` is
   already present from the source.
3. Read `cnmsql-snapshot.json` (online) or, when absent and the Backup was
   cold, derive the anchor from `mysql.gtid_executed` through the temporary
   server in the next step. Refuse to continue if neither is possible and PITR
   was requested.
4. Remove relay logs, `*.pid`, `*.sock*`, the slow log and the previous
   `cnmsql-snapshot.json`'s companion files that must not survive a clone
   (same list the join path already cleans).
5. `reconcileRestoredServer` as today: `RESET REPLICA ALL`, credential reset,
   under `--skip-log-bin`. On MySQL, crash recovery of the snapshot happens
   here, on the first start of the temporary server.
6. `resetArchiveIdentity`, so the restored instance never writes under the
   source's archive identity.
7. `maybeReplay` with the anchor from step 3 when a recovery target is set,
   reading the archive from `status.binlogObjectStore` recorded on the Backup
   (same resolution as xbstream recovery).
8. Write `.cnmsql-adopted`.

`checkRecoveryTarget` and the dead-branch check keep working: the anchor is
on the Backup status like any physical backup's.

Replicas are unchanged: they join by cloning the restored primary.

### 6. Operator plumbing

- Dependency `github.com/kubernetes-csi/external-snapshotter/client/v8`,
  types added to the scheme in `cmd/main.go`.
- CRD detection at startup through the discovery API, cached;
  no unconditional `Watches` on VolumeSnapshot so the manager starts on
  clusters without the snapshotter. When the CRD is present a watch mapped by
  the `mysql.cnmsql.co/backup` label feeds the Backup queue; the step machine
  also requeues every 10 s so it works without the watch.
- RBAC markers on the Backup reconciler: `volumesnapshots`
  get/list/watch/create/patch/delete; `volumesnapshotclasses` and
  `volumesnapshotcontents` get/list/watch. On the Cluster reconciler:
  `volumesnapshots` get/list/watch (recovery readiness).
- `kubectl cnmsql backup download` refuses a snapshot Backup with a message
  pointing at the VolumeSnapshot. `kubectl cnmsql status` lists a snapshot
  hold when the instance reports one.
- `BackupBeforeUpgrade` keeps using xtrabackup; a snapshot does not travel
  across a major-version upgrade any better, and the upgrade path expects an
  archive.

### 7. Validation and failure surfaces

Backup-level (controller, since Backup has no webhook): method requires the
cluster section; `spec.logical` with this method is `InvalidSpec` (existing
rule); `spec.online: false` on a cluster with any fenced instance is
`FencedInstancesPresent`; Group Replication clusters accept the online path
only, cold is `UnsupportedOnGroupReplication` until tested.

Cluster-level (`validateRecovery`): a snapshot Backup with a `recoveryTarget`
requires an archive store resolvable from the Backup or the cluster, same as
today; a snapshot Backup in another namespace is rejected at reconcile with a
condition.

Every failure reason above is a `Progressing=False` condition reason and an
event, following the existing `failBackup` shape.

## Consequences

- The Backup lifecycle gets a second shape next to the worker Job: a
  requeue-driven step machine with no Job and no object-store dependency. It
  is isolated in `backup_snapshot.go` so the Job path stays as it is.
- Online snapshots stall writes on a primary target for the cut, typically
  seconds, bounded by `lockTimeout`. This is the MySQL cost of an exact anchor
  and is the reason prefer-standby stays the default.
- A cold snapshot takes the source instance out for the cut plus a mysqld
  restart; on a primary that is a read-only cluster for the duration.
- The anchor lives in three places: the Backup status, the VolumeSnapshot
  annotations, and `cnmsql-snapshot.json` inside the volume. Only the last one
  is needed to restore, which is what makes raw snapshot recovery a small
  follow-up.
- Two RBAC additions on the operator, one new client dependency, and the
  e2e suite learns to install the snapshot CRDs, snapshot-controller and
  csi-driver-host-path the way it installs cert-manager.

## Alternatives considered

- **`LOCK INSTANCE FOR BACKUP` instead of FTWRL for the online hold.**
  Keeps DML running, so the source stays writable. Rejected: it does
  not stop the binlog or InnoDB from moving, so `gtid_executed` read under it
  is not the position of the cut, and a restore would replay from a wrong
  anchor. Crash-consistency alone would still restore, but PITR would be
  wrong by an unknown number of transactions.
- **Cold only (fence, snapshot, unfence).**
  Simplest and gives an exact anchor from a clean shutdown. Rejected as the
  only mode because a replica restart on every scheduled backup is a
  replication catch-up and a routing change each time; kept as the
  `online: false` path.
- **No quiesce, crash-consistent cut, anchor from the restored server.**
  Zero impact on the source. Rejected for the default because the anchor is
  then whatever crash recovery produces, and on a replica with the applier
  running the snapshot can land mid-transaction-batch; PITR from it is
  replayable but the dead-branch check loses its precision. Could return as
  an explicit `consistency: crash` option if users ask.
- **Owner reference only, no `reclaimPolicy` delete.** Rejected
  because this project already exposes `reclaimPolicy` on every Backup and
  the two would otherwise mean different things per method.
- **Snapshot only the data PVC once 036 adds a separate binlog PVC, and rely
  on the archive for the rest.** Rejected ahead of time: MySQL crash recovery
  consults the last binlog to decide prepared transactions, so a data volume
  without its binlog is not a consistent restore. Both volumes must be cut
  under the same hold. Recorded here so 036 inherits the rule.

## Implementation plan

Each task is a PR; tests first within each.

1. **API and docs scaffolding.** Cluster and Backup fields, status types,
   CEL rule, `make manifests generate api-docs`, `dist/install.yaml`
   regenerated with the released tag. INSTRUCTION.md decision row D25,
   INDEX.md entry.
2. **Instance manager quiesce.** `begin`/`end` routes, hold with timer and
   dedicated connection, `cnmsql-snapshot.json`, status field, MariaDB and
   Group Replication branches. Unit tests with a fake connection; envtest not
   needed.
3. **Backup reconciler snapshot branch.** Dependency, scheme, CRD detection,
   preflight, step machine, labels and annotations, finalizer delete,
   RBAC markers. Envtest installs the VolumeSnapshot CRDs from the client
   module's `config/crd` and drives `status` by hand, since no snapshotter
   runs there.
4. **Recovery from a snapshot Backup.** `resolveRecovery`, `ensurePVC` data
   source and size, adopt bootstrap mode, restore `--from-volume`, PITR from
   the anchor file, validation and conditions.
5. **e2e lane.** Install snapshot CRDs, snapshot-controller and
   csi-driver-host-path in the suite; one spec: cluster with archiving,
   writes, online snapshot Backup on the replica, assert Completed and
   `readyToUse`, more writes, bootstrap a second cluster from the Backup with
   `targetTime` between the two write sets, assert data; a second spec for the
   cold path on a replica. Run on CI only (memory: leave e2e to CI).
6. **User docs.** `docs/src/volume-snapshots.md` (requirements, hot versus
   cold, the primary-target warning, ownership and reclaim, recovery, what is
   not covered yet), cross-links from `backup-recovery.md`,
   `backup-retention-deletion.md` and `scheduled-backups.md`.

## Implementation notes

Shipped as one PR (issue #46) rather than the six above. Where it departs from
the decision:

- **No CRD defaults on `VolumeSnapshotConfiguration`.** The struct is shared by
  the Cluster and the Backup override; a defaulted field on the Backup would
  always win over the cluster's. The defaults (`none`, `online: true`, `10m`,
  `2m`) are applied by the controller. `spec.online` keeps its CRD default of
  `true`, so only an explicit `false` there counts, between the Backup's
  `volumeSnapshot.online` and the cluster's.
- **A cold snapshot records no anchor on the Backup.** Asking the instance once
  it is back returns a position past the cut: a replica resumes applying as soon
  as mysqld restarts. The adopt Job derives the exact anchor from the stopped
  server's data directory instead (a temporary server with the binary log on),
  and writes it as the manifest.
- **`GET /cluster/snapshot`** reports the hold and whether the instance is
  fenced with mysqld stopped, without querying mysqld. The cold path waits on it
  instead of Pod readiness, which can flip before the shutdown finished.
- **The heartbeat writer skips its stamp during a hold.** On a primary target
  the stamp would queue behind the read lock and hold a control connection for
  as long.
- **`mysql.cnmsql.co/snapshot-release` finalizer** is held from before the
  prepare step to the release, so a Backup deleted mid-hold still releases its
  source. The cold fence is marked with `mysql.cnmsql.co/snapshot-fence` so the
  release only ever clears a fence it set.
- **The hold polls every 2 s**, not 10 s: every poll the hold lasts is a write
  stall on a primary target. After the release it polls every 10 s.
- **The adopt sentinel holds the adopting cluster's UID**, and the adoption
  removes the source's replay sentinel and its own: a snapshot of a cluster that
  was itself recovered from one carries both.
- **The class-to-driver preflight reads the PV and classes uncached**, and skips
  them on Forbidden: a namespaced install cannot read cluster-scoped objects,
  and a cached read would block on an informer it may not run.
- **A recovery `targetTime` is checked against the cut** (the snapshot's
  `creationTime`), not the Backup's completion, which waits for the upload.
- **`SnapshotCrossNamespace`** is not implemented: `recovery.backup` is a local
  reference, so the Backup is always in the cluster's namespace.
