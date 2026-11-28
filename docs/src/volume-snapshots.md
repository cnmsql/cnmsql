---
title: "Volume Snapshot Backups"
description: "Back up a cluster with CSI VolumeSnapshots instead of streaming the data directory, and bootstrap a new cluster from one, with or without point-in-time recovery."
sidebar_position: 13
---

# Volume snapshot backups

A `Backup` with `method: volumeSnapshot` asks the CSI driver of the instance's
data volume for a snapshot instead of streaming the data directory to an object
store. On a large volume the snapshot is cut in seconds, where an XtraBackup
stream takes as long as it takes to read and upload every byte. A new cluster
can then be bootstrapped from the snapshot, and point-in-time recovery replays
the binlog archive from it like from any physical backup.

| | XtraBackup (`xtrabackup`) | Volume snapshot (`volumeSnapshot`) |
|---|---|---|
| Where the backup lives | object store | the CSI driver's snapshot storage |
| Time to take | proportional to the data | the driver's cut time, plus its upload |
| Impact on the source | I/O while streaming | a short write stall (online) or a short stop (cold) |
| Recovery | download, prepare, copy back | a new volume created from the snapshot |
| Point-in-time recovery | yes, with binlog archiving | yes, with binlog archiving |
| Recovery without the Backup object | yes, from the raw object store | not yet |
| Retention | yes | yes, see [Retention](#retention) |
| Cluster reclaim | yes | not yet, see [Limits](#limits) |

## Requirements

- The VolumeSnapshot CRDs (`snapshot.storage.k8s.io/v1`) and the snapshot
  controller are installed in the Kubernetes cluster.
- The cluster's data volumes come from a CSI driver that supports snapshots,
  and a `VolumeSnapshotClass` exists for that driver.
- The Cluster has a `spec.backup.volumeSnapshot` section. It does not need an
  object store, unless you also archive binlogs for point-in-time recovery.

```yaml
apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: shop
spec:
  instances: 3
  storage:
    size: 200Gi
    storageClass: csi-hostpath-sc
  backup:
    volumeSnapshot:
      className: csi-hostpath-snapclass
```

| Field | Default | Meaning |
|---|---|---|
| `className` | the driver's default class | `VolumeSnapshotClass` for the data volume. |
| `labels`, `annotations` | none | Added to every VolumeSnapshot. The operator's own keys win. |
| `snapshotOwnerReference` | `none` | Who owns the VolumeSnapshots: `none`, `backup` or `cluster`. See [Ownership and deletion](#ownership-and-deletion). |
| `online` | `true` | Online (read lock) or cold (instance fenced). See [Online and cold snapshots](#online-and-cold-snapshots). |
| `deadline` | `10m` | How long the Backup waits for the snapshot to be cut and become ready to use, retrying CSI errors. |
| `lockTimeout` | `2m` | How long the source instance may stay locked or fenced waiting for the cut. |

The operator starts without the snapshot CRDs. A snapshot Backup on a
Kubernetes cluster without them fails with `ClusterHasNoVolumeSnapshotCRD`.

## Taking a snapshot backup

```yaml
apiVersion: mysql.cnmsql.co/v1alpha1
kind: Backup
metadata:
  name: shop-nightly
spec:
  cluster:
    name: shop
  method: volumeSnapshot
```

`spec.volumeSnapshot` on the Backup overrides the cluster's section field by
field, for example to take one cold snapshot or to use another class:

```yaml
spec:
  method: volumeSnapshot
  volumeSnapshot:
    online: false
    snapshotOwnerReference: backup
```

`spec.target` works as for other methods: `prefer-standby` (the default)
snapshots a healthy replica, `primary` the primary. `spec.objectStore` and
`spec.jobTemplate` are ignored, because no worker Job runs and nothing is
uploaded. They are accepted so a `ScheduledBackup` can share a template across
methods: `method: volumeSnapshot` works on a ScheduledBackup like any other
method.

The command line plugin takes one too:

```bash
kubectl cnmsql backup shop --method volumeSnapshot
```

### What happens

1. **Preflight.** The operator checks the snapshot API is installed, the
   cluster has a `volumeSnapshot` section, no instance is fenced, the source
   volume is bound, and the class exists and serves the volume's CSI driver.
2. **Prepare.** The source instance is quiesced (see below).
3. **Create.** A VolumeSnapshot named after the Backup is created for the
   instance's data volume.
4. **Provisioned.** As soon as the driver reports the snapshot cut (the content
   is bound and `creationTime` is set), the source instance is released. It does
   not wait for the upload.
5. **Ready.** The Backup waits for the snapshot's `readyToUse`.
6. **Complete.** The Backup is `completed` and the VolumeSnapshot is annotated
   with the end time.

The Backup polls every few seconds while the source is held. When the snapshot
CRDs were present when the operator started, it also watches VolumeSnapshots,
so a change is picked up at once.

## Online and cold snapshots

MySQL has no way to read an exact binlog position without stopping writes. A
snapshot only needs to be crash consistent to restore, but point-in-time
recovery needs to know which transactions it holds. So the snapshot is taken
in one of two modes.

**Online (default).** The source instance's manager stops the replica's SQL
thread (the I/O thread keeps fetching), takes `FLUSH TABLES WITH READ LOCK` on
a dedicated connection, reads the GTID position and binlog coordinates under
the lock, and writes them into `cnmsql-snapshot.json` at the root of the data
directory, so they travel inside the snapshot. The lock is released as soon as
the snapshot is cut. Writes on that instance are blocked for that time,
typically a few seconds.

The instance releases the lock on its own after `lockTimeout`, even if the
operator never comes back. The Backup then fails with `SnapshotLockExpired`
instead of recording an anchor that may not match the snapshot.

On a Group Replication secondary the applier is not stopped (it cannot be
without leaving the group); the read lock blocks it for the hold, which flow
control tolerates for a bounded time.

**Cold (`online: false`).** The source instance is fenced (mysqld stops
cleanly, the manager stays up), the snapshot is cut, and the instance is
unfenced. A replica restarts and catches up; that is a replication catch-up and
a routing change for every cold snapshot. Cold snapshots are not supported on
Group Replication clusters yet (`UnsupportedOnGroupReplication`).

A stopped server has no position to read, so the anchor of a cold snapshot is
read on the instance's next start. Before the cut, the operator arms a capture
on the instance. The next mysqld start then runs with replication held
(`--skip-replica-start`), and the manager reads the GTID position and binlog
coordinates before anything else can write. Nothing ran since the clean stop,
so that position is the snapshot's. The manager then resumes replication, and
the operator records the anchor on the Backup before completing it, usually
a few seconds after the release.

Before it releases the instance, the operator checks that the capture is still
waiting. If mysqld started in between (for example after a manager restart),
the position it read could be past the cut. The operator then discards it and
completes the Backup without an anchor. The same happens when the instance
gives up after five failed reads, or when nothing is read within `deadline`. A
cold snapshot without a recorded anchor still restores, and point-in-time
recovery from it still works: the restore reads the anchor from the snapshot's
own data directory.

A Backup is cold when `spec.volumeSnapshot.online` is `false`, or, when that is
unset, when `spec.online` is `false`, or, when that is unset too, when the
cluster's `volumeSnapshot.online` is `false`. `spec.online` defaults to `true`
in the CRD, so only an explicit `false` there counts.

:::warning Snapshots of the primary
With `target: primary`, an online snapshot stalls every write on the cluster for
the hold, and a cold snapshot makes the cluster read-only until mysqld is back.
The operator emits a `SnapshotOnPrimary` warning event. Keep the default
`prefer-standby` unless the cluster has a single instance.
:::

## Status

A completed snapshot Backup records:

- `status.method: volumeSnapshot`
- `status.instanceName`: the instance it was taken from
- `status.online`: whether it was taken online or cold
- `status.volumeSnapshots[]`: one entry per volume, with the VolumeSnapshot
  `name`, the `volume` role (`data`), the source `pvcName`, the `className`, and
  the driver's `readyToUse`, `restoreSize` and `creationTime`
- `status.endGTID` and `status.beginGTID`: the anchor, the GTID position of the
  cut
- `status.endBinlog`: the binlog coordinates of the cut, as `file:position`
- `status.binlogObjectStore`: the archive the anchor belongs to, when the
  cluster archives binlogs

`jobName`, `destinationPath`, `objectStore`, `sha256` and `backupId` stay
empty. The anchor of a cold snapshot is the one read on the instance's next
start (see [Online and cold snapshots](#online-and-cold-snapshots)). It stays
empty when that read was discarded or failed.

The VolumeSnapshot itself carries labels (`mysql.cnmsql.co/cluster`,
`mysql.cnmsql.co/backup`, `mysql.cnmsql.co/instance`,
`mysql.cnmsql.co/volume: data`) and annotations that describe it without the
Backup: `mysql.cnmsql.co/anchor-gtid`, `anchor-binlog`, `online`, `flavor`,
`server-version`, `backup-start`, `backup-end` and `binlog-object-store`
(the archive location, without credentials).

While an instance is held, `kubectl cnmsql status` shows it as
`Snapshot hold (<backup>)`.

### When a snapshot backup fails

The Backup's `Degraded` condition carries the reason. The source instance is
always released before the Backup fails, and the VolumeSnapshot, if one was
created, is left for inspection.

| Reason | Meaning |
|---|---|
| `ClusterHasNoVolumeSnapshotCRD` | The VolumeSnapshot API is not installed. |
| `ClusterHasNoVolumeSnapshotSection` | The Cluster has no `spec.backup.volumeSnapshot`. |
| `FencedInstancesPresent` | An instance of the cluster is fenced. Unfence it first. |
| `UnsupportedOnGroupReplication` | A cold snapshot was asked on a Group Replication cluster. |
| `PVCNotBound` | The source instance's volume is missing or not bound. |
| `VolumeSnapshotClassMismatch` | The class does not exist, serves another CSI driver than the volume's, or no default class exists for the driver. Also when the volume is not a CSI volume. |
| `NoBackupSource` | No instance to take the snapshot from. |
| `SnapshotPrepareFailed` | The source could not be quiesced: the read lock timed out behind a long query, another snapshot holds it, or the instance Pod is gone. |
| `SnapshotCreateFailed` | The VolumeSnapshot could not be created, or one of that name exists that the Backup did not create. |
| `SnapshotLockExpired` | The snapshot was not cut within `lockTimeout`, or the instance released its lock before the cut was confirmed. |
| `SnapshotDeadlineExceeded` | The snapshot was not provisioned and ready within `deadline`. CSI errors on the VolumeSnapshot are retried until then and shown in the `Progressing` condition. |
| `SnapshotMissing` | The VolumeSnapshot was deleted before it was ready. |

A failed Backup is not retried. Create a new one.

## Ownership and deletion

VolumeSnapshots outlive their Backup and their Cluster by default, so deleting
a Kubernetes object never destroys the only copy of a recovery point by
accident. Two settings change that, and they combine:

- `snapshotOwnerReference: backup` or `cluster` makes the Backup or the Cluster
  the VolumeSnapshot's controller owner, so Kubernetes garbage-collects it with
  that object.
- `reclaimPolicy: Delete` on the Backup makes the operator delete the Backup's
  VolumeSnapshots when the Backup is deleted, as it removes the archive of an
  XtraBackup Backup. It works with any owner setting.

Whether deleting a VolumeSnapshot also deletes the data is the class's
`deletionPolicy`, as for any VolumeSnapshot. The `VolumeSnapshotContent`
carries none of the operator's annotations; back up the VolumeSnapshot objects
with a Kubernetes-level tool if you need them after their namespace is gone.

A Backup deleted while it holds its source instance releases it first: a
finalizer, `mysql.cnmsql.co/snapshot-release`, is held for exactly that time.

## Recovery

A new cluster bootstraps from a completed snapshot Backup with
`spec.bootstrap.recovery.backup`, as from an XtraBackup Backup:

```yaml
apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: shop-copy
spec:
  instances: 3
  storage:
    size: 200Gi
    storageClass: csi-hostpath-sc
  bootstrap:
    recovery:
      backup:
        name: shop-nightly
```

The Backup must be `completed`, its VolumeSnapshot ready to use and present in
the same namespace; until then the cluster stays `Blocked` with
`SnapshotNotReady`. The operator creates the primary's volume with the
VolumeSnapshot as its `dataSource`, at least as large as the snapshot's restore
size. The bootstrap Job (`<instance>-adopt`) then adopts the copied data
directory: it removes the source's process files, clears the inherited
replication configuration, resets the internal accounts to the new cluster's
credentials and gives the instance a new archive identity. Replicas clone the
recovered primary as usual. The storage class must use the same CSI driver as
the snapshot.

When the primary's volume already exists, is not bootstrapped yet, and was not
created from the snapshot, the cluster is `Blocked` with
`RecoverySourceMismatch`: delete the volume to let the operator create it from
the snapshot.

### Point-in-time recovery

With binlog archiving on the source, add a `recoveryTarget`. The archive is the
one the Backup records in `status.binlogObjectStore`; the recovering cluster
needs `spec.backup.objectStore` as for any point-in-time recovery.

```yaml
  bootstrap:
    recovery:
      backup:
        name: shop-nightly
      recoveryTarget:
        targetTime: "2026-11-28T10:05:00Z"
  backup:
    objectStore:
      # ...
```

Replay starts at the anchor in `cnmsql-snapshot.json`. For a cold snapshot the
adopt Job reads it first from a temporary server started over the data
directory. A `targetTime` must not be before the snapshot was cut
(`status.volumeSnapshots[0].creationTime`), which is earlier than the Backup's
completion. See [Point-In-Time Recovery](pitr.md).

## Retention

`spec.backup.retentionPolicy` covers snapshot Backups. A completed snapshot
Backup expires when it completed before the window. The operator then deletes
the Backup and its VolumeSnapshots, whatever the Backup's `reclaimPolicy`, the
same way retention deletes an expired object-store backup. The newest snapshot
Backup is always kept, and so is the newest object-store backup: a recent
snapshot never expires the last backup that lives outside the Kubernetes
cluster.

Snapshots count for the binlog archive too. The recovery horizon is the oldest
start time among the retained backups of both kinds, so the archive keeps every
binlog a retained snapshot needs for point-in-time recovery.

A cluster that only takes snapshots can set a `retentionPolicy` without an
object store:

```yaml
spec:
  backup:
    retentionPolicy: 14d
    volumeSnapshot:
      className: csi-hostpath-snapclass
```

See [Retention GC](backup-retention-deletion.md#retention-gc).

## Limits

- **Cluster reclaim.** `spec.backup.reclaimPolicy: Delete` on the Cluster
  ignores snapshot Backups.
- **Same namespace.** A volume can only be created from a VolumeSnapshot in its
  own namespace.
- **Replicas** are still cloned from the primary, not created from a snapshot.
- **Recovery needs the Backup object.** A recovery that names a VolumeSnapshot
  directly is a follow-up; the snapshot already carries everything it needs.
- **Inherited binlogs.** The recovered primary's data directory holds the
  source's binary logs. They are kept (MariaDB derives its GTID state from
  them), and the new cluster's archiver uploads them once into its own archive.
- **Namespaced installs** cannot read PersistentVolumes or
  VolumeSnapshotClasses, which are cluster-scoped. The class-to-driver
  preflight is skipped there, and a missing or mismatched class shows up as a
  CSI error, retried until `deadline`.
- **One volume per instance.** A separate binlog volume is not supported by the
  operator yet; when it is, both volumes will be cut under the same hold.
- `BackupBeforeUpgrade` always takes an XtraBackup backup.
- `kubectl cnmsql backup download` refuses a snapshot Backup: there is nothing
  in the object store to download.
