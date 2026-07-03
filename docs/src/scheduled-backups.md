---
title: "Scheduled Backups"
description: "How ScheduledBackup creates Backup objects on a six-field cron schedule."
sidebar_position: 16
---

# ScheduledBackup architecture

`ScheduledBackup` is the scheduling layer for physical backups. It does not move
backup bytes itself. Instead, it creates ordinary `Backup` objects on a cron
cadence, and the existing Backup controller runs the XtraBackup-to-object-store
data path.

For one-off on-demand backups, skip the YAML and use the plugin:

```bash
kubectl cnmsql backup <cluster>
```

This page covers recurring scheduled backups.

```mermaid
flowchart LR
    SB["ScheduledBackup"]
    Scheduler["ScheduledBackup Reconciler"]
    Backup["Backup"]
    BackupCtl["Backup Reconciler"]
    Job["Backup Worker Job"]
    Store["Object Store"]

    SB --> Scheduler
    Scheduler -->|"create/adopt"| Backup
    Backup --> BackupCtl
    BackupCtl --> Job
    Job --> Store
    Scheduler -->|"last/next times"| SB
```

## Schedule format

`spec.schedule` is a six-field cron expression including seconds:

```text
second minute hour day-of-month month day-of-week
```

Examples:

- `"0 0 2 * * *"`: every day at 02:00:00.
- `"0 */15 * * * *"`: every 15 minutes.
- `"30 0 */6 * * *"`: every six hours at minute 0, second 30.

Five-field Unix cron expressions are intentionally rejected because the leading
seconds field is required.

## Basic example

```yaml
apiVersion: mysql.cnmsql.co/v1alpha1
kind: ScheduledBackup
metadata:
  name: cluster-sample-daily
spec:
  schedule: "0 0 2 * * *"
  cluster:
    name: cluster-sample
  immediate: true
  backupOwnerReference: self
  method: xtrabackup
  target: prefer-standby
  online: true
  reclaimPolicy: Retain
```

The generated Backup inherits the cluster reference, method, target, and online
setting. The object store is resolved by the Backup controller, usually from
`Cluster.spec.backup.objectStore`.

## Object-store cleanup on deletion

By default, deleting a generated Backup leaves its archive (`backup.xbstream` +
`metadata.json`) in the object store. Set `spec.reclaimPolicy: Delete` on the
schedule to have every generated Backup inherit that policy; the Backup controller
then stamps the `mysql.cnmsql.co/cleanup-backup-files` finalizer and removes the
archive when a generated Backup is deleted. It defaults to `Retain`, keeping
deletion non-destructive. See
[Backup retention and deletion](backup-retention-deletion.md) for the full
reclaim-policy semantics, including reclaiming a whole Cluster's archive on
teardown.

## Immediate backup

When `spec.immediate` is true, the scheduler creates one Backup as soon as it
first reconciles the ScheduledBackup, in addition to the cron cadence.

The immediate path has an adoption guard. If the operator creates the immediate
Backup but restarts before patching status, the next reconcile finds the
existing immediate child Backup by label and adopts it instead of firing a
second one.

## Concurrency guard

cnmsql never overlaps backups for the same ScheduledBackup. Before evaluating
the next cron slot, the controller lists child Backups for that schedule. If any
child Backup is not done, meaning its phase is neither `completed` nor `failed`,
the scheduler requeues and waits.

This protects the cluster and object store from accidental backup pileups when a
backup takes longer than the schedule interval.

## Deterministic names

Scheduled backups use deterministic names:

```text
<scheduledbackup-name>-<YYYYMMDDHHMMSS>
```

The timestamp is the UTC scheduled time. Deterministic names make retries
idempotent: if a previous reconcile created the Backup but missed the status
update, the next reconcile observes and adopts the same child.

If another Backup already occupies the deterministic name and is not labelled as
owned by this ScheduledBackup, cnmsql refuses adoption, emits a warning Event,
skips that iteration, and resumes with the next schedule slot.

## Owner reference modes

`spec.backupOwnerReference` controls owner references on generated Backup
objects:

- `self`: the Backup is owned by the ScheduledBackup. Deleting the
  ScheduledBackup can garbage-collect its Backups.
- `cluster`: the Backup is owned by the referenced Cluster.
- `none`: the Backup has no owner reference and remains standalone.

Every generated Backup is labelled with:

```text
mysql.cnmsql.co/scheduled-backup=<scheduledbackup-name>
```

Immediate Backups also receive:

```text
mysql.cnmsql.co/immediate-backup=true
```

Labels are used for adoption and child lookup regardless of owner-reference
mode.

## Suspension

Set `spec.suspend: true` to pause scheduling:

```yaml
spec:
  suspend: true
```

Suspension prevents new Backup creation. It does not cancel a Backup that has
already been created, and it does not delete completed Backups.

## Status fields

`ScheduledBackup.status` records scheduler progress:

- `lastCheckTime`: the last time the schedule was evaluated.
- `lastScheduleTime`: the last scheduled time that produced or adopted a Backup.
- `nextScheduleTime`: the next expected cron slot.

The generated Backups carry the actual backup phase, Job name, checksum,
destination path, and error details.

## Retention

The scheduler currently creates Backup objects only. Retention GC is a separate
follow-on slice. Until retention is implemented, object-store lifecycle rules,
manual cleanup, or external automation must preserve the recovery window you
intend to keep.

For PITR, base-backup retention must be considered together with binlog archive
retention. Deleting a base backup can make older binlog segments unusable, and
deleting binlogs can shorten the recovery window even when base backups remain.

## Operational notes

- Choose a schedule interval longer than the normal backup duration to avoid
  constant concurrency deferral.
- Use `target: prefer-standby` when replicas are available and can carry backup
  load.
- Keep `immediate: true` for schedules where the first backup should exist
  without waiting for the first cron slot.
- Use `backupOwnerReference: none` if backups must survive deletion of the
  schedule object.
- Monitor both the ScheduledBackup status and the generated Backup statuses.

## Verification coverage

Unit tests cover schedule parsing, defaults, deterministic names, Backup field
propagation, suspended schedules, immediate creation and adoption, first-check
status stamping, due-slot creation, owner-reference modes, concurrency guarding,
and name-collision skips. The live Kind + MinIO e2e for scheduled backups is
planned with the same backup harness used by one-shot backups.
