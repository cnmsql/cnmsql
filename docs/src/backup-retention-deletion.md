---
title: "Backup Retention and Deletion"
description: "Current Backup deletion behavior, ScheduledBackup owner references, object-store cleanup, and planned retention GC."
sidebar_position: 12
---

# Backup retention and deletion

cloudnative-mysql currently separates Kubernetes object lifecycle from object-store
artifact lifecycle. This is deliberate: a Kubernetes object deletion should not
silently destroy the only copy of a recovery point unless the user explicitly
opts into that behavior.

## What happens today

Deleting a `Backup` object does not delete remote S3-compatible objects.

The remote objects remain under:

```text
<path>/<cluster>/<backup-name>/<backup-id>/backup.xbstream
<path>/<cluster>/<backup-name>/<backup-id>/metadata.json
```

The Kubernetes Job is owned by the Backup, so Kubernetes garbage collection may
remove the Job. Object-store artifacts are not owned by Kubernetes and are not
removed.

## ScheduledBackup owner references

`ScheduledBackup.spec.backupOwnerReference` controls only Kubernetes owner
references on generated Backup objects:

- `self`: generated Backups are owned by the ScheduledBackup.
- `cluster`: generated Backups are owned by the Cluster.
- `none`: generated Backups are standalone.

These modes do not change S3 deletion behavior. If a generated Backup is
garbage-collected, its remote objects still remain.

## Why remote cleanup is not automatic yet

Remote backup cleanup is data-destructive. A Backup object may be deleted
accidentally, by namespace cleanup, by owner-reference cascade, or by a GitOps
prune. Automatically deleting `backup.xbstream` and `metadata.json` in those
cases could destroy the recovery window.

cloudnative-mysql therefore needs an explicit policy before adding remote deletion.

## Planned finalizer behavior

A future Backup finalizer can support remote cleanup. The intended shape is:

1. Add a finalizer to Backups that opt into remote cleanup.
2. On Backup deletion, resolve the recorded bucket/key from Backup status.
3. Delete `backup.xbstream` and `metadata.json`.
4. Record or surface failures so deletion does not silently leave half-cleaned
   state.
5. Remove the finalizer only after cleanup succeeds or the user explicitly
   bypasses it.

This should likely be opt-in at the Backup, ScheduledBackup, or Cluster backup
policy level.

## Retention GC

Set `spec.backup.retentionPolicy` on a Cluster to have the operator expire old
archives automatically. The value is a time window: `<n>d`, `<n>w`, or `<n>m`
(days, weeks, months, where a month is 30 days):

```yaml
spec:
  backup:
    retentionPolicy: 30d
    objectStore:
      bucket: my-backups
      # ...
```

A `retentionPolicy` requires an object store; setting one without
`spec.backup.objectStore` is rejected by validation.

### What gets deleted

The operator runs a throttled retention pass (at most once per hour, tracked in
`status.lastRetentionRunTime`) on clusters that have a policy, an object store,
and an established primary. Each pass:

1. **Expires old base backups.** A base backup is deletable when its
   `completedAt` is older than `now - window`. Its whole archive directory
   (`backup.xbstream` + `metadata.json`) is removed.
2. **Always keeps the newest base backup** as a floor, even if it is older than
   the window. A cluster must always have something to recover from. So the
   deletable set is `{expired} \ {newest}`.
3. **Expires uncoverable binlog segments.** The PITR horizon is the oldest
   *retained* base backup's start time. Binlog segments whose last event predates
   that horizon can no longer be replayed onto any retained base, so they are
   deleted and `_index.json` is rewritten to match.

Binlog GC is conservative: a segment with an unknown (zero) last-event time is
kept rather than risk shortening the PITR window. The index is rewritten **last**,
so a mid-run failure leaves a still-valid index and orphans are cleaned on the
next pass.

A successful pass that deletes anything emits a Normal `BackupRetention` event.
Transient object-store errors requeue the reconcile rather than corrupting the
archive.

### What it does not do

- It does **not** delete `Backup` Kubernetes objects, only object-store
  artifacts. Pruning expired `Backup` CRs stays the scheduler/owner-ref's job.
- Count-based retention (keep N backups) is not supported; the policy is purely
  time-based.

## What operators should do now

- Use object-store lifecycle rules carefully, and align them with the recovery
  window you need.
- Keep Backup objects for important restore points so their status remains easy
  to inspect.
- Preserve both `backup.xbstream` and `metadata.json`.
- Test recovery before deleting old prefixes manually.
- Document any external cleanup automation outside cloudnative-mysql.

## Manual cleanup checklist

Before deleting remote backup data:

- Confirm no Cluster uses `bootstrap.recovery.backup` or
  `bootstrap.recovery.source` for that Backup.
- Confirm no runbook references the backup ID.
- Confirm a newer base backup exists and is restorable.
- Confirm PITR archive coverage still satisfies the required recovery window.
- Delete both the archive and metadata object together.
