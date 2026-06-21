# 010 — Scheduled Backup

**Status:** done
**Milestone:** M8

Cron-scheduled `Backup` CR creation, base-backup retention GC with anchor-horizon binlog cleanup, reusing the M6 data path unchanged.

## Goal

Take physical backups on a recurring cron schedule, mirroring CNPG's
`ScheduledBackup`. M6 already gives us the one-shot `Backup` CRD + `BackupReconciler`
(creates a worker Job, streams xbstream → S3, writes status). M8 adds a controller
that **creates `Backup` objects on a schedule**, plus the helpers/validation the
`ScheduledBackup` type needs. No changes to the backup data path itself.

The `ScheduledBackup` Go type already exists (scaffolded in M1, `api/v1alpha1/scheduledbackup_types.go`):
`spec.schedule` (6-field cron w/ seconds), `cluster`, `suspend`, `immediate`,
`backupOwnerReference` (none|self|cluster), `method`, `target`, `online`; status
has `lastCheckTime`/`lastScheduleTime`/`nextScheduleTime`. No funcs, no controller,
not wired into `cmd/main.go`. M8 fills those in.

## Scope

### In (M8.1 — the scheduler)

1. **API helpers + validation** (`api/v1alpha1/scheduledbackup_funcs.go`, new):
   - `IsSuspended()`, `IsImmediate()`, `GetSchedule()` (mirror CNPG dereference-with-default).
   - `BackupName(t time.Time) string` — deterministic `<sb-name>-<compactISO8601(t)>`
     so reconcile retries don't duplicate Backups.
   - `CreateBackup(name string) *Backup` — builds a `Backup` from the SB's
     method/target/online (objectStore inherited from the Cluster, as today).
   - `SetDefaults()` for the SB (method=xtrabackup, target=prefer-standby, online=true,
     ownerRef=self) to match the Backup defaults.
   - Validation: cron parse check on `schedule` (reject invalid up front). Driver-free —
     parse with the chosen cron lib in a small validating helper, called from a test +
     (optionally) a future webhook. No webhook exists yet in this project, so validation
     is surfaced by the controller (Event + no requeue) like CNPG's `cron.Parse` error path.

2. **Cron dependency**: add `github.com/robfig/cron/v3`. The type doc says "6 fields,
   including seconds", so use `cron.NewParser(cron.Second | cron.Minute | cron.Hour |
   cron.Dom | cron.Month | cron.Dow)` (v3 omits seconds by default). One small wrapper
   `parseSchedule(string) (cron.Schedule, error)` shared by controller + validation.

3. **`ScheduledBackupReconciler`** (`internal/controller/scheduledbackup_controller.go`, new) —
   port CNPG's logic, trimmed (no VolumeSnapshot branch; method must be xtrabackup):
   - Skip when suspended.
   - **Concurrency guard**: list child Backups (parent label, via field indexer); if any
     is not done (`phase` ∉ {completed, failed}), requeue 60s — never overlap backups.
   - Parse schedule; on error emit `InvalidSchedule` Warning, no requeue.
   - **Immediate** (first reconcile, `lastCheckTime==nil`, `immediate==true`): create a
     Backup now, labeled `immediate=true`; operator-restart guard adopts a pre-existing
     immediate Backup (list by parent+immediate label) instead of double-firing.
   - **First check** (`lastCheckTime==nil`, not immediate): stamp `lastCheckTime`, requeue
     until `schedule.Next(now)`.
   - **Steady state**: `next = schedule.Next(lastCheckTime)`; if `now < next` requeue until
     then; else Get-first the deterministic Backup name → create if NotFound, adopt if it's
     ours, skip-iteration on a non-owned name collision (CNPG's `skipIterationOnNameCollision`).
   - `advanceScheduledBackupStatus` updates `lastCheckTime`/`lastScheduleTime`/
     `nextScheduleTime` and requeues for the next slot; conflict → short requeue.
   - Owner reference per `backupOwnerReference` (none|self|cluster). `self` = owned by the SB
     (so deleting the SB GCs its Backups via cascade); `cluster` = owned by the Cluster;
     `none` = standalone.

4. **Labels + indexer** (constants in `internal/controller`):
   - `parentScheduledBackupLabel = "mysql.cnmsql.co/scheduled-backup"`
   - `immediateBackupLabel = "mysql.cnmsql.co/immediate-backup"`
   - Field indexer on the parent label over `Backup` for efficient child lookup, registered
     in `SetupWithManager` (matches CNPG).

5. **Wiring**: register `ScheduledBackupReconciler` in `cmd/main.go`; RBAC markers
   (`scheduledbackups` get/list/watch/update/patch + `/status`; `backups`
   get/list/watch/create; events). `make manifests` to regenerate role + (already-present) CRD.

6. **Tests**:
   - API unit: defaults, `BackupName` determinism, `CreateBackup` field propagation,
     schedule validation (valid 6-field, reject 5-field/garbage).
   - Controller unit (envtest/fake like `backup_controller_test.go`): suspended → no-op;
     immediate creates one Backup + adopts on retry; first-check stamps lastCheckTime;
     due slot creates the deterministically-named Backup with parent label + owner ref per
     mode; concurrency guard requeues while a child runs; name-collision skip.
   - E2E (`test/e2e`): a ScheduledBackup with `immediate: true` + a tight schedule against a
     running cluster + MinIO produces ≥1 completed Backup with the parent label, and a second
     slot fires. Reuse the M6 MinIO harness. (Gated like the other heavy e2e specs.)

### Out (proposed M8.2 — retention, separate slice; see Decision below)

PLAN-009 parks "base-backup retention GC tied to binlog retention windows" in M8 territory.
That's a distinct mechanism (a retention policy on `spec.backup`, plus a GC pass that deletes
expired base-backup archives **and** the now-uncoverable binlog segments from the object
store, guarding the PITR window). Recommend landing the scheduler first (M8.1) and doing
retention as M8.2 once schedules exist to produce backups worth expiring.

## Decisions (confirmed 2026-06-13)

- **D-M8a Retention scope.** Ship the scheduler as **M8.1 now**; base-backup retention GC
  (expire old archives + uncoverable binlog segments, guarding the PITR window) is a separate
  **M8.2** follow-on slice.
- **D-M8b Cron lib.** `github.com/robfig/cron/v3` with the seconds-enabled parser
  (`cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)`).

## M8.2 — Base-backup retention GC (planned)

Enforce `spec.backup.retentionPolicy` (the `^[1-9][0-9]*[dwm]$` field that already
exists on `BackupConfiguration` but is currently dead). Expire old base backups
**and** the now-uncoverable binlog segments, guarding the PITR window — never
leave binlogs with no base to apply them onto, and never delete the data needed
to recover within the policy window.

### Model (CNPG-parity, adapted to GTID)

- **Time-based.** `cutoff = now - window` (`d`=24h, `w`=7d, `m`=30d, matching
  CNPG's barman semantics). A base backup expires when its `CompletedAt < cutoff`.
- **Floor: always keep the most recent base backup**, even if older than the
  window — a cluster must always have something to recover from. So the set of
  deletable base backups is `{expired} \ {newest}`.
- **PITR horizon = the oldest *retained* base backup.** Its anchor
  (`xtrabackup_binlog_info` start, surfaced as the backup's `BeginGTID` /
  `StartedAt`) is the earliest point we can still replay forward from. Any binlog
  segment that ends entirely *before* that anchor is uncoverable → delete it.
- Deletes are object-store side (the archive lives in S3, not in K8s). The
  `Backup` CR is a separate concern: by D-docs, deleting a `Backup` object does
  not delete S3 data, and vice-versa. M8.2 GCs the **archive**; pruning expired
  `Backup` CRs (and their owned children) stays the scheduler/owner-ref's job.

### In

1. **API helper** (`api/v1alpha1`): `ParseRetentionPolicy(string) (time.Duration, error)`
   (`d`/`w`/`m` → duration), reused by validation and the GC. No new spec fields;
   `retentionPolicy` already exists. Optional `status` surface:
   `status.lastRetentionRunTime *metav1.Time` so the run is throttled and visible.

2. **Object-store primitives** (`pkg/.../objectstore/client.go`):
   - `Remove(ctx, bucket, key)` — single object delete.
   - `RemovePrefix(ctx, bucket, prefix)` — delete every object under a backup
     directory (`<cluster>/<backup>/<id>/`), used to drop an expired base backup
     wholesale (archive + metadata).
   - `ListObjects(ctx, bucket, prefix)` → `[]ObjectInfo{Key, Size, LastModified}`
     (non-recursive common-prefix listing for backup dirs; recursive for binlogs).

3. **Retention engine** (`pkg/.../objectstore/retention.go`, pure + unit-tested):
   - `ListBaseBackups(ctx, client, store, cluster) ([]BackupMetadata, error)` —
     walk `ClusterPrefix`, read each `metadata.json`.
   - `PlanRetention(backups []BackupMetadata, binlogIndex ArchiveIndex, cutoff time.Time)
     RetentionPlan` — pure decision function: which base-backup prefixes to delete,
     which binlog segments/files to delete (segment fully older than the oldest
     retained anchor), and the rewritten `ArchiveIndex`. Keeps-newest floor and
     anchor-horizon logic live here so they're table-test-driven.
   - `ApplyRetention(ctx, client, store, plan)` — execute the deletes, then
     rewrite `_index.json`. Index rewrite happens **last** so a mid-run failure
     leaves a still-valid (if stale-pointing) index; orphaned objects get cleaned
     on the next pass rather than corrupting discovery.

4. **Wiring** (`internal/controller/cluster_retention.go`): a `reconcileRetention`
   pass called from the cluster controller steady state, gated to clusters with
   `retentionPolicy != ""`, an object store, and an established primary; throttled
   via `status.lastRetentionRunTime` (default once/hour, const
   `retentionInterval`). Reuses `objectStoreConfig`/`NewClient` from the guard.
   Emits a Normal `BackupRetention` event summarizing deletions; transient store
   errors requeue without failing the reconcile. RBAC already covers status
   patch; no new verbs (object-store deletes are not K8s API).

5. **Tests**:
   - Unit: `ParseRetentionPolicy` (d/w/m, reject garbage); `PlanRetention` table
     tests (expired deleted, newest kept as floor, binlog segment older than
     anchor deleted, segment straddling anchor kept, index rewrite correctness);
     httptest-S3 `ListBaseBackups`/`ApplyRetention` round-trip (mirrors the
     existing backup-guard httptest pattern); controller throttle + gating test.
   - E2E (gated, reuse M6/M8 MinIO harness): a cluster with a tight
     `retentionPolicy` and several backups + archived binlogs prunes the expired
     base backups and pre-anchor binlogs while keeping the newest backup and the
     binlogs needed to cover it; PITR to a still-in-window target still succeeds.

### Out / deferred

- Count-based retention (`keep N backups`) — CNPG has it; not in the
  `^...[dwm]$` field. Add later if asked.
- A backup-deletion finalizer that deletes S3 data when a `Backup` CR is removed
  (documented as future work). Orthogonal to time-based archive GC.

### Acceptance criteria (M8.2)

- Expired base backups (older than the window) are deleted from the object store,
  except the most recent, which is always retained.
- Binlog segments wholly older than the oldest retained base backup's anchor are
  deleted; `_index.json` is rewritten to match; no in-window PITR target becomes
  unsatisfiable.
- The pass is throttled, idempotent, and surfaced via status + events; transient
  store failures requeue rather than corrupt the archive. Unit + e2e green, lint
  clean, CRD drift only for the new status field.

## Acceptance criteria (M8.1)

- A ScheduledBackup creates Backups on its cron cadence; `immediate` fires once at creation.
- Never overlaps: a new slot is skipped/deferred while a prior child Backup is still running.
- Retries/operator restarts don't duplicate Backups (deterministic names + adoption).
- `backupOwnerReference` honored; `self` cascades deletion.
- Status reflects last/next schedule times. Unit + e2e green, lint clean, no CRD drift.
