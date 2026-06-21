# 005 — Replicas, Primary Tracking & Traffic Routing

**Status:** done
**Milestone:** M4

Grow the M3 single-instance reconciler into a multi-instance cluster: provision GTID replicas cloned from the primary over an mTLS-gated XtraBackup stream, track which instance is primary, and expose the default `rw`/`ro`/`r` Services with role-based routing. No switchover or automated failover.

## Overview

M4 is the step from "one managed instance" to "a real replicated topology with stable client endpoints." The primary stays the bootstrap instance for the whole milestone; M4 builds and exercises the promote/demote/clone machinery that M5 will drive for switchover and failover.

## Design

### Scope

#### In Scope

- **Scale-up.** For `spec.instances == N`, create replica PVCs and Pods `<cluster>-2 .. <cluster>-N`, each joining from the current primary.
- **Replica provisioning by streamed XtraBackup.** XtraBackup can only back up a *local* datadir, so the backup must run on the primary and stream to the joining replica. Add a streaming backup endpoint to the instance manager control API (mTLS) on the primary; the replica's init container pulls the stream, extracts it, prepares it, copies it back, and configures GTID replication.
- **MySQL transport TLS + replication mTLS** (deferred from M3). Render mysqld `ssl_ca`/`ssl_cert`/`ssl_key`; replicas connect to the primary presenting their **per-instance server cert** as the replication client cert, so the existing `--replication-require-x509` grant from M3 is now enforced over TLS. Reuse the cert-manager CA from M3. `require_secure_transport` is left **unset** — TLS is available but not mandatory; the user can opt in via `spec.mysql.parameters`.
- **Primary tracking (not election).** The primary is deterministically the bootstrap instance `<cluster>-1`. The controller records `currentPrimary`, `targetPrimary`, `currentPrimaryTimestamp`, and keeps the `role` label on each Pod in sync with its replication role. Changing the primary (promote a replica) is explicitly deferred to M5; M4 refuses to move the primary automatically.
- **Default Services + role routing:**
  - `<cluster>-rw` → selects the Pod labelled `role=primary`.
  - `<cluster>-ro` → selects Pods labelled `role=replica`.
  - `<cluster>-r` → selects all ready instances (any role).
  Honour `spec.managed.services.disabledDefaultServices` (`rw`/`ro`/`r`). Keep the per-instance headless Services from M3 for stable DNS / `report_host`.
- **Semi-sync wiring.** Apply `spec.minSyncReplicas` / `spec.maxSyncReplicas` to the primary's semi-sync source config (the renderer already takes `WaitForReplicaCount`); install the semi-sync replica plugin on replicas.
- **Scale-down.** Remove the highest-ordinal replicas first; delete only the Pod and **retain the PVC** for the user to keep or delete. Never scale below 1 and never remove the current primary.
- **Status aggregation across instances:** `instances`, `readyInstances`, `instanceNames`, per-instance `role`, `gtidExecutedByInstance`, and a replica-health/lag summary derived from each instance manager `/status`.
- **Ordered, idempotent reconciliation:** add one replica at a time, only once the previous replica is streaming/healthy, to bound primary load.

#### Out of Scope

- Switchover (promote a chosen replica) and automated failover with RPO/RTO and quorum — M5.
- Backup to object store, `ScheduledBackup`, PITR — M6/M7.
- Declarative `Database`/users/config beyond bootstrap — M8.
- PodDisruptionBudget, monitoring/PodMonitor, guards, broader self-healing — M9.
- ProxySQL/pooler, binlog streaming, in-place major upgrades — M10.
- `spec.replica` (replica cluster following external source) and `externalClusters` — separate milestone.
- Webhooks — later hardening pass.

### Resource Model (Additions to M3)

| Resource | Name | Owner | Notes |
|----------|------|-------|-------|
| Pod | `<cluster>-2 .. -N` | Cluster | Replicas; `role=replica`. |
| PVC | `<cluster>-2 .. -N` | Cluster | One data volume per replica. |
| Service | `<cluster>-rw` | Cluster | Selector `role=primary`; client read-write endpoint. |
| Service | `<cluster>-ro` | Cluster | Selector `role=replica`; read-only endpoint. |
| Service | `<cluster>-r` | Cluster | Selector across all ready instances. |
| Service | `<cluster>-2 .. -N` | Cluster | Per-instance headless (as M3's `<cluster>-1`). |

Labels extend M3's set; `role` becomes dynamic (`primary` / `replica`) and is the selector key for `rw`/`ro`.

### Instance-Manager Additions

1. **Streaming backup endpoint** (primary side) on the **existing mTLS control port**, e.g. `GET /cluster/backup` → runs `xtrabackup --backup --stream=xbstream` (compression on by default) against the local datadir and streams stdout to the caller. Auth via the existing client cert; the XtraBackup connection uses a **dedicated `cnmsql_backup` user with `BACKUP_ADMIN`**, added to the M3 bootstrap.
2. **Manager-driven join over the stream.** Extend `instance join` (or a new `--source-manager-url`) so the replica pulls the stream from the primary's endpoint, runs `xbstream -x` into the backup dir, then the existing prepare → copy-back → `ProvisionFromBackup` path. The current file-based `--backup-dir` stays as the tested seam.
3. **mysqld TLS config keys** in `pkg/management/mysql/config` (`ssl_ca`, `ssl_cert`, `ssl_key`, optional `require_secure_transport`), version-gated as needed.
4. No new promote/demote code — M2 already ships `Promote`/`Demote`; M4 only wires the role labels, leaving the triggers to M5.

### Reconciliation Loop

1. Reconcile the primary exactly as M3 (its readiness gates replica creation).
2. Compute desired replicas from `spec.instances`; diff against owned Pods/PVCs.
3. Scale-up, one replica at a time: ensure PVC, then a Pod whose init container clones from the primary's streaming endpoint and configures replication; wait for it to report a healthy replica before adding the next.
4. Reconcile the three default Services (minus `disabledDefaultServices`) and the per-instance Services.
5. Keep each Pod's `role` label in sync with its observed replication role.
6. Scale-down: delete highest-ordinal replica Pods first; guard the primary and the floor of 1; **retain the PVC**.
7. Poll every instance manager `/status`; aggregate into cluster status.
8. Steady-state resync (reuse M3's `readyResync`) to refresh replica lag/GTID.

### Addendum — Replica-creation primary-health guard (post-M4)

The ordered scale-up above gates each new replica on the *previous* instance being
Ready. That alone is not enough: a replica is provisioned by **cloning from the
primary** (the streamed XtraBackup join), so a new replica must never be created
while the primary it would clone from is not OK. A clone from an unreachable or
not-yet-promoted primary fails, and seeding from a primary that is about to be
failed over risks diverging the fresh replica.

`reconcileInstances` therefore adds a guard before creating a replica
(ordinal > 1) whose Pod does **not yet exist**: it requires `primaryHealthy(observed)`
(the helper from [006](006-switchover-failover.md) — primary reachable, `IsReady`,
and reporting `RolePrimary`). If the primary is not healthy, the loop stops early
and the reconcile requeues; the new replica's Pod (and PVC) is not created. This
covers both bootstrap (replicas wait for the bootstrap primary to come up) and
later scale-up (a new replica waits for a degraded primary to recover).

Scope and interplay:

- **Only new replicas are gated.** Existing replica Pods are still reconciled
  normally (their data is already cloned), so a transient primary blip does not
  block routine reconciliation of a running cluster.
- **No failover deadlock.** Automatic failover runs *before* `reconcileInstances`.
  When an established primary fails, a healthy replica is promoted first, so the
  guard then sees a healthy primary. During initial bootstrap there is no replica
  to fail over to, and the failover path deliberately yields to provisioning —
  but cloning from a down primary would fail anyway, which is exactly what the
  guard prevents.

Tested by the unit test `TestReconcileInstancesGuardsReplicaOnUnhealthyPrimary`
(primary Pod Ready but control API reports non-primary ⇒ replica deferred) and the
`Replica creation guard` e2e specs (bootstrap ordering and scale-up while the
primary is unavailable).

## Testing

- **Unit (operator):** replica plan diff (scale up/down, ordinals), service selector generation + `disabledDefaultServices`, role-label sync, status aggregation with mixed ready/unready replicas, primary-immutability guard.
- **Unit (manager):** streaming endpoint produces a valid xbstream; join consumes a streamed backup and configures replication (sqlmock + a fake stream).
- **Integration (Docker, existing harness):** primary streams to a replica container, replica catches up via GTID — extend the M2 replication integration test.
- **E2E (Kind):** 3-instance cluster → `readyInstances=3`, write-on-primary visible via `<cluster>-ro`; scale to 1 converges; default Services resolve to the right Pods.

## Decisions

- Streaming transport: **xbstream over the existing mTLS control port** (no new port/sidecar).
- Backup identity: **dedicated `cnmsql_backup` user** (`BACKUP_ADMIN`), not a reused account.
- Replication client cert: **reuse the per-instance server cert** as the replication client cert (no separate cert issued).
- Scale-down: **retain replica PVCs** — delete only the Pod; the user decides whether to delete the leftover PVC. GC owned PVCs only on cluster delete.
- `require_secure_transport`: **not enforced** on mysqld. TLS material is configured and available, but the user decides whether to require it (exposed via `spec.mysql.parameters`).

## Verification

- A fresh `instances: 3` cluster reaches `Ready=True`, `readyInstances=3`, one `role=primary` and two `role=replica`, all sharing the primary's GTID history.
- `<cluster>-rw` routes only to the primary; `<cluster>-ro` only to replicas; `disabledDefaultServices` suppresses the named Services.
- Replica clone runs entirely over mTLS/TLS (stream endpoint + MySQL transport).
- Scale 3→1 removes replicas highest-ordinal-first without touching the primary.
- The primary never changes automatically (switchover/failover is M5).
- `make generate manifests`, `make lint`, `make test`, integration, and the M4 e2e pass.
