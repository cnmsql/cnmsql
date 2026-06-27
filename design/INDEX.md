# CNMSQL Design Index

Quick-reference index of every design document. Use this to find relevant plans instead of scanning all files.

**Convention:** Documents are numbered sequentially (`NNN-title.md`). Status values: `proposed` (plan written, not yet approved/implemented), `done` (design finalized), `superseded` (replaced by a newer design).

## All Plans

| ID | Title | Status | Milestone | Description |
|----|-------|--------|-----------|-------------|
| 001 | [API Design](001-api-design.md) | done | M1 | CRD surface as Go types with kubebuilder markers. No controllers. Mirrors CNPG adapted to MySQL. |
| 002 | [Instance Manager](002-instance-manager.md) | done | M2 | In-Pod PID1 binary that supervises mysqld, bootstraps/joins instances, configures GTID replication, exposes mTLS control API. |
| 003 | [Custom Slim Instance Image](003-instance-image.md) | done | M2.5 | Debian+Percona APT image, rootless uid 1001, ~75% smaller than upstream. Multi-version matrix in `images/versions.json`. |
| 004 | [Cluster Reconciler Bootstrap](004-cluster-reconciler.md) | done | M3 | First operator-side `Cluster` reconciler: single initdb instance, owned PVC/Pod/secrets/TLS, mTLS `/status` polling, status conditions. |
| 005 | [Replicas, Primary Tracking & Routing](005-replicas-and-services.md) | done | M4 | Multi-instance cluster: N-1 GTID replicas cloned via mTLS XtraBackup stream, primary tracking, rw/ro/r role-routed services. |
| 006 | [Switchover and Failover](006-switchover-failover.md) | done | M5 | Planned switchover + automatic failover with GTID-set RPO checking, maxSwitchoverDelay RTO bound, old-primary fencing, errant-transaction guards. |
| 007 | [Dynamic Instance Role](007-dynamic-role.md) | done | M5.5 | CNPG pull-model: in-Pod reconciler watches Cluster and self-promotes/self-follows based on `status.targetPrimary`/`currentPrimary`. |
| 008 | [Physical Backup & Recovery](008-physical-backup-recovery.md) | done | M6 | `Backup` CRD → xbstream to S3-compatible object store, `instance restore`, `bootstrap.recovery.backup` for new clusters. |
| 009 | [Binlog Streaming](009-binlog-streaming.md) | done | M7 | Continuous MySQL binary-log archiving to S3 (gapless, GTID-addressable). PITR replay via `mysqlbinlog \| mysql`. |
| 010 | [Scheduled Backup](010-scheduled-backup.md) | done | M8 | Cron-scheduled `Backup` CR creation + base-backup retention GC with anchor-horizon binlog cleanup. |
| 011 | [Raw S3 Recovery](011-raw-s3-recovery.md) | done | M9 | Bootstrap new cluster directly from S3 bucket without `Backup` CR, using `BootstrapRecovery.Source` and `ExternalClusters`. |
| 012 | [User-Defined Exposition](012-user-defined-exposition.md) | done | M10 | Declarative `spec.managed.services` with templates and additional role-routed services, following CNPG `ManagedService` pattern. |
| 013 | [User-Managed TLS](013-user-managed-tls.md) | done | M11 | Independent per-field TLS cert overrides (`ServerCASecret`, `ServerTLSSecret`, `ClientCASecret`, `ReplicationTLSSecret`). |
| 014 | [Declarative Config/Users/Databases](014-declarative-config-users-databases.md) | done | M12 | Managed roles in Cluster spec + `Database` CRD controller. Operator-side reconciliation via instance manager SQL execution API. Config key denylist. |
| 015 | [Monitoring, Self-Healing, Guards](015-monitoring-self-healing-guards.md) | done | M13 | Prometheus exporter + PodMonitor, PDBs, semi-sync self-healing, fencing annotation, deletion guard, liveness isolation. |
| 016 | [kubectl cnmsql CLI Plugin](016-kubectl-plugin.md) | done | M16 | Cobra-based kubectl plugin: status, logs, mysql shell, promote, fence, backup, bench, metrics, user/database CRUD, diagnostics. |
| 017 | [Primary Lease Fencing](017-primary-lease.md) | done | M13.4 | Per-cluster Lease object the acting primary must hold before accepting writes. Split-brain guard for async replication failover. |
| 018 | [Manager Binary Injection](018-bootstrap-dbs.md) | done | M18 | Bootstrap-controller init container copies `/manager` from operator image into shared EmptyDir. Removes manager binary from instance image. |
| 019 | [Operator Upgrades](019-operator-upgrade.md) | done | — | Rolling + in-place instance-manager upgrades. Spike-proven re-exec keeps mysqld alive. PID-based `DetachedSupervisor` + adopt mode. |
| 020 | [Status Instance Webhook](020-status-instance-webhook.md) | done | — | Per-instance ServiceAccount identity + validating webhook to enforce least-privilege updates to `status.currentPrimary`. |
| 021 | [Deployment Modes](021-deployment-modes.md) | done | — | Cluster-wide vs namespaced operator topologies. `WATCH_NAMESPACE`-scoped cache, namespaced RBAC overlay, and per-namespace webhook (unique name + namespaceSelector) so multiple operators cohabit one cluster. |
| 023 | [DatabaseUser CR](023-database-user-cr.md) | done | M-DBU | Standalone `DatabaseUser` CRD: installation-wide MySQL account (not scoped to a Database) with grants, password rotation, reclaim policy, and conflict/adoption of pre-existing accounts. Inline `Database` user struct renamed to `InlineUser`. Grant denylist + safe-DBaaS-superuser recipe + declarative `kubectl cnmsql databaseuser` commands. |
| 022 | [Group Replication](022-group-replication.md) | done | M-GR | Second replication topology: quorum-based MySQL Group Replication behind an immutable `spec.replication.mode`. Operator observes group decisions (auto-failover handled by quorum); switchover via `set_as_primary`; GR-native fencing; quorum guards; phased M-GR.1–M-GR.7. |
| 024 | [MySQL Major Version Upgrade](024-major-version-upgrade.md) | done | — | Safe orchestrated server upgrades along `8.0 → 8.4 → 9.x`. Catalog keyed by **series** not integer major (8.0≠8.4), admission guard (no downgrade/skip), config gating for removed sysvars/auth defaults, per-instance upgrade-complete signal, backup-gated replica-first rollout, GR comm-protocol finalization. |
| 025 | [E2E Testing Overhaul](025-e2e-testing-overhaul.md) | proposed | M-E2E | Make the e2e suite rock solid on a single 8 vCPU/16 GiB runner: label-based test tiering (`core`/`feature`/`flavor`/`disruptive`/`heavy`/`flaky`), resource-budgeted execution partitions, per-spec **ephemeral Kind clusters** for operator-mutating specs (kills the shared-operator flake class + the transient-webhook retry machinery), deterministic fail-fast readiness (retire blanket 20m waits), build-outside-suite + ldflags hash (no `cmd/main.go` mutation), one-command `./hack/e2e.sh --focus/--k8s/--mysql/--tier`, CI lanes + runner hygiene + JUnit/must-gather artifacts, and flake governance. |

## Quick Navigation by Topic

**Cluster Lifecycle:** 003 (image) → 004 (single instance) → 005 (replicas) → 012 (services)

**Replication & HA:** 006 (switchover/failover) → 007 (dynamic role) → 017 (primary lease) → 022 (group replication)

**Status Authorization & Security:** 020 (status authz webhook)

**Deployment Topology:** 021 (cluster-wide vs namespaced)

**Data Management:** 008 (backup/recovery) → 009 (binlog/PITR) → 010 (scheduled backup) → 011 (raw S3 recovery)

**Operator Internals:** 018 (binary injection) → 019 (operator upgrades) → 024 (MySQL major upgrade) → 015 (monitoring/guards)

**Testing & CI:** 025 (e2e testing overhaul)

**User-Facing APIs:** 001 (CRD types) → 013 (TLS) → 014 (users/databases) → 023 (databaseuser) → 016 (kubectl plugin)

## Superseded Documents

*None yet. When a plan is superseded, it moves here and its status in the main table changes to `superseded`.*

## Superseding Convention

When a design document replaces an existing one:

1. **Create a new document** with the next available number (e.g. `020-operator-upgrades-v2.md`).
2. **Make it self-contained** — copy all relevant context, decisions, and implementation notes from the old document into the new one. A reader must never need to open the superseded document to understand the new design.
3. **Add a note at the top** of the new document:
   ```
   > Supersedes [019-operator-upgrade.md](019-operator-upgrade.md) — this document
   > incorporates and replaces all content from the original.
   ```
4. **Add a banner at the top** of the old document:
   ```
   > **Superseded by [020-operator-upgrades-v2.md](020-operator-upgrades-v2.md)** —
   > this document is kept for historical reference only. Use the new document for
   > implementation.
   ```
5. **Update this index** — change the old document's status to `superseded` and update its description to `Superseded by [020-title](020-title.md)`. Add the new document to the main table with `Status: in-progress` or `done`.
6. **Update the Quick Navigation** sections to use the new document reference.
