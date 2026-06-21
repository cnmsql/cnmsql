# 006 — Switchover and Failover

**Status:** done
**Milestone:** M5

Add controlled primary changes to the M4 replicated topology: planned switchovers by promoting a healthy replica and demoting/rejoining the old primary, plus an automatic failover loop for an unreachable primary bounded by `spec.failoverDelay` and conservative RPO checks. Backup/PITR, quorum leasing, and advanced self-healing remain later milestones.

## Overview

M4 made `<cluster>-1` the fixed primary. M5 removes that assumption while preserving the same Pod/PVC-per-instance model and the same `rw`/`ro`/`r` Services. A primary change should be visible in `status.currentPrimary`, `status.targetPrimary`, `status.currentPrimaryTimestamp`, Pod role labels, and Events.

## Design

### Scope

#### In Scope

- **Planned switchover.** Promote a selected replica to primary and reconfigure the old primary as a replica of the new primary.
- **Manual trigger.** Model after CNPG's promote command by patching `status.targetPrimary`, `status.targetPrimaryTimestamp`, and phase fields. Use a small test/helper function for e2e ergonomics if needed. No kubectl plugin or CLI command required for M5.
- **Unsupervised primary update hook.** Use the same switchover machinery when a future primary update requires moving traffic off the primary. Actual rolling upgrades remain later, but the mechanism honours `primaryUpdateStrategy`/`primaryUpdateMethod` where already meaningful.
- **Automatic failover, conservative first pass.** If the current primary is unreachable for longer than `spec.failoverDelay`, choose the best healthy replica and promote it, provided RPO constraints are satisfied. `spec.failoverDelay=0` means fail over immediately (matching CNPG).
- **Candidate selection.** Prefer replicas that are ready, have replication threads healthy before failure, are closest to the primary's last known GTID, and are not being deleted. Break ties by ordinal/name for deterministic tests.
- **RPO/RTO guards:**
  - RPO: do not promote a replica whose GTID position is behind the best known primary/cluster position beyond what M5 can prove safe. M5 uses a conservative "GTID set equality or best known superset" check; richer GTID arithmetic can be refined later.
  - RTO: record elapsed operation time and fail/condition the operation when it exceeds `spec.maxSwitchoverDelay` or `spec.failoverDelay` expectations.
- **Fencing old primary.** During planned switchover, demote the old primary before promotion where reachable. During failover, delete or mark the old primary Pod unsafe before routing `rw` to the new primary.
- **Replica reconfiguration.** After promotion, configure all other instances to replicate from the new primary over the existing mTLS/TLS path.
- **Role labels and Services.** Update Pod role labels so `<cluster>-rw` points only at the new primary and `<cluster>-ro` points at non-primary replicas.
- **Status/events.** Add operation phases such as `Switchover`, `FailingOver`, and `Degraded`, set `targetPrimary`, update `currentPrimaryTimestamp`, and emit phase-transition Events.
- **Unit and e2e coverage.** Cover candidate selection, status transitions, service routing after primary change, old-primary fencing, planned switchover e2e, and automatic failover e2e.

#### Out of Scope

- Backup to object store, PITR, or timeline/binlog archive recovery (M6/M7).
- Declarative database/user reconciliation (M8).
- PodDisruptionBudget, richer health checks, and broader self-healing (M9).
- ProxySQL/pooler integration (M10).
- In-place major upgrades and primary rolling updates beyond providing the switchover primitive.
- Distributed quorum/lease-based fencing. M5 uses Kubernetes object ownership and Pod deletion/role-label gating.
- External replica clusters (`spec.replica`/`externalClusters`).

### Existing Building Blocks

- `replication.Manager.Promote` stops and resets replication, then clears `super_read_only`/`read_only`.
- `replication.Manager.Demote` sets `read_only`/`super_read_only`.
- `replication.Manager.EnsureReplicaConfigured` can point an instance at a new source and start replication.
- The instance manager already exposes `/promote`, `/demote`, `/status` and reports role/read-only/GTID state.
- M4 services route by Pod role labels, so traffic changes can be made by label changes once MySQL state is safe.
- `ClusterStatus` already has `currentPrimary`, `targetPrimary`, `currentPrimaryTimestamp`, and per-instance GTID status.

### API and Status Model

No new CRDs. The operator treats `status.targetPrimary != status.currentPrimary` as the operation request. Trigger by patching the Cluster status subresource:

1. `status.targetPrimary=<instance-name>`;
2. `status.targetPrimaryTimestamp=<now>`;
3. `status.phase=Switchover`;
4. `status.phaseReason="Switching over to <instance-name>"`.

The operator then:
1. Validates the target is a healthy replica;
2. Records an Event and drives the switchover;
3. Sets `currentPrimary=target`, updates `currentPrimaryTimestamp`, and normalizes `targetPrimary=currentPrimary` when complete.

Do not add an annotation trigger for M5 (CNPG does not use an annotation as the normal switchover trigger).

The same status fields are used internally by automatic failover: the controller first moves `targetPrimary` away from the failed primary, then waits for the candidate to become `currentPrimary`.

Potential new condition types:

- `PrimaryReady`: current primary is reachable and writable.
- `SwitchoverReady`: at least one safe promotion candidate exists.
- `FailoverBlocked`: automatic failover is blocked by RPO/fencing/candidate constraints.

### Reconciliation Design

1. Fetch cluster and observe all desired instances as M4 does, but include replica role/read-only/lag information in `observedCluster`.
2. Determine current primary from observed roles first, falling back to `status.currentPrimary`, then bootstrap ordinal only when no status exists.
3. If `status.targetPrimary` is set and differs from `currentPrimary`, enter a planned switchover:
   - validate target exists, is a replica, ready, not deleting, and passes RPO;
   - stop writes on old primary via `/demote`;
   - wait for target to catch up to old primary's last known GTID;
   - call `/promote` on target;
   - update role labels: target `primary`, old primary `replica`;
   - reconfigure old primary and other replicas to follow target;
   - patch status and emit success/failure Events.
4. If current primary is unreachable:
   - record first-unreachable time in status/conditions;
   - before `failoverDelay`, mark `Degraded` but do not promote;
   - after delay, choose best safe candidate;
   - fence old primary by deleting/withholding the old primary Pod before moving the `primary` role label;
   - promote candidate and reconfigure surviving replicas;
   - follow CNPG's model: the former primary Pod restarts/comes back, detects that it is no longer the primary, and rejoins as a replica of the promoted primary.
5. During normal reconcile, ensure non-primary instances follow `currentPrimary`, not ordinal 1.
6. Keep M4 scale-up/down semantics, but prevent scaling down the current primary.

### Candidate Selection

**Planned switchover:**
1. The target must be explicitly named.
2. The target must be observed as `RoleReplica`, `IsReady=true`, `Replication.IORunning=true`, `Replication.SQLRunning=true`.
3. The target's `GTIDExecuted` should equal or contain the old primary's last observed `GTIDExecuted`. If not, wait up to `maxSwitchoverDelay`.

**Automatic failover:**
1. Filter to ready replicas with healthy SQL state and no last replication error.
2. Prefer the highest/apparently most complete `GTIDExecuted` set.
3. If GTID sets are incomparable with the current simple parser, block failover rather than risk data loss.
4. Tie-break by lowest ordinal for deterministic behavior.

M5 needs a small GTID-set comparator for MySQL GTID intervals (`uuid:1-10,uuid2:1-3`) with unit tests, living in `pkg/management/mysql/replication`.

### Fencing Model

**Planned switchover:**
- Old primary is reachable, so demote it first.
- Only move the `primary` role label after the target is promoted and old primary is read-only.

**Automatic failover:**
- Old primary is unreachable, so assume its MySQL state is unknown.
- Remove its `primary` role label and delete the Pod before promoting a replica.
- Retain the old primary PVC.
- When the old primary comes back, follow CNPG-style rejoin: the Pod detects from Cluster status that it is no longer primary, starts read-only, and is configured as a replica of the promoted primary if its GTID state is compatible. If not compatible, block loudly instead of silently deleting the retained PVC.

## Implementation Notes

1. Add GTID set parsing/comparison helpers with unit tests.
2. Extend instance status parsing/JSON if needed to include executed/retrieved GTID and richer replication errors.
3. Extend `observedCluster` to track per-instance role, readiness, read-only, replication state, deletion, and GTID.
4. Refactor topology planning so `currentPrimary` is dynamic instead of ordinal 1; update pod args and source-host rendering to point replicas at the current primary.
5. Implement planned switchover state machine and tests with fake status client.
6. Implement replica reconfiguration after primary change.
7. Add an e2e helper that patches the status subresource with target-primary, timestamp, phase, and phase reason.
8. Implement failover detection delay, candidate selection, and conservative fencing.
9. Update status conditions/events and unit tests.
10. Add e2e planned switchover: write on old primary, request promotion, verify `rw` endpoint and writes on new primary, verify old primary rejoins as replica.
11. Add e2e failover: delete current primary Pod, wait for promotion, verify `rw` endpoint and replication health.

## Testing

- **Unit:**
  - GTID set parsing and contains/equality comparisons;
  - target-primary validation failures;
  - planned switchover happy path call order;
  - old-primary demotion failure blocks planned switchover;
  - failover waits for `failoverDelay`;
  - no safe candidate produces `FailoverBlocked`;
  - role-label/service routing after promotion;
  - scale-down never removes current primary.
- **Integration:** real two/three-container promotion and reconfiguration where practical, using the existing testcontainers harness.
- **E2E:**
  - three-instance planned switchover;
  - automatic failover by deleting/killing the primary Pod;
  - optional failure case where lag/RPO blocks failover.

## Decisions

- Do not create a cnmsql plugin or command in M5. Add a helper for e2e tests if useful.
- Treat `spec.failoverDelay=0` as immediate failover, matching CNPG.
- Handle former-primary recovery like CNPG: restart/rejoin it as a replica when compatible; block and surface the problem when it cannot safely rejoin.

## Verification

- User-triggered switchover through a CNPG-style status transition completes and `rw` routes only to the promoted instance.
- Old primary becomes a read-only replica following the promoted primary.
- Writes accepted after switchover replicate to the remaining replicas.
- Automatic failover promotes a safe replica after `failoverDelay` when the primary is unreachable.
- Failover is blocked, loudly and safely, when no RPO-safe candidate exists.
- `currentPrimary`, `targetPrimary`, `currentPrimaryTimestamp`, role labels, conditions, and Events accurately describe the operation.
- `make generate manifests`, `make lint-fix`, `make test`, integration, and M5 e2e pass.
