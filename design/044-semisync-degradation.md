# 044 — Observing and bounding semi-sync degradation

Status: accepted (2026-11-25)

Related: issue #285, 015 (self-healing: semi-sync ack-count reconciliation),
042 (storage-full read-only watchdog, whose pattern this reuses), 026
(MariaDB support).

## Problem

MySQL and MariaDB semi-synchronous replication fall back to asynchronous
replication without telling anyone:

1. **Timeout fallback.** When no acknowledgement arrives within
   `rpl_semi_sync_*_timeout`, the live status flag
   (`Rpl_semi_sync_source_status` / `Rpl_semi_sync_master_status`) flips OFF
   and writes continue asynchronously.
2. **Silent re-enable.** Once a replica catches up the flag flips back ON, and
   nothing records that there was an unprotected window.

The operator's status read only the *configuration* flag
(`rpl_semi_sync_*_enabled`), so the Cluster said semi-sync was on while the
primary was running asynchronously.

### `wait_no_slave`: the issue had it backwards

Issue #285 proposed setting `rpl_semi_sync_master_wait_no_slave=OFF` in
`required` mode. Both servers document the opposite:

- `ON` (the default): the replica count may drop to zero and the primary
  still blocks each commit for the full timeout before falling back.
- `OFF`: the primary reverts to asynchronous replication **as soon as** the
  replica count drops below the wait count.

(MySQL 8.0.26+ spells it `rpl_semi_sync_source_wait_no_replica`. MariaDB keeps
`rpl_semi_sync_master_wait_no_slave`.) So the default is already the durable
choice. The actual hole is a user setting it `OFF` through
`spec.mysql.parameters`, which nothing prevented.

Neither value removes the timeout: even with `ON`, a `required` primary stops
waiting after `timeoutMillis`. MySQL has no setting for "never degrade".

## Decision

### 1. Observe the live flag (both modes, both engines)

The instance manager runs a **semi-sync monitor** loop (every 2 seconds, plus
one pass at startup) while semi-sync is enabled. On the writable primary (the
same gate as design 042: expected role primary, not fenced, not shutting
down, `status.targetPrimary` names it) it reads:

- the live flag `Rpl_semi_sync_{source,master}_status`,
- the connected semi-sync replica count `Rpl_semi_sync_{source,master}_clients`,

and remembers when the flag last went OFF. Every transition is logged in both
directions (`Semi-sync fell back to asynchronous replication` /
`Semi-sync resumed`), so even a window shorter than an operator resync leaves
a record.

`/status` gains `semiSync.sourceActive`, `semiSync.sourceClients` and
`semiSync.inactiveSeconds` (how long the flag has been OFF, measured on the
instance's own clock, so operator/instance clock skew does not matter).

The operator raises **`SemiSyncDegraded`** on the Cluster while the primary
reports the flag OFF for at least `spec.mysql.semiSync.degradedAfterSeconds`
(default 10, `0` reports immediately). The grace period is the hysteresis: the
flag flips on the first failed wait, so a short ack gap would otherwise become
an incident. It clears as soon as the flag is back ON. Transitions emit a
`Warning SemiSyncDegraded` and a `Normal SemiSyncRestored` event. The
condition exists only for async clusters with semi-sync enabled, and only once
the primary has reported the live flag (an older instance manager does not),
so it never shows a false `False`. Since the operator evaluates it on its
resync, it can be reported up to one resync (30 s) after the grace period
ends. The instance-manager log has the exact timestamps.

### 2. Close the `wait_no_slave` hole in `required` mode

In `required` mode:

- the renderer pins `loose-<wait_no var>=ON` in my.cnf,
- the monitor re-asserts it with `SET GLOBAL` whenever it reads `OFF` (on every
  instance, so a promoted replica is already correct), which repairs a manual
  `SET GLOBAL` too,
- the webhook rejects any `spec.mysql.parameters` key that names the variable
  (either spelling, `loose-`/modifier prefixes and unique abbreviations
  included), because the operator owns it in that mode.

`preferred` mode leaves the server default (`ON`) alone and still accepts the
parameter. Its ack-count self-heal already trades durability for availability
on purpose. A user who wants immediate fallback instead of one timeout stall
can set `OFF` there. This is documented.

### 3. Opt-in fencing in `required` mode

`spec.mysql.semiSync.readOnlyAfterDegradedSeconds` (≥ 1, `required` mode only,
unset by default). It is **the** answer to "`required` is still bounded by the
timeout": when the live flag has been OFF on the writable primary for that
long, the monitor puts the primary into read-only mode (`read_only`, plus
`super_read_only` where the engine has it) and reports
`semiSyncReadOnlyEngaged`. Writes stall instead of continuing asynchronously.
The worst-case unprotected window is `timeoutMillis` (the commit that timed
out) plus `readOnlyAfterDegradedSeconds` plus one 2 s tick.

Release: the fence is released when the live flag is back ON, **or** when at
least the required number of semi-sync replicas is connected
(`Rpl_semi_sync_*_clients` ≥ the live `wait_for_replica_count`, or 1 on
MariaDB). The second condition is needed. The server only switches back
to semi-sync when a replica acknowledges a position at or past the last
commit, and a read-only primary does not commit, so waiting for the flag
alone could deadlock. After a clients-based release the grace period
restarts from the release time. If the next commit still gets no ack, the
primary goes read-only again after another `readOnlyAfterDegradedSeconds`
instead of flapping on every tick.

Like design 042, enforcement lives in the instance manager (a wedged or
partitioned operator cannot disable it). The engagement is an in-memory latch
that is rebuilt from live facts after a manager restart. The two watchdogs
share the read-only flags, and neither clears them while the other is
engaged. Releasing one latch while the other holds just drops the latch. The
role reconciler holds the lease for a fenced primary instead of
"promoting" over the fence, exactly as for storage protection. Failover
policy does not change: a fenced primary is healthy and keeps serving reads.

The `SemiSyncDegraded` condition message says when writes are paused, and a
`Warning SemiSyncReadOnly` event marks the fence engaging.

The webhook warns (it does not reject) when `required` mode is set without
`readOnlyAfterDegradedSeconds`, since such a cluster still degrades to async
after `timeoutMillis`.

### Failover interaction

No change. A primary that degraded before dying makes
`status.gtidExecutedByInstance` a lower bound, which the failover bound
already treats conservatively. `SemiSyncDegraded` is reported, not fed into
the failover policy.

### MariaDB

Same live flag (`Rpl_semi_sync_master_status`), clients count
(`Rpl_semi_sync_master_clients`) and `rpl_semi_sync_master_wait_no_slave`.
The release threshold is one client. All three parts apply.

## Consequences

- `api/v1alpha1`: `semiSync.degradedAfterSeconds`,
  `semiSync.readOnlyAfterDegradedSeconds`, validation and warning,
  `ConditionSemiSyncDegraded`. CRDs, API docs and `dist/install.yaml`
  regenerated.
- `pkg/management/mysql/version` + `pkg/engine`: status, clients and
  wait-no-replica variable names per engine/version.
- `pkg/management/mysql/replication`: live status reads,
  `SetSemiSyncWaitNoReplica`.
- `pkg/management/mysql/config`: pinned render, `IsSemiSyncWaitNoReplicaKey`.
- `pkg/management/mysql/instance`: semi-sync monitor (decision function +
  loop), shared read-only ownership with the storage watchdog, new `/status`
  fields; role reconciler holds a fenced primary.
- `internal/controller`: `SemiSyncDegraded` condition + events.
- e2e: semi-sync degradation (all replicas fenced → condition True → replicas
  back → False) and the required-mode fence (writes refused while degraded).
- Docs: replication/failover guide section.
