# 042 — Read-only protection on storage exhaustion

Status: accepted (2026-11-20)

Related: 015 (monitoring, self-healing, guards — `StoragePressure`), 007
(dynamic role), 022 (group replication), 026 (MariaDB support).

## Problem

When a data volume fills, mysqld cannot write: InnoDB transactions fail or
hang, and when the binlog shares the volume a failed binlog write shuts the
server down (`binlog_error_action=ABORT_SERVER` is the default). The
supervisor then restarts a writable server into a disk that is still full — a
crash loop, with every iteration a chance to corrupt data or leave a
half-written state that PITR cannot cleanly recover from.

CloudNativePG's answer for PostgreSQL is to refuse to (re)start the instance
when the WAL volume is full and to avoid failover, because PostgreSQL has no
runtime switch to stop accepting writes. MySQL does: `read_only` /
`super_read_only` are dynamic globals. We can keep the server serving reads
while blocking writes, which is strictly better than a hard stop.

## Decision

**Opt-in, off by default.**

```yaml
spec:
  storageProtection:
    readOnlyOnPressure: true      # default false
    readOnlyThresholdPercent: 90  # default 90, range 50–99
```

**Enforcement lives in the instance manager**, not the operator. Detection is
a local statfs the instance manager already takes for the storage metrics and
the `StoragePressure` condition; a wedged or partitioned operator must not
disable the protection; and no new control-API endpoint is needed. The
threshold reaches the Pod as `--read-only-protection-threshold` on the
instance-manager command line, so changing it rolls the cluster through the
normal Pod-update path.

The watchdog loop (every 10 seconds, plus one tick immediately after startup
before the control API starts serving) acts only on an instance that is the
**writable primary**: expected role primary, no configured replica state, not
fenced, not shutting down.

- usage ≥ threshold (or zero bytes available to an unprivileged writer) →
  `SET GLOBAL read_only=ON` and, when the engine supports it
  (`engine.Engine.HasSuperReadOnly`; MariaDB does not), `super_read_only=ON`;
  the engagement is remembered in memory.
- engaged and usage ≤ threshold − 5 points (fixed hysteresis, not
  configurable) → clear both flags.
- engaged and the instance stops being the writable primary → drop the
  engagement without touching the flags; demotion and promotion own them from
  then on.

Rationale for role gating: replicas are permanently read-only already and
fencing stops mysqld entirely, so the only writable surface a full disk can
corrupt is the primary. After an instance-manager restart the engagement
latch is rebuilt from the same facts, so a still-full disk re-engages on the
first tick (the residual write window is one tick after mysqld starts) and a
recovered disk never clears a flag the operator or a user set deliberately.

**The operator observes and reports; it does not command.** The instance
manager adds `readOnlyProtectionEngaged` to its `/status` document. The
operator aggregates it into a `StorageReadOnly` condition (true when any
instance is engaged, message naming them) with a `Warning` event on engage
and a `Normal` event on release — the same pattern as the `StoragePressure`
condition. No failover policy changes: a protected primary is healthy, so
automatic failover does not fire (writes pause, reads keep serving); a
protected instance is never a promotion candidate (candidates must be
writable); and manual promotion of a replica with free space remains the way
to restore writes before the volume is expanded. Data growth replicates, so
every replica is usually near-full too — a failover would buy minutes and
complicate recovery, the same reasoning CloudNativePG gives for avoiding
failover on a full WAL disk.

**Threshold semantics.** The ratio is used/capacity, consistent with the
storage metrics and the `StoragePressure` condition, with one exception:
engagement also fires when the space actually available to mysqld (statfs
`Bavail`, excluding root-reserved blocks) reaches zero, since reserved blocks
can make used% read below any chosen threshold while mysqld is already
getting ENOSPC. Release requires only the ratio hysteresis.

**Scope and known limits.** v1 protects the data volume only, matching the
`StoragePressure` signal; a full *binlog* volume on `spec.binlogStorage`
still trips `ABORT_SERVER` (follow-up: watch that volume in the same loop).
On MariaDB only `read_only` exists, so users holding SUPER can still write —
documented, not fixable. Statfs on emulated storage classes (Kind,
csi-hostpath) may not report real usage, so the feature cannot be covered by
e2e tests; correctness is pinned by unit tests with injected usage snapshots.

## Consequences

- `api/v1alpha1`: `StorageProtection` (+ CRD validation markers), CRD and
  API docs regenerated.
- `pkg/management/mysql/instance`: storage-protection watchdog (decision
  function + loop), `readOnlyProtectionEngaged` in the status document,
  `--read-only-protection-threshold` flag.
- `internal/controller`: flags on the instance-manager command line,
  `StorageReadOnly` condition + events.
- Docs: storage guide section, sample.
