# 022 — Group Replication Support

**Status:** proposed
**Milestone:** M-GR (proposed, phased M-GR.1 … M-GR.7)

Add MySQL **Group Replication (GR)** as a second replication topology alongside the
existing async / semi-sync GTID primary-replica topology (decision D4, "Group
Replication deferred"). GR is a Paxos-based, quorum-driven, virtually-synchronous
replication layer: the *group itself* owns membership, write certification, and
primary election. This inverts several of the operator's current
responsibilities — most importantly **the operator stops electing primaries and
becomes an observer of the group's decisions**.

This is a deep, safety-critical change. The plan is explicitly phased so the
proven async path is never regressed, and so the dangerous operations (group
bootstrap, quorum recovery) land last, behind strong guards and heavy testing.

## Overview

Today (`design/004`–`007`, `017`) the operator runs an async/semi-sync topology:

- The operator **elects** the primary by writing `status.targetPrimary`; each
  instance's in-Pod reconciler (`pkg/management/mysql/instance/rolereconciler`)
  self-promotes or follows `status.currentPrimary` (CNPG pull-model).
- Automatic failover (`internal/controller/cluster_failover.go`) detects an
  unreachable primary, selects a candidate by **GTID dominance**, fences the old
  primary by deleting its Pod, and repoints `targetPrimary`.
- A per-cluster **primary Lease** (`design/017`) is the split-brain guard.
- **Fencing** stops mysqld while keeping the manager alive and flips
  `routable=false`.
- **Switchover** validates a target, then the in-Pod reconcilers do the
  stop-replica / promote / demote dance.
- **Semi-sync** self-healing (`cluster_semisync.go`) tunes the ack count.

Under Group Replication, the group provides quorum-based consensus. The operator
no longer makes HA decisions; it **declares desired membership** and **reflects**
what the group reports. Concretely:

| Concern | Async (today) | Group Replication (this plan) |
|---|---|---|
| Primary election on failure | Operator selects candidate, fences, repoints `targetPrimary` | **Group auto-elects**; operator only observes & mirrors into `currentPrimary` |
| Switchover | Operator + in-Pod stop/promote/demote | `group_replication_set_as_primary(uuid)` UDF; operator observes result |
| Split-brain guard | Primary Lease + GTID divergence checks | **Quorum** (majority consensus); Lease disabled |
| Fencing | Stop mysqld, `routable=false` | `STOP GROUP_REPLICATION` (graceful leave) + `routable=false`, **quorum-guarded** |
| Sync durability | Semi-sync ack count self-heal | `group_replication_consistency` + certification; semi-sync disabled |
| Replica provisioning | XtraBackup clone + `CHANGE REPLICATION SOURCE` | Distributed recovery (Clone plugin), optional XtraBackup pre-seed |
| Replica catch-up health | `SHOW REPLICA STATUS` IO/SQL threads | `performance_schema.replication_group_members` member state |

## Background: what Group Replication is (operator-relevant subset)

- **Single-primary mode (target for v1).** One member is `PRIMARY` (read-write);
  all others are auto-set `super_read_only` `SECONDARY`. On primary loss the group
  elects a new primary by member weight then UUID. Multi-primary is out of scope
  for v1.
- **Quorum.** Writes and membership changes need a majority. A member that loses
  contact with the majority is expelled or **blocked**; a group that loses
  majority (e.g. 2 of 3 down) is stuck until an operator runs
  `group_replication_force_members` — a dangerous manual operation.
- **Member states.** `OFFLINE → RECOVERING → ONLINE`, plus `ERROR` and
  `UNREACHABLE`. A member is only serving consistent traffic when `ONLINE`.
- **Group bootstrap.** Exactly one member, exactly once in the group's life,
  starts with `group_replication_bootstrap_group=ON` to *create* the group; all
  others just `START GROUP_REPLICATION` and join via distributed recovery. **Two
  bootstraps = split-brain.** This is the single most dangerous operation.
- **Distributed recovery.** A joining member catches up from a donor via binlog,
  or via the **Clone plugin** (full snapshot, 8.0.17+) when behind the donor's
  purged GTIDs / past `group_replication_clone_threshold`.
- **Config.** Requires `gtid_mode=ON`, `enforce_gtid_consistency=ON`,
  `log_bin`, `log_replica_updates=ON`, `binlog_format=ROW` (all already rendered),
  plus the `group_replication_*` family, the `group_replication.so` plugin, a
  dedicated `group_replication_recovery` channel + recovery user, and group
  communication SSL.
- **Observability.** `performance_schema.replication_group_members` (member id,
  host, port, state, role, version) and `replication_group_member_stats`. Any
  ONLINE member in the majority can report the whole group view.

## Design

### Scope

#### In scope (across the phases)

- A new immutable topology mode `spec.replication.mode: groupReplication` (default
  stays `async`). Async behaviour is completely unchanged when the mode is unset.
- **Single-primary** GR: bootstrap, join, steady-state, planned switchover,
  observed automatic failover, fencing, scale up/down, quorum guards.
- A `groupreplication` management package and version-aware config rendering.
- Operator-side **observe-and-reflect** of group membership into Cluster status
  and routing.
- A GR variant of the in-Pod reconciler (joins/keeps the member in the group;
  never self-elects).
- Quorum-aware PDBs, scale-down/fence guards, quorum-loss detection, and a
  guarded operator-assisted recovery path (opt-in, never automatic).
- Webhook validation and version gating.
- Backup/PITR, kubectl plugin, monitoring, and upgrades integrated for GR.
- Unit + integration (testcontainers, version matrix) + E2E coverage at every
  phase, plus an async **regression** suite.

#### Out of scope (v1)

- **Multi-primary mode** (`enforce_update_everywhere_checks`, conflict-heavy
  workloads). Reserve the API shape but reject it in the webhook for now.
- **Live migration** of an existing async cluster to GR (and vice-versa). Topology
  mode is immutable; switching is a deliberate rebuild / clone-into-new-cluster
  operation, designed later.
- Automatic, unattended `force_members` quorum recovery. The operator detects and
  surfaces quorum loss and offers a guarded, opt-in recovery; it never forces a
  new membership on its own.
- Cross-cluster GR (replica clusters following an external group); GR on MySQL
  < 8.0.

### Guiding principle: the group decides, the operator reflects

Every GR design choice follows from one rule: **the operator must never make an
HA decision the group is responsible for.** The operator declares *who should be
in the group* (desired membership from `spec.instances`) and *who should be
primary on a planned switchover* (a request via the UDF). It then **observes**
`replication_group_members` and mirrors reality into `status.currentPrimary`,
role labels, and routing. It must not fight the group's auto-election by writing
a conflicting `targetPrimary`. This is what makes "automatic failover is handled
by the group" concrete in the code.

### Topology strategy split (key structural change)

To add a deep behaviour change without destabilising the proven async path,
introduce a **topology strategy interface** on both sides, selected by
`spec.replication.mode`:

- **Operator-side** (`internal/controller`): factor the topology-specific steps —
  failover, switchover, fencing semantics, primary tracking, semi-sync, primary
  lease — behind a `topologyReconciler` interface with two implementations:
  - `asyncTopology` — the current code paths, moved largely unchanged.
  - `groupReplicationTopology` — the new observe-and-reflect logic.
  The main loop in `cluster_controller.go` keeps its overall ordering but calls
  through the strategy for these steps. Steps that are topology-agnostic
  (credentials, RBAC, PKI, services scaffolding, PVCs, backup checks, PodMonitor,
  managed roles/databases, retention, reload) are shared.
- **In-Pod** (`rolereconciler`): the `Reconcile` loop's role logic splits into an
  `asyncRoleStrategy` (today's promote/follow) and a `groupRoleStrategy` (ensure
  the member is configured and `START GROUP_REPLICATION` is running; report
  membership; never self-promote). `LocalInstance` gains GR methods (see below).

This isolation is the central safety mechanism: the async path is exercised by
its existing tests unchanged, and GR logic cannot leak into it.

### API and status model

**Spec — new, minimal, immutable mode selector** (`api/v1alpha1/cluster_types.go`):

```go
// ReplicationConfiguration selects and tunes the replication topology.
type ReplicationConfiguration struct {
    // Mode is the replication topology. Immutable after creation.
    // +kubebuilder:validation:Enum=async;groupReplication
    // +kubebuilder:default:=async
    Mode string `json:"mode,omitempty"`

    // GroupReplication tunes the group when Mode=groupReplication.
    // +optional
    GroupReplication *GroupReplicationConfiguration `json:"groupReplication,omitempty"`
}

type GroupReplicationConfiguration struct {
    // Consistency maps to group_replication_consistency.
    // +kubebuilder:validation:Enum=EVENTUAL;BEFORE_ON_PRIMARY_FAILOVER;BEFORE;AFTER;BEFORE_AND_BEFORE
    // +kubebuilder:default:=BEFORE_ON_PRIMARY_FAILOVER
    Consistency string `json:"consistency,omitempty"`
    // ExitStateAction maps to group_replication_exit_state_action.
    // +kubebuilder:validation:Enum=READ_ONLY;OFFLINE_MODE;ABORT_SERVER
    // +kubebuilder:default:=READ_ONLY
    ExitStateAction string `json:"exitStateAction,omitempty"`
    // AutoRejoinTries maps to group_replication_autorejoin_tries.
    // +kubebuilder:default:=3
    AutoRejoinTries *int32 `json:"autoRejoinTries,omitempty"`
    // GroupName, when set, pins group_replication_group_name (a UUID). Generated
    // and persisted to status on first bootstrap when unset. Immutable.
    GroupName string `json:"groupName,omitempty"`
}
```

`spec.replication` is added to `ClusterSpec`; the default keeps every existing
cluster `async`. The `group_replication_local_address` port (default 33061) is
operator-owned, not user-tunable in v1.

**Status — group view** (`ClusterStatus`):

```go
// GroupReplication reflects the live group membership and quorum, mirrored from
// performance_schema.replication_group_members. Nil for async clusters.
GroupReplication *GroupReplicationStatus `json:"groupReplication,omitempty"`

type GroupReplicationStatus struct {
    GroupName       string                  `json:"groupName,omitempty"`
    Bootstrapped    bool                    `json:"bootstrapped,omitempty"` // group created at least once
    PrimaryMember   string                  `json:"primaryMember,omitempty"`
    Members         []GroupMember           `json:"members,omitempty"`
    HasQuorum       bool                    `json:"hasQuorum,omitempty"`
    ViewID          string                  `json:"viewId,omitempty"`
}

type GroupMember struct {
    Instance string `json:"instance"`           // pod name
    State    string `json:"state"`              // ONLINE/RECOVERING/OFFLINE/ERROR/UNREACHABLE
    Role     string `json:"role"`               // PRIMARY/SECONDARY
    Reachable bool  `json:"reachable"`
}
```

`Bootstrapped` and `GroupName` are sticky (set once, never auto-cleared); they
are the persisted memory that makes group bootstrap exactly-once across restarts
and re-elections. `currentPrimary` continues to exist and is set from
`PrimaryMember` so all downstream consumers (services, plugin, status) are
unchanged.

**Webserver `/status` extension** (`pkg/management/mysql/webserver/status.go`):
add a `GroupReplication *GroupReplicationMemberStatus` field reporting this
member's state/role and its view of the whole group (member host/port/state/role,
quorum). The operator reads it in `observe()` exactly like it reads
`Replication` today. `Role` continues to report `primary`/`replica` derived from
the member's GR role so the existing routing keeps working.

### Status authorization webhook under GR (extends `design/020`)

`design/020` added per-instance ServiceAccount identities and a validating
webhook (`internal/webhook/v1alpha1/cluster_webhook.go`) on `clusters/status`
updates. Its rule today: an instance identity may change **only**
`status.currentPrimary` (to itself, when it is the designated `targetPrimary`)
and `status.currentPrimaryTimestamp`; every other status field is masked and must
stay byte-identical. This exists because the async pull-model has the promoting
instance write `currentPrimary` itself.

GR changes the status-write model, so the webhook must change with it:

- **There is no self-promotion under GR.** The group elects the primary; the
  operator observes `replication_group_members` and is the **sole writer** of
  `currentPrimary` (and of the whole `groupReplication` status block). So under
  GR an instance identity must be allowed to write **nothing** to status — the
  allow-list of instance-writable fields is empty, including `currentPrimary`.
- **The webhook branches on topology.** A status-subresource admission request
  carries the full object (spec is present and unchanged), so the handler reads
  `newCluster.Spec.Replication.Mode`:
  - `async` → existing rules unchanged (instance may set its own `currentPrimary`).
  - `groupReplication` → deny *any* status change from an instance identity
    (old/new status must be byte-identical with no field mask).
- **New high-value fields are protected automatically — but verified explicitly.**
  Because the deep-equal mask defaults to "instances cannot touch anything not on
  the allow-list" (design/020 Decision 3), the new `status.groupReplication.*`
  fields are already out of reach for instances. The plan keeps it that way and
  adds explicit deny tests for forging `primaryMember`, `members`, `hasQuorum`,
  `bootstrapped`, and `groupName`.
- **Monotonic invariants for the two split-brain-critical fields, enforced for
  *all* callers (operator included), as defense in depth against a bug or a
  compromised operator token:**
  - `status.groupReplication.bootstrapped`: `false → true` allowed, `true → false`
    **denied**. Re-arming bootstrap is the path to a second group (split-brain);
    it must never happen, and total-outage recovery re-bootstraps the *same*
    group without clearing this flag.
  - `status.groupReplication.groupName`: `"" → value` allowed,
    `value → different` **denied** (immutable once pinned; a changed group name
    fractures the group).
- **The bootstrap designation stays operator-only.** Bootstrap is signalled by
  (`targetPrimary == me`) ∧ (`bootstrapped == false`), both operator-written.
  `targetPrimary` is already masked from instance writes, so a compromised
  instance cannot nominate itself as the bootstrap member and start a competing
  group.
- **RBAC hardening (optional).** Because the GR in-Pod reconciler never patches
  `clusters/status`, the per-instance Role's `update/patch` grant on
  `clusters/status` is *unused* for GR clusters. The webhook already reduces its
  effective capability to zero; as extra hardening the per-cluster Role
  (`cluster_rbac.go`) may omit that grant entirely for GR clusters. Kept as an
  option, not a requirement — the webhook is the authoritative gate.

**Self-reporting vs. operator-sole-writer (two channels).** It is tempting to let
the instance manager keep writing `currentPrimary` under GR — not as
self-promotion (the group elects, the instance does not), but as *self-reporting*
("I observe that the group made me PRIMARY"). The key realisation is that
self-reporting already has a home: the **mTLS control API** (`/status`), which is
the instance → operator channel `observe()` already polls for role/GTID/
replication state. Reporting there needs **no** Kubernetes status-write
capability. We deliberately do **not** let the instance write `currentPrimary` on
the **Kubernetes** channel under GR, for two reasons:

1. **The webhook cannot authorise it.** In async the write is gated on
   `currentPrimary == oldStatus.targetPrimary` (operator-designated, trusted).
   Under GR the *group* elects the primary, so on an auto-failover there is no
   operator-written `targetPrimary` to check against, and the webhook cannot query
   MySQL to verify the claim — a compromised instance writing `currentPrimary =
   self` would be indistinguishable from a real one, reopening the
   forge-`currentPrimary` → redirect-`rw` blast radius `design/020` closed.
2. **Aggregation gives a free quorum cross-check.** When the operator polls every
   member over mTLS, `replication_group_members` is consistent across the ONLINE
   majority, so a single lying member is outvoted by the group view. A direct
   K8s write of `currentPrimary` is taken as ground truth with no cross-check.

So self-reporting rides mTLS; the operator stays the sole writer of the K8s
`currentPrimary` and mirrors the *cross-validated* group primary. The only real
cost — convergence latency — is addressed by **event-driven change detection**
(§Change detection: readiness transitions + the `gr-observed` doorbell wake the operator),
not by polling and not by widening the instance's K8s blast radius. If a
low-latency *direct* instance write is ever wanted, the only safe webhook rule is
"allow `currentPrimary = self` iff `oldStatus.groupReplication.primaryMember ==
self`" — but once the operator has observed `primaryMember`, it can write
`currentPrimary` itself, so that path collapses back to operator-sole-writer.

Net effect: GR instances have a strictly **smaller** status blast radius than
async instances (zero writable fields vs one), and the fields an adversary would
most want to forge — who is primary, whether quorum exists, and whether the group
may be bootstrapped again — are operator-only, with the two bootstrap-critical
fields additionally pinned monotonic/immutable for every caller.

### Config rendering (`pkg/management/mysql/config`)

Add a `TopologyMode` and a `GroupReplication` struct to `ServerConfig`. When mode
is GR, `managedSettings` additionally renders:

- `plugin_load_add = group_replication.so`
- `group_replication_group_name = <uuid>` (from status/spec, stable for life)
- `group_replication_local_address = <pod-fqdn>:33061`
- `group_replication_group_seeds = <all member fqdns>:33061`
- `group_replication_start_on_boot = OFF` — **the operator controls start**, so a
  restarting member never rejoins or re-bootstraps unsupervised.
- `group_replication_bootstrap_group = OFF` — never rendered ON; bootstrap is a
  runtime `SET GLOBAL`, never a config-file default (a config-file ON would
  re-bootstrap on every boot = split-brain).
- `group_replication_single_primary_mode = ON`,
  `group_replication_enforce_update_everywhere_checks = OFF`
- `group_replication_consistency`, `group_replication_exit_state_action`,
  `group_replication_autorejoin_tries` from spec
- `group_replication_ssl_mode = REQUIRED`, `group_replication_recovery_use_ssl = ON`
  and recovery-channel SSL paths (reusing the cluster's TLS material)
- `group_replication_ip_allowlist` scoped to the cluster's Pod CIDR/service domain
- `binlog_checksum = NONE` only when the server version requires it
  (`version.GroupReplicationRequiresNoBinlogChecksum()`); 8.0.21+ does not.

The **entire `group_replication_*` namespace is added to `managedKeys`/`deniedKeys`**
so a user parameter can never destabilise the group. `slave_preserve_commit_order`
and `enforce_gtid_consistency` interactions are validated. `gtid_mode`,
`log_bin`, `log_replica_updates`, `binlog_format=ROW` remain managed (GR requires
them, and they are already rendered).

Version gating (`pkg/management/mysql/version`): add
`SupportsGroupReplication()` (8.0+; recommend a floor of **8.0.22** for stable
single-primary auto-rejoin and consistency levels) and
`HasGroupReplicationClone()` (8.0.17+). 5.6/5.7 GR is unsupported (consistent with
D2).

### Bootstrap: exactly-once group creation

The most dangerous operation. Rules:

1. **Election.** On a fresh cluster the operator designates one bootstrap member
   (ordinal 1), recorded as the existing `targetPrimary` *and* gated by
   `status.groupReplication.bootstrapped == false`.
2. **Generate and pin `group_replication_group_name`** (a UUID) into status on
   first reconcile; it is sticky and immutable thereafter. Every member renders
   the same name.
3. The bootstrap member's in-Pod GR reconciler, **only** when the operator signals
   "you are the bootstrap member and the group is not yet bootstrapped", runs:
   `SET GLOBAL group_replication_bootstrap_group=ON; START GROUP_REPLICATION;
   SET GLOBAL group_replication_bootstrap_group=OFF;`. It then reports ONLINE
   PRIMARY.
4. The operator observes the member ONLINE, sets
   `status.groupReplication.bootstrapped=true` and `currentPrimary`. From this
   instant, **no member may ever bootstrap again** — subsequent joins always
   `START GROUP_REPLICATION` without the bootstrap flag.
5. The bootstrap signal is single-shot and idempotent: re-running `START
   GROUP_REPLICATION` on an already-ONLINE member is a no-op; the bootstrap flag
   is never set a second time because `bootstrapped` is sticky.

**Total-outage re-bootstrap** (every member down at once, group view lost): this
is a guarded recovery, not normal bootstrap. The operator must pick the member
with the **most-advanced `gtid_executed`** (reusing `replication.GTIDContains`
dominance, like `selectFailoverCandidate`) to re-bootstrap from, so no committed
transaction is lost, then let the others rejoin. Until a safe most-advanced
member is provable, the cluster stays `Blocked` and surfaces the condition rather
than guessing. This is opt-in/manual-confirmed in v1 (see Quorum guards).

### Provisioning / distributed recovery

New members are provisioned by `instance join` as today, but the join target is
the group, not a single source:

- Reuse the existing replication user (`cloudnative-mysql_repl`) extended with the
  privileges GR recovery needs (`REPLICATION SLAVE`, `BACKUP_ADMIN`,
  `CLONE_ADMIN`, `GROUP_REPLICATION_*` as required), configured on the
  `group_replication_recovery` channel via `CHANGE REPLICATION SOURCE TO ... FOR
  CHANNEL 'group_replication_recovery'` with mTLS.
- **Recovery method (decision):** default to **Clone-plugin distributed recovery**
  (install `clone.so`, set `group_replication_clone_threshold`) — the GR-native
  path; the joining member clones a full snapshot from a donor automatically when
  it is too far behind. Optionally keep the existing **XtraBackup pre-seed** to
  bound recovery time for large datasets (seed the empty data dir from a backup,
  then GR recovers only the delta). Phase 3 ships Clone-first; XtraBackup pre-seed
  is an optimisation.
- The provisioning gate in `reconcileInstances` (`cluster_topology.go`) keeps its
  one-at-a-time ramp, but the "primary healthy" precondition becomes "the group
  has quorum and at least one ONLINE donor", and "is the previous instance ready"
  becomes "is the previous member ONLINE in the group".

### Switchover (planned primary change)

`reconcileSwitchover` is reimplemented for GR:

- Trigger is unchanged (set `status.targetPrimary` to a member), so the kubectl
  plugin and CNPG-style ergonomics are preserved.
- The operator validates the target is an `ONLINE SECONDARY`, then invokes the
  online UDF `SELECT group_replication_set_as_primary('<target-uuid>')` (via the
  in-Pod control API on any ONLINE member). No read-only dance, no relay drain —
  the group hands over the role atomically.
- Bounded by `spec.maxSwitchoverDelay` exactly as today (`ensureSwitchoverStarted`
  / `abortSwitchover` are reused); on timeout the operator clears `targetPrimary`
  back to `currentPrimary`. There is nothing to "promote back" because the group
  never left a consistent state.
- The operator observes the new `PRIMARY` from `replication_group_members` and
  mirrors it into `currentPrimary` + role labels + services.
- `primaryUpdateMethod`/`primaryUpdateStrategy` are honoured: `switchover` uses
  the UDF; `restart` is only meaningful for in-place primary updates.

The in-Pod reconciler does **not** self-promote in GR mode; `set_as_primary` is an
operator action against the group, and GR sets the read-only flags itself.

### Automatic failover: observation only

`reconcileFailover` is **disabled** for GR. The operator does not select
candidates, does not fence the old primary to promote a replica, and does not
write a failover `targetPrimary`. Instead, the GR strategy's steady-state step:

1. Reads the group view from every reachable member and takes the **majority**
   verdict on who is `PRIMARY` (a single member's self-report is never trusted on
   its own; the ONLINE majority's `replication_group_members` is consistent and
   outvotes a lying or stale member). If that cross-validated primary differs from
   `status.currentPrimary`, the group has already elected a new primary — mirror
   it into `currentPrimary`, repoint role labels and `-rw`, emit a
   `FailoverObserved` Event. RTO is bounded by GR's own election (sub-second to
   seconds) plus the time to deliver the Kubernetes event that wakes the operator
   (a readiness transition or the `gr-observed` doorbell — see §Change detection), not a
   poll interval and not `spec.failoverDelay`.
2. If the old primary's member is `UNREACHABLE`/expelled, the group has already
   removed it from quorum; the operator just stops routing to it.
3. `spec.failoverDelay`, the GTID-dominance `selectFailoverCandidate`, the
   `fenceInstancePod`-before-promote, and the **primary Lease** are all inert for
   GR (the lease is removed from the GR infrastructure path; quorum is the
   split-brain guard).

This is the literal implementation of "automatic failover is handled by the group
and the quorum inside of it."

### Change detection: event-driven, not polling

The HA decisions happen *inside* the group, where Kubernetes has no visibility: a
re-election, a `RECOVERING → ONLINE` transition, a member expelled, or quorum lost
change no Kubernetes object and emit no Kubernetes event. A naive operator would
discover them only by polling every member every N seconds — wasteful and laggy.
The operator must **react to changes**; the timed requeue is a backstop, not the
detection mechanism.

The design moves the *watching* to where the change originates — the in-Pod
instance manager, which already supervises mysqld over the always-available admin
interface (D9) and already runs a controller-runtime reconciler watching the
Cluster — and has it **convert GR-internal transitions into Kubernetes events**
the operator's existing watches already consume. Four sources, by who detects
what:

1. **Kubernetes object watches (already wired).** `For(&Cluster{})` +
   `Owns(&Pod{}, &ConfigMap{}, …)`. Pod add/update/delete, scaling, spec edits,
   and the operator's own status writes already trigger reconciles. A primary
   *Pod* dying is already event-driven.
2. **GR health ⇄ Pod readiness (the main GR bridge).** Bind the in-Pod `/readyz`
   to the member's GR state — **`ONLINE` ⇒ ready; `RECOVERING`/`ERROR`/`OFFLINE`/
   `UNREACHABLE` ⇒ not ready.** The in-Pod manager watches
   `replication_group_members` locally over the admin socket (cheap, low-latency,
   invisible to the API server) and flips the probe; the kubelet turns each
   transition into a Pod `Ready` condition change — a Kubernetes event the
   operator already watches via `Owns(&Pod{})`. So member up/down/recover/expel,
   and an isolated old primary going read-only under
   `group_replication_exit_state_action`, become event-driven **with no operator
   polling and no new instance writes to the API**. The existing
   `deRouteGracePeriod`/`unreachableSince` de-routing then runs off these events
   instead of a timer.
  3. **GR snapshot change with no health change (the doorbell).** Some transitions
   are invisible to both object watches and readiness — most importantly a clean
   `set_as_primary` handover, where membership is unchanged (so the **GR view id
   does not change**) and both members stay `ONLINE`/Ready, yet the primary role
   moved. A doorbell keyed only on the view id would miss this; one keyed only on
   "my role" would miss a membership change that does not touch this member. So the
   in-Pod manager publishes a doorbell whenever **any** part of its locally
   observed GR snapshot changes — a single advisory annotation on its *own* Pod,
   `mysql.cloudnative-mysql.io/gr-observed`, whose value is a short fingerprint of
   `(primaryMemberUUID, viewId, myMemberState, hasQuorum)`. It bumps on
   election/switchover, join/leave/expel, and quorum gain/loss alike.
   `Owns(&Pod{})` turns the bump into an immediate reconcile. The annotation is a
   **wake-up, never authority** — on reconcile the operator still reads the
   cross-validated *majority* group view over mTLS before changing
   `currentPrimary`/routing (see §Self-reporting). A compromised instance ringing
   the doorbell falsely only causes a no-op reconcile; it cannot move traffic. The
   instance SA gets `patch` on *its own* Pod only (scoped by `resourceNames` to the
   Pod whose name equals the instance name).
4. **Timed requeue = backstop only.** A long `RequeueAfter` (≈ the existing
   `readyResync`, possibly *longer* for GR) survives solely to heal missed events
   / informer gaps — not as the detection path. Precisely because (2) and (3) make
   real transitions event-driven, the backstop gets longer, not shorter.

Rejected alternative: a long-lived streaming watch (gRPC/SSE) from the operator to
each instance's `/status`. It adds per-Pod connection lifecycle, reconnection, and
scaling cost and buys nothing over the readiness + doorbell bridges, which reuse
robust Kubernetes watch machinery.

#### Annotation surface (and what deliberately needs none)

The governing rule keeps the annotation surface tiny: **an annotation is only a
doorbell (a wake-up) or a human/plugin-initiated request — never the carrier of
authoritative GR state.** Authoritative state always travels over mTLS `/status`
and is cross-validated against the majority group view. With that rule, GR adds
exactly **two** annotations on top of the existing set:

| Annotation | On | Written by | Purpose / operator reaction |
|---|---|---|---|
| `mysql.cloudnative-mysql.io/gr-observed` | instance Pod | in-Pod manager (own Pod, `resourceNames`-scoped) | Doorbell. Bumps on any GR snapshot change (primary, view, member state, quorum). Wakes a reconcile; value is a hint, never trusted. |
| `mysql.cloudnative-mysql.io/force-quorum-recovery` | Cluster | human / kubectl plugin | Explicit, confirmed opt-in to the guarded `force_members` / total-outage re-bootstrap path. `For(&Cluster{})` triggers the reconcile; the operator still computes and proves the safe survivor set before acting. |

Existing annotations are reused unchanged under GR and already trigger reconciles:
`fencing` (now meaning `STOP GROUP_REPLICATION`), `restart`, `reload`, `reinit`
(the remedy for a member that cannot rejoin after errant transactions),
`unreachable-since`/`reload-applied` (operator bookkeeping).

**Transitions that deliberately need _no_ annotation**, because they are already
delivered by a richer channel and read in detail over mTLS once the operator is
woken:

- **Member ONLINE/RECOVERING/ERROR/OFFLINE/UNREACHABLE** → Pod readiness flip
  (source 2); the operator reads the precise state + reason over mTLS.
- **Quorum loss** → folded into readiness (`/readyz` = `ONLINE ∧ quorate`), so a
  minority-blocked member flips `NotReady`; the operator reads `hasQuorum=false`
  over mTLS and sets `Blocked`. (The `gr-observed` doorbell also bumps, as a
  belt-and-suspenders wake-up for a primary that stays Ready while a secondary is
  expelled.)
- **A member cannot rejoin (errant transactions / `ERROR`)** → `NotReady` +
  mTLS-reported error; surfaced like `ReplicationBrokenInstances`, remediated via
  the existing `reinit` annotation.
- **Group bootstrap completed** → the bootstrap member going `ONLINE PRIMARY` is a
  readiness event; the operator then sets the sticky `status.groupReplication.
  bootstrapped`. The bootstrap *signal* (operator → instance) rides Cluster status
  (`targetPrimary == me ∧ bootstrapped == false`), which the in-Pod reconciler
  already watches via `For(&Cluster{})` — no annotation either direction.
- **GTID advancing** → continuous, not a discrete event; the operator keeps its
  existing throttled persistence of `gtidExecutedByInstance` plus the backstop
  requeue.

### Fencing (different under GR)

Fencing is requested the same way (the `fencing` annotation on a Pod), but the
in-Pod action and the guards change:

- **In-Pod:** instead of stopping mysqld (`FenceGate`), the GR fence does
  `STOP GROUP_REPLICATION` — the member gracefully leaves the group and becomes
  `super_read_only`/`OFFLINE`. mysqld stays up and reachable for inspection.
  Unfence does `START GROUP_REPLICATION` to rejoin via distributed recovery.
- **Operator:** still flips `routable=false` so the fenced member leaves routing
  (`reconcileFencing`).
- **Quorum interaction (critical):** fencing the **primary** is a graceful leave
  that *triggers the group to elect a new primary* — clean and expected. But
  fencing a member shrinks the group, so the operator must **refuse to fence a
  member when doing so would drop the group below quorum** (`floor(N/2)+1`),
  surfacing `Blocked` instead. Fencing is never allowed to manufacture a
  quorum-loss outage.

### Quorum guards

New guard logic, all centred on `quorum = floor(activeMembers/2) + 1`:

- **PDB** (`cluster_guard.go`): replace the primary/replica split with a single
  group-aware PDB whose `maxUnavailable = N − quorum` (e.g. N=3 → 1, N=5 → 2), so
  voluntary disruptions can never break quorum.
- **Scale-down** (`scaleDownReplicas`): never remove a member if it would drop the
  group below quorum; remove members from the group with `STOP GROUP_REPLICATION`
  *before* deleting the Pod so the group view shrinks cleanly (avoid leaving
  ghost `UNREACHABLE` members). Never scale below 1; warn on scaling to an even
  size.
- **Quorum-loss detection:** if `observe()` finds the group has lost majority
  (reachable ONLINE members `< quorum`), set `Phase=Blocked`,
  `PhaseReason="group has lost quorum; manual recovery required"`, emit a Warning
  Event, and **do nothing destructive**. The operator keeps observing and waits.
- **Guarded recovery (opt-in):** recovery from quorum loss
  (`group_replication_force_members`) and total-outage re-bootstrap are exposed as
  an explicit, confirmed action (an annotation like
  `.../force-quorum-recovery` plus a kubectl command), never automatic. The
  operator computes the safe survivor set / most-advanced member and refuses if it
  cannot prove safety.

### Features changed or disabled under GR

| Component | GR behaviour |
|---|---|
| `cluster_failover.go` | Disabled — group auto-elects; operator observes only |
| `cluster_primary_lease.go` | Disabled — quorum is the split-brain guard |
| `cluster_semisync.go` | Disabled — GR certification + `group_replication_consistency` replace semi-sync; `minSyncReplicas`/`maxSyncReplicas` and `spec.mysql.semiSync` are rejected with GR by the webhook |
| Diverged-replica detection | Replaced by group member `ERROR`/can't-join detection; `reinit` annotation remains the remedy (re-clone) |
| `cluster_switchover.go` | Reimplemented with `set_as_primary` UDF |
| `reconcileFencing` | Operator routing unchanged; in-Pod action becomes `STOP GROUP_REPLICATION`; quorum-guarded |

The async implementations stay in place and run for `mode: async`; GR simply
selects different strategy implementations.

### Services and routing

`-rw` → the group's `PRIMARY`; `-ro`/`-r` → `ONLINE SECONDARY` members only. Role
labels are driven by the reported GR member role, and **non-`ONLINE` members
(RECOVERING/ERROR/UNREACHABLE) are pulled from `-ro`/`-r`** (they are not serving
consistent reads) — a natural extension of the existing `routable` gating in
`reconcileFencing`. No new Services; only the source of the role label changes.

### Upgrades and lifecycle

- **Rolling instance upgrades** (`cluster_upgrade.go`): roll `SECONDARY` members
  first, one at a time, waiting for each to rejoin `ONLINE` before the next
  (so quorum is preserved throughout); switch the primary away via
  `set_as_primary` before rolling it last. Respect `group_replication` member
  version-compatibility rules during the window.
- **In-place instance-manager upgrade** (`design/019`): unaffected in principle —
  the re-exec keeps mysqld (hence the running group member) alive — but add an
  explicit test that the group member stays `ONLINE` across a manager re-exec.
- **Backup/PITR** (`design/008`/`009`): take physical backups from an `ONLINE
  SECONDARY` (offload from the primary); continuous archiving runs on the primary
  as today. Restore bootstraps a *fresh* single-member group from the restored
  data dir, then scales up — recovery never rejoins an old group.

### kubectl plugin (`design/016`)

GR changes what several commands *do*, and adds the most dangerous command in the
whole CLI (quorum recovery). The plugin is also exactly where an operator reaches
under stress, so the command set and its **documentation** must explain not just
*what* a command does but its *consequences*. Two parts: the GR command behaviour,
and a documentation contract that applies to every command (async included).

**GR command behaviour:**

- `status` shows the group view (members, states, roles, primary, quorum health),
  and clearly flags a degraded/quorum-lost group.
- `promote <member>` uses `set_as_primary` (still via `targetPrimary`); the
  consequence note changes from "brief write interruption + GTID catch-up" (async)
  to "near-instant role handover, no write loss" (GR).
- `fence on`/`off` map to `STOP`/`START GROUP_REPLICATION`. The command **refuses
  (or requires `--force`) when fencing would drop the group below quorum**, and
  warns that fencing the primary triggers a group re-election.
- New guarded `group recover` command for operator-assisted `force_members` /
  total-outage re-bootstrap. It is the only command that can lose data or cause
  split-brain if misused, so it prints the computed survivor set / most-advanced
  member, requires a typed confirmation (not just `--yes`), and refuses when
  safety is unprovable.

**Documentation contract (every command, in-CLI `--help` *and* the docs site):**

Each command's help and runbook entry follows one structure so consequences are
never buried:

1. **What it does** — one line (the existing `Short`).
2. **How it works** — the object/field/annotation it touches and who acts on it
   (e.g. "stamps the fencing annotation; the operator removes the Pod from
   routing"). Demystifies the declarative indirection.
3. **Preconditions / refusals** — what must hold and what the command rejects
   (e.g. `promote` refuses a diverged or fenced instance).
4. **Effect** — the observable result and where to see it (`status`, which
   Service, which condition).
5. **Consequences & risks** — the part missing today: write interruption,
   failover/election triggered, archiving paused, PDBs relaxed, **data loss
   potential**, **quorum impact**, and reversibility.
6. **Topology differences** — async vs GR, where they differ. Where practical the
   command detects `spec.replication.mode` at runtime and prints the *relevant*
   consequence rather than both.
7. **Example + what to verify afterward.**

**Safety affordances (behaviour the docs describe and the CLI enforces):**

- Destructive/disruptive commands (`reinit`, `destroy`, `maintenance set`,
  `fence`, `restart` of a primary, `group recover`) lead their `Long` text with
  the consequence, print a one-line consequence summary before acting, and gate on
  confirmation — `--yes` for disruptive, a typed cluster/instance name for
  data-destroying ones. A `--dry-run` previews the effect without applying.
- A **command safety matrix** in the docs: command → destructive? → write impact →
  quorum/availability impact → reversible? — so an operator can scan risk at a
  glance.

**Where the docs live:** expand `docs/src/operations.md` runbooks with GR variants
and a "Consequences" callout per dangerous operation, and add a per-command
reference page generated from the cobra help so the in-CLI text and the site never
drift. The structured `Long` text is the single source both consume.

> Note: items 1–7 and the safety affordances are general improvements that also
> benefit the async CLI; GR is the catalyst. If preferred they can be lifted into
> their own small design doc and landed independently of the GR milestones — but
> the GR-specific consequence text (quorum, `group recover`) must ship with the
> phase that introduces each command.

### Webhook validation and version gating (`internal/webhook`)

- `spec.replication.mode` is **immutable** (reject changes on update).
- Reject `groupReplication` on server versions `< 8.0.22`.
- Reject `groupReplication` together with `spec.mysql.semiSync`,
  `minSyncReplicas`/`maxSyncReplicas`, or a replica cluster
  (`spec.replica`/`externalClusters` GR source) in v1.
- Reject `multiPrimary`/update-everywhere configs in v1.
- Warn (not reject) on even `spec.instances` (quorum prefers odd 3/5/7); warn on
  `instances: 1` GR (no fault tolerance, but allowed for dev).
- Validate `group_replication_group_name` is a UUID if user-pinned and immutable.

## Phasing (safe, incremental milestones)

Each phase is independently shippable, leaves async untouched, and has its own
tests. GR stays behind `mode: groupReplication` throughout.

- **M-GR.1 — Foundations (no runtime behaviour).** API surface
  (`spec.replication`, status fields), version gating, GR config rendering +
  golden tests, webserver `/status` GR fields, `groupreplication` package
  skeleton, spec webhook validation, and the **status-authz webhook extension**
  (mode-branch denying instance status writes on GR clusters + monotonic
  `bootstrapped`/`groupName` invariants — these must exist *before* any group is
  bootstrapped, since they guard the very fields M-GR.2 starts writing). Nothing
  starts a group yet.
- **M-GR.2 — Single-member group.** Bootstrap a 1-member group (exactly-once
  guard), in-Pod GR role strategy, operator observe→`currentPrimary`. Integration
  test: plugin loads, group bootstraps, status reports ONLINE PRIMARY.
- **M-GR.3 — Multi-member join & steady state.** 3-member group via distributed
  recovery (Clone), quorum-aware provisioning gate, routing by group role,
  non-ONLINE de-routing, and the **GR-health ⇄ `/readyz` bridge** (§Change
  detection source 2) so member up/down/recover is event-driven. E2E: 3-member
  cluster reaches Ready and serves rw/ro.
- **M-GR.4 — Planned switchover.** `set_as_primary` path, `maxSwitchoverDelay`
  bound, observe new primary, plus the **`gr-observed` doorbell** (§Change
  detection source 3) so a clean handover moves `-rw` on an event, not a poll.
  E2E switchover.
- **M-GR.5 — Observed automatic failover.** Kill the primary; group elects;
  operator mirrors within RTO via readiness/doorbell events (not a poll interval);
  async failover loop / lease / semi-sync disabled for GR. E2E failover proving
  the **operator does not promote**.
- **M-GR.6 — Fencing & quorum guards.** GR fence (`STOP GROUP_REPLICATION`),
  quorum-preserving PDB/scale-down/fence guards, quorum-loss detection + `Blocked`
  surfacing, opt-in guarded recovery via `force_members` from the most-advanced
  surviving member (GTID-dominance selection). E2E quorum-loss and recovery.
  (Total-outage re-bootstrap — re-forming the group when no member survives
  ONLINE — lands in M-GR.7 alongside the other lifecycle recovery paths.)
- **M-GR.7 — Lifecycle integration.** Rolling + in-place upgrades, scale up/down,
  total-outage re-bootstrap (**done** — re-form the same group from the
  most-advanced member once the group has no ONLINE survivor: opt-in via the same
  `force-quorum-recovery` annotation, but with a stricter safety bar than
  `force_members` — every configured instance must be reachable with a known
  `gtid_executed` and the survivor's set must dominate all of them, else the
  cluster stays `Blocked`; the survivor is stamped `force-group-rebootstrap` and
  bootstraps the group, others rejoin via distributed recovery),
  backup/restore into a fresh group (**done** — routes through the
  topology-agnostic bootstrap path: the recovery primary restores the physical
  backup and the in-Pod role strategy bootstraps a fresh single-member group,
  secondaries initialise empty and join via distributed recovery; never rejoins
  an old group, guaranteed by a fresh pinned group name plus
  `group_replication_start_on_boot=OFF`. Backups offload to an ONLINE secondary
  via the existing prefer-standby source selection),
  monitoring (**done** — the operator publishes each GR cluster's authoritative
  `status.groupReplication` on its existing `/metrics` endpoint via a collector on
  the controller-runtime registry: `cnmysql_cluster_gr_has_quorum`,
  `_gr_bootstrapped`, `_gr_view_size` (the quorum denominator), and
  `_gr_members{state}` counts per member state; reads the cached client at scrape
  time, so no extra reconcile or in-Pod query, and async clusters emit nothing),
  kubectl plugin GR commands (**done** — `kubectl cnmysql group status` renders
  the operator's cross-validated group view (group name, bootstrapped, quorum,
  primary, per-member state/role/reachability) and refuses against async
  clusters; `kubectl cnmysql group recover` requests a guarded quorum recovery by
  stamping the `force-quorum-recovery` annotation, gated on the same
  bootstrapped-and-quorum-lost precondition the operator enforces, behind a
  consequence summary + confirmation. The documentation contract and safety
  affordances ship in the plugin README: structured `--help`/runbook text,
  consequence summaries, confirmations, and a command safety matrix).
  Full E2E matrix + async regression suite.
  (GR-specific consequence text — quorum impact, `group recover` — ships with the
  phase that first introduces each command, e.g. `fence` quorum-guard in M-GR.6.)

## Testing strategy

The user-stated requirement is heavy unit + integration + E2E coverage. Concretely:

- **Unit (`make test`, envtest/no server):**
  - Config golden files for the GR block across the version matrix (8.0/8.4/9.x),
    including the `binlog_checksum` and clone-threshold version branches.
  - Spec webhook: mode immutability, version floor, GR⊥semi-sync, multi-primary
    rejection, even-instance warnings, group-name UUID validation.
  - Status webhook (extends `internal/webhook/v1alpha1/cluster_webhook_test.go`):
    for a GR cluster, an instance identity is denied writing `currentPrimary`
    (to self or anyone), `groupReplication.primaryMember`/`hasQuorum`/`members`,
    `bootstrapped`, and `groupName`; the operator account is still allowed to
    write them; `bootstrapped` `true→false` and `groupName` `value→different` are
    denied for *all* callers; async-cluster instance self-promotion is still
    allowed (regression).
  - Status aggregation: `replication_group_members` → `currentPrimary`, quorum
    math, non-ONLINE de-routing.
  - Quorum guards: PDB `maxUnavailable`, scale-down refusal, fence refusal below
    quorum.
  - Bootstrap election: exactly-once gate (sticky `bootstrapped`), never a second
    bootstrap across simulated restarts; total-outage most-advanced selection
    (reusing the `selectFailoverCandidate` GTID-dominance tests).
  - Strategy split: async strategy behaviour is byte-for-byte unchanged (golden
    reconcile traces).
- **Integration (`make test-integration`, testcontainers, real Percona, version
  matrix):**
  - Bootstrap a 1-member group; verify `replication_group_members` ONLINE PRIMARY.
  - 3-member group join via Clone distributed recovery; write on primary visible
    on secondaries.
  - `set_as_primary` switchover changes the PRIMARY.
  - Kill a member; group re-elects; remaining members keep quorum and stay
    writable.
  - `STOP GROUP_REPLICATION` fence; member leaves and rejoins cleanly.
  - Quorum loss (kill 2 of 3) → minority blocked; guarded `force_members`
    recovery restores it.
  - Total outage → re-bootstrap from the most-advanced member; others rejoin with
    no data loss.
- **E2E (`make test-e2e`, Kind + MinIO):** full 3-member GR lifecycle — provision,
  route, planned switchover, observed automatic failover (operator does not
  promote), fence/unfence, scale up/down (quorum preserved), rolling and in-place
  upgrades, backup→restore into a fresh group, quorum-loss surfaced + guarded
  recovery. A dedicated **async regression** E2E proves the default path is
  unchanged.
- **Change detection (integration + E2E).** Assert reactions are event-driven, not
  timer-driven: with the backstop requeue set very long, (a) a `RECOVERING →
  ONLINE` member flips Pod `Ready` and is added to `-ro`/`-r` promptly; (b) an
  expelled/isolated member flips `Ready=false` and is de-routed promptly; (c) a
  clean `set_as_primary` handover (both members stay ONLINE/Ready) still moves
  `-rw` within seconds via the `gr-observed` doorbell; (d) a falsely-rung
  doorbell from a non-primary member produces a no-op reconcile and never moves
  traffic (cross-validation guard).

## Safety / split-brain analysis

The four hazards and their guards:

1. **Double group bootstrap → two groups, split brain.** Guard: sticky
   `status.groupReplication.bootstrapped`, bootstrap signalled single-shot by the
   operator only when `false`, `group_replication_bootstrap_group` never written
   to the config file, `start_on_boot=OFF` so a restart never re-bootstraps. The
   status webhook additionally pins `bootstrapped` monotonic (`true→false`
   denied) and keeps `targetPrimary`/`bootstrapped` writable only by the operator,
   so a compromised instance cannot re-arm bootstrap or nominate itself.
5. **Status forgery by a compromised instance Pod → bad routing / hidden quorum
   loss / split brain.** A breached instance SA could try to forge
   `currentPrimary` (redirect `-rw`), `groupReplication.primaryMember`/`members`/
   `hasQuorum` (mask a real quorum loss or fake a different primary), or
   `bootstrapped`/`groupName` (arm a second group). Guard: under GR the status
   webhook (`design/020`, extended above) denies instance identities *every*
   status write — the operator is the sole writer of `currentPrimary` and the
   whole `groupReplication` block — and enforces the monotonic/immutable
   invariants on `bootstrapped`/`groupName` for all callers.
2. **Operator fights the group's election.** Guard: `reconcileFailover` disabled
   for GR; the operator only mirrors the group's reported PRIMARY, never writes a
   failover `targetPrimary`.
3. **Manufactured quorum loss via disruption.** Guard: quorum-aware PDB, and
   scale-down/fence refuse to drop below `floor(N/2)+1`.
4. **Unsafe quorum recovery / data loss on re-bootstrap.** Guard: `force_members`
   and total-outage re-bootstrap are opt-in and confirmed, choose the
   most-advanced member by GTID dominance, and refuse when safety is unprovable;
   the cluster stays `Blocked` and surfaced rather than guessing.

## Decisions

- **Single-primary mode only in v1.** Multi-primary is reserved in the API but
  webhook-rejected.
- **Topology mode is immutable;** default `async`. Existing clusters are wholly
  unaffected.
- **Operator observes, group decides.** No operator-side candidate selection,
  fencing-to-promote, `failoverDelay`, or primary Lease under GR.
- **Self-report over mTLS, operator is the sole K8s status writer under GR.**
  Instances report their group role on the existing mTLS `/status` channel; the
  operator cross-validates against the ONLINE majority's group view and is the
  only writer of `currentPrimary` and `groupReplication.*`. Instances get zero
  Kubernetes status-write capability on GR clusters (the only thing they patch is
  an advisory doorbell annotation on their *own* Pod). Convergence is event-driven
  (§Change detection), not poll-driven, so the timed requeue is a backstop only.
- **Distributed recovery is Clone-first** (8.0.17+), with optional XtraBackup
  pre-seed as a large-dataset optimisation.
- **Minimum server version 8.0.22** for GR (stable auto-rejoin + consistency).
- **Quorum recovery is never automatic** — detect, surface, and offer a guarded
  opt-in action.
- **Strategy split** keeps async and GR isolated so the proven path cannot
  regress.

## Open questions

1. **Recovery seeding default** — Clone-only first (simpler, GR-native) vs always
   offering XtraBackup pre-seed (faster for big datasets, more moving parts)?
   Plan assumes Clone-first; pre-seed as a follow-up optimisation.
2. **Quorum recovery UX** — annotation-only, kubectl-only, or both? Plan assumes
   both (annotation is the source of truth; the plugin is the ergonomic front).
3. **Even instance counts** — warn only, or reject? Plan warns (some users
   legitimately run 2 for cost during a scale-up window).
4. **`group_replication_consistency` default** — `BEFORE_ON_PRIMARY_FAILOVER`
   (read-your-writes on failover, low overhead) vs `EVENTUAL` (CNPG-like async
   feel)? Plan defaults to `BEFORE_ON_PRIMARY_FAILOVER`.
5. **Migration path async↔GR** — out of scope here; design later as a
   clone-into-new-cluster operation, since the mode is immutable.

## Verification (definition of done for the milestone set)

- A `mode: groupReplication` cluster bootstraps a single group exactly once,
  scales to N ONLINE members, and routes `-rw`/`-ro`/`-r` by group role.
- Planned switchover moves the primary via `set_as_primary`; `-rw` follows.
- Killing the primary triggers a **group** election; the operator mirrors the new
  primary without ever selecting a candidate, and never double-bootstraps.
- Fencing removes a member from the group (quorum-guarded) and rejoins on unfence.
- Quorum loss is detected and surfaced `Blocked`; the guarded recovery restores
  the group with no data loss.
- Semi-sync, the primary Lease, and the async failover loop are inert for GR;
  `mode: async` behaviour and its full test suite are unchanged.
- `make generate manifests`, `make lint-fix`, `make test`, integration, and the GR
  + async-regression E2E suites pass on the 8.0/8.4/9.x matrix; `docs/` updated.
```
