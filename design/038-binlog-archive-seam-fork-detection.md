# 038 — Binlog archive seams: fork detection and divergence-safe PITR

Status: accepted (2026-10-06)

Related: 009 (binlog streaming), 026 (MariaDB support), 034 (purge replica
floor).

## Problem

The archive stores one segment per server UUID and stitches them by GTID at
recovery. Nothing checks the seam between segments: whether the instance that
took authority actually holds what the archive already contains. Four
consequences:

1. **Errant transactions can sit in the archive and be resurrected by PITR.**
   If a primary crashes after a file containing transaction `1:219` was already
   rotated and uploaded, and the lagged successor promotes at `1:218`, that
   file stays in the old segment. `PlanReplay` replays it: the recovered cluster
   ends at `{1:1-219, 2:1-300}` while the live cluster served
   `{1:1-218, 2:1-300}`. Either replay fails on a conflict between `1:219` and
   a later `2:*`, or it succeeds and the recovered state never existed. MariaDB
   is worse: the successor reuses the sequence numbers (`0-1-219` dead,
   `0-2-219` live), and the positional planner, which orders by sequence alone,
   can splice the two branches.
2. **The replay-time fork check cannot fire.** `planReplayWithOps` rejects when
   the index's declared coverage is not reconstructable from the segments, but
   `updateIndex` defines coverage as the union of the segments, so the check is
   a tautology. `ArchiveSegment.HandoffGTID` (design 009) was never written and
   never read.
3. **The drain can ship a dead branch on MySQL.** The drain ships a demoted
   primary's stranded tail once replication streams, arguing that acceptance
   proves ancestry. That argument holds only for MariaDB, whose
   `MASTER_USE_GTID=current_pos` handshake refuses diverged instances (1236).
   MySQL's `AUTO_POSITION` accepts errant transactions, and a returning primary
   can self-configure as a replica in the window before the operator marks it
   diverged (`status.divergedInstances` is only computed once it is reachable
   and comparable). Its tail then enters the archive through the drain.
4. **MariaDB cannot see a fork at all.** The engine's MariaDB GTID model is a
   *position*, the highest sequence per domain, compared by sequence alone with
   the server id ignored. Once the successor commits `0-2-219`, the dead
   `0-1-219` compares as contained. The same model drives
   `detectDivergedReplicas`, so a forked former primary is never marked
   diverged: it shows up only as `ReplicationBroken` after 1236, which
   `candidateEligible` does not exclude, and a later failover can promote it
   and resurrect its dead branch on the live cluster.

Design 009 anticipated the archive side ("refuse to straddle a forked
timeline") but the mechanism was never built.

## Goal

- Detect at archive time when the archive holds transactions the current
  authority never executed, and record precisely which, on the segment that
  holds them. Keep checking, so a fork that lands late or appears after a
  failback is still caught.
- Make replay of `targetTime`/latest exclude recorded dead-branch transactions,
  so a forked archive recovers the state the cluster actually served, while an
  explicit `targetGTID` can still recover the branch deliberately.
- Make it impossible — not merely unlikely — for the drain to add a
  disowned transaction to the archive, on both flavors.
- Give MariaDB the history its positions lack, so a forked MariaDB instance is
  marked diverged and never promoted, and so the archive check works the same
  on both flavors.

Non-goal: recovering, through the drain, transactions the surviving timeline
disowned. The drain keeps its other job, filling canonical holes; see "Drain
purpose" below.

## Decisions already taken

- Replay default for `targetTime`/latest is **exclude**, not refuse: the
  recovered state equals the surviving timeline's history, and an
  `ArchiveForked` condition + Warning event make the exclusion loud.
  `targetGTID` is the operator's explicit choice and is never rewritten.
- MariaDB fork handling, including divergence detection, is in the same change,
  not a follow-up.

## Design

### 1. Fork records

`ArchiveSegment.HandoffGTID` is removed (never written). In its place:

```go
// ArchiveSegment gains:

// Fork records the transactions this segment archived that the surviving
// timeline does not hold. Whichever primary detects them writes it; it only
// ever grows, and it leaves the index with the segment when retention drops
// the segment.
Fork *ArchiveFork `json:"fork,omitempty"`

type ArchiveFork struct {
	// GTIDSet (MySQL) is the disowned subset of the segment's GTIDSet.
	GTIDSet string `json:"gtidSet,omitempty"`
	// AfterSeq (MariaDB) maps a replication domain to the sequence the
	// surviving timeline inherited: every transaction this segment holds in
	// that domain with a higher sequence is disowned.
	AfterSeq map[uint32]uint64 `json:"afterSeq,omitempty"`
	// AuthorityGTIDSet is what the check compared against (MySQL
	// gtid_executed, MariaDB the primary's position). Audit only; replay never
	// reads it.
	AuthorityGTIDSet string    `json:"authorityGTIDSet,omitempty"`
	DetectedAt       time.Time `json:"detectedAt"`
	// DetectedBy is the instance name and archive identity of the primary
	// whose check found it.
	DetectedBy string `json:"detectedBy"`
}

// ArchiveIndex gains:

// ForkCheck records the last fork check a primary ran over the whole index.
// nil means no primary has checked this index since the change shipped.
ForkCheck *ArchiveForkCheck `json:"forkCheck,omitempty"`

type ArchiveForkCheck struct {
	CheckedAt time.Time `json:"checkedAt"`
	CheckedBy string    `json:"checkedBy"`
}
```

The record sits on the segment that **holds** the dead transactions, not on the
segment of the primary that found them, for two reasons: replay works per
planned segment, and retention (`rewriteIndex`) drops a segment once all its
files are gone, so the record and the dead files leave together. `rewriteIndex`
copies segments whole, so `Fork` survives partial retention untouched, but it
rebuilds the index from named fields, so it must be taught to copy `ForkCheck`.

### 2. Fork check (MySQL)

The writable primary runs the check:

- on every pass that writes the index (it already holds a freshly read copy),
  and
- on the first writable pass of its process, even with nothing pending, so a
  promotion, failback or restart checks the archive without waiting for the
  next rotation. This pass writes the index only if a record grew or
  `ForkCheck` is nil.

After loading the index it reads `@@GLOBAL.gtid_executed` and, for every
segment other than its own, computes `seg.GTIDSet \ executed`. Anything not
already in `seg.Fork.GTIDSet` is unioned in, and `ForkCheck` is updated on the
write. Its own segment is skipped: it holds only its own binlog, which its
executed set contains.

The rule needs no promotion-time coordination because of three properties:

- **Executed sets only grow** within an incarnation. `gtid_executed` shrinks
  only with `RESET MASTER` / `RESET BINARY LOGS AND GTIDS`, which the instance
  manager issues only when preparing a fresh Group Replication member
  (`PrepareGroupJoin`), never on a primary. A re-clone or rejoin mints a fresh
  `server_uuid` (`removeAutoCnf`), so no incarnation reuses another's GNOs.
- **Disowned GTIDs are never regained**, as long as a diverged instance is
  never promoted. Replication is pull-only: an instance's executed set grows
  with its own writes (fresh UUID) and with what its source's binlogs hold. The
  only holder of a disowned transaction is the instance that diverged,
  `candidateEligible` refuses to promote it, and nothing replicates from a
  non-primary. So reading `gtid_executed` later can never hide a fork, and
  reading it earlier can never invent one. The check can run at any time, any
  number of times.
- **No false positives.** Each successor's executed set contains what it
  received, so whatever a past primary committed and the chain received is in
  the current primary's executed set. What remains is exactly what some past
  primary committed and the chain never received. Archived content is a subset
  of what its primary committed (only rotated files ship), so the record is
  always a subset of the truly disowned set. This still holds after retention:
  segment `GTIDSet`s are not trimmed when files are deleted, so the check only
  revisits content that was archived at some point.

An earlier draft stamped a seam once per segment, at creation. Repeating the
check covers what a single stamp misses:

| Case | Why a single stamp misses it | Repeated check |
|------|------------------------------|----------------|
| Failback: A → S → A, S crashed with `2:301` uploaded, A promoted at `2:300` | A's segment already exists, so no new stamp | A's first writable pass records `{2:301}` on S's segment |
| Late upload: the old primary's `ArchivePending`, started while writable, finishes after its lease expired and the successor stamped | Stamp already written | The successor's next pass |
| Lost write: the record's index write loses the read-modify-write race with a drain or retention | Needs a separate late-stamp rule | Re-detected next pass; the GTIDs are still disowned |
| Archive written before this change | `Seam == nil` forever | Checked by the first primary pass after upgrade |

Cost: one `SELECT @@GLOBAL.gtid_executed` and one set difference per segment,
on passes that already read and write the index.

### 3. MariaDB: the primary timeline

A MySQL GTID names its author (the UUID), so a set says exactly which
transactions an instance holds. A MariaDB position says only how far each
domain got, and the author of the *last* transaction. What is missing is who
authored each stretch of sequence numbers on the surviving timeline. Only one
instance authors in a domain at a time (the primary), and the operator sees
every change of primary, so it can record that history directly, the way a
PostgreSQL timeline history file does.

```go
// ClusterStatus gains (MariaDB only):

// MariaDBTimeline records each change of primary, oldest first. Entry i says
// that, in every domain d, the transactions after Handoff[d] up to the next
// entry's Handoff[d] were authored by ServerID. The oldest entry is pruned
// only once nothing still needs it (see "Pruning").
MariaDBTimeline []MariaDBEpoch `json:"mariadbTimeline,omitempty"`

type MariaDBEpoch struct {
	// Instance is the primary of this epoch.
	Instance string `json:"instance"`
	// ServerID is its @@server_id, the server component of the GTIDs it
	// authors.
	ServerID uint32 `json:"serverID"`
	// Handoff is its @@gtid_slave_pos when it took authority: per domain, the
	// last transaction it inherited.
	Handoff string `json:"handoff,omitempty"`
	// Since is when the operator first observed it as primary.
	Since metav1.Time `json:"since"`
}
```

**Recording.** The instance manager's status reports, on MariaDB, its
`gtidSlavePos` and `serverID`. When the status reconcile first observes a
writable primary whose name differs from the last epoch's (failover,
switchover, failback, or the first primary of a cluster with no timeline yet),
it appends an epoch, before computing divergence in the same pass.
`gtid_slave_pos` is the right handoff, and reading it late is harmless:
promotion stops replication, and a primary's own writes advance
`gtid_binlog_pos`, not `gtid_slave_pos`, so on a primary it stays at what it
inherited. The first epoch of a timeline is the exception: its primary did not
necessarily author the history it holds (a cluster bootstrapped from a backup
holds the source cluster's transactions under other server ids, a cluster
upgraded mid-life holds history no epoch observed), so its handoff is the
primary's `gtid_current_pos` at first observation, and everything before it
gets no verdict.

**Verdict.** For a GTID `d-s-n`, find the epoch whose range in `d` contains `n`:

- that epoch's `ServerID` is `s` → **on the timeline**;
- it is another server → **off the timeline** (a dead branch);
- `n` is at or below the oldest epoch's `Handoff[d]`, or falls in a stretch the
  timeline marks unknown (below) → **no verdict**, and callers fall back to
  what they do today.

A stretch is unknown when a primary authored it but the operator never
observed it as primary, for example a successor that died within seconds of
promoting. The next epoch exposes the gap: its `Handoff[d]` names a server id
that is not the previous epoch's author. The range between the previous
epoch's last known sequence and that handoff gets no verdict.

Because a domain's history is one linear chain, the last GTID per domain
settles the whole prefix: if an instance's last `d-s-n` is on the timeline,
everything it holds in `d` is too, and if it is off, everything after the
handoff that ended `s`'s authorship is dead. That is why one position per
domain is enough input, and why the record shape below is a suffix.

**Pruning.** Dropping the oldest entry raises the floor to the next entry's
`Handoff`: anything at or below it gets no verdict. So the operator drops the
oldest entry only when no position it still has to judge sits at or below
that new floor, in any domain. The positions it has to judge are:

- every instance's recorded position in `status.gtidExecutedByInstance`,
  diverged ones included (a diverged instance's mark clears on positive proof,
  and that proof must not fall back to a position comparison);
- the oldest segment position in the archive, which the primary's archiver
  reports after each fork check (`oldestSegmentPosition`, per domain, mirrored
  into `ContinuousArchivingStatus`). Retention moves it forward as it drops
  segments. When archiving is off, this reference does not apply.

An instance with no recorded position does not pin anything: it has never
been compared, and it will be judged against the timeline as it is when it
first reports. The rule runs in the same reconcile that appends epochs.

With nothing pinning old entries, a healthy cluster's timeline stays a handful
of entries long: one per change of primary since the oldest archived segment
or the most stale instance. A hard ceiling of 256 entries protects the Cluster
object regardless. Hitting it drops the oldest entry anyway and emits a
`MariaDBTimelineTruncated` Warning event naming what it still pinned, since
that history then gets no verdict.

The verdict lives in `pkg/engine` next to the MariaDB GTID model
(`MariaDBTimeline.Verdict(gtid)`), so the operator and the instance manager
share one implementation.

**Divergence detection.** `detectDivergedReplicas` on MariaDB marks a replica
diverged when position containment fails (as today) **or** when any of its
per-domain GTIDs is off the timeline. A forked former primary is now marked
as soon as it is reachable, whether or not it has tried to replicate, so
`candidateEligible` excludes it before any failover could pick it. As a
backstop for history with no verdict, a MariaDB replica whose I/O thread
stopped with error 1236 while its source is the current primary is also
marked diverged. That needs `ReplicationStatus` to carry `lastIOErrno`
(`Last_IO_Errno`), since today it only reports the error text. 1236 also
covers a replica whose position the primary already purged; that replica needs
a re-clone too, so the mark leads to the right outcome.

**Replica clusters** (`spec.replica`) do not record a timeline: their
designated primary replicates from the source cluster, so authorship in a
domain is not theirs to track. They keep position comparison and the 1236
backstop.

### 4. Fork check (MariaDB)

The MariaDB primary runs the check at the same moments as the MySQL one, with
the timeline from the Cluster view it already follows (`clusterFloor` exposes
`status.mariadbTimeline`). For every other segment and domain `d`, take the
segment's position `d-s-n`:

- off the timeline → record `AfterSeq[d]` = the latest point `s` could
  legitimately have reached before `n`: the `Handoff[d]` of the epoch that
  ended an epoch `s` authored, or a stretch whose successor inherited from `s`;
  without either, the timeline's floor (everything above it is attributed to
  another author, which keeps the check working once pruning dropped `s`'s
  epoch). Keep the lower value if one is recorded already;
- on the timeline, or no verdict → nothing.

This covers what a frontier taken from the checking primary alone could not:
a successor that died before writing the index leaves the fork for the next
primary, and the timeline still says who authored those sequence numbers. It
also exempts, without a special case, the primary's own transactions that a
draining former primary re-logged into its segment: they are on the timeline.

Within a domain, a segment's disowned transactions are a suffix: its instance
had them past the point the next primary inherited, and once marked diverged
(§3) it never streams, drains or promotes again, so its segment does not grow
past them.

### 5. Drain gate: ship only what the surviving timeline provably holds

The drain's authorization becomes four conditions, all required:

1. `HasSegment` — the instance archived while primary (unchanged).
2. `Streaming()` — the source accepted the instance's position (unchanged; on
   MariaDB the 1236 handshake makes this a strong signal, on MySQL a weak one).
3. Not listed in `status.divergedInstances` (unchanged, from the Cluster view).
4. **New:** the file is on the surviving timeline, judged from the same Cluster
   view the purge floor uses (`clusterFloor` also exposes
   `status.currentPrimary` and, on MariaDB, the timeline):
   - **MySQL:** the file's GTID set is contained in the recorded position of
     `status.currentPrimary` (`status.gtidExecutedByInstance`).
   - **MariaDB:** each of the file's per-domain GTIDs is on the timeline (§3)
     and contained in the current primary's recorded position. No verdict
     defers.

   A file that fails is deferred to the next tick, not shipped and not
   recorded, and the pass stops there: files ship in order, so a later file
   never moves `LastArchivedBinlog` past a deferred one. If the current primary
   is this instance, or its position is unknown, the drain defers.

**Why only the primary's position.** An earlier draft took the union over every
non-diverged instance (`ReplicaFloor.Positions()`). That union is not a subset
of the surviving timeline. Divergence is computed only for reachable instances,
and the operator keeps the last recorded position of an unreachable one. A
replica that received `1:219` from the old primary, had that position
recorded, and went down in the same outage stays unmarked with `1:219` on
record, and the union would let the old primary ship it. The current primary's
position needs no such argument, because it *is* the surviving timeline. It
costs no liveness either: a canonical transaction is by definition one the
primary executed.

**Safety.** On MySQL the primary's recorded position is a subset of its
executed set (`gtid_executed` only grows; a stale record only understates), and
its executed set never contains a disowned transaction (§2). On MariaDB
position containment alone is blind to forks (the dead `0-1-219` compares as
contained in `0-2-300`), which is what the timeline verdict adds; gate 2 still
stands behind it. Either way a shipped file never contains a disowned
transaction.

**Liveness.** A canonical tail is in the current primary's executed set, its
recorded position contains it after at most one refresh
(`gtidPersistInterval`, ≤5 minutes), and the file ships on the next drain
tick. On MariaDB the epoch for the current primary is recorded in the reconcile
that first sees it writable, so it is there before the position refresh. A
disowned tail is never on the timeline, so the gate defers forever and the dead
branch never enters the archive through the drain. Together with gate 3 this
closes the MySQL race's *effect*, not just its window. A MariaDB tail in a
stretch with no verdict defers too; that costs liveness only for history older
than the timeline, which gate 2 already governs.

**Adversarial interleavings.** With `M` = operator divergence mark:

| # | Scenario | Outcome |
|---|----------|---------|
| 1 | Clean failover, successor caught up | Tail re-logged; a drain (if any) passes gate 4 as overlap. No fork recorded. |
| 2 | Lagged promotion, tail un-rotated, old primary loses the boot race (`M` before self-configure) | Gate 3 blocks. Tail never archived. Checks find nothing. PITR = live history. |
| 3 | Lagged promotion, old primary wins the boot race (streams before `M`) | Gate 4 defers forever (the dead tail is never in the primary's position, nor on the MariaDB timeline); gate 3 blocks shortly after. The race's effect is closed, not merely detected. |
| 4 | `1:219` rotated and uploaded before the crash | The only path a disowned transaction reaches the archive. The successor's first writable pass records `{1:219}` on the old segment → replay excludes it for time/latest; `targetGTID` recovers it explicitly. |
| 5 | Failback, canonical tail: preferred primary returns, crashes again, drains a tail the interim primary re-logged but has not archived | Gate 4 compares against the interim primary's *position* (executed), not its archived sets, so the tail ships within one refresh and no fork is recorded. A check against archived sets would have deferred it, or recorded a false fork. |
| 6 | Failback, dead tail: A → S → A, S crashed with `2:301` uploaded, A promoted at `2:300` | A's first writable pass records `{2:301}` on S's segment (§2, §4). |
| 7 | An unreachable replica's stale position holds the dead tail | Not read: gate 4 uses the primary's position only. |
| 8 | Scale-down / unknown positions | Only the primary's position matters; unknown ⇒ defer until the next refresh. |
| 9 | Index write race (primary, drain, retention) | Every index write is a compare-and-swap (`If-Match` on the read ETag, `If-None-Match: *` to create); a loser re-reads and re-applies its change, so retention's drop of a forked segment is never undone by a stale copy. On a store without conditional PUTs (501), writes fall back to unconditional: a lost drain write re-folds on the next tick, a lost fork record is re-detected on the primary's next pass. |
| 10 | Group Replication | No async replication is configured, so `Streaming()` is false and the drain is inert. Each GR primary runs the fork check; transactions carry the group UUID, so a fork needs a forced-quorum split brain, which set semantics catch. |
| 11 | MariaDB successor dies before writing the index; R promoted from it | The timeline holds S's epoch (or marks its stretch unknown). R's check records the fork on the old segment if S's epoch was observed; otherwise the restore backstop (§6) fails closed. |
| 12 | MariaDB forked old primary returns, then the current primary dies | Marked diverged on first contact by the timeline verdict (§3), so `candidateEligible` never offers it to the failover. |

**Drain purpose.** The drain still fills canonical holes: transactions the
surviving timeline executed but never logged. A successor provisioned by clone
holds the clone point's history in its executed set but not in its binlog; if
the old primary dies before rotating, the transactions between its last
rotation and the clone point survive only in its active binlog. They are in
the current primary's position (and on the MariaDB timeline), so gate 4 passes
and the drain ships them.

What changes is that the drain no longer ships a tail the surviving timeline
does not hold, even before divergence is marked. That is a deliberate behavior
change. A disowned transaction is now recoverable only through case 4
(pre-crash upload, recorded fork, explicit `targetGTID`). The drain doc comment
in `loop.go` is rewritten accordingly: it keeps the hole-fill rationale and
moves the safety argument from "the source accepted us" to gates 2 and 4.

### 6. Replay consumption

The plan logs the fork records it applied and, when `ForkCheck` is nil,
`archive not fork-checked` (no primary has written the index since upgrade).
Neither is an error or a condition.

**MySQL** (`planReplayWithOps`). For time/latest, merge every planned segment's
`Fork.GTIDSet` into `ExcludeGTIDs` (`anchor ∪ forks`, via
`UnionGTIDStrings`); the motivating scenario recovers `{1:1-218, 2:1-300}`.
`targetGTID` applies the user's include-set only, so a target that names fork
transactions recovers them. A target that names both the fork and the
successor's later writes recovers a state that never existed, so the plan logs
a warning when the include-set intersects a fork.

**MariaDB.** Today only `targetGTID` uses the positional planner. Time/latest
concatenate files from the anchor position with `--stop-datetime`, which can
neither skip a fork nor notice one. So:

- `TxnBoundary` gains `Server uint32`, parsed from the same `GTID d-s-n` line,
  so a boundary names a transaction, not only a sequence.
- Time/latest run through `PlanMariadbPositional` too, with the target at the
  highest non-disowned sequence and `StopDatetime` applied to each chunk. The
  positional planner is single-domain; the operator does not set
  `gtid_domain_id`, so archives are single-domain by default. A multi-domain
  archive keeps today's path when no planned segment carries a fork, and fails
  closed when one does.
- A fork marks the **end** of its segment in that domain. Skipping disowned
  boundaries like already-applied ones is not enough, because the planner
  replays whole files and would replay a dead tail with the file. The file
  holding the first disowned boundary becomes its own chunk, with
  `StopPosition` at that boundary, and the segment's later files in the domain
  are dropped.
- A target whose `(server, seq)` falls in a fork selects that branch: the
  holding segment is used up to the target and every other segment is cut at
  `AfterSeq`. A target on the surviving branch is unaffected. This is how
  `targetGTID` stays un-rewritten on MariaDB: the server id in the target
  already names the branch.
- **Backstop.** After scanning, if the downloaded files carry two different
  servers at the same `(domain, seq)` and no fork record explains it, fail
  with `ErrForkedTimeline` instead of splicing. Restore runs without the
  source Cluster's status, so it cannot consult the timeline; this catches
  forks the archive check had no verdict for.

`SelectMariadbSegments` is unchanged: forks are extra coverage, not holes, and
a superset selection is harmless because the positional planner decides what
replays. `ErrForkedTimeline` keeps its meaning (index incoherence) plus the
unexplained-author case; a recorded fork is data, not an error.

**Base backup on the dead branch.** If the anchor intersects a planned fork
(MySQL: `anchor ∩ Fork.GTIDSet ≠ ∅`; MariaDB: the anchor's `(server, seq)`
falls in a fork), the backup was taken on the dead branch, and replay cannot
remove what it already holds. Time/latest fail closed with
`ErrBackupOnDeadBranch`, naming the backup. `targetGTID` proceeds when the
target contains the anchor, as today.

### 7. GTID algebra

- `replication.GTIDSet.Difference(other) GTIDSet` — interval subtraction;
  `Contains`/`Union`/`MissingCount` exist, difference does not.
- `engine.MariaDBTimeline` with `Verdict(gtid) (onTimeline, known bool)` and
  `DeadAfter(gtid) (seq uint64, ok bool)` over `[]MariaDBEpoch` (§3). No
  MariaDB `Difference`: positions cannot express one.

### 8. Status and API

- `binlog.State` carries the fork records **as read from the index** on the
  primary's last check, not only those this process detected, so a restart or
  failover does not drop them: the next primary's first writable pass reads
  them back (§2).
- `webserver.ArchivingStatus` and `api` `ContinuousArchivingStatus` gain
  `forkGTIDs []string`, one entry per forked segment (the MySQL set, or the
  MariaDB range rendered `0-1-219..0-1-225` from `AfterSeq` and the segment's
  position), and `forkDetectedAt` (the earliest `DetectedAt`).
- A dedicated `ArchiveForked` condition, on the `BinlogPurgeHeld` pattern: True
  while any segment carries a fork record, with a Warning event on the
  False→True transition, False once retention drops the last forked segment.
  It is not folded into `ContinuousArchiving`: archiving keeps working, a fork
  can stay in the archive for the whole retention window, and a long-lived
  `ContinuousArchiving=False` would hide real archiving failures behind it.
- `ClusterStatus.mariadbTimeline` (§3). `webserver.Status` gains
  `gtidSlavePos` and `serverID` (MariaDB); `ReplicationStatus` gains
  `lastIOErrno`; `ArchivingStatus` and `ContinuousArchivingStatus` gain
  `oldestSegmentPosition` (MariaDB, for timeline pruning).
- `make manifests generate api-docs`.

## Code changes

| Area | Change |
|------|--------|
| `replication/gtid.go` | `GTIDSet.Difference` + tests/fuzz |
| `replication/reader.go` | `Last_IO_Errno`; MariaDB `@@gtid_slave_pos` and `@@server_id` |
| `pkg/engine` (MariaDB GTID model) | `MariaDBTimeline`: `Verdict`, `DeadAfter`, unknown-stretch inference, `Prune(references)` |
| `objectstore/binlog.go` | remove `HandoffGTID`; add `ArchiveFork`, `ArchiveSegment.Fork`, `ArchiveIndex.ForkCheck` |
| `objectstore/retention.go` | `rewriteIndex` copies `ForkCheck` (segments, and so `Fork`, are already copied whole) |
| `binlog/reader.go` | `ExecutedGTIDSet` (MySQL `@@GLOBAL.gtid_executed`), flavor identity-query pattern |
| `binlog/archiver.go` | fork check on index writes (MySQL set difference, MariaDB timeline); per-file allow predicate on `ArchivePending` that stops the pass at the first deferred file |
| `binlog/loop.go` | first-writable-pass check; drain gate 4 against the current primary's position (and the timeline on MariaDB); drain doc comment rewritten |
| `binlog/scan.go` | `TxnBoundary.Server` |
| `binlog/replay.go` | MySQL fork exclusion; MariaDB fork-as-segment-end, branch selection, unexplained-author backstop; `ErrBackupOnDeadBranch` |
| `instance/archiving.go` | wire `ExecutedGTIDSet` into `ArchiverOptions`; `clusterFloor` exposes `status.currentPrimary` and `status.mariadbTimeline` |
| `instance/restore_pitr.go` | MariaDB time/latest through the positional planner (single-domain) |
| `webserver/status.go`, `api/v1alpha1` | fork fields, `mariadbTimeline`, `gtidSlavePos`, `serverID`, `lastIOErrno`; `make manifests generate api-docs` |
| `internal/controller/cluster_status.go` | record and prune epochs before divergence (`MariaDBTimelineTruncated` event at the ceiling); mirror fork fields and `oldestSegmentPosition`; `ArchiveForked` condition + event |
| `internal/controller/async/cluster_observation.go` | MariaDB divergence: timeline verdict and 1236 backstop |
| `docs/src/pitr-internals.md`, `docs/src/replication-failover.md` | fork records, exclusion rule, drain behavior change, MariaDB timeline and divergence |

## Tests

- Algebra: `Difference` unit + fuzz (existing patterns in
  `gtid_fuzz_test.go`).
- Timeline: verdict on, off and below the oldest epoch; failback (same server
  id in two epochs); unknown stretch inferred from a handoff naming an
  unrecorded author; a re-cloned instance (same `server_id`, canonical
  position) is on the timeline.
- Pruning: the oldest entry is kept while an instance's recorded position, a
  diverged instance's included, or the oldest segment position sits at or
  below the next entry's handoff in any domain; dropped once both move past;
  an instance with no recorded position pins nothing; the archive reference is
  ignored with archiving off; the 256-entry ceiling drops the oldest entry
  and emits `MariaDBTimelineTruncated`.
- Epoch recording: appended on failover, switchover and failback, recorded
  before divergence in the same pass, not duplicated by a primary restart,
  absent on replica clusters.
- MariaDB divergence: forked former primary marked on first contact, before it
  replicates; marked by 1236 from the current primary when the timeline has no
  verdict; not offered to failover; cleared after re-clone.
- Fork check, MySQL: fork recorded on the holding segment (case 4); nested
  failover records nothing (case 1); first segment; failback records on the
  interim segment (case 6); late upload after the successor's first write;
  lost write re-detected; legacy index checked on the first pass; own segment
  skipped; a record only grows; retention keeps `Fork` and `ForkCheck`.
- Fork check, MariaDB: detected after the successor has written past the fork;
  detected by a later primary when the successor never wrote the index (case
  11); a drained re-log of the primary's own transactions is not a fork;
  `AfterSeq` keeps the lower value; no verdict records nothing.
- Drain gate: defers on a miss and ships once the primary's position covers
  the file (case 5); diverged blocks even while streaming; a stale position of
  an unreachable replica holding the tail does not authorize (case 7); unknown
  primary position defers; the pass stops at the first deferred file; deferred
  tail stays visible in `PendingFiles`; MariaDB off-timeline and no-verdict
  files defer.
- Replay, MySQL: time/latest exclude fork GTIDs; `targetGTID` unaffected (both
  including and excluding the fork); warning on a target that mixes branches;
  anchor on the dead branch fails closed.
- Replay, MariaDB: fork cut mid-file with `StopPosition`; later files of the
  forked segment dropped; a target inside the fork replays that branch and cuts
  the other; an unexplained duplicate author fails with `ErrForkedTimeline`; a
  multi-domain archive with a fork fails closed.
- Operator: `ArchiveForked` True + event on detection, still True after a
  primary restart and after a failover, False once retention drops the
  segment.
- e2e (separate step): the divergence scenario end to end — force rotation +
  archive on the primary, hard-kill it, promote a lagged replica, let the old
  primary rejoin diverged, reinit it, PITR to latest → recovered row set equals
  the live cluster's, `ArchiveForked` condition set. On MariaDB, also check the
  returning old primary is listed in `status.divergedInstances` before its
  replication is configured.

## Known limitations

- The MariaDB timeline starts when this change ships (or when a cluster is
  created or restored): history before the first epoch, and a stretch authored
  by a primary the operator never observed, get no verdict. There, divergence
  falls back to position comparison plus the 1236 backstop, the archive check
  records nothing, the drain defers, and the restore backstop fails closed
  rather than excluding.
- Pruning never removes history something still references, short of the
  256-entry ceiling. Reaching it takes 256 changes of primary while one
  instance stays unreachable or one archive segment survives retention.
  History dropped there gets no verdict, with the same fallbacks, and the
  `MariaDBTimelineTruncated` event says so.
- Replica clusters record no timeline (§3).
- A disowned transaction is recoverable only if it reached the archive before
  the crash (case 4) and only via an explicit `targetGTID`. The drain never
  ships one, by design.
- The index has three writers (primary, drain, operator retention). They
  write it with S3 conditional PUTs (`objectstore.UpdateArchiveIndex`), and a
  loser rebases its change on the winner's index. A store that does not
  implement conditional PUTs (501) gets unconditional writes, the behavior
  before this change: a lost write delays a fork record by one pass and can
  bring a segment retention dropped back until the next retention pass, but
  cannot ship a disowned transaction. A store that silently ignores the
  headers behaves the same way (see 027).
