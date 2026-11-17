# 041 — Primary-only binlog archiving on MySQL

Status: accepted (2026-11-17)

Related: 009 (binlog streaming), 034 (purge replica floor), 038 (archive seams,
fork detection), 039 (PITR archive safety).

Supersedes, for MySQL only: 038 §5 "Drain gate" and its "Drain purpose" note,
and the grace period of 039 §4. MariaDB is unchanged.

## Problem

Every transaction should be stored in the archive once. Two paths store it
again.

1. **A promoted replica uploads its whole local history.** With
   `log_replica_updates` a replica re-logs everything it replicates. On
   promotion `ArchivePending` ships every closed binlog on its disk, including
   the files that only re-log what the old primary already archived. Each
   failover adds up to one `binlogExpireSeconds` window of duplicates.
2. **A demoted primary drains.** It keeps writing binary logs as a replica, and
   the drain (038 §5) shipped them all. Filtering the drain against the index's
   coverage fixed the volume but kept a second archiver.

The second archiver is the larger cost. The index is one object updated by
read-modify-write, and on a store without conditional writes every extra writer
is a source of lost index entries. The drain also needs four gates, a memo of
the files it left out, and its own status accounting, each of which has had
bugs (`PendingFiles` growing on a healthy replica, a stranded tail dropped from
the index and never restored).

## Decision

On MySQL only the writable primary archives. A non-writable instance does not
upload binlogs and does not write the index.

This is sound because promotion defines the timeline. Whatever the old primary
committed and the new primary never received is disowned (038) and must not be
archived. Whatever the new primary did receive, it re-logged into its own
binlogs, and it archives those itself.

### 1. No drain on MySQL

`Loop.tick` calls `drain` only on MariaDB. `DrainOnlyUncovered`,
`judgeDrainFile`, the `unneeded` memo and the coverage cache are removed.
MariaDB keeps the drain and its four gates as they are: its replay is
positional, and this design does not re-examine it.

### 2. A primary skips files the archive already covers

Before uploading a closed binlog that has no manifest, the primary compares the
file's GTID set with the index's `coveredGTIDSet`:

- every transaction of the file is covered: the file is not uploaded;
- anything else: the file is uploaded whole, as today.

A file that straddles the old primary's last archived transaction is uploaded
whole, so a failover duplicates at most one file. A file with no transactions
is uploaded as today.

A skipped file advances the pass's frontier like an archived one, so the purge
gate and `PendingFiles` treat it as done. It is not added to the segment and
writes nothing.

The coverage is read from the index at most every five minutes, and forgotten
when the instance stops being writable. A stale read can only understate
coverage, which uploads a file that was not needed; it can never skip one that
was. A file left out stays memoized with its scan and is judged again on every
pass, so it ships if the coverage shrinks (retention dropping a whole segment).

Recovery then reads those transactions from the old primary's segment. A file
there may mix them with transactions the timeline disowned; replay already
handles that with `--exclude-gtids` (038).

### 2a. A failback stays recognisable at restore

Restore has a backstop for forks no primary recorded (`checkUnrecordedForks`).
It told a failback (A, then S, then A again) from a dead branch by A's segment
holding S's transactions, which A re-logged while it was S's replica. With the
skip, A leaves those files out, and the backstop refused a legitimate failback
with `ErrForkedTimeline`.

Each manifest now records the file's Previous-GTIDs set (`previousGTIDs`), and
each MySQL segment records `seenGTIDSet`: everything its server had executed by
the end of the files it archived. A segment that has seen its successor's
transactions came back after it, whether or not it archived them. Older
manifests and segments lack the fields and behave as before.

### 3. Coverage that outlives its files (accepted limitation)

The skip trusts `coveredGTIDSet`. Retention keeps a segment's set when it
expires old files, which is what makes a local file holding only expired
transactions be skipped instead of uploaded and expired again.
`DropDanglingIndexEntries` does the same when it removes an entry whose
manifest is gone, and most such entries are expired files a stale index write
brought back.

The cost: when an archived file inside the recovery window disappears from the
store, its transactions stay covered, so a primary that still has the file
locally does not upload it again. Shrinking the coverage there was considered
and rejected: the manifest that would say what to subtract is the object that
is missing, and recomputing a segment's set from its remaining manifests also
uncovers every expired file, which a promoted replica would then upload on each
restart for retention to delete. Recovery still fails closed across such a
file (039 §3, post-replay check).

### 4. Holes the primary cannot fill

Two cases leave transactions that the new primary holds as data but in no
binlog file:

- it was seeded (clone, backup) past the old primary's last archived
  transaction and promoted before the old primary archived the rest;
- archiving was failing for longer than `binlogExpireSeconds` and the replica's
  copies expired.

The drain used to fill the first once the old primary rejoined. Now neither is
filled. Nothing is lost in the running cluster; the archive has a gap, and
recovery from a base backup older than the gap cannot cross it.

039 §4 already covers this: the primary reports the gap, the `ArchiveGap`
condition turns True, and the operator takes a base backup past it. On MySQL
the grace period no longer waits for a drain: it drops from seven minutes to
two, the time an index entry lost to a racing write needs to be restored. Recovery to a point inside the gap, or across it from
an older backup, fails closed with `ErrArchiveGap`. The election preference of
039 §4 (prefer a candidate whose `gtid_purged` the archive covers) still makes
the first case rarer.

## Alternatives considered

- **Keep the drain as a hole filler.** It repairs the clone-point hole without
  waiting for a backup. Rejected: it keeps the second index writer and its
  gates for a case the automatic backup already resolves.
- **Start a promoted primary's segment at its promotion point.** Simpler than a
  coverage test, but it drops the old primary's unarchived tail, which only the
  new primary's re-logged files carry.

## Changes

| Area | Change |
|------|--------|
| `binlog/loop.go` | `tick` drains on MariaDB only |
| `binlog/archiver.go` | coverage skip in the primary pass; drain-only filter removed |
| `binlog/replay.go`, `objectstore/binlog.go` | `seenGTIDSet` on segments, `previousGTIDs` on manifests, used by the restore-time failback check |
| `binlog/replay_order.go` | a file with no transactions is placed by time, not replayed first; a file whose first transaction is already applied is ready; unknown times sort last |
| `binlog/drain_uncovered.go` | removed |
| `binlog/archiver.go` | `updateIndex` restores dropped files before folding the new one, so a stale read that lacks the whole segment restores it |
| `binlog/covered.go` | new: coverage read and comparison |
| `controller/cluster_archive_gap.go` | two-minute grace period on MySQL |
| `docs/src/pitr-internals.md` | drain section marked MariaDB-only; primary skip and the hole cases described |

## Testing

- Unit (`covered_test.go`): a promoted primary with re-logged files uploads
  only the straddling file and its own; the frontier passes the skipped file;
  nothing is written when everything is covered; a skipped file is decoded once
  and ships once the coverage shrinks; no coverage ships everything; MariaDB
  ships everything.
- Unit (`loop_test.go`, `drain_gate_test.go`): a non-writable MySQL instance
  performs no store writes; the drain gates run on MariaDB.
- e2e (`archive_duplicates_test.go`): after a failover the archive holds no
  file of the demoted primary that only re-logs, and no file of the promoted
  replica that only repeats what the old primary archived; PITR across the
  failover restores every row.
- e2e (`pitr_safety_test.go`, existing clone-gap spec): a replica cloned past
  the archive frontier and then promoted raises `ArchiveGap` and gets a base
  backup; recovery to latest works once it completes.
