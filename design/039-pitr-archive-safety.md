# 039 — PITR archive safety: fencing, gaps and dead-branch backups

Status: accepted (2026-10-07)

Related: 009 (binlog streaming), 011 (raw S3 recovery), 017 (primary lease),
034 (purge replica floor), 036 (separate binlog storage), 038 (archive seams,
fork detection).

## Problem

Design 038 made the archive record dead branches and made replay leave them
out. A review of the result against "every restore either recovers a state the
cluster served or fails closed" found cases it does not cover. Two were
reproduced with unit tests before this change.

1. **A demoted primary can mark its successor's history as dead.** The loop
   checks writability once per tick, and a pass reads the fork-check authority
   once and reuses it. A pass that straddles a failover (large file, slow store,
   backlog) writes the index after the successor started archiving and judges
   the successor's segment against its own `gtid_executed`. The successor's
   transactions become a fork record; records only grow, so every backup taken
   on the successor fails with `ErrBackupOnDeadBranch` until retention drops a
   segment that never empties. MariaDB has the same race through a stale
   Cluster view.
2. **One failed index write drops a file from the index for good.** The status
   object is written before the index. When the index write fails, the next
   pass finds the manifest and the status coverage already in place, so nothing
   advances and the file is never indexed, even while it is still on disk.
3. **MySQL replay has no gap detection.** The coverage check in
   `planReplayWithOps` cannot fail (the frontier contains every segment by
   construction), and only `targetGTID` checks containment. A hole is replayed
   over silently. Holes come from (2), from a successor provisioned by clone
   whose clone point the archive never received, and from
   `binlog_expire_logs_seconds` expiring a binlog that was never archived.
4. **Some holes can never be filled.** The drain is the only repair for the
   clone-point hole, and it is blocked when the former primary is diverged,
   when a file mixes the hole with a dead tail, or when the volume is gone.
5. **A backup on a dead branch is invisible when the dead tail never reached
   the archive.** The anchor check consults fork records, which only exist for
   archived content. Retention can also drop the forked segment while a
   dead-branch backup is retained, erasing the only record.
6. **A disowned transaction can become canonical again.** MySQL divergence is
   only computed against a live primary, and the failover election measures
   missing transactions, not errant ones. A returning former primary that was
   never compared can be promoted. The fork record on its own segment then
   makes every new backup fail.
7. **Backup selection ignores the target.** Raw-S3 recovery takes the newest
   backup whatever `targetTime` says, so a target before it silently restores a
   later state, and a dead-branch newest backup fails the recovery although an
   older one would work. A named backup is not checked against `targetTime`
   either.
8. **The MariaDB timeline lives only in Cluster status.** Restore cannot read
   it, and recreating the Cluster without its status resets it.
   `gtid_domain_id` is not reserved, so a user can make the archive
   multi-domain, where any fork makes time/latest recovery fail closed.

Smaller: no periodic re-check on an idle primary; no MySQL restore-time
fallback for unrecorded forks; a `targetTime` past the end of the archive
succeeds with less data; MariaDB time targets apply `--stop-datetime` per
chunk, which can replay a non-prefix; a primary that starts with transactions
in its active log never rotates them out while idle.

## Goal

Every point-in-time recovery either recovers a state the cluster served, or
fails with an error naming why. Faults that would make a later recovery fail
are surfaced on the Cluster while there is still time to act.

## Design

### 1. Fenced fork checks

`ClusterStatus` gains `currentPrimaryGeneration` (int64). The instance that
records itself as `currentPrimary` increments it in the same patch; under Group
Replication the operator does. The status webhook lets an instance change it
only together with `currentPrimary`, and only to the old value plus one, so it
is monotonic and names exactly one primary.

`ArchiveIndex` gains `generation`, the highest generation of a primary that
wrote it. The archiver's primary path runs only while its Cluster view names it
`currentPrimary`, and stamps every index write with that view's generation.
Inside every index read-modify-write attempt, a writer whose generation is
below the index's runs no fork check. The authority (`gtid_executed`, or the
timeline) is read inside the same attempt, after the index, and the MySQL
source returns no judge when the server is not writable.

A demoted primary's in-flight pass therefore either writes before its
successor's first write (no successor segment to misjudge) or reads an index
stamped with a newer generation and skips the check. A successor's segment is
always created by a write stamped with the successor's generation, because the
successor does not archive until its view names it primary.

### 2. Index reconciliation

The archiver keeps, per process, the set of its files known to be in its index
segment, seeded from one index read on the first pass and after any failed
index write. A file whose manifest exists but is missing from the segment counts
as advanced, so the pass folds it into the index. This repairs both a crash
between the status and index writes and an exhausted compare-and-swap retry.

### 3. MySQL gap detection at restore

`GTIDSet.Holes()` returns, per UUID, the ranges between its intervals.

- Plan time, latest: holes of `anchor ∪ planned segments` that are neither
  holes of the anchor nor recorded forks fail with `ErrArchiveGap`.
- After replay, for every target: the temporary server's `gtid_executed` must
  have no hole the anchor and the fork records do not explain, and must contain
  the `targetGTID` when one is set. Otherwise `ErrArchiveGap`.

The post-replay check covers every hole source, including ones the index
cannot see, and only fails when the replay actually crossed the hole: a time
target that stops before it is fine.

### 4. Archive gaps at archive time

The primary's check also computes the archive's gaps: MySQL, the holes of the
index's covered set; MariaDB, the gaps in the union of segment ranges per
domain. They are reported in archiving status and mirrored into
`continuousArchiving.gaps`. The `ArchiveGap` condition is True while a gap is
not contained in the newest completed backup's anchor, with a Warning event on
the False→True transition. On that transition, when the cluster has a backup
object store, the operator creates a Backup (`<cluster>-archive-gap-<hash>`,
owned by the Cluster) so recovery to latest works again as soon as it
completes.

To make the clone-point hole rarer, instance status reports `gtidPurged` and
the failover election prefers, among equally advanced candidates, one whose
`gtid_purged` the archive covers (status `continuousArchiving.coveredGTIDSet`).
Failover is never blocked on archive continuity: availability wins, and the
gap is then detected and repaired by the backup above.

### 5. Dead-branch backups

- MySQL physical backups record their anchor (`metadata.anchorGTID`) from
  xtrabackup's binlog-position line, as MariaDB backups already do. On
  completion the operator copies the anchor into `Backup.status.endGTID`.
- `ArchiveIndex` gains `disowned`: the MySQL set (`gtidSet`) and MariaDB ranges
  (`ranges`, `{domain, server, after, through}`) of every transaction ever
  recorded as disowned. The primary folds each fork record into it, the
  operator folds dead-branch backup anchors into it, and retention copies it,
  so it outlives the segments.
- The operator judges every completed physical Backup of the cluster against
  the writable primary (MySQL `anchor \ gtid_executed`, MariaDB the timeline
  verdict) and sets the Backup's `DeadBranch` condition.
- Replay's anchor check consults `disowned` as well as the planned segments'
  fork records.

### 6. Regained transactions

MySQL fork records and the `disowned` set become `(record ∪ new) \ executed`
under a current-generation authority, own segment included, so a transaction
the surviving timeline holds again is no longer excluded. The fencing in §1 is
what makes retraction safe. MariaDB records keep growing only: a promoted
instance that holds a dead MariaDB transaction is already prevented by the
timeline verdict.

Divergence detection (MySQL) also marks any instance whose GTID set intersects
`continuousArchiving.disownedGTIDs`, including while the primary is
unreachable, so a returning former primary that holds an archived dead
transaction is never a failover candidate.

### 7. Backup selection and targets

- Raw-S3 recovery picks the newest backup whose `completedAt` is at or before
  `targetTime`, whose anchor is contained in `targetGTID`, and, for time and
  latest targets, whose anchor holds nothing in the archive's `disowned` set.
  Anchors that are unknown (legacy metadata) are not judged.
- A named Backup is rejected when `targetTime` is before its completion.
- `ArchiveIndex` gains `archivedThrough`: the primary stamps the time before
  which every committed transaction is archived (its last flush once every
  rotated file is shipped, or now when nothing was written since). A
  `targetTime` after it fails with `ErrTargetBeyondArchive` instead of silently
  recovering less.

### 8. MariaDB timeline in the archive

The primary writes the Cluster's timeline into `ArchiveIndex.mariadbTimeline`
on its fenced writes, keeping older epochs the index has that the Cluster no
longer holds. The archiving status reports it, and the operator seeds an empty
`status.mariadbTimeline` from it. Restore judges each segment against it before
planning, so forks the live check missed are cut, not only detected.
`gtid_domain_id` joins the denied parameters.

### 9. Smaller fixes

- The primary re-runs the fork check every flush interval, so an idle cluster
  still catches a late upload, and stamps `archivedThrough` on the same write.
- MySQL restore backstop: for time/latest recovery, when a later segment
  re-logged part of an earlier segment's own transactions and authored its own
  afterwards, the earlier segment's own transactions beyond the re-logged ones
  are a fork; unless recorded, recovery fails with `ErrForkedTimeline`. A later
  segment that holds none of them, and an earlier segment that re-logged the
  later one's transactions (failback), give no verdict.
- MariaDB time targets become a sequence target: the highest sequence before
  the first transaction, in sequence order, stamped at or after the target, so
  multi-chunk replay stays a prefix. MySQL's single `mysqlbinlog` stream
  already stops at the first event at or after the target, which is a prefix.
- The flush baseline starts unknown, so a primary whose active log holds
  transactions from before it started rotates them out after one interval.

## Known limitations

- A store without conditional writes can still lose an index write to a
  concurrent writer; §2 and the periodic check repair it on the next pass.
- Gaps below the oldest archived transaction of a UUID (or domain) are not
  detected at archive time; the restore-time check still fails closed on them.
- A backup with no recorded anchor (legacy metadata) is not judged before
  restore; replay still fails closed on recorded forks.
- The restore backstop in §9 has no verdict when the successor's re-logged
  binlogs expired before they were archived.
