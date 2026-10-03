# 037 — Usable slow query log

Status: proposed (2026-10-03)

Issue #177.

## Problem

`slow_query_log` can be turned on through `spec.mysql.parameters`, but
`slow_query_log_file` is denied, so mysqld writes `<hostname>-slow.log` into the
datadir:

- It sits on the data PVC, nothing rotates it, and it can grow until the volume
  fills.
- It never reaches `kubectl logs` or a log collector.
- There is no supported way to read the queries back without `exec`.

`log_output` is not managed either. `log_output=TABLE` sends the slow log to
`mysql.slow_log`, a CSV table on the data PVC with the same growth problem.

## Goals

- Per-cluster on/off, declared in the Cluster.
- Every slow query reaches the instance container's stdout as one structured
  record, so `kubectl logs` and any log collector pick it up. No `exec`.
- A hard bound on the space the slow log can use, enforced by the kernel. A full
  slow log never affects queries, never fills a PVC and never evicts the Pod.
- `long_query_time` and the other tuning parameters stay user parameters.
- Same behaviour on MySQL (8.0, 8.4, 9.7) and MariaDB (10.11, 11.4, 12.3).

## Non-goals

- The general query log. It can reuse this mechanism later; `general_log_file`
  stays denied.
- Query digests or aggregation. Collectors and `performance_schema` cover that.
- A configurable volume size. The size is a constant until someone needs it to
  change.

## Spike findings

All checks ran against the `ghcr.io/cnmsql/cnmsql-instance` (8.0, 8.4, 9.7) and
`ghcr.io/cnmsql/cnmsql-mariadb-instance` (10.11, 11.4, 12.3) images.

**MySQL refuses a FIFO.** Upstream `File_query_log::open` (`sql/log.cc`) rejects
anything that is not a regular file:

```c
/* File is regular writable file */
if (my_stat(log_file_name, &f_stat, MYF(0)) && !MY_S_ISREG(f_stat.st_mode))
    goto err;
```

mysqld logs `Could not use … for logging` and turns the slow log off. That rules
out a FIFO, `/dev/stderr` and a symlink to either. MariaDB accepts a FIFO, but
one mechanism for both engines is worth more than the FIFO's simplicity, so the
slow log is a regular file that the instance manager tails.

**Rename + `FLUSH LOCAL SLOW LOGS` rotates without loss** on all six images:

| entry written | lands in |
|---|---|
| before the rename | old inode |
| between the rename and the flush | old inode |
| after the flush | new file at the original path |

After the flush mysqld holds no descriptor on the old inode. A runtime
`SET GLOBAL slow_query_log=OFF/ON` recreates a deleted file.

**Plain `FLUSH SLOW LOGS` is written to the binary log** and consumes a GTID on
all six images. On a replica that is an errant transaction. `FLUSH LOCAL SLOW
LOGS` writes nothing to the binary log and works under `super_read_only`. The
manager only ever issues the `LOCAL` form.

**A full slow-log filesystem is harmless to queries.** With the volume at 100%,
queries keep their normal latency, mysqld stays up, logs one `Error writing file
… errno 28`, drops entries and keeps `slow_query_log=ON`. Freeing space and
flushing resumes logging. On MariaDB the first flush after the volume fills
returns the same error, and logging resumes regardless.

**A full shared run volume breaks MySQL restarts.** With `/var/run/mysqld` full,
a MySQL restart in the same Pod aborts with `Could not write unix socket lock
file mysqld.sock.lock errno 28`, and the manager cannot rewrite its pidfile.
MariaDB has no socket lock file and boots. The manager therefore keeps space
free in the volume (see *Volume budget*).

**Slow log formats differ** (samples captured on all six images):

- MySQL writes `# Time:` before every entry, as ISO 8601 UTC. MariaDB writes
  `# Time: YYMMDD HH:MM:SS` only when the second changes, so most MariaDB
  entries start at `# User@Host:`.
- A value can be empty: `# Schema:   Last_errno: 0` (MySQL),
  `# Schema:   QC_hit: No` (MariaDB).
- Body lines can start with `#`: `# administrator command: Quit;` is the
  statement text.
- `use shop;` (MySQL) and `` use `shop`; `` (MariaDB) can precede
  `SET timestamp=N;`.
- `log_slow_extra=ON` (MySQL 8.0.14+) puts ~30 extra pairs on the `Query_time`
  line. Percona's `log_slow_verbosity` and MariaDB's `log_slow_verbosity` add
  further `# Key: value` lines, some indented (`#   InnoDB_IO_r_ops: 0`).
- Each time mysqld opens the file it writes a three-line preamble:
  `<binary>, Version: … started with:`, `Tcp port: … Unix socket: …`,
  `Time  Id Command  Argument`. A runtime OFF/ON toggle appends it mid-file.

## Design

### Configuration surface

There is no new API field. `slow_query_log` (and MariaDB's `log_slow_query`)
stays a user parameter, applied at runtime by the existing `Reload`. So are
`long_query_time`, `log_slow_verbosity`, `log_slow_extra`,
`log_queries_not_using_indexes`, `log_slow_admin_statements`,
`min_examined_row_limit` and `log_slow_rate_limit`.

The renderer (`pkg/management/mysql/config`) adds two managed settings:

| key | value |
|---|---|
| `slow_query_log_file` | `/var/run/mysqld/mysqld-slow.log` |
| `log_output` | `FILE` |

MariaDB 10.11 renamed the variable to `log_slow_query_file` and kept
`slow_query_log_file` as an alias. The spike ran the alias on 10.11, 11.4 and
12.3, so the renderer writes one spelling for both engines. `ServerConfig` gains
`SlowLogFile string`, rendered only when set; the operator always sets it.

`slow_query_log_file` moves from `deniedKeys` to `managedKeys`, and
`log_slow_query_file` and `log_output` join it, so neither spelling can be
overridden on either engine. A cluster that already sets any
of them goes `phase: Blocked` with the usual reason naming the key. Setting
`log_output` was legal before this change, so the release notes call it out.

The temporary servers (initdb, join, import, restore and PITR restore) read the
same my.cnf. A dump import with `slow_query_log=ON` and `long_query_time=0`
would write an unread log into the Job container. Every temporary server
therefore starts with `--slow-query-log=OFF`, which both engines accept.

### The run volume

`run`, mounted at `/var/run/mysqld`, becomes:

```go
EmptyDir: &corev1.EmptyDirVolumeSource{
    Medium:    corev1.StorageMediumMemory,
    SizeLimit: ptr.To(resource.MustParse("32Mi")),
}
```

The kernel enforces the size: a write past 32 Mi fails with ENOSPC, which the
spike showed is harmless to mysqld. tmpfs pages are charged to the memory cgroup
of the container that writes them, which is the instance container. Normal use
is a few MiB and the worst case is the 32 Mi cap. The docs tell users to budget
for it in `resources.limits.memory`.

The volume and the rendered config are both part of the hashed Pod template, so
the operator upgrade that ships this design rolls every instance once with the
usual strategy: replicas first, then a switchover, then the primary. An instance
updated in place but not yet restarted keeps its disk-backed volume and its old
config. The new manager code runs unchanged there: nothing creates the slow log
file, so the tailer idles.

### Volume budget

| | size | owner |
|---|---|---|
| volume `sizeLimit` | 32 Mi | kernel |
| reserve | 4 Mi | sockets, `.sock.lock` files, pidfile and `.tmp`, FIFO |
| hard cap, all `mysqld-slow.log*` together | 24 Mi | watchdog |
| soft threshold, active file | 4 Mi | watchdog |

At most two files exist: the active `mysqld-slow.log` and a rotated
`mysqld-slow.log.1` that is still being drained. In normal operation they total
under 8 Mi.

### The instance manager: `pkg/management/mysql/slowlog`

A new package, wired from `instance/runner.go`. It has three parts.

**Parser.** `github.com/percona/go-mysql/log/slow` (BSD-3-Clause, maintained,
the parser behind Percona PMM's query analytics). It parsed every entry of every
fixture from the six images, including MySQL's ISO 8601 `# Time:`, MariaDB's
`# explain:` lines and `# administrator command:` bodies. It reports each
entry's byte `Offset`, which the tailer uses as its resume point.
`github.com/go-mysql/slowlog` was considered and rejected: it is GPL-3.0, it
loses the timestamp of every MySQL 8.x entry, and it copies MariaDB's
`# explain:` rows into the statement text.

The library parses a file from an offset to EOF and does not tail. Two of its
behaviours shape the tailer:

- At EOF it emits the entry it was reading, even if mysqld has not finished
  writing it.
- It drops a trailing line without a newline.

**Tailer.** A goroutine that polls every 250 ms. It keeps an open descriptor on
the file it reads and remembers `off`, the offset of the first entry it has not
emitted yet. On each poll:

- If the file grew since the last poll, it parses from `off` to EOF. It emits
  every entry except the last one, and holds the last one, because mysqld may
  still be writing it. `off` becomes the held entry's offset, so the next parse
  reads that entry again in full.
- If the file did not grow since the last poll, the held entry is complete
  (mysqld writes an entry under `LOCK_log` in one burst). The tailer emits it,
  and `off` becomes the end of the file.
- If the file is smaller than `off`, the watchdog truncated it. The tailer
  drops the held entry and starts again at offset 0.
- If the file does not exist (slow log off), the tailer waits.
- If the active path now names a different inode, a rotation is in progress.
  The tailer keeps reading its open descriptor. Once the watchdog reports the
  flush done and the old inode has stopped growing, the tailer emits the held
  entry, closes the descriptor, unlinks `.1` and opens the new active file at
  offset 0.

When it opens a file, the tailer prefers `.1` if one exists, so a pending
rotation is drained before the active file. A parser panic is recovered; if a
parse fails without producing anything, the tailer skips to EOF and counts the
bytes as dropped, so a malformed region cannot stall it.

**Watchdog.** A separate goroutine that runs every second and only uses `stat`,
`statfs`, `rename`, `truncate` and the control connection. It never waits on the
tailer, so the bound holds even when stdout is backpressured.

1. If the active file is ≥ 4 Mi and no `.1` exists: rename the active file to
   `.1`, then run `FLUSH LOCAL SLOW LOGS` on the control connection. If the
   flush fails (mysqld starting, connection lost), retry on the next tick.
   mysqld keeps writing into the renamed inode in the meantime, and the tailer
   keeps reading it, so nothing is lost.
2. If all slow log files together are ≥ 24 Mi:
   - If `.1` exists, `truncate` it to zero. This frees the space even though the
     tailer still holds it open. The unread bytes go into the dropped counter.
     While a flush keeps failing, `.1` is the file mysqld is still writing to;
     truncating it is safe for the reason below.
   - If the total is still ≥ 24 Mi, `truncate` the active file in place and
     count its unread bytes as dropped. mysqld opens the log with `O_APPEND` and
     keeps writing at the new end of file (verified on all six images).
3. Exports `slow_log_bytes`, the total size of the slow log files.

**Start-up cleanup**, before mysqld is launched and only when not adopting.
The tmpfs survives a container restart within the Pod, so the previous run's
files can still be there.

1. If `mysqld-slow.log.1` or `mysqld-slow.log` exist, parse and emit them from
   offset 0, in that order, then delete them. These are often the entries from
   just before a crash. Draining is bounded by the 24 Mi hard cap. Above that,
   the files are deleted unread and counted as dropped.
2. Delete a stale `mysqld.pid.tmp`.
3. `statfs` the volume. If less than the 4 Mi reserve is free, log the directory
   listing with sizes and continue. Nothing but our own files should be large,
   and they are gone by then.

Entries the previous container already emitted are emitted again. Across a
container restart delivery is at-least-once, and the docs say so.

**In-place manager upgrade.** Before `execve`, the re-exec path stops the
tailer and the watchdog and asks for the tailer's cursor, `<inode>:<offset>` of
the first entry it has not emitted. The stop waits at most five seconds; a
tailer stuck on stdout hands over the cursor of the entry it was emitting, so
that entry is emitted again after the exec. The cursor travels in the
`CNMSQL_SLOWLOG_CURSOR` environment variable, set next to the existing
`CNMYSQL_ADOPT_MYSQLD_PID`. If the exec fails, the tailer and watchdog resume.

The new image opens `.1` if it exists, then the active file, and starts at the
cursor's offset when the inode matches, at offset 0 otherwise. A rotation left
pending by the old image is finished by the new one: with `.1` present, the
watchdog treats the flush as not done and issues it again, which is harmless if
it already happened. Without the variable, which means an upgrade from a
manager that predates this design, it starts from offset 0. The old manager
never emitted anything, so that produces no duplicates. The pidfile is not
involved, so a full volume cannot break the handoff.

### Record format

One record per entry on the instance container's stdout, through the manager's
logger:

- logger: `mysqld.slowlog`
- msg: `Slow query`

| field | from | type |
|---|---|---|
| `time` | `# Time:` | RFC 3339, UTC; absent when the entry had none |
| `user`, `host` | `# User@Host:` | string |
| `db` | `Schema:`, else the body's `use` | string |
| `query_time`, `lock_time` | `Query_time`, `Lock_time` | float seconds |
| `rows_sent`, `rows_examined`, `rows_affected`, `bytes_sent`, `thread_id` | header | int |
| `admin` | `# administrator command:` | bool, present only when true |
| `query` | body | string, capped at 64 KiB |
| `query_truncated` | | bool, present only when true |
| `attributes` | every other metric the parser read | map: floats for `*_time` and `*_wait`, booleans for `Yes`/`No`, integers otherwise |

Typed fields only appear when the header carried them. MariaDB writes `# Time:`
only when the second changes, so most MariaDB records have no `time`; the log
pipeline's own timestamp is within a poll interval of it. `log_slow_extra`'s
`Start` and `End` are left out of `attributes`, because the parser reads them as
integers and reports 0. The client IP and MySQL's connection `Id` are not
extracted by the parser and are not in the record.

### Metrics

Registered next to `VolumeCollector` in the instance manager's registry, using
the same `mysql_instance_` prefix:

- `mysql_instance_slow_log_bytes` (gauge): total size of the slow log files.
- `mysql_instance_slow_log_entries_total` (counter): records emitted.
- `mysql_instance_slow_log_dropped_bytes_total` (counter): bytes deleted or
  truncated before they were read.
- `mysql_instance_slow_log_rotations_total` (counter): successful rotations.

### Failure modes

| what happens | result |
|---|---|
| stdout backpressured | the tailer stalls; the watchdog rotates and then truncates; entries are dropped and counted; queries unaffected |
| `FLUSH LOCAL SLOW LOGS` fails | retried every second; at the hard cap the active file is truncated in place |
| manager wedged | nothing enforces the reserve, so the slow log can fill all 32 Mi; mysqld drops entries and queries are unaffected; a wedged manager fails its probes, and the container restart's start-up cleanup frees the volume before mysqld starts again |
| container restart | leftover files are drained and deleted before mysqld starts; at-least-once delivery |
| in-place manager upgrade | resumes at the handed-over cursor; no loss, no duplicates unless the tailer was stuck emitting |
| user sets `log_output` / `slow_query_log_file` | `phase: Blocked`, reason names the key |

### Security

Statement text includes literals such as emails and tokens, and turning
`slow_query_log` on sends all of it to the cluster's log pipeline. The docs say
this plainly and point to `log_slow_rate_limit` and `long_query_time` to reduce
volume.

MySQL rewrites the password out of account statements (`CREATE USER …
IDENTIFIED BY`) before logging them. MariaDB does not: the integration test
found `IDENTIFIED BY 'password'` verbatim on 10.11, 11.4, 11.8 and 12.3. That
includes statements the operator sends with passwords from the cluster's
Secrets (managed roles, `CHANGE MASTER … MASTER_PASSWORD`). The manager
therefore redacts password literals in every record, on both engines, the way
MySQL does (`IDENTIFIED BY <secret>`): `IDENTIFIED BY [PASSWORD]`,
`IDENTIFIED VIA|WITH … USING|AS [PASSWORD(…)]`, `SET PASSWORD … =`, and
`[MASTER_|SOURCE_]PASSWORD =`. Redaction is best-effort; it covers account and
replication syntax, not a password hidden in arbitrary SQL, and it also hides
the literal of a `password = '…'` comparison.

The statement body is user-controlled. A multi-line statement can contain a line
that looks like `# User@Host:` and split one entry into two records. MySQL does
not escape the slow log, and every slow-log parser shares this limitation. The
injected record can only carry data the same client could already put in the
log. The docs mention it.

## Testing

- **Unit, records:** the six images' plain and verbose fixtures, in
  `pkg/management/mysql/slowlog/testdata/`, go through the library and the
  record mapping. Every fixture yields one record per `# User@Host:` line, and
  known entries map to the expected fields on both engines. Also covered: the
  64 KiB cap, admin commands, and dropping `Start`/`End`.
- **Unit, tailer and watchdog:** in a temp dir, with a fake writer standing in
  for mysqld (append writes, a reopen on a fake flush) and an injectable
  flusher. Covers soft rotation without loss, a failing flush, the hard cap
  truncating `.1` and then the active file, a stalled emitter, start-up drain
  and cursor handoff.
- **Unit, config:** the managed keys render per flavor; `log_output`,
  `slow_query_log_file` and `log_slow_query_file` are rejected;
  `slow_query_log` is still allowed.
- **Unit, controller:** the `run` volume is `Memory` with a 32 Mi `sizeLimit`.
- **Integration** (`test/integration`, MySQL and MariaDB images): run the
  manager against the real image with `slow_query_log=ON` and
  `long_query_time=0`, and assert records appear on its output. Force rotations
  with a low threshold (a hidden flag) and assert that no entry is lost, that
  rotations happened, and that the GTID state did not change. Run a
  `CREATE USER … IDENTIFIED BY` and assert whether the password appears.
- **E2E** (Kind): one MySQL and one MariaDB test. Enable the slow log through
  `spec.mysql.parameters`, run a query, and read the record back through the
  Pods API logs, without exec. Assert that the `run` volume is memory-backed.

## Documentation

- `docs/src/monitoring.md`: a "Slow query log" section covering how to enable
  it, the record format, the metrics, the memory budget, the security note and
  at-least-once delivery.
- `docs/src/cluster-lifecycle.md` and `docs/src/multi-tenancy.md`: add
  `log_output` and `slow_query_log_file` to the managed-key lists.
- `docs/src/mariadb.md`: the `log_slow_query_*` names.
- `INSTRUCTION.md`: key decision D23.

## Follow-ups

- The general query log through the same tailer.
- `sizeLimit` as a Cluster field, if 32 Mi turns out to be wrong for someone.
