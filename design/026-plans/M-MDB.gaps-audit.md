# M-MDB — gap audit (what's missing from M-MDB.1–.5)

- **Status:** audit — 2026-07-04 (resolution pass 2026-07-04: G3.1, G4.1, G4.2,
  G5.1, G5.2 addressed; see the resolution note at the end)
- **Purpose:** reconcile the M-MDB.1–.5 acceptance criteria against the code as
  actually merged (through commit `7434dd2`), list the concrete remaining work,
  and order it. Several items are marked "done" in their plan's status log but are
  not actually exercised on the MariaDB path.
- **Method:** re-read each plan's acceptance criteria; grep the merged code for the
  facet/gating each criterion requires.

## Severity legend

- 🔴 **blocker** — a `flavor: mariadb` cluster does the wrong thing at runtime.
- 🟠 **correctness gap** — works in the happy path, breaks on a real operation
  (upgrade, PITR, admin-exhaustion).
- 🟡 **coverage/polish** — behaviour is right but under-tested or under-documented.

---

## M-MDB.1 — engine extraction

Complete. The only deferral (`InitDataDirArgs`/`UpgradeArgs`/`ServerdCommand`) was
picked up in M-MDB.3. `UpgradeArgs` is still an unwired stub — tracked under
M-MDB.3 below, not here.

## M-MDB.2 — API field + webhook

Complete. `spec.flavor`, `ResolvedFlavor()`, immutability + cross-field webhook,
advisory catalog flavor, and the `CNMSQL_FLAVOR` plumbing are all present and used
downstream.

---

## M-MDB.3 — bootstrap & config — **remaining**

### G3.1 🟠 `mariadb-upgrade` is not wired
`eng.UpgradeArgs()` has **no consumer anywhere** (`grep` finds only the interface
decl + the two impls). MySQL 8.0.16+ self-upgrades on start, so this was invisible;
MariaDB requires `mariadb-upgrade` after a major-version bump or it can start with
stale system tables. `mariadbEngine.UpgradeArgs()` currently returns empty.
- **Task:** decide the upgrade trigger point (post-binary-swap, before serving),
  add an engine-driven upgrade step that runs `eng.UpgradeArgs()` with the
  flavor's binary, and give `mariadbEngine.UpgradeArgs()` a real value
  (`mariadb-upgrade` args). No-op for MySQL (empty args) keeps MySQL unchanged.
- **Files:** `pkg/management/mysql/instance/upgrade.go`, `pkg/engine/engine.go`.

### G3.2 🟡 No MariaDB golden-file config tests
Task 4 asked for golden-file `my.cnf` coverage. Only an integration assertion
(`TestRenderMyCnfMariaDBIsValid`) exists. MySQL goldens are untouched (good).
- **Task:** add a MariaDB `my.cnf` golden (minimal + semi-sync-ish cluster) so
  config drift is caught by diff, matching the MySQL golden style.

## M-MDB.4 — replication & GTID — **remaining (has blockers)**

Dialect verbs, GTID model wiring, `MASTER_USE_GTID` clone positioning, and
semi-sync naming (built-in, master/slave) are done and tested. Three acceptance
criteria are **not** actually met on MariaDB:

### G4.1 🔴 `super_read_only` gated on the version predicate, not the engine
`Manager.SetSuperReadOnly` returns early on `!m.version.HasSuperReadOnly()`, which
is `AtLeast(5,7,8)` → **true for MariaDB 11.4**. So `Demote()` (and every
non-primary/read-only transition) emits `SET GLOBAL super_read_only=ON`, which
MariaDB rejects (it has only `read_only`). This breaks switchover/demote on
MariaDB — the exact caveat Task E7 called out.
- **Task:** gate `SetSuperReadOnly` on the engine capability (false for MariaDB),
  not the version. The Manager already carries a dialect after M-MDB.4 — thread
  `HasSuperReadOnly` the same way `SemiSyncNaming` was, or pass the engine into the
  controller's Manager (already engine-aware via `NewController`). For MariaDB,
  enforce read-only via `read_only=ON` + the existing kill-writers path.
- **Files:** `pkg/management/mysql/replication/manager.go`,
  `pkg/management/mysql/instance/controller.go`.

### G4.2 🔴 No admin-interface fallback for MariaDB
`cluster_pod.go` passes `--admin-address`/`--admin-port` **unconditionally** for
both flavors (lines ~311–312, no flavor gate). MariaDB's `mariadbd` has no
administrative interface, so the control pool will try to reach an admin port that
does not exist. Task F9 wanted this gated on `eng.HasAdminInterface(version)` with
a fallback to the reserved `CONNECTION_ADMIN` slot.
- **Task:** stop emitting `--admin-address`/`--admin-port` (or set the run
  command's `hasAdminInterface` false) when `ResolvedFlavor()==mariadb`; verify the
  control pool falls back to the standard port + reserved-slot path.
- **Files:** `internal/controller/cluster_pod.go`,
  `internal/cmd/manager/instance/run/cmd.go`, `pkg/management/mysql/pool/control.go`.

### G4.3 🟡 Missing SUPER-writer read-only guard test (design §15)
Task E8 asked for a dedicated test that a SUPER-holding writer on a MariaDB replica
is prevented/caught (SUPER bypasses `read_only`). Not present.
- **Task:** add the guard test once G4.1's read-only path is engine-driven.

## M-MDB.5 — backups & PITR — **remaining**

Base backup/restore/join now run the MariaDB tools (facet + wiring fixed). Deferred
and open items:

### G5.1 🟠 MariaDB PITR (GTID replay) unimplemented (guarded off)
`Restore` fails loudly when `--source-cluster` is set on MariaDB. `binlog.ReplayArgs`
emits `--include-gtids`/`--exclude-gtids`, which `mariadb-binlog` rejects, and
MariaDB GTID positions differ from the shared parser's assumptions.
- **Task:** implement MariaDB-native binlog replay — position/time-based bounding
  instead of GTID include/exclude, `mariadb_backup_binlog_info` anchor parsing —
  and drop the guard. Likely its own sub-task; needs a real MariaDB to validate.
- **Files:** `pkg/management/mysql/binlog/binlog.go` (flavor-aware arg builder),
  `pkg/management/mysql/instance/restore_pitr.go`, `pkg/engine` (GTID/replay facet).

### G5.2 🟠 Continuous binlog archiving is not engine-aware
`archiving.go` builds `binlog.MysqlbinlogScanner(cfg.MysqlbinlogPath, …)` with
`MysqlbinlogPath` defaulting to a PATH lookup of `mysqlbinlog`. MariaDB continuous
archiving needs `mariadb-binlog`. The M-MDB.5 diff scoped archiving out, but
without it MariaDB has no PITR source data — so this blocks G5.1 in practice.
- **Task:** route the archiving scanner binary through `eng.Backup().BinlogClientBinary()`.

### G5.3 🟡 Verify `ParseBinlogInfo` against a real `mariadb_backup_binlog_info`
`mariadbBackupTool.ParseBinlogInfo` delegates to the MySQL parser. The single-line
column layout matches, but the multi-line GTID continuation format and MariaDB GTID
shape are untested against a real file.
- **Task:** add a MariaDB-format fixture test; adjust the parser only if it diverges.

### G5.4 🟡 `spec.backup.xtrabackupOptions` passthrough unverified for MariaDB
Acceptance item left unchecked — confirm user-supplied tool flags still flow to
`mariabackup` and document them as the flavor's tool flags.

---

## Cross-cutting (design defers these to M-MDB.6)

- No MariaDB **E2E / manual smoke** is recorded as passing end-to-end; the bring-up
  is happening now via manual smoke (base boot + bootstrap fixed this session).
- Container **images** (`cnmsql-mariadb-instance:11.4`) must actually ship the
  tools the facets name (`mariabackup`, `mbstream`, `mariadb-binlog`,
  `mariadb-upgrade`) — an ecosystem/build concern for M-MDB.6.

## Resolution note — 2026-07-04

Resolution pass + review. Implemented and verified (`go build`/`vet`/`gofmt`/
`go test ./...` all clean):

- **G4.1 (super_read_only) — DONE.** `ReplDialect`/`Dialect` gained
  `HasSuperReadOnly()` (MySQL true, MariaDB false); `SetSuperReadOnly` /
  `ReadOnly` now gate on **both** the dialect capability **and** the version
  predicate (`m.version.HasSuperReadOnly()`), so MariaDB is skipped while MySQL
  < 5.7.8 stays byte-identical (the review restored this version gate — the first
  cut dropped it, which would have emitted `super_read_only` on legacy MySQL). The
  legacy MySQL no-op tests are kept alongside the new MariaDB ones.
- **G4.2 (admin interface) — DONE.** `cluster_pod.go` only emits
  `--admin-address`/`--admin-port` for MySQL; the control pool already gated on
  `eng.HasAdminInterface(ver)` in `runner.go`, so both layers agree and MariaDB
  falls back to the standard port.
- **G3.1 (mariadb-upgrade) — DONE.** `Engine` gained `UpgradeBinary()` +
  `UpgradeArgs(socket)`; `runner.go` runs the flavor upgrade binary after the
  control connection is up. The review added a `needsUpgrade` version-boundary
  gate so `mariadb-upgrade --force` runs only when the data dir actually crossed a
  version (mirrors MySQL `--upgrade=AUTO`), not on every boot.
- **G5.2 (archiving binary) — DONE.** `runner.go` defaults the archiver's binlog
  client to `eng.Backup().BinlogClientBinary()` (mariadb-binlog on MariaDB).
- **G5.1 (MariaDB PITR replay) — IMPLEMENTED (needs real-MariaDB validation).**
  The GTID-set planner is abstracted behind `gtidOps`; MariaDB uses a
  domain-server-seq model (`PlanReplayWithModel`) and positional replay
  (`StartPosition` + anchor file from `mariadb_backup_binlog_info`) instead of
  `--include-gtids`/`--exclude-gtids`. The loud "not supported" guard is removed.
  The review made MariaDB GTID `Parse` validate eagerly so malformed
  anchor/segment positions fail loudly like the MySQL path.

Still open (coverage/polish, not blockers): **G3.2** (MariaDB config goldens),
**G4.3** (SUPER-writer guard test — now unblocked by G4.1), **G5.3**
(`mariadb_backup_binlog_info` fixture), **G5.4** (`xtrabackupOptions` passthrough
for `mariabackup`).

## Statement-sweep pass — 2026-07-04

A second audit swept every SQL statement the operator emits (not just the M-MDB
plan surface) for MariaDB divergences. Findings + fixes (`go build`/`vet`/
`gofmt`/`go test ./...` all clean):

### G-USER.1 🔴 `REVOKE IF EXISTS` — DONE (syntax); partial_revokes residual
The `user` package (Database + DatabaseUser CRDs) was entirely flavor-blind and
always emitted `REVOKE IF EXISTS`, which MariaDB rejects (no `IF EXISTS` on
REVOKE).
- **Fixed:** `user.Dialect` (`MySQLDialect`/`MariaDBDialect`);
  `CreateUserStatementsWithDialect`/`AlterUserStatementsWithDialect` emit a plain
  `REVOKE` on MariaDB. `user.Manager` tolerates `ER_NONEXISTING_GRANT` (1141) /
  `ER_NONEXISTING_TABLE_GRANT` (1147) on the MariaDB revoke path so
  re-application stays idempotent (MySQL gets that from `IF EXISTS`).
  `instance.Controller` selects the dialect from `eng.Flavor()`.
- **Residual (not a syntax bug):** MariaDB has **no `partial_revokes`**, so a
  revoke that *narrows a global grant* (the `mysql.*` carve-out pattern) cannot
  be enforced — the plain REVOKE errors with "non-existing grant" and is
  tolerated as a no-op, leaving the account with the broader access. Options:
  reject `spec.revokes` on MariaDB clusters at the webhook, or document it as a
  hard flavor limitation. **Product decision pending** — flagged, not silently
  swallowed.

### G-STMT.1 🟠 `server_uuid` not dialect-routed — DONE
MariaDB has no `server_uuid`; the archive index partitions segments by it.
`ReplDialect`/`Dialect` gained `ServerIdentityQuery()` (MySQL `server_uuid`,
MariaDB `server_id`). `replication.Manager.ServerUUID` and the archiver's
`binlog.Reader` (`NewReaderWithIdentityQuery`, wired from
`eng.Repl().ServerIdentityQuery()` in `runner.go`) now use it. Unblocks MariaDB
continuous archiving at the identity layer (complements G5.2).

### G-STMT.2 🟡 `gtid_purged` read + semi-sync status naming — DONE
`ReplDialect`/`Dialect` gained `GTIDPurgedQuery()` (empty on MariaDB → the
Manager skips the read instead of erroring). `SemiSyncStatus` now reads
`m.repl.SemiSyncNaming(m.version)` (dialect) instead of the version-number
`m.version.SemiSync()`, so MariaDB reads master/slave variable names rather than
silently-absent source/replica ones.

Confirmed already-correct / excluded during the sweep: `INSTALL PLUGIN` semi-sync
(gated `eng.SemiSyncIsPlugin()`), `SET GLOBAL gtid_slave_pos` write path, `CHANGE
MASTER TO … MASTER_USE_GTID`, Group Replication (engine capability; Galera is out
of scope), `ListUsersQuery`/`mysql.user` columns (present in MariaDB's compat
view).

## Suggested execution order

1. **G4.1** (super_read_only) — 🔴 without it switchover/demote corrupt or error on
   MariaDB; smallest, highest-value fix, same pattern as the semi-sync fix already
   landed.
2. **G4.2** (admin interface) — 🔴 needed for the control plane to connect at all
   under connection exhaustion.
3. **G3.1** (mariadb-upgrade) — 🟠 needed before any MariaDB version bump.
4. **G5.2** (archiving binary) — 🟠 unblocks having PITR source data.
5. **G5.1** (MariaDB PITR replay) — 🟠 the large one; depends on G5.2.
6. **Coverage:** G4.3, G3.2, G5.3, G5.4 — fold in alongside the above.
