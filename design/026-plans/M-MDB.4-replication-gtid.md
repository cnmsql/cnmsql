# M-MDB.4 — MariaDB replication & GTID

- **Status:** blocked (needs M-MDB.3 done)
- **Depends on:** M-MDB.1 (GTIDModel exists), M-MDB.3 (instance boots)
- **Design refs:** §10 (all), §13 (control plane), §15 (read-only test)
- **Risk:** HIGHEST — correctness of switchover/failover/fencing. Test heavily.

## Objective

Make MariaDB async + semi-sync GTID clusters work end to end: replica clone,
`CHANGE MASTER TO ... MASTER_USE_GTID=slave_pos`, switchover, failover (RPO pick),
fencing, semi-sync, and the `super_read_only`-absence caveat. The GTID **math**
already exists (`mariadbGTID` in `pkg/engine`); this milestone wires it into the
replication dialect and the reconciler's safety guards.

## Background you must read first

- [../../pkg/engine/gtid_mariadb.go](../../pkg/engine/gtid_mariadb.go) +
  [gtid_test.go](../../pkg/engine/gtid_test.go) — the containment/compare semantics
  you will rely on. **Do not reimplement GTID logic elsewhere** — call `eng.GTID()`.
- [../../pkg/management/mysql/replication/statements.go](../../pkg/management/mysql/replication/statements.go)
  — verbs; [manager.go](../../pkg/management/mysql/replication/manager.go);
  [reader.go](../../pkg/management/mysql/replication/reader.go) — `ServerVersion()`
  and `SHOW ... STATUS` parsing.
- The reconciler's switchover/failover/diverged-instance logic (grep
  `divergedInstances`, `GTID_SUBSET`, `gtidExecutedByInstance`,
  `super_read_only`, `SetSuperReadOnlyStatement`).

## Tasks

### A. Replication SQL dialect (design §10.1)

1. Flesh out `ReplDialect` (started in M-MDB.1) for MariaDB. MariaDB never adopted
   SOURCE/REPLICA as canonical, so:
   - `ChangeSource` → `CHANGE MASTER TO ... MASTER_USE_GTID=slave_pos` (MySQL:
     `CHANGE REPLICATION SOURCE TO ... MASTER_AUTO_POSITION=1`).
   - verbs → `START SLAVE`, `STOP SLAVE`, `RESET SLAVE`, `SHOW SLAVE STATUS`.
   - `ResetBinaryLogs` → MariaDB always `RESET MASTER`.
   - status **column names** the manager parses come from the dialect
     (`Slave_IO_Running`/`Slave_SQL_Running`/`Exec_Master_Log_Pos` vs the MySQL
     `Replica_*` names). Feed `reader.go`'s status struct column names from the
     dialect instead of the `UsesReplicaTerminology()` version predicate.
2. Add a **GTID-position query** to the dialect (design §10.2, §13): MySQL reads
   `@@GLOBAL.gtid_executed`; MariaDB reads `@@gtid_binlog_pos` /
   `@@gtid_current_pos` / `@@gtid_slave_pos` as appropriate. This is the piece the
   `GTIDModel` (string math) does **not** cover — the dialect owns *which SQL to
   run*. Wire `reader.go`'s GTID probe through it.
3. Dialect unit tests: exact statement strings per flavor.

### B. Semi-sync (design §10)

4. MariaDB semi-sync is **built-in** — no `INSTALL PLUGIN`, no `.so`. Vars are
   `rpl_semi_sync_master_enabled`/`rpl_semi_sync_slave_enabled`,
   `rpl_semi_sync_master_wait_point`, `rpl_semi_sync_master_timeout`. The
   `SemiSyncIsPlugin()` flag (M-MDB.1) suppresses the install step for MariaDB.
   Ensure the semi-sync setup path honors it.

### C. Replica clone & auto-position (design §10.2 item 1)

5. On join/clone, capture the donor position from the backup metadata
   (`xtrabackup_binlog_info` / `mariadb_backup_binlog_info`) and seed
   `SET GLOBAL gtid_slave_pos = '<pos>'` before `CHANGE MASTER TO ...
   MASTER_USE_GTID=slave_pos`. (The mariabackup clone itself lands in M-MDB.5; here
   wire the *positioning* using whatever clone path exists, guarded so MySQL is
   untouched.)

### D. Safety guards through GTIDModel (design §10.2 items 2 & 3)

6. Replace direct `GTID_SUBSET(...)` SQL / MySQL-only containment in the
   switchover/failover/diverged detection with `eng.GTID().Contains(...)` /
   `.Compare(...)`. For MySQL this must produce the **same decisions** as today
   (the MySQL `GTIDModel` delegates to the same parser) — verify with existing
   tests. For MariaDB it uses the domain-wise math.
   - errant-transaction / `status.divergedInstances`: candidate is diverged when
     `Compare(candidate, primary) == diverged`.
   - RPO failover pick: choose the replica with `Compare(a, b) == ahead`.

### E. read-only without super_read_only (design §10.2 item 3, §15)

7. MariaDB has only `read_only` (no `super_read_only`). Where the operator sets
   `super_read_only=ON` (fencing, non-primary, primary guard), the MariaDB path
   uses `read_only=ON` **plus** the existing kill-writers path, and must ensure no
   SUPER-holding app accounts exist (a SUPER user bypasses `read_only`). The
   primary Lease (design §017) is the authority. Gate on
   `eng.HasSuperReadOnly()` (already false for MariaDB).
8. Add the dedicated guard test from design §15: a SUPER writer on a MariaDB
   replica must be prevented/caught.

### F. Control-plane touch points (design §13)

9. Admin connection under exhaustion: MariaDB has no `admin_port`; fall back to the
   reserved SUPER/`CONNECTION_ADMIN` slot path (already exists for pre-8.0.14
   MySQL). Gate via `eng.HasAdminInterface(version)`.

— checkpoint — `go test ./...` green with **no** MySQL decision changes; new
MariaDB dialect/guard tests pass; `gofmt`/`go vet` clean.

## Acceptance criteria

- [ ] MariaDB `ReplDialect`: CHANGE MASTER / SLAVE verbs / status columns / RESET MASTER / GTID-position query.
- [ ] Semi-sync built-in path (no plugin install) for MariaDB.
- [ ] Replica auto-position via `gtid_slave_pos` + `MASTER_USE_GTID=slave_pos`.
- [ ] Switchover/failover/diverged guards run through `eng.GTID()`; MySQL decisions unchanged.
- [ ] read-only caveat implemented + SUPER-writer guard test.
- [ ] Admin-interface fallback gated on the engine.

## Status log

### 2026-07-04 — (unassigned)
- state: blocked
- did: plan authored. GTID math already exists in pkg/engine (design §18).
- next: unblock when M-MDB.3 is `done`; then Task A1 (MariaDB dialect verbs).
- blockers: M-MDB.3 (a MariaDB instance must boot first).
- verify: not started
