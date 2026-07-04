# M-MDB.5 — MariaDB backups & PITR

- **Status:** blocked (needs M-MDB.4 done)
- **Depends on:** M-MDB.3 (config/lifecycle), M-MDB.4 (dialect + GTID for PITR)
- **Design refs:** §11
- **Risk:** MEDIUM — mostly binary/extractor substitution; object-store layout is flavor-agnostic.

## Objective

Make physical backup, restore, and PITR work for MariaDB by swapping the backup
tool (`xtrabackup`→`mariabackup`), the stream extractor (`xbstream`→`mbstream`)
and the PITR replay client (`mysqlbinlog | mysql` → `mariadb-binlog | mariadb`)
behind the engine's `BackupTool` facet. The object-store layout, streaming to S3,
`Backup` CRD, scheduled backups, retention GC and archive index are **unchanged**
— they move engine-opaque bytes and GTID strings.

## Background you must read first

- [../../pkg/management/mysql/xtrabackup/xtrabackup.go](../../pkg/management/mysql/xtrabackup/xtrabackup.go)
  + [xtrabackup_test.go](../../pkg/management/mysql/xtrabackup/xtrabackup_test.go)
  — arg builders to wrap.
- [../../pkg/management/mysql/instance/backup.go](../../pkg/management/mysql/instance/backup.go),
  [restore.go](../../pkg/management/mysql/instance/restore.go),
  [restore_pitr.go](../../pkg/management/mysql/instance/restore_pitr.go),
  [join.go](../../pkg/management/mysql/instance/join.go) — callers that hard-code
  `xtrabackup`/`xbstream`.
- [../../pkg/management/mysql/binlog/replay.go](../../pkg/management/mysql/binlog/replay.go)
  — PITR replay pipeline.
- [../../pkg/management/mysql/config/config.go](../../pkg/management/mysql/config/config.go)
  `binlogExpire` (~L633) — moved onto the engine in M-MDB.3; reuse here.

## Tasks

### A. BackupTool facet

1. Add `Backup() BackupTool` to `Engine` (or finish it if M-MDB.1 stubbed the
   accessor). `BackupTool` exposes the binary names + arg builders:
   - backup stream: MySQL `xtrabackup --backup --stream=xbstream`; MariaDB
     `mariabackup --backup --stream=xbstream` (mariabackup accepts `--stream=xbstream`).
   - extractor: MySQL `xbstream -x`; MariaDB `mbstream -x`.
   - prepare / copy-back: MySQL `xtrabackup --prepare|--copy-back`; MariaDB
     `mariabackup --prepare|--copy-back`. Same flags, different binary.
   - binlog-info file name to read the donor GTID position from
     (`xtrabackup_binlog_info` vs `mariadb_backup_binlog_info`) — used by M-MDB.4's
     clone positioning.
2. MySQL impl wraps the existing `xtrabackup.go` builders **verbatim**; prove with
   a test that MySQL args are byte-identical (the existing `xtrabackup_test.go`
   expectations must not move).

### B. Rewire callers

3. `backup.go`, `restore.go`, `join.go` obtain binary + args from
   `eng.Backup()` instead of literal `xtrabackup`/`xbstream`. Select `eng` from
   `CNMSQL_FLAVOR`.

### C. PITR replay (design §11)

4. In `restore_pitr.go` / `binlog/replay.go`, the replay client comes from the
   engine: MySQL `mysqlbinlog | mysql`; MariaDB `mariadb-binlog | mariadb`.
   GTID-addressed replay uses the MariaDB position format via `eng.GTID()` — do not
   parse positions locally.

### D. Binlog expiry

5. Confirm the binlog-expiry var (moved to the engine in M-MDB.3) is applied on the
   MariaDB config path (10.6+ `binlog_expire_logs_seconds`, older `expire_logs_days`).

### E. Tests

6. Unit: MariaDB backup/restore/prepare/copy-back arg builders; MySQL args proven
   unchanged. PITR replay command selection per flavor. Binlog-info file-name selection.

— checkpoint — `go test ./...` green, `xtrabackup_test.go` (MySQL) unchanged;
`gofmt`/`go vet` clean.

## Acceptance criteria

- [ ] `BackupTool` facet: mariabackup/mbstream + prepare/copy-back + binlog-info file name.
- [ ] `backup.go`/`restore.go`/`join.go` no longer hard-code the tool; select via engine.
- [ ] PITR replay uses `mariadb-binlog | mariadb` for MariaDB, GTID via `eng.GTID()`.
- [ ] MySQL backup args byte-identical (existing tests green, unedited).
- [ ] `spec.backup.xtrabackupOptions` still applies (documented as flavor's tool flags).

## Status log

### 2026-07-04 — (unassigned)
- state: blocked
- did: plan authored.
- next: unblock when M-MDB.4 is `done`; then Task A1 (BackupTool facet).
- blockers: M-MDB.4 (GTID position + clone path).
- verify: not started
