# M-MDB.3 — MariaDB engine: bootstrap & config

- **Status:** blocked (needs M-MDB.2 done)
- **Depends on:** M-MDB.1, M-MDB.2
- **Design refs:** §7, §8, §9, §17 (decisions 1 & 3)
- **Goal:** a single MariaDB instance boots, initializes its data dir, seeds
  accounts, renders a valid `my.cnf`, and serves — no replication yet.
- **Risk:** MEDIUM — new command paths; MySQL paths must stay identical.

## Objective

Implement the MariaDB side of the version/series, config-rendering and
lifecycle-command facets so that a `Cluster` with `flavor: mariadb` and a MariaDB
image produces a bootable single instance. Uses image
`ghcr.io/cnmsql/cnmsql-mariadb-instance:11.4` (current LTS default) and the series
chain `10.6 → 10.11 → 11.4 → 12.3`.

## Background you must read first

- [../../pkg/management/mysql/instance/initializer.go](../../pkg/management/mysql/instance/initializer.go),
  [bootstrap.go](../../pkg/management/mysql/instance/bootstrap.go),
  [upgrade.go](../../pkg/management/mysql/instance/upgrade.go) — where MySQL init
  (`mysqld --initialize-insecure`), account seeding, and `mysql_upgrade` run.
- [../../pkg/management/mysql/config/config.go](../../pkg/management/mysql/config/config.go)
  — `managedSettings` (~L451), `binlogExpire` (~L633), `removedAt` (~L268),
  `filterRemovedParams` (~L438).
- [../../internal/controller/cluster_plan.go](../../internal/controller/cluster_plan.go)
  — `resolveServerVersion` (~L488), `defaultMySQL{80,84,9x}ServerVersion` (~L167),
  default image (~L485); [cluster_controller.go](../../internal/controller/cluster_controller.go)
  `defaultInstanceImage` (~L45).

## Tasks

### A. Version, series, defaults (design §9)

1. Implement on the `mariadb` engine (from M-MDB.1's interface):
   - `UpgradeChain()` → `[10.6, 10.11, 11.4, 12.3]` (design §9.1). `Series()` =
     plain `major.minor`. `CheckUpgrade` reuses the shared single-hop logic.
   - `ParseServerVersion(raw)` — MariaDB `@@version` looks like
     `11.4.3-MariaDB-1:11.4.3+maria~ubu2404`. Verify `version.Parse` already drops
     the `-suffix` so `11.4.3` parses; **add a unit test** with a real MariaDB
     version string. If it does not parse, normalize in the mariadb impl only.
   - `DefaultImage()` → `ghcr.io/cnmsql/cnmsql-mariadb-instance:11.4`.
   - `DefaultServerVersion(tag)` → resolve `10.6/10.11/11.4/12.3` to concrete
     versions (mirror the MySQL constant table; put the MariaDB constants next to
     the engine, not in `cluster_plan.go`).
2. Move the MySQL default constants/`resolveServerVersion` behind the engine too
   (this is the M-MDB.1-deferred part) so `cluster_plan.go` calls
   `eng.DefaultServerVersion(tag)` / `eng.DefaultImage()`. **MySQL output must not
   change** — keep the same constant values; existing `cluster_controller_test.go`
   expectations (e.g. `defaultMySQL80ServerVersion`) stay valid.

### B. Config rendering (design §7, §11 binlog expiry)

3. Determine the MariaDB-divergent `[mysqld]` settings vs MySQL. Key differences to
   encode on the engine's config facet:
   - binlog expiry: MariaDB 10.6+ has `binlog_expire_logs_seconds`; older falls
     back to `expire_logs_days` (same rounding as MySQL's pre-8.0 path already in
     `binlogExpire`).
   - never emit Group-Replication settings for MariaDB (`SupportsGroupReplication()`
     is already `false`).
   - auth: default plugin pins `mysql_native_password` (decision 3) where the
     config/account path sets it.
4. Add **golden-file config tests** for MariaDB (new `config_mariadb_test.go` or
   table rows) covering a minimal cluster and a semi-sync-ish cluster. The MySQL
   golden files must not move.

### C. Lifecycle commands (design §8)

5. Implement the lifecycle facet on both engines (finishing M-MDB.1's deferred
   `InitDataDirArgs`/`UpgradeArgs`/`ServerdCommand`):
   - MySQL: `mysqld --initialize-insecure --datadir=...`, `mysql_upgrade`,
     `mysqld`. (Lift the exact args currently in `initializer.go`/`upgrade.go` —
     no change to the produced command.)
   - MariaDB: `mariadb-install-db --datadir=... --auth-root-authentication-method=normal --skip-test-db`,
     `mariadb-upgrade`, `mariadbd`. MariaDB has no `--initialize-insecure`; the
     install script pre-creates system tables, so the existing "start → wait ready
     → seed root/app/replication accounts" flow is unchanged, only the init leaf
     differs. Root/app accounts created with `mysql_native_password`; skip the
     MySQL-only `caching_sha2` public-key fetch (`GET_SOURCE_PUBLIC_KEY`).
6. Rewire `initializer.go`/`bootstrap.go`/`upgrade.go` to call the engine
   (selected from `CNMSQL_FLAVOR`) for these leaf commands. **Assert with a test
   that the MySQL engine returns the exact pre-existing command args** so the
   refactor is provably output-preserving.

— checkpoint — `go build ./... && go test ./...` green; MySQL golden files and
command-arg tests unchanged; `gofmt`/`go vet` clean.

### D. Manual smoke (if a cluster is available)

7. Apply a `flavor: mariadb` single-instance sample (imageName
   `ghcr.io/cnmsql/cnmsql-mariadb-instance:11.4`). Confirm the pod initializes,
   becomes ready, and `status.flavor: mariadb`. Record the outcome in the status
   log. If no cluster is available, note that E2E is deferred to M-MDB.6.

## Acceptance criteria

- [ ] MariaDB `UpgradeChain`/`Series`/`ParseServerVersion`/`DefaultImage`/
      `DefaultServerVersion` implemented + unit-tested (incl. real MariaDB version string).
- [ ] Config facet renders valid MariaDB `[mysqld]` (binlog expiry fallback, no GR,
      native auth) with golden tests; MySQL goldens unchanged.
- [ ] Lifecycle facet: MariaDB init/upgrade/serverd commands; MySQL commands proven identical.
- [ ] `resolveServerVersion`/default image now engine-driven for both flavors.
- [ ] Full suite green; a MariaDB single instance boots (or smoke deferred with note).

## Status log

### 2026-07-04 — (unassigned)
- state: blocked
- did: plan authored.
- next: unblock when M-MDB.2 is `done`; then Task A1 (MariaDB series chain).
- blockers: M-MDB.2 (spec.flavor selects the engine per cluster).
- verify: not started
