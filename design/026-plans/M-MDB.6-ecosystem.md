# M-MDB.6 — Ecosystem (monitoring, kubectl, samples, docs, CI)

- **Status:** blocked (needs M-MDB.5 done)
- **Depends on:** M-MDB.3, M-MDB.4, M-MDB.5 (behaviour must be finished before it's documented/tested end-to-end)
- **Design refs:** §14, §15 (E2E)
- **Risk:** LOW per item, but the E2E lane is where real bugs surface.

## Objective

Round out MariaDB support: monitoring query review, the kubectl plugin's shell
subcommand, samples, docs, and the CI E2E MariaDB lane including a major-version
upgrade spec.

## Background you must read first

- The vendored `mysqld_exporter` and its default query set (grep the exporter
  config / query yaml).
- The kubectl plugin `status`/`promote`/`fence`/`backup`/`mysql` subcommands
  (grep `cnmsql` under the plugin dir).
- E2E harness from design 025 — the `flavor` test tier / `--engine` flag.
- `config/samples/` — existing MySQL samples.

## Tasks

### A. Monitoring (design §14)

1. Review the exporter default query set for MariaDB-specific `SHOW` differences.
   Gate any MySQL-only query (e.g.
   `performance_schema.replication_group_members`, GR-only views) on flavor so it
   isn't scraped against MariaDB. No new exporter — `mysqld_exporter` scrapes
   MariaDB fine.

### B. kubectl plugin (design §14)

2. `status`/`promote`/`fence`/`backup` are flavor-agnostic — verify they read
   `status.flavor` where they print it. The `mysql` shell subcommand must invoke
   the `mariadb` client for MariaDB clusters (detect from `status.flavor`).
   Diagnostics/`status` output prints the flavor.

### C. Samples & docs (design §14)

3. Add `config/samples/*_mariadb.yaml` (single instance + a 3-node async cluster).
   Every existing sample stays MySQL by **omitting** `flavor`.
4. Add a docs page: flavor selection, MariaDB series/upgrade chain, the
   `super_read_only` caveat, Galera being out of scope, no cross-flavor migration.

### D. CI E2E lane (design §15)

5. Parameterize the existing E2E specs by flavor (rename/extend the `--mysql` flag
   to `--engine`). Run a MariaDB lane on an LTS series: bootstrap, replica join,
   switchover, failover, backup + PITR restore, managed users.
6. Add a MariaDB major-upgrade spec (`10.11 → 11.4`).
7. Add the `super_read_only`-absence guard spec (SUPER writer on a MariaDB replica)
   if not already covered by the M-MDB.4 unit test at E2E level.

— checkpoint — `go build ./...` green; `gofmt`/`go vet` clean; E2E MariaDB lane
green in CI (or, if CI runners lack MariaDB images, document how to run it locally
and record the local run result in the status log).

## Acceptance criteria

- [ ] Exporter query set reviewed; MySQL-only queries gated on flavor.
- [ ] kubectl `mysql` shell uses `mariadb` client for MariaDB clusters; flavor printed.
- [ ] MariaDB samples added; existing samples unchanged (still MySQL).
- [ ] Docs page for MariaDB support (caveats + non-goals).
- [ ] CI MariaDB E2E lane (bootstrap→PITR) + major-upgrade spec green.
- [ ] Update design 026 §18 to mark M-MDB done; flip design Status to `done` and
      update `design/INDEX.md`.

## Status log

### 2026-07-04 — (unassigned)
- state: blocked
- did: plan authored.
- next: unblock when M-MDB.5 is `done`; then Task A1 (exporter query review).
- blockers: M-MDB.5.
- verify: not started
