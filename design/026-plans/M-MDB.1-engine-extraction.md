# M-MDB.1 — Engine extraction (no behaviour change)

- **Status:** ready
- **Depends on:** nothing (foundation already exists)
- **Design refs:** §6, §6.1, §18
- **Risk:** HIGH — touches many call sites. Changes **no** output.

## Objective

Route every existing engine-divergent MySQL decision through
[../../pkg/engine/](../../pkg/engine/) instead of calling
`version.Version` predicates or `config`/`replication` free functions directly.
With the default flavor (`mysql`) the resulting output must be **byte-identical**.
This is a pure refactor: extract, don't change.

The `pkg/engine` package already exists with `Engine`, `GTIDModel`, and both
flavor impls (design §18). This plan **grows the interface** to cover the version,
replication-dialect, semi-sync, config and lifecycle facets, wires the MySQL impl
to delegate to today's code, and threads `engine.For(...)` through call sites.

## Background you must read first

- [../../pkg/engine/engine.go](../../pkg/engine/engine.go) — current interface (4
  methods) + `ForFlavor`/`MustForFlavor`.
- [../../pkg/management/mysql/version/version.go](../../pkg/management/mysql/version/version.go)
  — the predicates being wrapped: `Series()` (~L74), `UpgradeSeriesChain` (~L89),
  `CheckUpgrade` (~L114), `UsesReplicaTerminology()` (~L155), `HasSuperReadOnly()`
  (~L160), `HasAdminInterface()` (~L184), `UsesResetBinaryLogsAndGtids()` (~L191),
  `SupportsGroupReplication()` (~L199), `SemiSync()` (~L245).
- [../../pkg/management/mysql/replication/statements.go](../../pkg/management/mysql/replication/statements.go)
  — dialect functions: `ChangeSourceStatement`, `StartReplicaStatement`,
  `StopReplicaStatement`, `ResetReplicaStatement`, `ShowReplicaStatusStatement`,
  `ResetBinaryLogsStatement`, `InstallSemiSync*Statement`.
- [../../pkg/management/mysql/config/config.go](../../pkg/management/mysql/config/config.go)
  — `managedSettings` (~L451), `groupReplicationSettings` (~L572), `binlogExpire`
  (~L633), `isGroupReplicationManagedKey` (~L311), `IsManagedKey`, `IsDeniedKey`.

## Guardrails

- **Do not modify** the bodies of the `version`, `replication`, or `config`
  functions. Wrap them. The MySQL engine methods should *call* the existing code.
- **Do not change any `*_test.go` expected value.** If `config_test.go`,
  `statements_test.go`, or `version_test.go` would need edited expectations, you
  changed behaviour — revert and wrap instead.
- Avoid import cycles: `engine` may import `version`, `replication`, `config`;
  none of those may import `engine`. (`replication` already has its own GTID
  parser — the MySQL `GTIDModel` delegates to it; keep that direction.)

## Tasks

### A. Grow the interface (additive; each method fully implemented for MySQL)

1. Add to `Engine` in [../../pkg/engine/engine.go](../../pkg/engine/engine.go),
   one facet at a time, each backed by a MySQL impl that delegates:
   - **Version facet** — `ParseServerVersion(raw string) (version.Version, error)`,
     `Series(version.Version) version.Version`, `UpgradeChain() []version.Version`,
     `CheckUpgrade(from, to version.Version) error`. MySQL delegates to
     `version.Parse`, `.Series()`, `version.UpgradeSeriesChain`, `version.CheckUpgrade`.
   - **Replication dialect** — add `Repl() ReplDialect` returning a `ReplDialect`
     interface with `ChangeSource`, `StartReplica`, `StopReplica`, `ResetReplica`,
     `ShowReplicaStatus`, `ResetBinaryLogs` (mirroring the `statements.go`
     signatures, which take `version.Version`). MySQL impl calls the existing
     `statements.go` functions verbatim.
   - **Semi-sync** — `SemiSync(version.Version) version.SemiSyncNaming` (reuse the
     existing type; do not redefine it) + a bool `SemiSyncIsPlugin()` (MySQL true).
   - **Capability** — `HasAdminInterface(version.Version) bool`,
     `UsesResetBinaryLogsAndGtids(version.Version) bool` (MySQL delegates;
     these matter for M-MDB.4).
   - **Config** — `GroupReplicationManagedKey(normalized string) bool` and a
     `BinlogExpire(ver version.Version, seconds int) (name, value string)` that
     MySQL delegates to `config.binlogExpire` (export it or add a thin exported
     wrapper in `config`). Keep `managedSettings`/`groupReplicationSettings` in
     `config` for now; M-MDB.3 pulls the flavor-divergent slice onto the engine.
   - **Lifecycle** (stubs are NOT allowed — only add these once MySQL has a real
     impl to point at; the concrete command strings live in `instance/`, so if
     that code isn't factored yet, **defer** `InitDataDirArgs`/`UpgradeArgs`/
     `ServerdCommand` to M-MDB.3 and note it in the status log).

   > If a facet has no clean MySQL delegate yet, leave it out and record why in
   > the status log. A smaller-but-honest interface is correct; declaring a method
   > with no implementation is not.

2. For each method added, extend
   [../../pkg/engine/engine_test.go](../../pkg/engine/engine_test.go) so the MySQL
   engine's result is asserted to equal the underlying `version`/`replication`
   function's result across a small version matrix (8.0.22, 8.0.26, 8.4.0, 9.0.0).
   This *is* the "no behaviour change" proof at the engine boundary.

— checkpoint — `go build ./... && go test ./pkg/engine/...` green; `gofmt -l pkg/engine` empty.

### B. Thread the MySQL call sites through the engine

3. Introduce the cluster→engine selector. Add `engine.For(flavor Flavor) Engine`
   is already there as `ForFlavor`; add a convenience in `internal/controller`
   (or wherever the reconciler lives) that reads the resolved flavor. **Until
   M-MDB.2 lands the `spec.flavor` field, the flavor is always `mysql`** — hard-code
   `engine.ForFlavor(engine.FlavorMySQL)` at each site and leave a
   `// TODO(M-MDB.2): resolve from cluster.ResolvedFlavor()` marker so .2 can grep
   for them. This keeps .1 shippable independently.

4. Replace direct predicate calls with engine calls, one file per commit:
   - `pkg/management/mysql/replication/manager.go` and any caller of
     `statements.go` verbs → go through `eng.Repl()`.
   - config renderer callers → `eng`-mediated where the facet moved.
   - `internal/controller/cluster_plan.go` version/series resolution → engine
     (only the parts §6 lists; leave `resolveServerVersion` for M-MDB.3 which adds
     the MariaDB default — just mark the TODO).
   - the instance manager boot: set `CNMSQL_FLAVOR` env alongside `MYSQL_VERSION`
     in [../../internal/controller/cluster_pod.go](../../internal/controller/cluster_pod.go)
     (~L376) so the in-Pod manager can call `engine.ForFlavor`. Value is `mysql`
     for now.

— checkpoint — full suite: `go build ./... && go test ./...`. **Every test green,
zero expectation edits.** `gofmt -l pkg/ internal/ api/` empty, `go vet ./...` clean.

### C. Wrap up

5. Update design §18 "Implementation status": mark the threaded facets done, list
   any deferred to M-MDB.3.
6. Commit(s): `refactor(gr): route MySQL engine decisions through pkg/engine`.

## Acceptance criteria

- [ ] `Engine` exposes version + replication-dialect + semi-sync facets, each with
      a MySQL impl that delegates to existing code.
- [ ] MySQL call sites obtain divergent decisions via an `Engine`, not direct
      predicate calls (except the ones explicitly deferred to M-MDB.3, marked with
      TODO(M-MDB.2/.3)).
- [ ] `go test ./...` green with **no** changed expected values anywhere.
- [ ] `gofmt`/`go vet` clean. `CNMSQL_FLAVOR=mysql` plumbed to the instance manager.

## Status log

### 2026-07-04 — (unassigned)
- state: ready
- did: plan authored; `pkg/engine` foundation (GTID + 4 booleans) already exists per design §18.
- next: Task A1 — add the version facet to the interface + MySQL delegate.
- blockers: none
- verify: not started
