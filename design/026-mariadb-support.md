# 026 — MariaDB Support (Engine Flavors)

- **Status:** proposed
- **Milestone:** M-MDB
- **Supersedes:** none

## 1. Summary

Today CNMSQL is hard-wired to a single engine: Percona Server for MySQL. Engine
assumptions (version feature-gates, replication syntax, GTID model, backup tool,
config keywords, bootstrap command) are scattered across the instance manager,
the config renderer, the reconciler and the webhook, each branching only on a
**server version** (`version.Version`).

This design adds **MariaDB** as a first-class second engine, selected by a new
immutable `spec.flavor` field on `Cluster` (and carried through the image
catalogs). The core move is to stop branching on version alone and instead
branch on a `(flavor, version)` pair, funnelled through a single **`Engine`
abstraction** so that every engine-specific decision lives behind one interface
instead of being re-derived at each call site.

MariaDB instance images already exist (`ghcr.io/cnmsql/cnmsql-mariadb-instance`,
series `10.11`, `11.4`, `12.x`). This design is about the operator, API and
instance-manager changes needed to drive them.

## 2. Motivation

- MariaDB is a drop-in-ish MySQL alternative with a large install base; many
  users specifically want MariaDB, not MySQL.
- The operator's HA/backup/PITR machinery is engine-agnostic in spirit but
  MySQL-specific in its details. Factoring the engine out makes those details
  explicit and testable, and pays off beyond MariaDB (e.g. a future Percona
  XtraDB Cluster or vanilla MySQL flavor).

## 3. Goals

- `spec.flavor: mysql | mariadb` (immutable, default `mysql` for back-compat).
- A `pkg/engine` abstraction that owns every engine-divergent decision. Existing
  MySQL behaviour is preserved bit-for-bit as the `mysql` engine.
- MariaDB async / semi-sync GTID clusters: initdb bootstrap, replica cloning,
  switchover, failover, fencing, backup/restore, PITR, managed users/databases,
  monitoring — the full feature set that async MySQL clusters have today.
- Image catalogs (`ImageCatalog`, `ClusterImageCatalog`) express MariaDB series.
- Webhook validation and major-version-upgrade guards understand MariaDB's own
  series chain.

## 4. Non-Goals (this milestone)

- **Galera / synchronous multi-primary.** MariaDB has no Group Replication; its
  quorum story is Galera (wsrep), which is a different reconciler entirely.
  `spec.replication.mode: groupReplication` is **rejected** for `flavor: mariadb`.
  A future `mode: galera` is sketched in §12 but out of scope here.
- **Cross-flavor migration.** `flavor` is immutable; converting a MySQL cluster
  to MariaDB (or vice-versa) is a dump/restore exercise, not an in-place change.
- **Cross-flavor replication** (a MariaDB replica of a MySQL source, via
  `spec.replica` / `externalClusters`). Same-flavor only for now.

## 5. The `flavor` field

### 5.1 API surface

```go
// Flavor selects the database engine a Cluster runs. It is immutable: the
// on-disk data directory, GTID model and system schema are engine-specific, so
// a flavor change is a migration, not an update.
// +kubebuilder:validation:Enum=mysql;mariadb
type Flavor string

const (
    FlavorMySQL   Flavor = "mysql"
    FlavorMariaDB Flavor = "mariadb"
)
```

Added to `ClusterSpec`:

```go
// Flavor selects the database engine: "mysql" (Percona Server for MySQL, the
// default) or "mariadb". Immutable after creation.
// +kubebuilder:default:=mysql
// +optional
Flavor Flavor `json:"flavor,omitempty"`
```

- **Default `mysql`** keeps every existing manifest and every existing stored
  object byte-identical — critical because CRD defaulting rewrites nothing that
  already has a value, and existing clusters have no `flavor` set → they read
  back as `mysql`.
- Surfaced as a print column (`+kubebuilder:printcolumn name=Flavor`) and echoed
  to `status` (`status.flavor`) so a resolved value is always visible even if the
  spec relied on the default.

### 5.2 Immutability

Enforced in `Cluster.ValidateUpdate` (webhook `internal/webhook/v1alpha1`),
alongside the existing replication-mode and GR-group-name immutability checks:

```
if old.ResolvedFlavor() != new.ResolvedFlavor() {
    allErrs = append(allErrs, field.Invalid(path("spec","flavor"), new.Spec.Flavor,
        "flavor is immutable"))
}
```

`ResolvedFlavor()` (an `api` helper, mirroring the existing `ReplicationMode()`)
returns `spec.flavor` or `FlavorMySQL` when empty, so old-object decode of a
pre-flavor cluster compares cleanly.

### 5.3 Cross-field validation (`ValidateCreate` / `ValidateUpdate`)

For `flavor: mariadb`:

- `spec.replication.mode == groupReplication` → rejected (`groupReplication is
  only supported with flavor mysql; MariaDB uses Galera which this operator does
  not yet manage`).
- The resolved series (from `imageName` tag or `imageCatalogRef.series`) must be
  a known MariaDB series (§9). A MySQL-looking series (`8.0`, `8.4`, `9.0`) under
  `flavor: mariadb` is rejected, and vice-versa. This catches the most common
  misconfiguration — a MariaDB image under the default MySQL flavor — at admission
  rather than at a confusing crash in the instance manager.

## 6. The `Engine` abstraction

Today the divergence axis is `version.Version` and callers do
`ver.UsesReplicaTerminology()`, `ver.SemiSync()`, etc. We keep `version.Version`
(patch/series math is still needed **within** an engine) but introduce an
`Engine` that owns the flavor-dependent decisions and, where a decision is both
flavor- and version-dependent, takes the version as input.

New package `pkg/engine`:

```go
package engine

type Flavor string // "mysql" | "mariadb"

// Engine is the single source of truth for every engine-divergent decision.
// One implementation per flavor; selected once per reconcile / per instance-
// manager boot from the resolved (flavor, version).
type Engine interface {
    Flavor() Flavor

    // --- versioning ---
    // ParseServerVersion normalizes a raw @@version string (MariaDB reports
    // "11.4.3-MariaDB-...") into a comparable Version.
    ParseServerVersion(raw string) (version.Version, error)
    // Series maps a runtime version to its catalog/upgrade series.
    Series(version.Version) version.Version
    // UpgradeChain is the ordered, single-hop-only series chain (§9).
    UpgradeChain() []version.Version
    // DefaultServerVersion resolves an image tag/series to a concrete version.
    DefaultServerVersion(series string) (string, bool)
    DefaultImage() string

    // --- replication SQL dialect ---
    Repl() ReplDialect        // §10: CHANGE MASTER vs CHANGE REPLICATION SOURCE,
                              //      START SLAVE vs START REPLICA, status columns
    GTID() GTIDModel          // §10: gtid_executed vs @@gtid_binlog_pos,
                              //      subset/containment semantics, RPO compare
    SemiSync(version.Version) SemiSyncNaming
    HasSuperReadOnly() bool   // MariaDB: false — only read_only exists

    // --- config rendering ---
    // ManagedSettings returns the engine-owned [mysqld] pairs for this cluster
    // (server_id, gtid mode, log_bin naming, binlog expiry var, etc.).
    ManagedSettings(ServerConfigInput, version.Version) []Pair
    // SupportsGroupReplication gates the groupReplication mode / GR settings.
    SupportsGroupReplication() bool
    ManagedKeyDenylist() []string

    // --- lifecycle commands (run by the instance manager) ---
    // InitDataDir bootstraps an empty data dir (mysqld --initialize-insecure
    // vs mariadb-install-db / mysql_install_db).
    InitDataDirArgs(InitParams) []string
    UpgradeArgs() []string    // mysql_upgrade vs mariadb-upgrade
    ServerdCommand() []string // mysqld path/flags

    // --- backups ---
    Backup() BackupTool       // §11: xtrabackup vs mariabackup, xbstream vs mbstream
}
```

Concrete impls: `pkg/engine/mysql` (a straight lift of today's `version.go`
predicates, `config.go` branches, `xtrabackup`, and bootstrap command building)
and `pkg/engine/mariadb`.

**Selection.** One helper resolves an `Engine` from a `Cluster`:

```go
func For(cluster *v1alpha1.Cluster) engine.Engine   // by spec.flavor
func ForFlavor(f v1alpha1.Flavor) engine.Engine
```

The instance manager, which runs in-Pod without the CR, receives the flavor via
an env var (`CNMSQL_FLAVOR`, set from `plan` next to the existing
`MYSQL_VERSION`) and calls `engine.ForFlavor`.

### 6.1 Migration mechanics (keeping MySQL byte-identical)

The refactor lands in two commits so review can prove no MySQL behaviour change:

1. **Extract, no behaviour change.** Move the existing `version.Version`
   predicates and `config.go` branches into `pkg/engine/mysql`, and have every
   current call site go through `engine.For(cluster)` which, with default flavor,
   returns the MySQL engine. Golden-file config tests (`config_test.go`) must be
   unchanged. This is the risky commit; it touches many files but changes no
   output.
2. **Add MariaDB.** `pkg/engine/mariadb` + API field + webhook + catalogs +
   images wiring, all additive.

## 7. Divergence catalog

The table below is the authoritative list of what the `mariadb` engine overrides.
Each row maps to a subsystem section.

| Concern | MySQL (today) | MariaDB | §  |
|---|---|---|---|
| Bootstrap empty datadir | `mysqld --initialize-insecure` | `mariadb-install-db --auth-root-authentication-method=normal` | 8 |
| Post-upgrade fixups | `mysql_upgrade` | `mariadb-upgrade` | 8 |
| Default auth plugin | `caching_sha2_password` | `mysql_native_password` (`ed25519` optional) | 8 |
| Replica setup | `CHANGE REPLICATION SOURCE TO` + `MASTER_AUTO_POSITION=1` | `CHANGE MASTER TO ... MASTER_USE_GTID=slave_pos` | 10 |
| Replication verbs | `START/STOP REPLICA`, `SHOW REPLICA STATUS` | `START/STOP SLAVE`, `SHOW SLAVE STATUS` (canonical) | 10 |
| GTID identity | `@@gtid_executed` (`UUID:1-N`) | `@@gtid_binlog_pos` / `@@gtid_current_pos` (`domain-server-seq`) | 10 |
| GTID subset test | `GTID_SUBSET()` | no builtin; operator-side domain-wise compare | 10 |
| Reset binlogs | `RESET BINARY LOGS AND GTIDS` (8.4+) / `RESET MASTER` | `RESET MASTER` | 10 |
| `super_read_only` | yes | **no** — only `read_only` + kill non-super writers | 10, 15 |
| Semi-sync vars | `rpl_semi_sync_source_*` (8.0.26+) | `rpl_semi_sync_master_*` (built-in, no INSTALL PLUGIN) | 10 |
| Admin interface | `admin_address`/`admin_port` (8.0.14+) | none — use `extra_port` or reserved SUPER slot | 13 |
| Binlog expiry var | `binlog_expire_logs_seconds` | `binlog_expire_logs_seconds` (10.6+) else `expire_logs_days` | 11 |
| Quorum topology | Group Replication | Galera (out of scope) | 12 |
| Physical backup | `xtrabackup` + `xbstream` | `mariabackup` + `mbstream` | 11 |
| Clone plugin | `mysql_clone.so` (GR recovery) | none | 12 |
| Series chain | 8.0 → 8.4 → 9.0 | 10.6 → 10.11 → 11.4 → 12.x | 9 |
| Exporter | mysqld_exporter | mysqld_exporter (works against MariaDB) | 14 |

## 8. Instance-manager bootstrap & lifecycle

`pkg/management/mysql/instance/{bootstrap,initializer,upgrade}.go` currently
build MySQL command lines directly. These move behind `Engine`:

- **initdb:** `InitDataDirArgs` → MySQL keeps `mysqld --initialize-insecure
  --datadir=...`; MariaDB uses `mariadb-install-db --datadir=... --auth-root-
  authentication-method=normal --skip-test-db`. MariaDB has no
  `--initialize-insecure`; the install script pre-creates the system tables, so
  the manager's existing "start mysqld, then set root password / create app
  user" sequence still applies, just after a different init step.
- **auth:** the root/app/replication/control accounts today are created with the
  MySQL default plugin. MariaDB path pins `mysql_native_password` (the operator's
  Go MySQL driver and `mariabackup` both speak it) so the operator's connection
  logic is unchanged. `caching_sha2` public-key fetch (`GET_SOURCE_PUBLIC_KEY`)
  is MySQL-only and skipped.
- **upgrade:** `UpgradeArgs` → `mysql_upgrade` vs `mariadb-upgrade`, run by the
  existing post-start upgrade step in `upgrade.go`.
- **serverd:** MariaDB's server binary is `mariadbd` (with `mysqld` symlink on
  the images); `ServerdCommand` returns the right path so we don't depend on the
  symlink.

The bootstrap **flow** (start → wait ready → seed accounts → configure
replication) is unchanged; only the leaf commands differ, which is exactly what
the `Engine` interface isolates.

## 9. Versions, series & catalogs

### 9.1 Series chains

MySQL's `version.UpgradeSeriesChain` becomes `engine.UpgradeChain()`:

- **mysql:** `8.0 → 8.4 → 9.0` (unchanged).
- **mariadb:** the ordered set of **LTS series** we qualify, e.g.
  `10.6 → 10.11 → 11.4 → 12.3`. Each entry is a discrete `major.minor` LTS line
  (the images track LTS releases — MariaDB 12.3 is an LTS, hence the built
  image); there is **no** MySQL-9.x-style "rolling line collapses to one series"
  rule. `Series()` for MariaDB is the plain `major.minor` of the version.
  MariaDB minor releases within an LTS line are patch-equivalent for upgrade
  purposes; like MySQL we reason per series, one hop forward, no skips, no
  in-place downgrade. `CheckUpgrade` moves onto the engine and consults the
  engine's chain, so the webhook's major-upgrade guard (§024) works per flavor
  with no other change.

### 9.2 Catalogs

`CatalogImage` / `ImageCatalogRef` need no structural change — `series` is
already `major.minor` and the regex `^[0-9]+\.[0-9]+$` accepts `10.11`, `12.3`.
Two adjustments:

- **Doc/comment** only: the `CatalogImage.Image` godoc says "Percona Server for
  MySQL image"; generalize to "engine image matching the Cluster's flavor".
- **Flavor coherence.** A catalog is not intrinsically flavored, but a `Cluster`
  resolving a series from a catalog asserts, in the webhook, that the series is
  valid for the cluster's flavor (§5.3). **Decided:** add an **advisory**
  optional `spec.flavor` to `ImageCatalogSpec`, used only to produce a clearer
  admission error ("catalog X is a mariadb catalog, cluster is mysql"). It does
  **not** gate resolution — resolution stays flavor-driven by the Cluster — so a
  catalog with the field unset keeps working for either flavor.

### 9.3 Default image / version

`resolveServerVersion` and the `defaultMySQL*ServerVersion` constants in
`cluster_plan.go` move into the engine:

- `engine.DefaultImage()` — MySQL keeps `defaultInstanceImage`; MariaDB returns
  `ghcr.io/cnmsql/cnmsql-mariadb-instance:11.4` (the current LTS) as the default
  when neither `imageName` nor `imageCatalogRef` is set.
- `engine.DefaultServerVersion(tag)` — resolves `10.11`/`11.4`/`12.x` tags to
  concrete versions the same way MySQL resolves `8.0`/`8.4`/`9.x`.

## 10. Replication & GTID (the hard part)

This is where MariaDB diverges most and where correctness risk is highest.

### 10.1 SQL dialect (`ReplDialect`)

`pkg/management/mysql/replication` today emits `CHANGE REPLICATION SOURCE TO` /
`START REPLICA` gated on `ver.UsesReplicaTerminology()`. For MariaDB the verbs
are `CHANGE MASTER TO` / `START SLAVE` / `SHOW SLAVE STATUS` — MariaDB never
adopted SOURCE/REPLICA as canonical. `ReplDialect` returns the verb set and the
`SHOW ... STATUS` column names the manager parses (e.g. `Slave_IO_Running`
vs `Replica_IO_Running`). The existing status-parsing struct gets fed column
names from the dialect rather than a version predicate.

### 10.2 GTID model (`GTIDModel`) — the crux

The operator leans on GTIDs in three safety-critical places:

1. **Replica auto-positioning** — MySQL `MASTER_AUTO_POSITION=1`; MariaDB
   `MASTER_USE_GTID=slave_pos` after seeding `@@gtid_slave_pos` from the donor's
   `@@gtid_binlog_pos` (captured during the `mariabackup` clone, from
   `xtrabackup_binlog_info` / `mariadb_backup_binlog_info`).
2. **Errant-transaction / diverged-instance detection** (switchover & failover
   guards, `status.divergedInstances`) — MySQL compares with `GTID_SUBSET(replica,
   primary)`. MariaDB has **no** `GTID_SUBSET`; a MariaDB GTID position is a set
   of `domain-server-seq` triples, one per replication domain. `GTIDModel`
   implements containment operator-side: parse both positions into
   `map[domain]struct{server,seq}` and test that every domain in the candidate is
   `<=` the reference's seq for that domain (and no extra domains) — the
   MariaDB-correct "is-subset / has-errant" test.
3. **RPO comparison during failover** (pick the most-advanced replica) — MySQL
   compares executed GTID sets; MariaDB compares `@@gtid_current_pos` domain-wise
   (max seq per domain). `GTIDModel.Compare(a, b)` returns "a ahead / behind /
   diverged".

`status.gtidExecutedByInstance` stays a `map[instance]string`; the string is
just engine-opaque (a MySQL gtid set or a MariaDB position). Only `GTIDModel`
interprets it, so the status schema is unchanged.

The `version.Version` semi-sync/reset/read-only predicates (`SemiSync`,
`UsesResetBinaryLogsAndGtids`, `HasSuperReadOnly`) move onto `Engine`:

- **semi-sync:** MariaDB is built-in (no `INSTALL PLUGIN`, no `.so` load), vars
  `rpl_semi_sync_master_enabled` / `rpl_semi_sync_slave_enabled`,
  `rpl_semi_sync_master_wait_point`, `rpl_semi_sync_master_timeout`. `SemiSync()`
  returns these and a flag suppressing the plugin-install step.
- **reset:** MariaDB always `RESET MASTER`.
- **`super_read_only`:** MariaDB lacks it. Read-only enforcement (fencing,
  non-primary, `status.currentPrimary` guard) uses `read_only=ON` **plus** the
  existing kill-writers path; without `super_read_only` a user with SUPER can
  still write, so the operator additionally ensures no such app accounts exist
  and relies on the primary Lease (§017) as the authority. Documented as a known
  MariaDB caveat.

## 11. Backups, PITR & object store

`pkg/management/mysql/xtrabackup` becomes `Engine.Backup() BackupTool`:

- **mysql:** `xtrabackup --backup --stream=xbstream`, `xbstream -x`,
  `xtrabackup --prepare/--copy-back` (unchanged args).
- **mariadb:** `mariabackup --backup --stream=xbstream` (mariabackup accepts
  `--stream=xbstream`; the extractor binary is `mbstream -x`), `mariabackup
  --prepare/--copy-back`. Same flags surface, different binary + extractor name.
  `BackupTool` returns binary names + arg builders; the callers in
  `instance/backup.go`, `restore.go`, `join.go` stop hard-coding `xtrabackup`.

The **object-store layout, xbstream/mbstream streaming into S3, the `Backup` CRD,
scheduled backups, retention GC and the archive index are all flavor-agnostic** —
they move bytes and GTID strings, both engine-opaque. The only binlog concerns:

- **PITR replay:** `mysqlbinlog | mysql` → MariaDB `mariadb-binlog | mariadb`.
  Behind `BackupTool` / `ReplDialect`. GTID-addressed replay uses the MariaDB
  position format via `GTIDModel`.
- **Binlog expiry** var name (`binlogExpire`) moves onto the engine (MariaDB
  10.6+ has `binlog_expire_logs_seconds`; older falls back to `expire_logs_days`,
  same rounding logic already present).

`spec.backup.xtrabackupOptions` stays the field name for back-compat but is
documented as "extra flags for the flavor's physical-backup tool". (A `mariadb`
alias is not worth a schema change.)

## 12. Replication topology & the Galera boundary

`spec.replication.mode` stays `async | groupReplication`. `Engine.
SupportsGroupReplication()` is `false` for MariaDB, so:

- The webhook rejects `mode: groupReplication` under `flavor: mariadb` (§5.3).
- `config.go`'s `groupReplicationSettings` and the GR reconciler / GR status
  block are never reached for MariaDB (already guarded by `ReplicationMode()`).

**Future `mode: galera`** (sketch, not this milestone): Galera is synchronous
multi-primary via `wsrep_*`; there is no single primary to elect, no GTID
switchover, no binlog-clone join (SST/IST instead). It needs its own reconciler
much like GR got design 022. Reserving the enum value now
(`galera`, mariadb-only) keeps the door open without committing. Recommend
**not** reserving it until designed, to avoid an advertised-but-unimplemented
mode.

## 13. Instance manager control plane

The mTLS control API, `/status`, executable-hash upgrade streaming, fencing and
Lease logic are engine-agnostic (they act on the Pod and on generic SQL the
`Engine` supplies). Two touch points:

- **Admin connection under connection exhaustion:** MySQL uses `admin_port`
  (8.0.14+); MariaDB has no admin interface. `Engine` reports no admin interface,
  and the manager falls back to the reserved `SUPER`/`CONNECTION_ADMIN` slot path
  that already exists for pre-8.0.14 MySQL. Optionally wire MariaDB's `extra_port`
  + `extra_max_connections` later; the reserved-slot fallback is sufficient for M1.
- **Version probe:** `ServerVersion()` in `replication/reader.go` returns the raw
  `@@version`; `Engine.ParseServerVersion` normalizes MariaDB's
  `11.4.3-MariaDB-1:11.4.3+maria~ubu2404` suffix (the existing `Parse` already
  drops the `-suffix`, so `11.4.3` parses — verify and add a test).

## 14. Monitoring, kubectl plugin, samples

- **Exporter:** the vendored `mysqld_exporter` scrapes MariaDB fine; default
  query set is reviewed for MariaDB-specific `SHOW` differences and any
  MySQL-only `performance_schema.replication_group_members` query is gated on
  flavor. No new exporter.
- **kubectl cnmsql:** `status`/`promote`/`fence`/`backup` are flavor-agnostic;
  the `mysql` shell subcommand invokes `mariadb` client for MariaDB clusters
  (detected from `status.flavor`). Diagnostics print the flavor.
- **Samples & docs:** add `config/samples/*_mariadb.yaml` and a docs page. Every
  existing sample stays MySQL by omission of `flavor`.

## 15. Testing

- **Unit:** `pkg/engine/mariadb` gets table tests for GTID containment/compare
  (the highest-risk logic), config golden files, dialect verb selection, series
  chain / `CheckUpgrade`. MySQL golden files must not move in commit 1 of §6.1.
- **E2E:** the E2E overhaul (design 025) already introduces a `flavor` **test
  tier**. MariaDB reuses the same specs parameterized by flavor: bootstrap,
  replica join, switchover, failover, backup+PITR restore, managed users. Run a
  MariaDB lane (`--mysql`→`--engine` flag) on the LTS series in CI, plus a
  MariaDB major-upgrade spec (10.11 → 11.4).
- The `super_read_only`-absence path (a SUPER writer on a MariaDB replica) gets a
  dedicated guard test.

## 16. Rollout / phasing

> Per-milestone execution plans (work orders with concrete file/function anchors,
> checklists, and per-plan status logs agents update in place) live in
> [026-plans/](026-plans/) — start at [026-plans/README.md](026-plans/README.md).


- **M-MDB.1 — Engine extraction (no behaviour change).** §6.1 commit 1. MySQL
  goes through `pkg/engine/mysql`; golden tests frozen.
- **M-MDB.2 — API + webhook.** `spec.flavor`, immutability, cross-field
  validation, `status.flavor`, print column. Default `mysql`.
- **M-MDB.3 — MariaDB engine: bootstrap & config.** initdb/upgrade/serverd,
  config rendering, version/series/catalog resolution, default image. Single
  MariaDB instance boots and serves.
- **M-MDB.4 — MariaDB replication.** dialect + GTID model, replica clone via
  `mariabackup`, switchover, failover, fencing, semi-sync, read-only caveat.
- **M-MDB.5 — MariaDB backups/PITR.** `mariabackup`/`mbstream`, PITR replay,
  binlog expiry, scheduled backup + retention (mostly free once §11 lands).
- **M-MDB.6 — Ecosystem.** monitoring query review, kubectl `mariadb` shell,
  samples, docs, CI MariaDB lane, major-upgrade spec.

## 17. Resolved decisions

1. **Series granularity.** MariaDB series are discrete `major.minor` **LTS
   lines** (`10.11`, `12.3`, …), not a rolling line. No MySQL-9.x-style series
   collapsing; `Series()` returns plain `major.minor`. (§9.1)
2. **Advisory `flavor` on catalogs.** Add it as optional, non-gating metadata
   for clearer admission errors; resolution stays Cluster-flavor-driven. (§9.2)
3. **Auth.** Standardize on `mysql_native_password` for operator↔server on
   MariaDB for M1. `ed25519` may be offered later. (§8)
4. **`mode: galera`.** Do **not** reserve the enum value until Galera is
   designed, to avoid advertising an unimplemented mode. (§12)

## 18. Implementation status

- **M-MDB.1 (foundation) — complete.** See plan for details.
- **M-MDB.2 (API + webhook) — complete.** `spec.flavor` enum field (default mysql, optional), `status.flavor` echo, print column. `ResolvedFlavor()` helper (empty → mysql). Webhook enforces immutability and rejects mariadb+GR and flavor/series mismatch. Advisory `flavor` on `ImageCatalogSpec`. All TODO(M-MDB.2) markers resolved: CNMSQL_FLAVOR env set from cluster, instance-manager reads it from env, pool.ControlConfig accepts bool (breaks engine→pool cycle), cluster_funcs.go threads engine.CheckUpgrade. status.flavor populated by controller status patching. All go test ./... green, gofmt/go vet clean, lint 0 issues.
- Remaining M-MDB.2 work: none — ready for M-MDB.3.
