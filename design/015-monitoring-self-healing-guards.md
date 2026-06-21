# 015 — Monitoring, Self-Healing, and Guards

**Status:** done
**Milestone:** M13

Prometheus metrics exporter with vendored mysqld_exporter scrapers, PodMonitor, PodDisruptionBudget, node-maintenance window, semi-sync self-healing, fencing annotation, and deletion guard.

**Goal:** Fill the three gaps in M13: (1) Prometheus metrics exporter + PodMonitor +
default/custom monitoring queries, (2) PodDisruptionBudget + node-maintenance
window + semi-sync self-healing, and (3) fencing annotation + deletion guard.

## Progress

- **M13.1 — Monitoring: DONE.** Vendored mysqld_exporter scrapers, exporter +
  queries collector, metrics server on :9187, metrics exporter user, opt-in
  PodMonitor. **E2E tests for metrics passed** (endpoint reachable, scrapers +
  custom queries surfaced). Committed on `feat/m13-monitoring-vendored-scrapers`.
- **M13.2 — Self-healing: DONE.**
  - **PDB creation/management: DONE.**
  - **Node maintenance window: DONE.**
  - **Semi-sync self-healing (`dataDurability` preferred/required): DONE.**
  - **Liveness isolation check: DONE.**
- **M13.3 — Guards: DONE.**

## Why

The API types for monitoring (`MonitoringConfiguration`), PDB (`EnablePDB`), and
node maintenance (`NodeMaintenanceWindow`) already exist in
`api/v1alpha1/cluster_types.go` but none of them are wired into the controller or
instance manager. RPO/RTO guards, diverged-instance detection, failover delay, and
auto-rejoin are already done (M5/M6). This milestone closes the remaining
operator-level self-healing and observability surface.

## Scope

### M13.1 — Monitoring (metrics exporter + PodMonitor + queries)

#### Architecture: how metrics are surfaced (end-to-end)

##### Decision: embed scrapers from mysqld_exporter (Apache 2.0), don't import

`github.com/prometheus/mysqld_exporter` ships 30+ pre-built MySQL scrapers
(`ScrapeGlobalStatus`, `ScrapeSlaveStatus`, `ScrapeInnodbCmp`, etc.) under the
`collector` package. Each scraper does one `SHOW ...` / `SELECT ...` query and maps
the results to `prometheus.Metric` values. However, the scrapers cannot be imported
as a Go library because:

1. The `Scraper.Scrape(ctx, instance, ch, logger)` interface takes an unexported
   `*instance` parameter — we cannot construct it from outside the package.
2. The `collector.Collector` wrapper manages its own `*sql.DB` pool via a DSN
   string, duplicating our existing per-instance connection management.

We will **vendor the scraper source files** we need (Apache 2.0 license permits
this with attribution) into `pkg/management/mysql/metrics/scrapers/`, adapting the
unexported `instance` type to accept our own `*sql.DB` handle. The scrapers are
small (50–150 lines each) and straightforward to adapt.

##### Metrics pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│ Instance Pod (port 9187)                                         │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ metricserver.MetricsServer (:9187 /metrics)              │   │
│  │                                                          │   │
│  │  prometheus.NewRegistry()                                │   │
│  │    .Register(Exporter)       ← prometheus.Collector     │   │
│  │    .Register(GoCollector)    ← go_* runtime metrics     │   │
│  │  promhttp.HandlerFor(registry) → HTTP handler           │   │
│  └─────────────┬────────────────────────────────────────────┘   │
│                │                                                 │
│  ┌─────────────▼────────────────────────────────────────────┐   │
│  │ Exporter (implements Describe + Collect)                 │   │
│  │                                                          │   │
│  │  Collect(ch): every Prometheus scrape                    │   │
│  │  1. Run vendored scrapers (one *sql.DB per scrape)       │   │
│  │     - mysqld_exporter::global_status → SHOW GLOBAL STATUS  │
│  │     - mysqld_exporter::slave_status → SHOW SLAVE STATUS    │
│  │     - mysqld_exporter::global_variables → SHOW VARIABLES   │
│  │     - mysqld_exporter::innodb_cmp → INNODB_CMP            │
│  │     - mysqld_exporter::innodb_cmpmem → INNODB_CMPMEM      │
│  │     - mysqld_exporter::query_response_time → QRT plugin   │
│  │     - ... each scraper sends metrics → ch                │   │
│  │  2. If TTL exceeded: queries.Update()                    │   │
│  │     - Runs all user-defined + default YAML SQL queries   │   │
│  │     - Caches results in computedMetrics                  │   │
│  │  3. queries.Collect(ch) → ch (cached results)            │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  Connection: Unix socket →                                     │
│    cnmsql_metrics_exporter@unix(/var/run/mysqld/mysqld.sock)/ │
└─────────────────────────────────────────────────────────────────┘
```

**Key design decisions:**

1. **Separate HTTP server** — Metrics are served on a standalone `http.Server` on
   port 9187, completely independent from the mTLS control webserver on port
   8000. The server is added to the controller-runtime manager via
   `mgr.Add(metricsServer)`.

2. **Dedicated `prometheus.Registry`** — Fresh registry per instance, scoped to
   metrics only. `Exporter` + `GoCollector` registered.

3. **Vendored mysqld_exporter scrapers** — Source files from
   `github.com/prometheus/mysqld_exporter/collector/` are vendored into
   `pkg/management/mysql/metrics/scrapers/` with the `*instance` type adapted to
   accept our `*sql.DB`. This gives us 30+ production-tested MySQL metrics
   (global status, variables, slave status, InnoDB, query response time,
   performance_schema tables) for zero new query-design work. Attribution header
   kept.

4. **`Exporter` orchestrates scrapers + custom queries** — Implements
   `prometheus.Collector` (Describe + Collect). On `Collect(ch)`:
   - Iterates all vendored scrapers, passing our `*sql.DB` → each scraper
     runs its query and sends `prometheus.Metric` values to `ch`.
   - Delegates custom/default user-defined queries to `QueriesCollector`,
     which is rate-limited by `MetricsQueriesTTL` (default 30s).

5. **`QueriesCollector`** — Handles user-defined and default queries from
   ConfigMaps/Secrets. Parses YAML format (`query` + `metrics` list with
   `type: gauge|counter|histogram`, `labels`, `target_databases`). Maps SQL
   result columns to Prometheus metric families via `MetricMapSet`.
   Thread-safe via `sync.RWMutex`.

6. **Metrics exporter MySQL user** — Created at instance bootstrap.
   `cnmsql_metrics_exporter@localhost` with `PROCESS`, `REPLICATION CLIENT`,
   `REPLICATION SLAVE`, `SELECT` on `performance_schema.*` and user-defined
   databases. Connects via local Unix socket (passwordless).

7. **PodMonitor** — Auto-created by operator when `enablePodMonitor=true`.

8. **Default monitoring ConfigMap** — Shipped as `config/manager/default-monitoring.yaml`,
   copied into cluster namespaces by operator.

#### Files to create/modify

| File | Purpose |
|------|---------|
| `pkg/management/mysql/metrics/scrapers/global_status.go` | **VENDOR** — From `mysqld_exporter/collector/global_status.go`. `SHOW GLOBAL STATUS` scraper. |
| `pkg/management/mysql/metrics/scrapers/global_variables.go` | **VENDOR** — From `mysqld_exporter/collector/global_variables.go`. `SHOW GLOBAL VARIABLES` scraper. |
| `pkg/management/mysql/metrics/scrapers/slave_status.go` | **VENDOR** — From `mysqld_exporter/collector/slave_status.go`. `SHOW SLAVE STATUS` scraper. |
| `pkg/management/mysql/metrics/scrapers/innodb_cmp.go` | **VENDOR** — From `mysqld_exporter/collector/info_schema_innodb_cmp.go`. |
| `pkg/management/mysql/metrics/scrapers/innodb_cmpmem.go` | **VENDOR** — From `mysqld_exporter/collector/info_schema_innodb_cmpmem.go`. |
| `pkg/management/mysql/metrics/scrapers/query_response_time.go` | **VENDOR** — From `mysqld_exporter/collector/query_response_time.go`. |
| `pkg/management/mysql/metrics/scrapers/binlog_size.go` | **VENDOR** — From `mysqld_exporter/collector/binlog_size.go`. |
| `pkg/management/mysql/metrics/scrapers/heartbeat.go` | **VENDOR** — From `mysqld_exporter/collector/heartbeat.go`. Replication heartbeat lag. |
| `pkg/management/mysql/metrics/scrapers/instance.go` | **VENDOR+ADAPT** — Adapted `instance` type: drop DSN management, accept `*sql.DB` directly. No version-based gating (all our supported versions have the queried tables). |
| `pkg/management/mysql/metrics/scrapers/scraper.go` | **VENDOR** — `Scraper` interface (adapted to take `*sql.DB` instead of `*instance`). |
| `pkg/management/mysql/metrics/exporter.go` | **NEW** — Implements `prometheus.Collector`. `Describe` forwards to all scrapers + queries collector. `Collect` iterates vendored scrapers → `ch`, then delegates to `QueriesCollector` with TTL check. |
| `pkg/management/mysql/metrics/collector.go` | **NEW** — `QueriesCollector`: YAML parsing, `MetricMapSet`/`ColumnMapping` conversion, `computeMetrics()` SQL execution, TTL cache (`computedMetrics`, `timeLastUpdated`, `ShouldUpdate`, `Collect`) |
| `pkg/management/mysql/metrics/parser.go` | **NEW** — YAML parser for query definitions (`UserQueries`, `UserQuery`, `Mapping` with `Usage` enum: GAUGE/COUNTER/LABEL/DISCARD/HISTOGRAM) |
| `pkg/management/mysql/metrics/mappings.go` | **NEW** — `MetricMapSet`, `ColumnMapping`, `DBToFloat64`/`DBToString` conversion functions |
| `pkg/management/mysql/webserver/metricserver/metrics.go` | **NEW** — `MetricsServer` struct, `New(instance, exporter)` constructor: creates `prometheus.Registry`, registers `Exporter` + `GoCollector`, mounts `/metrics` on a standalone HTTP server on port 9187 |
| `pkg/management/mysql/instance/controller.go` | **MODIFY** — Add `CreateMetricsExporterUser()` at bootstrap; expose `GetMetricsDB(dbName)` method that returns a `*sql.DB` for the metrics exporter connection pool (Unix socket as `cnmsql_metrics_exporter`) |
| `cmd/manager/subcmd/run.go` (or equivalent instance entry point) | **MODIFY** — Create `Exporter`, create `MetricsServer`, `mgr.Add(metricsServer)` |
| `config/manager/default-monitoring.yaml` | **NEW** — Default monitoring ConfigMap with MySQL custom queries |
| `internal/controller/cluster_monitoring.go` | **NEW** — Operator-side: `reconcilePodMonitor` (create/update/delete PodMonitor CR), `injectDefaultMonitoringConfigMap` (copy default queries into cluster namespace), watch for custom query ConfigMap/Secret changes |
| `api/v1alpha1/cluster_types.go` | **MODIFY** — Add `DisableDefaultQueries`, `MetricsQueriesTTL`, `TLSConfig` to `MonitoringConfiguration` |
| `go.mod` | **MODIFY** — Add `github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring` for `PodMonitor` type |

### M13.2 — Self-healing (PDB + node maintenance + semi-sync durability)

**In scope:**

- **PDB creation/management** — when `EnablePDB` is `true` (default), the
  operator creates two `PodDisruptionBudget` objects:
  - **Primary PDB**: `{cluster}-primary`, `maxUnavailable: 1`, matches pods
    with `role=primary`.
  - **Replica PDB**: `{cluster}-replicas`, `maxUnavailable` = floor(N/2) for N
    replicas, matches pods with `role=replica`. Single-instance clusters skip
    the replica PDB.
  Both are owner-referenced and cleaned up when the flag is set to `false` or
  the cluster is deleted.
- **Node maintenance window** — when `nodeMaintenanceWindow.inProgress` is
  `true` AND `reusePVC` is `true`:
  - Temporarily delete the replica PDB so nodes can drain.
  - For single-instance clusters, also delete the primary PDB.
  - On instance re-creation (Pod recreated on same node), re-attach the
    existing PVC instead of provisioning a new one.
  - When `inProgress` is reset to `false`, restore all PDBs.
- **Semi-sync self-healing** — when `dataDurability` is `preferred` (default),
  auto-reduce the number of expected synchronous replicas if one becomes
  unhealthy, rather than blocking writes. When `required`, writes are blocked
  for missing sync replicas.
- **Liveness isolation check** — the instance manager fails its liveness probe
  if it cannot reach the API server or cluster peers (~30s timeout), causing
  kubelet to restart the container as a last-resort self-healing measure.

### M13.3 — Guards (fencing annotation + deletion guard)

**In scope:**

- **Fencing annotation** `cnmsql.cnmsql.co/fencing` — when set on
  an instance Pod, the operator:
  - Removes the instance from all routing services (rw/ro/r and user-defined).
  - The instance manager skips binlog archiving when fenced (pre-stop hook
    gates the streaming loop).
  - The instance does not participate in failover as a candidate.
  - Clearing the annotation restores full functionality.
- **Deletion guard** — an admission webhook or reconciliation-time check
  prevents accidental `kubectl delete cluster` when the cluster still has
  running instances. The guard can be bypassed with
  `cnmsql.cnmsql.co/skipDeleteGuard=true`.

**Out of scope:**

- `FailoverQuorum` CRD (CNPG's `R + W > N` quorum model — depends on
  mature sync replication and is deferred).
- pre-stop hook fencing for non-fenced instances (already works for binlog
  archiving; only the fence-skip path is new).

### Integration & docs

- **Unit tests**: PDB build/delete/reconcile, monitoring query parse/execute,
  fencing annotation toggles, liveness isolation timeout.
- **E2E tests**: Metrics endpoint reachable on port 9187, PodMonitor created
  and targets discovered, default ConfigMap injected, PDB created and removed
  during node maintenance window simulation, fencing prevents routing.
- **Docs**: New `monitoring.md` page, update `api-reference.md` for new
  MonitoringConfiguration fields, update `replication-failover.md` for
  fencing.

## API type changes

```go
// MonitoringConfiguration — new fields added.
type MonitoringConfiguration struct {
    // Existing
    EnablePodMonitor       bool                   `json:"enablePodMonitor,omitempty"`
    CustomQueriesConfigMap []ConfigMapKeySelector  `json:"customQueriesConfigMap,omitempty"`
    CustomQueriesSecret    []SecretKeySelector     `json:"customQueriesSecret,omitempty"`

    // New
    DisableDefaultQueries *bool                       `json:"disableDefaultQueries,omitempty"`
    MetricsQueriesTTL     *metav1.Duration            `json:"metricsQueriesTTL,omitempty"`
    TLSConfig             *ClusterMonitoringTLSConfig `json:"tls,omitempty"`
}

type ClusterMonitoringTLSConfig struct {
    Enabled bool `json:"enabled,omitempty"`
}
```

No changes to `NodeMaintenanceWindow` or the `EnablePDB` / `managed.services`
fields — they already exist and are correct.

## Controller changes

```
cluster_controller.go   → add r.reconcilePDB + r.reconcileMon to Reconcile loop
                           (before steadyState; monitoring after certs, PDB
                            after service reconciliation)
cluster_monitoring.go   → NEW: reconcilePodMonitor, injectDefaultMonitoringConfigMap,
                           collectCustomQueries
cluster_guard.go        → NEW: reconcilePDB, reconcileNodeMaintenanceWindow,
                           reconcileFencingAnnotation, checkDeletionGuard

SetupWithManager:        → Owns(&policyv1.PodDisruptionBudget{})
                           Owns(&monitoringv1.PodMonitor{})
```

## Instance manager changes

```
pkg/management/mysql/webserver/metrics.go  → NEW: /metrics handler (promhttp)
                                              + query executor + cache
pkg/management/mysql/webserver/server.go   → add /metrics route
pkg/management/mysql/instance/controller.go → create cnmsql_metrics_exporter user
                                              at bootstrap; wire MetricsCollector
```

## Conventions

- PodMonitor selector label: `cnmsql.io/cluster: <cluster-name>`
- Fencing annotation: `cnmsql.cnmsql.co/fencing` (`true` / `false`)
- Deletion guard annotation: `cnmsql.cnmsql.co/skipDeleteGuard`
- Default monitoring ConfigMap name: `cnmsql-default-monitoring` (in operator namespace)
- Metrics port: 9187, path: `/metrics`
- Metrics exporter MySQL user: `cnmsql_metrics_exporter`
- All queries run via `application_name=cnmsql_metrics_exporter`

## Execution order

1. M13.1: `/metrics` endpoint + default ConfigMap + PodMonitor → make metrics
   surface before addressing guards
2. M13.2: PDB + node maintenance window + semi-sync self-healing
3. M13.3: Fencing annotation + deletion guard
4. Integration: wire everything into the Reconcile loop, add RBAC, test
5. Docs: `monitoring.md`, `api-reference.md` updates, `replication-failover.md`
   fencing section

## References

- CNPG monitoring: `cloudnative-pg/docs/src/monitoring.md`
- CNPG PDB logic: `cloudnative-pg/internal/controller/cluster_create.go`
- CNPG MonitoringConfiguration: `cloudnative-pg/api/v1/cluster_types.go`
- CNPG specs builder: `cloudnative-pg/internal/controller/specs/`
- Current cnmsql types: `api/v1alpha1/cluster_types.go`
- Current reconciliation: `internal/controller/cluster_controller.go`
