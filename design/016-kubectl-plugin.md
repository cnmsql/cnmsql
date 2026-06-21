# 016 — kubectl cnmsql CLI Plugin

**Status:** done
**Milestone:** M16

A `kubectl` plugin for managing cnmsql clusters, modeled on CNPG's `kubectl cnpg`. Provides cluster health at-a-glance, guarded state mutations, and admin commands.

**Goal:** Build a `kubectl` plugin (`kubectl cnmsql`) that gives operators a
first-class CLI for managing and administering cnmsql clusters. Modeled on
CNPG's `kubectl cnpg` plugin, leveraging the same communication channels already
used by the cnmsql operator.

## Why

Currently, cnmsql administration requires raw `kubectl` commands, `kubectl
patch` on status subresources, direct HTTP/mTLS calls to instance managers, or
SQL shells inside pods. This is usable but slow and error-prone. A dedicated
plugin provides:

- **At-a-glance cluster health** (like CNPG's `status` — the most-used command).
- **Guarded state mutations** (fence, promote, restart, maintenance) via the
  same operator-annotations/status-subresource contracts the reconciler already
  understands.
- **Instance-level debugging** (logs, psql-equivalent MySQL shell, status dumps,
  per-instance metrics).
- **Benchmarking primitives** (sysbench for MySQL performance, fio for storage).
- **Declarative user/database management** via the instance manager control API.

## Architecture

### Plugin entry point

**Binary:** `cmd/kubectl-cnmsql/` (separate binary, built by Makefile).

**Framework:** Cobra + `k8s.io/cli-runtime` for kubeconfig/context/namespace
auto-discovery (same as `kubectl`, `kubectl cert-manager`, `kubectl cnpg`).

**Build target:** `kubectl-cnmsql` binary deployed in `$PATH` so `kubectl cnmsql
<cmd>` works natively.

### Communication channels (three tiers)

| Tier | Mechanism | Use cases |
|------|-----------|-----------|
| **T1 — Kubernetes API (CRDs)** | controller-runtime `client.Client` | Read Cluster/Backup/ScheduledBackup/Database status; patch Cluster status (switchover); list pods/PVCs/secrets/services |
| **T2 — Instance manager control API (via port-forward)** | client-go SPDY port-forward to the pod's `control` port (8080), then mTLS HTTP over `https://localhost:<localPort>/<path>` | Per-instance `/status`, user/database CRUD, semi-sync adjustment, per-instance restart |
| **T3 — Pod exec / port-forward** | client-go SPDY remotecommand + port-forward | Log streaming, config inspection, benchmark job management. NOTE: the instance image is slimmed — it ships **no `mysql` client and no `curl`/`wget`** — so the MySQL shell and metrics scrape use **local tooling over a port-forward**, not in-pod exec. |

**Reachability (important):** Unlike the operator — which runs in-cluster and
can dial `<instance>.<ns>.svc:8080` directly — the plugin runs on an operator's
workstation where Pod/Service DNS is not routable. Every T2 call therefore goes
through a **client-go SPDY port-forward** to the target Pod, after which the
plugin dials `https://localhost:<localPort>`. The control API requires and
verifies a client cert (`tls.RequireAndVerifyClientCert`), so the plugin
presents the operator client cert and **must override `tls.Config.ServerName`
to `<instance>.<ns>.svc`** so server-cert verification succeeds over the
localhost connection. TLS material comes from the operator-managed secrets
`<cluster>-ca` and `<cluster>-client-tls` — same pattern as the operator's
`HTTPControlClient`.

### Project layout

```
cmd/kubectl-cnmsql/
  main.go                          # Cobra root, plugin setup
  plugin/
    client.go                      # k8s client + config init
    tls.go                         # mTLS HTTP client factory
    print.go                       # Output formatters (table, json, yaml)
    helpers.go                     # Shared utilities (resolve instance, etc.)
    portforward.go                 # client-go SPDY port-forward helper (T2/T3)
  cmd/
    status.go                      # Full cluster status overview
    logs.go                        # Instance/cluster log streaming
    mysql.go                       # MySQL shell (psql equivalent)
    promote.go                     # Planned switchover → patch Cluster.status.targetPrimary
    fence.go                       # Fence/unfence instance → pod annotation
    backup.go                      # Trigger on-demand backup → Create ScheduledBackup(immediate)
    restart.go                     # Restart cluster/instance → annotation + delete pod
    reload.go                      # Reload config → Cluster annotations
    bench.go                       # Sysbench benchmark → Job scheduler
    fio.go                         # Storage benchmark (fio) → Job scheduler
    maintenance.go                 # Node maintenance window toggle
    destroy.go                     # Destroy instance (PVC-aware)
    metrics.go                     # Scrape/view per-instance metrics
    certificate.go                 # Generate client cert from cluster CA
    user.go                        # User CRUD via control API
    database.go                    # Database CRUD via control API
    report.go                      # Diagnostic bundle (operator + cluster)
    version.go                     # Build info
```

## Commands

### Command tree

```
kubectl cnmsql
├── status CLUSTER                    [quick-glance health overview]
├── logs CLUSTER [INSTANCE]           [stream logs, follow]
├── mysql CLUSTER [INSTANCE]          [shell into MySQL pod]
├── promote CLUSTER INSTANCE          [promote replica → primary]
├── fence on|off CLUSTER INSTANCE     [fence/unfence from routing]
├── backup CLUSTER                    [trigger on-demand base backup]
├── restart CLUSTER [INSTANCE]        [restart all or single instance]
├── reload CLUSTER                    [reload my.cnf parameters]
├── bench CLUSTER                     [run sysbench benchmark]
├── fio [NODE]                        [run fio storage benchmark]
├── maintenance set|unset [CLUSTER]   [toggle node maintenance window]
├── destroy CLUSTER INSTANCE          [destroy instance + optional PVC]
├── metrics CLUSTER [INSTANCE]        [scrape/view Prometheus metrics]
├── certificate CLUSTER               [generate client TLS cert]
├── user create|alter|drop|list CLUSTER  [user management]
├── database create|drop|list CLUSTER    [database management]
├── report operator|cluster           [diagnostic bundles]
├── version                           [plugin version info]
└── completion [bash|zsh|fish]        [shell autocomplete]
```

### 1. `status CLUSTER` — Quick-glance health overview

**Purpose:** The flagship command. Show the complete state of a cluster.

**Data sources:**
- `Cluster.status` via T1 (topology, phase, GTIDs, diverged instances, fenced
  instances, archiving health, certificates, managed roles, backup retention).
- `GET /status` on each instance via T2 (role, GTID, replication lag,
  semi-sync, archiving per-instance, uptime).
- Pod listing via T1 (container restarts, nodes, resource usage).

**Data source priority:** Read everything available from `Cluster.status` and
the Pod list (T1) **first** — primary/target primary, roles, fenced/diverged
instances, GTIDs, certificates, managed roles and archiving health are already
reconciled there. Fall back to per-instance `GET /status` (T2 port-forward)
**only** for live deltas not in the CR (replication lag, uptime). T2 fetches
run **concurrently with a per-instance timeout**; an unreachable instance is
rendered as a degraded row rather than failing the whole command.

**Display sections (CNPG-style, color-coded with `aurora`/`tabby`):**
1. **Cluster summary** — name, namespace, phase, image, instances
   (ready/total), current primary, target primary (if switchover in
   progress).
2. **Instances** — table: name, role (primary/replica), readiness, GTID
   executed, replication lag (seconds), container restarts, node, uptime.
3. **Continuous archiving** — enabled, last binlog, last GTID, pending
   files, last error (if any), RPO lag.
4. **Backups** — last base backup (ID, completed at), next scheduled
   backup (if ScheduledBackup exists), last retention run.
5. **Managed roles** — table: name@host, status (reconciled/not-managed/
   pending/cannot-reconcile), privileges.
6. **Certificates** — expiry table per cert secret.
7. **Services** — rw/ro/r cluster IPs, managed additional services.
8. **PDB** — primary/replica PDB status if enabled.

**Flags:** `-v` (verbose, shows raw JSON), `-o json|yaml` (output format).

### 2. `logs CLUSTER [INSTANCE]` — Log streaming

**Purpose:** Stream logs from all or one instance. CNPG's `logs cluster`.

**Mechanism:** Uses T1 (kubernetes typed client) to stream pod logs via
`CoreV1().Pods(namespace).GetLogs(podName, &v1.PodLogOptions{Follow: true})`.

**Behavior:**
- Without instance: streams logs from all pods in the cluster, prefixing
  each line with `[<instance>]`.
- With instance: streams only that instance's logs.
- Supports `-f`/`--follow`, `--tail=N`, `-t`/`--timestamps`.

**Subcommand:** `logs pretty` — reads JSON-structured logs from stdin and
color-formats them (key=value → colorized).

### 3. `mysql CLUSTER [INSTANCE]` — MySQL shell

**Purpose:** Like CNPG's `psql`, open an interactive `mysql` shell on the
primary (or a specific instance).

**Mechanism:** The instance image ships **no `mysql` client binary** (it is
stripped in `Dockerfile.instance`), so exec-into-pod is not possible. Instead
the plugin **port-forwards the pod's MySQL port (3306, or admin port 33062)**
to localhost and shells out to the operator's **locally installed `mysql`
client**: `mysql -h 127.0.0.1 -P <localPort> -u root ...`.

**Prerequisite:** a `mysql`/`mariadb` client installed on the operator's
machine (documented as a plugin prerequisite).

**Behavior:**
- Without instance: connects to primary.
- With instance: connects to that specific instance.
- Supports `--db=<name>` to select a database.
- Reads root password from operator-managed secret (via T1) and passes it via
  the `MYSQL_PWD` environment variable — **never** on the command line.
- Passes through `--` after the instance name to pass extra args to mysql
  (e.g., `kubectl cnmsql mysql mycluster -- --batch -e "SELECT 1"`).

### 4. `promote CLUSTER INSTANCE` — Planned switchover

**Purpose:** Promote a replica to primary.

**Mechanism:** Patches `Cluster.status` via T1:
- Sets `status.targetPrimary = <instance>`.
- Sets `status.targetPrimaryTimestamp = now()`.
- The operator's `switchover` reconciliation path handles the rest
  (validates GTID, demotes old primary, waits for catch-up bounded by
  `maxSwitchoverDelay`).

**Guards:**
- Rejects if instance is already the current primary.
- Rejects if instance is diverged or fenced.

**Alternative approach (faster):** Could call `POST /promote` on target and
`POST /demote` on old primary directly via T2, but the status-subresource
path is safer (operator coordinates the entire switchover, including
service routing, role labels, and other replicas).

### 5. `fence on|off CLUSTER INSTANCE` — Fencing

**Purpose:** Isolate an instance from routing (e.g., for maintenance,
debugging, or protecting a diverged replica).

**Mechanism:**
- `fence on`: `kubectl annotate pod <instance> cnmsql.cnmsql.co/fencing=true`
- `fence off`: `kubectl annotate pod <instance> cnmsql.cnmsql.co/fencing-`

The operator's `reconcileFencing` picks up the annotation and:
- Removes the instance from all routing services.
- Excludes it from failover candidates.
- The in-Pod role reconciler keeps it read-only and rejects promotion.

**Special:** Supports `*` as instance name to fence all instances.

### 6. `backup CLUSTER` — On-demand backup

**Purpose:** Trigger an immediate base backup.

**Mechanism:** Creates a `Backup` CR directly (T1) referencing the cluster via
`spec.cluster`. The `BackupReconciler` processes it and runs the backup job.
(Simpler and cleaner than minting a throwaway `ScheduledBackup`.)

**Flags:** `--backup-name` (optional, auto-generated if empty), `--wait`
(wait for completion and print status).

**Wait behavior:** Polls `Backup.status.phase` until `completed` or
`failed`, streaming backup progress from the worker pod logs.

### 7. `restart CLUSTER [INSTANCE]` — Restart

**Purpose:** Restart all instances (rolling) or a specific one.

**Mechanism:**
- Cluster-wide: writes a `cnmsql.cnmsql.co/restart` annotation
  with a RFC3339 timestamp on the Cluster CR. The reconciler performs a
  rolling restart (primary last if `primaryUpdateMethod=switchover`,
  immediate otherwise).
- Single instance: deletes the Pod (k8s recreates it with PVC retained).
  If the instance is the primary and a replica exists, the primary update
  strategy is honoured.

**Guards:** Confirmation prompt when restarting the primary.

### 8. `reload CLUSTER` — Reload configuration

**Purpose:** Apply `spec.mysql.parameters` changes without restarting.

**Mechanism:** Writes a `cnmsql.cnmsql.co/reload` annotation on
the Cluster CR with a RFC3339 timestamp. The reconciler re-renders my.cnf
ConfigMaps and triggers the instance manager to reload without restarting
mysqld (via unix-socket control connection: `SET GLOBAL ...` for supported
dynamic parameters).

**Note:** Static parameters (require mysqld restart) are logged and the
operator emits a Warning event.

### 9. `bench CLUSTER` — Sysbench benchmarking

**Purpose:** Run a MySQL performance benchmark against a cluster.

**Mechanism:** Creates a Kubernetes Job resource with a `sysbench` container
pointed at the cluster's `rw` service (T3 job management via T1):
- Creates a schema and test data via `sysbench oltp_read_write prepare`.
- Runs benchmarks: `oltp_read_write`, `oltp_read_only`, `oltp_write_only`,
  `oltp_point_select`.
- Reports results (transactions/sec, latency percentiles, queries/sec).
- Cleans up with `sysbench ... cleanup` on completion.

**Job image:** `severalnines/sysbench` or build our own slim sysbench image.

**Flags:** `--tables=N`, `--table-size=N`, `--threads=N`, `--time=N`,
`--tests=<list>`, `--db-name=<name>`, `--dry-run`, `--ttl` (auto-delete).

### 10. `fio [NODE]` — Storage benchmarking

**Purpose:** Benchmark storage performance (IOPS, throughput, latency) of
the PV underlying MySQL.

**Mechanism:** Creates a Deployment + PVC + ConfigMap running fio (T1):
- If `[NODE]` is specified, uses `nodeName` affinity to pin to that node.
- ConfigMap contains fio job definitions (randread, randwrite, randrw,
  read, write with ioengine=libaio).
- Results printed to stdout, optionally to JSON.

**Guards:** Confirmation prompt (fio can be disruptive if running on the
same physical disk as a live cluster).

**Flags:** `--storage-class=<sc>`, `--size=<size>`, `--dry-run`, `--ioengine=<engine>`.

### 11. `maintenance set|unset [CLUSTER]` — Node maintenance window

**Purpose:** Prepare a cluster for node maintenance (drain/reboot).

**Mechanism:** Patches `Cluster.spec.nodeMaintenanceWindow.inProgress` via T1:
- `set`: `inProgress = true`, optionally `reusePVC = true`.
  - If `reusePVC`, operator temporarily deletes PDBs so nodes can drain.
  - Recreated pods reattach existing PVCs.
- `unset`: `inProgress = false`, restores PDBs.

**Flags:** `--all-namespaces` (operate on all clusters), `--reuse-pvc`,
`-y`/`--yes` (skip confirmation).

### 12. `destroy CLUSTER INSTANCE` — Destroy instance

**Purpose:** Remove an instance permanently.

**Mechanism:** Deletes the Pod (T1), then finds and deletes associated Jobs
and PVCs (unless `--keep-pvc`). Inspired by CNPG's `destroy`.

**Behavior:**
- Without `--keep-pvc`: deletes PVCs, effectively removing the instance's
  data permanently.
- With `--keep-pvc`: removes owner references from PVCs so they survive
  cluster deletion, allowing later re-attachment.

**Guards:** Confirmation prompt listing what will be deleted (Pod, PVCs,
Jobs).

### 13. `metrics CLUSTER [INSTANCE]` — Metrics viewer

**Purpose:** Scrape the Prometheus `/metrics` endpoint from the instance
manager and display a curated, human-readable view.

**Mechanism:** Port-forwards the pod's metrics port (9187) to localhost and
scrapes `http://localhost:<localPort>/metrics`, then parses the Prometheus text
format. (Exec is not viable — the slim instance image ships no `curl`/`wget`.)
No external Prometheus required — the instance manager already exposes metrics
on port 9187.

**Display sections:**
- **MySQL global status** — connections, queries, bytes sent/received,
  InnoDB buffer pool hit rate, etc.
- **Replication** — slave status (if replica), seconds behind source,
  relay log space.
- **InnoDB** — buffer pool, row operations, IO, compression stats.
- **Binlog archiving** — last archived binlog, pending files, last error.
- **Custom queries** — user-defined queries if configured.

**Flags:** `-o json|yaml`, `--raw` (dump full Prometheus endpoint without
curation).

### 14. `certificate CLUSTER` — Client certificate generation

**Purpose:** Generate an X.509 client certificate signed by the cluster's
CA so operators/apps can connect to MySQL with mTLS.

**Mechanism:**
1. Reads the cluster's CA cert and key from `<cluster>-ca` Secret (T1).
2. Generates a new key pair signed by the cluster CA with the configured
   user CN.
3. Stores the resulting cert/key in a new Kubernetes Secret.

**Flags:** `--user=<name>` (CN), `--secret-name=<name>`, `--dry-run`,
`-o json|yaml`.

### 15. `user create|alter|drop|list CLUSTER` — User management

**Purpose:** Direct MySQL user management via the instance manager control
API (bypasses the Cluster CR's `managed.roles` — this is for ad-hoc
administration, not declarative reconciliation).

**Mechanism:** Calls T2 endpoints on the primary:
- `user create`: `POST /user/create` with JSON body (name, host, password,
  privileges, resource limits, tls requirement).
- `user alter`: `POST /user/alter` with partial JSON body (only changed
  fields).
- `user drop`: `POST /user/drop` with name and host.
- `user list`: `GET /user/list` → formatted table.

**Password handling:** Passwords are **never** accepted as CLI args (shell
history / `ps` leakage). `user create`/`user alter` read the password from
**`--password-stdin`** (or an interactive TTY prompt when stdin is a terminal)
and send it only in the JSON request body over the mTLS port-forward.

**Subcommands:**
```
kubectl cnmsql user create CLUSTER --name=app_user --host=% --privileges="SELECT,INSERT" --on="mydb.*" --password-stdin
kubectl cnmsql user alter CLUSTER --name=app_user --host=% --tls=x509
kubectl cnmsql user drop CLUSTER --name=app_user --host=%
kubectl cnmsql user list CLUSTER
```

### 16. `database create|drop|list CLUSTER` — Database management

**Purpose:** Direct MySQL database management via the instance manager
control API.

**Mechanism:** Calls T2 endpoints on the primary:
- `database create`: `POST /database/create` with JSON body (name,
  charset, collation).
- `database drop`: `POST /database/drop`.
- `database list`: `GET /database/list` → formatted list.

**Subcommands:**
```
kubectl cnmsql database create CLUSTER --name=myapp --charset=utf8mb4 --collation=utf8mb4_unicode_ci
kubectl cnmsql database drop CLUSTER --name=myapp
kubectl cnmsql database list CLUSTER
```

### 17. `report operator|cluster` — Diagnostic bundles

**Purpose:** Collect diagnostic information into a ZIP file for support.

**Mechanism:**
- `report operator`: Lists the cnmsql operator deployment(s), operator
  logs, related events, and cluster CRDs (T1).
- `report cluster CLUSTER`: Collects Cluster CR, Backup CRs,
  ScheduledBackup CRs, pods, PVCs, services, events, instance logs, and
  instance manager `/status` output (T1 + T2 + T3). Bundles into a ZIP
  file.

**Flags:** `--stop-redaction` (include potentially sensitive data in
logs/status), `--output=<file>`.

### 18. `version` — Version info

Standard version command printing build: version, commit SHA, build date,
Go version.

### 19. `completion` — Shell completion

Standard Cobra shell completion for `bash`, `zsh`, `fish`.

## Additional commands (future)

Beyond the user's explicit request, these are CNPG-parity commands that
would be natural additions:

| Command | Description | Priority |
|---------|-------------|----------|
| `logical show CLUSTER` | View binlog replication status (current log + position, GTID set, connected replicas) | Medium |
| `logical setup CLUSTER` | Set up binlog replication from an external cluster (wires `externalClusters` + `bootstrap.recovery`) | Low |
| `destroy cluster CLUSTER` | Force-delete a cluster (bypasses deletion guard, deletes all PVCs) | Low |
| `recover CLUSTER` | Bootstrap a new cluster from a backup (fill `bootstrap.recovery` on a fresh Cluster CR) | Low |
| `hibernate on|off CLUSTER` | Stop/start all instances (k8s StatefulSet scaling, not currently supported — future) | Low |

## Implementation order

### Phase 1 — Core infrastructure (M16.1)
1. `cmd/kubectl-cnmsql/main.go` — Cobra root, kubeconfig setup.
2. `plugin/client.go` — controller-runtime client + kubernetes typed client.
3. `plugin/tls.go` — mTLS HTTP client factory (reads secrets, builds
   `tls.Config`, targets `https://<pod>.<ns>.svc:8080`).
4. `plugin/print.go` — output helpers (color-coded tables via `tabby` +
   `aurora`, JSON/YAML marshalling).
5. `plugin/helpers.go` — shared helpers (resolve primary, list instances,
   build instance URL, parse instance name to ordinal).
6. Wire into Makefile (`make build-plugin`, `make install-plugin`).

### Phase 2 — Status & logs (M16.2)
1. `status` — full cluster overview.
2. `logs` — log streaming + pretty printer.
3. `version` — build info.

### Phase 3 — Cluster administration (M16.3)
1. `mysql` — interactive MySQL shell.
2. `promote` — planned switchover.
3. `fence` — fencing annotations.
4. `restart` — rolling/single restart.
5. `reload` — config reload.
6. `destroy` — instance destruction (PVC-aware).

### Phase 4 — Backup & metrics (M16.4)
1. `backup` — on-demand backup.
2. `metrics` — metrics viewer.
3. `maintenance` — node maintenance toggle.

### Phase 5 — Users, databases, certs (M16.5)
1. `user` — user CRUD subcommands.
2. `database` — database CRUD subcommands.
3. `certificate` — client cert generation.

### Phase 6 — Benchmarking & diagnostics (M16.6)
1. `bench` — sysbench Job manager.
2. `fio` — fio storage benchmark.
3. `report` — diagnostic bundles.
4. `completion` — shell completion.

### Phase 7 — Polish
1. E2E tests for all commands against a Kind cluster.
2. `make test-plugin` target.
3. Publish binary via GitHub Releases.

(Krew packaging deferred to a later pass.)

## Conventions

- **Plugin binary name:** `kubectl-cnmsql` → invoked as `kubectl cnmsql`.
- **TLS secrets for mTLS:** `<cluster>-ca` (CA cert), `<cluster>-client-tls`
  (client cert/key).
- **Instance manager access:** SPDY port-forward to the Pod's `control` port
  (8080), then `https://localhost:<localPort>/<path>` with TLS `ServerName`
  pinned to `<instanceName>.<namespace>.svc`.
- **Control port:** 8080 (mTLS), **health port:** 8081, **metrics port:** 9187.
- **Kubernetes annotations used:**
  - Fencing: `cnmsql.cnmsql.co/fencing: "true"`
  - Skip delete guard: `cnmsql.cnmsql.co/skipDeleteGuard: "true"`
  - Reload trigger: `cnmsql.cnmsql.co/reload: "<rfc3339>"`
  - Restart trigger: `cnmsql.cnmsql.co/restart: "<rfc3339>"`
- **Cluster short names:** `mysql`, `mysqlcluster`.
- **Backup short name:** `mybackup`.
- **ScheduledBackup short name:** `myscheduledbackup`.
- **Database short name:** `mydatabase`.

## Dependencies

| Dependency | Purpose |
|-----------|---------|
| `github.com/spf13/cobra` | CLI framework |
| `k8s.io/cli-runtime` | Kubeconfig & config flags |
| `k8s.io/client-go` | Typed Kubernetes client (pods, secrets, exec) |
| `sigs.k8s.io/controller-runtime` | CRD client (Cluster, Backup, etc.) |
| `github.com/cheynewallace/tabby` | Table rendering |
| `github.com/prometheus/common/expfmt` | Prometheus text format parsing |
| `github.com/go-sql-driver/mysql` | (optional, for direct MySQL connections in benchmarks) |

Existing dependencies in `go.mod` that do not need new imports:
- `sigs.k8s.io/controller-runtime` (already present)
- `k8s.io/client-go` (already present)
- `k8s.io/api` (already present)

## Verification

Each phase should pass:
- `go build ./cmd/kubectl-cnmsql/...`
- `go vet ./cmd/kubectl-cnmsql/...`
- `make lint-fix` (0 issues on plugin code)
- `go test ./cmd/kubectl-cnmsql/...` (unit tests for client setup, helpers, formatters)
- Manual smoke tests on a Kind cluster with a running cnmsql cluster
