# CNMSQL — AI Agent Instructions

Cloudnative MySQL: a CNPG-analog operator for Percona Server for MySQL.

## Project Overview

No operator exists to manage MySQL in a good way — some exist but they all have flaws. CNMSQL reproduces what CNPG did with PostgreSQL but for MySQL, ensuring good operations. Kubernetes APIs are the source of truth for MySQL operations, with guards and QoL like CNPG.

## Key Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| D1 | Mirror CNPG's design and resource set, adapted to MySQL | CNPG is a proven, sane DBMS-operator design |
| D2 | Build for **Percona Server for MySQL** only (8.0, 8.4, 9.x) | Percona ships the tooling (XtraBackup, etc.) we rely on. 5.6 is NOT supported |
| D3 | **Pods + PVCs + instance-manager** (CNPG-style), NOT StatefulSets | Max control over per-instance lifecycle, promotion, failover |
| D4 | First replication topology: **async / semi-sync GTID** primary-replica | Closest analog to CNPG streaming replication; Group Replication deferred |
| D5 | API group `mysql.cnmsql.co` | From scaffolded PROJECT file |
| D6 | Replica provisioning: **XtraBackup-first** (Clone plugin later) | Universal across supported versions, Percona-native |
| D7 | Operator↔instance control API: **HTTP + mTLS** | Simple, debuggable, CNPG parity |
| D8 | Instance image: **custom slim Debian+Percona APT image** (`Dockerfile.instance`), rootless (uid 1001) | Lighter than upstream percona/percona-server (~75% smaller), only tools we need |
| D9 | Instance manager reaches mysqld via **admin interface** (`admin_address`/`admin_port`, 8.0.14+), falls back to socket + reserved slot | Socket does NOT bypass `max_connections`; manager must never be locked out |
| D10 | Manager binary injected at pod startup via bootstrap-controller init container copying `/manager` from operator image into shared `scratch-data` emptyDir | CNPG pattern; operator and instance manager versions always identical |
| D11 | Backup worker Jobs use the **same cnmsql instance image** as the Cluster | Keeps worker version-aligned with XtraBackup tooling |
| D12 | Integration tests run a **version matrix** (8.0, 8.4, 9.x) | Every supported version exercised end-to-end |
| D13 | Structured logs project-wide (operator, instance manager, child processes) | controller-runtime `logr`, K8s logging style; child-process output wrapped into structured log entries |
| D14 | Per-instance ServiceAccount identity + validating status webhook | Prevents a rogue instance from patching `Cluster` status fields it does not own; see `design/020-status-instance-webhook.md` |

## Features

- Version 8.0, 8.4, and 9.x (Percona Server for MySQL)
- Scheduling management (CNPG-style)
- Physical backup and recovery (XtraBackup to S3-compatible object store)
- Automated failover with RPO/RTO (CNPG-style)
- Planned switchover by promoting a selected replica
- Scaling up/down automatically
- Persistent Volume Management
- In-place upgrade of cluster for major upgrades
- In-place operator/instance-manager upgrades (no pod restart)
- Binlog streaming (continuous archiving, PITR)
- Declarative management of MySQL configurations (settings, users, databases)
- Self-healing (PDBs, semi-sync reconciliation, liveness isolation)
- Guards (fencing, deletion guard, primary lease)
- Monitoring (Prometheus metrics, PodMonitor, mysqld_exporter scrapers)
- User-defined service exposition (additional services, templates)
- User-managed TLS certificates
- ProxySQL support (M14, pending)
- `kubectl cnmsql` CLI plugin
- Operation gated with mTLS where necessary, TLS otherwise

## Project Structure

```
cmd/main.go                     Manager entry (cobra: bootstrap, instance, operator)
api/v1alpha1/*_types.go         CRD schemas (+kubebuilder markers)
api/v1alpha1/zz_generated.*     Auto-generated (DO NOT EDIT)
internal/controller/*           Reconciliation logic (cluster, backup, scheduledbackup, database)
internal/controller/cluster_*.go  Cluster reconciler split by concern (plan, resources, pki, pod, status, upgrade, guard, semisync, managed_roles)
internal/webhook/*              Validation/defaulting (if present)
pkg/management/mysql/           Instance manager packages (webserver, instance, replication, config, pool, user, binlog, metrics, objectstore)
pkg/management/mysql/instance/  Instance lifecycle (runner, supervisor, process_log, isolation, rolereconciler)
config/crd/bases/*              Generated CRDs (DO NOT EDIT)
config/rbac/role.yaml           Generated RBAC (DO NOT EDIT)
config/samples/*                Example CRs (edit these)
Makefile                        Build/test/deploy commands
PROJECT                         Kubebuilder metadata (DO NOT EDIT)
Dockerfile                      Multi-stage operator image
images/Dockerfile.instance      Slim instance image
images/versions.json            Percona version matrix
design/                          Design documents and plans (COMMITTED) — see design/INDEX.md
docs/                           Docusaurus documentation site
```

## Critical Rules

### Never Edit These (Auto-Generated)
- `config/crd/bases/*.yaml` — from `make manifests`
- `config/rbac/role.yaml` — from `make manifests`
- `config/webhook/manifests.yaml` — from `make manifests`
- `**/zz_generated.*.go` — from `make generate`
- `PROJECT` — from kubebuilder CLI

### Never Remove Scaffold Markers
Do NOT delete `// +kubebuilder:scaffold:*` comments. CLI injects code at these markers.

### Keep Project Structure
Do not move files around. The CLI expects files in specific locations.

### Always Use CLI Commands
Always use `kubebuilder create api` and `kubebuilder create webhook` to scaffold. Do NOT create files manually.

### E2E Tests Require an Isolated Kind Cluster
Run e2e tests against a dedicated Kind cluster, not a real dev/prod cluster.

## Agent Workflow

### Progress Tracking
- **Committed plans:** See `design/INDEX.md` for the registry of all design documents.
- **New feature plans:** Store in `design/` with the naming convention `NNN-title.md` (next available number). Add entries to `design/INDEX.md`. Ask permission before implementing.
- **Decisions:** Record architectural decisions in this file under §Key Decisions.
- **Superseding plans:** When a new plan replaces an existing one, follow the convention in `design/INDEX.md` §Superseding Convention. Key rules: (1) the new plan must be self-contained — copy all context from the old one, (2) add a `> Supersedes [old](old.md)` note at the top, (3) add a `> Superseded by [new](new.md)` banner to the old file, (4) update INDEX.md to mark the old as `superseded` and add the new one.
- **Reference codebases:** The CNPG source code is used as a reference for patterns and design decisions. It is not committed to this repository — clone `cloudnative-pg/cloudnative-pg` separately for reference reading.

### Code Standards
- Every piece of code should be unit tested and e2e tested.
- Follow Kubernetes logging style: capitalized, active voice, no trailing period, balanced key/value pairs.
- Controller/manager code uses `logr` through controller-runtime context loggers.
- MySQL/child-process output wrapped into structured log entries with stream, process context fields.

### Conventional Commits
- Use conventional commits (e.g., `feat:`, `fix:`, `chore:`).
- No coauthor, keep casual, no commit body, no uppercases.

### Documentation
- For every milestone, update `docs/` accordingly.
- Run `npm run build` from `docs/` to verify the Docusaurus site builds cleanly.

## After Making Changes

```
make manifests  # Regenerate CRDs/RBAC from markers
make generate   # Regenerate DeepCopy methods + scraper code
make lint-fix   # Auto-fix code style
make test       # Run unit tests
```

## CLI Commands

### Create API
```bash
kubebuilder create api --group <group> --version <version> --kind <Kind>
```

### Create Webhooks
```bash
# Validation + defaulting
kubebuilder create webhook --group <group> --version <version> --kind <Kind> --defaulting --programmatic-validation

# Conversion webhook (multi-version APIs)
kubebuilder create webhook --group <group> --version v1 --kind <Kind> --conversion --spoke v2
```

### Controller for External Types
```bash
kubebuilder create api --group <external-group> --version <version> --kind <Kind> \
  --controller=true --resource=false \
  --external-api-path=<import-path> --external-api-domain=<domain>
```

## API Design

**Key markers for `api/v1alpha1/*_types.go`:**
```go
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced

// On fields:
// +kubebuilder:validation:Required
// +kubebuilder:validation:Minimum=1
// +kubebuilder:default="value"
```

- Use `metav1.Condition` for status (not custom string fields)
- Use predefined types: `metav1.Time` instead of `string` for dates
- Follow K8s API conventions: Standard field names (`spec`, `status`, `metadata`)

## Controller Design

**Implementation rules:**
- **Idempotent reconciliation**: Safe to run multiple times
- **Re-fetch before updates**: `r.Get(ctx, req.NamespacedName, obj)` before `r.Update`
- **Structured logging**: `log := log.FromContext(ctx); log.Info("msg", "key", val)`
- **Owner references**: Enable automatic garbage collection (`SetControllerReference`)
- **Watch secondary resources**: Use `.Owns()` or `.Watches()`, not just `RequeueAfter`
- **Finalizers**: Clean up external resources (buckets, S3 objects, etc.)

**RBAC markers:**
```go
// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=clusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=clusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=clusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
```

## Testing & Development

```bash
make test              # Run unit tests (uses envtest: real K8s API + etcd)
make test-integration  # Run integration tests (requires Docker, real Percona containers)
make test-e2e          # Run e2e tests (requires Kind cluster + MinIO)
make run               # Run locally (uses current kubeconfig context)
make lint              # Run golangci-lint
make lint-fix          # Auto-fix code style
```

Tests use **Ginkgo + Gomega** (BDD style). Check `suite_test.go` for setup.

## Deployment

```bash
export IMG=<registry>/<project>:tag
make docker-build docker-push IMG=$IMG
make deploy IMG=$IMG

# Verify
kubectl apply -k config/samples/
kubectl logs -n cnmsql-system deployment/cnmsql-controller-manager -c manager -f
```

## Operator Upgrade Architecture

### How the binary flows

The operator and instance manager are the **same binary**. The bootstrap-controller init container copies `/manager` from the operator image into each instance Pod's `scratch-data` emptyDir. Both the `bootstrap` and `mysql` containers execute `/controller/manager` from that shared volume.

### Upgrade detection

- The operator computes a SHA-256 hash of its own binary at startup → `status.operatorExecutableHash`
- Instance managers report their hash in `/status` on every poll
- Mismatch = stale instance → candidate for upgrade
- The pod template hash explicitly **excludes** the operator/bootstrap image — an operator bump does NOT mass-delete all Pods

### Two upgrade modes

1. **Rolling (default):** Delete and recreate Pods one at a time, replicas first, primary last. Switchover before primary delete (honors `primaryUpdateMethod` and `primaryUpdateStrategy`). Fenced instances skipped.
2. **In-place** (`spec.inPlaceInstanceManagerUpdates: true`): Stream the new binary via `POST /instance/manager/upgrade`, the instance manager re-execs itself via `syscall.Exec`. mysqld stays running throughout — no pod restart, no switchover.

### In-place upgrade internals

The spike (see `design/019-operator-upgrade.md`) proved `syscall.Exec` keeps the child mysqld alive:
- `execve()` preserves the PID and does NOT kill child processes
- The new image adopts mysqld via `syscall.Wait4(pid)` passed through `CNMYSQL_ADOPT_MYSQLD_PID` env var
- mysqld stdout/stderr are wired to inheritable `*os.File` targets (FIFO), NOT pipe-backed `processLogWriter`s, so fds survive the exec

The decoupling (see `design/019-operator-upgrade.md`) redesigned the supervisor:
- **`DetachedSupervisor`** supervises mysqld by PID (not `exec.Cmd`), using `syscall.Kill` for signals
- `AdoptProcess(pid)` re-adopts an already-running mysqld after re-exec
- The original `ProcessSupervisor` stays for short-lived bootstrap servers (join, initializer, restore, restore_pitr)

### Key upgrade files

- `internal/controller/cluster_upgrade.go` — rollout orchestration (stale detection, candidate ordering, serialized reconcile)
- `pkg/management/mysql/instance/detached_supervisor.go` — PID-based mysqld supervision + adopt mode
- `pkg/management/mysql/webserver/upgrade.go` — `POST /instance/manager/upgrade` and `POST /instance/manager/restart-inplace` endpoints
- `internal/controller/cluster_resources.go:restartTriggeringPodSpec` — normalizes bootstrap image to avoid mass-hash-invalidation

## Milestone Status

> **Authoritative index:** `design/INDEX.md` — use it for quick navigation.

- [x] **M1** — API design (types-only). → `design/001-api-design.md`
- [x] **M2** — Instance manager (PID1 wrapping mysqld). → `design/002-instance-manager.md`
- [x] **M2.5** — Custom slim instance image. → `design/003-instance-image.md`
- [x] **M3** — Cluster reconciler (single instance bootstrap). → `design/004-cluster-reconciler.md`
- [x] **M4** — Replicas, primary tracking, services & traffic routing. → `design/005-replicas-and-services.md`
- [x] **M5** — Switchover / failover (RPO/RTO). → `design/006-switchover-failover.md`
- [x] **M5.5** — Dynamic instance role (CNPG pull-model). → `design/007-dynamic-role.md`
- [x] **M6** — Physical backup & recovery (XtraBackup). → `design/008-physical-backup-recovery.md`
- [x] **M7** — Binlog streaming (M7.1 continuous archiving, M7.2 PITR). → `design/009-binlog-streaming.md`
- [x] **M8** — ScheduledBackup (M8.1 cron scheduler, M8.2 retention GC). → `design/010-scheduled-backup.md`
- [x] **M9** — Recover from raw S3 (backup & PITR). → `design/011-raw-s3-recovery.md`
- [x] **M10** — User-defined exposition. → `design/012-user-defined-exposition.md`
- [x] **M11** — User-managed TLS certificates. → `design/013-user-managed-tls.md`
- [x] **M12** — Declarative config / users / databases. → `design/014-declarative-config-users-databases.md`
- [x] **M13** — Monitoring, self-healing, guards. → `design/015-monitoring-self-healing-guards.md`
- [x] **M13.4** — Primary Lease for failover fencing. → `design/017-primary-lease.md`
- [x] **M16** — `kubectl cnmsql` CLI Plugin. → `design/016-kubectl-plugin.md`
- [x] **M18** — Manager binary injection at pod startup. → `design/018-bootstrap-dbs.md`
- [x] **Operator upgrades** — Rolling + in-place instance-manager upgrades. → `design/019-operator-upgrade.md`

## CNPG-Parity Follow-Ups (from NOTES.md)

- Check CNMSQL has the same safety posture as CNPG around instance status collection and failover decisions: operator-side instance-manager status queries during reconciliation/resync, Kubernetes Pod `Ready` as the decisive primary-unserviceable signal, no failover solely because the manager status endpoint is temporarily unreachable, instance-manager-side guarded status updates for primary role transitions.
- Add optional remote backup cleanup on `Backup` deletion through a finalizer. Needs an explicit safety policy: deleting a `Backup` CR could remove `backup.xbstream` and `metadata.json` from the object store, but should be opt-in or guarded so users don't accidentally destroy their recovery window.

## References

### Reference Codebases
- CNPG source — clone `cloudnative-pg/cloudnative-pg` for patterns, process models, upgrade logic

### Essential Reading
- **Kubebuilder Book**: https://book.kubebuilder.io
- **controller-runtime FAQ**: https://github.com/kubernetes-sigs/controller-runtime/blob/main/FAQ.md
- **Good Practices**: https://book.kubebuilder.io/reference/good-practices.html
- **Logging Conventions**: https://github.com/kubernetes/community/blob/master/contributors/devel/sig-instrumentation/logging.md
- **API Conventions**: https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md
- **Operator Pattern**: https://kubernetes.io/docs/concepts/extend-kubernetes/operator/
- **Markers Reference**: https://book.kubebuilder.io/reference/markers.html
