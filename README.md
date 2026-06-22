<p align="center">
  <img src="https://raw.githubusercontent.com/cnmsql/cnmsql/main/docs/static/img/cnmsql.png" alt="CNMSQL - CloudNative for MySQL" width="200" />
</p>

<p align="center">
  <a href="https://github.com/cnmsql/cnmsql/actions/workflows/test.yml"><img src="https://github.com/cnmsql/cnmsql/actions/workflows/test.yml/badge.svg" alt="Unit Tests" /></a>
  <a href="https://github.com/cnmsql/cnmsql/actions/workflows/test.yml"><img src="https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fcnmsql%2Fcnmsql%2Fbadges%2F.github%2Fbadges%2Fcoverage.json" alt="Code Coverage" /></a>
  <a href="https://github.com/cnmsql/cnmsql/actions/workflows/lint.yml"><img src="https://github.com/cnmsql/cnmsql/actions/workflows/lint.yml/badge.svg" alt="Lint" /></a>
  <a href="https://github.com/cnmsql/cnmsql/actions/workflows/e2e.yml?query=event%3Aschedule"><img src="https://github.com/cnmsql/cnmsql/actions/workflows/e2e.yml/badge.svg?branch=main&event=schedule" alt="Nightly E2E Tests" /></a>
  <a href="https://github.com/cnmsql/cnmsql/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License" /></a>
  <a href="https://goreportcard.com/report/github.com/cnmsql/cnmsql"><img src="https://goreportcard.com/badge/github.com/cnmsql/cnmsql" alt="Go Report Card" /></a>
  <a href="https://cnmsql.co"><img src="https://img.shields.io/badge/docs-cloudnative--mysql.io-blue" alt="Documentation" /></a>
</p>

# CNMSQL - CloudNative for MySQL

A Kubernetes operator for [Percona Server for MySQL](https://www.percona.com/software/mysql-database/percona-server). It runs MySQL clusters with operator-owned lifecycle management, GTID replication with automatic failover, physical backups to S3-compatible object storage, and point-in-time recovery.

> CNMSQL - CloudNative for MySQL is an independent project. It is not affiliated with, endorsed by, or associated with Oracle, MySQL, the [CNCF](https://www.cncf.io/), or the [CloudNativePG](https://cloudnative-pg.io/) project and its maintainers.

Full documentation at **[cnmsql.co](https://cnmsql.co)**.

## What It Does

Declare a `Cluster` resource and the operator provisions Pods, PVCs, credentials, TLS material, and role-routed Services.

**Replication and failover.** One primary plus GTID-based replicas. Planned switchover for upgrades, automatic failover when the primary goes down, and rejoin of a former primary as a replica.

Each cluster gets three Services: a read-write endpoint for the primary (`-rw`), a read-only endpoint for replicas (`-ro`), and a read endpoint for any ready instance (`-r`). Routing follows the `mysql.cnmsql.co/role` label and tracks failover automatically.

Physical backups run through XtraBackup to S3-compatible object storage. `Backup` resources trigger one-shot archives; `ScheduledBackup` handles cron-driven runs.

Continuous binlog archiving feeds point-in-time recovery so you can restore to a chosen timestamp, not just the last full backup.

`Database` resources handle schemas, owners, and privileges declaratively with no hand-run SQL.

`ImageCatalog` and `ClusterImageCatalog` resolve instance images from the MySQL major version for centralized version pinning.

Prometheus metrics with mTLS between the operator and instances, plus MySQL TLS.

API group: `mysql.cnmsql.co/v1alpha1`. Resources: `Cluster`, `Database`, `Backup`, `ScheduledBackup`, `ImageCatalog`, and `ClusterImageCatalog`. See the [API reference](https://cnmsql.co/api-reference) for every field.

## CLI Plugin

The repository includes a `kubectl` plugin, `kubectl cnmsql`, for day-to-day operations: cluster status, fencing, promotion, restart, reload, backups, and more.

**Install the latest release (no checkout needed):**

```bash
curl -sSfL https://github.com/cnmsql/cnmsql/raw/main/hack/install-cnmsql-plugin.sh | sh -s -- -b ~/.local/bin
```

**Or build from the repo:** `make install-plugin`.

## Quickstart

Bring up the operator and a three-instance cluster in a local Kind environment. The [full quickstart](https://cnmsql.co/quickstart) has the complete walkthrough.

**Prerequisites:** `go`, `docker`, `kubectl`, `kind`, `make`, and `cert-manager` in the target cluster.

```bash
# Build and load images
make docker-build IMG=cnmsql-controller:dev
docker pull ghcr.io/cnmsql/cnmsql-instance:8.4
kind load docker-image cnmsql-controller:dev --name cnmsql-test-e2e
kind load docker-image ghcr.io/cnmsql/cnmsql-instance:8.4 --name cnmsql-test-e2e

# Deploy the operator
make install
make deploy IMG=cnmsql-controller:dev
make install-plugin
```

Create a cluster:

```yaml
apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: cluster-sample
spec:
  instances: 3
  imageName: ghcr.io/cnmsql/cnmsql-instance:8.4
  storage:
    size: 10Gi
  mysql:
    binlogFormat: ROW
  bootstrap:
    initdb:
      database: app
      owner: app
```

```bash
kubectl wait --for=condition=Ready cluster/cluster-sample --timeout=15m
kubectl cnmsql status cluster-sample
```

Connect through `cluster-sample-rw`, `cluster-sample-ro`, or `cluster-sample-r`. Credentials are in a generated Secret:

```bash
kubectl get secrets -l mysql.cnmsql.co/cluster=cluster-sample
```

## Documentation

- [Cluster lifecycle](https://cnmsql.co/cluster-lifecycle)
- [Replication and failover](https://cnmsql.co/replication-failover)
- [Physical backup and recovery](https://cnmsql.co/backup-recovery)
- [Point-in-time recovery](https://cnmsql.co/pitr)
- [Scheduled backups](https://cnmsql.co/scheduled-backups)
- [Object store configuration](https://cnmsql.co/object-store)
- [Multi-tenancy](https://cnmsql.co/multi-tenancy)
- [Security model](https://cnmsql.co/security-model)
- [Operations runbooks](https://cnmsql.co/operations)
- [Troubleshooting](https://cnmsql.co/troubleshooting)

## Development

Built with [Kubebuilder](https://book.kubebuilder.io). Common targets:

```bash
make manifests generate   # Regenerate CRDs, RBAC, and DeepCopy after editing types
make lint-fix             # Auto-fix code style
make test                 # Run unit tests (Ginkgo + Gomega on envtest)
make run                  # Run the controller locally against the current kubeconfig
```

Run `make help` for the full list.

## License

Apache License 2.0. See [LICENSE](LICENSE).
