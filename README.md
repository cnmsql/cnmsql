<p align="center">
  <img src="https://raw.githubusercontent.com/CloudNative-MySQL/cloudnative-mysql/main/docs/static/img/cnmysql.png" alt="CloudNative MySQL" width="200" />
</p>

<p align="center">
  <a href="https://github.com/CloudNative-MySQL/cloudnative-mysql/actions/workflows/test.yml"><img src="https://github.com/CloudNative-MySQL/cloudnative-mysql/actions/workflows/test.yml/badge.svg" alt="Unit Tests" /></a>
  <a href="https://github.com/CloudNative-MySQL/cloudnative-mysql/actions/workflows/lint.yml"><img src="https://github.com/CloudNative-MySQL/cloudnative-mysql/actions/workflows/lint.yml/badge.svg" alt="Lint" /></a>
  <a href="https://github.com/CloudNative-MySQL/cloudnative-mysql/actions/workflows/e2e.yml"><img src="https://github.com/CloudNative-MySQL/cloudnative-mysql/actions/workflows/e2e.yml/badge.svg" alt="E2E Tests" /></a>
  <a href="https://github.com/CloudNative-MySQL/cloudnative-mysql/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License" /></a>
  <a href="https://goreportcard.com/report/github.com/CloudNative-MySQL/cloudnative-mysql"><img src="https://goreportcard.com/badge/github.com/CloudNative-MySQL/cloudnative-mysql" alt="Go Report Card" /></a>
  <a href="https://cloudnative-mysql.io"><img src="https://img.shields.io/badge/docs-cloudnative--mysql.io-blue" alt="Documentation" /></a>
</p>

# CloudNative MySQL

A Kubernetes operator for [Percona Server for MySQL](https://www.percona.com/software/mysql-database/percona-server). It runs MySQL clusters with operator-owned lifecycle management, GTID replication with automatic failover, physical backups to S3-compatible object storage, and point-in-time recovery.

> CloudNative MySQL is an independent project. It is not affiliated with, endorsed by, or associated with the [CNCF](https://www.cncf.io/) or the [CloudNativePG](https://cloudnative-pg.io/) project and its maintainers.

Full documentation at **[cloudnative-mysql.io](https://cloudnative-mysql.io)**.

## What It Does

Declare a `Cluster` resource and the operator provisions Pods, PVCs, credentials, TLS material, and role-routed Services.

**Replication and failover.** One primary plus GTID-based replicas. Planned switchover for upgrades, automatic failover when the primary goes down, and rejoin of a former primary as a replica.

Each cluster gets three Services: a read-write endpoint for the primary (`-rw`), a read-only endpoint for replicas (`-ro`), and a read endpoint for any ready instance (`-r`). Routing follows the `mysql.cloudnative-mysql.io/role` label and tracks failover automatically.

Physical backups run through XtraBackup to S3-compatible object storage. `Backup` resources trigger one-shot archives; `ScheduledBackup` handles cron-driven runs.

Continuous binlog archiving feeds point-in-time recovery so you can restore to a chosen timestamp, not just the last full backup.

`Database` resources handle schemas, owners, and privileges declaratively with no hand-run SQL.

`ImageCatalog` and `ClusterImageCatalog` resolve instance images from the MySQL major version for centralized version pinning.

Prometheus metrics with mTLS between the operator and instances, plus MySQL TLS.

API group: `mysql.cloudnative-mysql.io/v1alpha1`. Resources: `Cluster`, `Database`, `Backup`, `ScheduledBackup`, `ImageCatalog`, and `ClusterImageCatalog`. See the [API reference](https://cloudnative-mysql.io/api-reference) for every field.

## CLI Plugin

The repository includes a `kubectl` plugin, `kubectl cnmysql`, for day-to-day operations: cluster status, fencing, promotion, restart, reload, backups, and more.

**Install the latest release (no checkout needed):**

```bash
curl -sSfL https://github.com/CloudNative-MySQL/cloudnative-mysql/raw/main/hack/install-cnmysql-plugin.sh | sh -s -- -b ~/.local/bin
```

**Or build from the repo:** `make install-plugin`.

## Quickstart

Bring up the operator and a three-instance cluster in a local Kind environment. The [full quickstart](https://cloudnative-mysql.io/quickstart) has the complete walkthrough.

**Prerequisites:** `go`, `docker`, `kubectl`, `kind`, `make`, and `cert-manager` in the target cluster.

```bash
# Build and load images
make docker-build IMG=cloudnative-mysql-controller:dev
docker pull ghcr.io/cloudnative-mysql/cloudnative-mysql-instance:8.4
kind load docker-image cloudnative-mysql-controller:dev --name cloudnative-mysql-test-e2e
kind load docker-image ghcr.io/cloudnative-mysql/cloudnative-mysql-instance:8.4 --name cloudnative-mysql-test-e2e

# Deploy the operator
make install
make deploy IMG=cloudnative-mysql-controller:dev
make install-plugin
```

Create a cluster:

```yaml
apiVersion: mysql.cloudnative-mysql.io/v1alpha1
kind: Cluster
metadata:
  name: cluster-sample
spec:
  instances: 3
  imageName: ghcr.io/cloudnative-mysql/cloudnative-mysql-instance:8.4
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
kubectl cnmysql status cluster-sample
```

Connect through `cluster-sample-rw`, `cluster-sample-ro`, or `cluster-sample-r`. Credentials are in a generated Secret:

```bash
kubectl get secrets -l mysql.cloudnative-mysql.io/cluster=cluster-sample
```

## Documentation

- [Cluster lifecycle](https://cloudnative-mysql.io/cluster-lifecycle)
- [Replication and failover](https://cloudnative-mysql.io/replication-failover)
- [Physical backup and recovery](https://cloudnative-mysql.io/backup-recovery)
- [Point-in-time recovery](https://cloudnative-mysql.io/pitr)
- [Scheduled backups](https://cloudnative-mysql.io/scheduled-backups)
- [Object store configuration](https://cloudnative-mysql.io/object-store)
- [Multi-tenancy](https://cloudnative-mysql.io/multi-tenancy)
- [Security model](https://cloudnative-mysql.io/security-model)
- [Operations runbooks](https://cloudnative-mysql.io/operations)
- [Troubleshooting](https://cloudnative-mysql.io/troubleshooting)

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
