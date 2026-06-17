# cloudnative-mysql

A Kubernetes operator for [Percona Server for MySQL](https://www.percona.com/software/mysql-database/percona-server). It runs MySQL clusters with operator-owned lifecycle management, GTID replication with automatic failover, physical backups to S3-compatible storage, and point-in-time recovery.

Full documentation lives at **https://yyewolf.github.io/cloudnative-mysql/**.

## What it does

You declare a `Cluster` and the operator creates the Pods, PVCs, credentials, TLS material, and Services that back it. From there it handles the parts of running MySQL that are tedious to do by hand:

- **Replication and failover.** One primary plus GTID-based replicas. Planned switchover for upgrades, automatic failover when the primary goes away, and rejoin of a former primary as a replica.
- **Role-routed Services.** Each cluster gets a read-write endpoint for the primary (`-rw`), a read-only endpoint for replicas (`-ro`), and a read endpoint for any ready instance (`-r`). Routing follows the `mysql.cloudnative-mysql.io/role` label, so it tracks failover automatically.
- **Backups.** One-shot physical backups via XtraBackup, written to S3-compatible object storage. `Backup` and `ScheduledBackup` resources cover ad-hoc and cron-driven archives.
- **Point-in-time recovery.** Continuous binlog archiving lets you restore to a chosen timestamp rather than just the last full backup.
- **Declarative databases and users.** `Database` resources manage schemas, owners, and privileges without an out-of-band SQL step.
- **Image catalogs.** `ImageCatalog` and `ClusterImageCatalog` resolve instance images from the MySQL major version, so you can pin or roll versions centrally.
- **Monitoring and TLS.** Prometheus metrics with mTLS between the operator and instances, plus MySQL TLS.

The API is `mysql.cloudnative-mysql.io/v1alpha1` and covers `Cluster`, `Database`, `Backup`, `ScheduledBackup`, `ImageCatalog`, and `ClusterImageCatalog`. See the [API reference](https://yyewolf.github.io/cloudnative-mysql/api-reference) for every field.

## CLI plugin

The repository ships a `kubectl` plugin, `kubectl cnmysql`, for day-to-day operations: cluster status, fencing, promotion, restart, reload, backups, and more. Install it with `make install-plugin`, then run `kubectl cnmysql status <cluster>`.

## Quickstart

The steps below bring up the operator and a three-instance cluster in a local Kind environment. The [full quickstart](https://yyewolf.github.io/cloudnative-mysql/quickstart) has the complete walkthrough.

You will need `go`, `docker`, `kubectl`, `kind`, `make`, and `cert-manager` installed in the target cluster. cert-manager issues the certificates used for instance mTLS and MySQL TLS.

Build the operator image and pull the published instance image (built from the
separate [`containers`](https://github.com/CloudNative-MySQL/containers) repo),
then load both into Kind:

```bash
make docker-build IMG=cloudnative-mysql-controller:dev
docker pull ghcr.io/cloudnative-mysql/cloudnative-mysql-instance:8.4
kind load docker-image cloudnative-mysql-controller:dev --name cloudnative-mysql-test-e2e
kind load docker-image ghcr.io/cloudnative-mysql/cloudnative-mysql-instance:8.4 --name cloudnative-mysql-test-e2e
```

Install the CRDs and deploy the controller:

```bash
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

Wait for it and check the topology:

```bash
kubectl wait --for=condition=Ready cluster/cluster-sample --timeout=15m
kubectl cnmysql status cluster-sample
```

Connect through the role-routed Services (`cluster-sample-rw`, `cluster-sample-ro`, `cluster-sample-r`). Application credentials are stored in a generated Secret:

```bash
kubectl get secrets -l mysql.cloudnative-mysql.io/cluster=cluster-sample
```

## Documentation

The docs site covers the topics that don't fit in a README:

- [Cluster lifecycle](https://yyewolf.github.io/cloudnative-mysql/cluster-lifecycle)
- [Replication and failover](https://yyewolf.github.io/cloudnative-mysql/replication-failover)
- [Physical backup and recovery](https://yyewolf.github.io/cloudnative-mysql/backup-recovery)
- [Point-in-time recovery](https://yyewolf.github.io/cloudnative-mysql/pitr)
- [Scheduled backups](https://yyewolf.github.io/cloudnative-mysql/scheduled-backups)
- [Object store configuration](https://yyewolf.github.io/cloudnative-mysql/object-store)
- [Multi-tenancy](https://yyewolf.github.io/cloudnative-mysql/multi-tenancy)
- [Security model](https://yyewolf.github.io/cloudnative-mysql/security-model)
- [Operations runbooks](https://yyewolf.github.io/cloudnative-mysql/operations)
- [Troubleshooting](https://yyewolf.github.io/cloudnative-mysql/troubleshooting)

## Development

This project is built with [Kubebuilder](https://book.kubebuilder.io). The common targets:

```bash
make manifests generate   # Regenerate CRDs, RBAC, and DeepCopy after editing types
make lint-fix             # Auto-fix style
make test                 # Run unit tests (Ginkgo + Gomega on envtest)
make run                  # Run the controller locally against the current kubeconfig
```

Run `make help` for the full list. `AGENTS.md` documents the layout and the project conventions in more detail.

## License

GNU General Public License v3.0. See [LICENSE](LICENSE).
