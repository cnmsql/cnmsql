---
title: "CNMSQL - CloudNative for MySQL"
description: "A Kubernetes operator for Percona Server for MySQL: operator-owned lifecycle, GTID replication with automatic failover, physical backups, and point-in-time recovery."
sidebar_position: 1
---

import ThemedImage from '@theme/ThemedImage';

<p align="center">
  <img src="/img/cnmsql.png" alt="CNMSQL - CloudNative for MySQL" width="180" />
</p>

# CNMSQL - CloudNative for MySQL

**CNMSQL - CloudNative for MySQL** is a Kubernetes operator for [Percona Server for MySQL](https://www.percona.com/software/mysql-database/percona-server) that borrows design patterns from [CloudNativePG](https://cloudnative-pg.io/), adapted for MySQL. Declare a `Cluster` resource and the operator provisions Pods, PVCs, credentials, TLS, and role-routed Services, then handles replication, failover, backups, and point-in-time recovery so you don't have to.

:::note No affiliation

CNMSQL - CloudNative for MySQL is an independent project. It is **not** affiliated with, endorsed by, or associated with Oracle, MySQL, the [CNCF](https://www.cncf.io/), or the [CloudNativePG](https://cloudnative-pg.io/) project and its maintainers.

:::

---

## Architecture at a Glance

```mermaid
flowchart TD
    User["Kubernetes User"] -->|kubectl apply| CR["Cluster CR"]
    CR --> Op["Operator Controller"]
    Op --> Pods["Instance Pods\n(Primary + Replicas)"]
    Op --> Svc["Role-Routed Services\n(rw / ro / r)"]
    Op --> Backup["Backup / ScheduledBackup CRs"]
    Op --> ObjectStore["S3-Compatible\nObject Storage"]
    Pods -->|mTLS| Op
    Pods -->|GTID Replication| Pods
    Backup --> ObjectStore
    Pods -->|Metrics| Prometheus["Prometheus"]
```

Declare your desired state via Kubernetes custom resources. The operator continuously reconciles:
- **Cluster**: instances, storage, replication topology, TLS, Services
- **Backup**: one-shot physical snapshots via XtraBackup to S3
- **ScheduledBackup**: cron-driven backup schedules with retention
- **Database**: declarative schemas with managed roles and owners

---

## Key Features

| Category | Capabilities |
|----------|-------------|
| **Engines** | Percona Server for MySQL or MariaDB, selected per cluster with `spec.flavor` |
| **MySQL versions** | Percona Server 8.0, 8.4, and 9.x |
| **MariaDB versions** | MariaDB 10.11, 11.4, and 12.3 |
| **Replication** | GTID-based asynchronous and semi-synchronous replication, plus MySQL Group Replication with quorum-based consensus, planned switchover, and automatic failover |
| **Traffic routing** | Three role-aware Services: read-write, read-only (replicas), and read (any ready) |
| **Backups** | Physical backups via Percona XtraBackup to S3-compatible storage |
| **PITR** | Continuous binlog archiving for point-in-time recovery to any timestamp |
| **Security** | mTLS between operator and instances, MySQL TLS, per-instance ServiceAccount identity, admission webhook for status protection |
| **Multi-tenancy** | Cluster-per-tenant or schema-per-tenant via declarative `Database` and managed role resources |
| **Upgrades** | Rolling instance upgrades with primary switchover, plus in-place instance-manager binary swaps (no pod restart) |
| **Self-healing** | PDBs, semi-sync reconciliation, primary-lease fencing, broken-replica detection and re-initialization |
| **Observability** | Prometheus metrics, PodMonitor support, `kubectl cnmsql` CLI plugin for ad-hoc inspection |
| **Slim images** | Custom Debian-based instance images (~75% smaller than upstream Percona), rootless by default |

---

## API Resources

| Resource | Purpose |
|----------|---------|
| `Cluster` | Define a MySQL cluster: instances, storage, MySQL config, bootstrap, TLS |
| `Database` | Declarative schema management with owners and privilege scoping |
| `Backup` | One-shot physical backup via XtraBackup to S3-compatible storage |
| `ScheduledBackup` | Cron-scheduled backups with deterministic naming and retention |
| `ImageCatalog` | Cluster-wide image resolution by MySQL series |
| `ClusterImageCatalog` | Per-cluster image override catalog |

All resources live under the `mysql.cnmsql.co/v1alpha1` API group. See the [API Reference](./api-reference.md) for every field.

---

## Getting Started

1. **[Quickstart](./quickstart.md)**: install the operator via Helm, pull pre-built images, create your first cluster, connect, scale, and take a backup.
2. **[Cluster Lifecycle](./cluster-lifecycle.md)**: understand how a `Cluster` CR becomes running MySQL instances.
3. **[Instance Images](./instance-images.md)**: choose MySQL versions and understand the slim image layout.
4. **[MariaDB Flavor](./mariadb.md)**: run a cluster on MariaDB instead of MySQL, and the behavior that differs.

## Core Operations

5. **[Replication and Failover](./replication-failover.md)**: GTID replication model, planned switchover, automatic failover, and rejoin.
6. **[Group Replication](./group-replication.md)**: quorum-based consensus, automatic primary election, and event-driven observation.
7. **[Security Model](./security-model.md)**: mTLS, TLS, RBAC, per-instance identity, and the threat model.
8. **[Multi-Tenancy](./multi-tenancy.md)**: isolate tenants with Cluster-per-namespace or schema-per-tenant patterns.
9. **[Operator Upgrades](./operator-upgrades.md)**: rolling and in-place operator/instance-manager upgrades.

## Backup and Recovery

10. **[Physical Backup and Recovery](./backup-recovery.md)**: one-shot XtraBackup archives and restore.
11. **[Scheduled Backups](./scheduled-backups.md)**: cron-driven backup schedules.
12. **[Point-In-Time Recovery](./pitr.md)**: continuous binlog archiving and timestamped recovery.
13. **[Backup Retention and Deletion](./backup-retention-deletion.md)**: cleanup semantics and planned GC.
14. **[Object Store Configuration](./object-store.md)**: S3-compatible providers, credentials, and TLS.

## Day-2 Operations

15. **[Operations Runbooks](./operations.md)**: scaling, switchover, fencing, restart, reload, maintenance.
16. **[Monitoring](./monitoring.md)**: Prometheus metrics, PodMonitor, kubectl plugin inspection.
17. **[Troubleshooting](./troubleshooting.md)**: symptom-driven guide for common issues.

## Reference

18. **[API Reference](./api-reference.md)**: complete field reference for every CRD.
