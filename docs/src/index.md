---
title: "CNMySQL Documentation"
description: "Architecture and integration notes for operating CloudNative MySQL on Kubernetes."
sidebar_position: 1
---

# CNMySQL Documentation

CNMySQL is a Kubernetes operator for Percona Server for MySQL, designed around
operator-owned lifecycle management, physical backups, failover, and
point-in-time recovery.

## Guides

- [Quickstart](./quickstart.md): build local images, deploy the operator,
  create a Cluster, inspect services, scale, and take a first Backup.
- [Cluster Lifecycle](./cluster-lifecycle.md): how a `Cluster` becomes Pods,
  PVCs, credentials, TLS material, Services, and status.
- [Instance Images and Versions](./instance-images.md): supported Percona
  versions, slim image layout, rootless runtime, and image selection.
- [Security Model](./security-model.md): mTLS, MySQL TLS, RBAC, secrets,
  object-store credentials, and current security limits.
- [Operations Runbooks](./operations.md): scaling, switchover, failover,
  retained PVCs, status inspection, and maintenance habits.
- [Replication and Failover](./replication-failover.md): GTID replicas, role
  routing, planned switchover, automatic failover, and former-primary rejoin.
- [Physical Backup and Recovery](./backup-recovery.md): one-shot XtraBackup
  archives, object-store layout, Backup status, and restore flow.
- [Backup Retention and Deletion](./backup-retention-deletion.md): current
  deletion semantics, ScheduledBackup owner references, and planned retention GC.
- [Object Store Configuration](./object-store.md): S3-compatible fields,
  credentials, TLS, object layout, and provider notes.
- [API Reference](./api-reference.md): field guide for `Cluster`, `Backup`,
  `ScheduledBackup`, `ImageCatalog`, and `ClusterImageCatalog`.
- [Scheduled Backups](./scheduled-backups.md): six-field cron scheduling,
  deterministic Backup creation, owner modes, and retention notes.
- [Point-In-Time Recovery](./pitr.md): architecture, components, recovery flow,
  RPO/RTO model, and operational risks.
- [Troubleshooting](./troubleshooting.md): common symptoms, likely causes, and
  first commands to run.
