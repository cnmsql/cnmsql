---
title: "MariaDB Flavor"
description: "Run a Cluster on MariaDB instead of MySQL: how to select the flavor, supported versions, constraints, and the behavior that differs from MySQL."
sidebar_position: 5
---

# MariaDB flavor

cnmsql can run a `Cluster` on MariaDB instead of Percona Server for MySQL. You
pick the engine once, at creation time, with `spec.flavor`. The rest of the API
is the same: the same `Cluster`, `Backup`, `ScheduledBackup`, `Database`, and
`ImageCatalog` resources, the same role-routed Services, and the same restore
flow. Most of what you already know about running a MySQL cluster applies
unchanged.

This page covers what changes when you set the flavor to MariaDB: which images
and versions to use, the constraints the webhook enforces, and the few
behaviors that differ from MySQL because MariaDB's GTID and tooling work
differently.

## Selecting the flavor

Set `spec.flavor` to `mariadb`:

```yaml
apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: mariadb-sample
spec:
  flavor: mariadb
  instances: 3
  imageName: ghcr.io/cnmsql/cnmsql-mariadb-instance:11.4
  storage:
    size: 10Gi
```

The field accepts `mysql` or `mariadb` and defaults to `mysql`, so an existing
manifest without a `flavor` keeps running MySQL. The resolved value is echoed
back in `status.flavor` and shown in the `Flavor` printer column:

```console
$ kubectl get cluster
NAME             AGE   INSTANCES   FLAVOR
mariadb-sample   2m    3           mariadb
```

`spec.flavor` is immutable. The webhook rejects any update that changes it, so
you cannot convert a MySQL cluster to MariaDB in place or the reverse. To switch
engines, create a new cluster.

## Images and supported versions

MariaDB clusters use a separate instance image published alongside the MySQL
one:

```text
ghcr.io/cnmsql/cnmsql-mariadb-instance:<version>
```

The supported series, in upgrade order, are:

| Series | Notes |
|--------|-------|
| 10.11  | LTS. |
| 11.4   | LTS. Default image when none is specified. |
| 12.3   | Newest supported series. |

Select an image the same way you do for MySQL, either directly in
`spec.imageName` or through an `ImageCatalog`. An `ImageCatalog` entry can carry
an advisory `flavor` on each series so the catalog documents which engine a
series belongs to. See [Instance Images and Versions](instance-images.md) for
the selection mechanics, which are identical across flavors.

If you set neither `imageName` nor `imageCatalogRef`, a MariaDB cluster falls
back to `ghcr.io/cnmsql/cnmsql-mariadb-instance:11.4`. Use an explicit image or
catalog in production.

## Constraints the webhook enforces

The admission webhook checks three flavor-related rules and rejects the resource
with a clear message when one is violated.

The flavor and the version series must agree. MariaDB series are major version
10 and above; MySQL series are major 8 and 9. A MariaDB flavor pointed at an 8.x
or 9.x series is rejected, and a MySQL flavor pointed at a 10.x or later series
is rejected.

Group Replication is not available on MariaDB. Setting
`spec.replication.mode: groupReplication` on a MariaDB cluster is rejected.
MariaDB clusters use asynchronous replication, optionally with
semi-synchronous acknowledgement, and rely on the operator's own failover rather
than a group's quorum. See [Replication and Failover](replication-failover.md)
for that model.

The flavor cannot change after creation, as described above.

## What behaves the same

The following work the same way on MariaDB as on MySQL, and their existing
documentation applies without change:

- Physical backups and restore. MariaDB uses `mariadb-backup` and `mbstream`
  under the hood instead of Percona XtraBackup, but the archive object is still
  named `backup.xbstream`, and the `Backup` and `ScheduledBackup` resources and
  retention behave identically.
- Point-in-time recovery to a timestamp (`targetTime`) or to the latest archived
  transaction, plus recovery from a raw object store.
- Declarative `Database` and `DatabaseUser` resources, managed roles, and grant
  scoping. One revoke limitation is specific to MariaDB; see below.
- TLS between clients and the server, and mTLS between the operator and
  instances.
- Automatic failover, replica rejoin, and broken-replica re-initialization.
- Storage resize and autoscaling.
- Rolling instance upgrades with primary switchover.

## Database and user management

The `Database` and `DatabaseUser` resources work on MariaDB. You declare
schemas, users, and grants the same way as on MySQL, and the operator
reconciles them with flavor-aware SQL.

Grants apply normally. Revokes have one limitation that comes from MariaDB
itself, and it still holds as of MariaDB 12.3:

- MariaDB has no `REVOKE IF EXISTS` (that clause is MySQL 8.0.16 and later). The
  operator emits a plain `REVOKE` and tolerates a "non-existing grant" error so
  reconciliation stays idempotent, so this one is handled for you.
- MariaDB has no `partial_revokes`. A revoke that narrows a broader global grant
  down to a single schema cannot be expressed on MariaDB, so a
  `DatabaseUser` revoke that depends on that pattern will not take effect. Grant
  only what a user should have rather than granting broadly and revoking the
  difference.

## GTID model

MariaDB names transactions differently from MySQL, and you will see that in a
few places: recovery targets, replication status, and logs.

A MariaDB GTID is a `domain-server-seq` triple, for example `0-1-16`. A GTID
position is a comma-separated list with at most one entry per replication
domain, holding the highest sequence number reached in that domain, for example
`0-1-100,1-5-42`. This is different from MySQL's `uuid:interval` sets.

Within a domain, MariaDB orders transactions by sequence number. The server-id
component records which server wrote the most recent event; it does not identify
a separate timeline. When a failover promotes a replica, the new primary keeps
writing in the same domain with a higher sequence number, so a position like
`0-1-14` advancing to `0-2-57` is one linear progression in domain 0, not a
fork. The operator relies on this when it plans recovery across a failover.

## Point-in-time recovery to a GTID

MariaDB PITR uses the same `Backup` plus binlog archive design and the same
`recoveryTarget` API as MySQL. When you recover to a specific transaction, give
the target in MariaDB GTID form:

```yaml
spec:
  bootstrap:
    recovery:
      source: source-backup
      recoveryTarget:
        targetGTID: "0-1-16"
```

The webhook validates the syntax and, on a MariaDB cluster, expects the
`domain-server-seq` form rather than the MySQL `uuid:interval` form.

One limitation is specific to MariaDB. `mariadb-binlog` has no GTID filtering
(the `--include-gtids` and `--exclude-gtids` options are MySQL only), so the
operator bounds replay by byte position within a single replication domain. A
`targetGTID` that names a single domain, which is the normal case, is fully
supported: the operator restores the base backup, then replays archived binlogs
up to and including the target transaction and nothing past it, even when the
target lands after a failover. A multi-domain target is not supported for
positional replay. If you run multiple write domains and need to recover to a
GTID, recover to a `targetTime` instead, or recover to the latest archived
transaction.

For the full recovery architecture, RPO/RTO model, and target options, see
[Point-In-Time Recovery](pitr.md).

## Major version upgrades

MariaDB has its own single-hop upgrade chain:

```text
10.11 → 11.4 → 12.3
```

In-place major upgrades follow this chain one hop at a time, the same way MySQL
upgrades follow `8.0 → 8.4 → 9.0`. You cannot skip a series, and you cannot
cross flavors. The mechanics and rollout behavior are covered in
[MySQL Version Upgrades](major-version-upgrade.md), which applies to both engines
with each following its own chain.

## Testing and coverage

MariaDB has a dedicated end-to-end test suite that runs in its own CI lane,
separate from the MySQL lanes. It exercises the following feature areas against
real MariaDB instances:

- Cluster bootstrap and lifecycle.
- Asynchronous replication, automatic failover, and replica rejoin.
- Self-healing and broken-replica re-initialization.
- TLS certificate renewal.
- `Database` and `DatabaseUser` reconciliation and managed roles.
- Physical backup, the backup cleanup finalizer, and retention garbage
  collection.
- Scheduled backups.
- Point-in-time recovery, including recovery to a GTID across a failover under
  heavy write load.
- Storage volume resize and behavior under storage pressure.
- Horizontal and vertical autoscaling.
- In-place instance-manager upgrades.
- Major-version upgrade rollout across the series chain.
- Admission webhook guards for flavor and version constraints.

The suite mirrors the MySQL feature coverage rather than re-running every MySQL
spec, and it does not cover Group Replication, which MariaDB does not support.
