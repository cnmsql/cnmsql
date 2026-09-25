---
title: "Logical Backups"
description: "SQL dumps of application schemas in the object store, and importing them into a new cluster for partial restores, cross-version moves and schema exports."
sidebar_position: 12
---

# Logical backups

A logical backup is a SQL dump of your application schemas, stored in the same
object store as your physical backups. Use it when a physical backup can't help:

- **Partial restore:** bring back one database without touching the others.
- **Moving across server versions:** load an 8.0 dump into a fresh 9.x cluster,
  or go back to an older series.
- **Exporting a schema** for a developer, a test environment or another tool.

Physical backups stay the right tool for disaster recovery and point-in-time
recovery. A logical backup is slower to take and much slower to restore on large
datasets, it is not a base for binlog replay, and it does not contain users or
grants.

| | Physical (`xtrabackup`) | Logical (`logical`) |
|---|---|---|
| Format | xbstream of the data directory | SQL (`dump.sql.zst`) |
| Restores onto | the same server series | any supported series of the same flavor |
| Restore granularity | whole cluster | whole dump or selected databases |
| Point-in-time recovery | yes, with binlog archiving | no |
| Users and grants | included | not included, declare them as CRs |
| Speed on large datasets | fast | slow (single-threaded dump and load) |

## How it works

```mermaid
flowchart LR
    BackupCR["Backup CR\nmethod: logical"]
    Operator["Backup Reconciler"]
    Job["Backup Worker Job"]
    Source["Source Instance\ninstance-manager\nmysqldump / mariadb-dump"]
    Store["S3-compatible Object Store"]
    Import["Import Init Container\n(new cluster)"]

    BackupCR --> Operator
    Operator --> Job
    Job -->|"mTLS POST /cluster/dump"| Source
    Source -->|"SQL stream"| Job
    Job -->|"dump.sql.zst + logical.json"| Store
    Store --> Import
```

The source instance manager runs the engine's dump client (`mysqldump` on
Percona Server, `mariadb-dump` on MariaDB) over its local socket and streams the
output to the backup worker Job over mTLS. The worker compresses the stream with
zstd, checksums it and uploads it. Object-store credentials never enter the
instance Pod, which is the same split as for
physical backups.

The dump runs as `cnmsql_dump`, a read-only account the operator creates on
every cluster. It can only connect over the instance's local socket. Its
password is in the `<cluster>-dump` Secret, which the backup worker Job sends
along with the dump request. Clusters created before logical backup support get
the account on the first reconcile after the operator upgrade, with no Pod
restart. Logical Backups wait in `pending` (reason `DumpAccountNotReady`) until
the Cluster's `DumpAccountReady` condition is true:

```bash
kubectl get cluster shop -o jsonpath='{.status.conditions[?(@.type=="DumpAccountReady")]}'
```

To rotate the password, change `password` in the `<cluster>-dump` Secret. The
operator applies the new one to the account on its next reconcile.

Taking a logical backup needs an instance image that includes the dump tool.
Images published before logical backup support strip it. On such an image the
Backup fails with reason `LogicalToolUnavailable`; move the cluster to a newer
image tag of the same series (see [Instance Images and Versions](instance-images.md)).
The moving series tags (`8.4`, `11.4`, …) already point to images with the tool.
The first pinned tags that include it are:

| Image | First tag with the dump tool |
|---|---|
| `ghcr.io/cnmsql/cnmsql-instance` | `8.0-5`, `8.4-5`, `9.x-5` |
| `ghcr.io/cnmsql/cnmsql-mariadb-instance` | `10.11-4`, `11.4-4`, `11.8-4`, `12.3-4` |

Importing a dump works on any image.

Every dump is one consistent snapshot of all selected databases
(`--single-transaction`). Consistency covers InnoDB tables. Non-transactional
tables such as MyISAM can change while the dump runs.

## Taking a logical backup

```yaml
apiVersion: mysql.cnmsql.co/v1alpha1
kind: Backup
metadata:
  name: shop-dump
spec:
  cluster:
    name: shop
  method: logical
  target: prefer-standby
```

By default the dump contains every application schema. The system schemas
(`mysql`, `sys`, `performance_schema`, `information_schema`) and operator-owned
schemas such as `heartbeat` are always excluded. To dump only some databases:

```yaml
spec:
  method: logical
  logical:
    databases:
      - billing
      - catalog
```

With the plugin:

```bash
kubectl cnmsql backup shop --method logical --databases billing,catalog
```

`--databases` splits on commas: whitespace around each name is trimmed and
duplicates are dropped. Database names that contain a comma, or anything else
needing exact control, should go through a Backup manifest instead.

`target`, `objectStore`, `reclaimPolicy` and `jobTemplate` work the same as for
physical backups (see [Physical Backup and Recovery](backup-recovery.md)). The
dump always runs online: `online: false` is rejected, and so is a `logical`
block on a Backup whose method is not `logical`.

To pass extra flags to the dump tool, set `spec.backup.logicalOptions` on the
Cluster, or `logical.extraArgs` on one Backup (which replaces the cluster's
list). cnmsql does not check them: a flag that changes the output format or the
GTID handling can make the dump impossible to import.

```yaml
spec:
  method: logical
  logical:
    extraArgs:
      - --max-allowed-packet=1G
```

Taking the backup from a replica (`prefer-standby`, the default) is recommended.
The dump takes a brief global read lock at the start to record a consistent
binlog position, then reads for as long as the dump lasts.

### Scheduled logical backups

`ScheduledBackup` accepts the same `method` and `logical` fields:

```yaml
apiVersion: mysql.cnmsql.co/v1alpha1
kind: ScheduledBackup
metadata:
  name: shop-nightly-dump
spec:
  cluster:
    name: shop
  schedule: "0 0 3 * * *"
  method: logical
  successfulBackupsHistoryLimit: 7
```

A cluster often runs both: a physical schedule for disaster recovery and PITR,
and a logical one for exports and partial restores.

### Status

A completed logical Backup records:

- `status.method: logical`
- `status.destinationPath`: the `s3://` URI of `dump.sql.zst`
- `status.sha256`: checksum of the compressed object
- `status.databases`: the databases in the dump
- `status.beginBinlog` / `status.endBinlog`: the binlog position of the
  snapshot (`file:position`), for reference only
- `status.beginGTID` / `status.endGTID`: the GTID position of the snapshot, on
  MariaDB only. MySQL dumps are taken with `--set-gtid-purged=OFF` and do not
  report one.

`kubectl cnmsql status` lists logical backups with their method. They count as
the last successful backup, but never as a point of recoverability.

### When a logical backup fails

The Backup's `Degraded` condition carries the reason:

| Reason | Meaning |
|---|---|
| `DumpAccountNotReady` | Not a failure: the Backup waits in `pending` until the cluster's dump account exists. |
| `LogicalToolUnavailable` | The instance image has no dump tool. Move to a newer image tag (see above). |
| `InstanceManagerOutdated` | The source instance still runs an instance manager from before logical backups. Retry once the operator upgrade has reached it. |
| `DumpAccountMissing` | The source replica had not received the dump account yet after two minutes of retries. |
| `InvalidDumpRequest` | A database in `logical.databases` does not exist, or the cluster has no application database. |
| `DumpInProgress` | Another dump was still running on the source instance. |
| `DumpFailed` | The dump tool failed, or the stream ended without its completion footer. No manifest is written and the partial dump is removed. |
| `ManifestMissing` | The worker Job succeeded, but `logical.json` is missing from the object store or is not a valid manifest. Other errors reading it, such as the store being unreachable, are retried and leave the Backup running. |

## Object-store layout

Logical backups live next to physical ones under the cluster prefix:

```text
<path>/<cluster>/<backup-name>/<backup-id>/dump.sql.zst
<path>/<cluster>/<backup-name>/<backup-id>/logical.json
```

The manifest is `logical.json`, not `metadata.json`. Recovery from a raw object
store (`bootstrap.recovery.source`) and binlog retention only look at physical
backups, so a dump is never picked as a recovery base by mistake.

`dump.sql.zst` is plain zstd-compressed SQL. You can inspect it without cnmsql:

```bash
aws s3 cp s3://cnmsql-backups/production/shop/shop-dump/<id>/dump.sql.zst - | zstd -d | less
```

## Importing into a new cluster

:::caution Not available yet
Importing (`bootstrap.initdb.import`) is the next phase of
[#47](https://github.com/cnmsql/cnmsql/issues/47) and is not accepted by the
API yet. Until then, load a dump by hand: download `dump.sql.zst`, decompress
it and pipe it into the `mysql` / `mariadb` client of the target cluster.
:::

To load a dump into a fresh cluster, use `bootstrap.initdb.import`. The cluster
is initialised on its own server version, then the dump is loaded, so the target
can be a newer (or older) series than the source:

```yaml
apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: shop-84
spec:
  instances: 3
  imageName: ghcr.io/cnmsql/cnmsql-instance:8.4
  bootstrap:
    initdb:
      import:
        backup:
          name: shop-dump
        databases:
          - billing
  backup:
    objectStore:
      # ...
```

`databases` is optional. When set, only those databases are loaded from the
dump. Each one must be in the Backup's `status.databases`.

To import from an object store without a `Backup` object, for example in another
Kubernetes cluster, point at an `externalClusters` entry, as for raw-S3 recovery:

```yaml
spec:
  bootstrap:
    initdb:
      import:
        source: shop
        backupID: shop-dump-1760000000   # optional, latest dump when empty
  externalClusters:
    - name: shop
      objectStore:
        # ...
```

The import runs in an init container on the first instance, against a temporary
server with binary logging off. Replicas then clone the loaded primary as usual.
If the Pod restarts half-way, the import starts over and overwrites what was
already loaded.

`bootstrap.recovery.backup` does not accept a logical Backup. Recovery restores
a physical data directory; use `initdb.import` for dumps.

### Before you import

- **Declare users first.** The dump has no accounts. Create them with
  `spec.managed.roles`, `Database` or `DatabaseUser` objects. Views, routines
  and triggers keep their `DEFINER`, and they fail when called until that account
  exists.
- **Flavor must match.** A MySQL dump can't be imported into a MariaDB cluster,
  or the other way round.
- **Take a physical backup afterwards.** An imported cluster has no base backup,
  so point-in-time recovery starts only after its first physical backup.

## Retention and deletion

- `reclaimPolicy: Delete` on a logical Backup removes its directory from the
  object store when the Backup is deleted, like a physical backup.
- `ScheduledBackup` history limits apply per schedule.
- The cluster `spec.backup.retentionPolicy` also expires logical backups older
  than the window. It always keeps the newest one. Logical backups never affect
  which physical backups or binlogs are kept, and the newest physical backup is
  kept as the recovery floor even when newer dumps exist.

See [Backup Retention and Deletion](backup-retention-deletion.md).

## Limits

- Dump and load are single-threaded. For datasets in the hundreds of gigabytes,
  expect restores to take hours. Use physical backups for disaster recovery.
- Databases can't be renamed on import.
- Users, grants and system schemas are not included.
- Dumps are GTID-neutral: loading one does not change the target's
  `gtid_executed` or `gtid_purged`.
