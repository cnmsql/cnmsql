# MySQL Version Upgrades

This page covers upgrading the **MySQL server** version of a running cluster,
distinct from upgrading the operator itself (see
[Operator Upgrades](operator-upgrades.md)).

## Supported transitions

MySQL only supports upgrades between adjacent release series, and never a
downgrade in place. cnmsql enforces the same chain:

```
8.0  →  8.4  →  9.7
```

- You must move **one series at a time**. `8.0 → 9.7` directly is rejected; go
  `8.0 → 8.4`, then `8.4 → 9.7`.
- **Patch upgrades within a series** (e.g. `8.0.36 → 8.0.40`) are unrestricted.
- **Downgrades are not supported.** Once a server starts on the new series it
  upgrades its data dictionary, which is irreversible. The only way back is to
  restore a backup taken before the upgrade (see [Rollback](#rollback)).

The supported chain lives in `UpgradeSeriesChain`
(`pkg/management/mysql/version/version.go`) and is enforced in three places:

1. **Admission**: `Cluster.ValidateUpdate` rejects a downgrade, a skipped
   series, or a series change expressed through `imageName` instead of a catalog.
2. **The operator, before rolling**: it runs the new image in a probe Pod and
   checks the server version it reports against the current one and against
   the series the catalog entry names. This catches what admission cannot see:
   a digest-only image reference, or a catalog entry that points at another
   series' image. The cluster stays on its current image and the `ImageReady`
   condition explains why (see [How the operator learns the server
   version](instance-images.md#how-the-operator-learns-the-server-version)).
3. **The instance manager**: before starting mysqld, it compares the series
   recorded in the data directory against the image version and refuses to start
   on an unsupported transition, even if admission was bypassed.

## How to upgrade

Major upgrades must be driven through an `ImageCatalog` (or
`ClusterImageCatalog`), so the target series is explicit. The catalog is keyed by
**series** (`8.0`, `8.4`, `9.7`), not by integer major. 8.0 and 8.4 are distinct
upgrade targets.

1. Ensure the catalog lists the target series:

   ```yaml
   apiVersion: mysql.cnmsql.co/v1alpha1
   kind: ImageCatalog
   metadata:
     name: percona-images
   spec:
     images:
       - series: "8.0"
         image: ghcr.io/cnmsql/cnmsql-instance:8.0
       - series: "8.4"
         image: ghcr.io/cnmsql/cnmsql-instance:8.4
   ```

2. Point the cluster at the next series:

   ```yaml
   spec:
     imageCatalogRef:
       apiGroup: mysql.cnmsql.co
       kind: ImageCatalog
       name: percona-images
       series: "8.4"   # was "8.0"
   ```

3. Apply. The operator first takes a **pre-upgrade backup** (see below), then
   rolls instances **one at a time, replicas first and the primary last** (the
   primary via switchover where a healthy replica exists), so only one instance is
   down at a time and a newer replica never replicates from an older primary. Each
   instance must become Ready, which, with the default `--upgrade=AUTO`, means its
   data-dictionary upgrade has finished, before the next one rolls.

### Pre-upgrade backup gate

Because the data-dictionary upgrade is irreversible, the operator takes a fresh
backup before rolling any instance and waits for it to complete. This is
controlled by `spec.upgrade.backupBeforeUpgrade` (default `true`):

```yaml
spec:
  upgrade:
    backupBeforeUpgrade: true   # default; set false to skip
```

If it is enabled but no `spec.backup.objectStore` is configured, the upgrade is
**blocked** (status phase `Blocked`, event `BackupRequired`) rather than rolling
unprotected. Configure a backup destination or set `backupBeforeUpgrade: false`
(e.g. when an external backup process is in place).

### Group Replication

During a Group Replication upgrade, the group continues using its old
communication protocol while members roll. Once every member reports the target
series and is `ONLINE`, the operator automatically calls
`group_replication_set_communication_protocol` on the primary with the full
target version. The action is idempotent and the cluster briefly reports phase
`Upgrading` while the protocol is finalized. Cluster status records both the
effective `communicationProtocol` and the requested
`communicationProtocolTarget`. These can differ: MySQL 8.4 uses the effective
protocol `8.0.27` even when finalized with an 8.4 server target.

## Rollback

There is **no in-place downgrade**. To return to the previous series:

1. Provision a new cluster (or recover into one) on the **old** series.
2. Bootstrap it from the [backup](backup-recovery.md) taken before the upgrade
   using `bootstrap.recovery`.

A backup taken after the upgrade has already-upgraded data and cannot restore the
old series.

## Moving outside the chain

A [logical backup](logical-backups.md) moves data between any two supported
series of the same flavor, in either direction: take a dump of the source, then
create a new cluster on the target series with `bootstrap.initdb.import`. Use it
to skip series (`8.0` straight to `9.7`), to go back to an older series after
the upgraded cluster has taken writes, or to move only some databases. It is
slower than an in-place upgrade on large datasets, and the new cluster is a
separate cluster that clients must be moved to. See
[Moving to another server series](logical-backups.md#moving-to-another-server-series).

## Legacy 9.x innovation clusters

cnmsql supports LTS series only: 8.0 (while Percona still publishes it;
upstream MySQL 8.0 reached end of life in April 2026), 8.4 and 9.7. Operator
releases up to 0.8.x ran the 9.x innovation line under the catalog series `9.0`
and the `:9.x` image tags, which ended at 9.6. The operator now treats 9.6 as a
series of its own, outside the upgrade chain:

- A 9.6 cluster cannot upgrade to 9.7 in place. Admission rejects the series
  change with `unsupported source MySQL series`. A 9.7 image set through
  `imageName` is refused, and the cluster stays on 9.6 (the `ImageReady`
  condition says why).
- A physical backup of 9.6 cannot seed a 9.7 cluster: XtraBackup 9.7 only
  copies 9.7 servers.
- A 9.6 cluster whose image is set with `imageName` keeps running and is
  managed as usual. The `:9.x` tags stay published, frozen at their last 9.6
  build. A 9.6 cluster that still uses a catalog entry named `9.0` is
  `Blocked`; switch it to `imageName` to unblock it.

The simplest way to 9.7 is to move while the operator is still on 0.8.x; see
[Clusters on MySQL 9.x](operator-upgrades.md#clusters-on-mysql-9x). After the
operator upgrade, the only way is a [logical backup](logical-backups.md) of the
9.6 cluster loaded into a new 9.7 cluster; see
[Moving to another server series](logical-backups.md#moving-to-another-server-series).

Future innovation releases (9.8 onward, 10.x) are not supported until they
become an LTS series.

## Troubleshooting

- **The update is rejected on apply.** Admission refused the transition. Check the
  message: a skipped series (`upgrade to 8.4 first`), a downgrade, a legacy
  9.x innovation source (`unsupported source MySQL series`, see
  [Legacy 9.x innovation clusters](#legacy-9x-innovation-clusters)), or a series
  change via `imageName` (use `imageCatalogRef` instead).
- **The new image is not rolled out, or the cluster is `Blocked`.** The
  operator refused the image: it does not run the series the catalog entry or
  image tag names, or moving to it is not a supported upgrade. The `ImageReady`
  condition gives the version it found and the reason. A cluster that already
  runs an accepted image stays on it. A cluster with no accepted image yet (a
  new cluster, or any cluster right after the upgrade to operator 0.9.0) is
  `Blocked`, and the operator does not manage it until you fix the spec.
- **A Pod crash-loops right after the image change.** The instance manager refused
  an unsupported transition (the data directory's series does not match the
  image). The reason is in the Pod log: `Refusing to start mysqld: unsupported
  MySQL version transition`. Reconcile the catalog/series so the hop is a single
  forward step.
- **mysqld fails to start citing an "unknown variable".** A user-supplied
  `spec.mysql.parameters` value was removed in the target series. The operator
  drops known-removed variables automatically and emits a `RemovedParameter`
  warning event; for anything it does not yet know about, remove the offending
  variable from the spec. Common removals in 8.4 include
  `default_authentication_plugin`, `expire_logs_days`, and
  `master_info_repository`.
- **The upgrade is blocked on a backup.** The cluster status phase is `Blocked`
  with a `BackupRequired` event: `backupBeforeUpgrade` is enabled (the default)
  but no `spec.backup.objectStore` is configured. Configure a destination, or set
  `spec.upgrade.backupBeforeUpgrade: false`. While the pre-upgrade backup runs the
  phase is `Upgrading` with a "Waiting for pre-upgrade backup" reason.
- **The rollout stalls part-way.** The operator serializes the roll and waits for
  each instance to become Ready before the next. Inspect the cluster status phase
  and the per-instance logs to find the instance that is not becoming Ready.
- **All GR members upgraded but the protocol did not advance.** Confirm every
  member is `ONLINE` and reports the target server series, then inspect the
  primary instance-manager log for `Finalizing group communication protocol` or
  a failed `/group/set-communication-protocol` action. From MySQL, compare
  `group_replication_get_communication_protocol()` with
  `status.groupReplication.communicationProtocol`, and check that
  `communicationProtocolTarget` matches the upgraded server series. The operator
  retries on later reconciles once status is complete and healthy.
