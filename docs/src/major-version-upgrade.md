# MySQL Version Upgrades

This page covers upgrading the **MySQL server** version of a running cluster,
distinct from upgrading the operator itself (see
[Operator Upgrades](operator-upgrades.md)).

## Supported transitions

MySQL only supports upgrades between adjacent release series, and never a
downgrade in place. cnmsql enforces the same chain:

```
8.0  →  8.4  →  9.0
```

- You must move **one series at a time**. `8.0 → 9.0` directly is rejected; go
  `8.0 → 8.4`, then `8.4 → 9.0`.
- **Patch upgrades within a series** (e.g. `8.0.36 → 8.0.40`) are unrestricted.
- **Downgrades are not supported.** Once a server starts on the new series it
  upgrades its data dictionary, which is irreversible. The only way back is to
  restore a backup taken before the upgrade (see [Rollback](#rollback)).

The supported chain lives in `UpgradeSeriesChain`
(`pkg/management/mysql/version/version.go`) and is enforced in two places:

1. **Admission**: `Cluster.ValidateUpdate` rejects a downgrade, a skipped
   series, or a series change expressed through `imageName` instead of a catalog.
2. **The instance manager**: before starting mysqld, it compares the series
   recorded in the data directory against the image version and refuses to start
   on an unsupported transition, even if admission was bypassed.

## How to upgrade

Major upgrades must be driven through an `ImageCatalog` (or
`ClusterImageCatalog`), so the target series is explicit. The catalog is keyed by
**series** (`8.0`, `8.4`, `9.0`), not by integer major. 8.0 and 8.4 are distinct
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

## Troubleshooting

- **The update is rejected on apply.** Admission refused the transition. Check the
  message: a skipped series (`upgrade to 8.4 first`), a downgrade, or a series
  change via `imageName` (use `imageCatalogRef` instead).
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
