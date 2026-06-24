# MySQL Version Upgrades

This page covers upgrading the **MySQL server** version of a running cluster —
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

1. **Admission** — `Cluster.ValidateUpdate` rejects a downgrade, a skipped
   series, or a series change expressed through `imageName` instead of a catalog.
2. **The instance manager** — before starting mysqld, it compares the series
   recorded in the data directory against the image version and refuses to start
   on an unsupported transition, even if admission was bypassed.

## How to upgrade

Major upgrades must be driven through an `ImageCatalog` (or
`ClusterImageCatalog`), so the target series is explicit. The catalog is keyed by
**series** (`8.0`, `8.4`, `9.0`), not by integer major — 8.0 and 8.4 are distinct
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

3. Apply. The operator rolls instances **one at a time, replicas first and the
   primary last** (the primary via switchover where a healthy replica exists), so
   only one instance is down at a time and a newer replica never replicates from
   an older primary.

> **Take a backup first.** Until the operator's automatic pre-upgrade backup gate
> ships, take a [physical backup](backup-recovery.md) yourself before starting a
> major upgrade. The data-dictionary upgrade is irreversible; the backup is your
> only rollback path.

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
- **The rollout stalls part-way.** The operator serializes the roll and waits for
  each instance to become Ready before the next. Inspect the cluster status phase
  and the per-instance logs to find the instance that is not becoming Ready.
