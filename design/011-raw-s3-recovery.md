# 011 — Raw S3 Recovery

**Status:** done
**Milestone:** M9

Bootstrap a new cluster directly from an S3 bucket without requiring a `Backup` CR, using `BootstrapRecovery.Source` and `ExternalClusters` for disaster recovery.

**Goal:** Bootstrap a new cluster directly from an S3-compatible object-store
bucket + credentials without requiring a `Backup` object to exist in the API
server. Use the existing `BootstrapRecovery.Source` field (dead code today) and
the `ExternalClusters` pattern to point at a bucket, optionally narrow to a
specific `backupID`, and restore the base backup + binlog replay just like the
`Backup`-referenced path does.

## Why

A `Backup` CR is metadata in the *source* cluster's API server. That server may
have been destroyed (disaster recovery), the Backup CR may have been GC'd by
retention, or the user may want to recover across clusters/namespaces without
copying CRDs. CNPG's same feature is called "replica cluster from an external
archive" — we call it raw-S3 recovery.

**Recovery is entirely S3-driven.** No source `Cluster` CR and no `Backup` CR
need to exist in any API server. The `ExternalCluster` entry is a local
pointer — it carries its own `objectStore` (bucket, path, credentials) and its
`name` serves as the S3 key prefix for discovering backups and binlog archives.
The user points at a bucket, names the cluster whose backups live there, and
optionally picks a backup ID. Everything else is resolved from the objects
already in S3.

## Scope

### In scope

- `BootstrapRecovery.Source` field: accept a reference to an `ExternalClusters`
  entry. The entry's `objectStore` carries the S3 config and its `name` is the
  S3 key prefix (the cluster name backups were stored under). No Cluster or
  Backup CR from the source needs to exist.
- New `BootstrapRecovery.BackupID` field (optional): narrow to a specific base
  backup within the destination. When empty, discover and select the latest.
- Operator S3 discovery: list metadata.json objects under the cluster prefix,
  pick latest (or the named ID), derive archive & metadata keys.
- Full PITR support: `recoveryTarget` works identically to the Backup-based
  path — `sourceCluster` is derived from the external cluster name, binlog
  archive is resolved from the same object store.
- Validation: `Source` and `Backup` are mutually exclusive. When `Source` is
  set the referenced external cluster entry must exist and carry `objectStore`.
- Unit and e2e coverage.

### Out of scope

- Cross-cluster live replication from the archive (Replica.Source is already
  blocked in unsupportedReason, stays blocked).
- S3 credentials from anything other than `ExternalCluster.ObjectStore` secrets
  (no ambient IAM / IRSA for raw-S3 specifically — config is explicit).
- Backup retention / ScheduledBackup interaction (M8 territory, unchanged).

## API design

### BootstrapRecovery (existing, amended)

```go
type BootstrapRecovery struct {
    // Backup references a Backup object to recover from. Mutually exclusive
    // with Source.
    // +optional
    Backup *LocalObjectReference `json:"backup,omitempty"`

    // Source is the name of an entry in ExternalClusters whose objectStore
    // holds the backups to recover from. Mutually exclusive with Backup.
    // +optional
    Source string `json:"source,omitempty"`

    // BackupID narrows recovery to a specific base backup within the object
    // store. When Source is set and BackupID is empty, the latest completed
    // backup in the destination is selected.
    // +optional
    BackupID string `json:"backupID,omitempty"`

    // RecoveryTarget describes the point-in-time recovery target. When omitted,
    // recovery proceeds to the latest available point.
    // +optional
    RecoveryTarget *RecoveryTarget `json:"recoveryTarget,omitempty"`
}
```

- `Backup` and `Source` are mutually exclusive (exactly one must be set).
- `backupID` is only meaningful when `Source` is set; ignored with `Backup`
  (the Backup CR already identifies itself).
- `Source` must name an entry in `spec.externalClusters` that has a non-nil
  `objectStore`.

### ExternalCluster (existing, used as-is)

```go
type ExternalCluster struct {
    Name                string              `json:"name"`
    ConnectionParameters map[string]string   `json:"connectionParameters,omitempty"`
    Password            *SecretKeySelector  `json:"password,omitempty"`
    SSLCert             *SecretKeySelector  `json:"sslCert,omitempty"`
    SSLKey              *SecretKeySelector  `json:"sslKey,omitempty"`
    SSLRootCert         *SecretKeySelector  `json:"sslRootCert,omitempty"`
    ObjectStore         *S3ObjectStore      `json:"objectStore,omitempty"`
}
```

`ObjectStore` is the only field we consume for M9; connection parameters are
ignored (raw-S3 recovery does not connect to a live instance).

### Example: full raw-S3 recovery spec

```yaml
apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: recovered-cluster
spec:
  instances: 3
  imageName: cnmsql-instance:8.4
  storage:
    size: 10Gi
  bootstrap:
    recovery:
      source: prod-cluster            # ExternalCluster entry name
      backupID: ""                    # empty = latest; set to a known ID to pin
      recoveryTarget:                 # optional PITR
        targetGTID: "uuid:1-99"
  externalClusters:
    - name: prod-cluster              # S3 key prefix: path/prod-cluster/...
      objectStore:
        bucket: cnmsql-backups
        path: production
        endpoint: https://s3.example.com
        region: us-east-1
        credentials:
          accessKeyId:
            name: s3-creds
            key: accessKey
          secretAccessKey:
            name: s3-creds
            key: secretKey
```

No source `Cluster` CR or `Backup` CR named `prod-cluster` needs to exist
anywhere. The `externalClusters` entry is self-contained: it carries the
bucket/path/credentials and its `name` tells the operator what S3 key prefix to
scan for `metadata.json` and `binlogs/_index.json`.

## Validation (cluster_funcs.go)

`validateRecovery` currently requires `rec.Backup.Name != ""`. Change to:

```
if rec.Source == "" && (rec.Backup == nil || rec.Backup.Name == "") →
    "recovery requires a backup reference or source"
if rec.Source != "" && rec.Backup != nil && rec.Backup.Name != "" →
    "source and backup are mutually exclusive"
if rec.Source != "" && rec.BackupID != "" →
    validate backupID is a printable non-empty string
if rec.Source != "" && !hasExternalCluster(rec.Source) →
    "source must reference an entry in externalClusters"
if rec.Source != "" && externalCluster.ObjectStore == nil →
    "external cluster must have objectStore configured"
```

Recovery target validation remains identical (needs `backup.objectStore` on the
recovering cluster for binlog replay, mutual exclusion, RFC3339/GTID syntax).

## Controller changes

### unsupportedReason (cluster_plan.go:164)

Current gate:
```
case cluster.Spec.Bootstrap.Recovery != nil && cluster.Spec.Bootstrap.Recovery.Backup == nil:
    return "spec.bootstrap.recovery requires a backup reference"
```

Change to:
```
case cluster.Spec.Bootstrap.Recovery != nil &&
    cluster.Spec.Bootstrap.Recovery.Backup == nil &&
    cluster.Spec.Bootstrap.Recovery.Source == "":
    return "spec.bootstrap.recovery requires a backup reference or source"
```

(Validation already enforces mutual exclusion, so a cluster with both set won't
reach the reconciler.)

### resolveRecovery (cluster_plan.go:248)

Add a new path before the existing Backup lookup:

```
if rec.Source != "" {
    return r.resolveRawS3Recovery(ctx, cluster, rec)
}
// existing Backup-based path unchanged
```

`resolveRawS3Recovery`:

1. Look up the `ExternalCluster` entry named `rec.Source` in `cluster.Spec.ExternalClusters`.
2. Resolve its `ObjectStore` (defaults applied). This is the recovery store.
3. Build an operator-side object-store client via `objectStoreConfig` + `NewClient`.
4. Call `ListBaseBackups(ctx, client, *store, sourceCluster)` where `sourceCluster`
   is the external cluster name (or a new `clusterName` field — see decision).
5. Select the backup entry:
   - If `rec.BackupID` is set, find the entry whose `Meta.BackupID` matches.
     Error if not found.
   - If empty, pick the entry with the latest `Meta.CompletedAt`. Error if no
     backups exist.
6. Derive archive and metadata keys from `entry.Prefix` + `BackupArchiveName` /
   `BackupMetadataName` (the prefix already ends with a slash).
7. Build `storeEnv` from `backupObjectStoreEnv(*store)` + explicit
   `CNMYSQL_S3_BUCKET` / `CNMYSQL_S3_PATH` (same as the existing Backup path).
8. Return `recoveryPlan` with `SourceCluster = sourceCluster`, `HasTarget`,
   `TargetTime`, etc. — identical fields to the Backup path.

**Decision:** The `ExternalCluster.Name` is the source cluster name — the S3 key
prefix under which base backups (`<path>/<name>/...`) and binlogs
(`<path>/<name>/binlogs/...`) are stored. The `ExternalCluster` entry is a local
pointer carrying its own `objectStore`; no source `Cluster` CR with that name
needs to exist. `ListBaseBackups` walks `ClusterPrefix(store, extCluster.Name)`.
The same name seeds `SourceCluster` for binlog replay. Users who need a
different prefix than what the entry name provides can name the
`ExternalCluster` entry accordingly — it is purely an S3 key prefix.

### checkRecoveryTarget (cluster_backup_guard.go:109)

No structural change needed: `plan.Recovery.SourceCluster` is already set by
`resolveRecovery` (both Backup and raw-S3 paths), and `plan.Recovery.Store` is
already the resolved store. The method reads the archive index from
`ArchiveIndexKey(store, sourceCluster)` — works for both paths.

### checkBackupDestination (cluster_backup_guard.go:49)

Already skips recovery clusters (line 57–59). No change needed — raw-S3
recovery already has `cluster.Spec.Bootstrap.Recovery != nil`.

### buildPlan wiring

`buildPlan` calls `resolveRecovery`; the result populates `clusterPlan.Recovery`.
No structural change to `buildPlan`.

## Restore worker (instance restore)

No change. The worker already accepts:
- `--source-cluster` for binlog archive key reconstruction.
- `CNMYSQL_S3_*` env vars for bucket/path/credentials.

The archive and metadata keys flow through the init-container spec unchanged.

## Object-store discovery helpers

Add to `pkg/management/mysql/objectstore/`:

- `SelectLatestBackup(entries []BackupEntry) (BackupEntry, error)` — picks the
  entry with the latest `CompletedAt`. Error if the slice is empty.
- `FindBackupByID(entries []BackupEntry, id string) (BackupEntry, error)` —
  linear scan matching `Meta.BackupID`. Error if not found.

These are pure functions (no I/O), unit-testable in isolation.

## Implementation order

1. Add `backupID` field to `BootstrapRecovery` in `api/v1alpha1/cluster_types.go`.
2. Update `validateRecovery` for Source/Backup mutual exclusivity, external
   cluster existence, and objectStore presence.
3. Update `unsupportedReason` to accept Source-based recovery.
4. Add `SelectLatestBackup` / `FindBackupByID` in `objectstore/`.
5. Implement `resolveRawS3Recovery` in `cluster_plan.go`.
6. Wire `resolveRecovery` to call the new path when `Source != ""`.
7. Unit tests: validation, selection helpers, resolveRawS3Recovery (with
   httptest-backed S3), mutual exclusion.
8. `make generate manifests` (new api field).
9. `make lint-fix && make test`.
10. Kind+MinIO e2e: seed a backup in MinIO, delete the source cluster → recover
    into a new cluster via raw-S3 source.

## Testing

### Unit
- `validateRecovery`: Source set + no Backup = ok; both set = rejected; Source
  not in externalClusters = rejected; Source set but externalCluster has no
  objectStore = rejected; backupID with non-Source path = no-op/ignored.
- `SelectLatestBackup`: picks latest by CompletedAt, errors on empty.
- `FindBackupByID`: finds matching ID, errors on not-found.
- `resolveRawS3Recovery`: wired correctly from Source to recoveryPlan; latest
  backup selection; backupID selection; S3 listing error surfaces; empty
  destination surfacing.

### E2E
- Create source cluster, take a backup → MinIO has the data.
- Delete source cluster (simulating disaster).
- Create a new cluster with `bootstrap.recovery.source` pointing at an
  `ExternalCluster` with the same MinIO bucket/path.
- Assert cluster becomes Ready with recovered data.
- Stretch: add PITR writes, archive them, recover to targetGTID.

## Documentation

Update the Docusaurus docs at `docs/src/` for the new recovery path:

### `backup-recovery.md`
- Add a new section **"Restore from raw object store (no Backup CR)"** after the
  existing "Restore from a backup" section, showing the `bootstrap.recovery.source`
  + `externalClusters` YAML pattern with explanatory text.
- Update the "Failure surfaces" list to include raw S3-specific errors
  (missing external cluster entry, no objectStore on entry, no backups found,
  backupID not found).

### `cluster-lifecycle.md`
- Update the "Bootstrap modes" text (currently line 102-104) from:
  > `bootstrap.recovery.backup` restores a completed physical `Backup`
  to:
  > `bootstrap.recovery.backup` restores from a `Backup` object;
  > `bootstrap.recovery.source` restores directly from an object-store bucket
  > by referencing an `externalClusters` entry. A raw-S3 recovery discovers
  > the latest or named base backup in the destination.

### `backup-retention-deletion.md`
- Update the operational note (line 130) from "no Cluster uses
  `bootstrap.recovery.backup`" to "no Cluster uses
  `bootstrap.recovery.backup` or `bootstrap.recovery.source`".

### `object-store.md`
- Optionally add a short note that `ExternalCluster.objectStore` enables
  raw-S3 recovery, referencing the backup-recovery page.

### `pitr.md`
- Line 48-50 notes PITR recovers from a `Backup` object; add a sentence that
  raw-S3 recovery also supports PITR targets, referencing the backup-recovery
  page.

### Build validation
- `cd docs && npm run build` must pass with zero warnings/errors.

## Acceptance criteria

- A Cluster can bootstrap from `spec.bootstrap.recovery.source` (no `backup`
  field) by discovering the latest base backup in the object store.
- Optional `backupID` narrows to a specific backup; missing ID errors loudly.
- `spec.bootstrap.recovery.source` must name an ExternalCluster entry with
  `objectStore`; `source` and `backup` are mutually exclusive.
- PITR (recoveryTarget) works transparently with the Source path.
- `make generate manifests`, `make lint-fix`, `make test`, and the M9 MinIO e2e
  pass.
- Docusaurus docs updated and `npm run build` passes.
