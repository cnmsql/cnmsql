# 008 — Physical Backup and Recovery

**Status:** done
**Milestone:** M6

Turn XtraBackup clone machinery into durable physical backups: `Backup` CRD, S3-compatible object-store streaming, `instance restore`, and `bootstrap.recovery.backup` for new clusters.

**Goal:** Turn the XtraBackup clone machinery from M4 into user-facing,
durable physical backups and cluster recovery. M6 should let a user create a
`Backup` object, store an xbstream backup in an S3-compatible object store, and
bootstrap a new `Cluster` from that completed backup.

M6 is deliberately about *base backups*. Scheduled backups, retention
automation, continuous binlog archiving, PITR, external replica clusters, and
volume snapshots remain later work unless explicitly pulled in.

## Scope

### In scope

- **One-shot `Backup` reconciliation.** A `Backup` object resolves its target
  `Cluster`, selects a source instance, creates a backup worker Job, tracks
  phase/status, and records the object-store path and backup metadata.
- **XtraBackup streaming to S3.** Reuse the instance-manager
  `GET /cluster/backup` mTLS stream. The backup worker pulls the xbstream from
  the selected instance and uploads it to the configured object store.
- **S3-compatible object store.** Use the existing `S3ObjectStore` API:
  endpoint, region, bucket, path, path-style addressing, signature version,
  TLS CA/insecure setting, SSE/storage class, and credentials from Secret or IAM
  role.
- **Backup target selection.** Honour `Backup.spec.target`, falling back to
  `Cluster.spec.backup.target`, defaulting to `prefer-standby`. Prefer a healthy
  replica when available; otherwise use the primary.
- **Backup status and conditions.** Record `Pending`, `Running`, `Completed`,
  `Failed`, selected instance, method, backup ID, destination path, timings,
  GTID/binlog metadata when available, and Events.
- **Cluster recovery from a completed backup.** Support
  `spec.bootstrap.recovery.backup.name` for a new cluster. The bootstrap path
  downloads the backup from object storage, extracts/prepares/copy-backs it,
  starts as the first primary, and then lets existing replica provisioning clone
  from that recovered primary.
- **Backup CLI/worker plumbing.** Add an instance-manager command for backup
  upload/download/restore work so the operator can run it in Jobs/init
  containers instead of embedding large data movement in the controller process.
- **Unit, integration, and e2e coverage.** Cover S3 key construction, target
  selection, Job rendering, status transitions, restore command behavior, and a
  MinIO-backed Kind e2e backup+restore.

### Out of scope

- `ScheduledBackup` controller and cron semantics (M7).
- Retention cleanup/garbage collection beyond recording object paths.
- PITR to a timestamp/GTID, continuous binlog archiving, and timeline history.
- CSI `volumeSnapshot` backups.
- Cross-cluster replica mode through `spec.replica`/`externalClusters`.
- Incremental backups.
- Transparent backup encryption beyond object-store TLS/SSE settings.
- Object-store credential rotation/retry policy beyond normal Job retries.

## Existing building blocks

- `Backup` and `ScheduledBackup` API types already exist.
- `Cluster.spec.backup.objectStore` and `ExternalCluster.objectStore` already
  model S3-compatible object stores.
- The instance manager can stream a local XtraBackup archive over mTLS via
  `GET /cluster/backup`.
- `instance.FetchBackup` can pull the mTLS backup stream and extract xbstream.
- `instance.Join` already prepares and copy-backs a backup into an empty data
  directory, then reads `xtrabackup_binlog_info`.
- The slim instance image already includes XtraBackup and xbstream across the
  supported Percona versions.
- M5.5 made instance roles dynamic, so a recovered primary can become the first
  `currentPrimary` through the normal in-Pod role reconciler.

## Decisions

1. **Object store:** M6 is S3-only, using the existing compatibility-focused
   `S3ObjectStore` type.
2. **Data path:** object-store credentials should live in the backup worker
   Job/init container, not in long-running MySQL instance Pods. The worker pulls
   the instance-manager backup stream over mTLS and uploads/downloads to S3.
3. **Artifact format:** store a single xbstream object for M6, plus a small
   metadata object next to it. Do not invent a multi-file catalog yet.
4. **Backup source:** default to `prefer-standby`; if no healthy standby exists,
   fall back to the primary. If the user requested `primary`, use only the
   primary.
5. **Recovery target:** M6 restores to the backup's consistent point. The
   `recoveryTarget` fields are accepted by the API but remain blocked until
   PITR/binlog archiving is implemented.
6. **Compression:** keep compression off by default until the image has a
   proven, version-compatible compression toolchain. The API can carry explicit
   XtraBackup options, but the controller should not silently require qpress.
7. **Worker image:** backup and restore worker Jobs use the same cnmsql
   instance image as the Cluster. The image already carries the manager binary,
   XtraBackup, xbstream, TLS material paths, and the same version-compatible
   tooling used by instance Pods.
8. **Object-store override:** `Backup.spec` may override the destination object
   store. When it does not, the controller defaults to
   `Cluster.spec.backup.objectStore`.
9. **Metadata visibility:** backup metadata is written to `Backup.status` for
   fast inspection. A sidecar `metadata.json` object may still be uploaded next
   to the archive for out-of-cluster inspection.
10. **Checksum:** backup artifacts should record a SHA256 checksum. S3 ETag may
    be captured as provider metadata, but it is not the integrity source of
    truth.

## Backup object-store layout

Use deterministic keys so users can inspect and copy backups manually:

```text
<path>/<cluster>/<backup-name>/<backup-id>/backup.xbstream
<path>/<cluster>/<backup-name>/<backup-id>/metadata.json
```

`backup-id` should be stable once assigned, for example
`<backup-name>-<unix-seconds>` or a generated timestamp+uid suffix. The
metadata JSON should include at least:

- cluster namespace/name and Backup namespace/name;
- source instance name;
- method and format version;
- server image/version when known;
- started/stopped timestamps;
- GTID set and binlog coordinates from `xtrabackup_binlog_info`;
- object key, size, and checksum when available.

## API and status model

M6 should try to keep the public API close to what already exists. Likely
additions:

- `Backup.status.jobName` if useful for observability.
- `Backup.status.destinationPath` should be the full S3 URI or provider-neutral
  bucket/key string.
- `Backup.status.sha256` should record the SHA256 checksum of the uploaded
  `backup.xbstream` object.
- `Backup.status.beginGTID` / `endGTID` can initially both be the XtraBackup
  GTID point. A richer range belongs with binlog archiving/PITR.
- metadata that is needed to recover or inspect the backup should be mirrored in
  status, not only stored in object storage.
- `Backup.status.conditions` should use `Ready`, `Progressing`, and `Degraded`
  consistently.

The `Backup` spec can stay small for M6, with an optional object-store override:

```yaml
spec:
  cluster:
    name: cluster-sample
  method: xtrabackup
  target: prefer-standby
  online: true
  objectStore:
    bucket: backups
    path: cnmsql
    credentials:
      accessKeyId:
        name: minio-credentials
        key: ACCESS_KEY_ID
      secretAccessKey:
        name: minio-credentials
        key: SECRET_ACCESS_KEY
```

When `spec.objectStore` is omitted, the destination comes from
`Cluster.spec.backup.objectStore`. A Backup without either configured object
store should fail fast with a clear condition.

Recovery example:

```yaml
spec:
  bootstrap:
    recovery:
      backup:
        name: backup-sample
```

The recovered Cluster should use its own storage/PVCs and should not mutate the
source Backup or source Cluster.

## Reconciliation design

### Backup controller

1. Fetch the `Backup` and owning `Cluster`.
2. Reject unsupported methods (`volumeSnapshot`) and missing object-store config.
3. If no `backupID` is assigned, patch status with `Pending`, `backupID`, and
   the computed destination path.
4. Select the source instance:
   - prefer healthy replicas when target is `prefer-standby`;
   - fall back to the current primary if no healthy replica exists;
   - require the primary when target is `primary`;
   - block when no selected instance has the backup endpoint enabled/healthy.
5. Create or adopt a Job named from the Backup, owned by the Backup.
6. The Job runs the manager backup worker:
   - loads mTLS client material for the instance-manager stream;
   - resolves object-store credentials and TLS settings;
   - GETs `/cluster/backup`;
   - uploads the stream to S3 while computing SHA256;
   - writes `metadata.json`;
   - exits with machine-readable metadata for the controller to mirror into
     `Backup.status` where possible.
7. Reconcile Job status into `Backup.status.phase`, timestamps, conditions, and
   Events.
8. Do not rerun a completed Backup. Treat backups as immutable once completed.

### Recovery bootstrap

1. Allow `bootstrap.recovery.backup` through `unsupportedReason` when:
   - `initdb` is not set;
   - the referenced Backup exists and is `Completed`;
   - the Backup has a destination path and metadata.
2. For ordinal 1, render an init container/command that:
   - downloads `backup.xbstream` from S3;
   - extracts it into the backup directory;
   - prepares and copy-backs into the data directory;
   - validates `xtrabackup_binlog_info`;
   - leaves the server ready for `instance run`.
3. Mark the first instance as target/current primary through the existing M5.5
   dynamic role flow.
4. Scale-up replicas normally: they should clone from the recovered primary
   using the existing M4 streaming clone path.
5. Reject `recoveryTarget` until PITR/binlog archive support exists.

## Command/package design

Add a package for object-store operations, for example:

```text
pkg/management/mysql/objectstore/
```

Responsibilities:

- build an S3 client from `S3ObjectStore`;
- stream upload/download without buffering entire backups in memory;
- compute SHA256 while streaming uploads/downloads;
- support path-style addressing and custom endpoints;
- support explicit CA bundles and insecure test endpoints;
- return structured errors suitable for status conditions.

Add one or two commands under `internal/cmd/manager/instance`:

- `backup` or `backup upload`: stream from an instance-manager URL to S3;
- `restore` (currently a stub): download from S3, extract, prepare, and
  copy-back into the data directory.

The backup worker command should use structured logs and never log credentials.

## Security

- Do not mount S3 credentials into database Pods for M6.
- Backup worker Jobs should get only the Secrets they need:
  object-store credentials, optional CA bundle, and the existing instance
  control client certificate.
- Owner-reference backup Jobs to the Backup object.
- Use mTLS for the instance-manager stream as today.
- Redact object-store secrets and signed URLs from logs/status.
- Avoid writing backup data through the controller-manager process.

## Implementation order

1. Add the object-store package with S3 client construction, key building,
   streaming upload/download, and unit tests.
2. Implement the backup worker command that pulls `/cluster/backup` and uploads
   `backup.xbstream` + `metadata.json`.
3. Scaffold/register a `BackupReconciler` if not already present, with RBAC for
   Backups, Jobs, Pods/status reads as needed, Secrets, and Events.
4. Implement target selection from observed Cluster/Pod/instance status.
5. Render the backup Job and reconcile Job phase into Backup status.
6. Update samples for `Cluster.spec.backup.objectStore` and a one-shot Backup.
7. Implement `instance restore` around the existing fetch/extract/prepare/
   copy-back pieces plus S3 download.
8. Allow `bootstrap.recovery.backup` in the Cluster reconciler and render the
   restore init path for the first instance.
9. Add unit tests across controller/objectstore/commands.
10. Add integration tests with MinIO/testcontainers for upload/download.
11. Add Kind e2e: create cluster, write data, create Backup, delete/create a
    fresh cluster from that Backup, verify data and replica scale-up.

## Testing

- Unit:
  - S3 key construction and destination path formatting;
  - Backup object-store override and Cluster defaulting;
  - credentials/env/Secret projection for worker Jobs;
  - worker image selection from the Cluster image;
  - SHA256 computation while streaming;
  - backup target selection for primary/prefer-standby/fallback cases;
  - Backup phase transitions from Job states;
  - immutable completed Backup behavior;
  - restore rejects non-empty data directories;
  - recovery blocks missing/failed/incomplete Backup objects.
- Integration:
  - MinIO upload/download round trip for xbstream payload and metadata;
  - restore command against a real XtraBackup fixture where practical.
- E2E:
  - 3-instance cluster, create Backup to MinIO, assert Backup completes and
    object exists;
  - bootstrap a new cluster from the Backup and verify application data;
  - scale the restored cluster to 3 instances and verify replicas catch up.

## Acceptance criteria

- A user can configure `Cluster.spec.backup.objectStore` and create a `Backup`
  CR that reaches `Completed`.
- The backup artifact and metadata are present in the S3-compatible bucket.
- Backup status records selected instance, destination path, timings, method,
  phase, SHA256 checksum, and GTID/binlog metadata when available.
- A new Cluster can bootstrap from a completed Backup and serve the previously
  written data.
- Restored clusters can scale replicas using the existing XtraBackup clone path.
- Missing object-store config, unsupported methods, missing Backup references,
  and unsupported PITR targets fail loudly with useful conditions.
- `make generate manifests`, `make lint-fix`, `make test`, integration tests,
  and the M6 MinIO-backed e2e pass.

## Open questions

None for the initial M6 design.
