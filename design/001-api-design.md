# 001 — API Design

**Status:** done
**Milestone:** M1

Define the full CRD surface as Go types with kubebuilder markers covering `Cluster`, `Backup`, `ScheduledBackup`, `Database`, `ImageCatalog`, and `ClusterImageCatalog`. Pooler deferred to its own milestone; object store is S3-only with maximal compatibility.

## Overview

All types live in `api/v1alpha1` (single group `mysql`, domain `cnmsql.co`). Start at `v1alpha1` since the API is unstable. No reconciliation in M1 — output compiles, `make manifests generate` produces CRDs + deepcopy, and unit tests cover defaulting/validation helpers.

## Design

### CRD Set

| Kind | Scope | Purpose | CNPG analog |
|------|-------|---------|-------------|
| `Cluster` | Namespaced | Central resource: a MySQL cluster (1 primary + N replicas). | `Cluster` |
| `Backup` | Namespaced | One-shot physical backup request + status. | `Backup` |
| `ScheduledBackup` | Namespaced | Cron-scheduled backups producing `Backup` objects. | `ScheduledBackup` |
| `Database` | Namespaced | Declarative MySQL schema + users + grants for a `Cluster`. | `Database` |
| `ImageCatalog` | Namespaced | Maps major version → Percona image. | `ImageCatalog` |
| `ClusterImageCatalog` | Cluster | Cluster-wide version → image map. | `ClusterImageCatalog` |

Deferred (not in M1): `Pooler` (ProxySQL) — own milestone; `FailoverQuorum` (revisit with Group Replication); logical `Publication`/`Subscription` (not applicable to MySQL async repl).

### `Cluster` — the Core Type

#### `ClusterSpec`

- **Image selection**: `imageName string` (override) OR `imageCatalogRef *ImageCatalogRef`; `imagePullPolicy`, `imagePullSecrets`.
- **Topology**: `instances int` (default 1), `minSyncReplicas int`, `maxSyncReplicas int` (semi-sync waits).
- **MySQL config**: `mysql MySQLConfiguration` — `parameters map[string]string` (my.cnf), `serverVersion string`, `binlogFormat` (default ROW), `gtidMode` (always ON), `semiSync *SemiSyncConfiguration`.
- **Storage**: `storage StorageConfiguration` (size, storageClass, resizeInUseVolumes, pvcTemplate), optional `binlogStorage *StorageConfiguration` (separate volume for binlogs, analogous to WAL storage).
- **Bootstrap**: `bootstrap *BootstrapConfiguration` — `initdb *BootstrapInitDB` (fresh init: database, owner, secret, postInitSQL) | `recovery *BootstrapRecovery` (from backup/objectStore) | `pitr` later.
- **Superuser**: `rootSecret *LocalObjectReference`, `enableSuperuserAccess *bool`.
- **Certificates**: `certificates *CertificatesConfiguration` (serverCA/serverCert/clientCA secrets) — for mTLS.
- **Resources/scheduling**: `resources`, `affinity AffinityConfiguration`, `topologySpreadConstraints`, `tolerations`, `nodeSelector`, `priorityClassName`, `schedulerName`.
- **Lifecycle/update**: `primaryUpdateStrategy` (unsupervised|supervised), `primaryUpdateMethod` (switchover|restart), `maxStartDelay`, `maxStopDelay`, `maxSwitchoverDelay`, `failoverDelay`, `smartShutdownTimeout`.
- **Backup**: `backup *BackupConfiguration` (objectStore target, retentionPolicy, xtrabackup options).
- **Replica cluster**: `replica *ReplicaClusterConfiguration` (replicate from an external/source cluster) — designed now, wired later.
- **External clusters**: `externalClusters []ExternalCluster` (sources for replica/recovery).
- **Managed**: `managed *ManagedConfiguration` (managed roles/services).
- **Monitoring**: `monitoring *MonitoringConfiguration` (enablePodMonitor, custom queries CM/secret).
- **Guards**: `enablePDB *bool`; deletion guard via annotation/finalizer documented; `nodeMaintenanceWindow`.
- **Pod knobs**: `serviceAccountTemplate`, `env`, `envFrom`, `securityContext`, `podSecurityContext`, `seccompProfile`, `inheritedMetadata`, `projectedVolumeTemplate`, `ephemeralVolumesSizeLimit`.
- **ProxySQL/Pooler**: referenced by `Pooler`, not embedded.

#### `ClusterStatus`

`instances int`, `readyInstances int`, `instancesStatus map`, `currentPrimary string`, `targetPrimary string`, `currentPrimaryTimestamp`, `phase string` + `phaseReason`, `conditions []metav1.Condition`, `latestGeneratedNode int`, `instanceNames []string`, `healthyPVC/unusablePVC`, `gtidExecutedByInstance map`, `topology`, `certificates`, `image`, `pluginStatus`, `switchReplicaClusterStatus`.

#### Markers

`+kubebuilder:object:root=true`, `:subresource:status`, `:subresource:scale` (for `kubectl scale`), `:resource:scope=Namespaced,shortName=mysql;mysqlcluster`, printcolumns: Instances, Ready, Status, Primary, Age.

### `S3ObjectStore` (S3-only, Max Compatibility)

- `endpoint string` (optional; empty → AWS), `region string`, `bucket string`, `path string` (key prefix).
- `forcePathStyle bool` (default true — path-style `endpoint/bucket/key` for MinIO/Ceph/etc.).
- `signatureVersion string` enum `s3v4` (default) | `s3v2` (legacy providers).
- `credentials S3Credentials` (`accessKeyId SecretKeySelector`, `secretAccessKey SecretKeySelector`, `sessionToken *SecretKeySelector`); optional `inheritFromIAMRole bool` (IRSA / instance profile).
- `serverSideEncryption *string` (e.g. `AES256`, `aws:kms`), `storageClass *string`.
- `tls *S3TLSConfig` (`insecureSkipVerify bool`, `caBundleSecret *SecretKeySelector`) — self-signed endpoints.
- Used by `BackupConfiguration.objectStore` and `Backup.status.destinationPath`.

### Other Types (Field Outlines)

- **Backup**: spec `{ cluster ref, method (xtrabackup|volumeSnapshot), target (primary|prefer-standby), online bool }`; status `{ phase, startedAt, stoppedAt, backupId, beginLSN/binlog coords, destinationPath, error, ... }`.
- **ScheduledBackup**: spec `{ schedule (cron), cluster ref, suspend, immediate, backupOwnerReference, method, ...backup template }`; status `{ lastScheduleTime, lastCheckTime }`.
- **Database**: spec `{ cluster ref, name, ensure (present|absent), characterSet, collation, owner }` + `users []`, `grants []`; status `{ applied, observedGeneration, message }`.
- **ImageCatalog / ClusterImageCatalog**: spec `{ images []{ major int, image string } }` — shared via a Go interface (`genericimagecatalog`).
- **Shared/common types**: `LocalObjectReference`, `SecretKeySelector`, `StorageConfiguration`, `AffinityConfiguration`, `EmbeddedObjectMetadata`, `Metadata`, `CertificatesConfiguration` — placed in `common_types.go` / `base_types.go`.

## Implementation Notes

1. `kubebuilder create api --group mysql --version v1alpha1 --kind Cluster --resource --controller=false` (repeat per kind; `ClusterImageCatalog` with `--namespaced=false`).
2. Hand-author each `*_types.go` per outlines above; add `_funcs.go` for helpers (defaults, getters) where it aids testing.
3. Defaulting + validation: plain helper functions + (later) webhooks. M1 ships unit-testable helper funcs, not webhooks.
4. `make generate manifests` → deepcopy + CRDs.
5. Unit tests (Ginkgo/Gomega) for: default application, image resolution (catalog vs imageName), basic spec validation helpers, GTID/semisync defaults.
6. `make lint-fix && make test`.

## Decisions

- API version: `v1alpha1`.
- Pooler: **deferred** to its own milestone.
- Object store: **S3-only, max compatibility** (`S3ObjectStore` as defined above).
- Start at `v1alpha1` since the API is unstable.

## Verification

- `go build ./...` and `make test` green.
- CRDs generated under `config/crd/bases/` for all kinds.
- Each kind has at least defaulting + validation helper unit tests.
- No reconciliation logic beyond empty/auto-scaffolded controllers (or none if `--controller=false`).
