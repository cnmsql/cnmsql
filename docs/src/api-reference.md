---
title: "API Reference"
description: "Field guide for the Cluster, Backup, ScheduledBackup, ImageCatalog, and ClusterImageCatalog CRDs."
sidebar_position: 9
---

# API reference

This page is a practical field guide for the CNMySQL CRDs that are currently
used to run clusters, choose images, and manage backups.

The API group is:

```text
mysql.cloudnative-mysql.io/v1alpha1
```

The API is still `v1alpha1`, so fields can change while the operator is under
active development.

## Cluster

`Cluster` is the main namespaced resource. It describes a Percona Server for
MySQL topology, bootstrap method, storage, image, backup configuration, and
operational policy.

Short names:

```text
mysql
mysqlcluster
```

### Minimal example

```yaml
apiVersion: mysql.cloudnative-mysql.io/v1alpha1
kind: Cluster
metadata:
  name: cluster-sample
spec:
  instances: 3
  imageName: cnmysql-instance:8.4
  storage:
    size: 10Gi
  mysql:
    binlogFormat: ROW
  bootstrap:
    initdb:
      database: app
      owner: app
```

### Top-level spec fields

| Field | Type | Purpose |
|-------|------|---------|
| `description` | string | Human-readable description. |
| `inheritedMetadata` | object | Labels/annotations inherited by related objects. |
| `imageName` | string | Direct instance image reference. Mutually exclusive with `imageCatalogRef`. |
| `imageCatalogRef` | object | Resolve an image from `ImageCatalog` or `ClusterImageCatalog`. |
| `imagePullPolicy` | `Always`, `Never`, `IfNotPresent` | Pull policy for instance images. |
| `imagePullSecrets` | array | Pull Secrets for private registries. |
| `instances` | integer | Number of MySQL instances. Minimum `1`, default `1`. |
| `minSyncReplicas` | integer | Minimum semi-sync acknowledgers. |
| `maxSyncReplicas` | integer | Maximum semi-sync acknowledgers; must be lower than `instances`. |
| `mysql` | object | MySQL configuration. |
| `storage` | object | Required data PVC configuration. |
| `binlogStorage` | object | Optional separate PVC configuration for binlogs. |
| `bootstrap` | object | Fresh init or recovery bootstrap. |
| `rootPasswordSecret` | local object ref | Root password Secret. Generated when omitted. |
| `enableSuperuserAccess` | bool | Whether root access is exposed through the Secret. Default `false`. |
| `certificates` | object | TLS/mTLS Secret configuration. |
| `resources` | Kubernetes `ResourceRequirements` | CPU/memory requests and limits for instance containers. |
| `affinity` | object | Scheduling affinity, anti-affinity, node selectors, and tolerations. |
| `topologySpreadConstraints` | array | Kubernetes topology spread constraints. |
| `priorityClassName` | string | Pod priority class. |
| `schedulerName` | string | Scheduler name for instance Pods. |
| `primaryUpdateStrategy` | `unsupervised`, `supervised` | Strategy for primary updates. Default `unsupervised`. |
| `primaryUpdateMethod` | `switchover`, `restart` | Method for primary update. Default `switchover`. |
| `maxStartDelay` | integer seconds | Startup bound. Default `3600`. |
| `maxStopDelay` | integer seconds | Shutdown bound. Default `1800`. |
| `smartShutdownTimeout` | integer seconds | Graceful shutdown window before fast fallback. Default `180`. |
| `maxSwitchoverDelay` | integer seconds | Switchover bound. Default `3600`. |
| `failoverDelay` | integer seconds | Delay before automatic failover. Default `0`. |
| `backup` | object | Object-store, backup, retention, and archiving configuration. |
| `replica` | object | Replica-cluster mode from an external source. Designed for later milestones. |
| `externalClusters` | array | External sources for replication or recovery. |
| `managed` | object | Operator-managed services and future managed resources. |
| `monitoring` | object | PodMonitor and custom query references. |
| `nodeMaintenanceWindow` | object | Node-maintenance behavior. |
| `enablePDB` | bool | Create a PodDisruptionBudget. Default `true`. |
| `serviceAccountTemplate` | object | Metadata for generated ServiceAccount. |
| `env` | array | Extra environment variables on instance containers. |
| `envFrom` | array | Extra environment sources on instance containers. |
| `podSecurityContext` | Kubernetes `PodSecurityContext` | Pod security context. |
| `securityContext` | Kubernetes `SecurityContext` | Container security context. |
| `logLevel` | `error`, `warning`, `info`, `debug`, `trace` | Per-cluster operator log level. Default `info`. |

### Image selection

Use either `imageName` or `imageCatalogRef`.

```yaml
spec:
  imageName: cnmysql-instance:8.4
```

```yaml
spec:
  imageCatalogRef:
    apiGroup: mysql.cloudnative-mysql.io
    kind: ImageCatalog
    name: percona-images
    major: 8
```

### MySQL configuration

```yaml
spec:
  mysql:
    parameters:
      require_secure_transport: "ON"
      max_connections: "500"
    binlogFormat: ROW
    semiSync:
      enabled: true
      timeoutMillis: 1000
    additionalConfigFiles:
      custom.cnf: |
        [mysqld]
        sort_buffer_size=4M
```

| Field | Type | Purpose |
|-------|------|---------|
| `parameters` | map string/string | MySQL settings rendered under `[mysqld]`. Managed keys are protected. |
| `binlogFormat` | `ROW`, `STATEMENT`, `MIXED` | Binary-log format. Default `ROW`; required for safe replication and PITR. |
| `semiSync.enabled` | bool | Enable semi-synchronous replication. |
| `semiSync.timeoutMillis` | integer | Wait timeout before falling back to async behavior. |
| `additionalConfigFiles` | map string/string | Extra config files rendered into the MySQL config directory. |

### Storage

```yaml
spec:
  storage:
    storageClass: fast
    size: 100Gi
    resizeInUseVolumes: true
```

`storage` and `binlogStorage` use the same shape:

| Field | Type | Purpose |
|-------|------|---------|
| `storageClass` | string | StorageClass for generated PVCs. |
| `size` | string | Requested storage size. |
| `resizeInUseVolumes` | bool | Allow PVC growth. Default `true`. |
| `pvcTemplate` | Kubernetes `PersistentVolumeClaimSpec` | Full PVC template override. |

### Bootstrap

Fresh initialization:

```yaml
spec:
  bootstrap:
    initdb:
      database: app
      owner: app
      secret:
        name: app-credentials
      postInitSQL:
        - CREATE TABLE app.ready (id int primary key)
      characterSet: utf8mb4
      collation: utf8mb4_0900_ai_ci
```

Recovery from a `Backup`:

```yaml
spec:
  bootstrap:
    recovery:
      backup:
        name: backup-sample
```

Point-in-time recovery:

```yaml
spec:
  bootstrap:
    recovery:
      backup:
        name: backup-sample
      recoveryTarget:
        targetGTID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-500"
```

Raw object-store recovery is planned around `bootstrap.recovery.source` and
`externalClusters`, but only the implemented backup-object recovery path should
be considered stable today.

`recoveryTarget` accepts exactly one of:

| Field | Purpose |
|-------|---------|
| `targetTime` | RFC3339 timestamp target. |
| `targetGTID` | Inclusive MySQL GTID set target. |
| `targetImmediate` | Stop as soon as the base backup is consistent. |

An empty `recoveryTarget: {}` means replay to the latest archived point. No
`recoveryTarget` means restore the physical base backup only.

### Backup and archiving configuration

```yaml
spec:
  backup:
    objectStore:
      bucket: cnmysql-backups
      path: production
      endpoint: http://minio.minio.svc:9000
      credentials:
        accessKeyId:
          name: minio-creds
          key: accessKey
        secretAccessKey:
          name: minio-creds
          key: secretKey
    retentionPolicy: 30d
    target: prefer-standby
    continuousArchiving:
      enabled: true
      targetRPOSeconds: 300
      maxBinlogSizeMB: 16
      binlogExpireSeconds: 604800
```

| Field | Type | Purpose |
|-------|------|---------|
| `objectStore` | `S3ObjectStore` | Destination for backups and binlog archives. |
| `retentionPolicy` | string | Duration such as `30d`, `8w`, `12m`. Retention GC is future work. |
| `target` | `primary`, `prefer-standby` | Default source selection for backups. |
| `xtrabackupOptions` | array string | Extra XtraBackup flags. |
| `continuousArchiving.enabled` | bool | Enable primary-gated binlog archiving. |
| `continuousArchiving.targetRPOSeconds` | integer | Time-based binlog rotation/RPO bound. Default `300`. |
| `continuousArchiving.maxBinlogSizeMB` | integer | Size-based binlog rotation bound. Default `16`. |
| `continuousArchiving.binlogExpireSeconds` | integer | Expiry backstop under the purge guard. Default `604800`. |

### Managed services

```yaml
spec:
  managed:
    services:
      disabledDefaultServices:
        - ro
```

`disabledDefaultServices` accepts `rw`, `ro`, and `r`.

### External clusters

`externalClusters` defines named external sources for future replica or raw
object-store recovery flows:

```yaml
spec:
  externalClusters:
    - name: source-cluster
      connectionParameters:
        host: mysql.example.com
        port: "3306"
      password:
        name: source-creds
        key: password
      objectStore:
        bucket: source-backups
        path: production
        credentials:
          inheritFromIAMRole: true
```

Live external replica mode and raw S3 recovery are planned milestones; verify
current implementation status before relying on these fields.

### Cluster status

| Field | Purpose |
|-------|---------|
| `instances` | Total observed instances. |
| `readyInstances` | Ready instances. |
| `instanceNames` | Instance Pod names. |
| `currentPrimary` | Current primary instance. |
| `targetPrimary` | Desired primary during bootstrap, switchover, or failover. |
| `currentPrimaryTimestamp` | When the current primary was elected. |
| `targetPrimaryTimestamp` | When the current primary-change request started. |
| `divergedInstances` | Instances excluded because of errant GTIDs. |
| `primaryFailingSince` | When the current primary first became unhealthy. |
| `latestGeneratedNode` | Latest generated instance ordinal. |
| `phase` / `phaseReason` | Human-readable reconciliation state. |
| `image` | Resolved image in use. |
| `gtidExecutedByInstance` | Last observed GTID set by instance. |
| `certificates` | Managed certificate status. |
| `continuousArchiving` | Binlog archive health and frontier. |
| `lastRetentionRunTime` | Last retention GC pass time. |
| `observedGeneration` | Last reconciled generation. |
| `conditions` | Kubernetes conditions such as `Ready`, `Progressing`, `Degraded`. |

## Backup

`Backup` is a namespaced one-shot physical backup request.

Short name:

```text
mybackup
```

### Example

```yaml
apiVersion: mysql.cloudnative-mysql.io/v1alpha1
kind: Backup
metadata:
  name: backup-sample
spec:
  cluster:
    name: cluster-sample
  method: xtrabackup
  target: prefer-standby
  online: true
```

### Spec fields

| Field | Type | Purpose |
|-------|------|---------|
| `cluster.name` | string | Required Cluster to back up. |
| `objectStore` | `S3ObjectStore` | Optional destination override. Uses the Cluster object store when omitted. |
| `method` | `xtrabackup`, `volumeSnapshot` | Backup method. Default `xtrabackup`; only XtraBackup is implemented today. |
| `target` | `primary`, `prefer-standby` | Source instance selection. Default `prefer-standby`. |
| `online` | bool | Hot backup flag. Default `true`. |

### Status fields

| Field | Purpose |
|-------|---------|
| `phase` | `pending`, `running`, `completed`, or `failed`. |
| `instanceName` | Instance selected as backup source. |
| `method` | Method used by the controller. |
| `backupId` | Stable ID used in object-store keys. |
| `jobName` | Backup worker Job. |
| `destinationPath` | Full archive URI. |
| `sha256` | SHA256 checksum of `backup.xbstream`. |
| `beginGTID` / `endGTID` | GTID metadata. |
| `beginBinlog` / `endBinlog` | Binlog coordinate metadata. |
| `startedAt` / `stoppedAt` | Backup timing. |
| `error` | Failure message. |
| `conditions` | `Ready`, `Progressing`, and `Degraded` observations. |

Deleting a `Backup` object does not currently delete remote object-store data.

## ScheduledBackup

`ScheduledBackup` is a namespaced cron scheduler that creates `Backup` objects.

Short name:

```text
myscheduledbackup
```

### Example

```yaml
apiVersion: mysql.cloudnative-mysql.io/v1alpha1
kind: ScheduledBackup
metadata:
  name: cluster-sample-daily
spec:
  schedule: "0 0 2 * * *"
  cluster:
    name: cluster-sample
  immediate: true
  backupOwnerReference: self
  method: xtrabackup
  target: prefer-standby
  online: true
```

### Spec fields

| Field | Type | Purpose |
|-------|------|---------|
| `schedule` | six-field cron | Required schedule including seconds. |
| `cluster.name` | string | Required Cluster to back up. |
| `suspend` | bool | Pause schedule. Default `false`. |
| `immediate` | bool | Create one Backup at creation time. Default `false`. |
| `backupOwnerReference` | `none`, `self`, `cluster` | Owner reference mode for generated Backups. Default `self`. |
| `method` | `xtrabackup`, `volumeSnapshot` | Method propagated to generated Backups. Default `xtrabackup`. |
| `target` | `primary`, `prefer-standby` | Source selection propagated to generated Backups. Default `prefer-standby`. |
| `online` | bool | Hot backup flag propagated to generated Backups. Default `true`. |

`schedule` uses:

```text
second minute hour day-of-month month day-of-week
```

### Status fields

| Field | Purpose |
|-------|---------|
| `lastCheckTime` | Last schedule evaluation time. |
| `lastScheduleTime` | Last scheduled time that created or adopted a Backup. |
| `nextScheduleTime` | Next expected schedule time. |

Generated Backups are labelled with:

```text
mysql.cloudnative-mysql.io/scheduled-backup=<scheduledbackup-name>
```

Immediate Backups also carry:

```text
mysql.cloudnative-mysql.io/immediate-backup=true
```

## ImageCatalog

`ImageCatalog` is a namespaced mapping from MySQL major version to CNMySQL
instance image.

Short name:

```text
myimagecatalog
```

### Example

```yaml
apiVersion: mysql.cloudnative-mysql.io/v1alpha1
kind: ImageCatalog
metadata:
  name: percona-images
spec:
  images:
    - major: 8
      image: registry.example.com/cnmysql-instance:8.4
    - major: 9
      image: registry.example.com/cnmysql-instance:9.x
```

### Fields

| Field | Type | Purpose |
|-------|------|---------|
| `spec.images` | array | Required image mappings. Minimum one, maximum eight. |
| `spec.images[].major` | integer | Major key used by `Cluster.spec.imageCatalogRef.major`. |
| `spec.images[].image` | string | Fully qualified instance image reference. |

Each `major` value can appear at most once.

## ClusterImageCatalog

`ClusterImageCatalog` has the same `spec.images` shape as `ImageCatalog`, but it
is cluster-scoped and can be reused across namespaces.

Short name:

```text
myclusterimagecatalog
```

### Example

```yaml
apiVersion: mysql.cloudnative-mysql.io/v1alpha1
kind: ClusterImageCatalog
metadata:
  name: global-percona-images
spec:
  images:
    - major: 8
      image: registry.example.com/cnmysql-instance:8.4
    - major: 9
      image: registry.example.com/cnmysql-instance:9.x
```

Reference it from a Cluster:

```yaml
spec:
  imageCatalogRef:
    apiGroup: mysql.cloudnative-mysql.io
    kind: ClusterImageCatalog
    name: global-percona-images
    major: 8
```

## Shared object-store fields

`S3ObjectStore` appears in `Cluster.spec.backup.objectStore`,
`Backup.spec.objectStore`, and `Cluster.spec.externalClusters[].objectStore`.

| Field | Type | Purpose |
|-------|------|---------|
| `endpoint` | string | Custom S3-compatible endpoint. Empty targets AWS S3. |
| `region` | string | Bucket/signing region. |
| `bucket` | string | Required bucket name. |
| `path` | string | Key prefix. |
| `forcePathStyle` | bool | Path-style addressing. Default `true`. |
| `signatureVersion` | `s3v4`, `s3v2` | Signing scheme. Default `s3v4`. |
| `serverSideEncryption` | string | SSE algorithm, such as `AES256` or `aws:kms`. |
| `storageClass` | string | Object-store storage class. |
| `credentials.accessKeyId` | secret key selector | Access key ID. |
| `credentials.secretAccessKey` | secret key selector | Secret access key. |
| `credentials.sessionToken` | secret key selector | Optional session token. |
| `credentials.inheritFromIAMRole` | bool | Use workload/IAM credentials instead of static Secrets. |
| `tls.insecureSkipVerify` | bool | Disable endpoint certificate verification. Testing only. |
| `tls.caBundleSecret` | secret key selector | CA bundle for private object-store CAs. |

## Shared condition types

CNMySQL resources use Kubernetes `metav1.Condition` entries. Common condition
types are:

| Condition | Meaning |
|-----------|---------|
| `Ready` | Resource is fully functional. |
| `Progressing` | Resource is being created, updated, backed up, restored, or changed. |
| `Degraded` | Resource failed to reach or maintain the desired state. |
