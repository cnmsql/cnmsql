---
title: "API Reference"
description: "Field guide for the Cluster, Database, Backup, ScheduledBackup, ImageCatalog, and ClusterImageCatalog CRDs."
sidebar_position: 15
---

# API reference

This page is a practical field guide for the cloudnative-mysql CRDs that are used to run
clusters, choose images, and manage backups.

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
  imageName: ghcr.io/cloudnative-mysql/cloudnative-mysql-instance:8.4
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

### Monitoring

```yaml
spec:
  monitoring:
    enablePodMonitor: true
    disableDefaultQueries: false
    metricsQueriesTTL: 30s
    tls:
      enabled: false
```

Instances expose Prometheus metrics on the named container port `metrics`
(`9187`) at `/metrics`. The endpoint is plain HTTP unless monitoring TLS is
enabled.

| Field | Type | Purpose |
|-------|------|---------|
| `enablePodMonitor` | bool | Create an owned Prometheus Operator `PodMonitor`. Default `false`. |
| `customQueriesConfigMap` | array | ConfigMap keys that hold custom monitoring query definitions. |
| `customQueriesSecret` | array | Secret keys that hold custom monitoring query definitions. |
| `disableDefaultQueries` | bool pointer | Disable the built-in default query set once default query loading is enabled. |
| `metricsQueriesTTL` | Kubernetes `Duration` | Minimum interval between custom/default query executions. |
| `tls.enabled` | bool | Serve the metrics endpoint over TLS. Default `false`. |

### Image selection

Use either `imageName` or `imageCatalogRef`.

```yaml
spec:
  imageName: ghcr.io/cloudnative-mysql/cloudnative-mysql-instance:8.4
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
      dataDurability: preferred
    additionalConfigFiles:
      custom.cnf: |
        [mysqld]
        sort_buffer_size=4M
```

| Field | Type | Purpose |
|-------|------|---------|
| `parameters` | map string/string | MySQL settings rendered under `[mysqld]`. Denied keys block the cluster (see below). |
| `binlogFormat` | `ROW`, `STATEMENT`, `MIXED` | Binary-log format. Default `ROW`; required for safe replication and PITR. |
| `semiSync.enabled` | bool | Enable semi-synchronous replication. |
| `semiSync.timeoutMillis` | integer | Wait timeout before falling back to async behavior. |
| `semiSync.dataDurability` | `preferred`, `required` | How strictly `minSyncReplicas` is enforced when replicas are unhealthy. `preferred` (default) self-heals the acknowledgement count down to keep the primary writable; `required` keeps it fixed so writes block. |
| `additionalConfigFiles` | map string/string | Extra config files rendered into the MySQL config directory. |

#### Denied and deprecated parameters

`spec.mysql.parameters` is validated before any instance is provisioned. Keys are
compared case-insensitively with dashes and underscores treated as equivalent
(`log-bin` ≡ `log_bin`).

- **Denied keys** set the cluster `phase: Blocked` with a reason naming the
  offending key(s). These are either keys the operator manages directly
  (replication identity and topology, TLS material, binlog durability) or keys
  that would relocate on-disk paths or expose the administrative interface,
  for example `server_id`, `gtid_mode`, `read_only`, `log_bin`, `ssl_cert`,
  `sync_binlog`, `datadir`, `socket`, `tmpdir`, `plugin_dir`, `secure_file_priv`,
  `log_error`, `admin_address`, `admin_ssl_cert`, `tls_ciphersuites`,
  `skip_replica_start`, `auto_generate_certs`. `require_secure_transport` is
  not denied; requiring TLS for client connections is the user's choice.
- **Deprecated keys** are accepted but emit a `DeprecatedParameter` warning event
  pointing at the current spelling, for example `slave_parallel_workers`
  (→ `replica_parallel_workers`), `master_info_repository` (removed on 8.0.23+).

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

Raw object-store recovery (no `Backup` CR) points `bootstrap.recovery.source` at
an `externalClusters` entry. The entry carries its own `objectStore` and its
`name` is the S3 key prefix backups were stored under:

```yaml
spec:
  bootstrap:
    recovery:
      source: prod-cluster
      backupID: ""              # empty = latest completed; set to pin a backup
      recoveryTarget:           # optional, identical to the Backup path
        targetGTID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-500"
  externalClusters:
    - name: prod-cluster
      objectStore:
        bucket: cloudnative-mysql-backups
        path: production
        endpoint: http://minio.minio.svc:9000
        credentials:
          accessKeyId:
            name: minio-creds
            key: accessKey
          secretAccessKey:
            name: minio-creds
            key: secretKey
```

`recovery.backup` and `recovery.source` are mutually exclusive. `backupID` is
only meaningful with `source`; when empty the operator selects the latest
completed backup discovered under the source prefix. See
[Restore from raw object store](backup-recovery#restore-from-raw-object-store-no-backup-cr).

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
      bucket: cloudnative-mysql-backups
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
      template:
        metadata:
          labels:
            app.kubernetes.io/part-of: my-app
          annotations:
            service.beta.kubernetes.io/aws-load-balancer-scheme: internal
        spec:
          type: LoadBalancer
      additional:
        - name: mysql-lb
          selectorType: rw
          serviceTemplate:
            spec:
              type: LoadBalancer
        - name: mysql-internal-read
          selectorType: ro
          updateStrategy: replace
          serviceTemplate:
            metadata:
              labels:
                pool: reporting
            spec:
              type: ClusterIP
```

| Field | Type | Description |
| --- | --- | --- |
| `disabledDefaultServices` | array | Default services to disable. Accepts `ro` and `r`; the `rw` service cannot be disabled. |
| `template` | object | Service template merged onto each default rw/ro/r service. |
| `additional` | array | User-defined extra services routed to a role. |
| `additional[].name` | string | Rendered as `<cluster>-<name>`; unique and not colliding with default names. |
| `additional[].selectorType` | enum | `rw`, `ro`, or `r`. |
| `additional[].updateStrategy` | enum | `patch` (default, merge) or `replace` (swap defaults). |
| `additional[].serviceTemplate` | object | Same shape as `template`. |

The service template's `spec` exposes `type`, `externalTrafficPolicy`,
`sessionAffinity`, `loadBalancerSourceRanges`, `externalName`, and
`healthCheckNodePort`. The selector, ports, and `clusterIP` are operator-managed.

### Managed roles

`spec.managed.roles` declares MySQL users (roles) the operator reconciles on the
primary. The operator lists the live users, diffs them against the spec, and
issues the minimal `CREATE USER` / `ALTER USER` / `DROP USER` needed. Passwords
are read from a referenced Secret; when `passwordSecret` is omitted the operator
generates a password and stores it in a Secret named `<cluster>-<roleName>`
(key `password`).

```yaml
spec:
  managed:
    roles:
      - name: app
        host: "%"
        ensure: present
        passwordSecret:
          name: app-credentials
          key: password
        requireTLS: x509
        maxUserConnections: 50
        privileges:
          - privileges: [SELECT, INSERT, UPDATE, DELETE]
            "on": app.*
      - name: readonly
        ensure: present           # operator-generated password
        privileges:
          - privileges: [SELECT]
            "on": app.*
      - name: legacy
        ensure: absent            # dropped if present
```

| Field | Type | Description |
| --- | --- | --- |
| `name` | string | MySQL user name (max 32 chars). Reserved names (`root`, `mysql.*`, `cloudnative-mysql_*`) are rejected. |
| `host` | string | MySQL host part. Defaults to `%`. |
| `ensure` | enum | `present` (default) or `absent`. |
| `passwordSecret` | object | `{name, key}` selecting the password. Omit for an operator-generated password. |
| `superuser` | bool | Grants `ALL PRIVILEGES ON *.* WITH GRANT OPTION`. Mutually exclusive with `privileges`. |
| `maxUserConnections` | int32 | Per-account simultaneous-connection limit. `0` = no limit. |
| `maxQueriesPerHour` | int32 | Hourly query limit. `0` = no limit. |
| `maxUpdatesPerHour` | int32 | Hourly update limit. `0` = no limit. |
| `maxConnectionsPerHour` | int32 | Hourly connection limit. `0` = no limit. |
| `requireTLS` | enum | `none` (default), `ssl`, or `x509`. |
| `privileges` | array | Grants: `{privileges: [...], on: "db.*"}`. `on` defaults to `*.*`. |

Quote the `on` key (`"on": app.*`). Unquoted, YAML parses `on` as the boolean
`true`, and the API server rejects the manifest with a strict-decoding error.

Users present in MySQL but not declared in `spec.managed.roles` are left
untouched. To remove one, declare it with `ensure: absent`. Reconciliation runs
after the cluster reaches `Ready`; outcomes appear in
`status.managedRolesStatus`.

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
| `fencedInstances` | Instances fenced out of routing, with mysqld stopped. |
| `failedInstances` | Instances with positive failure evidence (Failed phase or CrashLoopBackOff). |
| `replicationBrokenInstances` | Reachable replicas whose replication aborted with a recorded SQL/IO error. |
| `primaryFailingSince` | When the current primary first became unhealthy. |
| `latestGeneratedNode` | Latest generated instance ordinal. |
| `phase` / `phaseReason` | Human-readable reconciliation state. |
| `image` | Resolved image in use. |
| `gtidExecutedByInstance` | Last observed GTID set by instance. |
| `certificates` | Resolved certificate Secret names and managed certificate expirations. |
| `continuousArchiving` | Binlog archive health and frontier. |
| `lastRetentionRunTime` | Last retention GC pass time. |
| `managedRolesStatus` | Per-role reconciliation state: `byStatus`, `cannotReconcile`, and applied `passwordStatus`. |
| `observedGeneration` | Last reconciled generation. |
| `conditions` | Kubernetes conditions such as `Ready`, `Progressing`, `Degraded`. |

## Database

`Database` is a namespaced CRD (short name `mydatabase`) that declares a MySQL
schema and the accounts scoped to it. It references a `Cluster` in the **same
namespace**, which makes it the unit you delegate to tenant teams; see
[Multi-Tenancy](./multi-tenancy.md). The controller diffs the spec against the
live server and issues the minimal SQL to converge; nothing is dropped unless
you ask for it.

### Example

```yaml
apiVersion: mysql.cloudnative-mysql.io/v1alpha1
kind: Database
metadata:
  name: tenant-a
  namespace: shared            # must match the Cluster's namespace
spec:
  cluster:
    name: shared
  name: tenant_a              # the MySQL schema; defaults to the resource name
  ensure: present
  characterSet: utf8mb4
  collation: utf8mb4_0900_ai_ci
  reclaimPolicy: retain
  users:
    - name: tenant_a_app
      host: "%"
      ensure: present
      passwordSecret:
        name: tenant-a-db
        key: password
      grants:
        - privileges: [SELECT, INSERT, UPDATE, DELETE]
          # `on` defaults to the managed schema (tenant_a.*)
```

### Spec fields

| Field | Type | Description |
| --- | --- | --- |
| `cluster` | object | `{name}` of the target `Cluster` in the same namespace. Required. |
| `name` | string | MySQL schema name. Defaults to the resource name. |
| `ensure` | enum | `present` (default) creates the schema; `absent` drops it. |
| `characterSet` | string | Schema character set (e.g. `utf8mb4`). |
| `collation` | string | Schema collation (e.g. `utf8mb4_0900_ai_ci`). |
| `reclaimPolicy` | enum | `retain` (default) keeps the schema on object deletion; `delete` drops it (finalizer-guarded). |
| `users` | array | Accounts managed for this schema (see below). |

#### `users[]`

| Field | Type | Description |
| --- | --- | --- |
| `name` | string | MySQL user name. Required. |
| `host` | string | Host part. Defaults to `%`. |
| `ensure` | enum | `present` (default) or `absent`. |
| `passwordSecret` | object | `{name, key}` selecting the password Secret in the same namespace. |
| `grants` | array | Grants: `{privileges: [...], on: "db.*"}`. `on` defaults to the managed schema. |

### Database status

| Field | Purpose |
| --- | --- |
| `applied` | `true` once MySQL matches the spec. |
| `message` | Detail, typically an error. |
| `observedGeneration` | Last reconciled generation. |
| `passwordStatus` | Per user (`name@host`), the source Secret `resourceVersion` last applied; drives password re-application only on change. |
| `conditions` | Latest observations of the database state. |

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

Deleting a `Backup` object does not delete remote object-store data.

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

`ImageCatalog` is a namespaced mapping from MySQL major version to cloudnative-mysql
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
      image: ghcr.io/cloudnative-mysql/cloudnative-mysql-instance:8.4
    - major: 9
      image: ghcr.io/cloudnative-mysql/cloudnative-mysql-instance:9.x
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
      image: ghcr.io/cloudnative-mysql/cloudnative-mysql-instance:8.4
    - major: 9
      image: ghcr.io/cloudnative-mysql/cloudnative-mysql-instance:9.x
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

cloudnative-mysql resources use Kubernetes `metav1.Condition` entries. Common condition
types are:

| Condition | Meaning |
|-----------|---------|
| `Ready` | Resource is fully functional. |
| `Progressing` | Resource is being created, updated, backed up, restored, or changed. |
| `Degraded` | Resource failed to reach or maintain the desired state. |
