---
title: "Object Store Configuration"
description: "S3-compatible configuration for backups, continuous archiving, PITR, and recovery."
sidebar_position: 13
---

# Object store configuration

cloudnative-mysql uses an S3-compatible object store for physical backups and continuous
binlog archiving. The same `S3ObjectStore` API is used by one-shot Backups,
ScheduledBackup-generated Backups, recovery, and PITR.

## Minimal MinIO-style configuration

```yaml
spec:
  backup:
    objectStore:
      bucket: cloudnative-mysql-backups
      path: production
      endpoint: http://minio.minio.svc:9000
      region: us-east-1
      forcePathStyle: true
      credentials:
        accessKeyId:
          name: minio-creds
          key: accessKey
        secretAccessKey:
          name: minio-creds
          key: secretKey
```

`forcePathStyle: true` is the compatibility-friendly default and is required by
many S3-compatible providers such as MinIO and Ceph RGW.

## AWS-style configuration

```yaml
spec:
  backup:
    objectStore:
      bucket: cloudnative-mysql-prod-backups
      path: clusters
      region: eu-west-1
      forcePathStyle: false
      credentials:
        inheritFromIAMRole: true
```

When `endpoint` is empty, cloudnative-mysql targets AWS S3. When static credentials are
omitted and IAM inheritance is enabled, workers use the environment's default
credential chain.

## Per-backup override

A `Backup` can override the Cluster object store:

```yaml
apiVersion: mysql.cloudnative-mysql.io/v1alpha1
kind: Backup
metadata:
  name: backup-to-dr-bucket
spec:
  cluster:
    name: cluster-sample
  objectStore:
    bucket: cloudnative-mysql-dr-backups
    path: manual
    endpoint: https://s3.example.com
    credentials:
      accessKeyId:
        name: dr-s3-creds
        key: accessKey
      secretAccessKey:
        name: dr-s3-creds
        key: secretKey
```

If omitted, the Backup uses `Cluster.spec.backup.objectStore`.

An `objectStore` also attaches to an `externalClusters` entry, which enables
[raw object-store recovery](backup-recovery#restore-from-raw-object-store-no-backup-cr):
a new Cluster bootstraps from the bucket directly, without a `Backup` CR.

## Fields

| Field | Purpose |
|-------|---------|
| `bucket` | Destination bucket. Required. |
| `path` | Key prefix inside the bucket. Optional. |
| `endpoint` | S3-compatible endpoint. Empty means AWS S3. |
| `region` | Signing and regional endpoint region. Defaults internally where needed. |
| `forcePathStyle` | Path-style addressing for MinIO/Ceph-style stores. |
| `signatureVersion` | `s3v4` by default, `s3v2` for legacy providers. |
| `serverSideEncryption` | Provider SSE setting, such as `AES256` or `aws:kms`. |
| `storageClass` | Provider storage class. |
| `credentials` | Static Secret references or IAM inheritance. |
| `tls` | Endpoint TLS verification settings. |

## TLS settings

Use a custom CA bundle for private endpoints:

```yaml
tls:
  caBundleSecret:
    name: objectstore-ca
    key: ca.crt
```

For local testing only:

```yaml
tls:
  insecureSkipVerify: true
```

Do not use insecure TLS settings in production.

## Object layout

Physical backups:

```text
<path>/<cluster>/<backup-name>/<backup-id>/backup.xbstream
<path>/<cluster>/<backup-name>/<backup-id>/metadata.json
```

Continuous binlog archive:

```text
<path>/<cluster>/binlogs/<server-uuid>/<binlog-file>
<path>/<cluster>/binlogs/<server-uuid>/<binlog-file>.json
<path>/<cluster>/binlogs/<server-uuid>/_archive_status.json
<path>/<cluster>/binlogs/_index.json
```

The `path` value should be unique per environment. cloudnative-mysql checks for existing
cluster archive prefixes in some recovery/archive paths to avoid adopting an
unrelated destination.

## Credential placement

One-shot backup Jobs receive object-store credentials only for the duration of
the Job.

Recovery init containers receive read access so they can download base backups
and binlogs.

Continuous archiving writes from the primary instance manager, so instance Pods
need object-store write credentials when archiving is enabled.

## Integrity

cloudnative-mysql records SHA256 checksums in metadata. S3 ETag is not treated as the
integrity source of truth because multipart uploads and provider behavior make
ETag semantics inconsistent.

Recovery verifies checksums before trusting downloaded backup data.

## Provider notes

- MinIO: set `endpoint`, use `forcePathStyle: true`, and usually set
  `region: us-east-1`.
- AWS S3: leave `endpoint` empty, set the real region, and use IAM inheritance
  where possible.
- Legacy S3-compatible providers: set `signatureVersion: s3v2` only when v4 is
  unsupported.
- Private CAs: use `tls.caBundleSecret`.
