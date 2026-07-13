---
title: "Object Store Configuration"
description: "S3-compatible configuration for backups, continuous archiving, PITR, and recovery."
sidebar_position: 13
---

# Object store configuration

cnmsql uses an S3-compatible object store for physical backups and continuous
binlog archiving. The same `S3ObjectStore` API is used by one-shot Backups,
ScheduledBackup-generated Backups, recovery, and PITR.

## Minimal MinIO-style configuration

```yaml
spec:
  backup:
    objectStore:
      bucket: cnmsql-backups
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
      bucket: cnmsql-prod-backups
      path: clusters
      region: eu-west-1
      forcePathStyle: false
      credentials:
        inheritFromIAMRole: true
```

When `endpoint` is empty, cnmsql targets AWS S3. With no static credentials
configured, workers fall back to the ambient AWS credential chain, in order: the
`AWS_*` environment variables, the shared credentials file, and then the instance
metadata endpoint. That last step covers both an EC2 instance profile and an IRSA
web-identity token projected into the pod.

## Per-backup override

A `Backup` can override the Cluster object store:

```yaml
apiVersion: mysql.cnmsql.co/v1alpha1
kind: Backup
metadata:
  name: backup-to-dr-bucket
spec:
  cluster:
    name: cluster-sample
  objectStore:
    bucket: cnmsql-dr-backups
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
| `region` | Signing and regional endpoint region. When empty, cnmsql signs with `auto` against Cloudflare R2 and `us-east-1` elsewhere. |
| `forcePathStyle` | Path-style addressing for MinIO/Ceph-style stores. |
| `signatureVersion` | `s3v4` by default, `s3v2` for legacy providers. |
| `serverSideEncryption` | SSE header on every upload: `AES256`, `aws:kms`, or `aws:kms:<key-id>`. Leave unset outside AWS. |
| `storageClass` | Storage class of every upload, e.g. `STANDARD_IA`. Leave unset on providers with a single class. |
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

The `path` value should be unique per environment. cnmsql checks for existing
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

cnmsql records SHA256 checksums in metadata. S3 ETag is not treated as the
integrity source of truth because multipart uploads and provider behavior make
ETag semantics inconsistent.

Recovery verifies checksums before trusting downloaded backup data.

## S3 compatibility

cnmsql restricts itself to the part of the S3 API that compatible stores
implement consistently. The whole of it is `PutObject` (including multipart
uploads of unknown length, which is how a streamed base backup is written),
`GetObject`, `HeadObject`, `ListObjectsV2` (with a fallback to the original
listing API, see below), and `DeleteObject` on a single key.

The operations providers most often omit or restrict are not used at all:

- No bulk delete. Expiring a base backup deletes its objects one request at a
  time rather than through the `POST ?delete` multi-object API. Retention is
  slower on a large archive, but it works everywhere.
- No prefix delete. No provider offers one, so "delete this backup directory" is
  always a list followed by per-object deletes.
- No bucket lifecycle, versioning, tagging or ACL calls. cnmsql drives retention
  itself, so a bucket with no lifecycle policy configured behaves exactly as
  documented here.
- No ETag-based integrity. ETag semantics differ across providers for multipart
  objects, so cnmsql verifies against SHA256 checksums it records in its own
  metadata.

Where a missing object is a legitimate outcome, it is reported as a miss rather
than an error. That covers a retention pass re-running after an interruption and
a probe for an archive that does not exist yet. Providers differ in how they
spell a 404, so cnmsql keys off the HTTP status rather than the error code
string.

### Listing API fallback

Modern stores speak `ListObjectsV2`, but a few endpoints implement only the
original listing API and reject V2 with a 400. The Google Cloud Storage XML
interop API is the one you are most likely to meet. cnmsql detects the rejection
and falls back to V1 listing for the rest of the process's life. You do not need
to configure anything.

### Provider matrix

| Provider | Status | Notes |
|---|---|---|
| MinIO | Verified | `forcePathStyle: true`. Region defaults to `us-east-1`. Exercised by the e2e suite on every CI run. |
| SeaweedFS | Verified | `forcePathStyle: true`. Requires an `s3.config` identity with `Admin`/`Read`/`Write`/`List`. |
| AWS S3 | Expected to work | Leave `endpoint` empty, set the real `region`, prefer `credentials.inheritFromIAMRole` with IRSA. The only provider where `serverSideEncryption` and `storageClass` are broadly meaningful. |
| Ceph RGW | Expected to work | `forcePathStyle: true`. |
| Cloudflare R2 | Expected to work | Leave `region` empty (cnmsql signs with `auto`, which is the only region R2 accepts). Do not set `serverSideEncryption`: R2 encrypts at rest unconditionally and rejects the header. |
| Backblaze B2 | Expected to work | Use the S3-compatible endpoint for your bucket's region. Do not set `storageClass`. |
| GCS (XML interop) | Expected to work | Uses the V1 listing fallback described above. Requires HMAC interoperability keys, not a service-account JSON. |

"Verified" means the conformance suite below passes against it. The rest match
each provider's documented API surface but nobody has run the suite against them.
If you do, the output says exactly which operations failed, and we would like to
hear about it.

### Qualifying a provider

Before trusting a store with your backups, run the object-store conformance suite
against it. It performs the same operations cnmsql's backup, archiving, recovery
and retention paths perform. Everything it writes goes under a unique prefix that
it removes on the way out, so it is safe to point at a real (empty) bucket:

```bash
export cnmsql_S3_ENDPOINT=https://s3.example.com
export cnmsql_S3_BUCKET=cnmsql-conformance
export cnmsql_S3_ACCESS_KEY_ID=... cnmsql_S3_SECRET_ACCESS_KEY=...
export cnmsql_S3_FORCE_PATH_STYLE=true   # omit for AWS S3

make test-s3-conformance
```

Each operation is a separate subtest, so a provider that fails one tells you
precisely which cnmsql feature it cannot support:

```text
--- PASS: TestConformance/Upload_of_unknown_length_(multipart)
--- PASS: TestConformance/Remove_is_idempotent
--- FAIL: TestConformance/RemovePrefix_empties_the_prefix
```

A failure in `RemovePrefix` or `Remove_is_idempotent` means retention will not
be able to expire backups on that provider. A failure in the multipart upload
means base backups cannot be written at all.

Two settings exist for stores that predate the current API: set
`signatureVersion: s3v2` only when the provider cannot do v4 signing, and point
`tls.caBundleSecret` at a PEM when the endpoint is fronted by a private CA.
