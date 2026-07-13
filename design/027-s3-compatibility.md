# 027 — S3 Compatibility

- **Status:** done
- **Milestone:** 0.7.0
- **Issue:** [#59](https://github.com/cnmsql/cnmsql/issues/59)
- **Supersedes:** none

## 1. Summary

The backup, archiving, recovery and retention paths reach the object store
through one client (`pkg/management/mysql/objectstore`). That client was written
against AWS S3 and MinIO, and three problems followed from it.

First, parts of the `S3ObjectStore` API were accepted and then ignored.
`serverSideEncryption`, `storageClass` and the whole `tls` block were documented,
defaulted, and plumbed nowhere: a user who set them got no encryption, no storage
class and no CA bundle, with nothing in the logs to say so.

Second, the client made AWS-shaped assumptions about responses. A not-found was
recognized only by the literal error code `NoSuchKey`, and listing was always
ListObjectsV2. A provider that answers differently turns a routine miss into a
hard failure, or cannot be listed at all.

Third, there was no way to qualify a provider short of pointing a production
cluster at it and finding out.

This design closes all three without adding an API field. The client adapts to
the store instead of asking the user to describe it.

## 2. What cnmsql requires of a store

The operations the operator performs, and nothing more:

| Operation | Used by |
|---|---|
| `PutObject`, incl. multipart of unknown length | Base backups (streamed from xtrabackup/mariabackup), binlog archiving |
| `GetObject` | Recovery, PITR replay, every manifest read |
| `HeadObject` | Existence probes (archive guards, recovery preflight) |
| `ListObjectsV2` (V1 fallback) | Retention, archive walks, the empty-prefix guard |
| `DeleteObject`, single key | Retention |

Four things are deliberately not used, because compatible stores omit or restrict
them. Bulk delete (`POST ?delete`) is the operation most commonly missing, so
`RemovePrefix` expands to per-object deletes. Prefix delete does not exist on any
provider, so it was never something to depend on. Lifecycle, versioning, tagging
and ACL calls are unused because cnmsql drives retention itself, which keeps
behaviour independent of how the bucket is configured. ETag is not treated as a
checksum, since multipart ETags are not content hashes and vary per provider;
integrity stays what it already was, a SHA256 cnmsql records in its own metadata.

That list is the compatibility contract. It is what the conformance suite (§5)
checks and what the docs publish.

## 3. Adapting to the store

### 3.1 Not-found is a status code, not a string

`Remove` and `Exists` matched `minio.ToErrorResponse(err).Code == "NoSuchKey"`. A
`HEAD` on a missing key has no response body to carry that code, and several
stores return a 404 with a different code or none at all. Both now go through
`isNotFound`, which keys off HTTP 404 and treats the error codes as a secondary
signal.

This is the failure mode issue #59 describes. A benign miss reported as an error
stalls a retention pass, and retention that never completes is a bucket that
grows without bound.

`ToErrorResponse` only type-asserts, so it misses an error this package has
wrapped for context. The predicates use `errors.As` instead.

### 3.2 Listing falls back to V1

The GCS XML interop endpoint implements only the original listing API and answers
`list-type=2` with a 400. `Client.listObjects` retries such a rejection under V1
and latches onto it (`atomic.Bool`), so the wasted request is paid once per
process rather than once per call. A 403 is explicitly not treated as
"unsupported": that is a credentials problem, and retrying it under V1 would only
hide it.

### 3.3 Region defaults per endpoint

An empty region signed as `us-east-1`, which Cloudflare R2 rejects. R2 accepts
only `auto`. `signingRegion` returns `auto` for `*.r2.cloudflarestorage.com` and
keeps `us-east-1` (which MinIO, Ceph and friends accept) everywhere else. An
explicit `spec.region` always wins.

### 3.4 Credentials use the real chain

`inheritFromIAMRole` mapped to `credentials.NewIAM("")`, which reaches only the
EC2/ECS metadata endpoint. IRSA, the way credentials actually arrive in a pod on
EKS, therefore did not work. It is now a chain: `EnvAWS`, then
`FileAWSCredentials`, then `IAM`, whose provider handles the web-identity token
file.

## 4. Wiring the dead API fields

`serverSideEncryption`, `storageClass` and `tls` are now read.

SSE and storage class become `PutObjectOptions` on every upload, built once at
client construction. `parseServerSideEncryption` maps `AES256` and `aws:s3` to
SSE-S3 and `aws:kms[:key-id]` to SSE-KMS, and rejects anything else rather than
sending an opaque header the provider answers with a confusing 400.

TLS builds a custom transport only when it has to. A CA bundle installs a root
pool (the system pool plus the PEM); `insecureSkipVerify` disables verification.
A bundle with no parseable certificate fails client construction instead of
silently falling back to the system roots. The bundle travels as PEM in an env
var sourced from the referenced secret key, which matches how credentials already
reach the workers, so there is no new volume mount and the operator's own client
resolves the same secret.

`Cluster.Validate` rejects an unsupported `serverSideEncryption`, and rejects
`insecureSkipVerify` combined with `caBundleSecret` (the bundle would be ignored).
A typo is then caught at admission rather than in a backup pod hours later.
`ConfigFromStore` takes a `StoreSecrets` struct now, because the parameter list
had outgrown positional strings.

## 5. Qualifying a provider

`test/conformance/s3` (build tag `conformance`, `make test-s3-conformance`) runs
the §2 contract against a real endpoint, configured from the same `cnmsql_S3_*`
environment the backup workers receive. Everything it writes goes under a unique
prefix that it removes on the way out, so it can be pointed at a real empty
bucket.

Each operation is its own subtest, so a provider that fails one says exactly
which cnmsql feature it cannot support. A `RemovePrefix` failure means retention
cannot expire backups there. A multipart failure means base backups cannot be
written at all.

MinIO and SeaweedFS both pass in full; SeaweedFS was chosen precisely because it
is an independent implementation rather than another MinIO. AWS S3, Ceph RGW, R2,
B2 and GCS are documented as expected to work, which is consistent with their
published API surface but not yet observed. The suite is how that changes. There
is no CI lane for them, since qualifying a hosted provider needs credentials CI
does not have.

## 6. Non-goals

A provider preset field (`provider: r2|b2|gcs|...`) was considered and dropped.
The two things a preset would set, region and listing version, are both detected,
and a preset is an API field that can go stale as providers change.

Compat escape hatches (`listObjectsVersion`, `disableChecksums`, `partSize`) were
also dropped. None has a known use once the above is in place, and offering them
invites configuration nobody can justify later.

A CI lane against hosted providers is out of scope: it needs credentials, costs
money, and produces a signal the conformance suite already gives on demand.
