---
title: "Instance Images and Versions"
description: "Supported Percona versions, custom slim image layout, build matrix, and version-specific behavior."
sidebar_position: 4
---

# Instance images and versions

cnmsql runs Percona Server for MySQL, not Oracle MySQL. The database Pods use a
custom cnmsql instance image that contains Percona Server, Percona XtraBackup,
and only the runtime tools needed by the operator. The cnmsql instance manager
is copied into each Pod from the operator image when the Pod starts.

This mirrors the CloudNativePG model: the operator controls the database image
and runs its own instance manager in it, instead of relying directly on
upstream database images.

:::note MariaDB
This page describes the MySQL images. To run MariaDB instead, set
`spec.flavor: mariadb` and use the MariaDB instance image. The selection
mechanics are the same; the versions and image name differ. See
[MariaDB Flavor](mariadb.md).
:::

## Supported majors

The current version matrix is:

| Major | Base image | Percona Server repo | XtraBackup repo | Notes |
|-------|------------|---------------------|-----------------|-------|
| 8.0 | `debian:bookworm-slim` | `ps-80` | `pxb-80` | Main modern line. |
| 8.4 | `debian:bookworm-slim` | `ps-84-lts` | `pxb-84-lts` | LTS line. |
| 9.x | `debian:bookworm-slim` | `ps-9x-innovation` | `pxb-9x-innovation` | Currently tracks Percona Server for MySQL 9.6. Built from Percona testing packages. |

## Where the images come from

The instance images are built and published from the separate
[`containers`](https://github.com/cnmsql/containers) repo, not from this
operator repo. Every image is pinned there to exact Percona Server and
XtraBackup package versions on a digest-pinned Debian base; Renovate proposes
a bump whenever Percona ships a release, and each image is smoke tested
(initialize, physical backup, prepare, restore) on amd64 and arm64 before it is
published. See the repo's
[supply chain design](https://github.com/cnmsql/containers/blob/main/design/001-image-supply-chain.md).

Tags, for Percona Server 8.4.11 built at 2026-10-01 12:00 UTC on bookworm:

| Tag | Moves? | Points to |
|---|---|---|
| `8.4.11-202610011200-bookworm` | never | this build |
| `8.4.11-bookworm`, `8.4.11` | yes | newest build of 8.4.11 |
| `8.4-bookworm`, `8.4` | yes | newest build of the newest 8.4 patch |

Older images carry `<major>-<patch>` tags (`8.4-5`); they stay published.

### Published catalogs

The containers repo publishes a `ClusterImageCatalog` per flavor and distro,
regenerated after every release, that pins each series to its newest image by
immutable tag and digest. It is the recommended way to pick images:

```bash
kubectl apply -f https://raw.githubusercontent.com/cnmsql/containers/main/catalogs/catalog-mysql-bookworm.yaml
```

```yaml
spec:
  imageCatalogRef:
    apiGroup: mysql.cnmsql.co
    kind: ClusterImageCatalog
    name: cnmsql-mysql-bookworm
    series: "8.4"
```

Applying a newer revision of the catalog is a patch upgrade: the operator
probes the new image and rolls the instances onto it.

### Verifying images

Every image is signed with [cosign](https://github.com/sigstore/cosign)
(keyless, by the containers repo's build workflow) and carries an SPDX SBOM and
SLSA provenance:

```bash
cosign verify ghcr.io/cnmsql/cnmsql-instance:8.4 \
  --certificate-identity-regexp '^https://github.com/cnmsql/containers/\.github/workflows/build\.yml@' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

An admission policy (Kyverno, sigstore policy-controller) can enforce the same
identity on every instance Pod.

## Cluster image selection

Direct image selection:

```yaml
spec:
  imageName: ghcr.io/cnmsql/cnmsql-instance:8.4
```

Catalog-based selection:

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
---
apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: cluster-sample
spec:
  imageCatalogRef:
    apiGroup: mysql.cnmsql.co
    kind: ImageCatalog
    name: percona-images
    series: "8.4"
```

Use an explicit image or catalog in production. The development fallback image
exists for local workflows only.

## How the operator learns the server version

The operator does not read the server version from the image tag. When a
cluster resolves to an image it has not used yet, the operator runs that image
once in a short-lived probe Pod (`<cluster>-image-<hash>`) that reports what its
`mysqld` binary says: the flavor and the exact server version. The probe uses
the cluster's pull secrets, pull policy, node selector, node affinity and
tolerations. The result is recorded in `status.targetImage` and shown in the
`VERSION` column of `kubectl get mysql`.

Before any instance moves to the new image, the operator checks that:

- its flavor is the cluster's;
- its series is the one the catalog entry names (or the image tag, when the
  tag starts with a series);
- going from the current image to it is a supported upgrade (see
  [MySQL Version Upgrades](major-version-upgrade.md)).

While the probe runs, or when the image is rejected (it cannot be pulled, or a
check fails), the cluster keeps running on its current image and the
`ImageReady` condition says why. A new cluster waits for its first probe.

Any image reference works, including a digest-only one
(`ghcr.io/cnmsql/cnmsql-instance@sha256:…`). Inside each Pod, the instance
manager also reads the version from `mysqld --version` itself.

## Runtime user and filesystem

The custom instance image is rootless. It runs as UID `1001` and keeps
database/runtime paths group-writable for Kubernetes environments that assign a
random compatible group.

The image keeps:

- `mysqld` and version-specific initialization tools;
- `mysql`, `mysqladmin`, and `mysqlbinlog`;
- `mysqldump`, for [logical backups](logical-backups.md);
- XtraBackup and `xbstream`.

The cnmsql `manager` binary is not in the image: the operator copies it into
each Pod at startup.

Images published before logical backup support strip `mysqldump` (and
`mariadb-dump` on MariaDB). Logical backups fail on them with
`LogicalToolUnavailable`. The first tags that ship the dump tool are:

| Image | First tag with the dump tool |
|---|---|
| `ghcr.io/cnmsql/cnmsql-instance` | `8.0-5`, `8.4-5`, `9.x-5` |
| `ghcr.io/cnmsql/cnmsql-mariadb-instance` | `10.11-4`, `11.4-4`, `11.8-4`, `12.3-4` |

The moving series tags (`8.4`, `11.4`, …) already point at them.

The image trims documentation, debug binaries, test fixtures, unused client
utilities, static libraries, and similar non-runtime payloads.

## Version-aware behavior

The manager renders configuration and SQL differently by server version. Examples:

- 8.0.23 and later use `SOURCE`/`REPLICA` terminology where supported.
- `GET_SOURCE_PUBLIC_KEY` style options are omitted on versions that do not
  support them.
- `super_read_only`, semi-sync plugin names, bootstrap SQL, and privilege grants
  are gated by server capability.

This version-aware layer is why cnmsql builds and tests the full matrix instead
of assuming all supported Percona majors behave the same way.

## Backup and restore compatibility

Backup workers and restore init containers use the same cnmsql instance image
as the Cluster. This keeps XtraBackup version-aligned with the server version
and avoids moving backup payloads through the controller-manager process.

When restoring, choose an image compatible with the source backup. Cross-major
restore is not supported. In-place **major** upgrades follow the supported series
chain (`8.0 → 8.4 → 9.0`); see [MySQL Version Upgrades](major-version-upgrade.md).

## Known limits

- Percona Server 5.6 is not supported.
- Percona 9.x packaging is currently taken from Percona's testing channel.
- Native Oracle MySQL images are not supported.
- Clone-plugin provisioning is deferred; replica provisioning is XtraBackup-first.
