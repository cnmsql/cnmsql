---
title: "Instance Images and Versions"
description: "Supported Percona versions, custom slim image layout, build matrix, and version-specific behavior."
sidebar_position: 4
---

# Instance images and versions

cloudnative-mysql runs Percona Server for MySQL, not Oracle MySQL. The database Pods use a
custom cloudnative-mysql instance image that contains Percona Server, Percona XtraBackup,
the cloudnative-mysql manager binary, and only the runtime tools needed by the operator.

This mirrors the CloudNativePG model: the operator controls a database image
with the instance manager built in instead of relying directly on upstream
database images.

## Supported majors

The current version matrix is:

| Major | Base image | Percona Server repo | XtraBackup repo | Notes |
|-------|------------|---------------------|-----------------|-------|
| 8.0 | `debian:bookworm-slim` | `ps-80` | `pxb-80` | Main modern line. |
| 8.4 | `debian:bookworm-slim` | `ps-84-lts` | `pxb-84-lts` | LTS line. |
| 9.x | `debian:bookworm-slim` | `ps-9x-innovation` | `pxb-9x-innovation` | Currently built from Percona testing packages. |

The source of truth for build arguments is `images/versions.json`.

## Build commands

Build one major:

```bash
make docker-build-instance INSTANCE_VERSION=8.4
```

Build every configured major:

```bash
make docker-build-instance
```

Push images:

```bash
make docker-push-instance INSTANCE_REGISTRY=registry.example.com/cloudnative-mysql-instance
```

The generated tags are intended to be selected directly in `Cluster.spec.imageName`
or through an `ImageCatalog`.

## Cluster image selection

Direct image selection:

```yaml
spec:
  imageName: cloudnative-mysql-instance:8.4
```

Catalog-based selection:

```yaml
apiVersion: mysql.cloudnative-mysql.io/v1alpha1
kind: ImageCatalog
metadata:
  name: percona-images
spec:
  images:
    - major: 8
      image: cloudnative-mysql-instance:8.4
---
apiVersion: mysql.cloudnative-mysql.io/v1alpha1
kind: Cluster
metadata:
  name: cluster-sample
spec:
  imageCatalogRef:
    apiGroup: mysql.cloudnative-mysql.io
    kind: ImageCatalog
    name: percona-images
    major: 8
```

Use an explicit image or catalog in production. The development fallback image
exists for local workflows only.

## Runtime user and filesystem

The custom instance image is rootless. It runs as UID `1001` and keeps
database/runtime paths group-writable for Kubernetes environments that assign a
random compatible group.

The image keeps:

- `mysqld` and version-specific initialization tools;
- `mysql`, `mysqladmin`, and `mysqlbinlog`;
- XtraBackup and `xbstream`;
- the cloudnative-mysql `manager` binary.

The image trims documentation, debug binaries, test fixtures, unused client
utilities, static libraries, and similar non-runtime payloads.

## Version-aware behavior

The manager renders configuration and SQL differently by server version. Examples:

- 8.0.23 and later use `SOURCE`/`REPLICA` terminology where supported.
- `GET_SOURCE_PUBLIC_KEY` style options are omitted on versions that do not
  support them.
- `super_read_only`, semi-sync plugin names, bootstrap SQL, and privilege grants
  are gated by server capability.

This version-aware layer is why cloudnative-mysql builds and tests the full matrix instead
of assuming all supported Percona majors behave the same way.

## Backup and restore compatibility

Backup workers and restore init containers use the same cloudnative-mysql instance image
as the Cluster. This keeps XtraBackup version-aligned with the server version
and avoids moving backup payloads through the controller-manager process.

When restoring, choose an image compatible with the source backup. Cross-major
restore and in-place major upgrade are not supported by the current milestone
set.

## Known limits

- Percona Server 5.6 is not supported.
- Percona 9.x packaging is currently taken from Percona's testing channel.
- Publishing and signing release images is outside the current implementation.
- Native Oracle MySQL images are not supported.
- Clone-plugin provisioning is deferred; replica provisioning is XtraBackup-first.
