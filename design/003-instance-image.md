# 003 — Custom Slim Instance Image

**Status:** done
**Milestone:** M2.5

Build a custom slim Percona Server image from a minimal Debian base + Percona APT repositories, carrying only `mysqld`, init tooling, XtraBackup, and the `manager` binary — replacing the fat upstream `percona/percona-server` image. Rootless (uid 1001), all four versions (5.6, 8.0, 8.4, 9.x) build and pass integration tests.

## Overview

Stop shipping the operator's instance on top of the fat upstream `percona/percona-server` image. Instead build a slim image from a minimal Debian base + Percona's APT repositories, carrying only what the instance manager needs at runtime: `mysqld`, the init tooling, XtraBackup, and the `manager` binary — "nothing more". This mirrors CNPG's approach of maintaining its own slim images instead of consuming upstream `postgres`.

### Outcome

- Built rootless (uid 1001); all four versions build.
- 9.x: published only in Percona's testing channel (server 9.6.0, `percona-xtrabackup-96`); added a `REPO_COMPONENT` build arg. initdb/run/join all pass on 9.6.0.
- 5.6: builds on `debian:buster-slim` after rewriting apt to `archive.debian.org` (buster is EOL). initdb/run pass.
- 5.6 `join`: un-skipped and passing. The XtraBackup/glibc blocker resolved (2.4 runs on buster); un-skipping surfaced and fixed three manager bugs — `--skip-slave-start` gating, `GET_MASTER_PUBLIC_KEY` omitted on 5.6 (8.0.4+), and `ProvisionFromBackup` made configure-only.
- Integration harness builds images via host `docker build`; `replication_integration_test.go` kept on the upstream image as a neutral mysqld fixture.

## Design

### Grounded Facts (Verified Against repo.percona.com)

| Version | PS repo keyword | XtraBackup repo keyword | Base image |
|---------|----------------|---|-----------------------|
| 5.6 | `ps-56` | `pxb-24` | `debian:buster-slim` |
| 8.0 | `ps-80` | `pxb-80` | `debian:bookworm-slim` |
| 8.4 | `ps-84-lts` | `pxb-84-lts` | `debian:bookworm-slim` |
| 9.x | `ps-9x-innovation` | `pxb-9x-innovation` | `debian:bookworm-slim` |

- `ps-56` apt dists publish only `stretch`/`buster` (Debian) + `xenial`/`bionic`/`focal` (Ubuntu) — **no bookworm**, hence buster for 5.6.
- `ps-9x-innovation` publishes `bullseye`/`bookworm`/`trixie` — bookworm is safe.
- Repo keywords confirmed from `percona/percona-repositories` `scripts/percona-release.sh`.

### Parameterized Dockerfile (`Dockerfile.instance`)

Multi-stage, driven entirely by build args so one file covers all versions:

```
ARG GO_VERSION=1.57
ARG BASE_IMAGE=debian:bookworm-slim
ARG PS_REPO=ps-80
ARG PXB_REPO=pxb-80
ARG PXB_PACKAGE=percona-xtrabackup-80   # pxb-24 → percona-xtrabackup-24, etc.

FROM golang:${GO_VERSION} AS builder
# build static ./cmd/manager → /out/manager

FROM ${BASE_IMAGE}
ARG PS_REPO PXB_REPO PXB_PACKAGE
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl ca-certificates gnupg2 lsb-release; \
    curl -fsSLo /tmp/percona-release.deb https://repo.percona.com/apt/percona-release_latest.$(lsb_release -sc)_all.deb; \
    apt-get install -y --no-install-recommends /tmp/percona-release.deb; \
    percona-release enable-only ${PS_REPO} release --scheme https; \
    percona-release enable ${PXB_REPO} release --scheme https; \
    apt-get update; \
    apt-get install -y --no-install-recommends percona-server-server ${PXB_PACKAGE}; \
    # trim: docs, man, mysql-test, *-debug, headers, static libs, apt lists
    rm -rf /usr/share/doc /usr/share/man /usr/share/info \
           /usr/lib/mysql-test /usr/share/mysql*/mysql-test \
           /usr/sbin/mysqld-debug /usr/lib/mysql/plugin/debug \
           /var/lib/apt/lists/* /tmp/percona-release.deb; \
    apt-get purge -y curl gnupg2 lsb-release; apt-get autoremove -y; \
    rm -rf /var/lib/mysql/* ;        # ship empty datadir; manager runs initdb

COPY --from=builder /out/manager /usr/local/bin/manager
ENTRYPOINT ["/usr/local/bin/manager"]
CMD ["instance", "run"]
```

- **No `percona-server-client`** — the manager talks to mysqld via the Go driver (`pkg/management/mysql/pool`) and the admin interface; XtraBackup is standalone. The `mysql` CLI is not a runtime dependency.
- Init tooling: `percona-server-server` ships `mysqld --initialize-insecure` (8.0+) and `mysql_install_db` (5.6) — both already driven version-aware by the manager.
- XtraBackup comes from its own package (not COPY-from-image), so apt pulls the right shared libs.

### Version Matrix Wiring

Single source of truth mapping version → build args in `images/versions.json`, consumed by both the Makefile target and integration tests:

```json
[
  {"version":"5.6","base":"debian:buster-slim","ps":"ps-56","pxb":"pxb-24","pxbPkg":"percona-xtrabackup-24","serverVersion":"5.6.51"},
  {"version":"8.0","base":"debian:bookworm-slim","ps":"ps-80","pxb":"pxb-80","pxbPkg":"percona-xtrabackup-80","serverVersion":"8.0.36"},
  {"version":"8.4","base":"debian:bookworm-slim","ps":"ps-84-lts","pxb":"pxb-84-lts","pxbPkg":"percona-xtrabackup-84","serverVersion":"8.4.0"},
  {"version":"9.x","base":"debian:bookworm-slim","ps":"ps-9x-innovation","pxb":"pxb-9x-innovation","pxbPkg":"percona-xtrabackup-9x-innovation","serverVersion":"9.x"}
]
```

Makefile targets: `docker-build-instance VERSION=8.0` and `docker-build-instance-all`.

### Tests

- Rewrite `flavors_test.go`: drop `perconaImage`/`xtrabackupImage`/`modernXtrabackup`; each flavor now carries `base`/`ps`/`pxb`/`pxbPkg`. The `dockerfile()` / `buildInstanceContext()` helpers build the new parameterized `Dockerfile.instance` with the flavor's build args.
- `replication_integration_test.go` switches to building/using the slim 8.0 image.
- Re-evaluate the 5.6 `join` skip: XtraBackup 2.4 needs glibc newer than EOL CentOS 5.6; on `debian:buster-slim` (glibc 2.28) `pxb-24` may work — attempt to un-skip.
- Expect slower integration builds (apt install per image). Mitigate with prebuilt-per-version image cached across subtests and BuildKit layer cache in CI.

### Image-Size Acceptance Check

A cheap assertion/inspection comparing the slim image against `percona/percona-server:<v>`. Target: meaningfully smaller, client/test/docs/debug stripped.

### CI

Update `.github/workflows/test-integration.yml` to build our images. Optionally add a `build-instance-images` job/workflow that builds (and later pushes) the four images.

## Implementation Notes

1. Rewrite `Dockerfile.instance` (parameterized) + add `images/versions.json`.
2. Build 8.0 locally, boot it via the initdb integration test → prove trim is safe.
3. Repeat for 8.4, then 9.x (pin strings), then 5.6 (buster).
4. Port `flavors_test.go` + `replication_integration_test.go` to the new builder.
5. Re-attempt 5.6 `join`; record outcome.
6. Makefile targets + CI workflow update.

## Decisions

- **Build strategy:** Debian slim base + Percona APT repo (`percona-release`), `apt-get --no-install-recommends`, aggressive trim. Not binary-COPY distroless, not Oracle Linux.
- **Version scope:** all four — 5.6, 8.0, 8.4, 9.x.
- **Out of scope:** publishing/signing images to a registry; 9.x integration run (blocked if no testable server release); non-amd64 arches (buildx matrix).

## Risks

- **5.6 on buster:** buster is EOL; `debian:buster-slim` still pullable but apt may need archive mirrors. Fallback: `ubuntu:focal` (also carries `ps-56`).
- **9.x package name** (`pxbPkg`, exact `percona-server-server` version) — pin on first real build.
- **Build time** in CI grows; addressed by caching.
- Trimming too aggressively could remove a charset/errmsg file `mysqld` needs — validate by booting each trimmed image in existing initdb/run integration tests.
