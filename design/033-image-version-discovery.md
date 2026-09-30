# 033 — Image version discovery

Status: accepted (2026-09-29)

Companion to the containers repo's `design/001-image-supply-chain.md`, which
pins every image, puts the exact server version in the tags and labels, and
publishes signed `ClusterImageCatalog`s.

## Problem

The operator decides which server version an image contains from its tag, and
only from its tag (`resolveServerVersion`, `internal/controller/cluster_plan.go`):

- a fixed table maps series tags to a guessed patch (`8.4` → `8.4.0`,
  `9.x` → `9.6.0`, MariaDB `11.4` → `11.4.3`) whatever the image really
  carries;
- any other tag that parses becomes the version as a raw string (`8.4-5`),
  which then ends up in `MYSQL_VERSION`, the data-dir version marker and the
  GR protocol target;
- a digest-only reference (`repo@sha256:…`) or a tag like `latest` blocks the
  cluster;
- the catalog's `series` is checked by admission but never compared with the
  image it points at, so a catalog entry `series: "8.4"` with a 9.x image
  rolls the cluster onto 9.x;
- nothing watches catalogs: an edit lands on the next resync.

The guess matters: my.cnf rendering, the SQL dialect, the upgrade guard, the
dump metadata and major-upgrade detection all key off it, and patch-level gates
(8.0.22, 8.0.23, 8.0.26 …) evaluate against the table, not the binary.

## Decisions

| # | Decision | Why |
|---|---|---|
| V1 | The instance manager detects the server version from the binary (`mysqld --version` / `mariadbd --version`) in every subcommand. `--server-version` stays only as an explicit override; `MYSQL_VERSION` and `--server-version=$(MYSQL_VERSION)` leave the Pod and Job specs | The binary is authoritative and always at hand. With nothing version-derived in the Pod spec, what the operator knows about an image can never roll Pods |
| V2 | Before the operator uses an image it **probes** it: a short-lived Pod runs `manager instance probe` in that image and reports the flavor, the server version, and the image digest the kubelet pulled | Works for any reference (tag, digest, mirror, private registry with the cluster's pull secrets) without the operator talking to registries |
| V3 | The result is recorded in `status.targetImage` (`image`, `imageID`, `flavor`, `serverVersion`); a new probe runs only when the resolved image reference changes | One probe per image change; `kubectl get` shows the real server version |
| V4 | A probed image is validated before anything rolls: its flavor must be the cluster's; its series must be the catalog entry's `series` (or the tag's, when the tag names one); the move from the previous `targetImage` must be a supported upgrade | Catches a mis-mapped catalog, a wrong digest and a skipped series, which admission cannot see |
| V5 | While a new image is being probed, or when it is rejected, the cluster keeps running and being reconciled on its previous `targetImage`; the `ImageReady` condition says why. A cluster with no previous image waits (provisioning) or blocks (rejected) | A bad or unpullable image must not stop failover and switchover handling of a healthy cluster |
| V6 | The operator watches `ImageCatalog` and `ClusterImageCatalog` and requeues the clusters that reference them | A published catalog revision rolls promptly |
| V7 | The fixed version tables (`Engine.DefaultServerVersion`) and `resolveServerVersion` are removed | Nothing guesses any more |

## Probe

`<cluster>-image-<hash>` (hash of the image reference), owned by the Cluster,
labelled `mysql.cnmsql.co/image-probe=<cluster>` (and deliberately not with the
cluster label, which selects instance Pods):

- init container `bootstrap-controller` (operator image) copies the manager
  binary into an emptyDir, exactly as instance Pods do;
- container `probe` runs the probed image with `/controller/manager instance
  probe`, which runs `mysqld --version` (the MariaDB images ship `mysqld` as a
  compat name) and writes `{"flavor":…,"serverVersion":…,"versionString":…}`
  to its termination message;
- the cluster's pull policy and pull secrets, node selector, affinity and
  tolerations (so it runs on the same architecture and can pull the same
  images), its security contexts, `automountServiceAccountToken: false`,
  `restartPolicy: Never`, tiny resource requests, a 5-minute deadline.

The operator reads the termination message and `containerStatuses[].imageID`
when the Pod succeeds, records the result, and deletes the Pod. A Pod stuck on
`ErrImagePull` / `ImagePullBackOff` / `InvalidImageName`, or failed, is
reported in the `ImageReady` condition; the Pod is recreated when the image
reference changes.

## Where the version comes from now

| Consumer | Before | After |
|---|---|---|
| my.cnf rendering, removed-parameter warnings, import checks, GR protocol target, major-upgrade detection (operator) | tag table / raw tag | `status.targetImage.serverVersion` (probed) |
| SQL dialect, data-dir marker and upgrade guard, dump metadata, bootstrap SQL (instance) | `--server-version=$(MYSQL_VERSION)` | detected from the binary |
| admission (series transitions) | catalog `series`, else tag | unchanged: catalog `series`, else tag; digest-only references are checked by V4 instead |
| `kubectl get cluster` | — | `VERSION` column |

A moving tag (`:8.4`) is probed once, at the digest pulled then; if the tag
later moves under running Pods, each instance still uses its real version
(V1), and only the operator-side rendering lags until the reference changes.
The supported path is the published catalogs, which reference images by
immutable tag and digest.

## Upgrade

The Pod spec loses `MYSQL_VERSION` and `--server-version`, so the Pod template
hash changes once and every instance rolls once after the operator upgrade,
through the normal path (replicas first, then the primary). Called out in the
upgrade notes, like design 030 C7. With `inPlaceInstanceManagerUpdates` the new
manager first runs in old Pods, where `--server-version` is still passed and is
honoured as an override until the Pod rolls.

The first reconcile after the upgrade probes the image each cluster runs; the
previous `targetImage` is empty then, so V4's transition check is skipped once.
The instance-side guard (V1) still refuses an unsupported data-dir transition.

## Non-goals

- Reading image labels from the registry. The containers repo labels every
  image (`co.cnmsql.image.server-version`, …), but the probe already covers
  every image, including ones not built there.
- Re-probing a moving tag on a timer.
- Verifying cosign signatures in the operator; admission controllers
  (Kyverno, sigstore policy-controller) already do it with the published
  identity.
