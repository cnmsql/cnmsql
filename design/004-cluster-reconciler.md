# 004 — Cluster Reconciler Bootstrap

**Status:** done
**Milestone:** M3

Implement the first operator-side reconciler for `Cluster`: given a valid `Cluster` with `instances: 1`, the operator creates and maintains the Kubernetes resources needed to bootstrap one Percona instance pod using the M2/M2.5 instance image. No replicas, promotion, failover, backup, or traffic policy logic.

## Overview

M3 is the bridge from the proven in-pod manager to a real Kubernetes-managed database instance. The reconciler is intentionally narrow: resolve the image, create one PVC and one Pod, provide credentials and TLS material, wait for the instance manager to report ready, and reflect that in `Cluster.status`.

## Design

### Scope

#### In Scope

- Scaffold and wire a `ClusterReconciler` using Kubebuilder/controller-runtime.
- Reconcile only fresh `bootstrap.initdb` clusters with `spec.instances == 1`.
- Create/update the primary instance PVC from `spec.storage`.
- Create/update the primary instance Pod running the M2 instance manager. Prefer `manager instance run` owning first-boot initialization if the existing manager supports it cleanly; otherwise use an init container for `manager instance initdb` guarded by a data-dir marker.
- Generate missing root/application/control secrets when the user did not provide them.
- Depend on cert-manager for per-cluster CA/server/client TLS secrets for the M2 mTLS control API. M3 creates `Issuer`/`Certificate` resources and waits for the resulting Secrets instead of hand-generating certificates. MySQL transport TLS is deferred to M4.
- Render the Pod spec from the existing `ClusterSpec` scheduling fields: resources, affinity, topology spread constraints, priority class, scheduler name, image pull policy/secrets, env/envFrom, and pod/container security contexts.
- Expose one stable per-instance Service if required for DNS and the instance manager's `report_host`; keep read/write/read-only services for M4.
- Poll the instance manager `GET /status` over mTLS when the Pod is reachable.
- Update `Cluster.status` with `instances`, `readyInstances`, `instanceNames`, `currentPrimary`, `currentPrimaryTimestamp`, `latestGeneratedNode`, `image`, `gtidExecutedByInstance`, `observedGeneration`, `phase`, `phaseReason`, and `Ready`/`Progressing` conditions.
- Add focused unit tests with fake client and scheme registration.
- Add an e2e test that applies a one-instance sample to an isolated Kind cluster, waits for Ready, and verifies the Pod can accept a simple write.

#### Out of Scope

- `spec.instances > 1`, replica PVC/Pod creation, and `instance join` — M4.
- Primary election, switchover, failover, RPO/RTO, and quorum logic — M4/M5.
- Default `rw`, `ro`, and `r` Services and traffic routing policies — M4.
- MySQL transport TLS and replication mTLS — M4 alongside replica networking.
- Backup/recovery, ScheduledBackup, object-store transport, and PITR — M6/M7.
- Declarative `Database` reconciliation — M8.
- PodDisruptionBudget, monitoring resources, guards, and self-healing beyond recreating the owned single Pod/PVC — M9.
- ProxySQL/pooler, binlog streaming, and in-place major upgrades — M10.
- Webhooks — later hardening pass. Existing helper defaulting/validation remains in-process for M3.

### Resource Model

Stable, predictable names so future milestones can extend the same objects instead of replacing them:

| Resource | Name | Owner | Notes |
|----------|------|-------|-------|
| Pod | `<cluster>-1` | Cluster | First and only instance in M3. |
| PVC | `<cluster>-1` or `<cluster>-1-data` | Cluster | Data directory volume from `spec.storage`. |
| Secret | `<cluster>-root` | Cluster | Generated only when `rootPasswordSecret` is absent. |
| Secret | `<cluster>-app` | Cluster | Generated only when `bootstrap.initdb.secret` is absent. |
| Secret | `<cluster>-control` | Cluster | Instance manager control account credentials. |
| Issuer | `<cluster>-selfsigned`, `<cluster>-ca` | Cluster | cert-manager issuers for M3 mTLS material. |
| Certificate | `<cluster>-ca`, `<cluster>-server`, `<cluster>-client` | Cluster | cert-manager certificates for M3 mTLS material. |
| Secret(s) | `<cluster>-ca`, `<cluster>-server-tls`, `<cluster>-client-tls` | Certificate | Created by cert-manager and mounted by the Pod. |
| Service | `<cluster>-1` or `<cluster>-instances` | Cluster | Only if needed for stable DNS/control access. |

Common labels on every owned object:

- `app.kubernetes.io/name=cnmsql`
- `app.kubernetes.io/instance=<cluster>`
- `app.kubernetes.io/component=mysql`
- `mysql.cnmsql.co/cluster=<cluster>`
- `mysql.cnmsql.co/instance=<cluster>-1`
- `mysql.cnmsql.co/role=primary`

### Reconciliation Loop

1. Fetch the `Cluster`, apply in-memory defaults, and validate the supported M3 shape. Unsupported but valid future shapes should set a clear condition and requeue without creating partial resources.
2. Resolve the image:
   - `spec.imageName` wins when set.
   - `ImageCatalogRef` reads `ImageCatalog`/`ClusterImageCatalog`.
   - If neither is set, use a local constant for the development image tag produced by `Dockerfile.instance`, and record it in status.
3. Ensure generated secrets exist. Do not overwrite user-provided secret data.
4. Ensure cert-manager Issuer/Certificate resources exist, then wait for the generated TLS Secrets or mount user-provided references.
5. Ensure the data PVC exists. Treat immutable PVC shape changes as a status warning for now; volume expansion behavior can wait.
6. Ensure the instance Pod exists and matches the desired template hash.
7. Read Pod readiness and, when possible, call the instance manager `/status`.
8. Patch status from observed Kubernetes and instance state.

Updates should be idempotent and conflict-aware. Re-fetch before status updates, use merge patch where it keeps ownership clean, and avoid deleting PVCs during normal reconciliation.

## Implementation Notes

1. Scaffold the `Cluster` controller with Kubebuilder and wire it in `cmd/main.go` without moving the existing API layout.
2. Add small internal helpers for names, labels, owner references, conditions, and secret generation.
3. Add image resolution for direct image names and catalogs.
4. Add PVC and Pod builders with unit tests against important object fields.
5. Add the reconcile loop for the single-instance initdb path.
6. Add a small mTLS status client for the instance manager API.
7. Add status aggregation and condition updates.
8. Add RBAC markers and run `make manifests`.
9. Add/update a one-instance sample that uses the locally built instance image.
10. Add Kind e2e for single-instance bootstrap.

## Testing

- **Unit tests:**
  - supported/unsupported spec classification;
  - image resolution from `imageName`, `ImageCatalog`, and `ClusterImageCatalog`;
  - secret generation does not overwrite existing secrets;
  - PVC and Pod templates include owner refs, labels, mounts, env, resources, and scheduling fields;
  - status conditions for pending Pod, ready Pod, and status-client failures.
- **E2E:**
  - create an isolated Kind cluster;
  - load/build the M2.5 instance image;
  - install CRDs/RBAC/manager;
  - apply a one-instance `Cluster`;
  - wait for `Ready=True`, `readyInstances=1`, and `currentPrimary=<cluster>-1`;
  - exec or connect through the Pod to create and read one table row.

## Decisions

- M3 depends on cert-manager for TLS from the beginning (not hand-generated self-signed material).
- The Pod runs `initdb` through the manager's `instance run` command owning first-boot initialization where supported; otherwise an init container for `manager instance initdb` guarded by a data-dir marker.
- Default development image: when neither `imageName` nor `imageCatalogRef` is set, M3 uses `cnmsql-instance:8.0`. The CR stays CNPG-like: users select an image directly or a catalog major, and the reconciler derives the exact manager runtime version from the selected image tag (`8.0` → `8.0.46`, `8.4` → `8.4.0`, etc.). Production users should set an explicit image or catalog.

## Verification

- A one-instance fresh `Cluster` reaches `Ready=True` in Kind.
- The reconciler owns exactly the expected Kubernetes objects and is idempotent across repeated reconciles.
- `Cluster.status` accurately reports the primary, readiness, image, generation, and instance GTID when the manager status endpoint is available.
- Unsupported M3 shapes are reported clearly without destructive side effects.
- `make generate manifests`, `make lint-fix`, `make test`, and the M3 e2e pass in the intended environment.
