# 020 — Admission Webhook for Instance Status AuthZ

**Status:** done

Adds per-instance Kubernetes identities and a validating admission webhook that prevents a rogue MySQL instance from corrupting the Cluster status.

**Goal:** Mitigate the risk that a compromised or misbehaving instance Pod uses its cluster-scoped Role to patch `.status` fields it does not own. Each instance now runs under its own ServiceAccount, and a validating webhook enforces that *only* the promoting instance may modify `status.currentPrimary` (and only to itself) plus the matching `status.currentPrimaryTimestamp`.

## Why (the issue this closes)

M5.5 introduced the CNPG pull model: the operator writes `status.targetPrimary` and the target instance self-promotes, then patches `status.currentPrimary` and `status.currentPrimaryTimestamp` via the Kubernetes API.

Before this design:

- All instance Pods shared a single per-Cluster ServiceAccount (`<cluster>-instance`).
- That ServiceAccount had `update/patch` on `clusters/status` for the entire Cluster status object.
- A single breached/restarted instance could therefore rewrite **any** status field: `currentPrimary` for another instance, `phase`, `readyInstances`, `divergedInstances`, `fencedInstances`, etc.
- The operator trusts `status.currentPrimary` and `status.currentPrimaryTimestamp` as ground truth for routing, label updates, and follow decisions; a wrong value can redirect traffic to a replica or trigger a bad failover.

This design removes that blast radius with a least-privilege identity model and an admission-time enforcement layer.

## Design

### Per-instance identity

Every instance Pod used to run as the same ServiceAccount. We now create one ServiceAccount **per desired instance**:

- Instance `demo-1` runs as ServiceAccount `demo-1-instance`.
- Instance `demo-2` runs as ServiceAccount `demo-2-instance`.
- The naming rule is `<cluster>-<ordinal>-instance`, derived from the instance Pod name `<cluster>-<ordinal>`.

Properties:

- Each SA is labeled with the cluster and instance labels (`mysql.cloudnative-mysql.io/cluster`, `mysql.cloudnative-mysql.io/instance`).
- Each SA is owned by the `Cluster` CR so it is garbage-collected with the cluster.
- The same per-Cluster Role and RoleBinding are reused; the binding subjects now enumerate every desired instance SA.
- When the cluster is scaled down, stale instance SAs are deleted so the removed identity cannot be reused later.

The single shared SA file no longer exists; `clusterPlan` now computes the per-instance SA name at Pod-template time via `instanceServiceAccountName(inst instancePlan)`.

### Validating webhook

A custom admission webhook is registered at:

```
/validate-mysql-cloudnative-mysql-io-v1alpha1-cluster-status
```

It only validates `UPDATE` requests to the `clusters` resource, and only the `status` subresource.

#### Failure policy and scope

The webhook uses `failurePolicy: Fail`: if the webhook is unreachable, Kubernetes rejects the update rather than silently admit it.

The generated rule targets `resources: [clusters/status]`, so the webhook is invoked only for updates to the `status` subresource. Metadata changes (annotations, labels) to a `Cluster` do not call the webhook at all, so a transient webhook outage does not block day-to-day operator commands such as `kubectl annotate` or `kubectl label`.

#### Rule 1: identify the caller

The webhook parses `admission.Request.UserInfo.Username`:

- Pattern: `system:serviceaccount:<namespace>:<cluster>-<ordinal>-instance`.
- The namespace must match the cluster namespace.
- The account basename (`<cluster>-<ordinal>`) must be a member of this cluster (`<cluster>-` prefix), and `<ordinal>` must be a plain decimal number. This prevents an arbitrarily named ServiceAccount such as `<cluster>-evil-instance` from being classified as a legitimate instance identity (RBAC is the primary gate — such an account is not a RoleBinding subject — but the webhook validates the shape as defense in depth).

If the caller is **not** an instance ServiceAccount, the request is admitted subject to normal Kubernetes RBAC.

#### Rule 2: must be the `status` subresource

Any request from an instance identity that is not targeting the `status` subresource is denied.

#### Rule 3: instance may only touch its own `currentPrimary` claim

For instance SA `demo-1-instance`, the only permitted mutation is:

- `status.currentPrimary` may change.
- The new value must be exactly `demo-1`.
- `status.currentPrimaryTimestamp` must change at the same time and must not be empty.

Deny if:

- `status.currentPrimary` is set to a different instance name.
- `status.currentPrimaryTimestamp` is removed or left empty while `currentPrimary` changes.

#### Rule 4: instance may not modify any other status field

After masking `currentPrimary` and `currentPrimaryTimestamp` from old and new objects, the remaining status must be byte-identical. This prevents an instance from changing `phase`, `readyInstances`, `divergedInstances`, `fencedInstances`, `failedInstances`, `primaryFailingSince`, `gtidExecutedByInstance`, `conditions`, etc.

#### Failure policy

- `failurePolicy: Fail` on the webhook configuration.
- If the webhook is unreachable, the API server rejects the update. This is intentional: a missing webhook is safer than allowing unreviewed instance writes.
- Operator and human users can still update status through normal RBAC; the webhook simply adds an extra gate for instance identities.
- **Availability tradeoff:** because the rule is scoped to `clusters/status` UPDATE cluster-wide with no caller-based bypass, while the webhook is *unreachable* the API server rejects *all* status writes — including the operator's own. So webhook downtime stalls reconciliation for every cluster, not just instance writes. We accept this: the webhook is served by the operator pod itself, so if the webhook is down the operator is almost certainly down too. The webhook handler must therefore be registered with the manager (`SetupClusterWebhookWithManager` in `cmd/main.go`); otherwise the configured path returns 404, which `failurePolicy: Fail` turns into a hard rejection of every status update.

### RBAC split

The per-Cluster Role still grants `update/patch` on `clusters/status` because the Kubernetes API cannot express field-level access. The webhook is the field-level enforcement mechanism.

The operator ServiceAccount is not affected by the webhook. It must still be able to write normal status fields such as `phase`, `readyInstances`, and `targetPrimary`.

## Implementation changes

| File | Change |
|------|--------|
| `internal/controller/cluster_plan.go` | Removed shared `InstanceServiceAccount` field; added `instanceServiceAccountName` helper; returns SA name per instance. |
| `internal/controller/cluster_pod.go` | `serviceAccountName` now uses the per-instance helper. |
| `internal/controller/cluster_rbac.go` | Reconciles per-instance SAs, one Role, one multi-subject RoleBinding, and prunes stale SAs on scale-down. |
| `internal/webhook/v1alpha1/cluster_webhook.go` | Custom validating handler with the caller-identity rules above. |
| `cmd/main.go` | Registers the webhook handler with the manager webhook server. |
| `Makefile` | Added `./internal/webhook/...` to `make manifests` paths so the ValidatingWebhookConfiguration is regenerated. |
| `config/webhook/manifests.yaml` | Generated ValidatingWebhookConfiguration pointing to the new path, scoped to `clusters/status` update. |

## Testing

- **Unit tests** in `internal/webhook/v1alpha1/cluster_webhook_test.go` cover:
  - Operator account changing arbitrary status fields is allowed.
  - Instance `demo-1` promoting itself is allowed.
  - Instance `demo-1` trying to set `currentPrimary` to `demo-2` is denied.
  - Instance trying to flip `phase` is denied.
  - Instance request not targeting `status` subresource is denied.
- **Controller unit tests** already verify per-instance SA names in the Pod template and that the RoleBinding contains the correct subjects.
- **E2E:** the existing suite now waits for the validating webhook CA injection/cert-manager secret before creating clusters.

## Decisions (resolved 2026-06-18)

1. **Per-instance vs shared ServiceAccount:** per-instance SAs. Slightly more objects, but gives the webhook an unforgeable caller identity tied to the Pod name. The alternative of using the Pod's projected service-account token does not help Kubernetes API calls; the API server authenticates as the SA.
2. **Webhook granularity:** a single validating webhook on `clusters/status` UPDATE, with runtime caller classification. Mutating/defaulting webhooks are not needed here.
3. **Field mask approach:** strip `currentPrimary` and `currentPrimaryTimestamp`, then deep-compare the rest of the status. This is robust to us adding new status fields later — the default rule remains "instances cannot touch them".
4. **What about `status.targetPrimary`?** Left as operator-only. Instances read it but must never write it; the webhook enforces that.
5. **Failure policy:** `Fail`, so a broken or unreachable webhook prevents instance writes rather than silently opening the gate. Cluster operations from the operator (which go through its own credentials) are unaffected.

## Acceptance criteria

- [x] Each instance Pod uses a dedicated ServiceAccount identified by its Pod name.
- [x] The validating webhook denies any instance attempt to modify status fields other than its own `currentPrimary` claim.
- [x] The webhook requirement is generated in `config/webhook/manifests.yaml` via `make manifests`.
- [x] Unit tests cover allow and deny cases.
- [x] `make test`, `make manifests`, and `go build ./...` pass.

## References

- `internal/webhook/v1alpha1/cluster_webhook.go` — webhook implementation.
- `internal/controller/cluster_rbac.go` — per-instance SA reconciliation.
- `internal/controller/cluster_plan.go` — SA naming derivation.
