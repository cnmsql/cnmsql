# 021 — Cluster-Wide and Namespaced Deployment Modes

**Status:** done

Adds a second supported operator topology. The operator can run **cluster-wide** (today's behavior: one instance, cluster-scoped RBAC, one webhook for the whole cluster) or **namespaced** (RBAC confined to a single namespace, multiple operator instances cohabiting in one cluster, each with its own webhook).

**Goal:** Let multiple operator instances coexist in one Kubernetes cluster, each owning a single namespace, without colliding on the cluster-scoped `ValidatingWebhookConfiguration` introduced in [020](020-status-instance-webhook.md) and without one tenant's webhook intercepting another tenant's `Cluster` status updates.

## Why (the issue this closes)

The operator currently only runs cluster-wide:

- The manager cache watches every namespace (no `Cache.DefaultNamespaces` in [cmd/main.go](../cmd/main.go)).
- RBAC is a `ClusterRole` + `ClusterRoleBinding` ([config/rbac/role.yaml](../config/rbac/role.yaml), [config/rbac/role_binding.yaml](../config/rbac/role_binding.yaml)).
- A single, fixed-named `ValidatingWebhookConfiguration` (`validating-webhook-configuration` in [config/webhook/manifests.yaml](../config/webhook/manifests.yaml)) matches `clusters` UPDATE in **all** namespaces.

Multi-tenant users want to grant the operator rights in only one namespace and run one operator per namespace. Two things block that today:

1. **Cache/RBAC scope.** A namespaced operator must not need (or use) cluster-wide list/watch.
2. **Webhook collision & mis-routing (the hard part).** `ValidatingWebhookConfiguration` is **cluster-scoped**. Multiple namespaced operators would (a) collide on the fixed name, and (b) each have a rule matching `clusters/status` UPDATE in *every* namespace — so tenant A's webhook would be called for tenant B's clusters, routed to A's Service. With [020](020-status-instance-webhook.md)'s `failurePolicy: Fail`, A being down would then block status writes for B. We fix this by giving each namespaced webhook config a **unique name** and a **`namespaceSelector`** scoping it to its own namespace.

## Design

### Mode selection — `WATCH_NAMESPACE`

The manager reads the `WATCH_NAMESPACE` environment variable (operator-SDK / CNPG convention):

- **Empty / unset** ⇒ cluster-wide (today's behavior, unchanged).
- **Set to a namespace** ⇒ namespaced; the operator watches only that one namespace.

In namespaced packaging the variable is injected from the pod's own namespace via the downward API (`valueFrom.fieldRef.fieldPath: metadata.namespace`), so "namespaced" always means "the namespace I'm deployed in".

### Cache scoping — [cmd/main.go](../cmd/main.go)

In the `RunE` closure, the manager options are built conditionally before `ctrl.NewManager`:

```go
if watchNamespace := os.Getenv("WATCH_NAMESPACE"); watchNamespace != "" {
    managerOptions.Cache = cache.Options{
        DefaultNamespaces: map[string]cache.Config{watchNamespace: {}},
    }
    managerOptions.LeaderElectionNamespace = watchNamespace
}
```

A namespace-scoped cache transparently filters every controller's List/Watch, so `ClusterReconciler`, `BackupReconciler`, `ScheduledBackupReconciler`, and `DatabaseReconciler` need **no change**. `LeaderElectionNamespace` is pinned to the watched namespace so cohabiting operators do not contend on a single cluster-wide lease. New import: `sigs.k8s.io/controller-runtime/pkg/cache`.

### Per-instance RBAC is already namespaced

[internal/controller/cluster_rbac.go](../internal/controller/cluster_rbac.go) (`ensureInstanceRBAC`) already creates namespaced `Role` / `RoleBinding` / per-instance `ServiceAccount` objects in the cluster's namespace. **No change** — it works identically in both modes.

### Webhook handler is unchanged; uniqueness comes from packaging

The registered route `/validate-mysql-cnmsql-io-v1alpha1-cluster-status` and the `ClusterStatusValidator` handler ([internal/webhook/v1alpha1/cluster_webhook.go](../internal/webhook/v1alpha1/cluster_webhook.go)) stay as-is. Each namespaced operator has its own webhook Server + Service, so the *route* need not be namespaced; uniqueness is provided by:

1. **A unique `ValidatingWebhookConfiguration` name** per tenant — the overlay's `namePrefix` renames the cluster-scoped resource (e.g. `tenant-a-validating-webhook-configuration`).
2. **A `namespaceSelector`** restricting the webhook to its own namespace, using the always-present `kubernetes.io/metadata.name` label:

   ```yaml
   namespaceSelector:
     matchExpressions:
       - key: kubernetes.io/metadata.name
         operator: In
         values: [<the-operator-namespace>]
   ```

`instanceIdentity` in the handler already enforces `parts[2] == namespace`, so its logic remains correct under either mode.

### Packaging — kustomize overlays

Two sibling overlays reuse the same lower bases (`../crd`, `../rbac`, `../manager`, `../webhook`, `../certmanager`):

- **[config/default](../config/default)** — the **cluster-wide** overlay; output unchanged (`ClusterRole`/`ClusterRoleBinding`, cluster-wide webhook, no selector).
- **[config/namespaced](../config/namespaced)** — the **namespaced** overlay:
  - **RBAC → namespaced.** It still pulls in `../rbac`, then deletes the cluster-scoped `manager-role` / `manager-rolebinding` via `$patch: delete` and adds local `role_namespaced.yaml` (`kind: Role`) + `role_binding_namespaced.yaml` (`kind: RoleBinding`). Leader-election RBAC is already namespaced; the optional human-facing admin/editor/viewer `ClusterRole`s are left as-is (they grant no operator privileges).
  - **Inject `WATCH_NAMESPACE`** via `manager_watch_namespace_patch.yaml` (downward API; the base Deployment has no `env`, so the patch creates it).
  - **Unique webhook name** via the overlay's `namePrefix` (per-tenant knob, alongside `namespace`, at the top of `kustomization.yaml`).
  - **`namespaceSelector`** via `webhook_namespace_selector_patch.yaml` (adds the selector) plus a `replacements` block copying the webhook Service's `.metadata.namespace` into `webhooks.0.namespaceSelector.matchExpressions.0.values.0`. The cert-manager CA-injection `replacements` from `config/default` are duplicated here.
  - All overlay-local files (RBAC, patches, `metrics_service.yaml`) live inside `config/namespaced/` because kustomize's default root-only load restrictor forbids referencing files in sibling directories.

CRDs ([config/crd](../config/crd)) stay cluster-scoped and are a **shared prerequisite** installed once by a cluster admin (`make install`), independent of mode.

### `ClusterImageCatalog` is unsupported in namespaced mode

`ClusterImageCatalog` is a **cluster-scoped** CRD, read at [cluster_plan.go](../internal/controller/cluster_plan.go) only when a `Cluster` references one. A namespaced `Role` cannot grant access to it, so the namespaced overlay's Role intentionally omits `clusterimagecatalogs` (and the cluster-scoped `namespaces` get). In namespaced mode, use the namespaced `ImageCatalog` instead. This keeps the operator's grant strictly confined to its namespace, matching the mode's intent.

## Implementation changes

| File | Change |
|------|--------|
| [cmd/main.go](../cmd/main.go) | Read `WATCH_NAMESPACE`; set `Cache.DefaultNamespaces` + `LeaderElectionNamespace`; startup mode log. Import `pkg/cache`. |
| [config/namespaced/kustomization.yaml](../config/namespaced/kustomization.yaml) | **New** overlay: namespaced RBAC swap, env patch, webhook name + selector, CA-injection replacements, per-tenant `namespace`/`namePrefix`. |
| `config/namespaced/role_namespaced.yaml` | **New** `Role` mirroring `role.yaml` minus cluster-scoped `namespaces` and `clusterimagecatalogs`. |
| `config/namespaced/role_binding_namespaced.yaml` | **New** `RoleBinding`. |
| `config/namespaced/webhook_namespace_selector_patch.yaml` | **New** JSON-patch adding `namespaceSelector`. |
| `config/namespaced/manager_watch_namespace_patch.yaml` | **New** JSON-patch injecting `WATCH_NAMESPACE`. |
| `config/namespaced/{metrics_service,manager_metrics_patch,manager_webhook_patch}.yaml` | Overlay-local copies of the shared default files (load-restrictor requirement). |
| [Makefile](../Makefile) | `OVERLAY ?= config/default`; `deploy`/`build-installer`/`undeploy` use `$(OVERLAY)`; new `deploy-namespaced` / `build-installer-namespaced` (with `NAMESPACE`/`NAME_PREFIX`). |

No change to [internal/controller/cluster_rbac.go](../internal/controller/cluster_rbac.go) or the [cluster_webhook.go](../internal/webhook/v1alpha1/cluster_webhook.go) handler logic.

## Testing

- **Build / render:** `make manifests generate fmt vet build`; `kustomize build config/default` and `kustomize build config/namespaced`. The namespaced render shows `kind: Role`/`RoleBinding` for the manager, **no** cluster-scoped `manager-role` ClusterRole, a `WATCH_NAMESPACE` env, a uniquely-named `ValidatingWebhookConfiguration`, a populated `namespaceSelector`, and the `cert-manager.io/inject-ca-from` annotation. The `config/default` render is unchanged (ClusterRole present, no env, no selector).
- **Namespaced, single tenant (kind/envtest):** install CRDs once; deploy `config/namespaced` in `ns-a`; a `Cluster` in `ns-a` reconciles; a `Cluster` in `ns-b` is **not** reconciled (cache scoped).
- **Namespaced, cohabiting tenants (core goal):** deploy the overlay twice (`ns-a`, `ns-b`) with distinct `NAMESPACE`/`NAME_PREFIX`. Two distinct `ValidatingWebhookConfiguration`s exist. An instance promotion in `ns-a` calls **only** `ns-a`'s webhook Service. Scaling `ns-a`'s operator to 0 leaves `ns-b` `Cluster` status writes working (proves `failurePolicy: Fail` no longer leaks across namespaces).
- **E2E** ([test/e2e/deployment_modes_test.go](../test/e2e/deployment_modes_test.go), `make test-e2e "deployment mode"`): a **2-instance** cluster reaching Ready drives a `status.currentPrimary` write from an instance identity, so the webhook is exercised in each mode. The cluster-wide spec reuses the shared suite operator. The namespaced specs are `Serial`: they stand the shared operator down (scale to 0 + delete its cluster-wide webhook), then deploy **two** namespaced operators via `make deploy-namespaced` in separate namespaces (`e2e-nsmode-a` / `e2e-nsmode-b`, distinct prefixes). A 2-instance cluster in each reaches Ready, each reconciled by its own operator. A final spec proves cross-namespace webhook isolation: with operator A scaled to 0 (its `Fail`-policy webhook endpoint unreachable), a status write in A's namespace is rejected while a status write in B's namespace still succeeds. AfterAll deletes the clusters and namespaces and restores the shared operator.

## Decisions (resolved 2026-06-18)

1. **Mode selection:** `WATCH_NAMESPACE` env var (empty = cluster-wide). Chosen over a CLI flag for operator-SDK/CNPG consistency and trivial downward-API injection; over RBAC auto-detection for being explicit and debuggable.
2. **Watch scope (namespaced):** the operator's own namespace only. Matches "multiple instances cohabiting", one operator per tenant namespace; keeps cache, RBAC, and webhook selector trivially aligned.
3. **Webhook isolation:** namespace the cluster-scoped config **name** *and* add a `namespaceSelector`. Name-only was rejected — it leaves cross-namespace mis-routing and `failurePolicy: Fail` blast radius unsolved.
4. **Packaging:** kustomize overlays (no Helm in repo). Two sibling overlays sharing the lower bases. Manager RBAC is swapped via `$patch: delete` + namespaced replacements rather than restructuring `config/rbac`.
5. **`ClusterImageCatalog`:** unsupported in namespaced mode (option a) — keeps the operator's grant confined to one namespace. Namespaced users use `ImageCatalog`.

## Acceptance criteria

- [x] `WATCH_NAMESPACE` unset ⇒ cluster-wide; set ⇒ cache + leader election scoped to that namespace.
- [x] `config/namespaced` renders namespaced manager RBAC (no cluster-scoped `manager-role`), the downward-API env, a uniquely-named webhook config, and a namespace-scoped `namespaceSelector`.
- [x] Two operators in different namespaces cohabit; neither's webhook intercepts the other's `Cluster` updates.
- [x] `config/default` output and all existing tests are unchanged.
- [x] E2E specs for both topologies bring a 2-instance cluster to Ready ([test/e2e/deployment_modes_test.go](../test/e2e/deployment_modes_test.go)).
- [x] `make test`, `make manifests`, and `go build ./...` pass.

## References

- [020-status-instance-webhook.md](020-status-instance-webhook.md) — the webhook this design must keep correct across modes.
- [cmd/main.go](../cmd/main.go) — manager setup / cache scoping.
- [internal/controller/cluster_rbac.go](../internal/controller/cluster_rbac.go) — already-namespaced per-instance RBAC.
- controller-runtime cache scoping: https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/cache#Options
