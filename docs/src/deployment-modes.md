---
title: "Deployment Modes"
description: "Cluster-wide vs namespaced operator topologies: how WATCH_NAMESPACE scopes the cache and RBAC, how the validating webhook is named and targeted per namespace, and how to run several operators in one cluster."
sidebar_position: 8
---

# Deployment modes

The operator runs in one of two topologies. Cluster-wide is the default: a single operator watches every namespace. Namespaced confines an operator to one namespace, so you can run several operators in the same cluster without them treading on each other.

You pick the mode at install time. The choice changes three things: which namespaces the operator's cache and controllers watch, whether its RBAC is a `ClusterRole` or a namespaced `Role`, and how the validating webhook from the [Security Model](./security-model.md#status-admission-webhook) is named and scoped.

| | Cluster-wide | Namespaced |
| --- | --- | --- |
| Watches | All namespaces | One namespace |
| Operator RBAC | `ClusterRole` + `ClusterRoleBinding` | `Role` + `RoleBinding` |
| Instances per cluster | One operator | Many, one per namespace |
| Webhook config name | Fixed | Unique per operator |
| Webhook scope | All namespaces | One namespace |
| CRDs | Shared, installed once | Shared, installed once |

## Cluster-wide

This is what `helm install` deploys by default. One operator runs with a `ClusterRole` that grants reconciliation rights across the cluster, its cache watches every namespace, and a single `ValidatingWebhookConfiguration` named `cnmsql-validating-webhook-configuration` validates Cluster status updates wherever they happen.

Use it when one team owns the operator and Clusters can live in any namespace.

## Namespaced

A namespaced operator watches only the namespace it runs in. Its RBAC is a `Role` and `RoleBinding` in that namespace rather than a `ClusterRole`, so the operator holds no rights outside its own namespace. Clusters it manages must live in the same namespace.

The per-instance identity RBAC described in the [Security Model](./security-model.md#per-instance-identity) is already namespaced in both modes, so nothing changes there. What does change is the webhook.

### Why the webhook needs a unique name and a namespace selector

A `ValidatingWebhookConfiguration` is a cluster-scoped object, even though the operator serving it is namespaced. Two problems follow when several namespaced operators coexist, and the namespaced overlay solves both.

First, the name. Every operator would otherwise create the same `validating-webhook-configuration` and overwrite the others. Helm solves this: each release gets a unique name (e.g., `cnmsql-validating-webhook-configuration` for a release named `cnmsql`, differentiated per release), so `tenant-a`'s and `tenant-b`'s webhooks never conflict.

Second, the scope. The webhook rule matches Cluster status updates, and without a namespace filter it matches them in every namespace. So `tenant-a`'s webhook would be invoked for `tenant-b`'s Clusters and routed to `tenant-a`'s webhook Service. Because the webhook uses `failurePolicy: Fail`, `tenant-a` being down would then block status writes for `tenant-b`. When `rbac.namespaced=true`, the chart adds a `namespaceSelector` keyed on the built-in `kubernetes.io/metadata.name` label, so each webhook only ever fires for Clusters in its own namespace:

```yaml
namespaceSelector:
  matchExpressions:
    - key: kubernetes.io/metadata.name
      operator: In
      values: [tenant-a]
```

## Selecting the mode

### WATCH_NAMESPACE

At runtime the operator reads the `WATCH_NAMESPACE` environment variable:

- unset or empty means cluster-wide;
- set to a namespace means namespaced, watching that one namespace.

The namespaced deployment injects it from the pod's own namespace with the downward API, so the operator always watches the namespace it is deployed into. When the variable is set, the operator also scopes its leader-election lease to that namespace, so two operators never contend for one lease.

### Deploying via Helm

Helm is the recommended way to install. The chart is published from the [charts](https://github.com/cnmsql/charts) repository:

**Cluster-wide deployment (default):**

```bash
helm repo add cnmsql https://cnmsql.github.io/charts
helm repo update
helm install cnmsql cnmsql/cnmsql \
  --namespace cnmsql-system \
  --create-namespace
```

**Namespaced deployment:**

Pass `rbac.namespaced=true` when installing. The chart will inject `WATCH_NAMESPACE` from the pod's namespace via the downward API and scope the webhook with a `namespaceSelector` matching the release namespace:

```bash
helm repo add cnmsql https://cnmsql.github.io/charts
helm repo update
helm install cnmsql cnmsql/cnmsql \
  --namespace tenant-a \
  --create-namespace \
  --set rbac.namespaced=true
```

The CRDs are cluster-scoped and shared by every operator. They are installed automatically with the chart or can be applied separately from the chart's templates.

## What namespaced mode does not cover

`ClusterImageCatalog` is a cluster-scoped resource, and a namespaced `Role` cannot grant access to it. A Cluster that references a `ClusterImageCatalog` will not resolve under a namespaced operator. Use the namespaced `ImageCatalog` instead. See [Instance Images and Versions](./instance-images.md).

## Running several operators in one cluster

To host operators side by side, give each its own namespace and install with `rbac.namespaced=true`:

```bash
helm install cnmsql cnmsql/cnmsql \
  --namespace tenant-a --create-namespace \
  --set rbac.namespaced=true
helm install cnmsql cnmsql/cnmsql \
  --namespace tenant-b --create-namespace \
  --set rbac.namespaced=true
```

Each release deploys into its own namespace with its own webhook configuration scoped to that namespace, its own cert-manager certificate, and RBAC restricted to that namespace only. None of them sees or reconciles another's Clusters.

This pairs with the cluster-per-tenant model in [Multi-Tenancy](./multi-tenancy.md#cluster-per-tenant): one namespace per tenant, one operator per namespace, and Kubernetes RBAC handing each tenant its own namespace and nothing else.

## See also

- [Security Model](./security-model.md#kubernetes-rbac)
- [Multi-Tenancy](./multi-tenancy.md)
- [Quickstart](./quickstart.md)
