---
title: "Deployment Modes"
description: "Cluster-wide vs namespaced operator topologies: how WATCH_NAMESPACE scopes the cache and RBAC, how the validating webhook is named and targeted per namespace, and how to run several operators in one cluster."
sidebar_position: 8
---

# Deployment modes

The operator runs in one of two topologies. Cluster-wide is the default: a
single operator watches every namespace. Namespaced confines an operator to one
namespace, so you can run several operators in the same cluster without them
treading on each other.

You pick the mode at install time. The choice changes three things: which
namespaces the operator's cache and controllers watch, whether its RBAC is a
`ClusterRole` or a namespaced `Role`, and how the validating webhook from the
[Security Model](./security-model.md#status-admission-webhook) is named and
scoped.

| | Cluster-wide | Namespaced |
| --- | --- | --- |
| Watches | All namespaces | One namespace |
| Operator RBAC | `ClusterRole` + `ClusterRoleBinding` | `Role` + `RoleBinding` |
| Instances per cluster | One operator | Many, one per namespace |
| Webhook config name | Fixed | Unique per operator |
| Webhook scope | All namespaces | One namespace |
| CRDs | Shared, installed once | Shared, installed once |

## Cluster-wide

This is what `make deploy` installs. One operator runs with a `ClusterRole` that
grants reconciliation rights across the cluster, its cache watches every
namespace, and a single `ValidatingWebhookConfiguration` named
`cnmsql-validating-webhook-configuration` validates Cluster status
updates wherever they happen.

Use it when one team owns the operator and Clusters can live in any namespace.

## Namespaced

A namespaced operator watches only the namespace it runs in. Its RBAC is a
`Role` and `RoleBinding` in that namespace rather than a `ClusterRole`, so the
operator holds no rights outside its own namespace. Clusters it manages must
live in the same namespace.

The per-instance identity RBAC described in the
[Security Model](./security-model.md#per-instance-identity) is already namespaced
in both modes, so nothing changes there. What does change is the webhook.

### Why the webhook needs a unique name and a namespace selector

A `ValidatingWebhookConfiguration` is a cluster-scoped object, even though the
operator serving it is namespaced. Two problems follow when several namespaced
operators coexist, and the namespaced overlay solves both.

First, the name. Every operator would otherwise create the same
`validating-webhook-configuration` and overwrite the others. The namespaced
overlay prefixes the name per operator (through kustomize `namePrefix`), so
`tenant-a` gets `tenant-a-validating-webhook-configuration` and `tenant-b` gets
its own.

Second, the scope. The webhook rule matches Cluster status updates, and without
a namespace filter it matches them in every namespace. So `tenant-a`'s webhook
would be invoked for `tenant-b`'s Clusters and routed to `tenant-a`'s webhook
Service. Because the webhook uses `failurePolicy: Fail`, `tenant-a` being down
would then block status writes for `tenant-b`. The namespaced overlay adds a
`namespaceSelector` keyed on the built-in `kubernetes.io/metadata.name` label, so
each webhook only ever fires for Clusters in its own namespace:

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

The namespaced overlay injects it from the pod's own namespace with the downward
API, so the operator always watches the namespace it is deployed into. When the
variable is set, the operator also scopes its leader-election lease to that
namespace, so two operators never contend for one lease.

### Kustomize overlays and make targets

Manifests come from two kustomize overlays:

- `config/default` is the cluster-wide overlay. `make deploy` and
  `make build-installer` use it.
- `config/namespaced` is the namespaced overlay. It swaps in the namespaced
  RBAC, injects `WATCH_NAMESPACE`, and applies the webhook name prefix and
  selector.

Deploy a namespaced operator with the namespace and prefix you want:

```bash
make deploy-namespaced NAMESPACE=tenant-a NAME_PREFIX=tenant-a-
```

`build-installer-namespaced` writes the same manifests to `dist/install.yaml`
instead of applying them. Both targets accept `NAMESPACE` and `NAME_PREFIX`.

The CRDs are cluster-scoped and shared by every operator, so a cluster admin
installs them once and they are independent of the mode:

```bash
make install
```

## What namespaced mode does not cover

`ClusterImageCatalog` is a cluster-scoped resource, and a namespaced `Role`
cannot grant access to it. A Cluster that references a `ClusterImageCatalog`
will not resolve under a namespaced operator. Use the namespaced `ImageCatalog`
instead. See [Instance Images and Versions](./instance-images.md).

## Running several operators in one cluster

To host operators side by side, give each its own namespace and a distinct
`NAME_PREFIX`:

```bash
make deploy-namespaced NAMESPACE=tenant-a NAME_PREFIX=tenant-a-
make deploy-namespaced NAMESPACE=tenant-b NAME_PREFIX=tenant-b-
```

Each operator then has its own webhook configuration name, its own webhook
Service and cert-manager certificate in its namespace, and a cache and RBAC
scoped to that namespace. None of them sees or reconciles another's Clusters.

This pairs with the cluster-per-tenant model in
[Multi-Tenancy](./multi-tenancy.md#cluster-per-tenant): one namespace per tenant,
one operator per namespace, and Kubernetes RBAC handing each tenant its own
namespace and nothing else.

## See also

- [Security Model](./security-model.md#kubernetes-rbac)
- [Multi-Tenancy](./multi-tenancy.md)
- [Quickstart](./quickstart.md)
