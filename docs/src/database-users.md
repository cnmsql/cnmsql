---
title: "Database Users"
description: "The DatabaseUser CR: standalone, installation-wide MySQL accounts with grants, password rotation, reclaim policy, conflict detection and adoption, and a safe DBaaS-admin recipe."
sidebar_position: 8
---

# Database Users

`DatabaseUser` is a namespaced custom resource for a **single, installation-wide
MySQL account**. Unlike the users embedded in a [`Database`](./multi-tenancy.md),
a `DatabaseUser` is not scoped to one schema: it owns one account
(`name@host`) and any set of grants across the instance. It is the self-service,
reconciled form of a MySQL user, just as a `Database` is the self-service form of
a schema.

## When to use it

cnmsql has three ways to declare a MySQL account. Pick by ownership:

| Mechanism | Lives in | Use it for |
|---|---|---|
| `spec.managed.roles` | the `Cluster` | cluster-level accounts the **platform team** owns; editing means editing the shared `Cluster`. |
| `Database.spec.users[]` | a `Database` | accounts whose life is **tied to one schema** and whose grants default to it. |
| **`DatabaseUser`** | its own CR | an **application-team account** that spans several schemas (or none), managed without touching the `Cluster`. |

A given `name@host` should be owned by **exactly one** of these. See
[Conflicts and adoption](#conflicts-and-adoption) for what happens otherwise.

## A first user

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: reporter-pw
  namespace: shared        # the Cluster's namespace
type: Opaque
stringData:
  password: "change-me"
---
apiVersion: mysql.cnmsql.co/v1alpha1
kind: DatabaseUser
metadata:
  name: reporter
  namespace: shared
spec:
  cluster:
    name: shared
  # name: reporter         # MySQL user name; defaults to metadata.name
  host: "%"
  passwordSecret:
    name: reporter-pw
    key: password
  reclaimPolicy: retain    # keep the MySQL account when this object is deleted
  grants:
    - privileges: [SELECT]
      on: "sales.*"
    - privileges: [SELECT]
      on: "marketing.*"
```

The operator finds the cluster's primary, creates the account if absent, and
applies the grants. `status.applied` flips to `true` once MySQL matches the
spec; the `Ready` condition carries detail. Because the secrets and the cluster
are read from the object's own namespace, a `DatabaseUser`, its Secret, and its
`Cluster` always live together.

```console
$ kubectl get databaseuser
NAME       USER       CLUSTER   APPLIED   REASON
reporter   reporter   shared    true      Reconciled
```

## Password rotation

The operator records the password Secret's `resourceVersion` in
`status.passwordSecretResourceVersion` and re-issues `ALTER USER ... IDENTIFIED
BY` only when that Secret changes. Rotate by updating the Secret; you do not need
to edit the `DatabaseUser`.

## Reclaim policy

`spec.reclaimPolicy` controls the MySQL account when the `DatabaseUser` object is
deleted:

- `retain` (default): the object is removed; the MySQL account stays.
- `delete`: the operator drops the MySQL account before releasing the finalizer.

## Conflicts and adoption

The operator never silently takes over an account it did not create. When it
finds the target `name@host` already present in MySQL but this `DatabaseUser` has
**never been applied**, it refuses to touch it and reports a conflict:

```console
$ kubectl get databaseuser reporter -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}'
UserConflict
```

This protects accounts owned by `spec.managed.roles`, by another `DatabaseUser`,
or created by hand. To take ownership deliberately, **adopt** the account by
setting an annotation:

```bash
kubectl annotate databaseuser reporter mysql.cnmsql.co/adopt=true --overwrite
```

On the next reconcile the operator adopts the account: it reconciles the password
and grants and sets `status.applied=true`. After that, ownership is implicit and
you can remove the annotation.

## A safe DBaaS admin

A common DBaaS ask is "give the tenant a near-root account on their data, but
make sure they can't break what the operator relies on" (replication, fencing,
the operator's own `cnmsql_*` accounts).

Express this with a **grant of `ALL`**, *not* `spec.superuser: true`:

```yaml
spec:
  cluster: { name: shared }
  passwordSecret: { name: tenant-admin-pw, key: password }
  grants:
    - privileges: [ALL]
      on: "*.*"          # full power over data and DDL...
```

The key is that `WITH GRANT OPTION` is emitted **only** for
`spec.superuser: true`. A plain `ALL` grant gives the tenant everything on the
data plane but **no ability to grant privileges to themselves**, so they cannot
escalate to `REPLICATION_SLAVE_ADMIN`, `SYSTEM_VARIABLES_ADMIN`,
`GROUP_REPLICATION_ADMIN`, `SUPER`, and the like.

As a second layer, cnmsql **rejects** a `DatabaseUser` that asks for any
cluster-control privilege directly (the same idea as the dangerous-config-key
denylist). Such an object stays unapplied with reason `Invalid`:

```
REPLICATION_SLAVE_ADMIN, REPLICATION_APPLIER, GROUP_REPLICATION_ADMIN,
GROUP_REPLICATION_STREAM, SYSTEM_VARIABLES_ADMIN, CONNECTION_ADMIN,
SERVICE_CONNECTION_ADMIN, PERSIST_RO_VARIABLES_ADMIN, BINLOG_ADMIN,
BINLOG_ENCRYPTION_ADMIN, CLONE_ADMIN, SUPER, SHUTDOWN, FILE,
GROUP_REPLICATION_FLOW_CONTROL_ADMIN
```

> Avoid `spec.superuser: true` for multi-tenant use: it is literal
> `ALL PRIVILEGES ON *.* WITH GRANT OPTION`. Reserve it for trusted cluster-level
> accounts only.

## Validation

A `DatabaseUser` is rejected (`status.conditions[Ready].reason = Invalid`) when:

- the resolved name is reserved (`root`, `mysql.*`, `cnmsql_*`);
- `host` is empty;
- `superuser: true` is combined with explicit `grants`;
- `requireTLS` is not one of `none`, `ssl`, `x509`;
- any grant requests a denied cluster-control privilege (see above).

## Managing users with `kubectl cnmsql`

The plugin's `databaseuser` (alias `dbuser`) noun edits these objects and their
Secrets declaratively. The operator still does the work, so status, password
rotation, and conflict/adoption behave the same as when you apply YAML:

```bash
# List users and their applied state
kubectl cnmsql databaseuser list

# Create a user with a generated password and a grant
kubectl cnmsql databaseuser create reporter --cluster shared --generate \
  --grant "SELECT ON sales.*"

# Add or remove grants
kubectl cnmsql databaseuser grant reporter --add "SELECT ON marketing.*"

# Rotate the password (writes the Secret, prints once)
kubectl cnmsql databaseuser passwd reporter --generate

# Resolve a UserConflict by adopting the existing account
kubectl cnmsql databaseuser adopt reporter

# Scaffold the safe DBaaS admin from the recipe above
kubectl cnmsql databaseuser dbaas tenant-admin --cluster shared --generate

# Delete the resource (optionally setting the reclaim policy first)
kubectl cnmsql databaseuser drop reporter --reclaim delete
```

The imperative [`kubectl cnmsql user`](./operations.md) commands still exist for
direct, non-declarative control-API access; prefer `databaseuser` for anything
you want the operator to keep reconciled.

See the [`DatabaseUser`](./api-reference.md#databaseuser) API reference for the
full field list.
