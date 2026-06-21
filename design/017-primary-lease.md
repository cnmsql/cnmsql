# 017 — Primary Lease for Failover Fencing

**Status:** in-progress
**Milestone:** M13.4

A per-cluster `coordination.k8s.io/v1` Lease object that the acting primary instance must hold before accepting writes, closing the split-brain gap during async-replication failover.

## What it does

A per-cluster `coordination.k8s.io/v1` Lease object that the acting primary instance
must hold before accepting writes. During failover, the operator waits for the
old primary's lease to expire before promoting a candidate. This gives us a
time-bounded token that a network-isolated instance must voluntarily surrender if
it cannot renew it.

## The fencing gap this fills

Before the lease, fencing worked like this:

1. **Annotation fencing**: the operator writes `status.fencedInstances` and the
   in-Pod reconciler blocks promotion when it sees its own name. Declarative,
   but depends on the operator polling and reacting.

2. **Pod deletion on failover**: `reconcileFailover` deletes the old primary's
   Pod before promoting. Works well but the old primary can still accept writes
   for the seconds until kubelet acts.

3. **Self-demotion**: an instance that is not `targetPrimary` demotes itself.
   Fast, but only works if the instance can still reach the API server and its
   own mysqld.

None of these layers produce a mutually exclusive, time-limited token. If the
old primary is network-partitioned, neither the annotation (the operator cannot
reach the Pod to fence it) nor self-demotion (the instance cannot reach the API
server) works. The operator can still delete the Pod, but what if kubelet on the
partitioned node is slow? The lease closes that gap: a 15-second TTL that the
old primary must keep renewing, and if it stops, the token expires on its own.

The lease is common machinery in etcd, ZooKeeper, and `pg_auto_failover`. CNPG
gets away without it because they use quorum-based synchronous replication, but
cnmsql supports async replication where quorum does not apply.

## How it works

### Lease lifecycle

```
Cluster "demo"

  Lease "demo-primary" (coordination.k8s.io/v1, same namespace)
    spec:
      leaseDurationSeconds:  15
      holderIdentity:        "demo-1"
      renewTime:             <RFC3339 microtime>
      acquireTime:           <RFC3339 microtime>
      leaseTransitions:      0
    ownerReferences:
      - apiVersion: mysql.cnmsql.co/v1alpha1
        kind: Cluster
        name: demo

  Primary instance manager (demo-1)
    1. On promotion: acquire (create or take over) the lease
    2. Every reconcile (~30s): renew (update renewTime)
    3. On demotion: release (delete) the lease
    4. On shutdown (SIGTERM): release the lease

  Operator (ClusterReconciler)
    - Before failover: if old primary's lease still exists and is unexpired,
      wait for expiry or explicit release
    - Cleanup: on cluster deletion, the Lease is GC'd via ownerReference
```

### Feature gate

A single field on `ClusterSpec`:

```go
// +kubebuilder:default:=true
// +optional
EnablePrimaryLease *bool `json:"enablePrimaryLease,omitempty"`
```

Defaults to `true`. Set it to `false` to skip all lease management for a cluster.
This is useful for single-instance or test clusters where the extra API call is
unnecessary.

### Operator side: two helpers

File: `internal/controller/cluster_primary_lease.go`

**`ensurePrimaryLease`** runs early in `Reconcile`, before instance provisioning.
It uses `controllerutil.CreateOrUpdate` to make sure a Lease skeleton exists for
the cluster. It sets `leaseDurationSeconds` to 15 and attaches an owner reference
to the Cluster so the Lease is garbage-collected when the Cluster is deleted. The
operator never touches the holder fields after creation; those are the instance's
job.

**`isPrimaryLeaseHeld`** is called from `reconcileFailover` after
`selectFailoverCandidate` succeeds but before the old primary Pod is fenced. It
reads the Lease and checks whether `holderIdentity` matches the old primary and
whether `renewTime` is still within `leaseDurationSeconds`. If the lease is still
held, failover returns with `RequeueAfter: 15s` and waits for expiry. This keeps
the operator from promoting a candidate while the old primary might still be
accepting writes.

### Instance side: acquire, renew, release

File: `pkg/management/mysql/instance/rolereconciler/lease.go`

The in-Pod role reconciler gets lease management in its promote and demote paths:

**`acquireOrRenewLease`** is called before promotion and periodically while the
instance is primary:
- Get the Lease by name (`{cluster}-primary`).
- If it does not exist, create it with `holderIdentity` set to this instance.
- If someone else holds it and the lease has not expired, return
  `errPrimaryLeaseHeld` and requeue.
- If the lease is expired or we already hold it, update `renewTime` to now. On a
  new acquisition, also set `acquireTime` and increment `leaseTransitions`.
- If it was a create, call `Create`. Otherwise call `Update`.

**`releaseLease`** is called on demotion, on fencing, and on shutdown (the
`Start` function's deferred cleanup):
- Get the Lease.
- If we hold it, delete it entirely. Clearing without deleting would leave a
  stale object with the old `holderIdentity` visible in the cache.
- If someone else holds it or it does not exist, do nothing.

**`leaseExpired`** is a helper: `time.Since(renewTime) > leaseDurationSeconds`.
Returns true if `renewTime` is nil (never acquired).

### Failover integration

File: `internal/controller/cluster_failover.go` (line 87-94)

```go
held, err := r.isPrimaryLeaseHeld(ctx, cluster, observed.PrimaryName)
if err != nil {
    return true, ctrl.Result{}, err
}
if held {
    reason := "Primary lease still held by old primary; waiting for expiry"
    return true, ctrl.Result{RequeueAfter: primaryLeaseDuration},
        r.patchOperationPhase(...)
}
```

The pod deletion and fencing still happen after the lease check, so the old
primary ends up double-fenced: its lease expired (or was released) and its Pod
was removed from the Service. This narrows the split-brain window from "until
kubelet acts" to "until kubelet acts **or** the lease TTL expires", whichever
comes first.

### Cache configuration for the in-Pod manager

File: `pkg/management/mysql/instance/rolereconciler/start.go` (line 175-178)

The in-Pod controller-runtime manager restricts its informer cache to only watch
the specific lease for this cluster:

```go
&coordinationv1.Lease{}: {
    Namespaces: map[string]cache.Config{opts.Namespace: {}},
    Field:      fields.OneTermEqualSelector("metadata.name", opts.ClusterName+"-primary"),
},
```

This keeps memory usage minimal. The instance does not need to watch every lease
in the cluster, just the one it competes for.

## RBAC

### Operator (on ClusterReconciler)

```go
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
```

The operator needs `get` and `create` for `ensurePrimaryLease`, `get` for
`isPrimaryLeaseHeld`, and `delete` for cleanup.

### Instance (operator-authored Role)

File: `internal/controller/cluster_rbac.go` (line 61-66)

Scoped to the specific lease name:

```yaml
- apiGroups: ["coordination.k8s.io"]
  resources: ["leases"]
  resourceNames: ["<cluster>-primary"]
  verbs: ["get", "create", "update", "patch", "delete", "watch"]
```

`watch` is needed for the informer cache to work.

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `leaseDurationSeconds` | 15 | TTL of the lease. Must be longer than the reconcile interval (~10s). Not exposed in `ClusterSpec` for now. |
| `spec.enablePrimaryLease` | `true` | Feature gate. When false, neither the operator nor the instance manager touches the Lease. |

## Files

| File | Purpose |
|------|---------|
| `api/v1alpha1/cluster_types.go` | `EnablePrimaryLease` field on `ClusterSpec` |
| `api/v1alpha1/cluster_funcs.go` | Default `EnablePrimaryLease = true` |
| `internal/controller/cluster_primary_lease.go` | Operator-side `ensurePrimaryLease` and `isPrimaryLeaseHeld` |
| `internal/controller/cluster_primary_lease_test.go` | Tests for operator-side lease logic |
| `internal/controller/cluster_rbac.go` | Lease permissions in the instance Role |
| `internal/controller/cluster_failover.go` | Lease-held guard before failover promotion |
| `internal/controller/cluster_controller.go` | Calls `ensurePrimaryLease` in Reconcile, RBAC marker, `coordinationv1` scheme registration |
| `pkg/management/mysql/instance/rolereconciler/lease.go` | Instance-side acquire, renew, expiry, release |
| `pkg/management/mysql/instance/rolereconciler/reconciler.go` | Lease calls wired into promote/demote paths |
| `pkg/management/mysql/instance/rolereconciler/start.go` | Lease release on shutdown, cache config |

## What is still pending

- Planned switchover should wait for the old primary's lease to disappear before
  setting `targetPrimary`.
- The isolation detector should include lease renewal age in the liveness
  decision so a primary that cannot reach the API server self-fences faster.
- Integration tests with testcontainers and Kind e2e tests for the full
  promotion cycle.

## Out of scope

- Configurable `leaseDurationSeconds` per cluster (can be added later).
- Lease-based leader election for the operator itself (controller-runtime
  already handles this).
- Multi-primary Group Replication lease topology.
- Exposing lease state in `ClusterStatus` (the lease is an internal detail).
