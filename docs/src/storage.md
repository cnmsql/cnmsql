---
title: "Storage"
description: "How cnmsql provisions per-instance PVCs and how to grow them safely, including online and offline volume expansion."
sidebar_position: 5
---

# Storage

cnmsql gives every instance its own PersistentVolumeClaim. Because the operator
manages Pods and PVCs directly rather than through a StatefulSet (see
[Cluster Lifecycle](./cluster-lifecycle.md)), it can grow a volume and, when the
storage backend requires it, recycle the owning Pod to finish the expansion —
one instance at a time, primary last.

## Configuring storage

Storage is configured under `spec.storage`:

```yaml
apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: cluster-sample
spec:
  instances: 3
  storage:
    size: 10Gi
    storageClass: fast-ssd      # optional; defaults to the cluster default class
  bootstrap:
    initdb:
      database: app
      owner: app
```

| Field | Purpose |
| --- | --- |
| `size` | Requested volume size. Required unless set in `pvcTemplate`. |
| `storageClass` | StorageClass for the PVCs. Applied after the template; defaults to the cluster default class. |
| `pvcTemplate` | A full `PersistentVolumeClaimSpec` for advanced cases (access modes, selectors, `volumeAttributesClassName`). |
| `resizeInUseVolumes` | Whether the backend can expand a mounted volume. Defaults to `true`. See [Resizing volumes](#resizing-volumes). |

The data volume name matches the instance: `<cluster>-1`, `<cluster>-2`, and so
on. PVCs are retained on scale-down — treat a retained PVC as database data, not
scratch space.

## Resizing volumes

To grow a cluster's storage, increase `spec.storage.size`:

```bash
kubectl patch cluster cluster-sample --type=merge \
  -p '{"spec":{"storage":{"size":"20Gi"}}}'
```

The operator reapplies the new request to every instance PVC. Volumes can only
grow: Kubernetes rejects a smaller request, and the operator never shrinks a
PVC. The StorageClass must allow expansion (`allowVolumeExpansion: true`),
otherwise the API server rejects the change.

### Online expansion (default)

With `resizeInUseVolumes: true` (the default), the operator assumes the backend
can expand a volume while it is mounted. It patches the PVC request and relies on
the kubelet to grow the filesystem in place. **No Pod is restarted** — the extra
capacity becomes available to a running mysqld without downtime. Most modern CSI
drivers support this.

### Offline expansion

Some backends cannot expand a volume that is in use: the node-side filesystem
resize stays pending until the volume is detached and remounted. For these, set:

```yaml
spec:
  storage:
    resizeInUseVolumes: false
```

The operator then completes the resize by recycling each instance Pod after
patching its PVC request. The fresh mount lets the kubelet finish the filesystem
grow. Recycling is **serialised the same way as any rolling change**: replicas
are rolled one at a time, each rejoined and healthy before the next, and the
primary is rolled last (after a switchover when applicable), so the cluster keeps
a writable primary throughout. The operator records a `VolumeResize` event on
each Pod it recreates.

This setting also covers the case where a resize is requested while an instance
has no running Pod (for example a fenced or stopped instance): the pending resize
finishes when the operator next creates the Pod and the volume is mounted.

## Observing a resize

PVCs whose expansion has not finished are listed in
`status.resizingPVC`:

```bash
kubectl get cluster cluster-sample -o jsonpath='{.status.resizingPVC}'
```

For an online expansion a name appears only briefly. For an offline expansion a
name lingers until the operator recycles the owning Pod and the new mount
completes the resize. An empty list means no resize is in flight.
