---
title: "Autoscaling"
description: "Targeting a cnmsql Cluster with the Horizontal and Vertical Pod Autoscalers through the scale subresource."
sidebar_position: 15
---

# Autoscaling

Every Cluster exposes a Kubernetes scale subresource, so the standard Horizontal
Pod Autoscaler (HPA) and Vertical Pod Autoscaler (VPA) can target it with no
cnmsql-specific configuration. The subresource maps three paths onto the Cluster:

| Scale field | Cluster field | Meaning |
| --- | --- | --- |
| `spec.replicas` | `spec.instances` | Desired instance count. |
| `status.replicas` | `status.instances` | Observed instance count. |
| `status.selector` | `status.labelSelector` | Label selector for the instance Pods. |

The operator writes `status.labelSelector` on every reconcile as
`mysql.cnmsql.co/cluster=<cluster-name>`, which matches every instance Pod the
cluster owns. Autoscalers use it to find the Pods they measure and resize.

Read the live scale view with:

```bash
kubectl get cluster cluster-sample --subresource=scale -o yaml
```

## Horizontal scaling

An HPA changes the number of instances by writing `spec.replicas` on the scale
subresource, which cnmsql applies to `spec.instances`. That is the same field you
set by hand (see [Operations](./operations.md#scale-up)), so HPA-driven scaling
follows the same rules: replicas are added one at a time, each cloned from the
primary before the next starts; scale-down removes the highest-ordinal replicas
first and keeps their PVCs; and the cluster never drops below one instance or
removes the current primary.

This example scales on CPU between 3 and 7 instances:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: cluster-sample
spec:
  scaleTargetRef:
    apiVersion: mysql.cnmsql.co/v1alpha1
    kind: Cluster
    name: cluster-sample
  minReplicas: 3
  maxReplicas: 7
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```

Adding a MySQL replica is not free. Each new instance clones the primary's data
before it can serve traffic, so a burst of scale-ups is slower and heavier than
scaling a stateless workload. Give the HPA a conservative `behavior.scaleUp`
stabilization window so it does not react to short spikes. Horizontal scaling
helps a read-heavy workload whose replicas serve reads; it does nothing for write
throughput, because every instance still applies the same writes.

## Vertical scaling

A VPA resizes the CPU and memory of the instance Pods instead of changing their
count. It discovers the Pods through the scale selector above, reads their usage,
and applies its recommendation to the running Pods.

`spec.resources` on the Cluster is the baseline requests and limits the operator
stamps on every instance Pod when it creates one. A VPA overrides those values on
the live Pods. It does not edit `spec.resources`.

This example lets the VPA manage the `mysql` container's requests, with a floor
that keeps mysqld from being starved:

```yaml
apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: cluster-sample
spec:
  targetRef:
    apiVersion: mysql.cnmsql.co/v1alpha1
    kind: Cluster
    name: cluster-sample
  updatePolicy:
    updateMode: "Auto"
  resourcePolicy:
    containerPolicies:
      - containerName: mysql
        controlledValues: RequestsOnly
        minAllowed:
          cpu: 250m
          memory: 1Gi
        maxAllowed:
          cpu: "4"
          memory: 8Gi
```

### The operator does not revert VPA changes

cnmsql decides whether to recreate an instance Pod by comparing a template hash.
It computes the hash from the desired Pod spec, which includes `spec.resources`,
and stores it as an annotation when it creates the Pod. On each reconcile it
recomputes the desired hash and compares it to the stored annotation. It never
reads the live Pod's resource values for this check.

A VPA changes the resources on a running Pod, not `spec.resources`, so the
recomputed hash still matches the stored annotation and the operator leaves the
Pod alone. Editing `spec.resources` yourself does change the hash, which rolls
each Pod once to apply the new baseline (replicas first, primary last). So the
VPA owns the live values, you own the baseline, and the two do not fight.

### Update modes and eviction

How a VPA applies a new recommendation depends on its `updateMode`:

| Mode | Behavior with cnmsql |
| --- | --- |
| `Off` | Recommendations only. Nothing changes; read them from the VPA's `status`. |
| `Initial` | Applied when a Pod is created, through the admission webhook. A Pod keeps its values until something else recreates it. |
| `Auto` / `Recreate` | The VPA evicts a Pod so it comes back with new values. The operator recreates the evicted Pod. |
| `InPlaceOrRecreate` | On Kubernetes 1.33 and later, the VPA resizes a running Pod in place and evicts only if it cannot. |

`Auto` evicts Pods, and two cnmsql behaviors shape what happens:

- PodDisruptionBudgets gate the eviction. The operator keeps a PDB that allows at
  most one primary disruption and at most `floor(replicas / 2)` replica
  disruptions at a time, or a single quorum-aware PDB under Group Replication. The
  VPA updater also refuses to evict a target that has fewer Pods than its
  `--min-replicas` setting (2 by default), so the updater never evicts a
  single-instance cluster.
- Evicting the primary triggers a failover, or a controlled switchover before the
  Pod stops when switchover-on-drain is enabled. Prefer `InPlaceOrRecreate` where
  it is available so a resize does not force a primary change.

### Running a VPA and an HPA together

Do not point an HPA and a VPA at the same resource metric. If both watch CPU, the
VPA raises requests while the HPA adds instances off the same signal, and they
oscillate. Use a VPA for CPU and memory together with an HPA on a custom or
external metric, or run one autoscaler at a time.
