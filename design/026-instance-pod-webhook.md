# 026 — Admission Webhook for Instance Pod Self-Patch

**Status:** done

Adds a validating admission webhook that constrains what a Group Replication instance Pod may change when it patches *itself*, complementing the per-instance identity model from [020](020-status-instance-webhook.md).

**Goal:** An instance holds `patch` on its own Pod so it can ring the operator's group-view doorbell. RBAC scopes that to the instance's own Pod but cannot restrict *which fields* the patch touches. This webhook is the field-level gate: an instance may only touch the Group Replication doorbell annotations on its own Pod, and nothing else. Not its container images, ephemeral containers, labels, ownerReferences, finalizers, or any operator-trusted annotation.

## Why (the issue this closes)

Under Group Replication ([022](022-group-replication.md)) the in-Pod reconciler publishes a *doorbell* annotation on its own Pod whenever its locally observed group view changes (`mysql.cnmsql.co/gr-observed`), and it *clears* operator-issued command annotations (`cnmsql.cnmsql.co/force-quorum-members`, `cnmsql.cnmsql.co/force-group-rebootstrap`) once it has executed them. All of this needs `patch` on the Pod.

The per-instance `<cluster>-<ordinal>-gr-doorbell` Role grants that with `resourceNames: [<instance-name>]`, so **RBAC already prevents an instance from patching another instance's Pod.** What RBAC cannot express is field-level access. So before this design a compromised or misbehaving instance could `patch` its own Pod and:

- change `spec.containers[].image`, a mutable field, so the kubelet pulls and restarts with an attacker-chosen image;
- add an ephemeral container (if it could reach the spec) or otherwise mutate the spec;
- rewrite `metadata.labels`, hijacking Service selectors and operator routing;
- forge `metadata.annotations` the operator trusts, faking the fencing annotation or re-issuing a `force-*` command to itself to trigger a quorum-force or group re-bootstrap and fork the group;
- inject a `finalizer` to block its own deletion.

This mirrors the blast-radius problem [020](020-status-instance-webhook.md) solved for `clusters/status`, and reuses the same identity model and field-masking technique.

## Design

### Validating webhook

A custom admission webhook is registered at:

```
/validate--v1-pod
```

It validates `UPDATE` requests to the core `pods` resource, **not** the `pods/status` subresource, so the kubelet's high-frequency status heartbeats never invoke it.

#### Rule 1: identify the caller

The webhook reuses `instanceIdentity` from [020](020-status-instance-webhook.md), deriving the cluster name from the **old** (stored) Pod's `mysql.cnmsql.co/cluster` label. The requester cannot forge that value, because the old object is what the API server already persisted.

If the caller is **not** an instance ServiceAccount (the operator reconciling annotations/labels, the kubelet, a human), the request is admitted subject to normal Kubernetes RBAC.

#### Rule 2: an instance may only patch its own Pod

For instance identity `demo-1-instance`, the request name must be `demo-1`. RBAC `resourceNames` is the primary gate; this re-assertion is defence in depth and ensures the field rules below are always reasoned about against the caller's own Pod.

#### Rule 3: the Pod spec and identity metadata are immutable to an instance

`spec`, `labels`, `ownerReferences`, and `finalizers` must be byte-identical between old and new. This blocks container-image swaps, ephemeral containers and every other spec mutation, label hijacking, ownerReference tampering, and finalizer injection.

#### Rule 4: only the doorbell annotations may change

After the other metadata is fixed, the annotation delta is constrained to:

- `mysql.cnmsql.co/gr-observed`: freely writable. It is a low-trust doorbell: the operator reads it and re-derives ground truth from MySQL, so forging it only causes a redundant observation.
- `cnmsql.cnmsql.co/force-quorum-members` and `cnmsql.cnmsql.co/force-group-rebootstrap`: the instance may only **clear** them (present to absent or empty) to acknowledge a command it has executed. Setting or changing them to a non-empty value is denied, so a compromised member cannot self-issue a quorum-force or re-bootstrap and fork the group.

Every other annotation must be byte-identical.

#### Failure policy and blast radius

`failurePolicy: Fail`, but scoped by an `objectSelector` on the `mysql.cnmsql.co/cluster` label (added via a kustomize patch, since the kubebuilder marker cannot express `objectSelector`). So unlike a naive cluster-wide Pod webhook, an outage can only block `UPDATE` on the operator's own instance Pods, not every Pod in the cluster. This mirrors [020](020-status-instance-webhook.md)'s reasoning: the webhook is served by the operator Pod, so if it is down the operator, the main writer of those Pods, is almost certainly down too.

### Namespaced deployment mode

The `objectSelector` matches instance Pods by their `mysql.cnmsql.co/cluster` label in **every** namespace. That is fine for a single cluster-wide operator, but when several namespaced operators cohabit one cluster (see [021](021-deployment-modes.md)), `tenant-a`'s Pod webhook would otherwise fire for `tenant-b`'s instance Pods and, under `failurePolicy: Fail`, block them while `tenant-a` is down. So the `config/namespaced` overlay adds a `namespaceSelector` to this webhook too, alongside the two Cluster webhooks, keyed on `kubernetes.io/metadata.name` and filled in with the operator's own namespace. The two selectors are ANDed: same namespace and carries the cluster label.

## Implementation changes

| File | Change |
|------|--------|
| `internal/webhook/v1alpha1/pod_webhook.go` | New `InstancePodValidator` handler with the rules above. |
| `internal/webhook/v1alpha1/cluster_webhook.go` | `SetupClusterWebhookWithManager` also registers `/validate--v1-pod`. |
| `config/webhook/manifests.yaml` | Generated `ValidatingWebhookConfiguration` entry, scoped to `pods` UPDATE. |
| `config/webhook/patches/pod_instance_selector.yaml` | Kustomize patch injecting the `objectSelector` on the instance-Pod webhook. |
| `config/webhook/kustomization.yaml` | Wires the new patch. |
| `internal/webhook/v1alpha1/pod_webhook_test.go` | Table-driven unit tests. |

## Testing

Unit tests in `pod_webhook_test.go` cover:

- Operator account changing any field (including the image) is allowed.
- Instance ringing the `gr-observed` doorbell is allowed.
- Instance clearing an operator `force-*` command is allowed; setting one is denied.
- Instance swapping its container image, adding an ephemeral container, hijacking labels, injecting a finalizer, or forging an operator-trusted annotation is denied.
- Instance patching another instance's Pod is denied.
