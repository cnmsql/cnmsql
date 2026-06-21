# 012 — User-Defined Exposition

**Status:** done
**Milestone:** M10

Replace hardcoded ClusterIP services with a declarative `spec.managed.services` model (templates, additional services, role-routed selectors) following CNPG's `ManagedService` pattern.

**Goal:** Replace the current hardcoded `ClusterIP` rw/ro/r services with a
declarative `spec.managed.services` model. Users can choose `ClusterIP`,
`LoadBalancer`, or `NodePort`, add custom labels and annotations, and define
additional services with arbitrary selectors — all following the CNPG
`ManagedService` pattern.

## Why

The current default services are always `ClusterIP` with no customization
surface. Users who need `LoadBalancer` exposure, specific annotations for cloud
load-balancer controllers, or additional role-routed services (e.g. a weighted
read pool) have to create and manage those services themselves outside the
operator, losing owner-reference lifecycle. CNPG solved this with
`managed.services.additional` — we follow the same model.

## Scope

### In scope

- **Default service customization**: Users can set `type`, `labels`, and
  `annotations` on the three default services (rw/ro/r) without needing to
  define them as "additional."
- **Additional managed services**: Users can declare extra services with a
  `selectorType` (rw/ro/r), an optional service `template` (type, labels,
  annotations, ports override), and an `updateStrategy` (`patch` or `replace`).
- **`spec.managed.services` API**: Extend `ManagedServices` with
  `additional` (CNPG parity) and a default-service template object.
- **RW service guard**: The `rw` service cannot be disabled (it is always
  created). Attempting to disable it is a validation error.
- **Selector protection**: User-defined services cannot override the Pod
  selector (the operator sets it based on `selectorType`). Attempting to do so
  is a validation error.
- **Service update strategies**:
  - `patch` (default): Merge user-provided metadata and spec onto the operator
    defaults. Labels/annotations are additive; spec fields are applied on top of
    the default.
  - `replace`: User template replaces the operator defaults entirely (except for
    the immutable selector and owner reference).
- **Per-instance headless services remain unchanged**: They are internal and not
  user-configurable.
- **Unit + e2e coverage**.

### Out of scope

- `ManagedConfiguration.Roles` (CNPG manages declarative database roles; we may
  do that in M12 but not here).
- Per-instance headless service customization (always `ClusterIP: None`).
- Advanced service features (externalTrafficPolicy, sessionAffinity,
  healthCheckNodePort) — these are naturally supported by embedding
  `corev1.ServiceSpec` in the template but not individually tested.
- ProxySQL pooler services (M14).

## API design

### ManagedServices (amended)

```go
// ManagedServices controls the services generated for the cluster.
type ManagedServices struct {
    // DisabledDefaultServices is the list of default services (rw, ro, r) to
    // disable. The rw service cannot be disabled.
    // +optional
    DisabledDefaultServices []ServiceSelectorType `json:"disabledDefaultServices,omitempty"`

    // Template applies to the three default services (rw, ro, r). Fields set
    // here are merged into each default service. The operator still chooses
    // the selector and port based on the service role.
    // +optional
    Template *ServiceTemplateSpec `json:"template,omitempty"`

    // Additional is a list of additional managed services specified by the
    // user. Each entry declares a selectorType and an optional template to
    // overlay on top of the role-specific defaults.
    // +optional
    Additional []ManagedService `json:"additional,omitempty"`
}
```

### ManagedService (new)

```go
// ManagedService describes a user-defined managed service.
type ManagedService struct {
    // SelectorType specifies the type of selectors the service will have.
    // Valid values are "rw", "r", and "ro".
    // +kubebuilder:validation:Required
    // +kubebuilder:validation:Enum=rw;r;ro
    SelectorType ServiceSelectorType `json:"selectorType"`

    // Name is the name of the additional service. Must be unique among all
    // managed services and must not collide with the default service names
    // (<cluster>-rw, <cluster>-ro, <cluster>-r).
    // +kubebuilder:validation:Required
    Name string `json:"name"`

    // UpdateStrategy describes how the service template is reconciled with the
    // operator defaults.
    // +kubebuilder:default:="patch"
    // +optional
    UpdateStrategy ServiceUpdateStrategy `json:"updateStrategy,omitempty"`

    // ServiceTemplate is the template specification for the service. When
    // UpdateStrategy is "patch", fields here are merged on top of the
    // role-specific defaults. When "replace", they replace the defaults
    // entirely (except for selector and owner reference).
    // +optional
    ServiceTemplate ServiceTemplateSpec `json:"serviceTemplate,omitempty"`
}
```

### ServiceUpdateStrategy (new)

```go
// ServiceUpdateStrategy describes how the service template is reconciled.
// +kubebuilder:validation:Enum=patch;replace
type ServiceUpdateStrategy string

const (
    // ServiceUpdateStrategyPatch merges user fields onto operator defaults.
    ServiceUpdateStrategyPatch ServiceUpdateStrategy = "patch"
    // ServiceUpdateStrategyReplace replaces operator defaults with the user template.
    ServiceUpdateStrategyReplace ServiceUpdateStrategy = "replace"
)
```

### ServiceTemplateSpec (new, embeds K8s types)

```go
// ServiceTemplateSpec describes the user-customisable parts of a managed
// Service.
type ServiceTemplateSpec struct {
    // Standard object's metadata applied to the Service.
    // +optional
    ObjectMeta *ObjectMetaTemplate `json:"metadata,omitempty"`

    // Specification of the desired behavior of the Service. The selector
    // field is operator-managed and cannot be overridden.
    // +optional
    Spec *ServiceTemplateServiceSpec `json:"spec,omitempty"`
}

// ObjectMetaTemplate carries the user-configurable metadata fields.
type ObjectMetaTemplate struct {
    // Labels added to the Service.
    // +optional
    Labels map[string]string `json:"labels,omitempty"`

    // Annotations added to the Service.
    // +optional
    Annotations map[string]string `json:"annotations,omitempty"`
}

// ServiceTemplateServiceSpec exposes the subset of corev1.ServiceSpec fields
// that users are allowed to customise.
type ServiceTemplateServiceSpec struct {
    // Type determines how the Service is exposed. Defaults to ClusterIP.
    // +kubebuilder:validation:Enum=ClusterIP;NodePort;LoadBalancer;ExternalName
    // +optional
    Type *corev1.ServiceType `json:"type,omitempty"`

    // ExternalTrafficPolicy describes how nodes distribute service traffic.
    // +optional
    ExternalTrafficPolicy *corev1.ServiceExternalTrafficPolicyType `json:"externalTrafficPolicy,omitempty"`

    // SessionAffinity configures session affinity.
    // +optional
    SessionAffinity *corev1.ServiceAffinity `json:"sessionAffinity,omitempty"`

    // LoadBalancerSourceRanges restricts load balancer access.
    // +optional
    LoadBalancerSourceRanges []string `json:"loadBalancerSourceRanges,omitempty"`

    // ExternalName is the external reference for ExternalName services.
    // +optional
    ExternalName string `json:"externalName,omitempty"`

    // HealthCheckNodePort specifies the health check node port.
    // +optional
    HealthCheckNodePort *int32 `json:"healthCheckNodePort,omitempty"`
}
```

**Note:** We wrap `corev1.ServiceSpec` fields explicitly rather than embedding
it directly. This prevents users from accidentally setting `selector`, `ports`,
`clusterIP`, or `loadBalancerIP` — all of which are either operator-managed or
forbidden. The operator retains full control over selector (based on
`selectorType`), port (`mysql:3306`), and owner reference.

### Example: LoadBalancer read-write, custom read service

```yaml
apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: exposed-cluster
spec:
  instances: 3
  imageName: cnmsql-instance:8.4
  storage:
    size: 10Gi
  managed:
    services:
      template:
        metadata:
          labels:
            app.kubernetes.io/part-of: my-app
          annotations:
            service.beta.kubernetes.io/aws-load-balancer-scheme: internal
      additional:
        - name: mysql-lb
          selectorType: rw
          serviceTemplate:
            spec:
              type: LoadBalancer
        - name: mysql-internal-read
          selectorType: ro
          updateStrategy: replace
          serviceTemplate:
            metadata:
              labels:
                pool: reporting
            spec:
              type: ClusterIP
```

## Validation (cluster_funcs.go)

Add `validateManagedServices` called from `ClusterSpec.ValidateCreate/Update`:

1. **`rw` cannot be disabled**: If `DisabledDefaultServices` contains `"rw"`,
   reject with "the rw service cannot be disabled."
2. **No duplicate names**: The names of `Additional` entries must be unique.
3. **No reserved names**: An additional service cannot be named `<cluster>-rw`,
   `<cluster>-ro`, or `<cluster>-r` (the default service names). Note: this
   validation runs in the API package which does not have access to the
   cluster name; we reject the well-known suffixes and let the controller
   catch the fully-qualified collision at reconciliation time.
4. **No user-set selector**: `ServiceTemplateSpec.Spec` must not expose a
   selector field. (Enforced by design — `ServiceTemplateServiceSpec` has no
   `Selector` field.)
5. **Valid selectorType**: Already enforced by `+kubebuilder:validation:Enum`.
6. **Valid updateStrategy**: Already enforced by `+kubebuilder:validation:Enum`.

Additional validation for the `template` field:
7. **Default template spec.type**: Must be a valid `ServiceType` if set.
   (Enforced by `+kubebuilder:validation:Enum`.)

## Controller changes

### cluster_services.go

Rewrite `ensureDefaultServices` to apply the default `template`:

1. For each of the three default services (rw/ro/r):
   - Start with the base spec: selector from role, port 3306, `ClusterIP` type.
   - If `spec.managed.services.template` is set, merge its `ObjectMeta`
     (labels/annotations) and `Spec` (type, externalTrafficPolicy, etc.) on top.
   - Apply `PublishNotReadyAddresses` for ro/r (not rw).
2. Add `ensureAdditionalServices` that iterates `additional`:
   - For each entry, build the role-specific base the same way.
   - Apply `ServiceTemplate` on top with `updateStrategy`:
     - `patch`: Deep-merge metadata and spec fields.
     - `replace`: Replace metadata and spec entirely (selector + owner ref
       preserved).
   - Drop any user-set selector from the template.
3. `ensureRoutingService` gains a `ServiceTemplateSpec` and
   `ServiceUpdateStrategy` parameter.

### cluster_plan.go

- `clusterPlan` gains `ServiceTemplate *ServiceTemplateSpec` and
  `AdditionalServices []ManagedService`.
- Plan construction copies them from `cluster.Spec.Managed.Services`.

### Service name collision

Additional services get fully-qualified names when rendered into plan:
`<cluster>-<entry.Name>`. Default service names are `<cluster>-rw`,
`<cluster>-ro`, `<cluster>-r`. At plan-build time, check for collisions between
additional and default service names, rejecting with a clear condition.

### Labels and annotations merging

Default labels (cluster identity, role) are set first. User labels from
`template.metadata.labels` are then applied on top (user wins on conflict).
Annotations are additive (both survive). For `replace` strategy on additional
services, only user labels/annotations are kept (plus the mandatory operator
labels for owner tracking).

## Per-instance headless services

No change. These remain `ClusterIP: None`, no user customization, purely
internal. Documented as such.

## Implementation order

1. Add new API types: `ManagedService`, `ServiceUpdateStrategy`,
   `ServiceTemplateSpec`, `ObjectMetaTemplate`, `ServiceTemplateServiceSpec`
   in `api/v1alpha1/cluster_types.go`.
2. Amend `ManagedServices` with `Template` and `Additional` fields.
3. Run `make generate manifests` (new CRD fields).
4. Add `validateManagedServices` in `api/v1alpha1/cluster_funcs.go` and wire
   into `ClusterSpec.ValidateCreate/Update`.
5. Unit-test the validation.
6. Add `buildManagedServices` + `buildAdditionalService` helpers in a new file
   or in `cluster_services.go`.
7. Rewrite `ensureDefaultServices` to apply the default template.
8. Implement `ensureAdditionalServices`.
9. Add `ServiceTemplate` + `AdditionalServices` to `clusterPlan` and its
   construction.
10. Unit tests: template merging, update strategies, service spec generation,
    name collision detection, rw-disable rejection.
11. `make lint-fix && make test`.
12. Kind e2e: create a Cluster with LoadBalancer rw service, custom annotations
    on default services, an additional read service, and a replace-strategy
    service. Assert generated Services match expectations.
13. Docs: update `backup-recovery.md` YAML examples to show `managed.services`
    usage, update `cluster-lifecycle.md` with the new service model.

## Testing

### Unit
- `validateManagedServices`: rw in disabledDefaultServices → rejected; duplicate
  additional names → rejected; suffix collision with rw/ro/r → rejected.
- `buildManagedServices`: template merges correctly onto each default role;
  `patch` strategy overlays user fields; `replace` strategy swaps entirely;
  selector is never user-settable; owner reference is always set.
- `clusterPlan.ServiceTemplate` / `AdditionalServices` populated from spec.

### E2E
- Cluster with `managed.services.template` setting `type: LoadBalancer` on
  defaults → rw service has `type: LoadBalancer`, ro/r have `type: ClusterIP`
  (default, unless template overrides).
- Additional service with `selectorType: ro` → created with replica selector and
  custom name.
- Additional service with `updateStrategy: replace` and custom labels →
  operator-default labels gone, user labels present.
- Disabling `ro` still works; disabling `rw` is rejected on create.

## Acceptance criteria

- Users can set `type`, `labels`, `annotations` on default rw/ro/r services via
  `spec.managed.services.template`.
- Users can declare additional managed services with arbitrary names, selectors
  (rw/ro/r), and service specs via `spec.managed.services.additional`.
- `patch` strategy merges user fields onto operator defaults; `replace` strategy
  replaces them.
- The `rw` service cannot be disabled. Additional service names must be unique
  and not collide with default names.
- User-set selectors are rejected by construction (not exposed in the type).
- Existing `DisabledDefaultServices` behavior (disable ro/r) continues working.
- Per-instance headless services remain unchanged.
- `make generate manifests`, `make lint-fix`, `make test`, and the M10 Kind e2e
  pass.
- Docusaurus docs updated and `npm run build` passes.
