# 023 — DatabaseUser CR

**Status:** done
**Milestone:** M-DBU

Add a standalone `DatabaseUser` CRD: a cluster-installation-wide MySQL user,
managed declaratively by an end user, with grants on arbitrary targets. Unlike
the inline users embedded in the `Database` CR, a `DatabaseUser` is **not scoped
to a single database** — it owns one MySQL account (`name@host`) and any set of
grants across the whole instance.

## Why

cnmsql has three ways to bring a MySQL user into existence today, and none of
them gives an end user a first-class, self-service object for an
installation-wide account:

1. **`spec.bootstrap.initdb`** — one-shot at cluster init, no reconciliation.
2. **`spec.managed.roles`** (`RoleConfiguration`, design 014) — declarative and
   reconciled, but lives **inside the Cluster spec**. Editing it means editing
   the Cluster CR, which is typically owned by the platform/operator team, not
   the application team. It also has no per-user status object, no per-user
   finalizer/reclaim, and no per-user RBAC surface.
3. **`Database.spec.users[]`** (`DatabaseUser` struct, design 014) — reconciled
   by the `DatabaseReconciler`, but the user's lifecycle is **tied to a single
   `Database`** and grants default to that one schema. There is no way to express
   "an account that spans several schemas" or "an account with no owning
   database" without contorting the model.

The gap: an **application team** wants to manage *their* MySQL user the same way
they manage a `Database` — a small namespaced CR they can `kubectl apply`,
with its own status, its own password Secret, its own grants, and its own
deletion semantics — without touching the Cluster CR and without pinning the
user to one schema.

`DatabaseUser` is to `spec.managed.roles` what `Database` is to
`spec.bootstrap.initdb`: the standalone, reconciled, self-service form.

## Naming collision (must resolve first)

The identifier `DatabaseUser` is **already taken** in
[api/v1alpha1/database_types.go](../api/v1alpha1/database_types.go) — it is the
struct embedded in `DatabaseSpec.Users`. We cannot have two Go types named
`DatabaseUser` in package `v1alpha1`, and only a top-level `+kubebuilder:object:root`
type can become a Kind.

**Recommendation:** rename the existing embedded struct to **`InlineUser`**
(it is only ever used as `Database.spec.users[]`), freeing `DatabaseUser` for the
new Kind. This is a source-level rename only — the JSON field stays `users` and
each element keeps the same shape, so **no CRD wire change** for `Database`.

Touch points for the rename:

- `DatabaseSpec.Users []DatabaseUser` → `[]InlineUser` in
  [database_types.go](../api/v1alpha1/database_types.go).
- `reconcileDatabaseUser(... du *mysqlv1alpha1.DatabaseUser ...)` and friends in
  [database_controller.go](../internal/controller/database_controller.go).
- `zz_generated.deepcopy.go` (regenerated).

Alternative considered: name the new Kind `MySQLUser` or `User`. Rejected —
`DatabaseUser` is what the user asked for, reads well next to `Database`, and the
embedded struct is the less-prominent of the two.

## Architecture

Same operator-side reconciliation model as design 014 (see its "Architecture
Decision" section): the operator owns policy, the instance manager stays a thin
SQL executor reached over the existing mTLS control API. **No new instance
manager endpoints or SQL builders are needed** — `DatabaseUser` reuses the exact
control-client surface that managed roles and inline users already use:

- `ListUsers`, `CreateUser`, `AlterUser`, `DropUser`
- `user.CreateUserRequest` / `AlterUserRequest` / `DropUserRequest` /
  `Privilege` from `pkg/management/mysql/user`

This makes the CR almost entirely an operator-side controller + API-types change.

## API

New file `api/v1alpha1/databaseuser_types.go`.

```go
// DatabaseUserSpec defines the desired state of DatabaseUser.
type DatabaseUserSpec struct {
    // Cluster references the MySQL cluster this user belongs to.
    // +kubebuilder:validation:Required
    Cluster LocalObjectReference `json:"cluster"`

    // Name is the MySQL user name. Defaults to the resource name if empty.
    // +kubebuilder:validation:MaxLength=32
    // +optional
    Name string `json:"name,omitempty"`

    // Host the user connects from. Defaults to "%".
    // +kubebuilder:default:=`%`
    // +optional
    Host string `json:"host,omitempty"`

    // Ensure controls whether the user is created or dropped.
    // +kubebuilder:default:=present
    // +optional
    Ensure EnsureOption `json:"ensure,omitempty"`

    // PasswordSecret references the secret key holding the user's password.
    // Required when Ensure=present.
    // +optional
    PasswordSecret *SecretKeySelector `json:"passwordSecret,omitempty"`

    // Superuser grants ALL PRIVILEGES on *.* WITH GRANT OPTION. Mutually
    // exclusive with Grants.
    // +kubebuilder:default:=false
    // +optional
    Superuser bool `json:"superuser,omitempty"`

    // MaxUserConnections resource limit. 0 = no limit.
    // +kubebuilder:validation:Minimum=0
    // +optional
    MaxUserConnections int32 `json:"maxUserConnections,omitempty"`

    // MaxQueriesPerHour resource limit. 0 = no limit.
    // +kubebuilder:validation:Minimum=0
    // +optional
    MaxQueriesPerHour int32 `json:"maxQueriesPerHour,omitempty"`

    // MaxUpdatesPerHour resource limit. 0 = no limit.
    // +kubebuilder:validation:Minimum=0
    // +optional
    MaxUpdatesPerHour int32 `json:"maxUpdatesPerHour,omitempty"`

    // MaxConnectionsPerHour resource limit. 0 = no limit.
    // +kubebuilder:validation:Minimum=0
    // +optional
    MaxConnectionsPerHour int32 `json:"maxConnectionsPerHour,omitempty"`

    // RequireTLS sets REQUIRE X509, REQUIRE SSL, or none.
    // +kubebuilder:validation:Enum=x509;ssl;none
    // +kubebuilder:default:=none
    // +optional
    RequireTLS string `json:"requireTLS,omitempty"`

    // Comment is an optional user comment.
    // +optional
    Comment string `json:"comment,omitempty"`

    // Grants is the list of grants applied to the user. Targets are
    // installation-wide and have no default schema (unlike Database.spec.users).
    // +optional
    Grants []DatabaseUserGrant `json:"grants,omitempty"`

    // ReclaimPolicy controls what happens to the MySQL user when the
    // DatabaseUser object is deleted.
    // +kubebuilder:validation:Enum=delete;retain
    // +kubebuilder:default:=retain
    // +optional
    ReclaimPolicy string `json:"reclaimPolicy,omitempty"`
}

// DatabaseUserGrant describes a single MySQL GRANT statement.
type DatabaseUserGrant struct {
    // Privileges is the list of privileges (e.g. "SELECT", "INSERT", "ALL").
    // +kubebuilder:validation:MinItems=1
    Privileges []string `json:"privileges"`

    // On is the target of the grant (e.g. "*.*", "mydb.*", "mydb.mytable").
    // Defaults to "*.*".
    // +kubebuilder:default:=`*.*`
    // +optional
    On string `json:"on,omitempty"`
}

// DatabaseUserStatus defines the observed state of DatabaseUser.
type DatabaseUserStatus struct {
    // Applied is true once the desired state has been reconciled.
    // +optional
    Applied *bool `json:"applied,omitempty"`

    // Message provides additional detail, typically an error.
    // +optional
    Message string `json:"message,omitempty"`

    // ObservedGeneration is the generation observed by the controller.
    // +optional
    ObservedGeneration int64 `json:"observedGeneration,omitempty"`

    // PasswordSecretResourceVersion records the source Secret resourceVersion
    // last applied to MySQL, so the password is re-applied only when it changes.
    // +optional
    PasswordSecretResourceVersion string `json:"passwordSecretResourceVersion,omitempty"`

    // Conditions represent the latest observations of the user state.
    // +listType=map
    // +listMapKey=type
    // +optional
    Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=myuser
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.cluster.name`
// +kubebuilder:printcolumn:name="User",type=string,JSONPath=`.spec.name`
// +kubebuilder:printcolumn:name="Applied",type=boolean,JSONPath=`.status.applied`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type DatabaseUser struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`
    Spec   DatabaseUserSpec   `json:"spec"`
    Status DatabaseUserStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type DatabaseUserList struct {
    metav1.TypeMeta `json:",inline"`
    metav1.ListMeta `json:"metadata,omitempty"`
    Items           []DatabaseUser `json:"items"`
}
```

Notes:

- Status uses a **single** `PasswordSecretResourceVersion` string, not the
  `map[string]string` `PasswordStatus` the `Database` controller needs — a
  `DatabaseUser` owns exactly one account, so there is nothing to key by.
- The `EnsureOption`, `LocalObjectReference`, `SecretKeySelector`, and the
  `ConditionReady` condition constant are reused as-is.
- The attribute set (superuser, resource limits, RequireTLS, comment) is
  deliberately the **same surface as `RoleConfiguration`** so the two stay
  conceptually interchangeable and a future migration tool is trivial.

## Validation

Webhook / `*.SetDefaults` + a `validateDatabaseUser()` helper (mirroring
`validateManagedRoles` in [cluster_funcs.go](../api/v1alpha1/cluster_funcs.go)):

- `Name` (resolved, defaulting to `metadata.name`) must not be a reserved
  account — reuse `isReservedRoleName` (`root`, `mysql.*`, `cnmsql_*`).
- `Host` must be non-empty (the default `%` covers the common case).
- `Superuser=true` is mutually exclusive with non-empty `Grants`.
- `RequireTLS` ∈ {x509, ssl, none}.
- When `Ensure=present`, `PasswordSecret` should be set (the controller also
  enforces this at reconcile time, matching `resolveUserPassword`).

## Controller

New `DatabaseUserReconciler` in `internal/controller/databaseuser_controller.go`,
structurally a trimmed copy of `DatabaseReconciler`:

- Finalizer `mysql.cnmsql.co/databaseuser`.
- `reclaimDelete` reuse: on delete with `ReclaimPolicy=delete`, `DropUser` then
  release the finalizer; with `retain`, just release.
- Reconcile steady state:
  1. Fetch `DatabaseUser`; resolve `Cluster` + `status.currentPrimary`
     (same ClusterNotFound / PrimaryNotReady handling as `DatabaseReconciler`).
  2. `Ensure=absent` → `DropUser` if present, mark applied.
  3. `Ensure=present`:
     - `ListUsers` once, look up `roleKey(name, host)`.
     - Resolve password from `PasswordSecret` (reuse the existing
       `resolveUserPassword` logic, generalised off the `InlineUser` type).
     - Build `[]user.Privilege` from `Grants` (or the superuser grant when
       `Superuser=true`); **no schema defaulting** — `On` defaults to `*.*`.
     - Not present → `CreateUser`. Present → diff password
       (`PasswordSecretResourceVersion`) + attributes + grants
       (reuse the `*Satisfied` grant-comparison helpers), `AlterUser` only on
       change.
  4. `markApplied` / `markNotApplied` writing `status.applied`,
     `ObservedGeneration`, `Ready` condition, and
     `PasswordSecretResourceVersion`.
- Requeue cadence: `provisioningRequeue` while waiting, `readyResync` when
  applied — same constants as the other controllers.

### Shared-helper extraction

`reconcileDatabaseUser` in
[database_controller.go](../internal/controller/database_controller.go) and the
new controller share almost all per-user logic (password resolution, grant diff,
create/alter/drop dispatch). Factor the common bits into helpers that take the
control client + plain args (name/host/privs/secretRV) so both the inline path
and the `DatabaseUser` path call the same code. This keeps grant-comparison and
password-rotation behaviour identical and avoids a second copy drifting.

### RBAC

```go
// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=databaseusers,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=databaseusers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=databaseusers/finalizers,verbs=update
// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=clusters,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
```

Register the reconciler in `cmd/main.go` alongside `DatabaseReconciler`.

## Conflict & ownership semantics

Three mechanisms can now target the same `name@host`: `spec.managed.roles`,
`Database.spec.users[]`, and `DatabaseUser`. MySQL has one account, so the last
writer wins and reconcilers can fight.

**Decision for v1:** do **not** add cross-resource locking. Instead:

- **Document** that a given `name@host` should be owned by exactly one
  mechanism, and recommend `DatabaseUser` as the preferred standalone form.
- **Best-effort detection:** the `DatabaseUser` reconciler emits a `Warning`
  event (`UserConflict`) and sets a non-fatal condition if the resolved
  `name@host` also appears in the referenced Cluster's `spec.managed.roles`.
  (Cluster spec is already in the operator's cache; inline-`Database` users are
  not cheaply discoverable and are left to documentation.)
- A hard, admission-time conflict check across CRs is **out of scope** (would
  need an indexer + webhook); revisit if it bites.

### Adoption of pre-existing accounts

The reconciler must distinguish "I created this account" from "this account was
already in MySQL before I ever ran". The signal is **our own status**: a
`DatabaseUser` that has never successfully applied has `status.applied == nil`
(and `ObservedGeneration == 0`).

On reconcile of an `Ensure=present` user, after `ListUsers`:

| `name@host` in MySQL? | `status.applied` | Action |
|---|---|---|
| no | any | `CreateUser` (normal create) |
| yes | non-nil (we own it) | `AlterUser` on diff (normal reconcile) |
| yes | **nil** (never applied by us) | **conflict** — see below |

The last row is the dangerous one: the account exists but this CR has no record
of creating it, so it is likely owned by something else (a managed role, a
hand-made account, another `DatabaseUser`). Blindly `ALTER`-ing it would hijack
or corrupt that owner's account.

Behaviour in the conflict case:

1. **Do not** touch the account. Emit a `Warning` event (`UserConflict`) and set
   a `Ready=False` condition with reason `UserConflict` and a message naming the
   account and telling the user how to adopt it.
2. **Bypass via annotation:** if the object carries
   `mysql.cnmsql.co/adopt: "true"`, the reconciler **adopts** the existing
   account — it proceeds with the normal `AlterUser` diff path (password,
   attributes, grants), records `status.applied=true`, and emits an `Adopted`
   event. Adoption is thereafter implicit (status now says we own it), so the
   annotation can be removed.

This gives a safe default (refuse to clobber unknown accounts) with an explicit,
auditable opt-in to take ownership — the same shape kubernetes uses for
`kubectl apply` field-manager adoption, expressed as a one-shot annotation.

Note this also resolves the managed-role overlap softly: if a managed role and a
`DatabaseUser` both name the account, whichever created it owns it in status; the
other will hit the conflict path and refuse until adopted.

## Recipe: a safe DBaaS admin

A common ask for a DBaaS-style offering is "give the tenant a near-root account
on their data, but make sure they can't break what the operator relies on."

What the operator depends on and must stay tenant-proof:

- The reserved accounts `cnmsql_control`, `cnmsql_repl`, `cnmsql_backup`,
  `cnmsql_metrics` (already protected by `isReservedRoleName` /
  `ListUsersQuery`'s filter for *naming*, but a tenant with the right privileges
  could still `DROP`/`ALTER` the existing ones).
- Replication topology: `CHANGE REPLICATION SOURCE`, `START/STOP REPLICA`,
  `super_read_only` fencing, GTID/server config.
- Server config the operator manages (`SET GLOBAL ...`, plugins, the data dir).

### `ALL ON *.*` is the trap (not a safe recipe)

An earlier draft of this design proposed granting `ALL` (without the `Superuser`
flag, so no `WITH GRANT OPTION`) and reasoned that the tenant could not then
re-grant cluster-control privileges. **That is wrong**, and an e2e test caught it.

In MySQL 8.0, `GRANT ALL [PRIVILEGES] ON *.*` grants all *static* global
privileges **and every dynamic privilege registered on the server at grant time**
— `REPLICATION_SLAVE_ADMIN`, `SYSTEM_VARIABLES_ADMIN`, `GROUP_REPLICATION_ADMIN`,
`CONNECTION_ADMIN`, `SHUTDOWN`, and the rest. The tenant therefore *holds* those
privileges directly; it does not need to grant them to itself, and `WITH GRANT
OPTION` is irrelevant. A global `ALL` is as dangerous as `Superuser: true`, and it
slips past a denylist of named privileges because none are named.

The key distinction: this only applies to the **global** target. `GRANT ALL ON
db.*` is schema-scoped and grants no global dynamic privileges, so it stays safe.

### The supported recipe: enumerate static privileges

Grant the broad **static** data/DDL privileges by name, on `*.*`. None of these
is a dynamic admin privilege, so the tenant gets full power over data and schema
objects across all databases and nothing on the control plane:

```yaml
apiVersion: mysql.cnmsql.co/v1alpha1
kind: DatabaseUser
metadata:
  name: tenant-admin
spec:
  cluster: { name: my-cluster }
  passwordSecret: { name: tenant-admin-pw, key: password }
  grants:
    - privileges: [SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX,
        REFERENCES, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, CREATE VIEW,
        SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, EVENT, TRIGGER]
      on: "*.*"
```

This list is `SafeDBaaSAdminPrivileges()` in `databaseuser_funcs.go`; the kubectl
plugin's `databaseuser dbaas` subcommand scaffolds exactly it.

### Validation (built)

`Validate()` is the safety net, mirroring the `spec.mysql.parameters` denylist
from design 014 applied to grants:

- **Named dynamic privileges** are rejected anywhere: `REPLICATION_SLAVE_ADMIN`,
  `REPLICATION_APPLIER`, `GROUP_REPLICATION_ADMIN`, `GROUP_REPLICATION_STREAM`,
  `SYSTEM_VARIABLES_ADMIN`, `CONNECTION_ADMIN`, `SERVICE_CONNECTION_ADMIN`,
  `PERSIST_RO_VARIABLES_ADMIN`, `BINLOG_ADMIN`, `BINLOG_ENCRYPTION_ADMIN`,
  `CLONE_ADMIN`, `SUPER`, `SHUTDOWN`, `FILE`,
  `GROUP_REPLICATION_FLOW_CONTROL_ADMIN`.
- **`ALL` / `ALL PRIVILEGES` on the global `*.*` target is rejected** (it pulls in
  the dynamic privileges above). `ALL` scoped to a database is allowed.
- `Superuser: true` remains available for trusted cluster-level accounts and is
  documented as unsafe for multi-tenant use.

## CLI layer (kubectl cnmsql)

Extend the kubectl plugin (design 016) so day-2 user management does not require
hand-writing YAML. Surface under a `user` noun that operates on `DatabaseUser`
CRs (and reads the existing managed-role/inline status for visibility):

```
kubectl cnmsql user list      <cluster>                 # all DatabaseUsers (+ managed roles), with Applied/Conflict
kubectl cnmsql user get       <user>                    # spec + status + resolved grants
kubectl cnmsql user create    <user> --cluster c \
        --password-secret s/key --grant "ALL ON app.*" --host '%'
kubectl cnmsql user grant     <user> --add "SELECT ON reports.*" --remove "..."
kubectl cnmsql user passwd    <user> [--secret s/key | --generate]   # rotate; writes/updates Secret
kubectl cnmsql user adopt     <user>                    # sets mysql.cnmsql.co/adopt=true, clears it after apply
kubectl cnmsql user drop      <user> [--reclaim delete|retain]
kubectl cnmsql user dbaas     <user> --cluster c        # scaffolds the safe-DBaaS-superuser recipe above
```

Notes:

- These are **thin CR editors** — `create`/`grant`/`passwd` mutate the
  `DatabaseUser` object (and its Secret) and let the reconciler do the work; they
  do **not** talk to MySQL directly. This keeps a single reconciliation path and
  preserves status/conflict semantics.
- `adopt` is the ergonomic front-end to the
  [adoption](#adoption-of-pre-existing-accounts) annotation, so an operator
  hitting a `UserConflict` has an obvious next step.
- `passwd --generate` creates a strong password, stores it in the referenced
  Secret, and prints it once — the standard rotate-and-show flow.
- `user list` highlights `UserConflict` / not-yet-applied users so day-2 drift is
  visible at a glance.

## Out of scope

- MySQL 8.0 roles (`CREATE ROLE`), role grants.
- Automatic migration of `spec.managed.roles` / inline users into `DatabaseUser`.
- Admission-time cross-resource conflict prevention.
- Per-table column privileges beyond what `On` already expresses.

## Implementation order

1. **Rename** embedded `DatabaseUser` → `InlineUser`; `make generate`; ensure
   tests green (pure refactor, no behaviour change).
2. **API types** `databaseuser_types.go` + defaults + `validateDatabaseUser`;
   `make generate manifests`.
3. **Extract shared per-user helpers** out of `database_controller.go`.
4. **`DatabaseUserReconciler`** + RBAC + `cmd/main.go` wiring.
5. Conflict-detection event/condition + adoption annotation.
6. Grant denylist in `validateDatabaseUser` (safe-DBaaS-superuser net).
7. kubectl plugin `user` noun: extend the existing user CRUD (design 016) to the
   new CR, including `adopt`, `passwd`, and the `dbaas` scaffold.

## Testing

### Unit (`internal/controller/databaseuser_controller_test.go`)

Table-driven, with a fake `InstanceControlClient` (same pattern as
`database_controller_test.go`):

- Create: absent in MySQL → `CreateUser` called with expected privileges/host.
- No-op: present + matching grants/password → no `AlterUser`.
- Password rotation: Secret resourceVersion change → single `AlterUser` with
  password; unchanged → none.
- Grant diff: added/removed grant → `AlterUser` with new privilege set.
- `Ensure=absent`: present → `DropUser`; absent → no-op.
- Reclaim: delete → `DropUser` on finalize; retain → finalizer released, no drop.
- ClusterNotFound / PrimaryNotReady → not-applied status, requeue, no MySQL call.
- **Conflict:** account present + `status.applied==nil` + no adopt annotation →
  no MySQL mutation, `UserConflict` event, `Ready=False`.
- **Adoption:** same precondition + `mysql.cnmsql.co/adopt=true` → `AlterUser`
  diff path runs, `status.applied=true`, `Adopted` event.
- Validation: reserved name, `Superuser`+`Grants`, denylisted dynamic privilege,
  bad `RequireTLS` → rejected.

### Unit (CLI)

- Each `user` subcommand renders the expected `DatabaseUser` mutation (golden
  object) without contacting an API server (fake client).
- `passwd --generate` writes the Secret and prints once; `adopt` sets/clears the
  annotation; `dbaas` emits the safe-superuser recipe.

### Integration (real Percona, build-tagged like existing suites)

- Full lifecycle on a live primary: create installation-wide user with
  multi-schema grants, connect as it, rotate password, alter grants, drop with
  both reclaim policies.
- Adoption against a hand-created account.

### e2e (Kind)

- `DatabaseUser` lifecycle **independent of any `Database`**: apply → Applied,
  rotate password Secret → reconciled, delete with reclaim delete → account gone.
- Conflict + `kubectl cnmsql user adopt` → resolves to Applied.
- Safe-DBaaS-superuser: tenant account can DDL/DML app schemas but is rejected /
  powerless on replication-admin privileges and operator accounts.

## Verification

- `go test ./...` (controller + CLI unit suites above).
- `make lint-fix` (0 issues), `make generate manifests` (no drift).
- Integration tests against real Percona 8.0/8.4/9.x.
- e2e on Kind: the scenarios above.
- `npm run build` from `docs/`.

## Documentation

- New page: `docs/src/database-user.md` (and cross-link from
  `managed-roles-and-databases.md`, explaining when to use which mechanism).
- Update `docs/src/api-reference.md`, `docs/sidebars.js`, and `design/INDEX.md`.
