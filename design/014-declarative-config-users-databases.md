# 014 — Declarative Config, Users, and Databases

**Status:** done
**Milestone:** M12

Add declarative management of MySQL users and databases via Cluster CR (`spec.managed.roles`) and `Database` CRD controller, with all reconciliation in the operator controller.

**Goal:** Add declarative management of MySQL users and databases to the Cluster
CR (managed roles) and implement the Database CRD controller, giving operators a
CNPG-style declarative contract for MySQL schemas and credentials. All
reconciliation runs in the operator controller; the instance manager exposes
thin SQL execution endpoints over its existing mTLS control API.

## Why

cnmsql currently has:

- **Bootstrap-only** database/user creation (`spec.bootstrap.initdb`): runs once
  during cluster initialisation; no ongoing reconciliation.
- **`spec.mysql.parameters`**: declarative `my.cnf` key/value map, already
  rendered to configuration files and Pod-template-hash tracked.
- **Database CRD types**: fully defined in `api/v1alpha1/database_types.go` but
  **no controller** exists.
- **No `spec.managed.roles`**: `ManagedConfiguration` currently only has
  `Services`.

The `spec.mysql.parameters` → `my.cnf` data path is M3-complete and needs no
material M12 work. M12 focuses on **users** and **databases**.

## Architecture Decision: Operator-Side Reconciliation

Reconciliation runs in the **operator controller**, not in the instance manager.
The operator:

1. Owns all policy and decision logic — it has full access to the Cluster CR,
   Secret data, status fields, and cross-resource context.
2. Calls the instance manager's mTLS control API to execute individual SQL
   operations on the primary MySQL instance.
3. The instance manager remains a thin execution layer — it receives a request,
   opens a control connection to MySQL, runs the SQL, and returns the result.

**Rationale:**

- **Separation of concerns**: The operator governs policy (what should exist,
  password rotation, reclaim). The instance manager only knows how to talk to
  mysqld.
- **Operator context**: The operator already reads Cluster status, Secrets, and
  all CRDs. Running reconciliation in the operator avoids duplicating Secret
  watches, status subresource updates, and CRD caches inside every Pod.
- **Consistent pattern**: The operator already calls the instance manager API for
  promote/demote/configure-replica/status. User and database operations extend
  this existing call path.

## Scope

### In scope

1. **Instance manager API** (`pkg/management/mysql/webserver/`):
   - New endpoints: `POST /user/create`, `POST /user/alter`, `POST /user/drop`,
     `GET /user/list`, `POST /database/create`, `POST /database/drop`,
     `GET /database/list`.
   - JSON request/response types.
   - Each endpoint opens a control connection, executes the SQL, and returns.

2. **Managed Roles** (`spec.managed.roles`):
   - New `RoleConfiguration` type with MySQL-native attributes.
   - MySQL user SQL builders.
   - Cluster controller reconciles `spec.managed.roles` against the primary
     instance via the instance manager API.
   - Password change detection via Secret resourceVersion tracking.
   - `status.managedRolesStatus` for visibility.

3. **Database Controller**:
   - New `DatabaseReconciler` in the operator (`internal/controller/`).
   - Watches `Database` CRDs, finds the referenced Cluster, calls instance
     manager API on the primary to execute SQL.
   - Reclaim policy (retain/delete) with finalizer.
   - `status.applied` + `status.conditions`.

4. **Config hardening**:
   - Validate `spec.mysql.parameters` rejects known-dangerous keys.
   - Warn on deprecated/ignored parameters.

### Out of scope

- Tables, MySQL 8.0 roles (`CREATE ROLE`), OCI extensions, instance-manager-side
  watching of CRDs.

---

## M12.1 — Instance Manager SQL Execution API

### New WebServer Endpoints

All endpoints run on the existing mTLS control API (`:8080`). Each is served
by an `actionHandler`-style handler that deserialises a JSON body, opens a MySQL
control connection, executes SQL, and returns JSON.

```
POST /user/create
POST /user/alter
POST /user/drop
GET  /user/list
POST /database/create
POST /database/drop
GET  /database/list
```

### Request / Response Types

```go
// webserver/user.go

type CreateUserRequest struct {
    Name               string   `json:"name"`
    Host               string   `json:"host"`
    Password           string   `json:"password"`
    Superuser          bool     `json:"superuser"`
    MaxUserConnections int32    `json:"maxUserConnections"`
    MaxQueriesPerHour  int32    `json:"maxQueriesPerHour"`
    MaxUpdatesPerHour  int32    `json:"maxUpdatesPerHour"`
    MaxConnectionsPerHour int32 `json:"maxConnectionsPerHour"`
    RequireTLS         string   `json:"requireTLS"`
    Privileges         []PrivilegeRequest `json:"privileges"`
}

type AlterUserRequest struct {
    Name               string   `json:"name"`
    Host               string   `json:"host"`
    Password           *string  `json:"password,omitempty"`
    Superuser          *bool    `json:"superuser,omitempty"`
    MaxUserConnections *int32   `json:"maxUserConnections,omitempty"`
    MaxQueriesPerHour  *int32   `json:"maxQueriesPerHour,omitempty"`
    MaxUpdatesPerHour  *int32   `json:"maxUpdatesPerHour,omitempty"`
    MaxConnectionsPerHour *int32 `json:"maxConnectionsPerHour,omitempty"`
    RequireTLS         *string  `json:"requireTLS,omitempty"`
    Privileges         *[]PrivilegeRequest `json:"privileges,omitempty"`
}

type DropUserRequest struct {
    Name string `json:"name"`
    Host string `json:"host"`
}

type PrivilegeRequest struct {
    Privileges []string `json:"privileges"`
    On         string   `json:"on"`
}

type ListUsersResponse struct {
    Users []UserInfo `json:"users"`
}

type UserInfo struct {
    Name    string   `json:"name"`
    Host    string   `json:"host"`
    Grants  []string `json:"grants"`
}

// webserver/database.go

type CreateDatabaseRequest struct {
    Name         string `json:"name"`
    CharacterSet string `json:"characterSet,omitempty"`
    Collation    string `json:"collation,omitempty"`
}

type DropDatabaseRequest struct {
    Name string `json:"name"`
}

type ListDatabasesResponse struct {
    Databases []string `json:"databases"`
}
```

### Handler Wiring

Extend `InstanceController` interface:

```go
type InstanceController interface {
    // ... existing methods ...

    CreateUser(ctx context.Context, req CreateUserRequest) error
    AlterUser(ctx context.Context, req AlterUserRequest) error
    DropUser(ctx context.Context, req DropUserRequest) error
    ListUsers(ctx context.Context) (*ListUsersResponse, error)
    CreateDatabase(ctx context.Context, req CreateDatabaseRequest) error
    DropDatabase(ctx context.Context, req DropDatabaseRequest) error
    ListDatabases(ctx context.Context) (*ListDatabasesResponse, error)
}
```

Register routes in `Handler()`.

### SQL Builders

New package `pkg/management/mysql/user/` (mirroring `replication/`):

```go
func CreateUserStatement(v version.Version, req CreateUserRequest) ([]string, error)
func AlterUserStatement(v version.Version, req AlterUserRequest) ([]string, error)
func DropUserStatement(name, host string) string
func CreateDatabaseStatement(req CreateDatabaseRequest) string
func DropDatabaseStatement(name string) string
func ListUsersQuery() string
func ListDatabasesQuery() string
func ShowGrantsQuery(user, host string) string
```

Version-aware: MySQL 5.6 uses `SET PASSWORD` / no `IF NOT EXISTS`; MySQL 5.7.6+
uses `ALTER USER ... IDENTIFIED BY` / `CREATE USER IF NOT EXISTS`.

### Operator HTTP Client

Extend `HTTPControlClient` in `internal/controller/status_client.go`:

```go
func (c *HTTPControlClient) CreateUser(ctx context.Context, podName string, req webserver.CreateUserRequest) error
func (c *HTTPControlClient) AlterUser(ctx context.Context, podName string, req webserver.AlterUserRequest) error
func (c *HTTPControlClient) DropUser(ctx context.Context, podName string, req webserver.DropUserRequest) error
func (c *HTTPControlClient) ListUsers(ctx context.Context, podName string) (*webserver.ListUsersResponse, error)
func (c *HTTPControlClient) CreateDatabase(ctx context.Context, podName string, req webserver.CreateDatabaseRequest) error
func (c *HTTPControlClient) DropDatabase(ctx context.Context, podName string, req webserver.DropDatabaseRequest) error
func (c *HTTPControlClient) ListDatabases(ctx context.Context, podName string) (*webserver.ListDatabasesResponse, error)
```

These call `c.do(ctx, podName, path, body, &result)` using the existing mTLS
transport.

---

## M12.2 — Managed Roles in Cluster Spec

### API Changes

Add `Roles` to `ManagedConfiguration` and `ManagedRolesStatus` to
`ClusterStatus`:

```go
type ManagedConfiguration struct {
    Services *ManagedServices    `json:"services,omitempty"`
    Roles    []RoleConfiguration `json:"roles,omitempty"`       // NEW
}

type RoleConfiguration struct {
    // Name is the MySQL user name.
    // +kubebuilder:validation:Required
    // +kubebuilder:validation:MaxLength=32
    Name string `json:"name"`

    // Host is the MySQL host part. Defaults to "%".
    // +kubebuilder:default:="%"
    // +optional
    Host string `json:"host,omitempty"`

    // Ensure controls whether the user should exist or be absent.
    // +kubebuilder:validation:Enum=present;absent
    // +kubebuilder:default:=present
    // +optional
    Ensure EnsureOption `json:"ensure,omitempty"`

    // PasswordSecret references a Secret containing the user's password.
    // +optional
    PasswordSecret *LocalObjectReference `json:"passwordSecret,omitempty"`

    // Superuser grants ALL PRIVILEGES on *.* WITH GRANT OPTION.
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

    // Privileges are grants (global or per-database).
    // +optional
    Privileges []RolePrivilege `json:"privileges,omitempty"`
}

type RolePrivilege struct {
    // Privileges is the grant list (SELECT, INSERT, ALL, etc.).
    // +kubebuilder:validation:MinItems=1
    Privileges []string `json:"privileges"`

    // On is the target (e.g. "*.*", "mydb.*"). Defaults to "*.*".
    // +optional
    On string `json:"on,omitempty"`
}
```

Status:

```go
// In ClusterStatus:
ManagedRolesStatus *ManagedRolesStatus `json:"managedRolesStatus,omitempty"`

type ManagedRolesStatus struct {
    ByStatus        map[ManagedRoleStatus][]string `json:"byStatus,omitempty"`
    CannotReconcile map[string][]string             `json:"cannotReconcile,omitempty"`
    PasswordStatus  map[string]RolePasswordState    `json:"passwordStatus,omitempty"`
}

type ManagedRoleStatus string

const (
    ManagedRoleReconciled            ManagedRoleStatus = "reconciled"
    ManagedRoleNotManaged            ManagedRoleStatus = "not-managed"
    ManagedRolePendingReconciliation ManagedRoleStatus = "pending-reconciliation"
    ManagedRoleReserved              ManagedRoleStatus = "reserved"
)

type RolePasswordState struct {
    SecretResourceVersion string      `json:"secretResourceVersion,omitempty"`
    LastApplied           metav1.Time `json:"lastApplied,omitempty"`
}
```

### Validation

In `cluster_funcs.go`, add `validateManagedRoles()`:

- Reject duplicate `(name, host)` tuples.
- Reject names matching reserved users (`root`, `mysql.*`, `cnmsql_*`).
- Reject `host=''`.
- Reject `Superuser=true` + non-empty `Privileges`.
- Reject invalid `RequireTLS` values.

### Cluster Controller Reconciliation

New method `reconcileManagedRoles()` called from `Reconcile()` in the steady
state (after the Cluster is Ready, before returning). Steps:

1. If `spec.managed.roles` is empty, skip.
2. Resolve the primary Pod name from `status.currentPrimary`.
3. Call `controlClient.ListUsers(ctx, primaryPod)` to get current MySQL state.
4. Diff `ListUsersResponse` against `spec.managed.roles`:
   - **User in spec, not in MySQL** → `controlClient.CreateUser()`.
   - **User in both, Ensure=absent** → `controlClient.DropUser()`.
   - **User in both, Ensure=present**:
     - Compare attributes: if changed → `controlClient.AlterUser()`.
     - Compare password Secret `resourceVersion` against
       `status.managedRolesStatus.passwordStatus[name]`.
       If changed → `controlClient.AlterUser()` with the new password.
     - Compare privileges: if changed → `controlClient.AlterUser()` with new
       privileges.
   - **User in MySQL, not in spec + no Ensure=absent entry** → IGNORE.
5. Update `status.managedRolesStatus`.
6. Emit Events for role changes.

Password Secrets are read from Kubernetes by the operator (already has Secret
read RBAC), not forwarded by the instance manager.

---

## M12.3 — Database Controller

### Controller Scaffold

```bash
kubebuilder create api --group mysql.cnmsql.co --version v1alpha1 \
  --kind Database --controller --resource=false
```

(The CRD types already exist; `--resource=false` avoids overwriting them. The
CLI adds a `DatabaseReconciler` skeleton to `internal/controller/` and registers
it in `cmd/main.go`.)

### Reconciliation Logic

`DatabaseReconciler.Reconcile()`:

1. Fetch the `Database` CR.
2. Resolve the referenced `Cluster` and its `status.currentPrimary`.
3. If deletion timestamp is set:
   - If `ReclaimPolicy=delete`: call
     `controlClient.DropUser()` for each `DatabaseUser` with `Ensure=present`,
     then `controlClient.DropDatabase()`. Remove finalizer.
   - If `ReclaimPolicy=retain`: remove finalizer (noop).
   - Return.
4. If `Ensure=absent`:
   - Call `controlClient.DropUser()` for each `DatabaseUser`.
   - Call `controlClient.DropDatabase()`.
   - Set `status.applied = true`, return.
5. If `Ensure=present`:
   - Call `controlClient.CreateDatabase()` with charset/collation.
   - For each `DatabaseUser`:
     - If `Ensure=absent`: `controlClient.DropUser()`.
     - If `Ensure=present`: read password from referenced Secret
       (`PasswordSecret`), call `controlClient.CreateUser()`, then for each
       `DatabaseGrant`: call `controlClient.AlterUser()` with privileges.
   - If `ReclaimPolicy=delete`, ensure finalizer is present.
   - Set `status.applied = true` + `Ready` condition.
6. Return.

### RBAC

The `DatabaseReconciler` needs:

```go
// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=databases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=databases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=databases/finalizers,verbs=update
// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=clusters,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
```

### Password Change Detection

The operator tracks the applied Secret `resourceVersion` in
`DatabaseStatus.Conditions` or a dedicated `AppliedSecretVersions` field.
When the Secret's `resourceVersion` changes, the operator re-issues `ALTER USER
... IDENTIFIED BY`.

---

## M12.4 — Config Hardening

### Dangerous-Key Denylist

In `cluster_funcs.go`, validate `spec.mysql.parameters` rejects:

```go
var deniedMySQLConfigKeys = map[string]bool{
    "basedir": true, "datadir": true, "socket": true, "pid_file": true,
    "port": true, "admin_address": true, "admin_port": true,
    "log_error": true, "tmpdir": true, "plugin_dir": true,
    "secure_file_priv": true, "log_bin_basename": true,
    "relay_log_basename": true, "general_log_file": true,
    "slow_query_log_file": true, "server_id": true, "server_uuid": true,
    "gtid_mode": true, "enforce_gtid_consistency": true,
    "read_only": true, "super_read_only": true,
    "skip_slave_start": true, "skip_replica_start": true,
    "report_host": true, "report_port": true,
    "auto_generate_certs": true,
    "ssl_ca": true, "ssl_cert": true, "ssl_key": true,
    "admin_ssl_ca": true, "admin_ssl_cert": true, "admin_ssl_key": true,
    "admin_tls_ciphersuites": true, "tls_ciphersuites": true,
    "require_secure_transport": true,
}
```

A denied key produces a `ClusterBlocked` reason with a descriptive message.

### Deprecated-Key Warnings

Optionally populate `status.managedRolesStatus` or a new condition with warnings
for deprecated keys (e.g., `expire_logs_days` on 8.0+).

---

## Implementation Order

1. **Instance manager API + SQL builders** (foundation for both M12.2 and
   M12.3):
   - `pkg/management/mysql/user/` SQL builders + Manager.
   - `webserver/` handlers + types.
   - Extend `InstanceController` + `Handler()`.
   - Extend `HTTPControlClient`.
   - Unit tests (SQL builders, handler marshalling, HTTP client).

2. **Managed roles**:
   - API types + validation + deepcopy.
   - `reconcileManagedRoles()` in Cluster controller.
   - `make generate manifests`.
   - Unit tests (validation, reconciler logic, password diff).
   - Integration tests (real Percona: create/alter/drop user, privilege change).
   - e2e (Kind: managed roles create/rotate/drop users).

3. **Database controller**:
   - `DatabaseReconciler` + RBAC.
   - `make generate manifests`.
   - Unit tests (reconciler, finalizer, reclaim policy).
   - Integration tests (real Percona: create/drop DB, DB-scoped users).
   - e2e (Kind: Database CR lifecycle).

4. **Config hardening**:
   - Denied-key validation + unit tests.
   - Deprecation warnings (optional).

## Verification

- `go test ./...` all suites.
- `make lint-fix` (0 issues).
- `make generate manifests` (no drift).
- Integration tests against real Percona 8.0/8.4/9.x.
- e2e on Kind: managed roles + Database CR lifecycle.
- `npm run build` from `docs/` (no regressions).

## Documentation

- New page: `docs/src/managed-roles-and-databases.md`.
- Update `docs/src/api-reference.md`.
- Update `docs/src/cluster-lifecycle.md`.
- Update `docs/sidebars.js`.
