# 035 — Declarative grants for the metrics account

Status: accepted (2026-10-02)

Issue #174. Follows #173 (custom monitoring queries).

## Problem

Custom monitoring queries run as `cnmsql_metrics@localhost`, a passwordless
account that initdb creates with:

- `PROCESS, REPLICATION CLIENT, REPLICATION SLAVE ON *.*`
- `SELECT ON performance_schema.*`

To query their own tables, users have to run a `GRANT` on the primary by hand.
None of the declarative paths can do it: `postInitSQL` only runs at creation,
and `spec.managed.roles` and `DatabaseUser` refuse `cnmsql_metrics` because it
is reserved. So the grants can't live in Git next to the queries that need
them, and nothing restores them after a revoke.

Clusters bootstrapped before the metrics account existed don't have it at all.

## Goal

- A `spec.monitoring.privileges` list of extra grants for the metrics account.
- The operator keeps the account's grants equal to *base ∪ declared* on the
  primary: it grants what is missing and revokes everything else, except the
  base grants, which it never revokes.
- The account is created when it is missing, on every cluster.
- Only read access can be declared. Anyone who can edit a query ConfigMap runs
  SQL as this account.

## Non-goals

- Grants on replicas made out of band. The operator applies grants on the
  primary and they reach replicas through replication, like every other
  account change.
- Partial revokes (`REVOKE ... ON db.*` lines that restrict a global grant).
  The declared grants are never global, so such lines are left alone.
- A password, TLS requirement or resource limits for the account.

## Decisions

| # | Decision | Why |
|---|----------|-----|
| 1 | Read-only allowlist: `SELECT` and `SHOW VIEW` only | A denylist lets every privilege a future server adds through by default. `EXECUTE` is refused because a `SQL SECURITY DEFINER` routine can write. |
| 2 | Targets must be `db.*` or `db.table`; `*.*` and the `mysql` schema are refused | `SELECT` on `mysql.user` exposes password hashes, and a query could publish them as a metric label. |
| 3 | Authoritative: every grant that is neither base nor declared is revoked | The account's grants are fully described by the Cluster. A manual `GRANT` or one from `postInitSQL` does not survive. Custom queries are unreleased (after v0.8.0), so no released documentation tells users to grant by hand. |
| 4 | The operator calls a new instance-manager endpoint on the primary | Same pattern as the dump account (design 028): status and events stay operator-side, and the instance manager already holds a connection with the privileges to grant. |
| 5 | The endpoint diffs `SHOW GRANTS` and only writes on a difference | Each `GRANT`/`REVOKE` is a binlog event. Applying blindly every 30 s would churn GTIDs on every replica. |
| 6 | The account is created when missing, whether or not the field is set | Fixes old clusters and accounts dropped by hand. Costs one `SHOW GRANTS` per resync. |

## API

```go
type MonitoringConfiguration struct {
	// ...existing fields...

	// Privileges are extra grants for the cnmsql_metrics account that custom
	// queries run as. Only SELECT and SHOW VIEW on a database (db.*) or a
	// table (db.table) are allowed. The operator applies them on the primary
	// and revokes any other grant the account holds, except its built-in ones.
	// +kubebuilder:validation:MaxItems=32
	// +optional
	Privileges []RolePrivilege `json:"privileges,omitempty"`
}
```

```yaml
spec:
  monitoring:
    customQueriesConfigMap:
      - name: app-queries
        key: queries.yaml
    privileges:
      - privileges: [SELECT]
        on: app.*
```

### Validation

`ClusterSpec.validateMonitoringPrivileges` runs in the Cluster webhook. Each
entry must satisfy `engine.ValidateMetricsPrivilege(privileges, on)`, which
lives in `pkg/engine` so the instance manager can apply the same check:

- At least one privilege. Each one, trimmed and case-folded, is `SELECT` or
  `SHOW VIEW`.
- `on` matches `<db>.*` or `<db>.<table>`. Each part is a bare identifier
  (`[A-Za-z0-9_$]+`) or a backticked one with no backtick inside.
- The database part is not `*`, contains no `%`, and does not match `mysql`
  (case-insensitive) when `_` is read as a one-character wildcard. GRANT
  treats `_` and `%` as wildcards in database names, so `mysq_.*` would
  cover `mysql`.

Errors are reported per entry as `spec.monitoring.privileges[i].privileges`
or `.on`.

## Base grants

`pkg/engine` gains:

```go
const (
	MetricsAccountName = "cnmsql_metrics"
	MetricsAccountHost = "localhost"
)

// MetricsAccountBaseGrants are the grants the account holds on every
// cluster; the reconciler never revokes them.
func MetricsAccountBaseGrants() []AccountGrant
```

Bootstrap (`instance/bootstrap.go`) and the controller constant
`metricsUser` use these, so the base set is defined once.

## Instance manager

### Endpoint

`POST /monitoring/account`, mTLS like every control route.

```json
// request
{"privileges": [{"privileges": ["SELECT"], "on": "app.*"}]}
// response
{"created": false, "granted": ["SELECT ON app.*"], "revoked": ["INSERT ON `app`.*"]}
```

The account name comes from the manager's `--metrics-user` flag, never from
the request. An entry that fails validation returns 400 and nothing runs.

### Steps

1. `SELECT COUNT(*) FROM mysql.user WHERE User = ? AND Host = 'localhost'`.
   When zero, `CREATE USER IF NOT EXISTS '<user>'@'localhost'` (passwordless,
   as bootstrap does) and set `created`.
2. `SHOW GRANTS FOR '<user>'@'localhost'`.
3. Build the plan with the pure `user.PlanMetricsGrants(observed, base,
   declared)`.
4. Run the `GRANT`s, then the `REVOKE`s, through the existing
   `Manager.execAll` path, so MariaDB's missing `REVOKE IF EXISTS` is handled
   as it is for other accounts.

### Parsing SHOW GRANTS

Each line becomes a set of (privilege, target) pairs:

| Line shape | Pairs | Revoked with |
|------------|-------|--------------|
| `GRANT A, B ON t TO acct` | `(a, t)`, `(b, t)` | `REVOKE A ON t FROM acct` |
| `GRANT SELECT (`c1`, `c2`) ON t TO acct` | `(select (c1, c2), t)` | the privilege text verbatim |
| `GRANT EXECUTE ON PROCEDURE db.p TO acct` | target `procedure db.p` | the target verbatim |
| `... WITH GRANT OPTION` | adds `(grant option, t)` | `REVOKE GRANT OPTION ON t FROM acct` |
| `GRANT USAGE ON *.* TO acct` | none | |
| `GRANT `role`@`%` TO acct` (no `ON`) | role `role@%` | `REVOKE `role`@`%` FROM acct` |
| `REVOKE ... FROM acct` (partial revoke) | none, ignored | |

Splitting on commas respects parentheses and backticks. Pairs are compared
after lowercasing, stripping backticks and mapping aliases. A revoke uses
the text as the server printed it, so quoting survives.

Aliases, applied to both sides before comparing:

| Printed | Compared as |
|---------|-------------|
| `binlog monitor` (MariaDB 10.5+) | `replication client` |
| `replication replica` | `replication slave` |

The plan is:

- **grant**: each declared or base pair not observed, grouped by target into
  one `GRANT` per target.
- **revoke**: each observed pair that is neither base nor declared, grouped by
  target. Base pairs are filtered out of the revoke set even when the
  declared list mentions them, so dropping `SELECT ON performance_schema.*`
  from the list never removes the base grant.

## Operator

`reconcileMetricsAccountBestEffort(ctx, cluster, observed)` runs right after
the dump account step in `ClusterReconciler.Reconcile`, so it runs on every
reconcile once the primary is up, including the 30 s ready resync. That
resync is what restores a revoked grant or removes an added one.

- No ready primary (`observed.PrimaryName` empty, not ready, or not reporting
  the primary role): `MetricsAccountReady=False`, reason `PrimaryNotReady`.
- Call failed (including 404 from an instance manager that predates the
  route): `MetricsAccountReady=False`, reason `ApplyFailed`, the error in the
  message, and a Warning event `MetricsAccountFailed`. It does not fail the
  Cluster reconcile.
- Success: `MetricsAccountReady=True`, reason `Applied`. When the response
  shows a create, grant or revoke, a Normal event `MetricsAccountUpdated`
  lists them, and a log line records them.

The condition is written only when its status, reason or message changes, so
the steady state writes nothing to the API server. No applied-grant list is
kept in status, since the authoritative diff doesn't need one.

`InstanceControlClient` gains `EnsureMetricsAccount(ctx, cluster, instance,
req) (*user.MetricsAccountResponse, error)`, and `HTTPControlClient`
implements it.

## Upgrade

- No Pod spec change and no restart. The route ships with the instance
  manager, which in-place or rolling operator upgrades replace.
- Until the primary's manager is upgraded the call 404s and the condition
  reads `ApplyFailed`. Nothing else is affected.
- On the first pass after the upgrade, a grant made by hand or through
  `postInitSQL` is revoked unless the Cluster declares it. The release notes
  call this out.

## Testing

**Unit**

- `pkg/engine`: `ValidateMetricsPrivilege` table: allowed privileges, case,
  every refused privilege class, target shapes, backticks, `*.*`, `mysql.*`,
  `` `MySQL`.user ``, `mysq_.*`, `%`.
- `api/v1alpha1`: the webhook reports the right field paths.
- `pkg/management/mysql/user`: `PlanMetricsGrants` on SHOW GRANTS fixtures
  from MySQL 8.0, 8.4, 9.7 and MariaDB. Cases: in sync (empty plan), missing
  declared, extra static and dynamic privileges, column-level, routine,
  `WITH GRANT OPTION`, role grants, partial-revoke lines, aliases, base
  listed in the declared set and then removed.
- Webserver handler: 400 on an invalid entry, response shape.
- Controller: condition transitions, events only on change, primary not
  ready, 404 surfaced as `ApplyFailed`.

**Integration** (`test/integration`, every flavor)

Against a real server: create the account when missing, grant the declared
set, revoke an out-of-band grant, keep base grants when the declared list
names and drops them, and a second pass with no change produces no new GTID.

**e2e** (`test/e2e/custom_queries_test.go`)

- Replace the manual `GRANT` with `spec.monitoring.privileges`; both instances
  publish `mysql_e2e_items_total`.
- A manual `REVOKE` on the primary is restored.
- A manual extra `GRANT` is revoked.
- Removing the entry revokes it, and the built-in metrics keep working.
- Dropping the account is repaired.
- The webhook rejects `INSERT`, `*.*` and `mysql.*`.
- MariaDB: the same declarative grant lets the query read `app.items`.

## Docs

`docs/src/monitoring.md` §Account and privileges is rewritten around the
field, including the authoritative behavior. A sample in `config/samples`
shows the field next to a custom query ConfigMap.
