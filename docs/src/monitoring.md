---
title: "Monitoring"
description: "Prometheus metrics and PodMonitor integration."
sidebar_position: 14
---

# Monitoring

cnmsql instances expose Prometheus metrics on port `9187` at `/metrics`.
The metrics server is separate from the mTLS control API and the health probe
server.

The current exporter publishes built-in Go runtime metrics plus MySQL global
status metrics from `SHOW GLOBAL STATUS`. For Group Replication clusters, the
operator also publishes cluster-level GR metrics (see below). You can add your
own metrics with [custom queries](#custom-queries).

## Group Replication metrics

The operator exposes Group Replication metrics on its `/metrics` endpoint under
the `cnmsql` namespace. These reflect the operator's own cross-validated view of
each GR cluster and are read from the manager's cached client at scrape time:

| Metric | Description |
|---|---|
| `cnmsql_cluster_gr_has_quorum` | 1 if the group has quorum, 0 otherwise. |
| `cnmsql_cluster_gr_bootstrapped` | 1 if the group has been bootstrapped. |
| `cnmsql_cluster_gr_view_size` | The sticky maximum group size used as the quorum denominator. |
| `cnmsql_cluster_gr_members` | Members per state (`ONLINE`, `RECOVERING`, `OFFLINE`, `ERROR`, `UNREACHABLE`). |

Labels are `namespace` and `cluster`. Async clusters emit nothing. Alert on
`cnmsql_cluster_gr_has_quorum == 0` for any GR cluster to catch quorum loss.

## Ad-hoc metrics inspection

Scrape an instance's current metrics directly from your terminal:

```bash
kubectl cnmsql metrics <cluster>                # primary
kubectl cnmsql metrics <cluster> <instance>     # specific instance
kubectl cnmsql metrics <cluster> -w             # refresh every 2s
kubectl cnmsql metrics <cluster> --filter=mysql_global_status_threads
```

The plugin opens an mTLS port-forward to the instance manager and scrapes
`/metrics`. This is useful for debugging and quick checks, not for production
monitoring. Use the `PodMonitor` for Prometheus integration.

## PodMonitor

When the Prometheus Operator CRDs are installed, cnmsql can create an owned
`PodMonitor` for a cluster:

```yaml
apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: cluster-sample
spec:
  monitoring:
    enablePodMonitor: true
```

The generated `PodMonitor` selects pods with:

```yaml
cnmsql.co/cluster: <cluster-name>
```

and scrapes the named container port `metrics`.

## Authenticated metrics over TLS

By default the metrics endpoint is served over plain HTTP. Setting
`spec.monitoring.tls.enabled` switches it to mutual TLS, reusing the same
PKI as the control API: the instance presents its server certificate and
requires the scraper to present a client certificate signed by the cluster CA.

```yaml
apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: cluster-sample
spec:
  monitoring:
    enablePodMonitor: true
    tls:
      enabled: true
```

No extra certificates are needed. The instance Pods already mount the
`server-tls` certificate and the `client-ca` bundle. When a `PodMonitor` is
generated, cnmsql wires the scrape-side TLS configuration automatically:

- the endpoint scheme becomes `https`;
- the cluster CA secret (`<cluster>-ca`, key `ca.crt`) verifies the server cert;
- the operator client certificate (`<cluster>-client-tls`) authenticates the
  scrape;
- the read Service hostname (`<cluster>-r.<namespace>.svc`), a SAN present on
  every instance certificate, is used as the verified server name.

Prometheus must be able to read those secrets in the cluster's namespace to
mount the client certificate and CA.

## Custom queries

Custom queries turn the result of a SQL query into Prometheus metrics. Put the
queries in a ConfigMap or a Secret, then reference the key from the Cluster:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-sample-monitoring
data:
  queries.yaml: |
    table_io:
      query: |
        SELECT object_schema AS table_schema, object_name AS table_name,
               count_read AS rows_read, count_write AS rows_written
        FROM performance_schema.table_io_waits_summary_by_table
        WHERE object_schema NOT IN ('mysql', 'sys', 'performance_schema')
      metrics:
        - table_schema:
            usage: LABEL
        - table_name:
            usage: LABEL
        - rows_read:
            usage: COUNTER
            description: Rows read from the table
        - rows_written:
            usage: COUNTER
            description: Rows written to the table
---
apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: cluster-sample
spec:
  monitoring:
    customQueriesConfigMap:
      - name: cluster-sample-monitoring
        key: queries.yaml
```

Each top-level key names a query. Its `metrics` list maps result columns, in
order, to one of these usages:

| Usage | Effect |
|---|---|
| `LABEL` | The column value becomes a label on every metric of the row. |
| `GAUGE` | The column is published as a gauge. |
| `COUNTER` | The column is published as a counter. |
| `DISCARD` | The column is ignored. Columns left out of the list are ignored too. |

A `GAUGE` or `COUNTER` column is published as `mysql_<query>_<column>`, so the
example above gives `mysql_table_io_rows_read` and `mysql_table_io_rows_written`, both
labelled with `table_schema` and `table_name`. Column names must match the
result exactly, so alias them as in the example. Query and column names may
only use letters, digits and underscores, and label columns may not start with
`__`. Names that would fall under a built-in family, such as
`mysql_global_status_` or `mysql_exporter_`, are rejected.

Rows with a `NULL` value skip that metric. A row that repeats the labels of an
earlier row, or whose label value is not valid UTF-8, is dropped and sets
`mysql_exporter_last_scrape_error` to 1.

### Account and privileges

Each instance runs the queries on its own server as `cnmsql_metrics`, a
passwordless account that only accepts connections from inside the Pod over
the local socket. It always has `PROCESS`, `REPLICATION CLIENT` and
`REPLICATION SLAVE` on all databases and `SELECT` on `performance_schema`.

To query your own tables, list the grants it needs under
`spec.monitoring.privileges`:

```yaml
spec:
  monitoring:
    customQueriesConfigMap:
      - name: cluster-sample-monitoring
        key: queries.yaml
    privileges:
      - privileges: [SELECT]
        on: app.*
      - privileges: [SELECT, SHOW VIEW]
        on: reports.daily
```

The operator applies them on the primary, and replication carries them to
every instance. It checks the account on every resync: a grant revoked by
hand comes back, and the account is recreated if it was dropped, including
on clusters created before it existed. The `MetricsAccountReady` condition
on the Cluster reports the outcome, and an event lists each change.

The list is authoritative. Any other grant on `cnmsql_metrics`, whether it
came from a manual `GRANT` or from `postInitSQL`, is revoked. The built-in
grants above are never revoked, even if you list one and remove it later.
`PROXY` grants are the exception: the operator's control account cannot
revoke them, so the condition reads `ApplyFailed` until you revoke one by
hand.

Only `SELECT` and `SHOW VIEW` are accepted, on a database (`db.*`) or a
table (`db.table`). The webhook refuses `*.*` and the `mysql` schema, since
`mysql.user` holds password hashes. MySQL reads `_` in a database name as a
one-character wildcard, so `my_app.*` also covers `myXapp`, and a name that
`_` would let match `mysql` is refused as well. `information_schema` is
readable without a grant and cannot be listed. A table-level grant needs the
table to exist: until it does, the condition reads `ApplyFailed`, and the
other grants and revokes are still applied. Managed roles and `DatabaseUser`
resources still cannot change `cnmsql_metrics`, because it is a reserved
account.

The queries use a separate connection from the instance manager's control
account, so a slow query cannot hold up health checks or failover. Each query
is cancelled after 10 seconds.

Queries run in a read-only transaction, but that does not stop a statement
that commits implicitly, such as `CREATE USER`. The privileges of
`cnmsql_metrics` are what limit the damage a query can do, which is why
`spec.monitoring.privileges` only accepts read access. Anyone who can edit a
referenced ConfigMap can run SQL as that account; use `customQueriesSecret`, which takes the same `name`/`key` pairs,
for queries that should stay private.

### Loading and updates

ConfigMaps are read first, then Secrets, each in list order. When two documents
define the same query name, the later one wins. A query whose metric names
clash with another query's is skipped and logged.

The operator grants the instance Pods `get` on the referenced ConfigMaps and
Secrets only, and the instance manager reads them through the Kubernetes API.
Edits are picked up within a minute and never restart a Pod. If a document
cannot be parsed or the API is unreachable, the instance keeps the queries it
last loaded from it and logs the error. If the ConfigMap, the Secret or the key
no longer exists, its queries stop. A query that fails at scrape time sets
`mysql_exporter_last_scrape_error` to 1.

Two more fields shape what runs on a scrape:

- `disableDefaultQueries: true` turns off the built-in MySQL queries (global
  status, variables, replication and the other mysqld_exporter families). The
  data volume, heartbeat and `mysql_exporter_last_scrape_error` metrics are
  still published, as are the custom queries.
- `metricsQueriesTTL` sets the minimum interval between two runs of the
  queries. A scrape that arrives sooner gets the previous results. Use it to
  keep expensive queries from running on every scrape:

```yaml
spec:
  monitoring:
    disableDefaultQueries: false
    metricsQueriesTTL: 1m
```
