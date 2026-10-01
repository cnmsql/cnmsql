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
    table_size:
      query: |
        SELECT table_schema, table_name, table_rows, data_length
        FROM information_schema.tables
        WHERE table_schema NOT IN ('mysql', 'sys', 'performance_schema', 'information_schema')
      metrics:
        - table_schema:
            usage: LABEL
        - table_name:
            usage: LABEL
        - table_rows:
            usage: GAUGE
            description: Estimated number of rows in the table
        - data_length:
            usage: GAUGE
            description: Size of the table data in bytes
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
example above gives `mysql_table_size_table_rows` and
`mysql_table_size_data_length`, both labelled with `table_schema` and
`table_name`. Query and column names may only use letters, digits and
underscores. Rows with a `NULL` value skip that metric, and a row that repeats
the labels of an earlier row is dropped.

Each instance runs the queries on its own server, in a read-only transaction,
as the instance manager's control account. Anyone who can edit a referenced
ConfigMap can therefore run SQL as that account; keep queries that should stay
private in a Secret with `customQueriesSecret`, which takes the same
`name`/`key` pairs.

ConfigMaps are read first, then Secrets, each in list order. When two documents
define the same query name, the later one wins. A query whose metric names
clash with another query's is skipped and logged.

The operator grants the instance Pods `get` on the referenced ConfigMaps and
Secrets only, and the instance manager reads them through the Kubernetes API.
Edits are picked up within a minute and never restart a Pod. If a document
cannot be read or parsed, the instance keeps the queries it last loaded from it
and logs the error. A query that fails at scrape time sets
`mysql_exporter_last_scrape_error` to 1.

Two more fields shape what runs on a scrape:

- `disableDefaultQueries: true` turns off the built-in metrics (global status,
  variables, replication and the other mysqld_exporter families), leaving only
  the custom queries.
- `metricsQueriesTTL` sets the minimum interval between two runs of the
  queries. A scrape that arrives sooner gets the previous results. Use it to
  keep expensive queries from running on every scrape:

```yaml
spec:
  monitoring:
    disableDefaultQueries: false
    metricsQueriesTTL: 1m
```
