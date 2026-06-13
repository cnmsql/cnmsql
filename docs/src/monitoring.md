---
title: "Monitoring"
description: "Prometheus metrics and PodMonitor integration."
sidebar_position: 12
---

# Monitoring

CNMySQL instances expose Prometheus metrics on port `9187` at `/metrics`.
The metrics server is separate from the mTLS control API and the health probe
server.

The current exporter publishes built-in Go runtime metrics plus MySQL global
status metrics from `SHOW GLOBAL STATUS`. More MySQL scraper families and custom
query loading are planned as M13.1 continues.

## PodMonitor

When the Prometheus Operator CRDs are installed, CNMySQL can create an owned
`PodMonitor` for a cluster:

```yaml
apiVersion: mysql.cloudnative-mysql.io/v1alpha1
kind: Cluster
metadata:
  name: cluster-sample
spec:
  monitoring:
    enablePodMonitor: true
```

The generated `PodMonitor` selects pods with:

```yaml
cnmysql.io/cluster: <cluster-name>
```

and scrapes the named container port `metrics`.

## Custom Queries

`customQueriesConfigMap`, `customQueriesSecret`, `disableDefaultQueries`, and
`metricsQueriesTTL` are API fields for the custom-query collector. The endpoint
is available now; query loading and default query injection are the next M13.1
slice.
