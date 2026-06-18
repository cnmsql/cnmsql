---
title: "Monitoring"
description: "Prometheus metrics and PodMonitor integration."
sidebar_position: 14
---

# Monitoring

cloudnative-mysql instances expose Prometheus metrics on port `9187` at `/metrics`.
The metrics server is separate from the mTLS control API and the health probe
server.

The current exporter publishes built-in Go runtime metrics plus MySQL global
status metrics from `SHOW GLOBAL STATUS`. More MySQL scraper families and custom
query loading are planned as M13.1 continues.

## Ad-hoc metrics inspection

Scrape an instance's current metrics directly from your terminal:

```bash
kubectl cnmysql metrics <cluster>                # primary
kubectl cnmysql metrics <cluster> <instance>     # specific instance
kubectl cnmysql metrics <cluster> -w             # refresh every 2s
kubectl cnmysql metrics <cluster> --filter=mysql_global_status_threads
```

The plugin opens an mTLS port-forward to the instance manager and scrapes
`/metrics`. This is useful for debugging and quick checks, not for production
monitoring. Use the `PodMonitor` for Prometheus integration.

## PodMonitor

When the Prometheus Operator CRDs are installed, cloudnative-mysql can create an owned
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
cloudnative-mysql.io/cluster: <cluster-name>
```

and scrapes the named container port `metrics`.

## Authenticated metrics over TLS

By default the metrics endpoint is served over plain HTTP. Setting
`spec.monitoring.tls.enabled` switches it to mutual TLS, reusing the same
PKI as the control API: the instance presents its server certificate and
requires the scraper to present a client certificate signed by the cluster CA.

```yaml
apiVersion: mysql.cloudnative-mysql.io/v1alpha1
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
generated, cloudnative-mysql wires the scrape-side TLS configuration automatically:

- the endpoint scheme becomes `https`;
- the cluster CA secret (`<cluster>-ca`, key `ca.crt`) verifies the server cert;
- the operator client certificate (`<cluster>-client-tls`) authenticates the
  scrape;
- the read Service hostname (`<cluster>-r.<namespace>.svc`), a SAN present on
  every instance certificate, is used as the verified server name.

Prometheus must be able to read those secrets in the cluster's namespace to
mount the client certificate and CA.

## Custom queries

`customQueriesConfigMap`, `customQueriesSecret`, `disableDefaultQueries`, and
`metricsQueriesTTL` are API fields for the custom-query collector. The endpoint
is available now; query loading and default query injection are the next M13.1
slice.
