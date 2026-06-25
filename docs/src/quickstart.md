---
title: "Quickstart"
description: "Deploy CNMSQL - CloudNative for MySQL, create a three-instance MySQL cluster, connect, scale, and take a backup."
sidebar_position: 2
---

# Quickstart

This guide walks through deploying CNMSQL - CloudNative for MySQL and a three-instance Percona Server for MySQL cluster in a local [Kind](https://kind.sigs.k8s.io/) environment.

## Prerequisites

| Tool | Purpose |
|------|---------|
| `helm` | Install the operator |
| `kubectl` | Interact with the Kubernetes cluster |
| `kind` | Local Kubernetes cluster |
| `cert-manager` | Issue TLS certificates for mTLS and MySQL TLS |

[Install cert-manager](https://cert-manager.io/docs/installation/) in your cluster if it isn't already present:

```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/latest/download/cert-manager.yaml
kubectl wait --for=condition=Available deployment/cert-manager-webhook -n cert-manager --timeout=5m
```

## 1. Install the Operator

Helm is the recommended way to install. The chart is published from the [charts](https://github.com/cnmsql/charts) repository:

```bash
helm repo add cnmsql https://cnmsql.github.io/charts
helm repo update
helm install cnmsql cnmsql/cnmsql \
  --namespace cnmsql-system \
  --create-namespace
```

The operator image defaults to `ghcr.io/cnmsql/cnmsql` pinned to the chart's `appVersion`, so no `--set` overrides are needed for a standard install.

Verify the controller is running:

```bash
kubectl get pods -n cnmsql-system
```

You should see a single `cnmsql-controller-manager` Pod in `Running` state.

## 2. Pull the Instance Image

Instance images are published from the [containers](https://github.com/cnmsql/containers) repository. Pull the pre-built image and load it into Kind:

```bash
docker pull ghcr.io/cnmsql/cnmsql-instance:8.4
kind load docker-image ghcr.io/cnmsql/cnmsql-instance:8.4 --name cnmsql-test-e2e
```

:::note Kind users
The operator image must also be loaded into Kind if your Kind cluster can't reach the registry. The image is `ghcr.io/cnmsql/cnmsql` tagged with the chart version:

```bash
docker pull ghcr.io/cnmsql/cnmsql:0.3.2
kind load docker-image ghcr.io/cnmsql/cnmsql:0.3.2 --name cnmsql-test-e2e
```
:::

## 3. Install the CLI Plugin

```bash
curl -sSfL https://github.com/cnmsql/cnmsql/raw/main/hack/install-cnmsql-plugin.sh | sh -s -- -b ~/.local/bin
```

The script downloads the latest release binary for your platform, verifies its checksum, and installs the plugin plus a tab-completion shim. Replace `~/.local/bin` with any directory on your `PATH` (e.g. `/usr/local/bin`).

Verify the plugin is registered:

```bash
kubectl cnmsql version
```

The plugin is available as `kubectl cnmsql`. It auto-detects the cluster in the current namespace, so you can omit the cluster name in most commands.

## 4. Create a Cluster

Apply a minimal three-instance cluster. An initial database `app` is bootstrapped with an `app` owner role:

```yaml
apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: cluster-sample
spec:
  instances: 3
  imageName: ghcr.io/cnmsql/cnmsql-instance:8.4
  storage:
    size: 10Gi
  mysql:
    binlogFormat: ROW
  bootstrap:
    initdb:
      database: app
      owner: app
```

Wait for the cluster to become ready:

```bash
kubectl wait --for=condition=Ready cluster/cluster-sample --timeout=15m
```

Inspect the topology with the CLI plugin:

```bash
kubectl cnmsql status cluster-sample
```

Expected result:
- Three Pods: `cluster-sample-1`, `cluster-sample-2`, `cluster-sample-3`
- One Pod labeled `mysql.cnmsql.co/role=primary`
- Two Pods labeled `mysql.cnmsql.co/role=replica`
- `status.readyInstances` is `3`

## 5. Connect to the Database

CNMSQL - CloudNative for MySQL creates three role-routed Services automatically:

| Service | Endpoint | Routes to |
|---------|----------|-----------|
| `cluster-sample-rw` | Read-write | Current primary |
| `cluster-sample-ro` | Read-only | Ready replicas |
| `cluster-sample-r`  | Read      | Any ready instance |

Service routing follows the `mysql.cnmsql.co/role` label and updates automatically after failover with no manual reconfiguration needed.

Application credentials are generated and stored in a Secret. List all Secrets for the cluster:

```bash
kubectl get secrets -l mysql.cnmsql.co/cluster=cluster-sample
```

To test connectivity, launch a temporary MySQL client Pod and connect:

```bash
kubectl run mysql-client --rm -it --image=mysql:8.4 --restart=Never -- \
  mysql -h cluster-sample-rw -u app -p$(kubectl get secret cluster-sample-app-app -o jsonpath='{.data.password}' | base64 -d) app
```

## 6. Scale the Cluster

Scale up to four instances:

```bash
kubectl patch cluster cluster-sample --type merge -p '{"spec":{"instances":4}}'
kubectl wait --for=condition=Ready cluster/cluster-sample --timeout=15m
```

Scale down to one instance:

```bash
kubectl patch cluster cluster-sample --type merge -p '{"spec":{"instances":1}}'
```

Scale-down removes replica Pods (highest ordinal first) but retains their PVCs. Delete retained PVCs only after confirming the data is no longer needed.

## 7. Take a Backup

Create an ad-hoc backup via the CLI:

```bash
kubectl cnmsql backup cluster-sample
```

This creates a `Backup` resource with defaults: XtraBackup, online, backed by the cluster's configured object store. Monitor progress:

```bash
kubectl get backup backup-sample -w
kubectl describe backup backup-sample
```

Once `status.phase` reaches `completed`, the backup is ready for restoration.

## 8. Clean Up

```bash
kubectl delete cluster cluster-sample
```

Deleting a `Backup` resource does **not** remove data from the object store. Use your object store's lifecycle policies or delete the objects manually.

## Next Steps

- [Configure object storage](./object-store.md) for production backups
- [Enable continuous archiving](./pitr.md#continuous-binlog-archiver) to unlock point-in-time recovery
- Read the [operations runbook](./operations.md) for day-two commands like switchover and failover
- Explore the [API reference](./api-reference.md) for every field and option
