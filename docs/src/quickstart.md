---
title: "Quickstart"
description: "Deploy CloudNative MySQL on a local Kind cluster, create a three-instance MySQL cluster, connect, scale, and take a backup."
sidebar_position: 2
---

# Quickstart

This guide walks through deploying CloudNative MySQL and a three-instance Percona Server for MySQL cluster in a local [Kind](https://kind.sigs.k8s.io/) environment.

## Prerequisites

| Tool | Purpose |
|------|---------|
| `go` | Build the operator binary |
| `docker` | Build and load container images |
| `kubectl` | Interact with the Kubernetes cluster |
| `kind` | Local Kubernetes cluster |
| `make` | Run build targets |
| `cert-manager` | Issue TLS certificates for mTLS and MySQL TLS |

[Install cert-manager](https://cert-manager.io/docs/installation/) in your cluster if it isn't already present:

```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/latest/download/cert-manager.yaml
kubectl wait --for=condition=Available deployment/cert-manager-webhook -n cert-manager --timeout=5m
```

## 1. Build and Load Images

Build the operator image from source:

```bash
make docker-build IMG=cloudnative-mysql-controller:dev
```

Pull the pre-built instance image. Instance images are published from the
[containers](https://github.com/CloudNative-MySQL/containers) repository:

```bash
docker pull ghcr.io/cloudnative-mysql/cloudnative-mysql-instance:8.4
```

Load both images into your Kind cluster:

```bash
kind load docker-image cloudnative-mysql-controller:dev --name cloudnative-mysql-test-e2e
kind load docker-image ghcr.io/cloudnative-mysql/cloudnative-mysql-instance:8.4 --name cloudnative-mysql-test-e2e
```

## 2. Deploy the Operator

Install the CRDs and deploy the controller manager:

```bash
make install
make deploy IMG=cloudnative-mysql-controller:dev
```

Verify the controller is running:

```bash
kubectl get pods -n cloudnative-mysql-system
```

You should see a single `cnmysql-controller-manager` Pod in `Running` state.

## 3. Install the CLI Plugin

**From a release (no source checkout needed):**

```bash
curl -sSfL https://github.com/CloudNative-MySQL/cloudnative-mysql/raw/main/hack/install-cnmysql-plugin.sh | sh -s -- -b ~/.local/bin
```

The script downloads the latest release binary for your platform, verifies its checksum,
and installs the plugin plus a tab-completion shim. Replace `~/.local/bin` with any
directory on your `PATH` (e.g. `/usr/local/bin`).

**From the repo (development):**

```bash
make install-plugin
```

Verify the plugin is registered:

```bash
kubectl cnmysql version
```

The plugin is available as `kubectl cnmysql`. It auto-detects the cluster in the current namespace, so you can omit the cluster name in most commands.

## 4. Create a Cluster

Apply a minimal three-instance cluster. An initial database `app` is bootstrapped with an `app` owner role:

```yaml
apiVersion: mysql.cloudnative-mysql.io/v1alpha1
kind: Cluster
metadata:
  name: cluster-sample
spec:
  instances: 3
  imageName: ghcr.io/cloudnative-mysql/cloudnative-mysql-instance:8.4
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
kubectl cnmysql status cluster-sample
```

Expected result:
- Three Pods: `cluster-sample-1`, `cluster-sample-2`, `cluster-sample-3`
- One Pod labeled `mysql.cloudnative-mysql.io/role=primary`
- Two Pods labeled `mysql.cloudnative-mysql.io/role=replica`
- `status.readyInstances` is `3`

## 5. Connect to the Database

CloudNative MySQL creates three role-routed Services automatically:

| Service | Endpoint | Routes to |
|---------|----------|-----------|
| `cluster-sample-rw` | Read-write | Current primary |
| `cluster-sample-ro` | Read-only | Ready replicas |
| `cluster-sample-r`  | Read      | Any ready instance |

Service routing follows the `mysql.cloudnative-mysql.io/role` label and updates automatically after failover — no manual reconfiguration needed.

Application credentials are generated and stored in a Secret. List all Secrets for the cluster:

```bash
kubectl get secrets -l mysql.cloudnative-mysql.io/cluster=cluster-sample
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

Scale-down removes replica Pods (highest ordinal first) but retains their PVCs. Delete retained PVCs only after you're certain the data is no longer needed.

## 7. Take a Backup

Create an ad-hoc backup via the CLI:

```bash
kubectl cnmysql backup cluster-sample
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
