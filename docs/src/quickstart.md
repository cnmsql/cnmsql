---
title: "Quickstart"
description: "Build the cloudnative-mysql images, deploy the operator, create a cluster, and verify write and read endpoints."
sidebar_position: 2
---

# Quickstart

This guide brings up cloudnative-mysql in a development Kubernetes cluster and creates a
three-instance Percona Server for MySQL cluster.

The commands assume a Kind-style local environment and the default development
image names used by the repository.

## Prerequisites

- `go`
- `docker` or a compatible container tool
- `kubectl`
- `kind`
- `make`
- `cert-manager` in the target cluster, unless you install it as part of your
  local e2e setup

cloudnative-mysql uses cert-manager-issued certificates for instance-manager mTLS and
MySQL TLS.

## Build images

Build the operator image:

```bash
make docker-build IMG=cloudnative-mysql-controller:dev
```

Pull the published instance image. These are built and published from the
separate [`containers`](https://github.com/CloudNative-MySQL/containers) repo:

```bash
docker pull ghcr.io/cloudnative-mysql/cloudnative-mysql-instance:8.4
```

For a local Kind cluster, load both images:

```bash
kind load docker-image cloudnative-mysql-controller:dev --name cloudnative-mysql-test-e2e
kind load docker-image ghcr.io/cloudnative-mysql/cloudnative-mysql-instance:8.4 --name cloudnative-mysql-test-e2e
```

## Deploy the operator

Install CRDs and deploy the controller:

```bash
make install
make deploy IMG=cloudnative-mysql-controller:dev
```

Check the controller manager:

```bash
kubectl get pods -n cloudnative-mysql-system
```

## Install the CLI plugin

```bash
make install-plugin
```

Verify:

```bash
kubectl cnmysql version
```

The plugin is now available as `kubectl cnmysql`. Most commands default to the
only cluster in the current namespace, so you can often skip the cluster name.

## Create a cluster

Apply a minimal three-instance cluster:

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

Wait for readiness:

```bash
kubectl wait --for=condition=Ready cluster/cluster-sample --timeout=15m
kubectl cnmysql status cluster-sample
```

Expected topology:

- `cluster-sample-1`, `cluster-sample-2`, and `cluster-sample-3` Pods exist.
- One Pod has `mysql.cloudnative-mysql.io/role=primary`.
- The remaining ready Pods have `mysql.cloudnative-mysql.io/role=replica`.
- `status.readyInstances` is `3`.

## Connect through services

cloudnative-mysql creates role-routed Services:

- `cluster-sample-rw`: read-write endpoint for the current primary.
- `cluster-sample-ro`: read-only endpoint for replicas.
- `cluster-sample-r`: read endpoint for any ready instance.

Inspect them:

```bash
kubectl get svc cluster-sample-rw cluster-sample-ro cluster-sample-r
```

The generated application credentials are stored in the application Secret. The
exact Secret name depends on the cluster plan; inspect the generated Secrets:

```bash
kubectl get secrets -l mysql.cloudnative-mysql.io/cluster=cluster-sample
```

For quick smoke testing, exec from a MySQL client image or a temporary debug Pod
inside the same namespace and connect to `cluster-sample-rw`.

## Scale the cluster

Increase the instance count:

```bash
kubectl patch cluster cluster-sample --type merge -p '{"spec":{"instances":4}}'
kubectl wait --for=condition=Ready cluster/cluster-sample --timeout=15m
```

Scale down:

```bash
kubectl patch cluster cluster-sample --type merge -p '{"spec":{"instances":1}}'
```

Scale-down deletes replica Pods highest ordinal first and retains their PVCs.
Delete retained PVCs only after you are sure the data is no longer needed.

## Take a backup

```bash
kubectl cnmysql backup cluster-sample
```

This creates a `Backup` object with defaults (xtrabackup, prefer-standby, online).
Track it:

```bash
kubectl get backup backup-sample -w
kubectl describe backup backup-sample
```

## Clean up

Delete the Cluster:

```bash
kubectl delete cluster cluster-sample
```

Deleting a `Backup` object does not delete S3 objects. Remove object store data
manually or through external lifecycle policies.

## Next steps

- Configure object storage and backups.
- Enable continuous archiving before relying on PITR.
- Read the operations runbook before testing switchover or failover.
