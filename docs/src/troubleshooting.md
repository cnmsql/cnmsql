---
title: "Troubleshooting"
description: "Common cloudnative-mysql symptoms, likely causes, and first commands to run."
sidebar_position: 18
---

# Troubleshooting

This page starts with symptoms and points to the first places to inspect. cloudnative-mysql
surfaces most issues through Cluster/Backup status, Kubernetes Events, and the
instance-manager logs.

## First commands

```bash
kubectl cnmysql status <cluster>
kubectl cnmysql logs <cluster>
kubectl describe cluster <cluster>
kubectl get events --sort-by=.lastTimestamp
kubectl get backup
kubectl get scheduledbackup
```

Operator logs:

```bash
kubectl logs -n cloudnative-mysql-system deployment/cloudnative-mysql-controller-manager -c manager
```

Instance logs:

```bash
kubectl logs pod/<cluster>-1 -c manager
```

## Cluster is not Ready

Check:

```bash
kubectl cnmysql status <cluster>
kubectl cnmysql logs <cluster>
kubectl describe pod <pod>
```

Common causes:

- cert-manager has not produced TLS Secrets yet;
- PVC is Pending due to storage class or capacity;
- image pull failed;
- unsupported Cluster shape is blocked by the controller;
- instance-manager `/status` is unavailable;
- initdb, restore, or join init container failed.

Look at `status.phase`, `status.phaseReason`, and Events first.

## Replica will not join

Check the replica init container logs:

```bash
kubectl logs pod/<replica-pod> -c initdb
```

Common causes:

- primary is not Ready yet;
- mTLS material is missing or invalid;
- source manager endpoint is unreachable;
- XtraBackup stream failed;
- target PVC already contains incompatible data;
- MySQL version/image is incompatible with the source backup.

Replica provisioning uses XtraBackup over the existing instance-manager mTLS
port. Network policies or service DNS issues can break the join path.

## Replica is Running but not replicating

A replica whose Pod is Running but whose replication has stopped at the SQL layer
(a halted IO/SQL thread with a recorded error, e.g. a duplicate-key conflict) is
reported under `status.replicationBrokenInstances` and marks the cluster
`Degraded`, even though the Pod looks healthy. The `Degraded` condition reason
names the instance and its replication error.

Check:

```bash
kubectl cnmysql status <cluster>
kubectl get cluster <cluster> -o jsonpath='{.status.replicationBrokenInstances}'
kubectl logs pod/<replica-pod> -c manager
```

If the break is not transient, re-initialise the replica (see below) to re-clone
it from a backup.

## Diverged or broken replica recovery

A replica listed in `status.divergedInstances` (errant GTIDs) or
`status.replicationBrokenInstances` (stopped replication) is held out of service.
MySQL has no `pg_rewind` to realign it surgically, so the remediation is to
re-initialise it. The operator deletes its Pod and PVC and re-clones a fresh
copy from a backup, keeping the instance's name and `server_id`:

```bash
kubectl cnmysql reinit <cluster> <replica>
```

This is destructive: data only on that instance (including errant transactions)
is lost. It is always human-triggered, and the current primary is refused, so
switch over first if you need to rebuild a former primary. See the
[operations runbook](./operations.md#re-initialise-an-instance-from-scratch).

## Primary change is stuck

Inspect:

```bash
kubectl cnmysql status <cluster>
```

Common causes:

- target replica is not healthy;
- target GTID set does not contain the old primary's observed GTID set;
- `spec.maxSwitchoverDelay` expired;
- old primary could not be demoted or fenced;
- a former primary returned with errant transactions.

Check `status.currentPrimary`, `status.targetPrimary`,
`status.targetPrimaryTimestamp`, `status.divergedInstances`, and Events.

## Automatic failover did not happen

cloudnative-mysql blocks failover when it cannot prove a safe candidate.

Check:

```bash
kubectl cnmysql status <cluster>
```

Likely explanations:

- failover delay has not elapsed;
- Kubernetes still reports the primary Pod as Ready;
- no ready replica exists;
- replication SQL state is unhealthy;
- GTID sets are incomparable or divergent;
- every surviving candidate is known-diverged (listed in
  `status.divergedInstances`), so promoting one would make errant transactions
  canonical; the blocked reason says "every replica candidate has diverged ...
  manual recovery required";
- the only candidate is being deleted.

Failover should not be triggered solely by a temporary manager status endpoint
failure while Kubernetes still routes the primary as Ready.

When failover is blocked because the only survivors are diverged, recover by
re-initialising a survivor (see below) and letting it re-clone from a backup.

## Backup failed

Inspect:

```bash
kubectl describe backup <backup>
kubectl get job <backup>-backup
kubectl logs job/<backup>-backup
```

Common causes:

- missing object-store configuration;
- missing or invalid S3 credentials;
- no healthy backup source;
- source instance-manager stream failed;
- XtraBackup failed;
- object-store upload failed.

The controller writes the backup phase, error, Job name, selected source
instance, destination path, and conditions into Backup status.

## ScheduledBackup did not create a Backup

Inspect:

```bash
kubectl describe scheduledbackup <scheduledbackup>
kubectl get backup -l mysql.cloudnative-mysql.io/scheduled-backup=<scheduledbackup>
```

Common causes:

- `spec.suspend: true`;
- invalid six-field cron expression;
- a child Backup is still running, so the concurrency guard is deferring;
- deterministic Backup name collision with a non-owned Backup;
- first scheduled time has not arrived and `immediate` is false.

The schedule has six fields including seconds.

## Continuous archiving is degraded

Inspect:

```bash
kubectl get cluster <cluster> -o jsonpath='{.status.continuousArchiving}'
kubectl describe cluster <cluster>
```

Common causes:

- object-store endpoint or credentials are wrong;
- primary cannot upload objects;
- active binlog has not rotated yet;
- object-store outage;
- archiver cannot update manifests or `_index.json`;
- purge guard is detecting lag.

PITR depends on the archive index and manifests, not just raw binlog objects.

## PITR target is unsatisfiable

Common causes:

- recovery target is before the base backup anchor;
- target GTID or target time is beyond archived coverage;
- `_index.json` is missing or stale;
- required binlog segment or manifest was deleted;
- archive has a forked or incoherent timeline.

Prefer `targetGTID` for exact recovery boundaries. `targetTime` depends on
binlog event timestamps and server clocks.

## Object-store data remains after deleting Backup

This is expected today. Deleting a `Backup` object does not delete
`backup.xbstream` or `metadata.json` from the object store. Remote cleanup is a
planned finalizer/retention feature.

## Useful labels

```text
mysql.cloudnative-mysql.io/cluster=<cluster>
mysql.cloudnative-mysql.io/instance=<instance>
mysql.cloudnative-mysql.io/role=primary|replica
mysql.cloudnative-mysql.io/scheduled-backup=<scheduledbackup>
```

These labels make it easier to list Pods, PVCs, Services, and generated Backups
for one Cluster or schedule.
