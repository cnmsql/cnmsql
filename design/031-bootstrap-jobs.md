# 031 — Instance Bootstrap as Jobs

- **Status:** accepted
- **Milestone:** 0.8.0
- **Issue:** [#127](https://github.com/cnmsql/cnmsql/issues/127)
- **Depends on:** [030 — Instance Credentials from the API](030-instance-credentials-from-api.md) (the bootstrap commands read their passwords through the API under the instance ServiceAccount)
- **Supersedes:** none

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Read §1–§5 before starting Task 1: the tasks argue from them.

**Goal:** Each instance's data directory is bootstrapped (initdb, restore, join, import) by a one-shot Job that runs before the instance Pod exists, and the instance Pod stops carrying the `bootstrap` and `import` init containers.

**Architecture:** A new PVC annotation records whether an instance's volume holds a bootstrapped data directory. While it does not, `ensureInstance` drives a per-instance Job `<instance>-<mode>` that mounts the PVC and runs the same `manager instance …` command the init container runs today. When the Job succeeds, the operator marks the PVC, deletes the Job, and only then creates the Pod. The recovery and import sources are resolved only until the bootstrap primary's volume is marked, so the source `Backup`, its object store and its Secrets can go away afterwards. Failed Jobs surface as a `BootstrapFailed` condition, a phase reason and an Event.

**Tech Stack:** Go, controller-runtime (`batchv1.Job`, `Owns`, fake client for unit tests), kubebuilder RBAC markers, Ginkgo e2e on Kind with MinIO.

**Spec:** this document (§1–§5) and [issue #127](https://github.com/cnmsql/cnmsql/issues/127).

## 1. Why

The data-directory bootstrap runs as the `bootstrap` init container of the instance Pod (`internal/controller/cluster_pod.go`, `bootstrapArgs`), and a cluster bootstrapped from a logical backup gets a second init container, `import` (`internal/controller/cluster_import.go`, `importContainer`). They stay in the Pod spec for the life of the Pod:

1. **The bootstrap source becomes a runtime dependency.** `buildPlan` calls `resolveRecovery` on every reconcile while `spec.bootstrap.recovery` is set. If the source `Backup` is deleted later (by hand, or by a ScheduledBackup history limit on the source cluster), every reconcile of a recovered cluster that has been healthy for months fails and the cluster goes `Blocked`.
2. **Object-store credentials stay in the instance Pod spec.** On a recovered primary, `bootstrapEnv` adds the recovery object-store env (Secret refs) to the init container. Every recreation of that Pod needs those Secrets, although the restore is a no-op once the data directory is initialised. A missing Secret leaves the Pod in `CreateContainerConfigError`. It also breaks the design 008 rule that object-store credentials stay out of database Pods.
3. **Sizing and scheduling are awkward.** A restore can need far more memory than steady-state mysqld. Only `backup.jobTemplate.resources` is copied onto the init container, as a special case, and no scheduling field can apply because the init container shares the instance Pod.
4. **Visibility is poor.** A failed restore shows as `Init:CrashLoopBackOff` on the instance Pod, with no object of its own carrying status, backoff and a deadline.
5. **More work is coming in this style.** Every new bootstrap mode adds to the Pod spec (and to the template-hash special cases in `restartTriggeringPodSpec`).

Every bootstrap command is already idempotent on an initialised data directory (`instance.IsInitialized`, the `.cnmsql-bootstrapped` sentinel; `restore` gates PITR replay on its own sentinel; `import` is a no-op once it finished). So on every Pod start after the first, the init containers do nothing. Moving them out loses no behaviour.

## 2. Scope

### In scope

- A PVC annotation marking the volume's bootstrap state, set at creation, backfilled on volumes that predate this change.
- One bootstrap Job per instance volume, for the four modes: `initdb` (primary, and Group Replication secondaries without an application schema), `restore` (physical recovery and PITR), `join` (async replica clone), `import` (initdb then logical load, in one Job).
- The Job lifecycle in `ensureInstance`: create, wait, mark the volume, delete the Job, then create the Pod. The Job and the Pod never run at the same time.
- The instance Pod loses the `bootstrap` and `import` init containers, the recovery object-store env, and the recovery-resources special case. `bootstrap-controller` stays.
- The recovery and import sources are resolved only until the bootstrap primary's volume is marked bootstrapped.
- Replica re-initialisation (`cluster_reinit.go`, and `cluster_auto_reinit.go` through it) and scale-down clean up bootstrap Jobs; a re-cloned replica goes through a new `join` Job.
- Failed bootstrap Jobs surface as a `BootstrapFailed` Cluster condition, a `Blocked`/`Degraded` phase reason and a Warning Event carrying the Job's reason.
- `backup.jobTemplate` shapes the bootstrap Jobs: priorityClassName, tolerations (added to the instance's), activeDeadline, labels and annotations (§3 B8), and resources for the `restore` Job only (§3 B9).
- One-time rolling restart on upgrade, shipped in 0.8.0 together with design 030's (§5).

### Out of scope

- **New API fields.** No `bootstrap.jobTemplate`, no `backoffLimit` field. The bootstrap Jobs reuse `spec.backup.jobTemplate`.
- **Keeping a succeeded Job for its logs.** The operator deletes it (§3 B4). The Event and the operator log record the run.
- **`LogicalRestore`** (design 029). It already runs as its own worker Job against a running cluster.
- **Re-bootstrapping a primary on an established cluster.** Refused and surfaced (§3 B11), as a safety guard. Recovering such a cluster stays a manual operation.
- **A per-instance status object.** Failures surface on the Cluster.

## 3. Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| B1 | One Job per volume bootstrap, named `<instance>-<mode>`, mode ∈ `initdb`, `restore`, `join`, `import`. The `import` Job runs `initdb` as an init container, then `import` as its main container | One Job = one volume state transition. Two Jobs for import would need a second marker for "initdb done, import pending" |
| B2 | PVC annotation `mysql.cnmsql.co/pvc-status`: `initializing` on creation, `ready` after the Job succeeded. A PVC **without** the annotation predates this change and counts as `ready`; `ensurePVC` backfills `ready` on it | Before this change a PVC only existed alongside the Pod that bootstrapped it, so an unannotated PVC was bootstrapped (or its old Pod is still bootstrapping it in place). The operator upgrade then never runs a Job or resolves a source for an existing cluster |
| B3 | The Job carries `mysql.cnmsql.co/pvc-uid` = the PVC's UID. A Job whose UID differs from the current PVC's is deleted, whatever its state | A re-initialised replica gets a new, empty PVC with the same name. A succeeded Job from the previous volume must never mark the new one bootstrapped |
| B4 | On success: patch the PVC to `ready`, then delete the Job with foreground propagation. The Pod is created on a later pass, once no bootstrap Job for the instance exists | The volume is RWO (or RWOP). Waiting until the Job and its Pods are gone guarantees they never overlap, and keeps the order crash-safe: a leftover Job next to a `ready` PVC is deleted first |
| B5 | On failure the Job stays. The operator replaces a failed Job only when the Job it would build now differs from it (annotation `mysql.cnmsql.co/bootstrap-spec-hash`, a hash of the desired `JobSpec`). Otherwise the user deletes the Job to retry. A running Job is never replaced | No hot loop on a persistent failure. A spec fix (a new backup, a longer deadline) and a failover (the `join` source host changes) retry by themselves |
| B6 | `backoffLimit` is a constant 6 (the Kubernetes default). `activeDeadlineSeconds` comes from `backup.jobTemplate.activeDeadline` (default 24h, via `backupJobActiveDeadlineSeconds`). No TTL | The init container retried forever; 6 retries with exponential backoff (~6 minutes) cover a briefly unavailable primary or object store. No API field needed |
| B7 | The Job runs under the instance's ServiceAccount (`<instance>-instance`) | Design 030's per-cluster Role already lets it `get`/`watch` the credential Secrets and `get` the Cluster; the commands are the same binary and the same trust level as the init container they replace |
| B8 | Scheduling comes from the **instance**: `nodeSelector`, `affinity`, `topologySpreadConstraints`, `schedulerName`, and `tolerations` from the Cluster spec. From `backup.jobTemplate`: `resources` (restore only, B9), `priorityClassName` (overrides the instance's), `tolerations` (appended), `activeDeadline`, `labels`, `annotations`. The template's `nodeSelector` and `affinity` are **not** applied | With a `WaitForFirstConsumer` storage class, the Job's Pod decides which node or zone the volume binds to. A template `nodeSelector` that sends the Job to a node the instance Pod cannot use (local volumes, zonal disks, anti-affinity) would leave the instance Pod unschedulable forever. **This narrows the issue's "full jobTemplate" wording; see §5.** |
| B9 | Resources: the `restore` Job uses `backup.jobTemplate.resources` if set, else `spec.resources`. The `initdb`, `join` and `import` Jobs always use `spec.resources` | Keeps today's sizing exactly: the template already sized the restore init container, and the `bootstrap`/`import` init containers otherwise ran with the instance's resources. `import` starts a temporary mysqld with the instance's `my.cnf`, sized for those resources. A template that users set small for backup workers must not start shrinking join and import |
| B10 | `buildPlan` resolves `bootstrap.recovery` and `bootstrap.initdb.import` only while the cluster is not established **and** the bootstrap primary's PVC is not bootstrapped (`primaryBootstrapped`) | Once the primary's data exists, the source is never read again. Legacy PVCs count as bootstrapped (B2), so an upgrade heals clusters whose Backup is already gone |
| B11 | On an established cluster, the operator never creates a Job for the current primary. It emits a `BootstrapRefused` Warning and leaves the instance down | With B10 the plan has no recovery source any more, so a primary Job would `initdb` an empty primary over a live cluster. Today's code would do the same through the init container; this closes it |
| B12 | The Job's Pod carries only `app.kubernetes.io/name`, `mysql.cnmsql.co/bootstrap-instance` and `mysql.cnmsql.co/bootstrap-mode`. No cluster, instance, role or routable label. The Job object carries the cluster label | Services, PDBs, the PodMonitor, `scaleDownReplicas` and `observe` select instance Pods by those labels and must never see a bootstrap Pod. The Job keeps the cluster label so `kubectl cnmsql report` collects it |
| B13 | Pod template change (init containers removed) ships in 0.8.0, the release that already rolls every instance once for design 030 | One roll instead of two. The commit is marked breaking |

## 4. Design

### 4.1 Volume states

```
            ensurePVC (new volume)                  Job Complete
  (none) ────────────────────────▶ initializing ─────────────────▶ ready ──▶ Pod created
                                        │  ▲                          ▲
                        Job Failed      │  │ spec changed / Job       │ backfill (no annotation:
                        (stays, surfaced)▼  │ deleted by user          │  volume predates 031)
                                   Job failed
```

`pvcBootstrapped(pvc)` is `annotation absent || annotation == "ready"`.

### 4.2 `ensureInstance` with bootstrap

```
reconcileReinit          (also deletes the instance's bootstrap Jobs, §4.5)
ensureConfigMap
ensurePVC                (new: annotate initializing; existing unannotated: backfill ready)
ensureInstanceService
ensureBootstrapped  ───▶ false: stop here for this instance (rolled=false)
rollForResize
ensurePod
```

`ensureBootstrapped(ctx, cluster, plan, inst) (bool, error)`:

1. Read the PVC and the instance's bootstrap Jobs (label `mysql.cnmsql.co/bootstrap-instance=<inst>`).
2. PVC bootstrapped → delete any leftover Job; return `true` only when none is left.
3. The instance Pod exists → return `false` (never run a Job next to the Pod).
4. `inst.IsPrimary && cluster.IsEstablished()` → `BootstrapRefused` Event, return `false` (B11).
5. Build the desired Job. For each existing Job:
   - terminating → wait;
   - `pvc-uid` differs from the PVC's → delete, wait (B3);
   - `Complete` → patch PVC `ready`, Event `InstanceBootstrapped`, delete the Job, wait (B4);
   - `Failed` with a different spec hash → delete, wait (B5);
   - `Failed` with the same hash, or running → wait.
6. No Job → create the desired one, wait.

"Wait" returns `false`; the `Owns(&batchv1.Job{})` watch re-triggers the reconcile.

### 4.3 The Job

| Field | Value |
|---|---|
| Name | `<instance>-<mode>` |
| Owner | the Cluster (controller reference) |
| Labels (Job) | `backup.jobTemplate.labels` + `app.kubernetes.io/name`, `app.kubernetes.io/managed-by`, `mysql.cnmsql.co/cluster`, `mysql.cnmsql.co/bootstrap-instance`, `mysql.cnmsql.co/bootstrap-mode` (operator keys win) |
| Annotations (Job) | template annotations + `mysql.cnmsql.co/pvc-uid`, `mysql.cnmsql.co/bootstrap-spec-hash` |
| `backoffLimit` / `activeDeadlineSeconds` / TTL | 6 / template (default 24h) / none |
| Pod labels | template labels + `app.kubernetes.io/name`, bootstrap-instance, bootstrap-mode |
| ServiceAccount | `<instance>-instance` |
| Volumes | the instance Pod's volumes (scratch, data PVC, run, backup, config, server-tls, client-ca) |
| Init containers | `bootstrap-controller` (copies the manager binary), plus `initdb` for mode `import` |
| Main container | named after the mode; the args `restoreArgs` / `initdbArgs` / `joinArgs` / `importArgs` build today; env `initEnv(plan)` plus the recovery or import store env |
| Scheduling / resources | §3 B8, B9 |
| Security | `podSecurityContext(cluster)`, container `cluster.Spec.SecurityContext`, `imagePullSecrets` |

The Job's Pod gets no `cluster.Spec.Env`/`EnvFrom`, like the init containers today.

### 4.4 Status

`observe` lists the cluster's bootstrap Jobs into `observedCluster.BootstrapJobs`, skipping completed and terminating ones. `computeClusterPhase`:

- a failed Job, cluster not established → `Blocked`, reason `Bootstrap Job <job> for <instance> failed: <Reason>: <message>`;
- a failed Job, established (a replica's `join`) → `Degraded`, same reason;
- no ready instance and a running Job → `Pending`, reason `Waiting for bootstrap Job <job> (<mode>) of <instance>` (replaces the import-specific reason).

`patchStatus` sets condition `BootstrapFailed` (True with the Job's reason, or False with reason `NoFailedBootstrapJobs`) on every full observation, and emits a `BootstrapJobFailed` Warning Event on the False→True transition.

### 4.5 Re-initialisation and scale-down

`reconcileReinit` deletes the Pod, then the instance's bootstrap Jobs, then the PVC, and reports the teardown complete only when all three are gone. A running `join` Job holds the PVC, so the PVC would stay `Terminating` otherwise. The normal pass then creates a new PVC (`initializing`) and a new `join` Job. `reconcileAutoReinit` goes through the same annotation. `removeInstanceResources` (scale-down) deletes the instance's bootstrap Jobs too, so a `join` for an instance that is no longer wanted stops — including when the instance's Pod was never created: a garbage-collect pass cleans up the bootstrap Jobs and initializing PVCs of ordinals beyond `spec.instances` on every reconcile (#143), and a PVC that never finished bootstrapping is deleted with the instance. Bootstrapped volumes keep the M4 retention policy.

`gateInstance` treats "PVC exists and is bootstrapped" as "member has data" (was: "PVC exists"). A PVC that is still `initializing` is a member being provisioned and needs a donor like a brand-new one.

## 5. Rollout, compatibility and follow-ups

- **Operator upgrade.** Existing PVCs have no annotation → `ready` (backfilled), so no Job runs and no source is resolved. The Pod template loses the `bootstrap` init container, so the template hash changes and every instance rolls once through the normal path (replicas first, switchover, primary). 0.8.0 rolls every instance for design 030 anyway; both changes land in the same roll.
- **Upgrade during a first bootstrap.** An instance whose old Pod is still in its `bootstrap` init container when the operator is upgraded keeps a volume without the annotation (→ `ready`). If the roll recreates that Pod before the init container finished, the new Pod starts `run` on an unfinished data directory and crash-loops. On an established cluster, auto-reinit re-clones a replica after 7 restarts; otherwise re-initialise it by hand. The upgrade note says to upgrade when no instance is initialising.
- **Operator downgrade.** The old operator re-adds the init containers (another roll) and ignores the annotation and the Jobs. Finished bootstrap Jobs left behind are owned by the Cluster and harmless; delete them.
  - **Never downgrade while a bootstrap Job runs.** The old operator does not know about Jobs: it would create the instance Pod while the Job still mounts the volume, and on an RWO volume both can land on the same node, so two processes would write one data directory. Before downgrading, `kubectl get jobs -A -l mysql.cnmsql.co/bootstrap-instance` must show no active Job. The upgrade docs say so.
- **Existing `backup.jobTemplate` settings.** `resources` keep their meaning: they size the restore, as before, and nothing else (B9). `priorityClassName` and `tolerations` now also reach the bootstrap Jobs; that is new behaviour, documented, and only loosens scheduling (tolerations are added to the instance's).
- **Visible changes.** A failing bootstrap no longer shows as `Init:CrashLoopBackOff` on the instance Pod: alerts keyed on that must move to the `BootstrapFailed` condition or the `BootstrapJobFailed` Event. The import-specific phase reason ("Waiting for the primary instance to initialise and import the logical backup") becomes "Waiting for bootstrap Job … (import) of …".
- **Recovered clusters whose source Backup is gone.** Today they are `Blocked` on every reconcile. After the upgrade, the primary's volume counts as bootstrapped and the plan stops resolving the source: the cluster recovers by itself.
- **Issue wording (B8).** The issue says the full `backup.jobTemplate` (including `nodeSelector`) should apply. This design deliberately keeps the template's `nodeSelector` and `affinity` off the bootstrap Jobs. Comment on #127 with the reason when this lands.
- **Follow-up (#47 phase 2).** Any later bootstrap mode is a new `bootstrapMode` and a branch in `bootstrapContainers`; the Pod spec does not change.

---

## 6. Implementation plan

### Global Constraints

- Go module `github.com/cnmsql/cnmsql`. Do not edit `config/rbac/role.yaml`, `config/crd/bases/*`, `zz_generated.*`, `dist/*` or `PROJECT` by hand. Run `make manifests generate` after changing RBAC markers or API doc comments.
- Branch from `main` once `feat/creds-from-api-server` (design 030) has merged, or stack on it. The bootstrap Jobs rely on the commands reading passwords through the API.
- Log messages follow the Kubernetes style: capital first letter, no trailing period, past tense for completed actions, balanced key/value pairs. Event reasons are CamelCase.
- Conventional commits: lowercase, no body, no co-author. Task 4's commit uses `feat(instance)!:` so git-cliff marks it breaking.
- After Go edits: `make lint-fix` and `make test`. After doc edits: `npm run build` in `docs/`.
- Reuse, do not duplicate: `restoreArgs`, `initdbArgs`, `joinArgs`, `importArgs`, `initEnv`, `volumeMounts`, `affinity`, `podSecurityContext`, `instanceServiceAccountName`, `mergeJobTemplates`, `hasResourceRequirements`, `combineStringMaps`, `workerJobLabels`, `backupJobActiveDeadlineSeconds`, `jobFinished`, `workerJobFailure`, `hashObject`, `managerBinary`, `managerBootstrapCmd`, `scratchVolumeName`, `appLabelValue`, `clusterLabel` (all in `internal/controller`).
- New code for this design goes in `internal/controller/cluster_bootstrap.go` and `internal/controller/cluster_bootstrap_test.go` unless a task says otherwise.

### Review Focus

1. **A succeeded Job from a previous volume must never mark a new, empty volume bootstrapped** (re-init reuses the PVC name). The Job is deleted and a new one created. Covered in Task 3 (`TestEnsureBootstrappedDeletesJobForOtherVolume`).
2. **Upgrading the operator on an existing cluster must not run a bootstrap Job or read the recovery source**, even when the source Backup is gone. Covered in Task 1 (`TestEnsurePVCBackfillsLegacyVolume`) and Task 5 (`TestBuildPlanSkipsRecoveryForLegacyVolume`).
3. **The Job and the instance Pod never overlap**: no Job while the Pod exists, no Pod while any bootstrap Job for the instance exists, including a finished one. Covered in Task 3 (`TestEnsureBootstrappedWaitsForInstancePod`, `TestEnsureBootstrappedRemovesLeftoverJobBeforePod`).
4. **An established cluster must never initdb or restore its current primary** because its volume is new. Covered in Task 3 (`TestEnsureBootstrappedRefusesPrimaryOnEstablishedCluster`).
5. **A failed Job must not be recreated in a loop, but a changed spec must retry** (a longer deadline, a new source host after failover). Covered in Task 3 (`TestEnsureBootstrappedKeepsFailedJobWithSameSpec`, `TestEnsureBootstrappedReplacesFailedJobWhenSpecChanges`) and end to end in Task 8.

---

### Task 1: Mark bootstrap state on instance volumes

**Files:**
- Create: `internal/controller/cluster_bootstrap.go`
- Create: `internal/controller/cluster_bootstrap_test.go`
- Modify: `internal/controller/cluster_resources.go` (`ensurePVC`)

**Interfaces:**
- Produces: constants `pvcStatusAnnotation`, `pvcStatusInitializing`, `pvcStatusReady`; `func pvcBootstrapped(pvc *corev1.PersistentVolumeClaim) bool`. `ensurePVC` creates new PVCs with `pvcStatusAnnotation: pvcStatusInitializing` and backfills `pvcStatusReady` on existing PVCs without the annotation.

- [ ] **Step 1: Write the failing tests** in `cluster_bootstrap_test.go`:

```go
package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestPVCBootstrapped(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		annotations map[string]string
		want        bool
	}{
		{"legacy volume without annotation", nil, true},
		{"initializing", map[string]string{pvcStatusAnnotation: pvcStatusInitializing}, false},
		{"ready", map[string]string{pvcStatusAnnotation: pvcStatusReady}, true},
		{"unknown value", map[string]string{pvcStatusAnnotation: "detached"}, false},
	} {
		pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Annotations: tc.annotations}}
		if got := pvcBootstrapped(pvc); got != tc.want {
			t.Errorf("%s: pvcBootstrapped = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestEnsurePVCMarksNewVolumeInitializing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	inst := testPlan().instanceFor(cluster, 1)

	if _, err := r.ensurePVC(ctx, cluster, inst); err != nil {
		t.Fatal(err)
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.PVCName}, pvc); err != nil {
		t.Fatal(err)
	}
	if got := pvc.Annotations[pvcStatusAnnotation]; got != pvcStatusInitializing {
		t.Fatalf("new PVC %s = %q, want %q", pvcStatusAnnotation, got, pvcStatusInitializing)
	}
}

func TestEnsurePVCBackfillsLegacyVolume(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	scheme := testScheme(t)
	inst := testPlan().instanceFor(cluster, 1)
	legacy := instancePVC(cluster, inst.PVCName)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, legacy).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	if _, err := r.ensurePVC(ctx, cluster, inst); err != nil {
		t.Fatal(err)
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.PVCName}, pvc); err != nil {
		t.Fatal(err)
	}
	if got := pvc.Annotations[pvcStatusAnnotation]; got != pvcStatusReady {
		t.Fatalf("legacy PVC %s = %q, want %q", pvcStatusAnnotation, got, pvcStatusReady)
	}
}

func TestEnsurePVCKeepsInitializingVolume(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	scheme := testScheme(t)
	inst := testPlan().instanceFor(cluster, 1)
	pvc := instancePVC(cluster, inst.PVCName)
	pvc.Annotations = map[string]string{pvcStatusAnnotation: pvcStatusInitializing}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, pvc).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	if _, err := r.ensurePVC(ctx, cluster, inst); err != nil {
		t.Fatal(err)
	}
	got := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.PVCName}, got); err != nil {
		t.Fatal(err)
	}
	if got.Annotations[pvcStatusAnnotation] != pvcStatusInitializing {
		t.Fatalf("ensurePVC rewrote an initializing volume to %q", got.Annotations[pvcStatusAnnotation])
	}
}
```

- [ ] **Step 2: Run them and see them fail**

Run: `go test ./internal/controller/ -run 'TestPVCBootstrapped|TestEnsurePVC(Marks|Backfills|Keeps)' -count=1`
Expected: build failure, `undefined: pvcStatusAnnotation`.

- [ ] **Step 3: Implement.** Create `cluster_bootstrap.go` with the license header copied from `cluster_reinit.go` and:

```go
package controller

import (
	corev1 "k8s.io/api/core/v1"
)

const (
	// pvcStatusAnnotation records whether an instance's data volume holds a
	// bootstrapped data directory (design 031). A new volume starts
	// initializing; the operator marks it ready when the instance's bootstrap
	// Job succeeds, and only then creates the instance Pod.
	pvcStatusAnnotation   = "mysql.cnmsql.co/pvc-status"
	pvcStatusInitializing = "initializing"
	pvcStatusReady        = "ready"
)

// pvcBootstrapped reports whether the volume holds a bootstrapped data
// directory. A volume without the annotation predates bootstrap Jobs: the
// instance Pod that created it bootstrapped it in an init container, so it
// counts as bootstrapped.
func pvcBootstrapped(pvc *corev1.PersistentVolumeClaim) bool {
	status, ok := pvc.Annotations[pvcStatusAnnotation]
	return !ok || status == pvcStatusReady
}
```

In `ensurePVC` (`cluster_resources.go`), in the create branch next to `pvc.Labels = …`:

```go
		pvc.Annotations = map[string]string{pvcStatusAnnotation: pvcStatusInitializing}
```

and right after the `Get` succeeds (before the `cluster.Spec.Storage.Size == ""` early return, so the backfill also runs for template-sized volumes):

```go
	// A volume from before bootstrap Jobs was bootstrapped by its Pod's init
	// container. Record that, so the state no longer depends on the annotation
	// being absent.
	if _, ok := pvc.Annotations[pvcStatusAnnotation]; !ok {
		before := pvc.DeepCopy()
		if pvc.Annotations == nil {
			pvc.Annotations = map[string]string{}
		}
		pvc.Annotations[pvcStatusAnnotation] = pvcStatusReady
		if err := r.Patch(ctx, pvc, client.MergeFrom(before)); err != nil {
			return false, err
		}
	}
```

- [ ] **Step 4: Run the tests** — same command, expect PASS. Then `make test`: existing tests still pass (nothing reads the annotation yet).

- [ ] **Step 5: Commit**

```bash
git add internal/controller/cluster_bootstrap.go internal/controller/cluster_bootstrap_test.go internal/controller/cluster_resources.go
git commit -m "feat(instance): record bootstrap state on instance volumes"
```

---

### Task 2: Build the per-instance bootstrap Job

**Files:**
- Modify: `internal/controller/cluster_bootstrap.go`
- Modify: `internal/controller/cluster_pod.go` (extract `instanceVolumes`)
- Test: `internal/controller/cluster_bootstrap_test.go`

**Interfaces:**
- Consumes: Task 1 constants.
- Produces:
  - `type bootstrapMode string` with `bootstrapModeInitDB = "initdb"`, `bootstrapModeRestore = "restore"`, `bootstrapModeJoin = "join"`, `bootstrapModeImport = "import"`.
  - constants `bootstrapInstanceLabel = "mysql.cnmsql.co/bootstrap-instance"`, `bootstrapModeLabel = "mysql.cnmsql.co/bootstrap-mode"`, `bootstrapPVCUIDAnnotation = "mysql.cnmsql.co/pvc-uid"`, `bootstrapSpecHashAnnotation = "mysql.cnmsql.co/bootstrap-spec-hash"`, `bootstrapJobBackoffLimit int32 = 6`.
  - `func (r *ClusterReconciler) bootstrapModeFor(cluster *mysqlv1alpha1.Cluster, plan clusterPlan, inst instancePlan) bootstrapMode`
  - `func bootstrapJobName(inst instancePlan, mode bootstrapMode) string`
  - `func bootstrapJobTemplate(cluster *mysqlv1alpha1.Cluster) mysqlv1alpha1.BackupJobTemplate`
  - `func (r *ClusterReconciler) bootstrapJob(cluster *mysqlv1alpha1.Cluster, plan clusterPlan, inst instancePlan, mode bootstrapMode, pvc *corev1.PersistentVolumeClaim) (*batchv1.Job, error)`
  - `func instanceVolumes(plan clusterPlan, inst instancePlan) []corev1.Volume` (in `cluster_pod.go`, used by `podSpec` and `bootstrapJob`)

- [ ] **Step 1: Write the failing tests** (append to `cluster_bootstrap_test.go`; add imports `slices`, `strings`, `k8s.io/apimachinery/pkg/api/resource`, `mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"`, `"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"`, `batchv1 "k8s.io/api/batch/v1"`):

```go
func TestBootstrapModeFor(t *testing.T) {
	t.Parallel()
	r := &ClusterReconciler{}
	async := baseCluster()
	async.Spec.Instances = 3
	gr := grCluster(&mysqlv1alpha1.GroupReplicationStatus{GroupName: "g"})
	gr.Spec.Instances = 3
	plain := testPlan()
	plain.Instances = 3
	recovery := plain
	recovery.Recovery = &recoveryPlan{Bucket: "bkt"}
	imported := plain
	imported.Import = &importPlan{Bucket: "bkt"}

	for _, tc := range []struct {
		name    string
		cluster *mysqlv1alpha1.Cluster
		plan    clusterPlan
		ordinal int
		want    bootstrapMode
	}{
		{"primary initdb", async, plain, 1, bootstrapModeInitDB},
		{"primary restore", async, recovery, 1, bootstrapModeRestore},
		{"primary import", async, imported, 1, bootstrapModeImport},
		{"async replica clones", async, recovery, 2, bootstrapModeJoin},
		{"group replication secondary initialises", gr, recovery, 2, bootstrapModeInitDB},
	} {
		inst := tc.plan.instanceFor(tc.cluster, tc.ordinal)
		if got := r.bootstrapModeFor(tc.cluster, tc.plan, inst); got != tc.want {
			t.Errorf("%s: mode = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func restoreTestFixture(t *testing.T) (*ClusterReconciler, *mysqlv1alpha1.Cluster, clusterPlan, instancePlan, *corev1.PersistentVolumeClaim) {
	t.Helper()
	cluster := baseBackupCluster()
	cluster.Status.CurrentPrimary = ""
	cluster.Spec.Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
	}
	cluster.Spec.Affinity.NodeSelector = map[string]string{"pool": "db"}
	cluster.Spec.Affinity.Tolerations = []corev1.Toleration{{Key: "db"}}
	plan := testPlan()
	plan.ClusterName = cluster.Name
	plan.Recovery = &recoveryPlan{
		Bucket: "bkt", ArchiveKey: "a/backup.xbstream", MetadataKey: "a/metadata.json",
		StoreEnv: []corev1.EnvVar{{Name: objectstore.EnvBucket, Value: "bkt"}},
	}
	inst := plan.instanceFor(cluster, 1)
	pvc := instancePVC(cluster, inst.PVCName)
	pvc.UID = "uid-1"
	return &ClusterReconciler{Scheme: testScheme(t)}, cluster, plan, inst, pvc
}

func containerNames(cs []corev1.Container) []string {
	names := make([]string, 0, len(cs))
	for _, c := range cs {
		names = append(names, c.Name)
	}
	return names
}

func TestBootstrapJobRestore(t *testing.T) {
	t.Parallel()
	r, cluster, plan, inst, pvc := restoreTestFixture(t)
	deadline := metav1.Duration{Duration: 2 * time.Hour}
	cluster.Spec.Backup.JobTemplate = &mysqlv1alpha1.BackupJobTemplate{
		ActiveDeadline:    &deadline,
		Resources:         corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("4Gi")}},
		PriorityClassName: "restore",
		Tolerations:       []corev1.Toleration{{Key: "restore"}},
		NodeSelector:      map[string]string{"pool": "backup"},
		Labels:            map[string]string{"team": "db"},
	}

	job, err := r.bootstrapJob(cluster, plan, inst, bootstrapModeRestore, pvc)
	if err != nil {
		t.Fatal(err)
	}
	if job.Name != inst.Name+"-restore" {
		t.Fatalf("job name = %q", job.Name)
	}
	if job.Annotations[bootstrapPVCUIDAnnotation] != "uid-1" || job.Annotations[bootstrapSpecHashAnnotation] == "" {
		t.Fatalf("job annotations = %v", job.Annotations)
	}
	if job.Labels[clusterLabel] != cluster.Name || job.Labels["team"] != "db" {
		t.Fatalf("job labels = %v", job.Labels)
	}
	if ref := metav1.GetControllerOf(job); ref == nil || ref.Name != cluster.Name {
		t.Fatalf("job controller = %v, want the Cluster", ref)
	}
	if *job.Spec.BackoffLimit != bootstrapJobBackoffLimit || *job.Spec.ActiveDeadlineSeconds != 7200 {
		t.Fatalf("backoff/deadline = %d/%d", *job.Spec.BackoffLimit, *job.Spec.ActiveDeadlineSeconds)
	}

	pod := job.Spec.Template
	for _, key := range []string{clusterLabel, instanceLabel, roleLabel, routableLabel, podMonitorClusterLabel} {
		if _, ok := pod.Labels[key]; ok {
			t.Fatalf("bootstrap Pod carries instance label %q: %v", key, pod.Labels)
		}
	}
	if pod.Labels[bootstrapInstanceLabel] != inst.Name || pod.Labels[bootstrapModeLabel] != "restore" {
		t.Fatalf("pod labels = %v", pod.Labels)
	}
	spec := pod.Spec
	if spec.RestartPolicy != corev1.RestartPolicyNever || spec.ServiceAccountName != inst.Name+"-instance" {
		t.Fatalf("restartPolicy/serviceAccount = %s/%s", spec.RestartPolicy, spec.ServiceAccountName)
	}
	if got := containerNames(spec.InitContainers); !slices.Equal(got, []string{"bootstrap-controller"}) {
		t.Fatalf("init containers = %v", got)
	}
	main := spec.Containers[0]
	args := strings.Join(main.Args, " ")
	if main.Name != "restore" || !strings.Contains(args, "instance restore") || !strings.Contains(args, "--bucket=bkt") {
		t.Fatalf("main container %q args %q", main.Name, args)
	}
	if !slices.ContainsFunc(main.Env, func(e corev1.EnvVar) bool { return e.Name == objectstore.EnvBucket }) {
		t.Fatal("restore container has no object-store env")
	}
	if !slices.ContainsFunc(spec.Volumes, func(v corev1.Volume) bool {
		return v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == inst.PVCName
	}) {
		t.Fatal("job does not mount the instance PVC")
	}
	// Scheduling follows the instance (B8): the template's nodeSelector must not
	// pull the volume onto a node the instance Pod cannot use.
	if spec.NodeSelector["pool"] != "db" {
		t.Fatalf("nodeSelector = %v, want the instance's", spec.NodeSelector)
	}
	var keys []string
	for _, tol := range spec.Tolerations {
		keys = append(keys, tol.Key)
	}
	if !slices.Equal(keys, []string{"db", "restore"}) {
		t.Fatalf("tolerations = %v, want instance then template", keys)
	}
	if spec.PriorityClassName != "restore" {
		t.Fatalf("priorityClassName = %q", spec.PriorityClassName)
	}
	if got := main.Resources.Limits.Memory().String(); got != "4Gi" {
		t.Fatalf("restore memory limit = %s, want the template's 4Gi", got)
	}
}

func TestBootstrapJobDefaultsToInstanceResources(t *testing.T) {
	t.Parallel()
	r, cluster, plan, inst, pvc := restoreTestFixture(t)
	job, err := r.bootstrapJob(cluster, plan, inst, bootstrapModeRestore, pvc)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range append(job.Spec.Template.Spec.InitContainers, job.Spec.Template.Spec.Containers...) {
		if got := c.Resources.Limits.Memory().String(); got != "1Gi" {
			t.Fatalf("container %s memory limit = %s, want the instance's 1Gi", c.Name, got)
		}
	}
}

// Only the restore takes the template's resources (B9): a template sized for
// small backup workers must not shrink initdb, join or import, whose temporary
// mysqld runs with the instance's my.cnf.
func TestBootstrapJobTemplateResourcesOnlySizeRestore(t *testing.T) {
	t.Parallel()
	r, cluster, plan, _, _ := restoreTestFixture(t)
	cluster.Spec.Instances = 2
	plan.Instances = 2
	plan.Import = &importPlan{Bucket: "bkt", DumpKey: "d", ManifestKey: "m"}
	cluster.Spec.Backup.JobTemplate = &mysqlv1alpha1.BackupJobTemplate{
		Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi")}},
	}

	for _, tc := range []struct {
		mode    bootstrapMode
		ordinal int
		want    string
	}{
		{bootstrapModeRestore, 1, "128Mi"},
		{bootstrapModeInitDB, 1, "1Gi"},
		{bootstrapModeImport, 1, "1Gi"},
		{bootstrapModeJoin, 2, "1Gi"},
	} {
		inst := plan.instanceFor(cluster, tc.ordinal)
		job, err := r.bootstrapJob(cluster, plan, inst, tc.mode, instancePVC(cluster, inst.PVCName))
		if err != nil {
			t.Fatal(err)
		}
		spec := job.Spec.Template.Spec
		for _, c := range append(spec.InitContainers, spec.Containers...) {
			if got := c.Resources.Limits.Memory().String(); got != tc.want {
				t.Errorf("%s: container %s memory limit = %s, want %s", tc.mode, c.Name, got, tc.want)
			}
		}
	}
}

func TestBootstrapJobImportRunsInitDBFirst(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	plan := testPlan()
	plan.ClusterName = cluster.Name
	plan.Import = &importPlan{Bucket: "bkt", DumpKey: "d/dump.sql.zst", ManifestKey: "d/logical.json"}
	inst := plan.instanceFor(cluster, 1)
	pvc := instancePVC(cluster, inst.PVCName)
	r := &ClusterReconciler{Scheme: testScheme(t)}

	job, err := r.bootstrapJob(cluster, plan, inst, bootstrapModeImport, pvc)
	if err != nil {
		t.Fatal(err)
	}
	spec := job.Spec.Template.Spec
	if got := containerNames(spec.InitContainers); !slices.Equal(got, []string{"bootstrap-controller", "initdb"}) {
		t.Fatalf("init containers = %v", got)
	}
	if got := strings.Join(spec.InitContainers[1].Args, " "); !strings.Contains(got, "instance initdb") || !strings.Contains(got, "--database="+appName) {
		t.Fatalf("initdb args = %q", got)
	}
	if got := strings.Join(spec.Containers[0].Args, " "); spec.Containers[0].Name != "import" || !strings.Contains(got, "--dump-key=d/dump.sql.zst") {
		t.Fatalf("import container %q args %q", spec.Containers[0].Name, got)
	}
}

func TestBootstrapJobGroupReplicationSecondaryHasNoSchema(t *testing.T) {
	t.Parallel()
	cluster := grCluster(&mysqlv1alpha1.GroupReplicationStatus{GroupName: "g"})
	cluster.Spec.Instances = 3
	plan := testPlan()
	plan.Instances = 3
	inst := plan.instanceFor(cluster, 2)
	r := &ClusterReconciler{Scheme: testScheme(t)}

	job, err := r.bootstrapJob(cluster, plan, inst, bootstrapModeInitDB, instancePVC(cluster, inst.PVCName))
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Join(job.Spec.Template.Spec.Containers[0].Args, " ")
	if !strings.Contains(args, "--group-replication") || strings.Contains(args, "--database=") {
		t.Fatalf("GR secondary initdb args = %q", args)
	}
}

func TestBootstrapJobHashFollowsSpec(t *testing.T) {
	t.Parallel()
	r, cluster, plan, inst, pvc := restoreTestFixture(t)
	first, err := r.bootstrapJob(cluster, plan, inst, bootstrapModeRestore, pvc)
	if err != nil {
		t.Fatal(err)
	}
	again, err := r.bootstrapJob(cluster, plan, inst, bootstrapModeRestore, pvc)
	if err != nil {
		t.Fatal(err)
	}
	if first.Annotations[bootstrapSpecHashAnnotation] != again.Annotations[bootstrapSpecHashAnnotation] {
		t.Fatal("spec hash is not stable across identical builds")
	}
	plan.Recovery.ArchiveKey = "b/backup.xbstream"
	changed, err := r.bootstrapJob(cluster, plan, inst, bootstrapModeRestore, pvc)
	if err != nil {
		t.Fatal(err)
	}
	if changed.Annotations[bootstrapSpecHashAnnotation] == first.Annotations[bootstrapSpecHashAnnotation] {
		t.Fatal("spec hash did not change with the restore source")
	}
}
```

Also add `"time"` to the imports.

- [ ] **Step 2: Run them and see them fail**

Run: `go test ./internal/controller/ -run 'TestBootstrap(ModeFor|Job)' -count=1`
Expected: build failure, `undefined: bootstrapMode`.

- [ ] **Step 3: Extract `instanceVolumes`** in `cluster_pod.go`. Move the `Volumes:` literal out of `podSpec` into:

```go
// instanceVolumes are the volumes an instance's Pod and its bootstrap Job
// mount: the data PVC, the scratch/run/backup emptyDirs, my.cnf and the TLS
// material.
func instanceVolumes(plan clusterPlan, inst instancePlan) []corev1.Volume {
	return []corev1.Volume{
		{Name: scratchVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: inst.PVCName}}},
		{Name: runVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: backupVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: inst.ConfigMapName}}}},
		{Name: "server-tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: inst.ServerTLSSecret}}},
		{Name: clientCAVolumeName, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: plan.ClientCASecretName}}},
	}
}
```

and set `Volumes: instanceVolumes(plan, inst),` in `podSpec`. The Pod spec is byte-identical, so the template hash does not move.

- [ ] **Step 4: Implement the builder** in `cluster_bootstrap.go` (add imports `cmp`, `slices`, `batchv1 "k8s.io/api/batch/v1"`, `metav1`, `controllerutil`, `mysqlv1alpha1`):

```go
// bootstrapMode is what an instance's bootstrap Job does to its empty volume.
type bootstrapMode string

const (
	bootstrapModeInitDB  bootstrapMode = "initdb"
	bootstrapModeRestore bootstrapMode = "restore"
	bootstrapModeJoin    bootstrapMode = "join"
	bootstrapModeImport  bootstrapMode = "import"
)

const (
	// bootstrapInstanceLabel and bootstrapModeLabel identify a bootstrap Job and
	// its Pod. The Pod carries no cluster or instance label, so Services, PDBs,
	// the PodMonitor and the instance listing never select it.
	bootstrapInstanceLabel = "mysql.cnmsql.co/bootstrap-instance"
	bootstrapModeLabel     = "mysql.cnmsql.co/bootstrap-mode"
	// bootstrapPVCUIDAnnotation binds a Job to the volume it bootstraps: a
	// re-initialised instance gets a new PVC with the same name, and a Job for
	// the old one must never mark it bootstrapped.
	bootstrapPVCUIDAnnotation = "mysql.cnmsql.co/pvc-uid"
	// bootstrapSpecHashAnnotation is the hash of the Job's spec. A failed Job is
	// replaced only when the Job the operator would build now differs.
	bootstrapSpecHashAnnotation = "mysql.cnmsql.co/bootstrap-spec-hash"
)

// bootstrapJobBackoffLimit is the Kubernetes default: with its exponential
// backoff it rides out a primary or an object store that is briefly away.
const bootstrapJobBackoffLimit int32 = 6

// bootstrapModeFor picks how an instance's empty volume is bootstrapped: the
// primary initialises a fresh data directory, restores a physical backup or
// loads a logical one; an async replica clones the primary; a Group
// Replication member initialises an empty server and provisions itself from a
// group donor via distributed recovery at run time.
func (r *ClusterReconciler) bootstrapModeFor(cluster *mysqlv1alpha1.Cluster, plan clusterPlan, inst instancePlan) bootstrapMode {
	if inst.IsPrimary {
		switch {
		case plan.Recovery != nil:
			return bootstrapModeRestore
		case plan.Import != nil:
			return bootstrapModeImport
		}
		return bootstrapModeInitDB
	}
	if r.topologyReconciler(cluster).PodPolicy(cluster).InitializeReplica {
		return bootstrapModeInitDB
	}
	return bootstrapModeJoin
}

func bootstrapJobName(inst instancePlan, mode bootstrapMode) string {
	return inst.Name + "-" + string(mode)
}

// bootstrapJobTemplate is the cluster-wide worker Job template the bootstrap
// Jobs take their priority, tolerations, deadline and metadata from, and the
// restore Job its resources.
func bootstrapJobTemplate(cluster *mysqlv1alpha1.Cluster) mysqlv1alpha1.BackupJobTemplate {
	if cluster.Spec.Backup == nil {
		return mysqlv1alpha1.BackupJobTemplate{}
	}
	return mergeJobTemplates(cluster.Spec.Backup.JobTemplate)
}

// bootstrapContainers returns the Job's extra init containers and its main
// container for mode. They run the same instance commands the Pod's init
// containers ran before design 031.
func (r *ClusterReconciler) bootstrapContainers(
	cluster *mysqlv1alpha1.Cluster, plan clusterPlan, inst instancePlan, mode bootstrapMode, resources corev1.ResourceRequirements,
) ([]corev1.Container, corev1.Container) {
	step := func(name string, args []string, extraEnv []corev1.EnvVar) corev1.Container {
		return corev1.Container{
			Name:            name,
			Image:           plan.Image,
			ImagePullPolicy: cluster.Spec.ImagePullPolicy,
			Command:         []string{managerBinary},
			Args:            args,
			Env:             append(initEnv(plan), extraEnv...),
			VolumeMounts:    volumeMounts(),
			Resources:       resources,
			SecurityContext: cluster.Spec.SecurityContext,
		}
	}
	switch mode {
	case bootstrapModeRestore:
		return nil, step(string(mode), restoreArgs(plan), plan.Recovery.StoreEnv)
	case bootstrapModeImport:
		initdb := step(string(bootstrapModeInitDB), r.initdbArgs(cluster, cluster.Spec.Bootstrap.InitDB), nil)
		return []corev1.Container{initdb}, step(string(mode), importArgs(plan), plan.Import.StoreEnv)
	case bootstrapModeJoin:
		return nil, step(string(mode), joinArgs(cluster, plan), nil)
	default:
		// A Group Replication secondary initialises an empty server: no
		// application schema, the data comes from a group donor.
		initdb := cluster.Spec.Bootstrap.InitDB
		if !inst.IsPrimary {
			initdb = nil
		}
		return nil, step(string(bootstrapModeInitDB), r.initdbArgs(cluster, initdb), nil)
	}
}

// bootstrapJob builds the one-shot Job that bootstraps inst's volume. It runs
// under the instance's ServiceAccount (design 030's Role lets it read the
// credential Secrets) and follows the instance's scheduling, so a volume that
// binds on first use lands where the instance Pod can run (design 031, B8).
func (r *ClusterReconciler) bootstrapJob(
	cluster *mysqlv1alpha1.Cluster, plan clusterPlan, inst instancePlan, mode bootstrapMode, pvc *corev1.PersistentVolumeClaim,
) (*batchv1.Job, error) {
	tpl := bootstrapJobTemplate(cluster)
	// Only the restore takes the template's resources, as the restore init
	// container did. initdb, join and import keep the instance's: import runs
	// a temporary mysqld whose my.cnf is sized for them (design 031, B9).
	resources := cluster.Spec.Resources
	if mode == bootstrapModeRestore && hasResourceRequirements(tpl.Resources) {
		resources = tpl.Resources
	}
	operatorImage := cmp.Or(plan.OperatorImage, plan.Image)
	extraInit, main := r.bootstrapContainers(cluster, plan, inst, mode, resources)

	podSpec := corev1.PodSpec{
		RestartPolicy:      corev1.RestartPolicyNever,
		ServiceAccountName: instanceServiceAccountName(inst),
		Volumes:            instanceVolumes(plan, inst),
		InitContainers: append([]corev1.Container{{
			Name:            "bootstrap-controller",
			Image:           operatorImage,
			ImagePullPolicy: cluster.Spec.ImagePullPolicy,
			Command:         []string{"/manager"},
			Args:            []string{managerBootstrapCmd, managerBinary},
			VolumeMounts:    volumeMounts(),
			Resources:       resources,
			SecurityContext: cluster.Spec.SecurityContext,
		}}, extraInit...),
		Containers:                []corev1.Container{main},
		NodeSelector:              cluster.Spec.Affinity.NodeSelector,
		Affinity:                  affinity(cluster),
		Tolerations:               append(slices.Clone(cluster.Spec.Affinity.Tolerations), tpl.Tolerations...),
		TopologySpreadConstraints: cluster.Spec.TopologySpreadConstraints,
		PriorityClassName:         cmp.Or(tpl.PriorityClassName, cluster.Spec.PriorityClassName),
		SchedulerName:             cluster.Spec.SchedulerName,
		SecurityContext:           podSecurityContext(cluster),
	}
	for _, pullSecret := range cluster.Spec.ImagePullSecrets {
		podSpec.ImagePullSecrets = append(podSpec.ImagePullSecrets, corev1.LocalObjectReference{Name: pullSecret.Name})
	}

	jobLabels := workerJobLabels(cluster.Name, bootstrapInstanceLabel, inst.Name)
	jobLabels[bootstrapModeLabel] = string(mode)
	podLabels := map[string]string{
		"app.kubernetes.io/name": appLabelValue,
		bootstrapInstanceLabel:   inst.Name,
		bootstrapModeLabel:       string(mode),
	}
	backoff := bootstrapJobBackoffLimit
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      bootstrapJobName(inst, mode),
			Namespace: cluster.Namespace,
			// Operator labels win over the template's.
			Labels: combineStringMaps(tpl.Labels, jobLabels),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:          &backoff,
			ActiveDeadlineSeconds: backupJobActiveDeadlineSeconds(tpl),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      combineStringMaps(tpl.Labels, podLabels),
					Annotations: combineStringMaps(tpl.Annotations, nil),
				},
				Spec: podSpec,
			},
		},
	}
	hash, err := hashObject(job.Spec)
	if err != nil {
		return nil, err
	}
	job.Annotations = combineStringMaps(tpl.Annotations, map[string]string{
		bootstrapPVCUIDAnnotation:   string(pvc.UID),
		bootstrapSpecHashAnnotation: hash,
	})
	if err := controllerutil.SetControllerReference(cluster, job, r.Scheme); err != nil {
		return nil, err
	}
	return job, nil
}
```

`initEnv(plan)` returns a fresh slice on every call, so the `append` in `step` never aliases.

- [ ] **Step 5: Run the tests** — same command, expect PASS. Then `make lint-fix` and `make test`.

- [ ] **Step 6: Commit**

```bash
git add internal/controller/cluster_bootstrap.go internal/controller/cluster_bootstrap_test.go internal/controller/cluster_pod.go
git commit -m "feat(instance): build per-instance bootstrap jobs"
```

---

### Task 3: Bootstrap instance volumes through the Job before creating the Pod

**Files:**
- Modify: `internal/controller/cluster_bootstrap.go`
- Modify: `internal/controller/cluster_topology.go` (`ensureInstance`, `gateInstance`, `instancePVCExists`)
- Modify: `internal/controller/cluster_controller.go` (RBAC marker, `SetupWithManager`)
- Test: `internal/controller/cluster_bootstrap_test.go`; fix-ups in `cluster_topology_test.go`, `cluster_controller_test.go`, `cluster_groupreplication_test.go` and any other test that expects `ensureInstance`/`reconcileInstances` to create a Pod for a fresh instance

**Interfaces:**
- Consumes: Task 1 (`pvcBootstrapped`, constants), Task 2 (`bootstrapModeFor`, `bootstrapJob`, labels/annotations).
- Produces:
  - `func (r *ClusterReconciler) ensureBootstrapped(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan, inst instancePlan) (bool, error)`
  - `func (r *ClusterReconciler) bootstrapJobs(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instance string) ([]batchv1.Job, error)` (sorted by name)
  - `func (r *ClusterReconciler) deleteBootstrapJobs(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instance string) (int, error)` — deletes every Job not already terminating, returns how many bootstrap Jobs still exist
  - `func (r *ClusterReconciler) instancePVCBootstrapped(ctx context.Context, cluster *mysqlv1alpha1.Cluster, inst instancePlan) (bool, error)` (replaces `instancePVCExists`)
  - test helper `func markVolumeBootstrapped(t *testing.T, ctx context.Context, c client.Client, cluster *mysqlv1alpha1.Cluster, inst instancePlan)`

- [ ] **Step 1: Write the failing tests** (append; add imports `apierrors`, `client`, `record "k8s.io/client-go/tools/record"`):

```go
// bootstrapFixture returns a reconciler over a fake client holding cluster and
// objs. Jobs are not registered as a status subresource, so c.Update writes
// their status too.
func bootstrapFixture(t *testing.T, cluster *mysqlv1alpha1.Cluster, objs ...client.Object) (*ClusterReconciler, client.Client) {
	t.Helper()
	scheme := testScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
		WithObjects(append([]client.Object{cluster}, objs...)...).
		Build()
	return &ClusterReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(20)}, c
}

func initializingPVC(cluster *mysqlv1alpha1.Cluster, name, uid string) *corev1.PersistentVolumeClaim {
	pvc := instancePVC(cluster, name)
	pvc.UID = types.UID(uid)
	pvc.Annotations = map[string]string{pvcStatusAnnotation: pvcStatusInitializing}
	return pvc
}

func finishJob(t *testing.T, ctx context.Context, c client.Client, cluster *mysqlv1alpha1.Cluster, name string, cond batchv1.JobConditionType) {
	t.Helper()
	job := &batchv1.Job{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, job); err != nil {
		t.Fatal(err)
	}
	job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{
		Type: cond, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded", Message: "Job was active longer than specified deadline",
	})
	if err := c.Update(ctx, job); err != nil {
		t.Fatal(err)
	}
}

func jobExists(t *testing.T, ctx context.Context, c client.Client, cluster *mysqlv1alpha1.Cluster, name string) bool {
	t.Helper()
	err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, &batchv1.Job{})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
	return err == nil
}

func TestEnsureBootstrappedCreatesJobForNewVolume(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	plan := testPlan()
	inst := plan.instanceFor(cluster, 1)
	r, c := bootstrapFixture(t, cluster, initializingPVC(cluster, inst.PVCName, "uid-1"))

	done, err := r.ensureBootstrapped(ctx, cluster, plan, inst)
	if err != nil || done {
		t.Fatalf("ensureBootstrapped = %v, %v; want false, nil", done, err)
	}
	job := &batchv1.Job{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.Name + "-initdb"}, job); err != nil {
		t.Fatalf("initdb Job not created: %v", err)
	}
	if job.Annotations[bootstrapPVCUIDAnnotation] != "uid-1" {
		t.Fatalf("job pvc-uid = %q", job.Annotations[bootstrapPVCUIDAnnotation])
	}
}

func TestEnsureBootstrappedMarksVolumeAndDeletesJobOnSuccess(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	plan := testPlan()
	inst := plan.instanceFor(cluster, 1)
	r, c := bootstrapFixture(t, cluster, initializingPVC(cluster, inst.PVCName, "uid-1"))

	if _, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil {
		t.Fatal(err)
	}
	finishJob(t, ctx, c, cluster, inst.Name+"-initdb", batchv1.JobComplete)

	done, err := r.ensureBootstrapped(ctx, cluster, plan, inst)
	if err != nil || done {
		t.Fatalf("pass after success = %v, %v; want false (the Job is still being deleted)", done, err)
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.PVCName}, pvc); err != nil {
		t.Fatal(err)
	}
	if !pvcBootstrapped(pvc) {
		t.Fatal("volume not marked bootstrapped after the Job succeeded")
	}
	if jobExists(t, ctx, c, cluster, inst.Name+"-initdb") {
		t.Fatal("succeeded Job not deleted")
	}
	if done, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil || !done {
		t.Fatalf("pass after deletion = %v, %v; want true", done, err)
	}
}

func TestEnsureBootstrappedKeepsFailedJobWithSameSpec(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	plan := testPlan()
	inst := plan.instanceFor(cluster, 1)
	r, c := bootstrapFixture(t, cluster, initializingPVC(cluster, inst.PVCName, "uid-1"))

	if _, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil {
		t.Fatal(err)
	}
	finishJob(t, ctx, c, cluster, inst.Name+"-initdb", batchv1.JobFailed)
	for range 3 {
		if done, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil || done {
			t.Fatalf("ensureBootstrapped = %v, %v; want false, nil", done, err)
		}
	}
	if !jobExists(t, ctx, c, cluster, inst.Name+"-initdb") {
		t.Fatal("failed Job with an unchanged spec was deleted")
	}
}

func TestEnsureBootstrappedReplacesFailedJobWhenSpecChanges(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	plan := testPlan()
	inst := plan.instanceFor(cluster, 1)
	r, c := bootstrapFixture(t, cluster, initializingPVC(cluster, inst.PVCName, "uid-1"))

	if _, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil {
		t.Fatal(err)
	}
	old := &batchv1.Job{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.Name + "-initdb"}, old); err != nil {
		t.Fatal(err)
	}
	finishJob(t, ctx, c, cluster, inst.Name+"-initdb", batchv1.JobFailed)

	plan.Image = "ghcr.io/cnmsql/cnmsql-instance:8.0.99"
	if _, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil {
		t.Fatal(err)
	}
	if jobExists(t, ctx, c, cluster, inst.Name+"-initdb") {
		t.Fatal("failed Job not deleted after the spec changed")
	}
	if _, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil {
		t.Fatal(err)
	}
	fresh := &batchv1.Job{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.Name + "-initdb"}, fresh); err != nil {
		t.Fatalf("replacement Job not created: %v", err)
	}
	if fresh.Annotations[bootstrapSpecHashAnnotation] == old.Annotations[bootstrapSpecHashAnnotation] {
		t.Fatal("replacement Job carries the old spec hash")
	}
}

func TestEnsureBootstrappedDeletesJobForOtherVolume(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 2
	plan := testPlan()
	plan.Instances = 2
	inst := plan.instanceFor(cluster, 2)
	r, c := bootstrapFixture(t, cluster, initializingPVC(cluster, inst.PVCName, "uid-old"))

	if _, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil {
		t.Fatal(err)
	}
	finishJob(t, ctx, c, cluster, inst.Name+"-join", batchv1.JobComplete)

	// The instance is re-initialised: a new, empty PVC with the same name.
	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.PVCName}, pvc); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, pvc); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, initializingPVC(cluster, inst.PVCName, "uid-new")); err != nil {
		t.Fatal(err)
	}

	if _, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.PVCName}, pvc); err != nil {
		t.Fatal(err)
	}
	if pvcBootstrapped(pvc) {
		t.Fatal("a Job for the previous volume marked the new volume bootstrapped")
	}
	if jobExists(t, ctx, c, cluster, inst.Name+"-join") {
		t.Fatal("Job for the previous volume not deleted")
	}
}

func TestEnsureBootstrappedWaitsForInstancePod(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	plan := testPlan()
	inst := plan.instanceFor(cluster, 1)
	r, c := bootstrapFixture(t, cluster,
		initializingPVC(cluster, inst.PVCName, "uid-1"),
		readyPod(cluster, inst.Name, rolePrimary))

	if done, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil || done {
		t.Fatalf("ensureBootstrapped = %v, %v; want false, nil", done, err)
	}
	if jobExists(t, ctx, c, cluster, inst.Name+"-initdb") {
		t.Fatal("bootstrap Job created while the instance Pod exists")
	}
}

func TestEnsureBootstrappedRemovesLeftoverJobBeforePod(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	plan := testPlan()
	inst := plan.instanceFor(cluster, 1)
	pvc := initializingPVC(cluster, inst.PVCName, "uid-1")
	pvc.Annotations[pvcStatusAnnotation] = pvcStatusReady
	leftover := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: inst.Name + "-initdb", Namespace: cluster.Namespace,
		Labels: map[string]string{clusterLabel: cluster.Name, bootstrapInstanceLabel: inst.Name},
	}}
	r, c := bootstrapFixture(t, cluster, pvc, leftover)

	if done, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil || done {
		t.Fatalf("first pass = %v, %v; want false while the leftover Job exists", done, err)
	}
	if jobExists(t, ctx, c, cluster, inst.Name+"-initdb") {
		t.Fatal("leftover Job not deleted")
	}
	if done, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil || !done {
		t.Fatalf("second pass = %v, %v; want true", done, err)
	}
}

func TestEnsureBootstrappedRefusesPrimaryOnEstablishedCluster(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	now := metav1.Now()
	cluster.Status.EstablishedAt = &now
	plan := testPlan()
	inst := plan.instanceFor(cluster, 1)
	r, c := bootstrapFixture(t, cluster, initializingPVC(cluster, inst.PVCName, "uid-1"))

	if done, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil || done {
		t.Fatalf("ensureBootstrapped = %v, %v; want false, nil", done, err)
	}
	if jobExists(t, ctx, c, cluster, inst.Name+"-initdb") {
		t.Fatal("established cluster got a Job that would re-initialise its primary")
	}
}
```

- [ ] **Step 2: Run them and see them fail**

Run: `go test ./internal/controller/ -run 'TestEnsureBootstrapped' -count=1`
Expected: build failure, `undefined: (*ClusterReconciler).ensureBootstrapped`.

- [ ] **Step 3: Implement** in `cluster_bootstrap.go` (imports: `context`, `sort`, `apierrors`, `types`, `client`, `logf "sigs.k8s.io/controller-runtime/pkg/log"`):

```go
// bootstrapJobs lists the instance's bootstrap Jobs, sorted by name.
func (r *ClusterReconciler) bootstrapJobs(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instance string) ([]batchv1.Job, error) {
	list := &batchv1.JobList{}
	if err := r.List(ctx, list, client.InNamespace(cluster.Namespace), client.MatchingLabels{
		clusterLabel:           cluster.Name,
		bootstrapInstanceLabel: instance,
	}); err != nil {
		return nil, err
	}
	sort.Slice(list.Items, func(i, j int) bool { return list.Items[i].Name < list.Items[j].Name })
	return list.Items, nil
}

// deleteBootstrapJobs deletes the instance's bootstrap Jobs and their Pods and
// returns how many still exist. Foreground propagation keeps a Job until its
// Pods are gone, so "none left" means nothing mounts the volume any more.
func (r *ClusterReconciler) deleteBootstrapJobs(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instance string) (int, error) {
	jobs, err := r.bootstrapJobs(ctx, cluster, instance)
	if err != nil {
		return 0, err
	}
	for i := range jobs {
		if err := r.deleteBootstrapJob(ctx, &jobs[i]); err != nil {
			return 0, err
		}
	}
	return len(jobs), nil
}

func (r *ClusterReconciler) deleteBootstrapJob(ctx context.Context, job *batchv1.Job) error {
	if job.DeletionTimestamp != nil {
		return nil
	}
	err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationForeground))
	return client.IgnoreNotFound(err)
}

// ensureBootstrapped drives inst's volume through its one-shot bootstrap Job
// (design 031). It returns true once the volume is marked bootstrapped and no
// bootstrap Job for the instance is left, so the caller may create the Pod.
// The Job and the Pod never run at the same time: the volume is RWO.
func (r *ClusterReconciler) ensureBootstrapped(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan, inst instancePlan) (bool, error) {
	log := logf.FromContext(ctx).WithValues("instance", inst.Name)
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.PVCName}, pvc); err != nil {
		// ensurePVC just created it; the cache has not caught up yet.
		return false, client.IgnoreNotFound(err)
	}
	if pvcBootstrapped(pvc) {
		left, err := r.deleteBootstrapJobs(ctx, cluster, inst.Name)
		return left == 0, err
	}

	podExists, err := r.instancePodExists(ctx, cluster, inst)
	if err != nil || podExists {
		if podExists {
			log.Info("Waiting for the instance Pod to go before bootstrapping its volume")
		}
		return false, err
	}
	// On an established cluster the plan no longer carries the bootstrap
	// source, so a primary Job would initialise an empty primary over a live
	// cluster. Leave the instance down for an operator to act on.
	if inst.IsPrimary && cluster.IsEstablished() {
		if r.Recorder != nil {
			r.Recorder.Eventf(cluster, corev1.EventTypeWarning, "BootstrapRefused",
				"Refusing to bootstrap the volume of primary %s on an established cluster; restore it by hand or fail over", inst.Name)
		}
		return false, nil
	}

	desired, err := r.bootstrapJob(cluster, plan, inst, r.bootstrapModeFor(cluster, plan, inst), pvc)
	if err != nil {
		return false, err
	}
	jobs, err := r.bootstrapJobs(ctx, cluster, inst.Name)
	if err != nil {
		return false, err
	}
	for i := range jobs {
		job := &jobs[i]
		switch {
		case job.DeletionTimestamp != nil:
			return false, nil
		case job.Annotations[bootstrapPVCUIDAnnotation] != string(pvc.UID):
			log.Info("Deleting bootstrap Job for a previous volume", "job", job.Name)
			return false, r.deleteBootstrapJob(ctx, job)
		case jobFinished(job, batchv1.JobComplete):
			before := pvc.DeepCopy()
			pvc.Annotations[pvcStatusAnnotation] = pvcStatusReady
			if err := r.Patch(ctx, pvc, client.MergeFrom(before)); err != nil {
				return false, err
			}
			log.Info("Bootstrapped instance volume", "job", job.Name)
			if r.Recorder != nil {
				r.Recorder.Eventf(cluster, corev1.EventTypeNormal, "InstanceBootstrapped",
					"Bootstrap Job %s bootstrapped the volume of %s", job.Name, inst.Name)
			}
			return false, r.deleteBootstrapJob(ctx, job)
		case jobFinished(job, batchv1.JobFailed):
			if job.Annotations[bootstrapSpecHashAnnotation] != desired.Annotations[bootstrapSpecHashAnnotation] {
				log.Info("Replacing failed bootstrap Job after a spec change", "job", job.Name)
				return false, r.deleteBootstrapJob(ctx, job)
			}
			return false, nil
		default:
			return false, nil
		}
	}
	log.Info("Created bootstrap Job", "job", desired.Name)
	return false, client.IgnoreAlreadyExists(r.Create(ctx, desired))
}

// instancePVCBootstrapped reports whether the instance's volume exists and
// holds a bootstrapped data directory: the member has its own copy of the
// data and needs no donor to come back up.
func (r *ClusterReconciler) instancePVCBootstrapped(ctx context.Context, cluster *mysqlv1alpha1.Cluster, inst instancePlan) (bool, error) {
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.PVCName}, pvc); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return pvcBootstrapped(pvc), nil
}
```

Wire it in `ensureInstance` (`cluster_topology.go`), right after `ensureInstanceService`:

```go
	// The Pod only comes up on a bootstrapped volume. Until then the instance's
	// bootstrap Job owns the volume (design 031).
	if bootstrapped, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil || !bootstrapped {
		return false, err
	}
```

In `gateInstance`, replace `r.instancePVCExists(ctx, cluster, inst)` with `r.instancePVCBootstrapped(ctx, cluster, inst)` and update the comment: a PVC still `initializing` is a member being provisioned, gated like a brand-new one. Delete `instancePVCExists` if nothing else calls it (`grep -rn instancePVCExists internal`).

In `cluster_controller.go`, add the marker next to the others and the watch:

```go
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
```

```go
		Owns(&corev1.Service{}).
		Owns(&batchv1.Job{}).
```

(import `batchv1 "k8s.io/api/batch/v1"`). Run `make manifests`; `config/rbac/role.yaml` should not change (the Backup controller already grants these verbs).

- [ ] **Step 4: Run the new tests** — same command, expect PASS.

- [ ] **Step 5: Fix the existing tests.** Add to `cluster_bootstrap_test.go`:

```go
// markVolumeBootstrapped creates or marks inst's PVC as bootstrapped, so a test
// about Pod handling skips the bootstrap Job.
func markVolumeBootstrapped(t *testing.T, ctx context.Context, c client.Client, cluster *mysqlv1alpha1.Cluster, inst instancePlan) {
	t.Helper()
	pvc := &corev1.PersistentVolumeClaim{}
	err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.PVCName}, pvc)
	if apierrors.IsNotFound(err) {
		pvc = instancePVC(cluster, inst.PVCName)
		pvc.Annotations = map[string]string{pvcStatusAnnotation: pvcStatusReady}
		if err := c.Create(ctx, pvc); err != nil {
			t.Fatal(err)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	before := pvc.DeepCopy()
	if pvc.Annotations == nil {
		pvc.Annotations = map[string]string{}
	}
	pvc.Annotations[pvcStatusAnnotation] = pvcStatusReady
	if err := c.Patch(ctx, pvc, client.MergeFrom(before)); err != nil {
		t.Fatal(err)
	}
}
```

Run `make test`. For each failure:
- A test that needs the instance's **Pod** (for example `TestReconcileInstancesGuardsReplicaOnUnhealthyPrimary` bootstrapping the primary with `ensureInstance`): call `markVolumeBootstrapped` for that instance first.
- A test asserting that a **new** instance was provisioned (a replica Pod appears once the gate opens): assert that its bootstrap Job `<instance>-join` (or `-initdb` under Group Replication) exists instead. That is what "provisioned" means now.
- A test asserting a new instance was **not** provisioned: also assert that no bootstrap Job exists for it.

- [ ] **Step 6: Run everything** — `make lint-fix && make test`, expect PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/controller config/rbac
git commit -m "feat(instance): bootstrap instance volumes with jobs before creating pods"
```

---

### Task 4: Drop the bootstrap and import init containers from instance Pods

**Files:**
- Modify: `internal/controller/cluster_pod.go` (`podSpec`, delete `bootstrapArgs`, `bootstrapEnv`)
- Modify: `internal/controller/cluster_import.go` (delete `importContainer`, `withoutImportContainer`, update comments)
- Modify: `internal/controller/cluster_resources.go` (`restartTriggeringPodSpec`)
- Modify: `api/v1alpha1/backup_types.go` (`BackupJobTemplate` doc comments), then `make manifests generate`
- Test: `internal/controller/cluster_pod_test.go`; rewrite in `backup_controller_test.go` (`TestRecoveryBootstrap*`), `cluster_controller_test.go` (~`:561`, `:636`, `:659`), `cluster_import_test.go` (~`:360`–`:384`), `cluster_groupreplication_test.go` (`TestBootstrapArgsGroupReplicationRecovery`)

**Interfaces:**
- Consumes: Task 2 `bootstrapJob`, `bootstrapModeFor`.
- Produces: instance Pods whose init containers are exactly `["bootstrap-controller"]`.

- [ ] **Step 1: Write the failing test** in `cluster_pod_test.go`:

```go
func TestPodSpecHasNoBootstrapContainers(t *testing.T) {
	t.Parallel()
	cluster := baseBackupCluster()
	cluster.Status.CurrentPrimary = ""
	plan := testPlan()
	plan.Instances = 2
	plan.Recovery = &recoveryPlan{
		Bucket: "bkt", ArchiveKey: "a", MetadataKey: "m",
		StoreEnv: []corev1.EnvVar{{Name: objectstore.EnvBucket, Value: "bkt"}},
	}
	plan.Import = &importPlan{Bucket: "bkt", DumpKey: "d", ManifestKey: "m"}
	r := &ClusterReconciler{}

	for ordinal := 1; ordinal <= 2; ordinal++ {
		spec := r.podSpec(cluster, plan, plan.instanceFor(cluster, ordinal))
		if got := containerNames(spec.InitContainers); !slices.Equal(got, []string{"bootstrap-controller"}) {
			t.Fatalf("instance %d init containers = %v, want only bootstrap-controller", ordinal, got)
		}
		for _, c := range append(spec.InitContainers, spec.Containers...) {
			for _, env := range c.Env {
				if env.Name == objectstore.EnvBucket {
					t.Fatalf("instance %d container %s carries the recovery object-store env", ordinal, c.Name)
				}
			}
		}
	}
}
```

(Continuous archiving is off in `baseBackupCluster`, so no container may carry `objectstore.EnvBucket`. If it turns out to be on, set `cluster.Spec.Backup.ContinuousArchiving = nil` in the test.)

- [ ] **Step 2: Run it** — `go test ./internal/controller/ -run TestPodSpecHasNoBootstrapContainers -count=1`. Expected: FAIL, init containers include `bootstrap` (and `import`).

- [ ] **Step 3: Implement.**
  - In `podSpec`, remove the `bootstrap` init container from `InitContainers`, the recovery-resources block ("During recovery the \"bootstrap\" init container restores…"), and the `importContainer` append.
  - Delete `bootstrapArgs` and `bootstrapEnv`. Keep `restoreArgs`, `initdbArgs`, `joinArgs`, `initEnv` (the Job uses them); fix their doc comments to say "the bootstrap Job's command" instead of "the init container's command".
  - In `cluster_import.go`, delete `importContainerName`, `importContainer` and `withoutImportContainer`. Update the `resolveImport` doc comment: the import runs in the primary's bootstrap Job; the plan stops resolving it once the primary is bootstrapped (Task 5).
  - In `restartTriggeringPodSpec`, remove both `withoutImportContainer` calls. Keep the args and bootstrap-controller image normalization.
  - In `api/v1alpha1/backup_types.go`, update the `BackupJobTemplate` comments:
    - type comment: "…applied to the backup, restore and instance bootstrap worker Jobs…"
    - `Resources`: replace "During recovery the same requests/limits from the cluster-level template are applied to the restore init container." with "During recovery the cluster-level template's requests/limits also apply to the restore Job. The other instance bootstrap Jobs (initdb, join, import) always use `spec.resources`."
    - `NodeSelector` and `Affinity`: add "Not applied to instance bootstrap Jobs: they follow the instance's scheduling so the data volume binds where the instance can run."
    - `Tolerations`: add "Added to the instance's tolerations on instance bootstrap Jobs."
    - `PriorityClassName`: add "Also applies to instance bootstrap Jobs, over `spec.priorityClassName`."
  - Run `make manifests generate`.

- [ ] **Step 4: Move the old init-container tests to the Job.** Each test that read `spec.InitContainers[1]` (the `bootstrap` container) builds the Job instead and reads its main container:

```go
	inst := plan.instanceFor(cluster, 1)
	job, err := reconciler.bootstrapJob(cluster, plan, inst, reconciler.bootstrapModeFor(cluster, plan, inst), instancePVC(cluster, inst.PVCName))
	if err != nil {
		t.Fatal(err)
	}
	main := job.Spec.Template.Spec.Containers[0]
	initArgs := strings.Join(main.Args, " ")
```

and keeps its assertions on `initArgs` / `main.Env` / `main.Resources`. Specifically:
  - `backup_controller_test.go`: `TestRecoveryBootstrapRestoresPrimaryFromObjectStore`, `TestRecoveryBootstrapAppliesJobTemplateResources` (now asserts the Job's container), `TestRecoveryBootstrapPITRTargetReplaysBinlogs`, and the replica `instance join` check (use instance 2: `joinArgs`).
  - `cluster_controller_test.go` `:561` (`instance initdb`), `:636` and `:659` (containers carrying env: loop over the Job's main container and the Pod's `Containers[0]`).
  - `cluster_import_test.go` `:360`–`:384`: the primary Pod's init containers are `["bootstrap-controller"]`; the import now lives in the Job: assert `containerNames(job.Spec.Template.Spec.InitContainers) == ["bootstrap-controller", "initdb"]` and main container `import` for the primary, and mode `join` for the replica.
  - `cluster_groupreplication_test.go` `TestBootstrapArgsGroupReplicationRecovery`: rename to `TestBootstrapJobGroupReplicationRecovery`, give its reconciler a scheme (`&ClusterReconciler{Scheme: testScheme(t)}`, since `bootstrapJob` sets the owner reference), build each Job with `bootstrapModeFor`, and assert the same strings on `Containers[0].Args`.
  - Delete any test that only covered `withoutImportContainer` or the recovery-resources special case on the Pod.

- [ ] **Step 5: Run everything** — `make lint-fix && make test`, expect PASS. `grep -rn '"bootstrap"' internal/controller` should only hit `managerBootstrapCmd`.

- [ ] **Step 6: Commit**

```bash
git add api internal/controller config
git commit -m "feat(instance)!: drop the bootstrap init containers from instance pods"
```

---

### Task 5: Stop resolving the bootstrap source once the primary is bootstrapped

**Files:**
- Modify: `internal/controller/cluster_plan.go` (`buildPlan`, `resolveRecovery` doc comment)
- Modify: `internal/controller/cluster_bootstrap.go` (`primaryBootstrapped`)
- Test: `internal/controller/cluster_bootstrap_test.go`

**Interfaces:**
- Consumes: Task 1 `pvcBootstrapped`.
- Produces: `func (r *ClusterReconciler) primaryBootstrapped(ctx context.Context, cluster *mysqlv1alpha1.Cluster) (bool, error)`. `buildPlan` leaves `plan.Recovery` and `plan.Import` nil when it returns true.

- [ ] **Step 1: Write the failing tests:**

```go
func recoveryCluster() *mysqlv1alpha1.Cluster {
	cluster := baseBackupCluster()
	cluster.Status.CurrentPrimary = ""
	cluster.Spec.Bootstrap = &mysqlv1alpha1.BootstrapConfiguration{
		Recovery: &mysqlv1alpha1.BootstrapRecovery{
			Backup: &mysqlv1alpha1.LocalObjectReference{Name: "backup-sample"},
		},
	}
	return cluster
}

func TestBuildPlanSkipsRecoveryOnceBootstrapped(t *testing.T) {
	t.Parallel()
	cluster := recoveryCluster()
	pvc := instancePVC(cluster, instanceName(cluster, 1))
	pvc.Annotations = map[string]string{pvcStatusAnnotation: pvcStatusReady}
	// No Backup object: it was deleted after the restore.
	r, _ := bootstrapFixture(t, cluster, pvc)

	plan, err := r.buildPlan(context.Background(), cluster)
	if err != nil {
		t.Fatalf("buildPlan failed although the primary is bootstrapped: %v", err)
	}
	if plan.Recovery != nil {
		t.Fatal("plan.Recovery resolved after the primary was bootstrapped")
	}
}

func TestBuildPlanSkipsRecoveryForLegacyVolume(t *testing.T) {
	t.Parallel()
	cluster := recoveryCluster()
	r, _ := bootstrapFixture(t, cluster, instancePVC(cluster, instanceName(cluster, 1)))

	if _, err := r.buildPlan(context.Background(), cluster); err != nil {
		t.Fatalf("buildPlan failed on a pre-031 volume whose Backup is gone: %v", err)
	}
}

func TestBuildPlanSkipsRecoveryOnEstablishedCluster(t *testing.T) {
	t.Parallel()
	cluster := recoveryCluster()
	now := metav1.Now()
	cluster.Status.EstablishedAt = &now
	r, _ := bootstrapFixture(t, cluster)

	if _, err := r.buildPlan(context.Background(), cluster); err != nil {
		t.Fatalf("buildPlan failed on an established cluster whose Backup is gone: %v", err)
	}
}

func TestBuildPlanResolvesRecoveryWhileInitializing(t *testing.T) {
	t.Parallel()
	cluster := recoveryCluster()
	backup := baseBackup()
	backup.Status = mysqlv1alpha1.BackupStatus{Phase: mysqlv1alpha1.BackupPhaseCompleted, BackupID: testBackupID}
	r, _ := bootstrapFixture(t, cluster, backup, initializingPVC(cluster, instanceName(cluster, 1), "uid-1"))

	plan, err := r.buildPlan(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Recovery == nil {
		t.Fatal("plan.Recovery not resolved while the primary volume is initializing")
	}
}
```

- [ ] **Step 2: Run them** — `go test ./internal/controller/ -run 'TestBuildPlan(Skips|Resolves)' -count=1`. Expected: the three `Skips` tests FAIL with `resolving recovery backup "backup-sample"`; `Resolves` passes.

- [ ] **Step 3: Implement** in `cluster_bootstrap.go`:

```go
// primaryBootstrapped reports whether the bootstrap primary's data exists, so
// the recovery or import source is never needed again: the cluster is
// established, or the current (else first) instance's volume is bootstrapped.
// A volume from before design 031 counts as bootstrapped.
func (r *ClusterReconciler) primaryBootstrapped(ctx context.Context, cluster *mysqlv1alpha1.Cluster) (bool, error) {
	if cluster.IsEstablished() {
		return true, nil
	}
	name := cmp.Or(cluster.Status.CurrentPrimary, instanceName(cluster, 1))
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, pvc); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return pvcBootstrapped(pvc), nil
}
```

(The PVC is named after the instance: `instanceFor` sets `PVCName: name`.)

In `buildPlan`, replace the recovery and import resolution with:

```go
	// The bootstrap source is only read until the primary's data exists: after
	// that the source Backup, its object store and its Secrets may go away.
	bootstrapped, err := r.primaryBootstrapped(ctx, cluster)
	if err != nil {
		return clusterPlan{}, err
	}
	if !bootstrapped {
		recovery, err := r.resolveRecovery(ctx, cluster)
		if err != nil {
			return clusterPlan{}, err
		}
		plan.Recovery = recovery

		imp, err := r.resolveImport(ctx, cluster, serverVersion)
		if err != nil {
			return clusterPlan{}, err
		}
		plan.Import = imp
	}
	return plan, nil
```

Update the `resolveRecovery` doc comment: the referenced Backup must stay present and completed only until the bootstrap primary's volume is bootstrapped.

- [ ] **Step 4: Run** the four tests (PASS), then `make lint-fix && make test`. Existing recovery tests that call `buildPlan` with no PVC keep resolving (no PVC → not bootstrapped).

- [ ] **Step 5: Commit**

```bash
git add internal/controller
git commit -m "fix(recovery): stop resolving the bootstrap source once the primary is bootstrapped"
```

---

### Task 6: Tear down bootstrap Jobs on re-initialisation and scale-down

**Files:**
- Modify: `internal/controller/cluster_reinit.go` (`reconcileReinit`)
- Modify: `internal/controller/cluster_topology.go` (`removeInstanceResources`)
- Test: `internal/controller/cluster_reinit_test.go`, `internal/controller/cluster_topology_test.go`

**Interfaces:**
- Consumes: Task 3 `deleteBootstrapJobs`.

- [ ] **Step 1: Write the failing tests.** In `cluster_reinit_test.go` (add `batchv1` import):

```go
func TestReconcileReinitDeletesBootstrapJobs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 2
	cluster.Annotations = map[string]string{reinitAnnotation: testReplica2}
	scheme := testScheme(t)
	// A join is still running: it holds the PVC, so the PVC cannot go until it does.
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: testReplica2 + "-join", Namespace: cluster.Namespace,
		Labels: map[string]string{clusterLabel: cluster.Name, bootstrapInstanceLabel: testReplica2},
	}}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
		WithObjects(cluster, instancePVC(cluster, testReplica2), job).
		Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	plan := testPlan()
	plan.Instances = 2
	inst := plan.instanceFor(cluster, 2)

	handled, err := r.reconcileReinit(ctx, cluster, inst)
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		t.Fatal("teardown reported complete while a bootstrap Job existed")
	}
	if !instanceMissing(t, c, cluster, testReplica2+"-join", &batchv1.Job{}) {
		t.Fatal("bootstrap Job not deleted during re-init teardown")
	}
	handled, err = r.reconcileReinit(ctx, cluster, inst)
	if err != nil || handled {
		t.Fatalf("second pass = %v, %v; want teardown complete", handled, err)
	}
}
```

In `cluster_topology_test.go`:

```go
func TestRemoveInstanceResourcesDeletesBootstrapJob(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 2
	scheme := testScheme(t)
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: testReplica2 + "-join", Namespace: cluster.Namespace,
		Labels: map[string]string{clusterLabel: cluster.Name, bootstrapInstanceLabel: testReplica2},
	}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, job).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	plan := testPlan()
	plan.Instances = 2

	if err := r.removeInstanceResources(ctx, cluster, plan.instanceFor(cluster, 2)); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: job.Name}, &batchv1.Job{}); !apierrors.IsNotFound(err) {
		t.Fatalf("bootstrap Job get = %v, want deleted on scale-down", err)
	}
}
```

- [ ] **Step 2: Run them** — `go test ./internal/controller/ -run 'TestReconcileReinitDeletesBootstrapJobs|TestRemoveInstanceResourcesDeletesBootstrapJob' -count=1`. Expected: FAIL (Job still present).

- [ ] **Step 3: Implement.** In `reconcileReinit`, after the Pod block and before the PVC block:

```go
	// A bootstrap Job (a join still cloning) holds the PVC too.
	jobsLeft, err := r.deleteBootstrapJobs(ctx, cluster, inst.Name)
	if err != nil {
		return false, err
	}
```

and change the completion check to `if !podGone || !pvcGone || jobsLeft > 0 {`. Update the function's doc comment: the normal reconcile then recreates the PVC empty and a `join` Job re-clones it before the Pod comes back.

In `removeInstanceResources`, before the loop:

```go
	if _, err := r.deleteBootstrapJobs(ctx, cluster, inst.Name); err != nil {
		return err
	}
```

and update its doc comment ("deletes the owned Pod, bootstrap Jobs, ConfigMap and Service").

- [ ] **Step 4: Run** the two tests (PASS), then `make lint-fix && make test`.

- [ ] **Step 5: Commit**

```bash
git add internal/controller
git commit -m "feat(instance): tear down bootstrap jobs on reinit and scale down"
```

---

### Task 7: Surface bootstrap Jobs in the Cluster status

**Files:**
- Modify: `api/v1alpha1/common_types.go` (condition constant)
- Modify: `internal/controller/cluster_bootstrap.go` (`bootstrapJobState`, `observeBootstrapJobs`, `bootstrapFailureReason`)
- Modify: `internal/controller/cluster_status.go` (`observedCluster`, `observe`, `computeClusterPhase`, `patchStatus`)
- Test: `internal/controller/cluster_bootstrap_test.go`, `internal/controller/cluster_status_test.go`

**Interfaces:**
- Consumes: Task 2 labels, Task 3 `bootstrapFixture`/`finishJob`.
- Produces:
  - `mysqlv1alpha1.ConditionBootstrapFailed = "BootstrapFailed"`
  - `type bootstrapJobState struct { Instance, Job string; Mode bootstrapMode; Failed bool; Reason, Message string }`
  - `observedCluster.BootstrapJobs []bootstrapJobState`
  - `func (r *ClusterReconciler) observeBootstrapJobs(ctx context.Context, cluster *mysqlv1alpha1.Cluster) ([]bootstrapJobState, error)`
  - `func failedBootstrapJobs(jobs []bootstrapJobState) []bootstrapJobState`
  - `func bootstrapFailureReason(failed []bootstrapJobState) string`

- [ ] **Step 1: Write the failing tests.** In `cluster_bootstrap_test.go`:

```go
func TestObserveBootstrapJobs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 2
	plan := testPlan()
	plan.Instances = 2
	primary, replica := plan.instanceFor(cluster, 1), plan.instanceFor(cluster, 2)
	r, c := bootstrapFixture(t, cluster,
		initializingPVC(cluster, primary.PVCName, "uid-1"),
		initializingPVC(cluster, replica.PVCName, "uid-2"))
	for _, inst := range []instancePlan{primary, replica} {
		if _, err := r.ensureBootstrapped(ctx, cluster, plan, inst); err != nil {
			t.Fatal(err)
		}
	}
	finishJob(t, ctx, c, cluster, primary.Name+"-initdb", batchv1.JobFailed)
	finishJob(t, ctx, c, cluster, replica.Name+"-join", batchv1.JobComplete)

	states, err := r.observeBootstrapJobs(ctx, cluster)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 1 {
		t.Fatalf("states = %+v, want only the failed initdb (completed Jobs are skipped)", states)
	}
	got := states[0]
	if got.Instance != primary.Name || got.Mode != bootstrapModeInitDB || !got.Failed || got.Reason != "DeadlineExceeded" {
		t.Fatalf("state = %+v", got)
	}
}
```

In `cluster_status_test.go` (reuse the file's existing way of building an `observedCluster` for `computeClusterPhase`; the literal below shows the fields that matter):

```go
func TestComputeClusterPhaseReportsBootstrapJobs(t *testing.T) {
	t.Parallel()
	failed := bootstrapJobState{Instance: "demo-1", Job: "demo-1-restore", Mode: bootstrapModeRestore,
		Failed: true, Reason: "DeadlineExceeded", Message: "Bootstrap Job failed: DeadlineExceeded"}
	running := bootstrapJobState{Instance: "demo-1", Job: "demo-1-restore", Mode: bootstrapModeRestore}
	now := metav1.Now()

	for _, tc := range []struct {
		name        string
		jobs        []bootstrapJobState
		established bool
		wantPhase   string
		wantReason  string
	}{
		{"failed before established", []bootstrapJobState{failed}, false, topology.PhaseBlocked, "demo-1-restore"},
		{"failed after established", []bootstrapJobState{failed}, true, topology.PhaseDegraded, "DeadlineExceeded"},
		{"running", []bootstrapJobState{running}, false, topology.PhasePending, "Waiting for bootstrap Job demo-1-restore"},
	} {
		cluster := baseCluster()
		if tc.established {
			cluster.Status.EstablishedAt = &now
		}
		o := observedCluster{BootstrapJobs: tc.jobs}
		o.computeClusterPhase(cluster, testPlan())
		if o.Phase != tc.wantPhase || !strings.Contains(o.PhaseReason, tc.wantReason) {
			t.Errorf("%s: phase %q reason %q, want %q containing %q", tc.name, o.Phase, o.PhaseReason, tc.wantPhase, tc.wantReason)
		}
	}
}

func TestPatchStatusSetsBootstrapFailedCondition(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	recorder := record.NewFakeRecorder(10)
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&mysqlv1alpha1.Cluster{}).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme, Recorder: recorder}
	observed := observedCluster{
		Plan:          testPlan(),
		InstanceNames: []string{"demo-1"},
		Phase:         topology.PhaseBlocked,
		BootstrapJobs: []bootstrapJobState{{Instance: "demo-1", Job: "demo-1-restore", Failed: true,
			Reason: "DeadlineExceeded", Message: "Bootstrap Job failed: DeadlineExceeded"}},
	}

	if err := r.patchStatus(ctx, cluster, observed); err != nil {
		t.Fatal(err)
	}
	got := &mysqlv1alpha1.Cluster{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, got); err != nil {
		t.Fatal(err)
	}
	cond := apimeta.FindStatusCondition(got.Status.Conditions, mysqlv1alpha1.ConditionBootstrapFailed)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "DeadlineExceeded" {
		t.Fatalf("BootstrapFailed condition = %+v", cond)
	}
	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, "BootstrapJobFailed") {
			t.Fatalf("event = %q, want BootstrapJobFailed", event)
		}
	default:
		t.Fatal("no Event on the BootstrapFailed transition")
	}
}
```

(Adjust the Ready-phase event draining if `patchStatus` also emits a phase-transition event first: read events until one contains `BootstrapJobFailed` or the channel is empty.)

- [ ] **Step 2: Run them** — `go test ./internal/controller/ -run 'TestObserveBootstrapJobs|TestComputeClusterPhaseReportsBootstrapJobs|TestPatchStatusSetsBootstrapFailedCondition' -count=1`. Expected: build failure.

- [ ] **Step 3: Implement.**

In `api/v1alpha1/common_types.go`, next to `ConditionDumpAccountReady`:

```go
	// ConditionBootstrapFailed is True while an instance's bootstrap Job
	// (initdb, restore, join or import) has failed and was not replaced. Its
	// reason is the Job's (for example BackoffLimitExceeded or
	// DeadlineExceeded). Delete the Job, or change the spec it was built from,
	// to retry.
	ConditionBootstrapFailed = "BootstrapFailed"
```

In `cluster_bootstrap.go`:

```go
// bootstrapJobState is the observed state of a bootstrap Job that has not
// completed: running, or failed.
type bootstrapJobState struct {
	Instance string
	Job      string
	Mode     bootstrapMode
	Failed   bool
	Reason   string
	Message  string
}

// observeBootstrapJobs lists the cluster's running and failed bootstrap Jobs,
// sorted by instance. Completed and terminating Jobs are about to go and are
// skipped.
func (r *ClusterReconciler) observeBootstrapJobs(ctx context.Context, cluster *mysqlv1alpha1.Cluster) ([]bootstrapJobState, error) {
	list := &batchv1.JobList{}
	if err := r.List(ctx, list, client.InNamespace(cluster.Namespace),
		client.MatchingLabels{clusterLabel: cluster.Name}, client.HasLabels{bootstrapInstanceLabel}); err != nil {
		return nil, err
	}
	var states []bootstrapJobState
	for i := range list.Items {
		job := &list.Items[i]
		if job.DeletionTimestamp != nil || jobFinished(job, batchv1.JobComplete) {
			continue
		}
		state := bootstrapJobState{
			Instance: job.Labels[bootstrapInstanceLabel],
			Job:      job.Name,
			Mode:     bootstrapMode(job.Labels[bootstrapModeLabel]),
		}
		if jobFinished(job, batchv1.JobFailed) {
			state.Failed = true
			state.Reason, state.Message = workerJobFailure(job, "Bootstrap")
		}
		states = append(states, state)
	}
	sort.Slice(states, func(i, j int) bool { return states[i].Instance < states[j].Instance })
	return states, nil
}

func failedBootstrapJobs(jobs []bootstrapJobState) []bootstrapJobState {
	var failed []bootstrapJobState
	for _, j := range jobs {
		if j.Failed {
			failed = append(failed, j)
		}
	}
	return failed
}

// bootstrapFailureReason names every failed Job, its instance and why.
func bootstrapFailureReason(failed []bootstrapJobState) string {
	parts := make([]string, 0, len(failed))
	for _, j := range failed {
		parts = append(parts, fmt.Sprintf("bootstrap Job %s for %s failed (%s): %s", j.Job, j.Instance, j.Reason, j.Message))
	}
	return strings.Join(parts, "; ")
}
```

In `cluster_status.go`:
- Add to `observedCluster`:

```go
	// BootstrapJobs are the cluster's running and failed instance bootstrap
	// Jobs (design 031).
	BootstrapJobs []bootstrapJobState
```

- In `observe`, just before `observed.computeClusterPhase(cluster, plan)`:

```go
	bootstrapJobs, err := r.observeBootstrapJobs(ctx, cluster)
	if err != nil {
		return observedCluster{}, err
	}
	observed.BootstrapJobs = bootstrapJobs
```

(match the function's existing error-return shape).

- In `computeClusterPhase`, inside the `switch`, right after the `DivergedInstances` case:

```go
		case len(failedBootstrapJobs(o.BootstrapJobs)) > 0:
			// Before the cluster is established a failed bootstrap needs a spec
			// change or a retry by hand; after it, only a replica's join failed.
			o.Phase = topology.PhaseBlocked
			if cluster.IsEstablished() {
				o.Phase = topology.PhaseDegraded
			}
			o.PhaseReason = bootstrapFailureReason(failedBootstrapJobs(o.BootstrapJobs))
```

and replace the `case o.ReadyInstances == 0 && plan.Import != nil:` case with:

```go
		case o.ReadyInstances == 0 && len(o.BootstrapJobs) > 0:
			j := o.BootstrapJobs[0]
			o.Phase = topology.PhasePending
			o.PhaseReason = fmt.Sprintf("Waiting for bootstrap Job %s (%s) of %s", j.Job, j.Mode, j.Instance)
```

Update any `cluster_status_test.go` assertion on the old import reason ("Waiting for the primary instance to initialise and import the logical backup").

- In `patchStatus`, next to `wasStoragePressured`:

```go
	wasBootstrapFailed := apimeta.IsStatusConditionTrue(before.Status.Conditions, mysqlv1alpha1.ConditionBootstrapFailed)
	if len(observed.InstanceNames) > 0 {
		r.applyBootstrapFailedCondition(latest, observed)
	}
```

with

```go
// applyBootstrapFailedCondition reports failed bootstrap Jobs. It only runs on
// a full observation: the early status patches (plan failed, waiting for
// certificates) carry no Job view and must not clear it.
func (r *ClusterReconciler) applyBootstrapFailedCondition(latest *mysqlv1alpha1.Cluster, observed observedCluster) {
	cond := metav1.Condition{
		Type:               mysqlv1alpha1.ConditionBootstrapFailed,
		Status:             metav1.ConditionFalse,
		Reason:             "NoFailedBootstrapJobs",
		Message:            "No instance bootstrap Job has failed",
		ObservedGeneration: latest.Generation,
	}
	if failed := failedBootstrapJobs(observed.BootstrapJobs); len(failed) > 0 {
		cond.Status = metav1.ConditionTrue
		cond.Reason = failed[0].Reason
		cond.Message = bootstrapFailureReason(failed)
	}
	apimeta.SetStatusCondition(&latest.Status.Conditions, cond)
}
```

and, after the status patch succeeds (next to `recordFailoverEvent`):

```go
	if !wasBootstrapFailed && r.Recorder != nil &&
		apimeta.IsStatusConditionTrue(latest.Status.Conditions, mysqlv1alpha1.ConditionBootstrapFailed) {
		r.Recorder.Event(latest, corev1.EventTypeWarning, "BootstrapJobFailed",
			apimeta.FindStatusCondition(latest.Status.Conditions, mysqlv1alpha1.ConditionBootstrapFailed).Message)
	}
```

- [ ] **Step 4: Run** the three tests (PASS), then `make manifests generate` (the constant needs no CRD change, but run it), `make lint-fix && make test`.

- [ ] **Step 5: Commit**

```bash
git add api internal/controller
git commit -m "feat(status): report failed instance bootstrap jobs"
```

---

### Task 8: E2E coverage

**Files:**
- Create: `test/e2e/bootstrap_jobs_test.go`

**Interfaces:**
- Consumes (existing helpers in `test/e2e`): `createTestNamespace`, `deleteTestNamespace`, `testNamespace`, `setupObjectStore`, `teardownObjectStore`, `applyManifest`, `deleteManifest`, `archivingClusterManifest`, `backupManifest`, `recoveryClusterManifest`, `expectClusterReady`, `expectBackupCompleted`, `kubectl`, `clusterField`, `e2eTimeout`, `instanceImage`, `e2eInstanceResources`, `e2eMySQLParameters`, `objectStoreYAML`.

- [ ] **Step 1: Write the spec.** One `Ordered` `Describe`, `Label("feature")`, with its own namespace and object store, following `backup_test.go`'s `BeforeAll`/`AfterAll` shape:

  1. **"bootstraps the source cluster's volumes with Jobs"** — create a 2-instance cluster from `archivingClusterManifest` with `instances: 2` (write `bootstrapSourceManifest(name)` as a copy with `instances: 2`). While it provisions, `Eventually` see Job `<name>-1-initdb`, then `<name>-2-join`. After `expectClusterReady(name, 2, …)`:
     - `kubectl get jobs -l mysql.cnmsql.co/bootstrap-instance -n <ns>` returns nothing (succeeded Jobs are deleted);
     - `kubectl get pvc <name>-1 -o jsonpath={.metadata.annotations.mysql\.cnmsql\.co/pvc-status}` = `ready` for both instances;
     - `kubectl get pod <name>-1 -o jsonpath={.spec.initContainers[*].name}` = `bootstrap-controller`.
  2. **"recovers from a backup and survives its deletion"** — take a Backup (`backupManifest`, `expectBackupCompleted`), create a recovered cluster (`recoveryClusterManifest`), `Eventually` see Job `<restored>-1-restore`, then `expectClusterReady(restored, 1, …)`. Assert no container of the restored Pod references the object-store credentials Secret: `kubectl get pod <restored>-1 -o json` does not contain the Secret name used by `objectStoreYAML`. Then `kubectl delete backup <backup>` and `kubectl delete pod <restored>-1`; `expectClusterReady(restored, 1, …)` again and assert `{.status.phase}` is `Ready`, not `Blocked`.
  3. **"reports a failed restore and retries after a spec change"** — create a recovery cluster with `recoveryClusterWithDeadlineManifest(name, backup, "5s")`:

```go
func recoveryClusterWithDeadlineManifest(name, backup, deadline string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %s
  namespace: %s
spec:
  instances: 1
  imageName: %s
  storage:
    size: 2Gi
%s
  mysql:
    binlogFormat: ROW
%s
  bootstrap:
    recovery:
      backup:
        name: %s
  backup:
%s
    jobTemplate:
      activeDeadline: %s
`, name, testNamespace, instanceImage, e2eInstanceResources, e2eMySQLParameters, backup, objectStoreYAML("    "), deadline)
}
```

     (create the Backup for this case again if step 2 deleted it). `Eventually` (3 min): `{.status.phase}` = `Blocked`, `{.status.conditions[?(@.type=="BootstrapFailed")].status}` = `True`, its `.reason` = `DeadlineExceeded`, and `kubectl get events --field-selector reason=BootstrapJobFailed` is not empty. Then `applyManifest` the same cluster with `"1h"`: `Eventually` the Job's `mysql.cnmsql.co/bootstrap-spec-hash` changes, then `expectClusterReady(name, 1, …)` and `BootstrapFailed` is `False`.
  4. **"re-clones a replica through a join Job"** — on the source cluster, `kubectl annotate cluster <name> <reinit annotation>=<name>-2 --overwrite` (use the annotation key the reinit e2e already uses; `grep -rn reinit test/e2e`). `Eventually` see Job `<name>-2-join` again, then `expectClusterReady(name, 2, …)`.

- [ ] **Step 2: Run it on a dedicated Kind cluster** (never a real cluster): `make test-e2e` with the suite's focus variable set to `Bootstrap Jobs` (see `hack/`/`Makefile` for the exact variable the other specs use, e.g. `GINKGO_FOCUS`). Expected: PASS. Then run the existing specs most likely to regress: physical backup and recovery, PITR, logical backups (`bootstrap.initdb.import`), Group Replication lifecycle, corruption recovery (auto-reinit), and the in-place operator upgrade.

- [ ] **Step 3: Commit**

```bash
git add test/e2e/bootstrap_jobs_test.go
git commit -m "test(e2e): cover instance bootstrap jobs"
```

---

### Task 9: Docs, decision record, design index

**Files:**
- Modify: `docs/src/cluster-lifecycle.md` (how an instance comes up: PVC → bootstrap Job → Pod; the `pvc-status` annotation; `BootstrapFailed`; retry by deleting the Job or changing the spec)
- Modify: `docs/src/backup-recovery.md`, `docs/src/pitr.md`, `docs/src/pitr-internals.md` (restore runs in `<instance>-restore`; sized and prioritised by `spec.backup.jobTemplate`; the source Backup can be deleted once the cluster has recovered)
- Modify: `docs/src/logical-backups.md` (import runs in `<instance>-import`, after `initdb` in the same Job)
- Modify: `docs/src/troubleshooting.md` (replace `Init:CrashLoopBackOff` guidance with: `kubectl get jobs -l mysql.cnmsql.co/bootstrap-instance`, `kubectl logs job/<instance>-<mode>`, the `BootstrapFailed` condition, `BootstrapRefused` events)
- Modify: `docs/src/security-model.md` and `docs/src/object-store.md` (recovery object-store credentials only in the restore Job, never in instance Pods)
- Modify: `docs/src/operator-upgrades.md` (extend the 0.8.0 "Instances roll once" note: the same roll also removes the `bootstrap` init container; upgrade when no instance is initialising; recovered clusters no longer need their source Backup; `jobTemplate.priorityClassName`/`tolerations` now also reach bootstrap Jobs; alerts on `Init:CrashLoopBackOff` move to the `BootstrapFailed` condition. Add a downgrade warning: never downgrade below 0.8.0 while `kubectl get jobs -A -l mysql.cnmsql.co/bootstrap-instance` shows an active Job, because the old operator would start the instance Pod on the volume the Job is still writing)
- Modify: `docs/src/api-reference.md` (the `BackupJobTemplate` field descriptions from Task 4; the `BootstrapFailed` condition)
- Modify: `INSTRUCTION.md` (§Key Decisions: add D18; amend D10 "Both the `bootstrap` and `mysql` containers execute…" to "The bootstrap Jobs and the `mysql` container execute…"; same fix in §Operator Upgrade Architecture "How the binary flows")
- Modify: `design/INDEX.md` (status `accepted` when implementation starts, `done` when merged)

- [ ] **Step 1: Write the docs.** Use the facts in §3–§5; describe current behaviour only (no "previously"). D18 row:

```markdown
| D18 | Instance data directories are bootstrapped (initdb / restore / join / import) by a one-shot **Job per instance volume** that runs before the instance Pod exists; a `mysql.cnmsql.co/pvc-status` PVC annotation records the result, and the bootstrap source is never read again once the primary's volume is bootstrapped | The bootstrap config and its object-store credentials stop being runtime dependencies of instance Pods; restores get their own resources, deadline, status and logs. See `design/031-bootstrap-jobs.md` |
```

- [ ] **Step 2: Build the docs** — `cd docs && npm run build`. Expected: success, no broken links.

- [ ] **Step 3: Commit**

```bash
git add docs INSTRUCTION.md design/INDEX.md design/031-bootstrap-jobs.md
git commit -m "docs: instance bootstrap runs as jobs"
```

- [ ] **Step 4: Comment on #127** with the B8 narrowing (template `nodeSelector`/`affinity` not applied to bootstrap Jobs, and why), once the PR is open.
