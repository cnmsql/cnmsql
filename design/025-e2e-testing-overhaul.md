# 025 — E2E Testing Overhaul

**Status:** proposed
**Milestone:** M-E2E

Make the end-to-end suite rock solid: kill cross-spec contention, eliminate the
flake classes that come from rolling the one shared operator under load, replace
blanket 20-minute waits with deterministic readiness, give it a single local
command (`./hack/e2e.sh`) with `--focus / --k8s / --mysql` arguments, and run it
reliably in CI on a **single self-hosted runner (8 vCPU / 16 GiB)**.

**Goal:** A contributor types one command and the whole suite runs locally,
hermetically, and finishes with a clear red/green. CI runs the same suite on the
single runner with no wasted re-runs, no false reds from resource pressure, and a
must-gather bundle on every failure. A genuinely broken change fails in minutes
with actionable diagnostics; a genuinely healthy change is green every time.

## Progress

- **Phase 1 — taxonomy + single source of truth: DONE.** Tier `Label`s on every
  Describe (`core` / `feature` / `flavor` / `heavy` / `disruptive`, with
  `node-failure` and `major-upgrade` kept as sub-selectors under `disruptive`).
  Image/version refs and resource shapes consolidated into `test/e2e/images.go`
  and `test/e2e/resources.go`.
- **Phase 2 — one-command runner: DONE.** `hack/e2e.sh` with
  `--focus/--label/--tier/--k8s/--mysql/--procs/--junit/--keep/--fresh/--dry-run`
  and tier-aware parallelism caps; `make e2e-build-images`; `make test-e2e`
  delegates to the script. The in-suite manager build is gated by
  `E2E_SKIP_IMAGE_BUILD` and `managerImage` is `E2E_MANAGER_IMAGE`-overridable so
  the image is built once, outside Ginkgo.
- **Phase 3 — operator/cluster isolation: CORE DONE.** Ephemeral-cluster harness
  in `test/e2e/dedicated.go` (`provisionDedicated` / `loadImage` / `teardown`),
  generalized from the node-failure pattern. All five disruptive Describes
  (Operator Upgrade ×2, In-place operator upgrade ×2, Namespaced deployment mode)
  plus node-failure now run on their own ephemeral Kind cluster, so **no spec
  rolls the shared suite operator**.
- **Phase 4 — fail-fast readiness: CORE DONE.** `expectClusterReady` now bails
  immediately (dumping diagnostics) on a terminal state — an operator-declared
  `Blocked` phase or an image-pull error — via `clusterTerminalState`, instead of
  burning the whole timeout. Timeout *ceilings* are left as-is (conservative, and
  `CrashLoopBackOff` is deliberately not treated as terminal to avoid false reds);
  intent-named per-phase timeouts can follow once convergence times are measured.
- **Phase 5 — CI lanes + runner hygiene + artifacts: DONE.** The matrix generator
  emits resource-budgeted lanes (`core-feature`, `heavy`, `operator-upgrade`,
  `major-upgrade`, `node-failure`, `flavor-MySQL-<v>`) instead of the whole suite
  per MySQL version. `e2e.yml` maps lane fields to env, sweeps leftover Kind
  clusters and names each run's cluster uniquely, uploads JUnit + run-log
  artifacts, and no longer cancels the nightly schedule. The PR gate is a single
  `e2e` pending status (`register-e2e-pending.yml`) that a maintainer's `/test`
  resolves; `authorize-e2e.yml` then dispatches the run **on the PR branch**
  (default-branch fallback for fork PRs), and the run posts the per-lane
  `E2E / <lane>` result statuses.
- **Phase 6 — flake governance: DONE.** Every gate lane excludes `flaky`
  (quarantine); `hack/e2e.sh` gained a `flaky` tier and a `GINKGO_EXTRA_ARGS`
  escape hatch (`--repeat`, `--allow-empty`); a non-blocking
  `e2e-flake-hunt.yml` repeats a lane nightly to surface flakes proactively.

### Deviations from the original plan (deliberate)

1. **Single-source + harness live in `package e2e`** (`test/e2e/images.go`,
   `resources.go`, `dedicated.go`) — not a separate `test/e2e/harness/` package.
   A sub-package would force moving `kubectl` / `deployOperator` / constants
   across a package boundary (circular-dependency risk + churn) for no functional
   gain; the "single source of truth, never re-literalled" goal is met either way.
2. **`isTransientWebhookError` is retained, not deleted (yet).** With the rollers
   moved off the shared operator it is now a harmless safety net rather than a
   flake-masker. Deleting it before the operator is right-sized (current limits
   are 512Mi / 500m) would risk new flakes from OOM-induced webhook blips. Delete
   once Phase 5 lanes + right-sizing land and the shared operator is confirmed
   stable.
3. **Alternate-hash images still use the `cmd/main.go` marker append**
   (`insertE2EMarker` / `insertInPlaceMarker`), not an ldflags marker. The manager
   builds via `go build cmd/main.go` (single file) with no ldflags and a Dockerfile
   that takes no build-arg, so the ldflags route means a test-only `var` in
   production `main` plus Dockerfile/Makefile changes that cannot be verified
   without building images. Revisit alongside the build-outside-suite work.

## Why

The suite is comprehensive (≈28 spec files, ≈100 `It`s, 8.5k LOC of Ginkgo) and
already does several things right: a shared operator/MinIO deployed once per
suite, per-process namespaces, a timeout multiplier for slow infra, rich
diagnostics on `expectClusterReady` failure, and a dedicated multi-node Kind
cluster for the node-failure spec. The problems are structural, and they cluster
into the three symptoms called out in the brief.

### Contention

- **One node hosts everything.** The shared Kind cluster is single-node
  ([Makefile](../Makefile) `setup-test-e2e`). CI runs `GINKGO_PROCS=3`
  ([.github/workflows/e2e.yml](../.github/workflows/e2e.yml)), and most specs
  stand up a 3-instance Cluster (`basicClusterManifest`,
  `semiSyncClusterManifest`, …). Three parallel processes × three mysqld Pods
  (limit `1536Mi` each, [helpers.go](../test/e2e/helpers.go) `e2eInstanceResources`)
  can demand far more than 16 GiB plus the operator, cert-manager, and MinIO.
  Memory pressure → eviction / OOM / slow scheduling → timeouts that look like
  product bugs.
- **Everything is pinned to that one node.** `deleteCluster` must block on PVC
  teardown so the next spec isn't scheduled "against a node still pinned by the
  previous cluster's resources" ([helpers.go](../test/e2e/helpers.go)). Cleanup is
  on the critical path and serializes specs that should be independent.
- **The flavor matrix re-runs the entire suite per MySQL version.** The
  generator ([.github/e2e-matrix-generator.py](../.github/e2e-matrix-generator.py))
  emits one job per MySQL version (8.0 / 8.4 / 9.x), and each job runs *all*
  non-labeled specs against the pinned version via `E2E_MYSQL_VERSION`
  ([archiving_helpers.go](../test/e2e/archiving_helpers.go) `sampleVersion`).
  Most specs are version-agnostic (TLS renewal, guards, PDB, services, kubectl
  plugin, deployment modes, webhook admission). Running them 3× triples both the
  wall-clock on the one runner and the flake surface for zero added coverage.

### Flakiness

- **Serial specs roll the one shared operator the rest of the suite depends on.**
  "Operator Upgrade", "Operator Upgrade defensive scenarios"
  ([e2e_test.go](../test/e2e/e2e_test.go)), "In-place operator upgrade"
  ([inplace_operator_upgrade_test.go](../test/e2e/inplace_operator_upgrade_test.go)),
  and "Namespaced deployment mode"
  ([deployment_modes_test.go](../test/e2e/deployment_modes_test.go)) `make deploy`,
  scale to zero, or delete the cluster-wide `ValidatingWebhookConfiguration`.
  The webhook is `failurePolicy: Fail`, so while it rolls, every other spec's
  `kubectl apply`/`annotate` can be rejected. The whole `isTransientWebhookError`
  retry machinery in [helpers.go](../test/e2e/helpers.go) exists only to paper
  over this self-inflicted disruption. `Serial` keeps them out of the *parallel*
  window, but they still mutate global state the suite assumes is stable, and any
  ordering slip is a flake.
- **The suite mutates its own source to build test images.** `insertE2EMarker` /
  `insertInPlaceMarker` append a line to `cmd/main.go` on disk, build, then
  restore it ([e2e_test.go](../test/e2e/e2e_test.go),
  [inplace_operator_upgrade_test.go](../test/e2e/inplace_operator_upgrade_test.go)).
  A panic between mutate and restore corrupts the working tree; on a persistent
  self-hosted runner that poisons every later run.
- **Floating image tags.** `minio/minio:latest`, `minio/mc:latest`,
  `curlimages/curl:latest` ([helpers.go](../test/e2e/helpers.go),
  [archiving_helpers.go](../test/e2e/archiving_helpers.go),
  [storage_pressure_test.go](../test/e2e/storage_pressure_test.go)) re-resolve
  over time and break the suite for reasons unrelated to the change under test.
- **Persistent-runner state bleed.** The Kind cluster name is fixed
  (`cnmsql-test-e2e`) and `setup-test-e2e` *reuses* an existing cluster. A run
  that crashes before `cleanup-test-e2e` leaves a dirty cluster, cert-manager,
  and namespaces that the next run silently inherits.

### Noisy indicators

- **20-minute blanket waits.** `expectClusterReady(..., 20*time.Minute)` appears
  63× and polls every 5s. It cannot tell "slow under load" from "wedged": a
  CrashLoopBackOff or a `Blocked` Cluster still burns the full 20 minutes before
  failing, and a flake that self-heals at minute 19 reports green. Failures take
  20–40 minutes (the self-hosted lane sets `E2E_TIMEOUT_MULTIPLIER=2`).
- **No machine-readable results, no artifacts.** [e2e.yml](../.github/workflows/e2e.yml)
  uploads nothing. There is no JUnit, no per-spec timing, no must-gather. A
  nightly failure is one giant log to scroll.
- **No flake policy.** No `--flake-attempts`, no quarantine lane, no proactive
  flake hunt. One transient blip fails the whole job; a newly flaky test blocks
  everyone until someone notices.

## Design overview

Eight pillars. Each is independently shippable; together they make the suite
deterministic on the constrained runner.

1. **Test taxonomy & labels** — classify every spec so it runs the right number
   of times in the right isolation.
2. **Execution partitions (resource budgeting)** — run the suite as a sequence
   of label-filtered passes with per-pass parallelism sized for 8 vCPU / 16 GiB.
3. **Operator & cluster isolation** — disruptive specs get their own ephemeral
   Kind cluster; the shared lane never mutates the shared operator. Retire the
   transient-webhook retry machinery.
4. **Deterministic readiness & fail-fast** — intent-named tiered timeouts that
   bail immediately on terminal states.
5. **Build & image hygiene** — build images once outside the suite; derive
   distinct binary hashes via ldflags, not source edits; pin and preload every
   helper image.
6. **One-command local runner** — `./hack/e2e.sh` with `--focus/--k8s/--mysql/--tier/--procs/--keep`.
7. **CI restructure & runner hygiene** — lanes that fit one runner, pre/post
   cleanup, JUnit + must-gather artifacts.
8. **Flake governance** — quarantine label, scoped flake-attempts, nightly flake
   hunt.

## Scope

### 1. Test taxonomy & labels

Add Ginkgo `Label`s to every top-level `Describe`. Labels drive both local
selection and CI lane composition, replacing the ad-hoc `node-failure` /
`major-upgrade` filter string in [e2e.yml](../.github/workflows/e2e.yml).

| Label | Meaning | Versions | Isolation | Default parallelism |
|-------|---------|----------|-----------|---------------------|
| `core` | Critical-path smoke: bootstrap, replication, switchover, failover, webhook admission, controller metrics | matrix | shared | with feature lane |
| `feature` | Version-agnostic functional coverage | one (default) | shared | high |
| `flavor` | Genuinely version-sensitive server behavior | matrix | shared | medium |
| `disruptive` | Mutates the operator or the cluster topology; needs a private control plane | one | **dedicated ephemeral cluster** | serial |
| `heavy` | Large footprint / long runtime (multi-member GR, semi-sync 3-node, stress) | as tagged | shared | **capped (procs=1–2)** |
| `flaky` | Quarantined; non-blocking | as tagged | as tagged | non-blocking lane |

`core` and `heavy` are orthogonal qualifiers that combine with the others (e.g.
the sample-bootstrap spec is `core,flavor`; semi-sync self-healing is
`feature,heavy`). Keep the existing `node-failure` and `major-upgrade` labels as
sub-selectors *under* `disruptive`.

**Initial spec → label mapping** (the implementer refines as they tag; the
principle is "version-agnostic runs once, operator-mutating gets its own cluster"):

| Spec file / Describe | Label(s) |
|----------------------|----------|
| [e2e_test.go](../test/e2e/e2e_test.go) "Manager" (bootstrap, switchover, failover, metrics, webhook) | `core,flavor` |
| [e2e_test.go](../test/e2e/e2e_test.go) "Operator Upgrade" + "…defensive scenarios" | `disruptive` |
| [inplace_operator_upgrade_test.go](../test/e2e/inplace_operator_upgrade_test.go) | `disruptive` |
| [deployment_modes_test.go](../test/e2e/deployment_modes_test.go) "Namespaced deployment mode" | `disruptive` |
| [deployment_modes_test.go](../test/e2e/deployment_modes_test.go) "Cluster-wide deployment mode" | `feature` |
| [node_failure_test.go](../test/e2e/node_failure_test.go) | `disruptive,node-failure` |
| [major_upgrade_test.go](../test/e2e/major_upgrade_test.go) rollout/defensive/single/backup-gate | `disruptive,major-upgrade` |
| [major_upgrade_test.go](../test/e2e/major_upgrade_test.go) admission (no rollout) | `feature` |
| [backup_test.go](../test/e2e/backup_test.go), [pitr_test.go](../test/e2e/pitr_test.go), [retention_test.go](../test/e2e/retention_test.go), [archiving_test.go](../test/e2e/archiving_test.go) | `flavor` |
| [inplace_upgrade_test.go](../test/e2e/inplace_upgrade_test.go) (instance-manager stream) | `flavor` |
| [groupreplication_test.go](../test/e2e/groupreplication_test.go) multi-member, [groupreplication_lifecycle_test.go](../test/e2e/groupreplication_lifecycle_test.go) | `feature,heavy` |
| [groupreplication_backup_test.go](../test/e2e/groupreplication_backup_test.go), [groupreplication_pitr_test.go](../test/e2e/groupreplication_pitr_test.go) | `flavor,heavy` |
| [groupreplication_guards_test.go](../test/e2e/groupreplication_guards_test.go), [groupreplication_metrics_test.go](../test/e2e/groupreplication_metrics_test.go), [groupreplication_test.go](../test/e2e/groupreplication_test.go) single-member | `feature` |
| [selfhealing_test.go](../test/e2e/selfhealing_test.go) | `feature,heavy` |
| [stress_failover_pitr_test.go](../test/e2e/stress_failover_pitr_test.go) | `flavor,heavy` |
| [guards_test.go](../test/e2e/guards_test.go), [tls_test.go](../test/e2e/tls_test.go), [volume_resize_test.go](../test/e2e/volume_resize_test.go), [storage_pressure_test.go](../test/e2e/storage_pressure_test.go), [managed_roles_databases_test.go](../test/e2e/managed_roles_databases_test.go), [databaseuser_test.go](../test/e2e/databaseuser_test.go), [scheduledbackup_test.go](../test/e2e/scheduledbackup_test.go), [failover_rejoin_test.go](../test/e2e/failover_rejoin_test.go), [diverged_failover_test.go](../test/e2e/diverged_failover_test.go), [replica_guard_test.go](../test/e2e/replica_guard_test.go) | `feature` |

**Once isolation (pillar 3) lands, most current `Serial` decorators can drop.**
`replica_guard` and similar are `Serial` only out of caution; they touch only
their own namespace and become parallel-safe. The only specs that stay serial are
`disruptive` ones, and they are serial *because each owns a cluster*, not because
they fight over a shared one.

### 2. Execution partitions (resource budgeting)

Ginkgo has no weighted scheduler, so model the resource budget as an ordered
sequence of label-filtered passes, each with its own `--procs`. The one local
command and each CI lane are thin wrappers over this sequence.

Budget for 8 vCPU / 16 GiB (reserve ~2 vCPU / ~3 GiB for the Kind control plane,
operator, cert-manager, and MinIO; a 3-instance Cluster steady-state is
≈1.2–1.6 GiB / ≈0.6 vCPU):

| Pass | Label filter | `--procs` | Rationale |
|------|--------------|-----------|-----------|
| light/feature | `feature && !heavy` | `3` | ~3 concurrent 3-node clusters fit in ~12 GiB |
| heavy | `heavy` | `1` | one multi-member/stress cluster at a time |
| flavor | `flavor && !heavy` | `2` | version-sensitive, moderate footprint |
| disruptive | `disruptive` | `1` (per-cluster) | each owns an ephemeral cluster |

Derive a sensible default `--procs` from `nproc` and `MemTotal` (cap at 3 on this
box) so a laptop and the runner both behave. Keep `E2E_TIMEOUT_MULTIPLIER` for
genuinely slower hardware, but it should become rarely needed once readiness is
deterministic (pillar 4).

Right-size the test clusters too: keep `innodb_buffer_pool_size: 128M`, drop the
per-instance memory **limit** from `1536Mi` toward `~768Mi` for non-perf specs
(headroom for more parallelism), and make storage sizes uniform and small. Centralize
these in one place (pillar 3's harness) instead of per-manifest literals.

### 3. Operator & cluster isolation

This is the highest-leverage fix. Two rules:

1. **The shared lane never mutates the shared operator.** No spec in `core` /
   `feature` / `flavor` / `heavy` may `make deploy`, scale, or delete the
   operator or its webhook.
2. **Every `disruptive` spec runs against its own ephemeral Kind cluster.**

Generalize the proven node-failure pattern
([node_failure_test.go](../test/e2e/node_failure_test.go):
`provisionDedicatedCluster` / `teardownDedicatedCluster`) into a reusable harness
so any disruptive spec gets a private control plane:

```
test/e2e/harness/
  cluster.go     // ProvisionDedicated(name, kindConfig) / Teardown — create kind,
                 // load prebuilt images, install cert-manager, deploy operator,
                 // wait ready; restore prior kube-context on teardown
  operator.go    // Deploy/Undeploy/Redeploy(image), DeployNamespaced(ns, prefix)
  images.go      // single source of truth for manager + instance + helper image refs
  resources.go   // e2eInstanceResources, mysql params, storage sizes (one place)
```

Disruptive specs then read:

```go
var _ = Describe("Operator Upgrade", Label("disruptive"), Ordered, func() {
    var c *harness.Cluster
    BeforeAll(func() { c = harness.ProvisionDedicated("op-upgrade", harness.SingleNode) })
    AfterAll(func()  { c.Teardown() })
    It("rolls primary-last", func() { c.RedeployOperator(v2Image); /* ... */ })
})
```

Consequences:

- **Delete `isTransientWebhookError` and its callers** in
  [helpers.go](../test/e2e/helpers.go). With no spec rolling the shared operator,
  a webhook rejection is a real failure again. `applyManifest` / `clusterAnnotate`
  become plain calls; this also removes a class of false greens (retries hiding a
  real intermittent webhook bug).
- Disruptive specs can run on their own CI lane concurrently with the shared lane
  *in principle* — but on one runner they queue; the win is correctness and the
  death of the retry machinery, not parallelism.
- The single-node vs multi-node choice is a `harness.SingleNode` /
  `harness.MultiNode` config; node-failure keeps
  [kind-multinode.yaml](../test/e2e/kind-multinode.yaml).

> **Alternative considered — namespaced operator per disruptive spec** (deploy a
> `config/namespaced` operator into the spec's namespace instead of a whole Kind
> cluster). Lighter, but it cannot isolate cluster-scoped objects (CRDs, the
> cluster-wide `ValidatingWebhookConfiguration`) that the operator-upgrade specs
> mutate, and the namespaced-mode spec specifically tests cohabitation. Dedicated
> ephemeral clusters are the recommended default; the namespaced helper stays
> available for the deployment-modes spec.

**Shared MinIO hardening.** Keep one MinIO per suite (the per-Describe
deploy/teardown it replaced cost ~6 min each), but bump its PVC well above the
current `1Gi` ([helpers.go](../test/e2e/helpers.go) `minioManifest`) so
concurrent backup/PITR/archiving/retention specs can't fill it, and confirm every
backup spec already prefixes object keys by cluster name (they do via
`objectStoreYAML`), so the only shared resource is disk capacity.

### 4. Deterministic readiness & fail-fast

Replace the blanket `expectClusterReady(..., 20*time.Minute)` with intent-named
helpers whose timeouts reflect a single phase, and which **abort early on
terminal states** instead of polling to exhaustion:

```go
// helpers.go — phase-scoped budgets (pre-multiplier)
const (
    timeoutBootstrap   = 6 * time.Minute  // initdb + first Pod Ready
    timeoutReplicaJoin = 5 * time.Minute  // clone + GTID catch-up per replica
    timeoutFailover    = 4 * time.Minute  // election + rw re-route
    timeoutSwitchover  = 4 * time.Minute
)

// expectClusterReady fails fast if the cluster reaches a terminal bad state.
func expectClusterReady(name string, instances int, timeout time.Duration) {
    // ... existing polling, plus on each tick:
    //   - phase == "Blocked"  -> dump diagnostics, Fail now (no point waiting)
    //   - any instance Pod in CrashLoopBackOff / ImagePullBackOff / Error
    //         -> dump diagnostics, Fail now
}
```

Because all images are preloaded into Kind (BeforeSuite already does this for
manager + instance images; pillar 5 adds the helper images), image pull is never
on the critical path, so these shorter budgets are safe. Net effect: a wedged
cluster fails in ~2–6 minutes with a must-gather, and the 20-minute false-green
window disappears.

Also: drop the bare `time.Sleep(5s)` "stabilize" waits
([e2e_test.go](../test/e2e/e2e_test.go):192, :988,
[helpers.go](../test/e2e/helpers.go):358) in favor of polling a real condition
(`waitForWebhookReady` already exists for the webhook case).

### 5. Build & image hygiene

- **Build the manager image once, outside Ginkgo.** Move `make docker-build` +
  `kind load` out of `doSuiteSetup` ([e2e_suite_test.go](../test/e2e/e2e_suite_test.go))
  into a Makefile target (`e2e-build-images`) that the local runner and CI call
  *before* invoking Ginkgo. The suite asserts the image is present and loaded,
  not builds it. This decouples build failures from test failures and stops
  rebuilding per Ginkgo invocation.
- **Distinct binary hashes via ldflags, not source edits.** Replace
  `insertE2EMarker` / `insertInPlaceMarker` (which edit `cmd/main.go`) with a
  build that injects a unique value through `-ldflags "-X <pkg>.buildMarker=..."`.
  `make e2e-build-images E2E_BUILD_MARKER=v2` produces a different hash with zero
  working-tree mutation. Delete `appendMainGoMarker` / `mainGoSnapshot` /
  `restoreE2EMarker`.
- **Pin and preload every helper image.** Pin `minio`, `mc`, and `curl` to a
  digest in `harness/images.go`, and preload them into Kind in
  `SynchronizedBeforeSuite` alongside the instance images. No spec pulls from a
  registry mid-run.

### 6. One-command local runner

`hack/e2e.sh` is the single entrypoint. It owns the full lifecycle (create Kind →
build & load images → deploy → run the partition sequence → teardown) and exposes
the brief's required arguments:

```
./hack/e2e.sh [flags]

  --focus <regex>     Ginkgo --focus (run a subset by description)
  --label <filter>    Ginkgo --label-filter (e.g. 'feature && !heavy')
  --tier <name>       Convenience preset: smoke | feature | flavor | disruptive | all  (default: all)
  --k8s <version>     kindest/node version, e.g. v1.36.1            (-> K8S_VERSION)
  --mysql <version>   pin one MySQL flavor: 8.0 | 8.4 | 9.x         (-> E2E_MYSQL_VERSION)
  --procs <N>         override per-pass parallelism (default: auto from CPU/RAM)
  --keep              do not tear down the Kind cluster afterwards (fast inner loop)
  --fresh             delete and recreate the Kind cluster before running
  --junit <path>      write a JUnit report

Examples
  ./hack/e2e.sh                                   # whole suite, auto-sized, hermetic
  ./hack/e2e.sh --tier smoke                      # core path only, fast
  ./hack/e2e.sh --focus 'switchover' --keep       # iterate on one spec, reuse cluster
  ./hack/e2e.sh --mysql 9.x --tier flavor         # version-sensitive specs on 9.x
  ./hack/e2e.sh --k8s v1.36.1 --label 'disruptive'
```

Defaults tuned for a laptop: reuse the Kind cluster across runs (`--keep` is
implied unless `--fresh`), run one MySQL version, auto-size `--procs`. `make
test-e2e` stays as a thin alias to `./hack/e2e.sh` so existing muscle memory and
docs keep working; the `E2E_FOCUS`-via-`MAKECMDGOALS` hack in
[Makefile](../Makefile) is retired in favor of `--focus`.

### 7. CI restructure & runner hygiene

Keep one workflow, but compose lanes from labels instead of re-running the whole
suite per version. On a single runner the lanes queue, so the objective is
**less total work + isolation**, not fan-out.

**Lanes** (each lane = one matrix entry = one partition sequence):

| Lane | Label filter | Versions | Cluster |
|------|--------------|----------|---------|
| `core+feature` | `(core \|\| feature) && !heavy` | default only | shared |
| `heavy` | `heavy` | default only | shared |
| `flavor` | `flavor` | per MySQL version (matrix) | shared |
| `disruptive` | `disruptive` | default only | per-spec ephemeral |

The flavor matrix shrinks from "whole suite × 3 versions" to "flavor specs × 3
versions" — the single biggest wall-clock and flake-surface reduction. The
generator ([.github/e2e-matrix-generator.py](../.github/e2e-matrix-generator.py))
changes from emitting per-version whole-suite jobs to emitting these four lanes
(with the flavor lane carrying the version axis). Pending-status registration
([register-e2e-pending.yml](../.github/workflows/register-e2e-pending.yml),
[authorize-e2e.yml](../.github/workflows/authorize-e2e.yml)) keys off the new lane
ids.

**Runner hygiene** (pre/post steps in [e2e.yml](../.github/workflows/e2e.yml),
critical for a persistent runner):

- *Pre-job:* delete any leftover Kind clusters matching the run prefix, `docker
  system prune -f`, and assert free disk + memory; fail fast if the box is
  unhealthy rather than producing a confusing red.
- *Unique cluster name per run:* `KIND_CLUSTER=cnmsql-e2e-${GITHUB_RUN_ID}` so a
  crashed prior run can never be silently reused.
- *Post-job (`if: always()`):* tear down the run's Kind cluster(s) and remove
  `/tmp/cnmsql-e2e-*` manifests.

**Artifacts (`if: always()`):**

- Ginkgo `--junit-report` XML and `--json-report` for per-spec timing/history.
- A must-gather bundle on failure: operator logs, all instance Pod logs, events,
  `describe nodes`, and Cluster YAML — reuse `dumpE2EDiagnostics`
  ([helpers.go](../test/e2e/helpers.go)) but write to files and `upload-artifact`
  instead of only `GinkgoWriter`.

**Concurrency:** keep `cancel-in-progress: true` for PR-triggered runs; the
nightly schedule must **not** cancel (so a long nightly isn't killed by a PR).

### 8. Flake governance

- **Quarantine label `flaky`.** Excluded from the blocking lanes' filters; run in
  a separate non-blocking job so a newly flaky spec stays visible without blocking
  merges. Promoting a spec out of quarantine requires it to pass the flake hunt.
- **Scoped `--flake-attempts=2`** for specs with irreducible timing
  nondeterminism (network blips, eventual routing). Apply per-spec via the
  `FlakeAttempts(2)` decorator, *not* suite-wide — a global retry hides real bugs.
- **Nightly flake hunt.** A scheduled job runs the `core+feature` lane under
  `ginkgo --repeat=N` (or `--until-it-fails` with a cap) and reports any spec that
  fails intermittently, so flakes are found by CI, not by contributors.

## Files to create / modify

| File | Change |
|------|--------|
| `hack/e2e.sh` | **NEW** — single local entrypoint; lifecycle + flag parsing + partition sequence |
| `test/e2e/harness/cluster.go` | **NEW** — `ProvisionDedicated` / `Teardown` (generalized from node-failure) |
| `test/e2e/harness/operator.go` | **NEW** — operator deploy/redeploy/namespaced helpers |
| `test/e2e/harness/images.go` | **NEW** — single source of truth for all image refs, pinned by digest |
| `test/e2e/harness/resources.go` | **NEW** — instance resources, mysql params, storage sizes |
| `test/e2e/*_test.go` | **MODIFY** — add `Label(...)` to every `Describe`; disruptive specs use the harness; drop now-unneeded `Serial` |
| `test/e2e/helpers.go` | **MODIFY** — phase-scoped timeouts + terminal-state fail-fast; **delete** `isTransientWebhookError` and retry wrappers; drop `time.Sleep` stabilizers |
| `test/e2e/e2e_test.go`, `inplace_operator_upgrade_test.go` | **MODIFY** — remove `cmd/main.go` mutation; use ldflags build marker via harness |
| `test/e2e/e2e_suite_test.go` | **MODIFY** — assert (don't build) the manager image; preload pinned helper images |
| `Makefile` | **MODIFY** — `e2e-build-images` target (ldflags marker, no source edit); `test-e2e` → alias to `hack/e2e.sh`; retire `MAKECMDGOALS` focus hack |
| `.github/e2e-matrix-generator.py` | **MODIFY** — emit the four lanes; version axis only on the flavor lane |
| `.github/workflows/e2e.yml` | **MODIFY** — lane composition, runner pre/post hygiene, unique cluster name, JUnit + must-gather artifacts, nightly-no-cancel |
| `.github/workflows/register-e2e-pending.yml`, `authorize-e2e.yml` | **MODIFY** — register the new lane ids |
| `.github/workflows/e2e-flake-hunt.yml` | **NEW** — nightly `--repeat`/`--until-it-fails` flake hunt + `flaky` lane |
| `docs/src/` + `CONTRIBUTING.md` | **MODIFY** — document `./hack/e2e.sh`, labels/tiers, and the CI lanes |

## Conventions

- **Labels:** `core`, `feature`, `flavor`, `disruptive`, `heavy`, `flaky`;
  sub-selectors `node-failure`, `major-upgrade` under `disruptive`.
- **Env vars (unchanged where they exist):** `E2E_MYSQL_VERSION`, `K8S_VERSION`,
  `E2E_TIMEOUT_MULTIPLIER`, `KIND_CLUSTER`; **new:** `E2E_BUILD_MARKER` (ldflags
  hash), `E2E_PROCS`.
- **Ephemeral cluster names:** `${KIND_CLUSTER}-<spec-slug>` (mirrors the existing
  `-nodefail` suffix), unique per run via `${GITHUB_RUN_ID}` in CI.
- **Artifacts:** JUnit at `test-results/junit-<lane>.xml`; must-gather at
  `test-results/must-gather-<lane>/`.
- **One source of truth** for image refs and cluster resource shapes lives in
  `test/e2e/harness`, never re-literalled in specs.

## Execution order

1. **Taxonomy first** — add `Label`s to every `Describe` and the
   `harness/resources.go` + `harness/images.go` single-source files. Pure
   addition; nothing breaks. Unlocks label-filtered local runs immediately.
2. **Local runner** — land `hack/e2e.sh` and the `e2e-build-images` target
   (ldflags marker). Contributors get the one-command UX and reproducible builds.
3. **Isolation** — build `harness/cluster.go` + `harness/operator.go`, migrate the
   disruptive specs onto ephemeral clusters, then **delete** the transient-webhook
   retry machinery. This is the flake-killer; do it as one reviewed change so the
   "no spec mutates the shared operator" invariant is enforced atomically.
4. **Readiness** — phase-scoped timeouts + terminal-state fail-fast; remove
   `time.Sleep` stabilizers. Validate that healthy runs are unaffected and wedged
   runs fail fast.
5. **CI restructure** — rewrite the matrix generator into lanes, add runner
   hygiene + JUnit + must-gather, wire pending-status lanes. Run a few nightlies
   to confirm green-on-green.
6. **Flake governance** — add the `flaky` quarantine lane and the nightly flake
   hunt; apply scoped `FlakeAttempts` only where a hunt proves irreducible
   timing.

## Risks & non-goals

- **Ephemeral clusters add per-disruptive-spec setup cost** (Kind create +
  operator deploy, ~1–2 min). Acceptable: there are few disruptive specs and they
  already dominate runtime; correctness and the death of the retry machinery are
  worth it. Mitigate by preloading images once and reusing them across the run.
- **Re-tagging specs is a one-time audit.** The mapping table above is a starting
  point; the implementer owns getting each `Describe` into the right lane.
- **Not in scope:** rewriting specs' assertions, changing product behavior,
  adding new feature coverage, or introducing a non-Ginkgo runner. This design is
  purely about how the existing specs are isolated, scheduled, built, surfaced,
  and run.
- **Single-runner reality:** this design does not assume horizontal CI scale. If a
  second self-hosted runner is added later, the lanes already parallelize across
  runners with no further change.

## References

- Current suite setup: [test/e2e/e2e_suite_test.go](../test/e2e/e2e_suite_test.go), [test/e2e/helpers.go](../test/e2e/helpers.go)
- Dedicated-cluster pattern to generalize: [test/e2e/node_failure_test.go](../test/e2e/node_failure_test.go), [test/e2e/kind-multinode.yaml](../test/e2e/kind-multinode.yaml)
- Operator-rolling Serial specs: [test/e2e/e2e_test.go](../test/e2e/e2e_test.go), [test/e2e/inplace_operator_upgrade_test.go](../test/e2e/inplace_operator_upgrade_test.go), [test/e2e/deployment_modes_test.go](../test/e2e/deployment_modes_test.go)
- Version helpers: [test/e2e/archiving_helpers.go](../test/e2e/archiving_helpers.go)
- CI: [.github/workflows/e2e.yml](../.github/workflows/e2e.yml), [.github/e2e-matrix-generator.py](../.github/e2e-matrix-generator.py), [.github/workflows/register-e2e-pending.yml](../.github/workflows/register-e2e-pending.yml), [.github/workflows/authorize-e2e.yml](../.github/workflows/authorize-e2e.yml)
- Build/run targets: [Makefile](../Makefile)
- Ginkgo labels & decorators: https://onsi.github.io/ginkgo/#spec-labels , https://onsi.github.io/ginkgo/#repeating-spec-runs-and-managing-flaky-specs
