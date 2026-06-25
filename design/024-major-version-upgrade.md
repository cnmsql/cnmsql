# 024 — MySQL Major Version Upgrade

**Status:** done

Safe, orchestrated MySQL **server** major-version upgrades along the supported
hop chain `8.0 → 8.4 → 9.x`, distinct from the operator/instance-manager upgrade
of [019](019-operator-upgrade.md).

## Overview

Today a change to the resolved instance image (via `spec.imageName` or
`spec.imageCatalogRef`) flows into the Pod spec, changes
`podTemplateHashAnnotation` (`internal/controller/cluster_resources.go:340`), and
`ensurePod` rolls instances one at a time, replicas first and primary last
(`internal/controller/cluster_resources.go:297-350`). The parsed server version
(`pkg/management/mysql/version/version.go`) already gates config/SQL dialect.

That mechanical roll is **necessary but not sufficient** for a *major* version
change. It does nothing to enforce the rules MySQL imposes on cross-major
upgrades, and the doc explicitly lists in-place major upgrade as unsupported
(`docs/src/instance-images.md:119-121`). The dedicated upgrade reconcile that
exists (`internal/controller/cluster_upgrade.go`) is for the **instance-manager
binary only** — it has nothing to do with the mysqld server version.

This design adds the guards and orchestration to make `8.0 → 8.4 → 9.x` safe.

## Background: what MySQL requires

These are server constraints, not operator choices — the design exists to honor
them:

1. **No version skipping.** Upgrades are only supported between adjacent
   GA/LTS series. `8.0 → 8.4 → 9.x` must pass through 8.4 (LTS). `8.0 → 9.x`
   directly is unsupported.
2. **No in-place downgrade.** Once mysqld upgrades the data dictionary on first
   start of the new binary, the step is irreversible; the only way back is
   restore-from-backup.
3. **Replica version ≥ source version** during the mixed-version window. A newer
   replica replicating from an older primary is supported; the reverse is not.
   Primary-last rollout is therefore mandatory, not just preferable.
4. **The data-dictionary / server upgrade runs on mysqld startup**
   (`--upgrade=AUTO`, default since 8.0.16). It can take time and is the
   irreversible step. The operator must wait for it to *complete* on each
   instance before advancing.
5. **Group Replication** requires the group communication protocol version to be
   raised (`group_replication_set_communication_protocol`) **after** every member
   is on the new version — never before.
6. **Removed / changed system variables and auth defaults** between majors. Most
   important: `mysql_native_password` is disabled-by-default in 8.4 and removed in
   9.x; numerous deprecated sysvars are removed in 8.4. If the rendered
   `my.cnf` names an option the new server rejects, mysqld fails to start
   *mid-roll*, stranding the cluster.

## Finding: ImageCatalog cannot currently express the hop chain

**The catalog API keys images by integer major, which collapses 8.0 and 8.4 into
the same key — so the very chain we want to support cannot be expressed.**

- `CatalogImage.Major` is an `int` with `+listMapKey=major`, so a major value is
  unique per catalog (`api/v1alpha1/imagecatalog_types.go:35-44`).
- `ImageCatalogRef.Major` is an `int` (`api/v1alpha1/cluster_types.go:439-441`).
- Resolution is `FindImageForMajor(int)` (`api/v1alpha1/imagecatalog_funcs.go:48`).
- The shipped sample and docs map `major: 8` to an **8.4** image
  (`config/samples/mysql_v1alpha1_imagecatalog.yaml`,
  `docs/src/instance-images.md:66-78`), and the `CatalogImage.Major` comment
  already hedges ("8 for 8.0/8.4 lines … uses the full version where needed").

For PostgreSQL (CNPG's model, which this mirrors) integer majors are the upgrade
unit — 14, 15, 16. For **MySQL they are not**: 8.0 and 8.4 are distinct upgrade
targets that both live under integer major 8. With the current API a single
catalog can hold *either* 8.0 *or* 8.4 but not both, and a Cluster cannot point
at "8.4" as distinct from "8.0". This blocks the `8.0 → 8.4` hop entirely.

This must be resolved before the orchestration is meaningful. See the API change
below.

## Design

### A. Catalog/series API change (prerequisite)

Make the upgrade unit the **MySQL series**, not the integer major. Preferred
option: replace the `int` key with a series string.

- `CatalogImage`: `Major int` → `Series string` (e.g. `"8.0"`, `"8.4"`, `"9.0"`),
  keep `+listMapKey` on the new field. Validate against the supported set.
- `ImageCatalogRef`: `Major int` → `Series string`.
- `FindImageForMajor(int)` → `FindImageForSeries(string)`.
- The GR floor check (`spec.ImageCatalogRef.Major < 8`,
  `api/v1alpha1/cluster_funcs.go:359-365`) becomes a series comparison.

Because this is a breaking field rename on an `v1alpha1` API with no other
consumers, do it as a rename (not an additive alias). Update the sample, docs,
and CRD bases.

> Alternative considered: keep `int` but encode the series (80, 84, 90). Rejected
> — it is unreadable, leaks into user-facing YAML, and still needs a parser. A
> `Series string` is self-documenting and matches the image tags
> (`8.0`/`8.4`/`9.x`) the build pipeline already publishes.

Major upgrades are expressed **only** through `imageCatalogRef`. A major-series
change via raw `imageName` is rejected at admission (below), so the major is
always explicit and validatable.

### B. New `spec.upgrade` API

```go
// UpgradeConfiguration tunes MySQL server major-version upgrades.
type UpgradeConfiguration struct {
    // BackupBeforeUpgrade controls whether the operator takes a fresh backup
    // before starting a major-version upgrade. Defaults to true. Set false to
    // skip (e.g. when an external backup process is in place).
    // +optional
    BackupBeforeUpgrade *bool `json:"backupBeforeUpgrade,omitempty"`
}
```

Default (`nil` → `true`): the operator auto-triggers a `Backup` and waits for it
to succeed before rolling any instance.

### C. Admission guard — `ValidateUpdate`

Extend `Cluster.ValidateUpdate` (`api/v1alpha1/cluster_funcs.go:209`):

- Resolve old vs. new target **series**.
- Reject **downgrade** (new series < old series).
- Reject **skip-level** transitions (must be an adjacent supported hop:
  `8.0→8.4`, `8.4→9.x`; `8.0→9.x` rejected).
- Reject a major-series change expressed via `imageName` (force `imageCatalogRef`
  for major upgrades; patch-level `imageName` changes within a series stay
  allowed).

This is the cheap, brick-preventing layer and ships first.

> **Status:** Phase 1 shipped the `ValidateUpdate` logic (unit-tested) but left it
> unwired, like the pre-existing `replication.mode` / `groupName` immutability
> checks. **Phase 2 wired it:** `ClusterSpecValidator`
> (`internal/webhook/v1alpha1/cluster_webhook.go`) is a validating webhook on
> Cluster create/update that runs `Validate` and, on update, `ValidateUpdate`, so
> the guard now rejects at apply time. It registers as the second webhook
> (`vclustervalidation-…`, sorted after `vclusterstatus-…`) so the status
> webhook's index-0 kustomize patches are unaffected; the namespaced overlay
> namespace-scopes both. The instance-manager hard refusal remains the
> defense-in-depth backstop for a bypassed webhook.

### D. Config-renderer version gating

Extend the version predicates in `pkg/management/mysql/version/version.go`
(the file already has this pattern, e.g. `UsesResetBinaryLogsAndGtids` for 8.4)
and apply them in `pkg/management/mysql/config/config.go` so the rendered
`my.cnf` never names an option the target server rejects — first and foremost the
auth-plugin defaults (`mysql_native_password`) for 8.4/9.x, plus sysvars removed
in 8.4. Without this, mysqld refuses to start on the new image mid-roll.

### E. Per-instance "upgrade complete" readiness signal

The in-Pod status already reports `version`
(`pkg/management/mysql/webserver/status.go:38-39`). Add an explicit
"data-dictionary upgrade finished" signal so the operator does not advance to the
next instance while a server upgrade is still running:

- Report running `version` (already present) **and** an `upgradeInProgress` /
  `upgradeComplete` flag derived from mysqld's upgrade state.
- The instance manager waits for the startup upgrade to complete before declaring
  the instance Ready for upgrade-advance purposes.

Optionally pass `--upgrade` explicitly in the runner
(`pkg/management/mysql/instance/runner.go:246`) for predictable behavior, tied to
this signal.

### F. MySQL-version upgrade reconcile path

A new reconcile sibling to `cluster_upgrade.go` (operator upgrade), keyed on
"running series < spec target series" rather than relying on the generic
pod-hash roll:

1. **Backup gate.** If `upgrade.backupBeforeUpgrade` (default true), ensure a
   fresh successful backup exists / trigger one (design [008](008-physical-backup-recovery.md)
   machinery) and wait. Refuse to proceed otherwise.
2. **Replicas first.** Roll one replica at a time; each must become Ready before
   the next. Readiness already implies upgrade-complete: with the default
   `--upgrade=AUTO`, mysqld does not accept connections (so the instance manager's
   `Readyz` does not pass) until the data-dictionary upgrade has finished. The
   existing readiness gate in `gateInstance`/`reconcileInstances` therefore
   serializes the upgrade roll unchanged — no separate per-instance gate is added.
   Signal E (the live version + `UpgradeComplete`) drives the operator-side
   *detection* of a pending upgrade (`majorUpgradePending`), not a second gate; an
   explicit "previous reached target series" gate was considered and rejected
   because it deadlocks the primary-last ordering (the primary, rolled last, stays
   Ready on the old series while replicas roll).
3. **Primary last**, via switchover where possible — reuse
   `upgradePrimaryViaSwitchover` (`internal/controller/cluster_upgrade.go:214`)
   and the existing `allowRoll`/primary-last ordering in `ensurePod`.
4. Surface a distinct phase/reason, mirroring how the operator-upgrade path
   publishes `topology.PhaseUpgrading` progress
   (`internal/controller/cluster_upgrade.go:247`).

### G. Group Replication finalization

In the GR path, after every member reports the new version (signal E), issue
`group_replication_set_communication_protocol` to the new version to complete the
upgrade. No equivalent exists today. Sequence with design
[022](022-group-replication.md).

> **Status:** Phase 3 is complete. The GR manager exposes the protocol get/set
> UDFs, each instance reports the live protocol in `/status`, and the operator
> uses a dedicated control endpoint to raise it on the primary only after every
> expected member reports the target series and `ONLINE`. Because MySQL reports
> the effective minimum-compatible protocol (8.4 reports 8.0.27), not necessarily
> the server version passed to the setter, the last successful server target is
> persisted in Cluster status as the reconciliation idempotency marker.

## What we already have

- Primary-last serialized rolling restart on template-hash change
  (`internal/controller/cluster_resources.go:297-350`).
- Version parsing + `AtLeast`-style predicates
  (`pkg/management/mysql/version/version.go`).
- Switchover machinery reusable for primary-last upgrade
  (`upgradePrimaryViaSwitchover`).
- In-Pod status reporting `version`
  (`pkg/management/mysql/webserver/status.go:38`).
- Backup machinery (design 008) for the pre-upgrade gate.
- `topology.PhaseUpgrading` progress-status helper pattern.

## Implementation status and tracked gaps

Sections A–G are implemented: series-keyed catalogs, admission and instance
guards, target-version config filtering, live version and upgrade-complete
status, the pre-upgrade backup gate, serialized replica-first rollout, and GR
communication-protocol finalization. The upgrade and rollback guide is also
published.

The remaining tracked gaps are deliberately not hidden:

1. The instance-manager refusal reason is logged but is not available through
   `/status`, because the manager exits before starting the control server.
2. The dedicated multi-image E2E job now covers a real three-member GR roll
   through `8.0 → 8.4 → 9.x`, including replica-first/primary-last ordering,
   data survival, and protocol finalization. Single-instance upgrades, the
   backup gate, config gating, rollback-via-restore, and bypassed-admission
   instance refusal remain tracked E2E gaps.

## Phasing

- **Phase 1 (done — brick-prevention):** A (catalog series) + C (admission guard
  + instance-manager guard) + D (config gating). Prevents the two unrecoverable
  failure modes — illegal transitions and mysqld failing to boot on the new image
  — and unblocks expressing the hop chain at all.
- **Phase 2 (done — orchestration):** B (`spec.upgrade`) + E (readiness signal) +
  F (reconcile path with backup gate).
- **Phase 3 (done — GR):** G finalizes the communication protocol after all
  members have reached the target series and are `ONLINE`.

Documentation and E2E coverage land **with each phase** (the corresponding guide
sections and E2E cases ship in the same PR as the behavior they describe), not as
a trailing phase.

## Resolved decisions

- **Catalog series representation:** `Series string` (Section A). The breaking
  `v1alpha1` field rename is **accepted**.
- **Adjacency enforcement: defense in depth.** The adjacency/no-skip/no-downgrade
  chain is enforced at **both** layers: at admission (Section C, the primary
  gate) **and** in the instance manager before it starts mysqld on a new binary.
  Admission can be bypassed (disabled webhook, direct edits, restored objects), so
  the instance manager independently refuses to boot a server whose data-dictionary
  series is more than one hop below the image series, or above it (downgrade). The
  manager records the running series in a marker file in the data directory and
  compares it to the image version on the next start; an unsupported transition is
  a hard refusal (the manager exits with the reason logged, before mysqld touches
  the irreversibly-upgraded data dictionary). **Phase split:** the hard refusal +
  marker ship in Phase 1. Surfacing the reason in `/status` requires serving the
  control API while mysqld is intentionally not started, which is built with the
  Phase 2 readiness signal (Section E) and folded in there.
- **Backup-gate with no object store:** **hard-fail.** When
  `backupBeforeUpgrade` is true (its default) and no usable object store is
  configured, the operator does not start the upgrade and sets a clear status
  condition (e.g. `UpgradeBlocked`, reason `BackupRequired`) explaining that the
  user must configure backups or explicitly set `backupBeforeUpgrade: false`. The
  upgrade never proceeds silently without the safety net.

## Documentation

A dedicated upgrade guide (`docs/src/major-version-upgrade.md`, linked from the
sidebar and from `instance-images.md`, whose current "in-place major upgrade not
supported" note is updated) must cover:

- **How to upgrade.** The supported hop chain `8.0 → 8.4 → 9.x`, that hops cannot
  be skipped, and the catalog/`imageCatalogRef` series workflow (Section A). The
  default auto-backup behavior and how to watch rollout progress (status phase,
  conditions, `kubectl cnmsql status`).
- **How to rollback.** That in-place downgrade is **impossible** once the data
  dictionary is upgraded, and the only supported path back is restore from the
  pre-upgrade backup (design [008](008-physical-backup-recovery.md)) — including the
  exact restore recipe and why the pre-upgrade backup gate exists.
- **How to troubleshoot.** The `UpgradeBlocked`/`BackupRequired` condition; a
  mysqld that fails to start on the new image (config gating, Section D); a
  stalled rollout waiting on a replica's upgrade-complete signal (Section E); GR
  members stuck pre-protocol-bump (Section G); and reading the instance-manager
  refusal reason from `/status` when the adjacency guard trips.

## E2E testing

E2E coverage (extending the existing matrix; see `.github/mysql_versions.json`
and `.github/e2e-matrix-generator.py`) must exercise every case above:

- **Happy-path hops:** `8.0 → 8.4` and `8.4 → 9.x`, single-instance and
  multi-instance, asserting replica-first/primary-last ordering, no data loss,
  and that each instance reports upgrade-complete before the next rolls.
  **Multi-instance GR done:** a dedicated latest-Kubernetes matrix job co-loads
  all three published images and rolls one persistent three-member cluster
  through both hops, checking order, readiness, live versions, and data.
- **Group Replication upgrade**, asserting the communication-protocol bump fires
  only after all members are on the new version (Section G). **Done** in the
  multi-image upgrade job, including effective and requested-target status.
- **Rejected transitions:** `8.0 → 9.x` (skip) and any downgrade are refused at
  admission, **and** the instance-manager guard refuses if admission is bypassed
  (defense in depth). **Done:** `test/e2e/major_upgrade_test.go` applies a
  series-keyed catalog and asserts the webhook rejects skip, downgrade, and
  both-image-sources at apply time, and allows the adjacent hop. The
  instance-manager-refusal half remains to be added to the multi-image job.

The dedicated upgrade matrix entry supplies the co-resident images needed for
cross-series tests. The remaining cases below still need scenarios in that job
and retain unit/integration coverage in the meantime.
- **Backup gate:** upgrade with `backupBeforeUpgrade` default takes a backup
  first; with no object store configured it hard-fails with the expected
  condition; with `backupBeforeUpgrade: false` it proceeds without one.
- **Config gating:** a cluster carrying a sysvar/auth option removed in the target
  series upgrades cleanly (mysqld starts) rather than crash-looping mid-roll.
- **Rollback:** restore from a pre-upgrade backup into a cluster on the old series
  after an upgrade, proving the documented recovery path.
