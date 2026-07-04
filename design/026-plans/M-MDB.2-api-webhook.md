# M-MDB.2 — API field + webhook

- **Status:** blocked (needs M-MDB.1 done)
- **Depends on:** M-MDB.1
- **Design refs:** §5, §5.1, §5.2, §5.3, §9.2
- **Risk:** LOW–MEDIUM — additive API field; default keeps existing objects identical.

## Objective

Add the immutable `spec.flavor` field to `Cluster`, a `status.flavor` echo, a
print column, a `ResolvedFlavor()` helper, and the webhook validation
(immutability + cross-field rules). Default `mysql` so every existing manifest and
stored object is byte-identical. Then replace the `TODO(M-MDB.2)` markers M-MDB.1
left with real `cluster.ResolvedFlavor()` calls.

## Background you must read first

- [../../api/v1alpha1/cluster_types.go](../../api/v1alpha1/cluster_types.go) —
  `ClusterSpec` and `ClusterStatus` structs.
- [../../api/v1alpha1/cluster_funcs.go](../../api/v1alpha1/cluster_funcs.go) —
  `ReplicationMode()` (~L322) is the pattern to mirror for `ResolvedFlavor()`.
- [../../internal/webhook/v1alpha1/cluster_webhook.go](../../internal/webhook/v1alpha1/cluster_webhook.go)
  — existing `ValidateCreate`/`ValidateUpdate`; find the replication-mode and
  GR-group-name immutability checks and add flavor checks alongside.
- [../../pkg/engine/engine.go](../../pkg/engine/engine.go) — mirror the `Flavor`
  const values exactly (`mysql`, `mariadb`).

## Tasks

### A. API types

1. In `cluster_types.go` add the `Flavor` string type with the kubebuilder enum
   marker and the two consts (design §5.1). Keep the API `Flavor` values equal to
   `engine.Flavor` values so conversion is a plain string cast.
2. Add the field to `ClusterSpec`:
   ```go
   // +kubebuilder:validation:Enum=mysql;mariadb
   // +kubebuilder:default:=mysql
   // +optional
   Flavor Flavor `json:"flavor,omitempty"`
   ```
3. Add `Flavor Flavor` to `ClusterStatus` (echo of the resolved value).
4. Add a print column marker on the `Cluster` type:
   `// +kubebuilder:printcolumn:name="Flavor",type=string,JSONPath=".status.flavor"`.
5. In `cluster_funcs.go` add `func (cluster *Cluster) ResolvedFlavor() Flavor`
   returning `spec.flavor` or `FlavorMySQL` when empty (mirror `ReplicationMode`).

### B. Regenerate

6. Run `make generate manifests` (deepcopy + CRD YAML). If `make` is unavailable,
   run `controller-gen` the same way the Makefile does. Verify the CRD schema now
   lists `flavor` with `default: mysql` and the print column, and that
   `zz_generated.deepcopy.go` compiles.

— checkpoint — `go build ./...` green; generated files updated and committed.

### C. Webhook

7. **Immutability** in `ValidateUpdate` (design §5.2): if
   `old.ResolvedFlavor() != new.ResolvedFlavor()` append
   `field.Invalid(spec.flavor, ..., "flavor is immutable")`.
8. **Cross-field** in both `ValidateCreate` and `ValidateUpdate` (design §5.3), for
   `ResolvedFlavor() == FlavorMariaDB`:
   - reject `ReplicationMode() == groupReplication` with the message in §5.3.
   - resolve the series (from `imageName` tag or `imageCatalogRef.series`) and
     reject a series that isn't valid for the flavor — a MySQL series (`8.0/8.4/9.x`)
     under mariadb, or a MariaDB series (`10.x/11.x/12.x`) under mysql. Use
     `engine.ForFlavor(...).UpgradeChain()` / `Series` from M-MDB.1 as the source
     of truth for "valid series for this flavor" rather than hardcoding regexes.
9. Add webhook table tests in
   [../../internal/webhook/v1alpha1/cluster_webhook_test.go](../../internal/webhook/v1alpha1/cluster_webhook_test.go):
   flavor immutable (mysql→mariadb rejected, empty→mysql allowed), mariadb+GR
   rejected, mariadb+MySQL-series rejected, mysql+MariaDB-series rejected, and the
   happy paths.

### D. Advisory catalog flavor (design §9.2, resolved decision 2)

10. Add optional non-gating `Flavor` to `ImageCatalogSpec` /
    `ClusterImageCatalogSpec`. It only improves the admission error message
    ("catalog X is a mariadb catalog, cluster is mysql"); it must **not** gate
    resolution. Regenerate CRDs.

### E. Replace M-MDB.1 TODOs

11. `grep -rn "TODO(M-MDB.2)"` and replace each hard-coded
    `engine.ForFlavor(engine.FlavorMySQL)` with `engine.ForFlavor(engine.Flavor(cluster.ResolvedFlavor()))`.
    Set the instance-manager `CNMSQL_FLAVOR` env value from `cluster.ResolvedFlavor()`.
12. Populate `status.flavor` from `ResolvedFlavor()` in the reconcile status update.

— checkpoint — `go test ./...` green (existing MySQL clusters, with no `flavor`,
still resolve to `mysql`; no expectation edits for MySQL paths). `gofmt`/`go vet` clean.

## Acceptance criteria

- [ ] `spec.flavor` enum field, default `mysql`, print column, `status.flavor` echo.
- [ ] `ResolvedFlavor()` helper; empty ⇒ `mysql`.
- [ ] Webhook enforces immutability + rejects mariadb+GR and flavor/series mismatch.
- [ ] Advisory `flavor` on catalogs (non-gating).
- [ ] All `TODO(M-MDB.2)` markers resolved; CRDs regenerated & committed.
- [ ] Full suite green; existing MySQL manifests unaffected.

## Status log

### 2026-07-04 — (implementation in progress)
- state: in progress
- did: plan authored.
- did: Task A completed — Flavor type with kubebuilder enum marker, spec.flavor field (default mysql, optional), status.flavor echo, print column on Cluster type, ResolvedFlavor() helper.
- did: Task B completed — make generate manifests green, go build green, CRD includes flavor with default: mysql and print column.
- did: Task C completed — flavor immutability in ValidateUpdate, cross-field validation (mariadb+GR rejected, flavor/series mismatch), webhook table tests (12 cases passing).
- did: Task D completed — advisory Flavor on ImageCatalogSpec (non-gating).
- did: Task E completed — all TODO(M-MDB.2) markers replaced. CNMSQL_FLAVOR env resolved from cluster.ResolvedFlavor() (with nil guard for init containers). Instance runner/join read flavor from env. pool.ControlConfig accepts hasAdminInterface bool (breaks engine→pool cycle). cluster_funcs.go threads engine.CheckUpgrade. status.flavor populated by controller reconciliation.
- did: go test ./... all green, gofmt/go vet clean, lint 0 issues.
- next: M-MDB.3 — MariaDB bootstrap, my.cnf rendering, GR-free semi-sync config.
- blockers: none
- verify: go test ./... — all green, zero expectation edits for MySQL paths.
