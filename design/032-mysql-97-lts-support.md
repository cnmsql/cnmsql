# MySQL 9.7 LTS Support (hard cut of the 9.x innovation line) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> Updates design/024-major-version-upgrade.md (its machinery is unchanged; the supported chain it defined becomes `8.0 → 8.4 → 9.7`).

**Goal:** Replace the experimental MySQL 9.x innovation line with Percona Server for MySQL 9.7 LTS: new 9.7 instance images, series `9.7` as the third upgrade hop (`8.0 → 8.4 → 9.7`), and a hard cut — clusters running 9.1–9.6 are no longer upgradable in place.

**Architecture:** Two repos. The `containers` repo publishes the slim instance images from `images/versions.json`; swapping its `9.x` row for a `9.7` row (release repos, not testing) produces `ghcr.io/cnmsql/cnmsql-instance:9.7[-N]`. The `cnmsql` operator repo drops the `Series()` hack that collapsed every 9.x runtime into catalog series `9.0`, extends the upgrade chain to `9.7`, and keeps `"9.x"` only as a frozen image-tag → `9.6.0` alias so existing `:9.x`-pinned clusters keep reconciling. Group Replication protocol finalization is already version-generic (reads/writes concrete server versions, `internal/controller/cluster_mysql_upgrade.go:84`) — no code change; the e2e GR roll validates it.

**Tech Stack:** Go (kubebuilder/controller-runtime), bash + Docker (containers repo), GitHub Actions, Docusaurus docs.

**Spec:** The user's 9.7 analysis (conversation, 2026-09-27) plus the settled decision: **hard cut** (no legacy `9.0` alias). Key upstream facts verified 2026-09-27 at repo.percona.com and docs.percona.com:
- `ps-97-lts` release carries PS `9.7.1-1` (GA) for bookworm/jammy/noble/trixie.
- `pxb-97-lts` release carries `percona-xtrabackup-97` `9.7.1~rc1` — **PXB 9.7 is still RC upstream**; the image installs it and picks up the GA build automatically on the next rebuild.
- PXB 9.7 backs up only 9.7 servers (not 8.0/8.4, not ≤9.6) — cross-series physical restore is upstream-impossible, which is also why the hard cut is safe to enforce.

## Global Constraints

- Exact image matrix values: `version: "9.7"`, `ps: "ps-97-lts"`, `pxb: "pxb-97-lts"`, `pxbPackage: "percona-xtrabackup-97"`, `component: "release"`, `serverVersion: "9.7.1"`, base `debian:bookworm-slim`.
- `UpgradeSeriesChain = [{8,0}, {8,4}, {9,7}]`; `Series()` returns plain `major.minor` (no 9.x collapse). 9.0–9.6 runtimes therefore resolve to unsupported series — that rejection is the hard cut, not a bug.
- Keep `mysqlDefaultServerVersion("9.x") == "9.6.0"` (frozen alias): existing clusters pinned to `:9.x` images must keep resolving their server version and reconciling.
- Never edit `config/crd/bases/*`, `config/rbac/role.yaml`, `**/zz_generated.*`, `PROJECT`, `config/webhook/manifests.yaml` by hand — regenerate with `make manifests generate`.
- Commits: conventional, lowercase, casual, no body, no co-author (e.g. `feat: mysql 9.7 lts series`).
- Do NOT commit `config/manager/kustomization.yaml` — it carries a pre-existing local-dev change (`example.com/cnmsql:v0.0.1`), unrelated to this work.
- Integration/e2e steps need Docker (integration: real Percona containers; e2e: Kind). Unit tests (`make test`) never do.
- MariaDB is untouched: `Series()` is shared, but plain major.minor is exactly what MariaDB already expects; the MariaDB chain lives in `engine.checkUpgradeChain` and its own tests.

## Review Focus

1. **Existing `:9.x`-pinned clusters must keep reconciling** after the operator upgrade — `resolveServerVersion` (`internal/controller/cluster_plan.go:531`) still needs the `"9.x"` tag alias. Pinned by the `{"9.x", "9.6.0", false}` row kept in `TestMySQLDefaults` (Task 3).
2. **A 9.6 data dir restarted on 9.6 must not trip the instance-manager guard** — same-series no-op must survive the collapse removal. Pinned by the `"9.6.0" → "9.6.0"` guard case (Task 2).
3. **Hard-cut rejections must say why** — a 9.6 → 9.7 attempt must fail with `unsupported source MySQL series 9.x…`, and 8.0 → 9.7 with the skip message naming 8.4. Pinned by unit tests asserting errors are non-nil for exactly those transitions (Task 2) and the e2e admission spec (Task 6).
4. **PXB 9.7 is an RC** — physical backup + join must actually work on 9.7, not just initdb. Pinned by running the integration suite on the 9.7 flavor (Task 5).
5. **CI lanes and image co-loading must use the new tag** — `.github/mysql_versions.json` drives `e2e-matrix-generator.py` lane names (`flavor-MySQL-9.7`) and the major-upgrade job co-loads `8.0/8.4/9.7`. Pinned by the e2e suite list changes (Task 6); a stale list fails at image pull in CI.

---

### Task 1: containers repo — 9.7 image matrix, build and verify

**Files:**
- Modify: `images/versions.json` (containers repo)
- Modify: `README.md` (containers repo, only if it lists the version matrix — grep first)

**Interfaces:**
- Produces: `ghcr.io/cnmsql/cnmsql-instance:9.7-<patch>` and moving tag `ghcr.io/cnmsql/cnmsql-instance:9.7` — consumed by every later task via `instanceImageFor("9.7")`.

- [ ] **Step 1: Replace the 9.x row in `containers/images/versions.json`**

Delete the `9.x` row (freezes 9.x: existing `9.x-5` and moving `9.x` tags stay in GHCR untouched) and add:

```json
  {
    "version": "9.7",
    "base": "debian:bookworm-slim",
    "ps": "ps-97-lts",
    "pxb": "pxb-97-lts",
    "pxbPackage": "percona-xtrabackup-97",
    "component": "release",
    "serverVersion": "9.7.1"
  }
```

- [ ] **Step 2: Check the containers README for version references**

`grep -n "9.x\|9\.6" README.md` — update any matrix/notes lines to 9.7 the same way.

- [ ] **Step 3: Build and verify locally**

Run (from the containers repo root; Docker required):
```bash
./images/build.sh 9.7
docker run --rm --entrypoint /bin/sh cnmsql-instance:9.7 -c 'mysqld --version && xtrabackup --version'
```
Expected: build + `check-tools.sh` pass; `mysqld` prints `9.7.1`, `xtrabackup` prints a `9.7.1~rc1` version string. If apt cannot resolve the repos, re-verify `percona-release enable-only ps-97-lts release` / `percona-release enable pxb-97-lts release` names before debugging anything else.

- [ ] **Step 4: Commit (containers repo)**

```bash
git add images/versions.json
git commit -m "feat: build mysql 9.7 lts instance images"
```

- [ ] **Step 5: Publish**

Push to a branch and open a PR first (CI builds + checks, no push). Merge to `main` makes `.github/workflows/build.yml` publish `9.7-<auto-patch>` + moving `9.7` to GHCR. Note in the PR description that PXB is 9.7.1~rc1 until Percona ships GA; the next patch rebuild picks the GA package up.

---

### Task 2: cnmsql — version.go hard cut + guard tests

**Files:**
- Modify: `pkg/management/mysql/version/version.go:71-93`
- Modify: `pkg/management/mysql/version/version_test.go:108-140` (TestCheckUpgrade)
- Modify: `pkg/management/mysql/instance/upgrade_guard_test.go:41-53`

**Interfaces:**
- Produces: `version.Series()` without the 9.x collapse; `version.UpgradeSeriesChain = [{8,0},{8,4},{9,7}]`. All later tasks consume these.

- [ ] **Step 1: Update the failing tests first**

In `version_test.go` TestCheckUpgrade, replace the case table with:

```go
		{"8.0.36", "8.0.40", false},  // patch bump within a series
		{"8.0.36", "8.4.3", false},   // single hop forward
		{"8.4.3", "9.7.1", false},    // single hop to the 9.7 LTS
		{"8.4.3", "9.6.0", true},     // 9.x innovation is no longer a supported target
		{"9.6.0", "9.7.1", true},     // hard cut: legacy 9.x innovation cannot upgrade in place
		{"9.0.1", "9.7.1", true},     // hard cut: legacy 9.x innovation cannot upgrade in place
		{"8.0.36", "9.7.1", true},    // skips 8.4
		{"8.0.36", "9.0.1", true},    // 9.x innovation is not a supported target
		{"9.7.1", "9.6.0", true},     // downgrade / unsupported target
		{"9.0.1", "8.4.3", true},     // unsupported source (legacy innovation)
		{"8.4.3", "8.0.36", true},    // downgrade
		{"5.7.44", "8.0.36", true},   // source series outside chain
		{"8.4.3", "10.0.0", true},    // target series outside chain
```

In `upgrade_guard_test.go`, replace the case table with:

```go
		{"same series patch bump", "8.0.36", "8.0.40", false},
		{"9.7 same series patch bump", "9.7.0", "9.7.1", false},
		{"single hop forward", "8.0.36", "8.4.3", false},
		{"second hop to 9.7 lts", "8.4.3", "9.7.1", false},
		{"9.6 data dir restarts on same version", "9.6.0", "9.6.0", false},
		{"legacy 9.6 data dir cannot upgrade to 9.7", "9.6.0", "9.7.1", true},
		{"skips a series", "8.0.36", "9.7.1", true},
		{"downgrade", "8.4.3", "8.0.36", true},
		{"downgrade 9.7 to 9.6", "9.7.1", "9.6.0", true},
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./pkg/management/mysql/version/... ./pkg/management/mysql/instance/ -run 'TestCheckUpgrade|TestDataDir' -v
```
Expected: FAIL — current `Series()` maps 9.6.0 → 9.0 so the hard-cut rows pass/flip incorrectly.

- [ ] **Step 3: Implement the hard cut in `version.go`**

Replace the `Series()` body and comment (version.go:71-82) with:

```go
// Series returns the major.minor release series of the version, with the patch
// component zeroed. MySQL upgrades are reasoned about per series (8.0, 8.4,
// 9.7), not per patch.
func (v Version) Series() Version {
	return Version{Major: v.Major, Minor: v.Minor}
}
```

Replace `UpgradeSeriesChain` (version.go:84-93) with:

```go
var UpgradeSeriesChain = []Version{
	{Major: 8, Minor: 0},
	{Major: 8, Minor: 4},
	{Major: 9, Minor: 7},
}
```

(The doc comment above the chain keeps its wording; adjust "e.g. 8.0 -> 9.0" to "e.g. 8.0 -> 9.7".)

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./pkg/management/mysql/version/... ./pkg/management/mysql/instance/ -v
```
Expected: PASS, including the pre-existing `{"9.0.1", "8.4.3", …}` rows that now fail as "unsupported source" instead of "downgrade".

- [ ] **Step 5: Run the whole unit suite to catch collapse-dependents**

```bash
go test ./...
```
Expected: PASS. `cluster_funcs_test.go:706` (9.0 → 8.0 rejected) still passes — the failure message changes to "unsupported source", the assertion only checks non-empty. `cluster_spec_webhook_test.go:99-104` (skip 8.0 → 9.0) still passes for the same reason.

- [ ] **Step 6: Commit**

```bash
git add pkg/management/mysql/version/version.go pkg/management/mysql/version/version_test.go pkg/management/mysql/instance/upgrade_guard_test.go
git commit -m "feat: mysql 9.7 lts series replaces the 9.x innovation alias"
```

---

### Task 3: cnmsql — default server versions + engine tests

**Files:**
- Modify: `pkg/engine/engine.go:578-591` (mysqlDefaultServerVersion)
- Modify: `pkg/engine/engine_facet_test.go:40` (versionMatrix), `:506-514` (TestMySQLDefaults table)
- Modify: `pkg/engine/engine_logical_test.go:100,211`

**Interfaces:**
- Produces: `DefaultServerVersion("9.7") == "9.7.1"` and `DefaultServerVersion("9.x") == "9.6.0"` (frozen alias) — consumed by `resolveServerVersion` (`internal/controller/cluster_plan.go:531`).

- [ ] **Step 1: Update `mysqlDefaultServerVersion` in `engine.go`**

```go
func mysqlDefaultServerVersion(tag string) (string, error) {
	switch tag {
	case "8.0":
		return "8.0.46", nil
	case "8.4":
		return "8.4.0", nil
	case "9.7":
		return "9.7.1", nil
	case "9.x":
		// Frozen: the containers repo no longer builds the 9.x innovation
		// line. Kept only so clusters pinned to the last published :9.x
		// image keep resolving their server version.
		return "9.6.0", nil
	default:
		return "", fmt.Errorf("unsupported MySQL series %q", tag)
	}
}
```

- [ ] **Step 2: Update the tests**

- `engine_facet_test.go:40`: `versionMatrix = []string{"8.0.22", "8.0.26", "8.4.0", "9.0.0", "9.7.1"}`.
- `engine_facet_test.go:79`: the CheckUpgrade parity pair `{"8.4.0", "9.0.0"}` → `{"8.4.0", "9.7.0"}` (it's an engine-vs-`version.CheckUpgrade` parity assertion, but the pair should be a genuinely allowed hop under the hard cut).
- `TestMySQLDefaults` table: keep `{"9.x", "9.6.0", false}` (frozen alias) and add `{"9.7", "9.7.1", false}`.
- `engine_logical_test.go:100`: `{"mysql 9.7", FlavorMySQL, "9.7.1", []string{"--source-data=2"}, []string{"--master-data=2"}}`.
- `engine_logical_test.go:211`: `[]string{"8.0.46", "8.4.11", "9.7.1"}`.

- [ ] **Step 3: Run tests to verify they pass**

```bash
go test ./pkg/engine/ -v
```
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add pkg/engine/engine.go pkg/engine/engine_facet_test.go pkg/engine/engine_logical_test.go
git commit -m "feat: resolve mysql 9.7 default server version"
```

---

### Task 4: cnmsql — API comments, samples, generated artifacts

**Files:**
- Modify: `api/v1alpha1/cluster_types.go:681` and `api/v1alpha1/imagecatalog_types.go:46` — comment text `(e.g. "8.0", "8.4", "9.0")` → `(e.g. "8.0", "8.4", "9.7")`
- Modify: `config/samples/mysql_v1alpha1_imagecatalog.yaml:14` — `series: "9.0"` → `series: "9.7"` and its image `…:9.x` → `…:9.7`
- Regenerate: `config/crd/bases/*`, `dist/install.yaml`, `docs/src/api-reference.md`

**Interfaces:**
- Consumes: nothing new. Produces: regenerated CRDs whose series descriptions say 9.7, and the committed `dist/install.yaml`.

- [ ] **Step 1: Check the other samples for 9.x/9.0 references**

`grep -n "9\.x\|\"9\.0\"" config/samples/` — fix any hits the same way (imagecatalog sample is the known one; cluster/clusterimagecatalog samples were clean at planning time).

- [ ] **Step 2: Regenerate**

```bash
make manifests generate
make build-installer
make api-docs
```

- [ ] **Step 3: Merge the generated API reference**

`make api-docs` writes `docs/src/api-reference-generated.md`. Merge its changed `series` descriptions into `docs/src/api-reference.md` (also update the sample catalog YAML inside that doc: `series: "9.0"` → `"9.7"`, image `…:9.x` → `…:9.7` at ~lines 739-740 and 1454-1455), then delete `api-reference-generated.md`.

- [ ] **Step 4: Verify regenerated files contain 9.7 and no stray 9.0-in-series text**

`grep -n '"8.4", "9.7"' config/crd/bases/*.yaml dist/install.yaml` — expect hits in all regenerated CRDs.

- [ ] **Step 5: Commit**

```bash
git add api/v1alpha1 config/samples config/crd dist docs/src/api-reference.md
git commit -m "chore: document series 9.7 in crds, samples and api reference"
```

---

### Task 5: cnmsql — integration matrix on 9.7

**Files:**
- Modify: `test/integration/flavors_test.go:64-69`

**Interfaces:**
- Consumes: the published `ghcr.io/cnmsql/cnmsql-instance:9.7` image (Task 1). Produces: the 9.7 integration flavor.

- [ ] **Step 1: Replace the 9.x flavor entry**

```go
	{
		name:              "9.7",
		version:           "9.7.1",
		hasAdminInterface: true,
		joinSupported:     true,
	},
```

- [ ] **Step 2: Run the integration suite on 9.7 (Docker required)**

```bash
E2E_MYSQL_VERSION=9.7 make test-integration
```
Expected: PASS. Watch two spots specifically: (a) `groupreplication_integration_test.go:132` and `replication_integration_test.go:163-168` create the replication user `WITH mysql_native_password` — if 9.7 dropped the plugin, switch those two statements to `WITH caching_sha2_password` and adjust the nearby comments; (b) `logical_integration_test.go` cross-series round trip (8.0 → 9.7) must still pass.

- [ ] **Step 3: Commit**

```bash
git add test/integration/flavors_test.go
git commit -m "test: run the integration matrix against mysql 9.7"
```

---

### Task 6: cnmsql — e2e suite + CI matrix

**Files:**
- Modify: `test/e2e/images.go:96`, `test/e2e/e2e_suite_test.go:208,221`
- Modify: `test/e2e/major_upgrade_test.go` (catalog manifests :22-40 and :67-82, skip spec :143-156, GR hops :284-291, defensive spec :385-389)
- Modify: `.github/mysql_versions.json`, `hack/e2e.sh:59,66`, `.github/e2e-matrix-generator.py:80` (comment)

**Interfaces:**
- Consumes: `ghcr.io/cnmsql/cnmsql-instance:9.7` (Task 1). Produces: CI lanes `flavor-MySQL-9.7` and a major-upgrade job rolling `8.0 → 8.4 → 9.7`.

- [ ] **Step 1: Update version lists**

- `images.go:96`: `return []string{"8.0", "8.4", "9.7"}`
- `e2e_suite_test.go:208`: `versions = append(versions, "8.0", "8.4", "9.7")`
- `e2e_suite_test.go:221`: `for _, v := range []string{"8.0", "8.4", "9.7"}`
- `.github/mysql_versions.json`: last row becomes `{"version": "9.7", "serverVersion": "9.7.1"}`

- [ ] **Step 2: Update major_upgrade_test.go**

- `upgradeCatalogManifest` (:22-40): replace the `- series: "9.0"` entry with `- series: "9.7"`, and add a fourth entry `- series: "9.0"` (any image ref — admission specs never pull) for the hard-cut spec in step 3.
- `majorUpgradeCatalogManifest` (:67-82): `series: "9.0"` → `series: "9.7"`, `instanceImageFor("9.x")` → `instanceImageFor("9.7")`.
- Skip spec (:148-149): `expectApplyRejected(cluster, catalogClusterManifest(cluster, testNamespace, "9.7"), "8.4")`.
- GR hop (:290): `{series: "9.7", serverPrefix: "9.7.", image: instanceImageFor("9.7")}`.
- Defensive catalog mutation (:385-389): `instanceImageFor("9.x")` → `instanceImageFor("9.7")` and the search/replace string `- series: "9.0"\n      image: ` → `- series: "9.7"\n      image: `.

- [ ] **Step 3: Add the hard-cut admission spec** (in the "MySQL major-version upgrade admission" Describe, after the skip/hop spec)

```go
	It("rejects a legacy 9.x innovation series as an upgrade source", func() {
		By("creating a cluster pinned to the legacy 9.0 series")
		applyManifest(cluster, catalogClusterManifest(cluster, testNamespace, "9.0"))
		DeferCleanup(func() { deleteCluster(cluster) })

		By("rejecting the hop to 9.7: the 9.x innovation line is hard-cut")
		expectApplyRejected(cluster, catalogClusterManifest(cluster, testNamespace, "9.7"), "unsupported source")
	})
```

- [ ] **Step 4: Update the runner help and matrix comment**

- `hack/e2e.sh:59,66`: `8.0 | 8.4 | 9.x` → `8.0 | 8.4 | 9.7`, and the example lines `--mysql 9.x` → `--mysql 9.7`.
- `e2e-matrix-generator.py:80`: comment `so 9.6.0 sorts above 8.4.0 above 8.0.46` → `so 9.7.1 sorts above 8.4.0 above 8.0.46`.

- [ ] **Step 5: Verify the matrix generator still emits sane lanes**

```bash
python3 .github/e2e-matrix-generator.py -m pull_request | python3 -c 'import json,sys; m=json.load(sys.stdin); print([i["id"] for i in m["include"]])'
```
Expected: ids include `major-upgrade`, `flavor-MySQL-8.0`, `flavor-MySQL-8.4`, `flavor-MySQL-9.7` — no `9.x`.

- [ ] **Step 6: Compile the e2e suite**

```bash
go build -tags e2e ./test/e2e/... && go vet -tags e2e ./test/e2e/...
```
Expected: clean. (Full e2e runs in CI or via `./hack/e2e.sh` on a dedicated Kind cluster; the GR roll needs `E2E_MAJOR_UPGRADE=true`.)

- [ ] **Step 7: Commit**

```bash
git add test/e2e .github/mysql_versions.json hack/e2e.sh .github/e2e-matrix-generator.py
git commit -m "test: e2e matrix rolls 8.0 to 8.4 to 9.7"
```

---

### Task 7: cnmsql — docs, policy, instructions

**Files:**
- Modify: `docs/src/instance-images.md` (:33 matrix row, :47 moving-tag list, :116 tags, :150 known limitation)
- Modify: `docs/src/major-version-upgrade.md` (:117 chain text + new hard-cut/migration section)
- Modify: `docs/src/index.md:54`, `docs/src/group-replication.md:82,540`, `docs/src/mariadb.md:86,223`, `docs/src/logical-backups.md:14,83,355,374`
- Modify: `INSTRUCTION.md` (:14 D2, :24 D12, :34 features, new D19) and `AGENTS.md` where it repeats them
- Modify: `design/INDEX.md` (add this plan's entry)

**Interfaces:**
- Consumes: everything above. Produces: the LTS-only policy statement and the 9.x migration story.

- [ ] **Step 1: Update the docs pages**

- `instance-images.md`: matrix row → `| 9.7 | debian:bookworm-slim | ps-97-lts | pxb-97-lts | LTS line. |`; moving tags `(8.0, 8.4, 9.7)`; tags list `8.0-5, 8.4-5, 9.7-N`; DELETE the "testing channel" known limitation (:150); add a short note: 9.x innovation images are frozen at their last 9.6 build and no longer rebuilt, and PXB 9.7 is RC upstream until Percona ships GA (the image picks it up on rebuild).
- `major-version-upgrade.md`: chain everywhere `8.0 → 8.4 → 9.7`; new section "Legacy 9.x innovation (hard cut)": clusters on 9.1–9.6 are rejected as an upgrade source (`unsupported source series`); PXB 9.7 cannot restore 9.6 physical backups; the migration path is a logical backup (`Backup.spec.method: logical`) → fresh 9.7 cluster → `LogicalRestore` / `bootstrap.initdb.import`. State the policy: cnmsql supports LTS series only (plus 8.0 while Percona publishes it; upstream MySQL 8.0 went EOL in April 2026). Future innovation releases (9.8+, 10.x) are unsupported until they land an LTS — no more catalog alias.
- `index.md:54`: "Percona Server 8.0, 8.4, and 9.7 LTS".
- `group-replication.md:82`: "8.4, and 9.7 are supported" (and :540 same substitution).
- `mariadb.md:86,223`: "9.x series" → "9.7 series" where it describes flavor/series validation.
- `logical-backups.md`: version-wording substitutions (`9.x` → `9.7`, tag lists `9.x-5` → `9.7-N`), and :14 keeps the cross-series dump claim — reword "fresh 9.x cluster" to "fresh 9.7 cluster".

- [ ] **Step 2: Update INSTRUCTION.md / AGENTS.md and add the policy decision**

In `INSTRUCTION.md`: D2 → "(8.0, 8.4, 9.7)", D12 → "(8.0, 8.4, 9.7)", Features → "Version 8.0, 8.4, and 9.7 (Percona Server for MySQL) LTS series only". Add row D19: `| D19 | LTS series only — 8.0, 8.4, 9.7. Innovation lines are unsupported; the 9.x line is hard-cut (no in-place upgrade from 9.1–9.6) | Removes the 9 → 9.0 catalog-alias hack; see design/032 |`. Apply the same line edits wherever `AGENTS.md` repeats D2/D12/Features. Add the design/INDEX.md entry for 032 (one line, `accepted`, mention: chain `8.0 → 8.4 → 9.7`, hard cut, frozen 9.x images, PXB 9.7 RC caveat).

- [ ] **Step 3: Build the docs site**

```bash
npm run build
```
Expected: clean Docusaurus build.

- [ ] **Step 4: Commit**

```bash
git add docs/src INSTRUCTION.md AGENTS.md design/INDEX.md
git commit -m "docs: mysql 9.7 lts policy, migration path for 9.x users"
```

---

### Task 8: cnmsql — final sweep and full verification

**Files:**
- Modify: whatever the sweep finds (docs/code/comments only — historical files in `design/002…031` are exempt records and stay as-is)

- [ ] **Step 1: Sweep both repos for stragglers**

```bash
rg -n '"9\.x"|9x-innovation|"9\.0"|9\.6\.0' --glob '!design/*.md' --glob '!dist/**'
```
Expected: no functional hits. `mysqlDefaultServerVersion`'s frozen `"9.x"` alias (engine.go) and the `"9.0"` string in the hard-cut e2e spec are the only allowed matches.

- [ ] **Step 2: Regenerate + lint + full unit suite**

```bash
make manifests generate lint-fix test
```
Expected: no diff from `make manifests generate` (idempotency), lint clean, `go test ./...` PASS.

- [ ] **Step 3: Commit any sweep fixes**

```bash
git add -A ':!config/manager/kustomization.yaml'
git commit -m "chore: mysql 9.7 leftovers"
```
(Never stage `config/manager/kustomization.yaml` — pre-existing local change.)

- [ ] **Step 4: E2E on a dedicated Kind cluster (explicit run, not CI)**

```bash
./hack/e2e.sh --mysql 9.7
E2E_MAJOR_UPGRADE=true ./hack/e2e.sh --label major-upgrade
```
Expected: full suite green on 9.7; the GR roll completes `8.0 → 8.4 → 9.7` and finalizes the communication protocol (`assertGroupCommunicationProtocol` with prefix `9.7.`).

---

## Explicit non-goals (decided at planning)

- No admission warning system for the hard cut: 9.0-series creates are mechanically still possible with the frozen `:9.x` image (catalog series is user data); only *transitions* are guarded. The docs carry the policy.
- No change to GR protocol finalization, flavor validation (`cluster_funcs.go:728` checks `Major == 8 || 9` — 9.7 still matches), or the MariaDB chain.
- No deletion of existing `9.x`/`9.x-5` GHCR tags (freeze in place, users may pin them).
- Historical design docs (024, INDEX entry) keep their original wording; this plan documents the change.
