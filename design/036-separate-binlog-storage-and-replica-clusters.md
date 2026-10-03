# 036 — Separate binlog archive storage and replica clusters

Status: proposed (2026-10-03). Phase 1 accepted for implementation; phases 2
and 3 are recorded here and wait for their own go-ahead.

Issue #65.

## Problem

Two limits are listed as future work in `docs/src/pitr.md`:

1. **Binlogs share the base-backup store.** `spec.backup.objectStore` holds the
   base backups (`<path>/<cluster>/<backupID>/...`) and the continuous binlog
   archive (`<path>/<cluster>/binlogs/<server_uuid>/...` plus `_index.json`).
   The archive cannot go to its own bucket, credentials, storage class,
   retention rules or object-lock settings.
2. **No cluster can follow another one.** `spec.replica` exists in the API
   since 001 but `unsupportedReason` rejects it. A cluster can be recovered once
   from an archive (011), but it cannot keep applying the source's changes as a
   standby (CNPG's "replica cluster", fed by streaming and/or the archive).

## Phases

| Phase | Scope | Status |
|-------|-------|--------|
| 1 | Separate binlog archive object store | accepted, implement now |
| 2 | MySQL replica clusters: live channel + archive follow | proposed |
| 3 | MariaDB replica clusters | proposed, sketch only |

Each phase is its own PR (phase 2 is split in three). Phase 1 touches no replica
code and is useful on its own.

---

## Phase 1 — Separate binlog archive store

### Goal

- The binlog archive can live in an object store of its own.
- Unset, nothing changes: the archive stays in `spec.backup.objectStore`.
- PITR from a `Backup` CR and raw-S3 recovery find the binlogs in that store.
- Retention and reclaim act on each store for what it holds.

### Non-goals

- Moving an existing archive between stores. The operator never copies
  objects.
- A path-only override inside the same store. A separate `S3ObjectStore` covers
  it (same bucket, other path) and also covers other buckets and credentials.
- `spec.binlogStorage` (a local PVC for binlogs). It stays blocked; it is about
  the instance's disk, not the archive.

### Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| S1 | New `spec.backup.continuousArchiving.objectStore`, defaulting to `spec.backup.objectStore` | Lives next to the other archiving knobs; no change for existing clusters |
| S2 | New `ExternalCluster.binlogObjectStore`, defaulting to `objectStore` | Raw-S3 recovery (and later replica clusters) need the archive's location and credentials |
| S3 | New `Backup.status.binlogObjectStore`, recorded when the backup runs | A `Backup` must keep pointing at the archive it was anchored to after the cluster spec changes, like `status.objectStore` does for the base backup |
| S4 | Same key layout in the binlog store: `<path>/<cluster>/binlogs/...` | One key builder, one planner, one retention code path; a dedicated store just has nothing else beside `binlogs/` |
| S5 | No immutability rule. The operator records the effective destination in `status.continuousArchiving.destination` and warns when it changes | Changing `spec.backup.objectStore` already moves the archive today without a guard; a rule on the new field only would be inconsistent. The warning tells the user what the move costs |
| S6 | The instance Pod's `cnmsql_S3_*` env describes the binlog store | The archiver is the only consumer of object-store credentials in the instance Pod; base backups are uploaded by the worker Job |
| S7 | The restore Job gets a second env set, `cnmsql_BINLOG_S3_*`, only when the binlog store differs from the base-backup store | The restore Job reads both; absent means "same store", so the common case renders exactly what it renders today |

### API

```go
type ContinuousArchivingConfiguration struct {
    // ... existing fields ...

    // ObjectStore is where the binary-log archive is written. When unset, the
    // archive goes to spec.backup.objectStore next to the base backups. The
    // archive keeps the same layout in either store:
    // <path>/<cluster>/binlogs/.
    // +optional
    ObjectStore *S3ObjectStore `json:"objectStore,omitempty"`
}

type ExternalCluster struct {
    // ... existing fields ...

    // BinlogObjectStore is where the external cluster's binary-log archive
    // lives, when it is not in ObjectStore. Recovery reads base backups from
    // ObjectStore and binlogs from here.
    // +optional
    BinlogObjectStore *S3ObjectStore `json:"binlogObjectStore,omitempty"`
}

type BackupStatus struct {
    // ... existing fields ...

    // BinlogObjectStore records the binary-log archive store of the cluster
    // when the backup ran. Point-in-time recovery from this backup replays
    // binlogs from it. Unset on backups taken before this field existed, which
    // means the archive is in ObjectStore.
    // +optional
    BinlogObjectStore *S3ObjectStore `json:"binlogObjectStore,omitempty"`
}

type ContinuousArchivingStatus struct {
    // ... existing fields ...

    // Destination is the archive location in use, "<endpoint>/<bucket>/<path>"
    // (endpoint empty for AWS). A change is reported with an ArchiveMoved
    // Warning event.
    // +optional
    Destination string `json:"destination,omitempty"`
}
```

Helpers in `api/v1alpha1`:

- `(*Cluster).BinlogObjectStore() *S3ObjectStore` returns
  `continuousArchiving.objectStore` when set, otherwise `backup.objectStore`.
- `(*ExternalCluster).GetBinlogObjectStore() *S3ObjectStore` returns
  `binlogObjectStore` when set, otherwise `objectStore`.

### Validation

- Archiving keeps requiring `spec.backup.objectStore`: base backups anchor
  every recovery, so an archive without a base store is not useful. The webhook
  already rejects `continuousArchiving.enabled` without it, and
  `IsArchivingEnabled()` already checks it; both stay as they are.
- `continuousArchiving.objectStore` and `ExternalCluster.binlogObjectStore` get
  the same defaulting (`SetDefaults`) and webhook validation
  (`S3ObjectStore.Validate`) as the other object stores.
- `continuousArchiving.objectStore` without `enabled: true` is accepted and
  ignored, like the other archiving fields.
- A recovery that needs binlogs (`recoveryTarget` set) from an external cluster
  needs `binlogObjectStore` or `objectStore`; the existing "has no objectStore"
  check already covers the second, so nothing new is needed.

### Resolution of the binlog store per consumer

| Consumer | Today | Phase 1 |
|----------|-------|---------|
| Archiver (instance Pod env, `runEnv`) | `backup.objectStore` | `cluster.BinlogObjectStore()` |
| `Backup` controller status | `status.objectStore` | also `status.binlogObjectStore = cluster.BinlogObjectStore()` when archiving is enabled, even when `backup.spec.objectStore` overrides the base store |
| Recovery from `Backup` CR (`resolveRecovery`) | backup store | `backup.status.binlogObjectStore`, falling back to the backup store |
| Raw-S3 recovery (`resolveRawS3Recovery`) | `ext.objectStore` | `ext.GetBinlogObjectStore()` |
| Recovery target guard (`cluster_backup_guard.go`) | `plan.Recovery.Store` | `plan.Recovery.BinlogStore` |
| PITR replay (`restore_pitr.go`) | `o.ObjectStore` / `o.Store` | new `o.BinlogObjectStore` / `o.BinlogStore`, defaulting to the base ones |
| Retention (`cluster_retention.go`) | one store | base backups listed and expired in the base store; binlogs, index read and binlog GC in the binlog store |
| Empty-destination guard (`checkBackupDestination`) | base store's cluster prefix must be empty | also the binlog store's cluster prefix, when it is a different location |
| Reclaim `Delete` (`cluster_backup_reclaim.go`) | `RemovePrefix` on the base store | `RemovePrefix` on both cluster prefixes, once when they are the same store and path |

A per-Backup `spec.objectStore` override moves only the base backup; the
archive is the cluster's. Today PITR from such a backup looks for binlogs in the
override, where they never were. Recording the cluster's binlog store in
`status.binlogObjectStore` fixes that as a side effect.

`recoveryPlan` gains `BinlogStore S3ObjectStore` and `BinlogStoreEnv
[]corev1.EnvVar`. When the binlog store equals the base store (same endpoint,
bucket and path, credentials ignored), `BinlogStoreEnv` is empty.

### Retention across two stores

`PlanRetention` already takes the base-backup list, the binlog list and the
index as separate inputs, so the planner does not change. `ApplyRetention` is
split in two calls (or takes two clients): expired base backups are deleted
from the base store, expired binlogs and the rewritten index go to the binlog
store. The order stays: base backups first, then binlogs, so a failure on the
binlog side never keeps an expired anchor alive and never deletes a binlog an
unexpired anchor still needs.

### Moving the archive

When `status.continuousArchiving.destination` differs from the effective
destination, the operator emits a Warning `ArchiveMoved` event naming both and
updates the status. In the new store the archiver finds no `_archive_status.json`
and ships every binlog still on the primary's disk, so the new archive starts at
the oldest local binlog. Base backups whose anchor is older than that can only
be recovered with the old store (raw-S3 recovery with `binlogObjectStore`
pointing at it). The docs recommend a new base backup right after a move.

### Credentials

- Instance Pod: `cnmsql_S3_*` and `cnmsql_S3_BUCKET`/`cnmsql_S3_PATH` render the
  binlog store. Setting `continuousArchiving.objectStore` (or changing its
  Secret references) changes the Pod template and rolls the instances, like any
  object-store change does today. Called out in the upgrade notes.
- Restore Job: the base store in `cnmsql_S3_*`, and the binlog store in
  `cnmsql_BINLOG_S3_*` when it differs. `objectstore` gains
  `ConfigFromEnvPrefix(prefix)`; the restore command reads the binlog config
  when `cnmsql_BINLOG_S3_BUCKET` is set and uses the base config otherwise.
- Operator: reads both stores' Secrets through `objectStoreConfig`, as today.

### Testing

Unit:

- `BinlogObjectStore()` / `GetBinlogObjectStore()` defaults.
- `IsArchivingEnabled()` unchanged when only `continuousArchiving.objectStore`
  is set (no base store means no archiving).
- `runEnv` renders the binlog store; unchanged output when it is unset.
- Recovery plans (Backup CR and raw S3): `BinlogStore` resolution, empty
  `BinlogStoreEnv` when the stores match, populated when they differ, and the
  fallback for a `Backup` without `status.binlogObjectStore`.
- Backup controller records `status.binlogObjectStore`, including for a
  Backup whose `spec.objectStore` overrides the base store.
- Retention with two fake stores: base backups deleted in one, binlogs and
  index rewritten in the other.
- Reclaim: two prefixes removed, one when they match.
- `ArchiveMoved` event and `destination` status on a change.
- Restore command picks the binlog env set when present.

E2E (existing SeaweedFS setup, a second bucket):

- Archive to a separate bucket, check that no `binlogs/` key lands in the base
  bucket, PITR to a `targetGTID` from the `Backup` CR with exact data
  assertions.
- Raw-S3 recovery with `objectStore` and `binlogObjectStore` on the external
  cluster entry.

### Docs

- `docs/src/pitr.md`: separate store in the diagram, components and integrator
  responsibilities; remove the "future work" line for storage (replica clusters
  stay listed until phase 2).
- `docs/src/backup-recovery.md`: configuring `continuousArchiving.objectStore`,
  raw-S3 recovery with `binlogObjectStore`, moving the archive.
- API reference regenerated.
- `INSTRUCTION.md`: decision D22.

### Phase 1 implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The binlog archive can live in an object store of its own, and every archive reader and writer (archiver, PITR, recovery guard, retention, reclaim, empty-destination guard) uses that store.

**Architecture:** One resolver per side: `Cluster.BinlogObjectStore()` for a running cluster, `Backup.status.binlogObjectStore` for a recovery from a Backup CR, `ExternalCluster.GetBinlogObjectStore()` for raw-S3 recovery. The key layout is unchanged, so only the store passed to the existing key builders changes. The restore Job learns about a second store through a `cnmsql_BINLOG_S3_*` twin of the existing env, rendered only when the locations differ.

**Tech Stack:** Go, kubebuilder markers, controller-runtime fake client, `httptest` S3 fakes, Ginkgo (api package and e2e), SeaweedFS on Kind.

**Spec:** this document, section "Phase 1 — Separate binlog archive store".

#### Global constraints

- No behaviour change when `continuousArchiving.objectStore` is unset: same Pod env, same restore Job env, same keys.
- Env var names: `cnmsql_BINLOG_S3_<NAME>` for every `cnmsql_S3_<NAME>` (built with `objectstore.BinlogEnvName`).
- "Same store" means same `Location()` (`<endpoint>/<bucket>/<path>`), credentials ignored.
- Kubernetes logging style, conventional commits (`feat:`, `test:`, `docs:`), lowercase, no body, no co-author.
- Never edit generated files by hand: run `make manifests generate` after API changes.
- After Go changes: `make lint-fix` and `make test`.

#### Review focus

1. A `Backup` taken **before** this change (no `status.binlogObjectStore`) still recovers with PITR from its base store. Pinned in Task 4.
2. A `Backup` with `spec.objectStore` overriding the base store records the **cluster's** archive store, not the override. Pinned in Task 5.
3. Two stores with the same bucket and path but different credentials count as one location: one env set, one `RemovePrefix`. Pinned in Tasks 4 and 6.
4. A path written with surrounding slashes (`/archive/`) is the same location as `archive`. Pinned in Task 1.
5. Retention never deletes a binlog through the base-store client, or a base backup through the binlog-store client. Pinned in Task 6.

---

#### Task 1: API fields and store helpers

**Files:**
- Modify: `api/v1alpha1/cluster_types.go` (`ContinuousArchivingConfiguration`, `ExternalCluster`, `ContinuousArchivingStatus`)
- Modify: `api/v1alpha1/backup_types.go` (`BackupStatus`)
- Modify: `api/v1alpha1/cluster_funcs.go` (helpers, `SetDefaults`, validation)
- Test: `api/v1alpha1/cluster_funcs_test.go`
- Regenerate: `make manifests generate`

**Interfaces:**
- Produces:
  - `func (cluster *Cluster) BinlogObjectStore() *S3ObjectStore`
  - `func (ext *ExternalCluster) GetBinlogObjectStore() *S3ObjectStore`
  - `func (store *S3ObjectStore) Location() string` (nil-safe, "" for nil)
  - `func (store *S3ObjectStore) SameLocation(other *S3ObjectStore) bool`
  - fields `ContinuousArchivingConfiguration.ObjectStore`, `ExternalCluster.BinlogObjectStore`, `BackupStatus.BinlogObjectStore`, `ContinuousArchivingStatus.Destination`

- [ ] **Step 1: Write the failing tests** (append to `api/v1alpha1/cluster_funcs_test.go`)

```go
var _ = Describe("Binlog archive object store", func() {
	base := &S3ObjectStore{Bucket: "backups", Path: "base"}
	archive := &S3ObjectStore{Bucket: "binlogs", Path: "archive"}

	It("falls back to the backup object store", func() {
		cluster := &Cluster{}
		Expect(cluster.BinlogObjectStore()).To(BeNil())
		cluster.Spec.Backup = &BackupConfiguration{ObjectStore: base}
		Expect(cluster.BinlogObjectStore()).To(Equal(base))
		cluster.Spec.Backup.ContinuousArchiving = &ContinuousArchivingConfiguration{Enabled: true}
		Expect(cluster.BinlogObjectStore()).To(Equal(base))
	})

	It("prefers continuousArchiving.objectStore", func() {
		cluster := &Cluster{}
		cluster.Spec.Backup = &BackupConfiguration{
			ObjectStore:         base,
			ContinuousArchiving: &ContinuousArchivingConfiguration{Enabled: true, ObjectStore: archive},
		}
		Expect(cluster.BinlogObjectStore()).To(Equal(archive))
	})

	It("does not enable archiving from the archive store alone", func() {
		cluster := &Cluster{}
		cluster.Spec.Backup = &BackupConfiguration{
			ContinuousArchiving: &ContinuousArchivingConfiguration{Enabled: true, ObjectStore: archive},
		}
		Expect(cluster.IsArchivingEnabled()).To(BeFalse())
	})

	It("resolves an external cluster's archive store", func() {
		ext := &ExternalCluster{Name: "prod", ObjectStore: base}
		Expect(ext.GetBinlogObjectStore()).To(Equal(base))
		ext.BinlogObjectStore = archive
		Expect(ext.GetBinlogObjectStore()).To(Equal(archive))
	})

	It("compares locations without credentials and with normalized paths", func() {
		a := &S3ObjectStore{Endpoint: "http://s3:8333/", Bucket: "b", Path: "/archive/",
			Credentials: S3Credentials{AccessKeyID: &SecretKeySelector{Name: "one", Key: "k"}}}
		b := &S3ObjectStore{Endpoint: "http://s3:8333", Bucket: "b", Path: "archive",
			Credentials: S3Credentials{AccessKeyID: &SecretKeySelector{Name: "two", Key: "k"}}}
		Expect(a.Location()).To(Equal("http://s3:8333/b/archive"))
		Expect(a.SameLocation(b)).To(BeTrue())
		Expect(a.SameLocation(&S3ObjectStore{Endpoint: "http://s3:8333", Bucket: "b", Path: "other"})).To(BeFalse())
		var none *S3ObjectStore
		Expect(none.Location()).To(Equal(""))
	})

	It("defaults the archive stores", func() {
		cluster := &Cluster{}
		cluster.Spec.Backup = &BackupConfiguration{
			ObjectStore:         &S3ObjectStore{Bucket: "backups"},
			ContinuousArchiving: &ContinuousArchivingConfiguration{ObjectStore: &S3ObjectStore{Bucket: "binlogs"}},
		}
		cluster.Spec.ExternalClusters = []ExternalCluster{{Name: "prod", BinlogObjectStore: &S3ObjectStore{Bucket: "x"}}}
		cluster.SetDefaults()
		Expect(cluster.Spec.Backup.ContinuousArchiving.ObjectStore.ForcePathStyle).NotTo(BeNil())
		Expect(cluster.Spec.ExternalClusters[0].BinlogObjectStore.SignatureVersion).To(Equal(SignatureVersionV4))
	})
})
```

Inside the same `Describe`, the webhook validation case:

```go
	It("validates the archive stores like the other object stores", func() {
		bad := func() *S3ObjectStore {
			sse := "rot13"
			return &S3ObjectStore{Bucket: "b", ServerSideEncryption: &sse}
		}
		cluster := &Cluster{Spec: ClusterSpec{
			ImageName: "percona/percona-server:8.0",
			Instances: 1,
			Storage:   StorageConfiguration{Size: "1Gi"},
		}}
		cluster.Spec.Backup = &BackupConfiguration{
			ObjectStore:         &S3ObjectStore{Bucket: "b"},
			ContinuousArchiving: &ContinuousArchivingConfiguration{Enabled: true, ObjectStore: bad()},
		}
		cluster.Spec.ExternalClusters = []ExternalCluster{{Name: "prod", BinlogObjectStore: bad()}}
		cluster.SetDefaults()

		var fields []string
		for _, e := range cluster.Validate() {
			fields = append(fields, e.Field)
		}
		Expect(fields).To(ContainElements(
			"spec.backup.continuousArchiving.objectStore.serverSideEncryption",
			"spec.externalClusters[0].binlogObjectStore.serverSideEncryption",
		))
	})
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./api/v1alpha1/ -run TestAPI -ginkgo.focus="Binlog archive object store"`
Expected: compile errors (`BinlogObjectStore`, `Location`, `ObjectStore` field undefined).

- [ ] **Step 3: Add the fields**

In `ContinuousArchivingConfiguration`, after `PurgeAfterArchive`:

```go
	// ObjectStore is where the binary-log archive is written. When unset, the
	// archive goes to spec.backup.objectStore next to the base backups. The
	// archive keeps the same layout in either store: <path>/<cluster>/binlogs/.
	// Changing it starts a new archive in the new store from the oldest binary
	// log still on the primary; take a new base backup afterwards.
	// +optional
	ObjectStore *S3ObjectStore `json:"objectStore,omitempty"`
```

In `ExternalCluster`, after `ObjectStore`:

```go
	// BinlogObjectStore is where the external cluster's binary-log archive
	// lives, when it is not in ObjectStore. Recovery reads base backups from
	// ObjectStore and binlogs from here.
	// +optional
	BinlogObjectStore *S3ObjectStore `json:"binlogObjectStore,omitempty"`
```

In `ContinuousArchivingStatus`, after `Enabled`:

```go
	// Destination is the archive location in use, "<endpoint>/<bucket>/<path>"
	// (endpoint empty for AWS). A change is reported with an ArchiveMoved
	// Warning event.
	// +optional
	Destination string `json:"destination,omitempty"`
```

In `BackupStatus`, after `ObjectStore`:

```go
	// BinlogObjectStore records the cluster's binary-log archive store when the
	// backup ran. Point-in-time recovery from this backup replays binlogs from
	// it. Unset on backups taken without continuous archiving or before this
	// field existed, in which case the archive is looked up in ObjectStore.
	// +optional
	BinlogObjectStore *S3ObjectStore `json:"binlogObjectStore,omitempty"`
```

- [ ] **Step 4: Add the helpers** in `api/v1alpha1/cluster_funcs.go`

After `(*S3ObjectStore).SetDefaults`:

```go
// Location identifies where the store's objects live, as
// "<endpoint>/<bucket>/<path>". Two stores with the same location hold the
// same objects whatever credentials reach them. Nil yields "".
func (store *S3ObjectStore) Location() string {
	if store == nil {
		return ""
	}
	return strings.TrimSuffix(store.Endpoint, "/") + "/" + store.Bucket + "/" + strings.Trim(store.Path, "/")
}

// SameLocation reports whether both stores address the same objects.
func (store *S3ObjectStore) SameLocation(other *S3ObjectStore) bool {
	return store.Location() == other.Location()
}
```

After `IsArchivingEnabled`:

```go
// BinlogObjectStore returns the object store the binary-log archive is written
// to: continuousArchiving.objectStore when set, otherwise backup.objectStore.
// Nil when neither is configured.
func (cluster *Cluster) BinlogObjectStore() *S3ObjectStore {
	if cluster.Spec.Backup == nil {
		return nil
	}
	if ca := cluster.Spec.Backup.ContinuousArchiving; ca != nil && ca.ObjectStore != nil {
		return ca.ObjectStore
	}
	return cluster.Spec.Backup.ObjectStore
}
```

After `FindExternalCluster`:

```go
// GetBinlogObjectStore returns the store holding the external cluster's
// binary-log archive: BinlogObjectStore when set, otherwise ObjectStore.
func (ext *ExternalCluster) GetBinlogObjectStore() *S3ObjectStore {
	if ext.BinlogObjectStore != nil {
		return ext.BinlogObjectStore
	}
	return ext.ObjectStore
}
```

Add `"strings"` to the imports if missing.

- [ ] **Step 5: Default and validate the new stores**

In `(*ClusterSpec).SetDefaults`, inside `if spec.Backup != nil {`, after the `spec.Backup.ObjectStore.SetDefaults()` block:

```go
		if ca := spec.Backup.ContinuousArchiving; ca != nil && ca.ObjectStore != nil {
			ca.ObjectStore.SetDefaults()
		}
```

and in the `ExternalClusters` loop:

```go
		if spec.ExternalClusters[i].BinlogObjectStore != nil {
			spec.ExternalClusters[i].BinlogObjectStore.SetDefaults()
		}
```

In the validation loop over `spec.ExternalClusters` (next to the `ObjectStore.Validate` call):

```go
		allErrs = append(allErrs, spec.ExternalClusters[i].BinlogObjectStore.Validate(
			specPath.Child("externalClusters").Index(i).Child("binlogObjectStore"))...)
```

In `validateBackup`, right after `if spec.Backup.ContinuousArchiving == nil { return allErrs }`:

```go
	allErrs = append(allErrs, spec.Backup.ContinuousArchiving.ObjectStore.Validate(
		path.Child("continuousArchiving", "objectStore"))...)
```

(`Validate` already accepts a nil receiver: `spec.Backup.ObjectStore` is passed the same way.)

- [ ] **Step 6: Regenerate and run the tests**

Run: `make manifests generate && go test ./api/...`
Expected: PASS; `config/crd/bases/mysql.cnmsql.co_clusters.yaml` and `..._backups.yaml` gain the new fields.

- [ ] **Step 7: Commit**

```bash
git add api/ config/crd/
git commit -m "feat(api): add a separate object store for the binlog archive"
```

---

#### Task 2: Object-store env twin and split retention apply

**Files:**
- Modify: `pkg/management/mysql/objectstore/client.go`
- Modify: `pkg/management/mysql/objectstore/retention.go` (`ApplyRetention`)
- Modify: `internal/controller/cluster_retention.go` (only the `ApplyRetention` call, to keep the build green)
- Test: `pkg/management/mysql/objectstore/client_test.go`, `pkg/management/mysql/objectstore/retention_test.go`

**Interfaces:**
- Produces:
  - `func BinlogEnvName(name string) string` — `"cnmsql_S3_BUCKET"` → `"cnmsql_BINLOG_S3_BUCKET"`
  - `func BinlogConfigFromEnv() Config`
  - `func BinlogStoreFromEnv() mysqlv1alpha1.S3ObjectStore`
  - `func HasBinlogStoreEnv() bool`
  - `func ApplyBackupExpiry(ctx context.Context, client *Client, store mysqlv1alpha1.S3ObjectStore, plan RetentionPlan) error`
  - `func ApplyBinlogExpiry(ctx context.Context, client *Client, store mysqlv1alpha1.S3ObjectStore, clusterName string, plan RetentionPlan) error`
  - `ApplyRetention` is removed.

- [ ] **Step 1: Write the failing tests**

In `client_test.go`:

```go
func TestBinlogEnvName(t *testing.T) {
	if got := BinlogEnvName(EnvBucket); got != "cnmsql_BINLOG_S3_BUCKET" {
		t.Fatalf("BinlogEnvName(%q) = %q", EnvBucket, got)
	}
	if got := BinlogEnvName(EnvSecretAccessKey); got != "cnmsql_BINLOG_S3_SECRET_ACCESS_KEY" {
		t.Fatalf("BinlogEnvName(%q) = %q", EnvSecretAccessKey, got)
	}
}

func TestBinlogConfigFromEnv(t *testing.T) {
	t.Setenv(EnvEndpoint, "http://base:9000")
	t.Setenv(EnvBucket, "base")
	if HasBinlogStoreEnv() {
		t.Fatal("no binlog env set, HasBinlogStoreEnv should be false")
	}
	t.Setenv(BinlogEnvName(EnvEndpoint), "http://archive:9000")
	t.Setenv(BinlogEnvName(EnvAccessKeyID), "archive-key")
	t.Setenv(BinlogEnvName(EnvForcePathStyle), "true")
	t.Setenv(BinlogEnvName(EnvBucket), "binlogs")
	t.Setenv(BinlogEnvName(EnvPath), "archive")

	if !HasBinlogStoreEnv() {
		t.Fatal("HasBinlogStoreEnv should be true once the binlog bucket is set")
	}
	cfg := BinlogConfigFromEnv()
	if cfg.Endpoint != "http://archive:9000" || cfg.AccessKeyID != "archive-key" || !cfg.ForcePathStyle {
		t.Fatalf("binlog config = %+v", cfg)
	}
	if store := BinlogStoreFromEnv(); store.Bucket != "binlogs" || store.Path != "archive" {
		t.Fatalf("binlog store = %+v", store)
	}
	if base := ConfigFromEnv(); base.Endpoint != "http://base:9000" {
		t.Fatalf("base config changed: %+v", base)
	}
}
```

In `retention_test.go`, a recording fake and one test (add imports `context`, `net/http`, `net/http/httptest`, `slices`, `sync`, `mysqlv1alpha1`):

```go
// recordingS3 accepts every request and records "METHOD /path".
func recordingS3(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Method+" "+r.URL.Path)
		mu.Unlock()
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`))
	}))
	t.Cleanup(server.Close)
	return server, func() []string { mu.Lock(); defer mu.Unlock(); return slices.Clone(seen) }
}

func testClient(t *testing.T, endpoint string) *Client {
	t.Helper()
	c, err := NewClient(Config{Endpoint: endpoint, ForcePathStyle: true, AccessKeyID: "k", SecretAccessKey: "s"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestApplyExpirySplitsStores(t *testing.T) {
	baseSrv, baseSeen := recordingS3(t)
	logSrv, logSeen := recordingS3(t)
	baseStore := mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "base"}
	logStore := mysqlv1alpha1.S3ObjectStore{Bucket: "binlogs", Path: "archive"}
	plan := RetentionPlan{
		DeleteBinlogKeys: []string{"archive/demo/binlogs/u/binlog.000001", "archive/demo/binlogs/u/binlog.000001.json"},
		NewIndex:         &ArchiveIndex{},
	}

	if err := ApplyBackupExpiry(context.Background(), testClient(t, baseSrv.URL), baseStore, plan); err != nil {
		t.Fatal(err)
	}
	if err := ApplyBinlogExpiry(context.Background(), testClient(t, logSrv.URL), logStore, "demo", plan); err != nil {
		t.Fatal(err)
	}

	if got := baseSeen(); len(got) != 0 {
		t.Fatalf("base store must not see binlog requests, got %v", got)
	}
	got := logSeen()
	for _, want := range []string{
		"DELETE /binlogs/archive/demo/binlogs/u/binlog.000001",
		"DELETE /binlogs/archive/demo/binlogs/u/binlog.000001.json",
		"PUT /binlogs/archive/demo/binlogs/_index.json",
	} {
		if !slices.Contains(got, want) {
			t.Fatalf("binlog store requests %v missing %q", got, want)
		}
	}
}
```

(`DeleteBackupPrefixes` is left empty so `RemovePrefix`'s listing does not muddy the base store's record; the controller test in Task 6 covers the base side.)

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./pkg/management/mysql/objectstore/ -run 'TestBinlogEnvName|TestBinlogConfigFromEnv|TestApplyExpirySplitsStores'`
Expected: compile errors (undefined `BinlogEnvName`, `ApplyBackupExpiry`, ...).

- [ ] **Step 3: Implement the env twin** in `client.go`

Below the `Env*` const block:

```go
// binlogEnvPrefix replaces the "cnmsql_" prefix of a cnmsql_S3_* name. The
// restore Job carries the binary-log archive store under these names when it
// is not the base-backup store.
const (
	envPrefix       = "cnmsql_"
	binlogEnvPrefix = "cnmsql_BINLOG_"
)

// BinlogEnvName returns the cnmsql_BINLOG_S3_* twin of a cnmsql_S3_* name.
func BinlogEnvName(name string) string {
	return binlogEnvPrefix + strings.TrimPrefix(name, envPrefix)
}

// HasBinlogStoreEnv reports whether the environment names a separate
// binary-log archive store.
func HasBinlogStoreEnv() bool {
	return os.Getenv(BinlogEnvName(EnvBucket)) != ""
}

// BinlogStoreFromEnv is StoreFromEnv for the binary-log archive store.
func BinlogStoreFromEnv() mysqlv1alpha1.S3ObjectStore {
	return mysqlv1alpha1.S3ObjectStore{
		Bucket: os.Getenv(BinlogEnvName(EnvBucket)),
		Path:   os.Getenv(BinlogEnvName(EnvPath)),
	}
}

// BinlogConfigFromEnv is ConfigFromEnv for the binary-log archive store.
func BinlogConfigFromEnv() Config {
	return configFromEnv(func(name string) string { return os.Getenv(BinlogEnvName(name)) })
}
```

Rewrite `ConfigFromEnv` as a wrapper so both share one body:

```go
// ConfigFromEnv builds a Config from the cnmsql_S3_* environment variables.
func ConfigFromEnv() Config {
	return configFromEnv(os.Getenv)
}

func configFromEnv(getenv func(string) string) Config {
	cfg := Config{
		Endpoint:             getenv(EnvEndpoint),
		Region:               getenv(EnvRegion),
		AccessKeyID:          getenv(EnvAccessKeyID),
		SecretAccessKey:      getenv(EnvSecretAccessKey),
		SessionToken:         getenv(EnvSessionToken),
		SignatureV2:          strings.EqualFold(getenv(EnvSignatureVersion), "s3v2"),
		ServerSideEncryption: getenv(EnvServerSideEncryption),
		StorageClass:         getenv(EnvStorageClass),
		CABundle:             getenv(EnvCABundle),
	}
	if force, err := strconv.ParseBool(getenv(EnvForcePathStyle)); err == nil {
		cfg.ForcePathStyle = force
	}
	if insecure, err := strconv.ParseBool(getenv(EnvTLSInsecure)); err == nil {
		cfg.InsecureSkipVerify = insecure
	}
	return cfg
}
```

- [ ] **Step 4: Split `ApplyRetention`** in `retention.go`

Replace `ApplyRetention` with:

```go
// ApplyBackupExpiry deletes the expired base backups from the store holding
// them. It runs before ApplyBinlogExpiry so a failure on the binlog side never
// keeps an expired recovery point alive.
func ApplyBackupExpiry(
	ctx context.Context,
	client *Client,
	store mysqlv1alpha1.S3ObjectStore,
	plan RetentionPlan,
) error {
	for _, prefix := range plan.DeleteBackupPrefixes {
		if err := client.RemovePrefix(ctx, store.Bucket, prefix); err != nil {
			return err
		}
	}
	return nil
}

// ApplyBinlogExpiry deletes the uncoverable binlogs from the archive store,
// then rewrites the archive index. The index rewrite is done last so a mid-run
// failure leaves a still-valid index; any objects deleted but not yet
// de-indexed are cleaned up on the next pass.
func ApplyBinlogExpiry(
	ctx context.Context,
	client *Client,
	store mysqlv1alpha1.S3ObjectStore,
	clusterName string,
	plan RetentionPlan,
) error {
	for _, key := range plan.DeleteBinlogKeys {
		if err := client.Remove(ctx, store.Bucket, key); err != nil {
			return err
		}
	}
	if plan.NewIndex != nil {
		indexKey := ArchiveIndexKey(store, clusterName)
		if err := client.PutJSON(ctx, store.Bucket, indexKey, plan.NewIndex); err != nil {
			return err
		}
	}
	return nil
}
```

In `internal/controller/cluster_retention.go` replace the single call with the two (same client and store for now; Task 6 splits them):

```go
		if err := objectstore.ApplyBackupExpiry(ctx, client, *store, plan); err != nil {
			return err
		}
		if err := objectstore.ApplyBinlogExpiry(ctx, client, *store, cluster.Name, plan); err != nil {
			return err
		}
```

Run `grep -rn ApplyRetention --include='*.go' .` (ignore `.kilo/`) and fix any other caller the same way.

- [ ] **Step 5: Run the tests**

Run: `go test ./pkg/management/mysql/objectstore/ ./internal/controller/ -run 'Binlog|Expiry|Retention'`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/management/mysql/objectstore/ internal/controller/cluster_retention.go
git commit -m "feat(objectstore): read a separate binlog store from the environment"
```

---

#### Task 3: Restore replays binlogs from the binlog store

**Files:**
- Modify: `pkg/management/mysql/instance/restore.go` (`RestoreOptions`)
- Modify: `pkg/management/mysql/instance/restore_pitr.go` (index read and binlog download)
- Modify: `internal/cmd/manager/instance/restore/cmd.go`
- Test: `pkg/management/mysql/instance/restore_pitr_test.go`, `internal/cmd/manager/instance/restore/cmd_test.go` (exists; add the tests and the `objectstore` import)

**Interfaces:**
- Consumes: `objectstore.HasBinlogStoreEnv`, `objectstore.BinlogConfigFromEnv`, `objectstore.BinlogStoreFromEnv` (Task 2)
- Produces: `RestoreOptions.BinlogStore *objectstore.Client`; `func (o *RestoreOptions) binlogClient() *objectstore.Client`; `func binlogStoreFromEnv() (*objectstore.Client, mysqlv1alpha1.S3ObjectStore, error)` in package `restore`

- [ ] **Step 1: Write the failing tests**

`restore_pitr_test.go`:

```go
func TestRestoreBinlogClientDefaultsToBaseStore(t *testing.T) {
	base := &objectstore.Client{}
	o := &RestoreOptions{Store: base}
	if o.binlogClient() != base {
		t.Fatal("without a binlog store, binlogs are read through the base-backup client")
	}
	archive := &objectstore.Client{}
	o.BinlogStore = archive
	if o.binlogClient() != archive {
		t.Fatal("a separate binlog store must be used for the archive")
	}
}
```

`internal/cmd/manager/instance/restore/cmd_test.go` (change its `import "testing"` to also import `"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"`):

```go
func TestBinlogStoreFromEnvFallsBackToBase(t *testing.T) {
	t.Setenv(objectstore.EnvBucket, "backups")
	t.Setenv(objectstore.EnvPath, "base")
	client, store, err := binlogStoreFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if client != nil {
		t.Fatal("no binlog env: the base-backup client is reused, so none is built")
	}
	if store.Bucket != "backups" || store.Path != "base" {
		t.Fatalf("store = %+v, want the base bucket/path", store)
	}
}

func TestBinlogStoreFromEnvSeparate(t *testing.T) {
	t.Setenv(objectstore.EnvBucket, "backups")
	t.Setenv(objectstore.BinlogEnvName(objectstore.EnvEndpoint), "http://127.0.0.1:9")
	t.Setenv(objectstore.BinlogEnvName(objectstore.EnvBucket), "binlogs")
	t.Setenv(objectstore.BinlogEnvName(objectstore.EnvPath), "archive")
	client, store, err := binlogStoreFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if client == nil {
		t.Fatal("a separate binlog store needs its own client")
	}
	if store.Bucket != "binlogs" || store.Path != "archive" {
		t.Fatalf("store = %+v, want the binlog bucket/path", store)
	}
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./pkg/management/mysql/instance/ -run TestRestoreBinlogClient && go test ./internal/cmd/manager/instance/restore/`
Expected: compile errors (`binlogClient`, `BinlogStore`, `binlogStoreFromEnv` undefined).

- [ ] **Step 3: Implement**

In `RestoreOptions`, after `ObjectStore`:

```go
	// BinlogStore reads the binary-log archive when it lives in a different
	// object store from the base backup. Nil means the archive is in the same
	// store, and Store is used.
	BinlogStore *objectstore.Client
```

In `restore_pitr.go`:

```go
// binlogClient returns the client that reads the binary-log archive: the
// separate archive store when there is one, otherwise the base-backup store.
func (o *RestoreOptions) binlogClient() *objectstore.Client {
	if o.BinlogStore != nil {
		return o.BinlogStore
	}
	return o.Store
}
```

Replace exactly the two archive reads with `o.binlogClient()`: the `_index.json` read (`o.Store.GetJSON(ctx, o.ObjectStore.Bucket, indexKey, &index)`) and the binlog download (`o.Store.Download(ctx, o.ObjectStore.Bucket, key, f)`). The metadata read (`o.Store.GetJSON(ctx, o.Bucket, o.MetadataKey, ...)`) stays on `o.Store`: the metadata sits next to the base backup. Re-run `grep -n 'o.Store\.' restore_pitr.go` afterwards; every remaining hit must use `o.Bucket`, not `o.ObjectStore.Bucket`.

In `cmd.go` add the helper (import `mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"`):

```go
// binlogStoreFromEnv returns the client and bucket/path of the binary-log
// archive. The restore Job carries cnmsql_BINLOG_S3_* only when the archive is
// in a different store from the base backup; otherwise the layout comes from
// cnmsql_S3_* and the client is nil, so restore reuses the base-backup client.
func binlogStoreFromEnv() (*objectstore.Client, mysqlv1alpha1.S3ObjectStore, error) {
	if !objectstore.HasBinlogStoreEnv() {
		return nil, objectstore.StoreFromEnv(), nil
	}
	client, err := objectstore.NewClient(objectstore.BinlogConfigFromEnv())
	if err != nil {
		return nil, mysqlv1alpha1.S3ObjectStore{}, fmt.Errorf("binlog object store: %w", err)
	}
	return client, objectstore.BinlogStoreFromEnv(), nil
}
```

In `RunE`, after building `store`:

```go
			binlogClient, binlogStore, err := binlogStoreFromEnv()
			if err != nil {
				return err
			}
```

and in the `RestoreOptions` literal replace `ObjectStore: objectstore.StoreFromEnv(),` with:

```go
				ObjectStore:     binlogStore,
				BinlogStore:     binlogClient,
```

Update the comments that say "bucket/path come from cnmsql_S3_*" (the `Long` text, the PITR comment above the target parsing, and the flags comment) to say "from cnmsql_BINLOG_S3_* when the archive is in its own store, cnmsql_S3_* otherwise".

- [ ] **Step 4: Run the tests**

Run: `go test ./pkg/management/mysql/instance/ ./internal/cmd/manager/instance/restore/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/management/mysql/instance/ internal/cmd/manager/instance/restore/
git commit -m "feat(restore): replay binlogs from a separate archive store"
```

---

#### Task 4: Operator renders the binlog store for the archiver and recovery

**Files:**
- Modify: `internal/controller/backup_controller.go` (env helpers next to `backupObjectStoreEnv`)
- Modify: `internal/controller/cluster_pod.go` (`runEnv`)
- Modify: `internal/controller/cluster_plan.go` (`recoveryPlan`, `resolveRecovery`, `resolveRawS3Recovery`)
- Modify: `internal/controller/cluster_backup_guard.go` (recovery target check)
- Test: `internal/controller/cluster_archiving_test.go`, `internal/controller/cluster_recovery_test.go`, `internal/controller/cluster_raw_recovery_test.go`

**Interfaces:**
- Consumes: `Cluster.BinlogObjectStore`, `ExternalCluster.GetBinlogObjectStore`, `S3ObjectStore.SameLocation` (Task 1); `objectstore.BinlogEnvName` (Task 2)
- Produces:
  - `func objectStoreLocationEnv(store mysqlv1alpha1.S3ObjectStore) []corev1.EnvVar`
  - `func binlogObjectStoreEnv(store mysqlv1alpha1.S3ObjectStore) []corev1.EnvVar`
  - `func recoveryStoreEnv(base, binlogs mysqlv1alpha1.S3ObjectStore) []corev1.EnvVar`
  - `recoveryPlan.BinlogStore mysqlv1alpha1.S3ObjectStore`

- [ ] **Step 1: Write the failing tests**

`cluster_archiving_test.go`:

```go
func TestArchivingEnvUsesSeparateArchiveStore(t *testing.T) {
	cluster := archivingCluster()
	cluster.Spec.Backup.ContinuousArchiving.ObjectStore = &mysqlv1alpha1.S3ObjectStore{
		Bucket: "binlogs", Path: "archive", Endpoint: "http://archive:8333",
	}
	env := runEnv(cluster, testPlan())
	if got := envValue(env, "cnmsql_S3_BUCKET"); got != "binlogs" {
		t.Fatalf("archiver bucket = %q, want the archive store", got)
	}
	if got := envValue(env, "cnmsql_S3_PATH"); got != "archive" {
		t.Fatalf("archiver path = %q", got)
	}
	if got := envValue(env, "cnmsql_S3_ENDPOINT"); got != "http://archive:8333" {
		t.Fatalf("archiver endpoint = %q", got)
	}
}
```

`cluster_recovery_test.go`:

```go
func TestResolveRecoveryBinlogStore(t *testing.T) {
	t.Parallel()

	base := &mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "base"}
	tests := []struct {
		name        string
		binlogStore *mysqlv1alpha1.S3ObjectStore
		wantBucket  string
		wantTwinEnv bool
	}{
		{name: "backup without a recorded archive store uses the base store", wantBucket: "backups"},
		{
			name:        "same location with other credentials is one store",
			binlogStore: &mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "/base/"},
			wantBucket:  "backups",
		},
		{
			name:        "recorded archive store wins",
			binlogStore: &mysqlv1alpha1.S3ObjectStore{Bucket: "binlogs", Path: "archive"},
			wantBucket:  "binlogs",
			wantTwinEnv: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			backup := recoveryBackupFixture(base, nil)
			backup.Status.BinlogObjectStore = tc.binlogStore
			cluster := recoveryTargetClusterFixture(nil)
			scheme := testScheme(t)
			r := &ClusterReconciler{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, backup).Build(),
				Scheme: scheme,
			}
			plan, err := r.resolveRecovery(context.Background(), cluster)
			if err != nil {
				t.Fatal(err)
			}
			if plan.BinlogStore.Bucket != tc.wantBucket {
				t.Fatalf("binlog store bucket = %q, want %q", plan.BinlogStore.Bucket, tc.wantBucket)
			}
			if got := envValue(plan.StoreEnv, "cnmsql_S3_BUCKET"); got != "backups" {
				t.Fatalf("base bucket env = %q", got)
			}
			twin := envValue(plan.StoreEnv, "cnmsql_BINLOG_S3_BUCKET")
			if tc.wantTwinEnv && twin != tc.wantBucket {
				t.Fatalf("binlog bucket env = %q, want %q", twin, tc.wantBucket)
			}
			if !tc.wantTwinEnv && twin != "" {
				t.Fatalf("binlog env rendered for a single store: %q", twin)
			}
		})
	}
}
```

(Check `recoveryBackupFixture`/`recoveryTargetClusterFixture` and the fake-client setup used by `TestResolveRecoveryBackupObjectStore` in the same file; if it adds more objects, add the same ones here.)

`cluster_raw_recovery_test.go`:

```go
func TestResolveRawS3RecoveryBinlogObjectStore(t *testing.T) {
	t.Parallel()

	server := rawS3Server(t, rawList, rawMetadata())
	defer server.Close()

	cluster := rawRecoveryCluster(server.URL, "")
	cluster.Spec.ExternalClusters[0].BinlogObjectStore = &mysqlv1alpha1.S3ObjectStore{
		Bucket: "binlogs", Path: "archive", Endpoint: server.URL,
	}
	plan, err := rawRecoveryReconciler(t).resolveRecovery(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Store.Bucket != "cluster-backups" || plan.BinlogStore.Bucket != "binlogs" {
		t.Fatalf("stores = base %q / binlog %q", plan.Store.Bucket, plan.BinlogStore.Bucket)
	}
	if got := envValue(plan.StoreEnv, "cnmsql_BINLOG_S3_PATH"); got != "archive" {
		t.Fatalf("binlog path env = %q", got)
	}
}
```

Also extend `TestResolveRawS3RecoveryLatest` with:

```go
	if plan.BinlogStore.Bucket != "cluster-backups" {
		t.Fatalf("without binlogObjectStore the archive is in the base store, got %q", plan.BinlogStore.Bucket)
	}
	if got := envValue(plan.StoreEnv, "cnmsql_BINLOG_S3_BUCKET"); got != "" {
		t.Fatalf("no binlog env expected, got %q", got)
	}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/controller/ -run 'TestArchivingEnvUsesSeparateArchiveStore|TestResolveRecoveryBinlogStore|TestResolveRawS3Recovery'`
Expected: compile errors (`BinlogStore` field undefined) / archiver bucket still "backups".

- [ ] **Step 3: Env helpers** in `backup_controller.go`, after `backupObjectStoreEnv`:

```go
// objectStoreLocationEnv is backupObjectStoreEnv plus the bucket and path, for
// consumers that build object keys themselves (the archiver, restore).
func objectStoreLocationEnv(store mysqlv1alpha1.S3ObjectStore) []corev1.EnvVar {
	return append(backupObjectStoreEnv(store),
		corev1.EnvVar{Name: objectstore.EnvBucket, Value: store.Bucket},
		corev1.EnvVar{Name: objectstore.EnvPath, Value: store.Path},
	)
}

// binlogObjectStoreEnv renders store under the cnmsql_BINLOG_S3_* names, the
// second object store a restore Job reads when the binary-log archive is not
// kept with the base backups.
func binlogObjectStoreEnv(store mysqlv1alpha1.S3ObjectStore) []corev1.EnvVar {
	env := objectStoreLocationEnv(store)
	for i := range env {
		env[i].Name = objectstore.BinlogEnvName(env[i].Name)
	}
	return env
}

// recoveryStoreEnv renders a restore Job's object-store environment: the base
// backup's store, plus the binary-log archive's store when it is a different
// location. A single store renders exactly what it rendered before archive
// stores could be separated.
func recoveryStoreEnv(base, binlogs mysqlv1alpha1.S3ObjectStore) []corev1.EnvVar {
	env := objectStoreLocationEnv(base)
	if !base.SameLocation(&binlogs) {
		env = append(env, binlogObjectStoreEnv(binlogs)...)
	}
	return env
}
```

- [ ] **Step 4: Archiver env** in `cluster_pod.go` `runEnv`, replace the archiving block:

```go
	if cluster != nil && cluster.IsArchivingEnabled() {
		env = append(env, objectStoreLocationEnv(*cluster.BinlogObjectStore())...)
	}
```

and update the function comment: "the binary-log archive store's credentials and destination (bucket/path) are appended".

- [ ] **Step 5: Recovery plan**

Add to `recoveryPlan` after `Store`:

```go
	// BinlogStore is the resolved (defaulted) store holding SourceCluster's
	// binary-log archive. It is Store unless the archive was kept apart.
	BinlogStore mysqlv1alpha1.S3ObjectStore
```

Update the `StoreEnv` comment: "... plus cnmsql_BINLOG_S3_* when the binlog archive is in another store".

In `resolveRecovery` replace the `storeEnv := append(...)` block and the plan literal's store fields:

```go
	// The archive is the source cluster's, recorded on the Backup when it ran.
	// A Backup without the record predates separate archive stores (or was taken
	// without archiving): its archive, if any, sits next to the base backup.
	binlogStore := store
	if backup.Status.BinlogObjectStore != nil {
		binlogStore = backup.Status.BinlogObjectStore.DeepCopy()
		binlogStore.SetDefaults()
	}

	plan := &recoveryPlan{
		Bucket:        store.Bucket,
		ArchiveKey:    keys.ArchiveKey,
		MetadataKey:   keys.MetadataKey,
		StoreEnv:      recoveryStoreEnv(*store, *binlogStore),
		SourceCluster: sourceCluster,
		Store:         *store,
		BinlogStore:   *binlogStore,
	}
```

In `resolveRawS3Recovery`, after `store.SetDefaults()`:

```go
	binlogStore := ext.GetBinlogObjectStore().DeepCopy()
	binlogStore.SetDefaults()
```

and replace its `storeEnv := append(...)` block and plan fields with `StoreEnv: recoveryStoreEnv(*store, *binlogStore)` and `BinlogStore: *binlogStore`.

- [ ] **Step 6: Recovery target guard** in `cluster_backup_guard.go`, in the recovery-target check, change `store := plan.Recovery.Store` to:

```go
	// The target is checked against the archive index, which lives in the
	// binary-log archive store.
	store := plan.Recovery.BinlogStore
```

Then add to `cluster_backup_guard_test.go` (no test covers `checkRecoveryTarget` yet; add `"strings"` to the imports):

```go
func TestCheckRecoveryTargetReadsArchiveStore(t *testing.T) {
	t.Parallel()

	const uuid = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	base := httptest.NewServer(http.NotFoundHandler())
	defer base.Close()
	archive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/binlogs/_index.json") {
			_, _ = w.Write([]byte(`{"clusterName":"src","segments":[],"coveredGTIDSet":"` + uuid + `:1-10","updatedAt":"2026-10-01T00:00:00Z"}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer archive.Close()

	cluster := baseBackupCluster()
	cluster.Status.CurrentPrimary = ""
	baseStore := cluster.Spec.Backup.ObjectStore.DeepCopy()
	baseStore.Endpoint = base.URL
	baseStore.SetDefaults()
	archiveStore := baseStore.DeepCopy()
	archiveStore.Endpoint = archive.URL
	archiveStore.Bucket = "binlogs"

	plan := clusterPlan{Recovery: &recoveryPlan{
		HasTarget:     true,
		TargetGTID:    uuid + ":1-5",
		SourceCluster: "src",
		Store:         *baseStore,
		BinlogStore:   *archiveStore,
	}}
	check := guardReconciler(t).checkRecoveryTarget(context.Background(), cluster, plan)
	if check.Retry != nil || check.Blocked != "" {
		t.Fatalf("target inside the archive store's coverage must pass, got blocked=%q retry=%v",
			check.Blocked, check.Retry)
	}
}
```

(`baseBackupCluster()` carries credentials referencing the `cluster-s3` Secret that `guardReconciler` seeds.)

- [ ] **Step 7: Run the tests**

Run: `go test ./internal/controller/ -run 'Archiving|ResolveRecovery|ResolveRawS3|RecoveryBootstrap|BackupDestination|RecoveryTarget'`
Expected: PASS, including the unchanged `TestRecoveryBootstrapPITRTargetReplaysBinlogs`.

- [ ] **Step 8: Commit**

```bash
git add internal/controller/
git commit -m "feat(cluster): archive and recover binlogs from a separate object store"
```

---

#### Task 5: Backups record the archive store they are anchored to

**Files:**
- Modify: `internal/controller/backup_controller.go` (status patch)
- Test: `internal/controller/backup_controller_test.go`

**Interfaces:**
- Consumes: `Cluster.BinlogObjectStore`, `Cluster.IsArchivingEnabled`, `BackupStatus.BinlogObjectStore` (Task 1)

- [ ] **Step 1: Write the failing test**

```go
func TestBackupRecordsBinlogObjectStore(t *testing.T) {
	t.Parallel()

	archive := &mysqlv1alpha1.S3ObjectStore{Bucket: "binlogs", Path: "archive"}
	tests := []struct {
		name      string
		archiving bool
		override  bool
		want      *mysqlv1alpha1.S3ObjectStore
	}{
		{name: "no archiving records nothing"},
		{name: "archiving records the archive store", archiving: true, want: archive},
		{name: "a per-Backup base store override keeps the cluster's archive", archiving: true, override: true, want: archive},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			scheme := testScheme(t)
			cluster := baseBackupCluster()
			if tc.archiving {
				cluster.Spec.Backup.ContinuousArchiving = &mysqlv1alpha1.ContinuousArchivingConfiguration{
					Enabled: true, ObjectStore: archive.DeepCopy(),
				}
			}
			backup := baseBackup()
			if tc.override {
				backup.Spec.ObjectStore = &mysqlv1alpha1.S3ObjectStore{Bucket: "elsewhere"}
			}
			r := &BackupReconciler{
				Client: fake.NewClientBuilder().WithScheme(scheme).
					WithStatusSubresource(&mysqlv1alpha1.Backup{}).
					WithObjects(cluster, backup, readyReplicaPod()).Build(),
				Scheme: scheme,
			}
			reconcileBackup(t, r, backup)

			got := &mysqlv1alpha1.Backup{}
			if err := r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: backup.Name}, got); err != nil {
				t.Fatal(err)
			}
			switch {
			case tc.want == nil && got.Status.BinlogObjectStore != nil:
				t.Fatalf("binlogObjectStore = %+v, want unset", got.Status.BinlogObjectStore)
			case tc.want != nil && (got.Status.BinlogObjectStore == nil || got.Status.BinlogObjectStore.Bucket != tc.want.Bucket):
				t.Fatalf("binlogObjectStore = %+v, want bucket %q", got.Status.BinlogObjectStore, tc.want.Bucket)
			}
		})
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/controller/ -run TestBackupRecordsBinlogObjectStore`
Expected: FAIL on the two archiving cases (`binlogObjectStore = <nil>`).

- [ ] **Step 3: Implement** in the `patchBackupStatus` closure, after `status.ObjectStore = store`:

```go
		// PITR from this backup replays the cluster's archive, which a per-Backup
		// spec.objectStore override does not move.
		if cluster.IsArchivingEnabled() {
			status.BinlogObjectStore = cluster.BinlogObjectStore().DeepCopy()
		}
```

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/controller/ -run 'TestBackup'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/controller/backup_controller.go internal/controller/backup_controller_test.go
git commit -m "feat(backup): record the binlog archive store a backup is anchored to"
```

---

#### Task 6: Retention, reclaim and the empty-destination guard act per store

**Files:**
- Modify: `internal/controller/cluster_retention.go`
- Modify: `internal/controller/cluster_backup_reclaim.go`
- Modify: `internal/controller/cluster_backup_guard.go` (`checkBackupDestination`)
- Test: `internal/controller/cluster_retention_test.go`, `internal/controller/cluster_backup_reclaim_test.go`, `internal/controller/cluster_backup_guard_test.go`

**Interfaces:**
- Consumes: `Cluster.BinlogObjectStore`, `S3ObjectStore.SameLocation` (Task 1); `objectstore.ApplyBackupExpiry`, `objectstore.ApplyBinlogExpiry` (Task 2)
- Produces: `func (r *ClusterReconciler) objectStoreClient(ctx context.Context, namespace string, store *mysqlv1alpha1.S3ObjectStore) (*objectstore.Client, error)` (the `objectStoreConfig` + `NewClient` pair every caller repeats)

Test fixture shared by the three tests (put it in `cluster_retention_test.go`):

```go
// recordingS3Server answers LIST with an empty result, HEAD with 404 and
// DELETE with 204, and records "METHOD /path?prefix=..." for each request.
func recordingS3Server(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Method+" "+r.URL.Path+"?prefix="+r.URL.Query().Get("prefix"))
		mu.Unlock()
		switch r.Method {
		case http.MethodHead:
			w.WriteHeader(http.StatusNotFound)
		case http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`))
		}
	}))
	t.Cleanup(server.Close)
	return server, func() []string { mu.Lock(); defer mu.Unlock(); return slices.Clone(seen) }
}

func storeAt(endpoint, bucket, path string) *mysqlv1alpha1.S3ObjectStore {
	store := &mysqlv1alpha1.S3ObjectStore{
		Bucket: bucket, Path: path, Endpoint: endpoint,
		Credentials: mysqlv1alpha1.S3Credentials{
			AccessKeyID:     &mysqlv1alpha1.SecretKeySelector{Name: "cluster-s3", Key: "access"},
			SecretAccessKey: &mysqlv1alpha1.SecretKeySelector{Name: "cluster-s3", Key: "secret"},
		},
	}
	store.SetDefaults()
	return store
}

func anyContains(seen []string, sub string) bool {
	return slices.ContainsFunc(seen, func(s string) bool { return strings.Contains(s, sub) })
}
```

- [ ] **Step 1: Write the failing tests**

`cluster_retention_test.go`:

```go
func TestReconcileRetentionListsBinlogsInArchiveStore(t *testing.T) {
	t.Parallel()

	baseSrv, baseSeen := recordingS3Server(t)
	logSrv, logSeen := recordingS3Server(t)
	cluster := baseCluster()
	cluster.Spec.Backup = &mysqlv1alpha1.BackupConfiguration{
		ObjectStore:     storeAt(baseSrv.URL, "backups", "base"),
		RetentionPolicy: "30d",
		ContinuousArchiving: &mysqlv1alpha1.ContinuousArchivingConfiguration{
			Enabled: true, ObjectStore: storeAt(logSrv.URL, "binlogs", "archive"),
		},
	}
	cluster.Status.CurrentPrimary = instanceName(cluster, 1)
	scheme := testScheme(t)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(cluster, s3CredentialsSecret()).Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	if err := r.reconcileRetention(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if anyContains(baseSeen(), "binlogs/") {
		t.Fatalf("base store was asked for binlogs: %v", baseSeen())
	}
	if !anyContains(logSeen(), "prefix=archive/demo/binlogs/") {
		t.Fatalf("archive store was not listed for binlogs: %v", logSeen())
	}
	if !anyContains(baseSeen(), "prefix=base/demo/") {
		t.Fatalf("base store was not listed for base backups: %v", baseSeen())
	}
}
```

(If `ClusterPrefix` normalizes paths differently, read it in `objectstore/objectstore.go` and adjust the expected prefixes; the assertion that matters is "binlogs only in the archive store".)

`cluster_backup_reclaim_test.go`:

```go
func TestClusterDeleteWipesBothStores(t *testing.T) {
	t.Parallel()

	baseSrv, baseSeen := recordingS3Server(t)
	logSrv, logSeen := recordingS3Server(t)
	cluster := reclaimCluster(storeAt(baseSrv.URL, "backups", "base"))
	cluster.Spec.Backup.ContinuousArchiving = &mysqlv1alpha1.ContinuousArchivingConfiguration{
		Enabled: true, ObjectStore: storeAt(logSrv.URL, "binlogs", "archive"),
	}
	cluster.Finalizers = []string{clusterBackupFinalizer}
	now := metav1.Now()
	cluster.DeletionTimestamp = &now
	scheme := testScheme(t)
	r := &ClusterReconciler{
		Client:   fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, s3CredentialsSecret()).Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	if err := r.reconcileClusterDelete(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if !anyContains(baseSeen(), "prefix=base/demo/") {
		t.Fatalf("base prefix not removed: %v", baseSeen())
	}
	if !anyContains(logSeen(), "prefix=archive/demo/") {
		t.Fatalf("archive prefix not removed: %v", logSeen())
	}
}

func TestClusterDeleteWipesSharedStoreOnce(t *testing.T) {
	t.Parallel()

	srv, seen := recordingS3Server(t)
	cluster := reclaimCluster(storeAt(srv.URL, "backups", "base"))
	cluster.Spec.Backup.ContinuousArchiving = &mysqlv1alpha1.ContinuousArchivingConfiguration{
		Enabled: true, ObjectStore: storeAt(srv.URL, "backups", "/base/"),
	}
	cluster.Finalizers = []string{clusterBackupFinalizer}
	now := metav1.Now()
	cluster.DeletionTimestamp = &now
	scheme := testScheme(t)
	r := &ClusterReconciler{
		Client:   fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, s3CredentialsSecret()).Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	if err := r.reconcileClusterDelete(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	lists := 0
	for _, s := range seen() {
		if strings.HasPrefix(s, "GET ") && strings.Contains(s, "prefix=base/demo/") {
			lists++
		}
	}
	if lists != 1 {
		t.Fatalf("one location must be listed once, got %d: %v", lists, seen())
	}
}
```

`cluster_backup_guard_test.go`, modelled on `TestCheckBackupDestinationBlocksNonEmpty` (read it first; reuse its non-empty LIST server for the archive store and an empty one for the base store):

```go
func TestCheckBackupDestinationBlocksNonEmptyArchiveStore(t *testing.T) {
	t.Parallel()

	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(listEmpty))
	}))
	defer empty.Close()
	nonEmpty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(listNonEmpty))
	}))
	defer nonEmpty.Close()

	cluster := freshArchivingCluster(empty.URL)
	cluster.Spec.Backup.ContinuousArchiving = &mysqlv1alpha1.ContinuousArchivingConfiguration{
		Enabled: true, ObjectStore: storeAt(nonEmpty.URL, "binlogs", "archive"),
	}

	check := guardReconciler(t).checkBackupDestination(context.Background(), cluster)
	if check.Retry != nil {
		t.Fatalf("unexpected retry: %v", check.Retry)
	}
	if !strings.Contains(check.Blocked, "binlogs") {
		t.Fatalf("a non-empty archive store must block and name its bucket, got %q", check.Blocked)
	}
}

func TestCheckBackupDestinationAllowsBothEmpty(t *testing.T) {
	t.Parallel()

	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(listEmpty))
	}))
	defer empty.Close()

	cluster := freshArchivingCluster(empty.URL)
	cluster.Spec.Backup.ContinuousArchiving = &mysqlv1alpha1.ContinuousArchivingConfiguration{
		Enabled: true, ObjectStore: storeAt(empty.URL, "binlogs", "archive"),
	}
	check := guardReconciler(t).checkBackupDestination(context.Background(), cluster)
	if check.Retry != nil || check.Blocked != "" {
		t.Fatalf("empty stores should pass, got blocked=%q retry=%v", check.Blocked, check.Retry)
	}
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/controller/ -run 'TestReconcileRetentionListsBinlogsInArchiveStore|TestClusterDeleteWipes|TestCheckBackupDestinationBlocksNonEmptyArchiveStore'`
Expected: FAIL (binlogs listed in the base store; archive prefix not removed; archive store never checked).

- [ ] **Step 3: Add the client helper** to `cluster_backup_reclaim.go` (or next to `objectStoreConfig`, wherever it is defined):

```go
// objectStoreClient resolves store's Secrets in namespace and returns a client
// for it.
func (r *ClusterReconciler) objectStoreClient(
	ctx context.Context, namespace string, store *mysqlv1alpha1.S3ObjectStore,
) (*objectstore.Client, error) {
	cfg, err := r.objectStoreConfig(ctx, namespace, store)
	if err != nil {
		return nil, err
	}
	return objectstore.NewClient(cfg)
}
```

- [ ] **Step 4: Retention** in `reconcileRetention`, keep the base-store client for base and logical backups, and add an archive client:

```go
	binlogStore := cluster.BinlogObjectStore()
	binlogClient := client
	if !store.SameLocation(binlogStore) {
		if binlogClient, err = r.objectStoreClient(ctx, cluster.Namespace, binlogStore); err != nil {
			return err
		}
	}
```

then read binlogs and the index through it:

```go
	binlogs, err := objectstore.ListArchivedBinlogs(ctx, binlogClient, *binlogStore, cluster.Name)
	...
	indexKey := objectstore.ArchiveIndexKey(*binlogStore, cluster.Name)
	if exists, err := binlogClient.Exists(ctx, binlogStore.Bucket, indexKey); err != nil {
	...
		if err := binlogClient.GetJSON(ctx, binlogStore.Bucket, indexKey, index); err != nil {
```

and apply:

```go
		if err := objectstore.ApplyBackupExpiry(ctx, client, *store, plan); err != nil {
			return err
		}
		if err := objectstore.ApplyBinlogExpiry(ctx, binlogClient, *binlogStore, cluster.Name, plan); err != nil {
			return err
		}
```

- [ ] **Step 5: Reclaim** — turn `cleanupClusterObjectStore` into a loop over the distinct locations:

```go
// cleanupClusterObjectStore removes the cluster's whole archive prefix (every
// base backup, the archived binlogs and the archive index) from the base-backup
// store and, when it is kept apart, from the binary-log archive store. It is a
// no-op when no object store is configured.
func (r *ClusterReconciler) cleanupClusterObjectStore(ctx context.Context, cluster *mysqlv1alpha1.Cluster) error {
	backup := cluster.Spec.Backup
	if backup == nil || backup.ObjectStore == nil {
		return nil
	}
	stores := []*mysqlv1alpha1.S3ObjectStore{backup.ObjectStore}
	if archive := cluster.BinlogObjectStore(); !archive.SameLocation(backup.ObjectStore) {
		stores = append(stores, archive)
	}
	for _, store := range stores {
		if err := r.removeClusterPrefix(ctx, cluster, store); err != nil {
			return err
		}
	}
	return nil
}
```

and move the existing body (client, `RemovePrefix`, log line, `Cleanup` event) into `removeClusterPrefix(ctx, cluster, store)` unchanged, using `r.objectStoreClient`.

- [ ] **Step 6: Empty-destination guard** in `checkBackupDestination`, after the base-prefix check returns "empty", check the archive store the same way when it is another location:

Factor the existing "client → ClusterPrefix → IsEmptyPrefix → Blocked" sequence (everything after the early returns) into:

```go
// checkEmptyPrefix blocks when the cluster's prefix in store already holds
// objects.
func (r *ClusterReconciler) checkEmptyPrefix(
	ctx context.Context, cluster *mysqlv1alpha1.Cluster, store *mysqlv1alpha1.S3ObjectStore,
) backupDestinationCheck {
	osClient, err := r.objectStoreClient(ctx, cluster.Namespace, store)
	if err != nil {
		return backupDestinationCheck{Retry: err}
	}
	prefix := objectstore.ClusterPrefix(*store, cluster.Name)
	empty, err := osClient.IsEmptyPrefix(ctx, store.Bucket, prefix)
	if err != nil {
		return backupDestinationCheck{Retry: err}
	}
	if !empty {
		return backupDestinationCheck{Blocked: fmt.Sprintf(
			"Backup destination s3://%s/%s is not empty; refusing to overwrite an existing archive. "+
				"Use a different cluster name or object-store path, or bootstrap with spec.bootstrap.recovery to restore it",
			store.Bucket, prefix)}
	}
	return backupDestinationCheck{}
}
```

and end `checkBackupDestination` with:

```go
	store := cluster.Spec.Backup.ObjectStore
	if check := r.checkEmptyPrefix(ctx, cluster, store); check.Blocked != "" || check.Retry != nil {
		return check
	}
	// A fresh cluster must not write into another cluster's archive either.
	if archive := cluster.BinlogObjectStore(); !archive.SameLocation(store) {
		return r.checkEmptyPrefix(ctx, cluster, archive)
	}
	return backupDestinationCheck{}
```

This replaces everything in `checkBackupDestination` from `store := cluster.Spec.Backup.ObjectStore` to the end; the early returns above it stay.

- [ ] **Step 7: Run the tests**

Run: `go test ./internal/controller/ -run 'Retention|ClusterDelete|Reclaim|BackupDestination'`
Expected: PASS, including the unchanged single-store tests.

- [ ] **Step 8: Commit**

```bash
git add internal/controller/
git commit -m "feat(cluster): apply retention, reclaim and destination checks per store"
```

---

#### Task 7: Report the archive destination and warn when it moves

**Files:**
- Modify: `internal/controller/cluster_status.go`
- Test: `internal/controller/cluster_archiving_test.go`

**Interfaces:**
- Consumes: `ContinuousArchivingStatus.Destination`, `Cluster.BinlogObjectStore`, `S3ObjectStore.Location` (Task 1)
- Produces: `func (r *ClusterReconciler) recordArchiveMovedEvent(latest, before *mysqlv1alpha1.Cluster)`; event reason constant `eventArchiveMoved = "ArchiveMoved"`

- [ ] **Step 1: Write the failing tests**

```go
func TestArchiveDestinationReported(t *testing.T) {
	cluster := archivingCluster()
	cluster.Spec.Backup.ContinuousArchiving.ObjectStore = &mysqlv1alpha1.S3ObjectStore{Bucket: "binlogs", Path: "archive"}
	if got, want := archiveDestination(cluster), "/binlogs/archive"; got != want {
		t.Fatalf("destination = %q, want %q", got, want)
	}
}

func TestArchiveMovedEventOnlyOnChange(t *testing.T) {
	withDest := func(dest string) *mysqlv1alpha1.Cluster {
		c := archivingCluster()
		c.Status.ContinuousArchiving = &mysqlv1alpha1.ContinuousArchivingStatus{Enabled: true, Destination: dest}
		return c
	}
	recorder := record.NewFakeRecorder(4)
	r := &ClusterReconciler{Recorder: recorder}

	r.recordArchiveMovedEvent(withDest("/backups/cnmsql"), withDest(""))
	r.recordArchiveMovedEvent(withDest("/backups/cnmsql"), withDest("/backups/cnmsql"))
	if len(recorder.Events) != 0 {
		t.Fatal("no event on the first report or without a change")
	}
	r.recordArchiveMovedEvent(withDest("/binlogs/archive"), withDest("/backups/cnmsql"))
	select {
	case ev := <-recorder.Events:
		if !strings.Contains(ev, "Warning ArchiveMoved") ||
			!strings.Contains(ev, "/backups/cnmsql") || !strings.Contains(ev, "/binlogs/archive") {
			t.Fatalf("event = %q", ev)
		}
	default:
		t.Fatal("expected an ArchiveMoved warning")
	}
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/controller/ -run 'TestArchiveDestinationReported|TestArchiveMovedEventOnlyOnChange'`
Expected: compile errors.

- [ ] **Step 3: Implement** in `cluster_status.go`

Next to `aggregateArchiving`:

```go
// eventArchiveMoved is the Warning event reason for a change of the archive
// destination.
const eventArchiveMoved = "ArchiveMoved"

// archiveDestination is the location the cluster archives binary logs to.
func archiveDestination(cluster *mysqlv1alpha1.Cluster) string {
	return cluster.BinlogObjectStore().Location()
}

// recordArchiveMovedEvent warns when the archive destination changed since the
// last reported status. Backups anchored before the move need the old store
// for point-in-time recovery, so the user should know and take a new one.
func (r *ClusterReconciler) recordArchiveMovedEvent(latest, before *mysqlv1alpha1.Cluster) {
	if r.Recorder == nil || latest.Status.ContinuousArchiving == nil || before.Status.ContinuousArchiving == nil {
		return
	}
	from, to := before.Status.ContinuousArchiving.Destination, latest.Status.ContinuousArchiving.Destination
	if from == "" || to == "" || from == to {
		return
	}
	r.Recorder.Eventf(latest, corev1.EventTypeWarning, eventArchiveMoved,
		"Binary-log archive moved from %s to %s; the new archive starts at the oldest binary log on the primary, "+
			"so take a new base backup. Point-in-time recovery from older backups needs the old store", from, to)
}
```

At the aggregation site:

```go
	if cluster.IsArchivingEnabled() {
		observed.ContinuousArchiving = aggregateArchiving(observed)
		observed.ContinuousArchiving.Destination = archiveDestination(cluster)
	}
```

and next to `r.recordBinlogPurgeHeldEvent(latest, wasPurgeHeld)`:

```go
	r.recordArchiveMovedEvent(latest, before)
```

Check `TestAggregateArchivingFromPrimary` still passes (it calls `aggregateArchiving` directly, which is unchanged).

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/controller/ -run 'Archiv'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/controller/cluster_status.go internal/controller/cluster_archiving_test.go
git commit -m "feat(cluster): report the archive destination and warn when it moves"
```

---

#### Task 8: E2E — archive to a separate bucket, recover from it

**Files:**
- Create: `test/e2e/separate_binlog_store_test.go`
- Modify: `test/e2e/archiving_helpers.go` (bucket-aware index read and coverage wait)

**Interfaces:**
- Produces: `readArchiveIndexIn(bucket, cluster string) (objectstore.ArchiveIndex, error)`, `expectArchiveCoversIn(bucket, cluster, want string, timeout time.Duration)`, `objectStoreYAMLFor(indent, bucket string) string`

- [ ] **Step 1: Make the helpers bucket-aware** in `archiving_helpers.go`

```go
// readArchiveIndex fetches the archive index from the suite's bucket.
func readArchiveIndex(cluster string) (objectstore.ArchiveIndex, error) {
	return readArchiveIndexIn(objectStoreBucket, cluster)
}

// readArchiveIndexIn fetches and decodes the cluster-level binlog archive index
// (`<cluster>/binlogs/_index.json`) from bucket. A missing index (the archiver
// has not written one yet) surfaces as an error so callers can poll.
func readArchiveIndexIn(bucket, cluster string) (objectstore.ArchiveIndex, error) {
	var idx objectstore.ArchiveIndex
	key := fmt.Sprintf("%s:%s/%s/binlogs/_index.json", s3Remote, bucket, cluster)
	out, err := rcloneExec("cat", key)
	if err != nil {
		return idx, fmt.Errorf("reading archive index %s: %w (%s)", key, err, out)
	}
	if err := json.Unmarshal([]byte(out), &idx); err != nil {
		return idx, fmt.Errorf("decoding archive index %s: %w (%s)", key, err, out)
	}
	return idx, nil
}
```

Give `expectFlavorArchiveCovers` a bucket: rename its body to `expectFlavorArchiveCoversIn(bucket string, cluster string, flavor engine.Flavor, want string, timeout time.Duration)` calling `readArchiveIndexIn(bucket, cluster)`, keep `expectFlavorArchiveCovers` as a wrapper passing `objectStoreBucket`, and add:

```go
// expectArchiveCoversIn is expectArchiveCovers for an archive kept in bucket.
func expectArchiveCoversIn(bucket, cluster, want string, timeout time.Duration) {
	GinkgoHelper()
	expectFlavorArchiveCoversIn(bucket, cluster, engine.FlavorMySQL, want, timeout)
}
```

In `helpers.go`, make `objectStoreYAML` a wrapper:

```go
func objectStoreYAML(indent string) string {
	return objectStoreYAMLFor(indent, objectStoreBucket)
}

// objectStoreYAMLFor is objectStoreYAML for another bucket of the same store.
func objectStoreYAMLFor(indent, bucket string) string {
	lines := []string{
		"objectStore:",
		"  endpoint: " + objectStoreEndpoint(),
		"  region: us-east-1",
		"  bucket: " + bucket,
		"  forcePathStyle: true",
		"  credentials:",
		"    accessKeyId:",
		"      name: " + objectStoreCredsSecret,
		"      key: ACCESS_KEY_ID",
		"    secretAccessKey:",
		"      name: " + objectStoreCredsSecret,
		"      key: SECRET_ACCESS_KEY",
	}
	for i, l := range lines {
		lines[i] = indent + l
	}
	return strings.Join(lines, "\n")
}
```

- [ ] **Step 2: Write the spec** `test/e2e/separate_binlog_store_test.go`

```go
package e2e

import (
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// The binlog archive goes to its own bucket (continuousArchiving.objectStore)
// while base backups stay in the suite's bucket. PITR from the Backup CR and
// raw-S3 recovery (externalClusters with binlogObjectStore) must both find the
// archive there, and nothing may land under binlogs/ in the base bucket.
var _ = Describe("Separate binlog archive store", Ordered, Label("feature"), func() {
	const (
		sourceCluster = "sep-src"
		fromBackup    = "sep-pitr"
		fromRawS3     = "sep-raw"
		backupName    = "sep-base"
		binlogBucket  = "cnmsql-binlogs"
	)
	version := archiveVersions()[0]

	var (
		password   string
		targetGTID string
		ns, prevNS string
	)

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("sepstore")
		setupObjectStore()
		DeferCleanup(teardownObjectStore)
		setupS3Client()
		DeferCleanup(teardownS3Client)

		By("creating the binlog bucket")
		out, err := rcloneExec("mkdir", fmt.Sprintf("%s:%s", s3Remote, binlogBucket))
		Expect(err).NotTo(HaveOccurred(), "Failed to create the binlog bucket: %s", out)

		By("creating the source cluster archiving to the binlog bucket")
		applyManifest(sourceCluster, separateStoreClusterManifest(sourceCluster, version, binlogBucket))
		DeferCleanup(func() {
			deleteManifest(sourceCluster, separateStoreClusterManifest(sourceCluster, version, binlogBucket))
		})
		expectClusterReady(sourceCluster, 1, 20*time.Minute)
		password = appPassword(sourceCluster)
	})

	It("archives to the binlog bucket and recovers from it", func() {
		primary := clusterPrimary(sourceCluster)

		By("taking a base backup")
		applyManifest(backupName, backupManifest(backupName, sourceCluster))
		DeferCleanup(func() { deleteManifest(backupName, backupManifest(backupName, sourceCluster)) })
		expectBackupCompleted(backupName, 8*time.Minute)
		recorded, err := kubectl("get", "backup", backupName, "-n", testNamespace,
			"-o", "jsonpath={.status.binlogObjectStore.bucket}")
		Expect(err).NotTo(HaveOccurred())
		Expect(recorded).To(Equal(binlogBucket), "Backup did not record the archive store")

		By("writing the target row and waiting for the archive to cover it")
		_, err = mysqlExec(primary, "app", password, "app",
			"CREATE TABLE ledger (id INT PRIMARY KEY, note VARCHAR(32)); INSERT INTO ledger VALUES (1, 'target');")
		Expect(err).NotTo(HaveOccurred())
		targetGTID = flushBinaryLogs(sourceCluster, primary, password)
		expectArchiveCoversIn(binlogBucket, sourceCluster, targetGTID, 5*time.Minute)

		By("writing a row past the target")
		_, err = mysqlExec(primary, "app", password, "app", "INSERT INTO ledger VALUES (2, 'past-target');")
		Expect(err).NotTo(HaveOccurred())
		flushBinaryLogs(sourceCluster, primary, password)

		By("checking nothing was archived to the base bucket")
		out, err := rcloneExec("lsf", "-R", objectKey("%s/binlogs/", sourceCluster))
		Expect(strings.TrimSpace(out)).To(BeEmpty(), "binlogs landed in the base bucket (err=%v)", err)

		for _, tc := range []struct{ name, manifest string }{
			{fromBackup, separateStorePITRFromBackupManifest(fromBackup, version, backupName, targetGTID)},
			{fromRawS3, separateStoreRawRecoveryManifest(fromRawS3, version, sourceCluster, binlogBucket, targetGTID)},
		} {
			By("recovering " + tc.name + " to the target GTID")
			applyManifest(tc.name, tc.manifest)
			DeferCleanup(func() { deleteManifest(tc.name, tc.manifest) })
			expectClusterReady(tc.name, 1, 20*time.Minute)
			restored := clusterPrimary(tc.name)
			Eventually(func(g Gomega) {
				out, err := mysqlExec(restored, "app", password, "app", "SELECT note FROM ledger WHERE id = 1;")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring("target"))
			}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
			out, err := mysqlExec(restored, "app", password, "app", "SELECT COUNT(*) FROM ledger WHERE id = 2;")
			Expect(err).NotTo(HaveOccurred())
			Expect(parseSingleValue(out)).To(Equal("0"), "%s contains a write past the target", tc.name)
		}
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})
```

Manifests at the bottom of the file:

```go
func separateStoreClusterManifest(name, version, binlogBucket string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: 1
  imageName: %[3]s
  storage:
    size: 2Gi
%[4]s
  mysql:
    binlogFormat: ROW
%[5]s
  bootstrap:
    initdb:
      database: app
      owner: app
  backup:
%[6]s
    continuousArchiving:
      enabled: true
      targetRPOSeconds: 10
      maxBinlogSizeMB: 1
%[7]s
`, name, testNamespace, instanceImageFor(version), e2eInstanceResources, e2eMySQLParameters,
		objectStoreYAML("    "), objectStoreYAMLFor("      ", binlogBucket))
}

func separateStorePITRFromBackupManifest(name, version, backup, targetGTID string) string {
	// Same as pitrRecoveryClusterManifest: the Backup's status carries the
	// archive store, so the recovering cluster needs nothing more.
	return pitrRecoveryClusterManifest(name, version, backup, targetGTID)
}

func separateStoreRawRecoveryManifest(name, version, source, binlogBucket, targetGTID string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: 1
  imageName: %[3]s
  storage:
    size: 2Gi
%[4]s
  mysql:
    binlogFormat: ROW
%[5]s
  bootstrap:
    recovery:
      source: %[6]s
      recoveryTarget:
        targetGTID: "%[7]s"
  externalClusters:
    - name: %[6]s
%[8]s
%[9]s
`, name, testNamespace, instanceImageFor(version), e2eInstanceResources, e2eMySQLParameters,
		source, targetGTID, objectStoreYAML("      "),
		strings.Replace(objectStoreYAMLFor("      ", binlogBucket), "objectStore:", "binlogObjectStore:", 1))
}
```

- [ ] **Step 3: Build the suite**

Run: `go vet ./test/e2e/...`
Expected: no errors.

- [ ] **Step 4: Run the spec on Kind**

Run: `./hack/e2e.sh --focus "Separate binlog archive store"` (see `hack/e2e.sh --help` for the cluster/image flags the other lanes use)
Expected: 1 passed. If the Kind cluster is not available, say so in the PR and leave this step unchecked; do not mark it done.

- [ ] **Step 5: Commit**

```bash
git add test/e2e/
git commit -m "test(e2e): archive binlogs to a separate bucket and recover from it"
```

---

#### Task 9: Docs and records

**Files:**
- Modify: `docs/src/pitr.md`, `docs/src/backup-recovery.md`, `docs/src/api-reference.md`
- Modify: `INSTRUCTION.md` (Key Decisions table)
- Modify: `design/036-separate-binlog-storage-and-replica-clusters.md` (status line), `design/INDEX.md` (status column)

- [ ] **Step 1: `pitr.md`**
  - Diagram: split the `Store` subgraph into `BackupStore` (`backup.xbstream`, `metadata.json`) and `ArchiveStore` (`binlogs/<server_uuid>/`, `_index.json`) with a note "same store unless `continuousArchiving.objectStore` is set".
  - Components / integrator responsibilities: "Preserve the object-store bucket/path containing both the base backup and `binlogs/` archive" becomes "Preserve the base-backup store and the archive store (the same one unless `continuousArchiving.objectStore` is set)".
  - Known risks: replace "Separate binlog storage and external replica recovery remain future work" with "Replica clusters (following another cluster's archive or a live source) remain future work; see design 036."

- [ ] **Step 2: `backup-recovery.md`** — add a section "Keeping the binlog archive in its own store" with:
  - a YAML example of `spec.backup.objectStore` plus `continuousArchiving.objectStore` (different bucket and credentials);
  - what goes where (base backups and logical dumps in `backup.objectStore`, `binlogs/` in the archive store);
  - that the instance Pods roll once when the archive store is set or its Secrets change;
  - moving the archive: what `ArchiveMoved` means, that the new store starts at the oldest local binlog, and to take a new base backup;
  - PITR from a `Backup` needs nothing new (`status.binlogObjectStore`);
  - raw-S3 recovery: YAML example with `externalClusters[].objectStore` and `binlogObjectStore`;
  - reclaim `Delete` removes the cluster prefix in both stores.

- [ ] **Step 3: API reference**

Run: `make api-docs`, then copy the new fields' rows (`continuousArchiving.objectStore`, `externalClusters[].binlogObjectStore`, `Backup.status.binlogObjectStore`, `continuousArchiving.destination`) from `docs/src/api-reference-generated.md` into `docs/src/api-reference.md` in its existing format. Do not commit the generated file unless the repo already tracks it (`git ls-files docs/src/api-reference-generated.md`).

- [ ] **Step 4: Records**
  - `INSTRUCTION.md`, Key Decisions, new row D22: "The binlog archive may live in its own object store (`spec.backup.continuousArchiving.objectStore`, default `spec.backup.objectStore`) with the same `<path>/<cluster>/binlogs/` layout; each `Backup` records the archive store it was anchored to (`status.binlogObjectStore`); moving the archive is warned, not blocked" | "Independent bucket, credentials, retention and object lock for binlogs; a Backup keeps finding its archive after spec changes; blocking only the new field would be inconsistent with today's unguarded `backup.objectStore`. See `design/036-separate-binlog-storage-and-replica-clusters.md`".
  - Design 036 status line: "Status: phase 1 done (YYYY-MM-DD); phases 2 and 3 proposed."; INDEX row stays `proposed` (phases 2–3 open) but its description says "Phase 1 (done)".

- [ ] **Step 5: Build the docs**

Run: `cd docs && npm run build`
Expected: build succeeds with no broken links.

- [ ] **Step 6: Full verification and commit**

Run: `make manifests generate lint-fix test`
Expected: no diff from generation, lint clean, all tests PASS.

```bash
git add docs/src/ INSTRUCTION.md design/
git commit -m "docs: document the separate binlog archive store"
```

---

## Phase 2 — MySQL replica clusters (proposed)

### Goal

A Cluster with `spec.replica.enabled` restores a cnmsql base backup of a source
and then keeps applying the source's changes, from a live replication channel to
any GTID MySQL server, from the source's binlog archive, or both. It stays
read-only until it is promoted with `spec.replica.enabled: false`.

### Scope decisions

| # | Decision |
|---|----------|
| R1 | MySQL flavor and async replication mode only. MariaDB is phase 3; Group Replication is rejected |
| R2 | Bootstrap only through `bootstrap.recovery` (`backup` or `source`) without `recoveryTarget`; `initdb` with `spec.replica` is rejected |
| R3 | The live path works with any GTID-enabled MySQL/Percona server; the archive path needs a cnmsql archive layout |
| R4 | Source account changes replicate (app users included). Each replica-cluster instance repairs its own internal accounts locally |
| R5 | Archived binlogs are applied through relay-log injection on a dedicated channel, gated by a spike; `mysqlbinlog \| mysql` replay is the fallback |
| R6 | No demotion of a promoted cluster back to replica |

### API

- `spec.replica` keeps `enabled` and `source`; `unsupportedReason` stops
  rejecting it.
- `ExternalCluster.connectionParameters` documented keys: `host`, `port`
  (default 3306), `user`. Authentication uses `password`, `sslCert`, `sslKey`,
  `sslRootCert`. Without `host` the cluster follows the archive only.
- The external entry referenced by `spec.replica.source` needs `host` or an
  object store (`binlogObjectStore` or `objectStore`).
- `status.replicaCluster`: `following` (`live` | `archive` | `none`),
  `lastAppliedGTID`, `archiveHead`, `lagSeconds`, `lastTransition`.
- Condition `ReplicaClusterFollowing` with reasons including `ArchiveGap`,
  `ArchiveDiverged`, `ApplierError`, `ObjectStoreError`.
- CEL: `enabled` cannot go from false back to true; `spec.replica` with
  `flavor: mariadb` is rejected with a message pointing at phase 3.

### Operator behaviour while `IsReplica()`

- The topology picks a designated primary the same way it picks a primary
  (`targetPrimary`/`currentPrimary`, primary Lease included). Other instances
  replicate from it on the default channel.
- Reconcilers that write SQL are skipped with a `Skipped: replica cluster`
  reason: managed roles, `Database`, `DatabaseUser`, dump account, metrics
  account, semi-sync. `LogicalRestore` is rejected.
- Physical and logical backups, and continuous archiving to the replica
  cluster's own store, stay allowed. Its binlogs carry the source's GTIDs, so
  its archive is a valid PITR archive.
- Services are unchanged; `rw` points at the read-only designated primary.
- Failover inside the replica cluster works as today; the new designated
  primary takes over the external follow.
- Major upgrades: the target series must be at least the source's series when a
  live channel reports it; documented otherwise.
- Promotion (`enabled: false`): the designated primary stops both external
  channels, runs `RESET REPLICA ALL` on them, and promotes on the normal path.
  The skipped reconcilers run afterwards. Fencing the old source is the user's
  job, as in CNPG.

### In-Pod follow engine (designated primary)

Channels:

- `cnmsql_external`: `CHANGE REPLICATION SOURCE TO ... SOURCE_AUTO_POSITION=1
  FOR CHANNEL 'cnmsql_external'`. The password goes through `START REPLICA ...
  USER=, PASSWORD=` so it is never stored in the connection metadata. TLS files
  are written by the manager to a 0600 scratch directory. The manager reads the
  external Secrets by name through the API (D17); the instance Role gains those
  `resourceNames`.
- `cnmsql_archive`: dummy source host, IO thread never started. A follower loop
  reads the source's `_index.json` from the external binlog store (credentials
  in a `cnmsql_SOURCE_S3_*` env set, rendered while `spec.replica` exists so
  promotion does not roll the Pods), plans with `binlog.PlanReplay` from the
  local `gtid_executed` to the latest point, downloads the missing files into
  the channel's relay directory as relay logs (byte cap per batch) and feeds the
  SQL thread. `relay_log_purge` reclaims applied files.

Both channels apply through the replication applier, which works under
`super_read_only=ON`, and a GTID applied on one channel is skipped on the other.

Switching policy:

- No `host`: archive only.
- With `host`: the live channel always runs. The archive channel turns on when
  the live IO thread has been in error for more than `archiveFallbackSeconds`
  (default 60), or at once on error 1236 (source purged needed binlogs). It
  turns off once live is healthy and `gtid_executed` contains the archive head.

Credential repair (every instance of a replica cluster):

- Every 30s the manager opens a fresh connection for each internal account
  (control, backup, dump, metrics). Open connections survive a password change,
  so this is how drift shows up.
- Online repair when an account fails and the control pool still has a
  connection: `SQL_LOG_BIN=0`, `CREATE USER IF NOT EXISTS` / `ALTER USER`
  (password and `REQUIRE`), with `super_read_only` lowered for those statements
  only; `read_only` stays on.
- Offline repair when control itself is locked out: stop mysqld, run the
  restore-time `--skip-grant-tables --skip-log-bin --skip-replica-start`
  credential reconcile, restart.
- Each repair emits an Event naming the account. App and metrics grants follow
  the source until promotion.

Errors (never auto-skip a transaction):

- SQL-thread error on either channel: the channel stops,
  `ReplicaClusterFollowing=False`, reason `ApplierError` with the MySQL error.
- Archive gap (source retention removed files still needed): `ArchiveGap`.
  Following continues on a healthy live channel; otherwise the documented fix is
  a re-bootstrap.
- Index fork that does not contain the local `gtid_executed`:
  `ArchiveDiverged`, following stops.
- Object-store errors: backoff and retry, `ObjectStoreError`.

Lag comes from
`performance_schema.replication_applier_status_by_worker.LAST_APPLIED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP`,
the same for both channels; the manager reports it in `/status` and the operator
aggregates it into `status.replicaCluster`.

### Delivery

- **2a, spike kept as integration tests** (Docker, 8.0, 8.4 and 9.7):
  continuous multi-batch relay-log injection on `cnmsql_archive`, duplicate GTID
  skipping across two channels, apply under `super_read_only=ON`. It settles
  whether new files can be appended while the SQL thread runs or whether each
  batch repositions it (`STOP SQL_THREAD`, `RELAY_LOG_FILE=`, `START`; the safe
  baseline). If a version fails, phase 2 switches to the replay fallback and
  this document is revised before going further.
- **2b, in-Pod engine**: channels, follower, switching policy, credential
  repair, `/status` fields. Table tests for the policy state machine, repair
  detection and online/offline selection.
- **2c, operator and API**: unblock `spec.replica`, CEL, reconciler gating,
  status and condition, Role `resourceNames`, env set, promotion.
- **E2E lane `replica-cluster`** (two clusters, one SeaweedFS): archive-only
  follow with data assertions; live follow; live outage (NetworkPolicy) falls
  back to the archive and returns to live; the source re-applies its dump
  password and the replica cluster survives through repair; failover inside the
  replica cluster keeps following; promotion makes it writable and runs the
  gated reconcilers.
- Docs: new `docs/src/replica-clusters.md`; `pitr.md` future-work line removed.
  `INSTRUCTION.md`: decision D23.

---

## Phase 3 — MariaDB replica clusters (sketch)

- Named connections (`CHANGE MASTER 'cnmsql_external' TO ...`,
  `'cnmsql_archive'`) instead of channels.
- `gtid_ignore_duplicates=ON` so the same domain can arrive on two connections;
  check its interaction with `gtid_strict_mode`.
- Archive segments are keyed by `server_id`; following the head hits the
  positional edge cases of 10.11 anchors on every file, not once at restore.
- Its own spike over 10.11, 11.4 and 12.3 before any code.
- Credential repair is the same as phase 2.

## Upgrade

Phase 1: no change for clusters that do not set the new field. Setting it rolls
the instances once (Pod env). `Backup` objects created before the upgrade have no
`status.binlogObjectStore` and recover from their base store, which is where
their archive was.
