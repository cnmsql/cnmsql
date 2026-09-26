# 030 — Instance Credentials from the API Server

- **Status:** accepted
- **Milestone:** —
- **Issue:** [#128](https://github.com/cnmsql/cnmsql/issues/128)
- **Supersedes:** none

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Read §1–§5 before starting Task 1: the tasks argue from them.

**Goal:** The instance manager reads its MySQL account passwords from the cluster's credential Secrets through the Kubernetes API, keeps them current through a watch, and the instance Pods stop carrying `MYSQL_*_PASSWORD` env vars.

**Architecture:** A new `pkg/management/mysql/credentials` package gives every manager command a `Source` of passwords. In production the source is a `Provider` that learns the Secret names from the Cluster object, `get`s each Secret, then watches it by name, with a periodic re-get as a fallback. The per-cluster instance Role gains one `get`/`watch` rule scoped by `resourceNames`. Long-lived consumers (control pool, backup streaming, load, dump) take a `func() string` so a rotated Secret reaches the next connection. The Pod spec drops the Secret env vars. That changes the Pod template hash once, and the resulting rolling restart is accepted and called out in the upgrade notes.

**Tech Stack:** Go, client-go typed clientset (`k8s.io/client-go/kubernetes`, fake clientset for tests), controller-runtime client (Cluster lookup), `github.com/go-sql-driver/mysql` v1.10 `BeforeConnect`, cobra, Ginkgo e2e on Kind.

**Spec:** this document (§1–§5) and [issue #128](https://github.com/cnmsql/cnmsql/issues/128).

## 1. Why

The instance manager gets its passwords from Secret-backed env vars set in `internal/controller/cluster_pod.go` (`secretEnv(...)` in `runEnv` / `initEnv`):

| Env var | Secret | Containers |
|---|---|---|
| `MYSQL_CONTROL_PASSWORD` | `<cluster>-control` | bootstrap, import, mysql (also read by `prestop`) |
| `MYSQL_BACKUP_PASSWORD` | `<cluster>-backup` | bootstrap, import, mysql |
| `MYSQL_ROOT_PASSWORD` | `<cluster>-root` or `spec.rootPasswordSecret` | bootstrap, import |
| `MYSQL_APP_PASSWORD` | `<cluster>-app` or `spec.bootstrap.initdb.secret` | bootstrap, import (initdb only) |

This causes three problems:

1. **Every new system account restarts every instance.** A new Secret env var changes the Pod spec, so the template hash changes and every instance rolls on operator upgrade. Logical backups (design 028, LB15) already work around this by carrying the `cnmsql_dump` password in the worker Job and sending it in the `POST /cluster/dump` body.
2. **Rotation needs a restart.** The env is fixed at container start.
3. **The passwords are exposed in more places than they need to be:** the Pod spec (as references), `/proc/<pid>/environ`, and every child process that inherits the environment.

## 2. Scope

### In scope

- A Role rule granting the instance ServiceAccounts `get` + `watch` on the cluster's own credential Secrets, by name.
- A credentials package in the manager: initial load with retry, watch plus periodic re-get, and in-memory values that follow rotation.
- Every manager command that reads a password today (`initdb`, `join`, `restore`, `import`, `run`, `prestop`) uses it.
- Long-lived consumers (control pool, `GET /cluster/backup`, `POST /cluster/load`, `POST /cluster/dump`) read the current value on each use.
- The instance manager reads `<cluster>-dump` itself. The logical backup worker no longer carries the dump password (`CNMSQL_DUMP_PASSWORD`), and `POST /cluster/dump` no longer takes one (`DumpRequest.Password` is removed). This closes the design 028 LB15 workaround.
- Removing the `secretEnv` entries from instance Pods.
- Removing the replication password code path: `MYSQL_REPLICATION_PASSWORD` reads in `initdb`, `join` and `run`, and `BootstrapParams.ReplicationPassword`. The replication account is X.509-only, and the join integration test moves to certificates.
- An upgrade note for the one-time rolling restart (0.8.0).

### Out of scope

- **Changing the MySQL account password when a Secret changes.** For `control`, `backup` and `root`, nothing runs `ALTER USER` when the Secret changes, both today and after this change. The provider only guarantees the manager *uses* the new value. The documented order stays: `ALTER USER` in MySQL, then update the Secret. Only the dump account is re-applied by the operator (`reconcileDumpAccount`).
- **Bootstrap as Jobs (#127).** If it lands, those Jobs need the same Role rule. That is noted in §5, not built here.
- **`replication.SourceOptions.Password`** (library level). Production never sets it after this change, but the replication and group replication integration tests create their own password-based replication user in SQL and drive the library directly.
- **Deleting the `<cluster>-replication` Secret** that `ensureCredentials` still creates. It is unused, but existing clusters have it, and deleting it is a separate cleanup.

## 3. Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| C1 | Secret names come from the **Cluster object at runtime**, through name helpers on `*v1alpha1.Cluster` that the operator's `clusterPlan` also uses | The manager needs only `--cluster-name` and `POD_NAMESPACE`. Adding an account later changes the Role, not the Pod args. Using one set of helpers means the operator and the manager cannot disagree on a name. |
| C2 | Role rule: `apiGroups: [""]`, `resources: [secrets]`, `verbs: [get, watch]`, `resourceNames: <root, app (initdb only), control, backup, dump>`. No `list`, and no object-store or replication Secrets | `list` would expose every Secret in the namespace. Object-store credentials stay in worker Jobs. |
| C3 | Watch one Secret at a time with `fieldSelector=metadata.name=<name>` (which RBAC authorises against `resourceNames`), restart the watch when it closes, and re-get every Secret every 5 minutes | This needs no `list` (so no informer) and recovers from missed events. |
| C4 | Commands take `--credentials-source=secrets\|env`, default `secrets`. `env` reads the legacy `MYSQL_{ROOT,APP,CONTROL,BACKUP}_PASSWORD` vars and exists only for the Docker integration tests and standalone runs | The integration tests run the binary outside Kubernetes. The operator never renders `env`. |
| C5 | The dump password leaves the worker and the API: no `CNMSQL_DUMP_PASSWORD` on the worker Job, no `password` in `DumpRequest`. The manager uses its own copy of `<cluster>-dump`, and a dump fails with `ErrInvalidDumpRequest` until it has read that Secret | 0.8.0 rolls every Pod anyway (C7), so there is no long-lived old-manager population to stay compatible with. The mixed-version window is the rollout itself (§5). |
| C6 | The replication password path is removed: no `MYSQL_REPLICATION_PASSWORD` anywhere, `BootstrapParams.ReplicationPassword` is deleted, and a replication user requires `ReplicationRequireX509` | Production replication is mTLS-only already (`--replication-require-x509`, `--source-ssl-*`). The one test relying on the password path (join integration) moves to X.509. |
| C7 | Dropping the env vars **rolls every instance once** after the upgrade to 0.8.0 (the template hash changes). This is accepted and goes in the upgrade notes and a `!` commit | Keeping the hash stable would require hashing phantom env vars forever. 0.8.0 changes the Pod template anyway. |
| C8 | Long-lived consumers take `PasswordFunc func() string`. The control pool applies it through `mysql.BeforeConnect`, so each new connection uses the current value, and open connections are not dropped | Rotation without reconnect storms. `SetConnMaxLifetime(5m)` already recycles connections. |
| C9 | On startup, `Load` retries with backoff (1s doubling, capped at 30s) until the required Secrets are read or the context ends. Once running, watch errors are logged and the last value is kept | The API server being down at start makes the probes fail, and the kubelet restarts the container. The API server being down later does not affect MySQL. |
| C10 | `prestop` reads the control password with a 5s timeout. If the read fails, it logs and lets shutdown proceed, like it already does when mysqld is unreachable | The preStop hook must never block a drain. |

## 4. Design

### 4.1 Name helpers (`api/v1alpha1/cluster_funcs.go`)

```go
func (cluster *Cluster) RootSecretName() string    // spec.rootPasswordSecret.name, else <name>-root
func (cluster *Cluster) AppSecretName() string     // spec.bootstrap.initdb.secret.name, else <name>-app; "" without initdb
func (cluster *Cluster) ControlSecretName() string // <name>-control
func (cluster *Cluster) BackupSecretName() string  // <name>-backup
func (cluster *Cluster) DumpSecretName() string    // <name>-dump
```

`clusterPlan` (`internal/controller/cluster_plan.go`) and `dumpAccountSecretName` use these helpers. `plan.AppSecretName` keeps its current value (`<name>-app` when there is no initdb), so nothing downstream changes. Only the Role consults `cluster.AppSecretName() == ""`.

### 4.2 Credentials package (`pkg/management/mysql/credentials`)

- `Account` (`Root`, `App`, `Control`, `Backup`, `Dump`)
- `Source` interface: `Password(Account) (string, error)`
- `Static` (a map, `FromEnv()`)
- `Provider` (API-backed: `Load`, `Run`, `Password`)
- `SecretNames(*Cluster) map[Account]string`
- `Open(ctx, Options, required ...Account) (Source, error)`
- `AddFlags(*pflag.FlagSet, *Options)`
- `Getter(Source, Account) func() string`

### 4.3 Consumers

| Command / component | Accounts loaded | How it is used |
|---|---|---|
| `initdb` | Root, Control, Backup, App (when `--database`) | once |
| `join` | Root | once |
| `restore` | Root, Control, Backup | once |
| `import` | Root | once |
| `run` | Control, Backup (when `--backup-user`); Dump is watched but optional | `Getter` into pool / BackupConfig / LoadConfig / DumpConfig; `go provider.Run(ctx)` |
| `prestop` | Control | once, 5s timeout |

The operator adds `--cluster-name=<cluster>` to the `initdb`, `join`, `restore`, `import` and `prestop` invocations. `run` already has it. The namespace comes from `POD_NAMESPACE`, which stays in the env.

## 5. Rollout, compatibility and follow-ups

- **Operator upgrade.** The same reconcile that adds the Role rule (`ensureInstanceRBAC` runs before the Pods) produces a Pod template without the env vars, so every instance rolls once through the normal path: replicas first, then a switchover, then the primary.
  - With `inPlaceInstanceManagerUpdates`, the new binary first runs inside old Pods. It ignores their env vars and reads the Secrets. The Pods still roll afterwards because of the hash change.
- **Mixed versions during the roll.** Old managers read env vars, which their Pods still have. New managers read the API. The Secrets don't change. One exception: a logical backup whose source Pod still runs the old manager fails, because the new worker sends no dump password (C5). The upgrade note says to avoid logical backups until the rollout finishes. A failed `Backup` is terminal, so a `ScheduledBackup` simply takes its next run. With `inPlaceInstanceManagerUpdates` the window is shorter, because the new manager is exec'd into old Pods before they roll.
- **Operator downgrade.** The old operator re-adds the env vars (the hash changes again, so it rolls the Pods back) and drops the Role rule. Until each Pod rolls, the new managers keep their last values (C9). A `prestop` that cannot read the password proceeds without the switchover handoff (C10).
- **#127 (bootstrap Jobs).** Those Jobs run under the instance ServiceAccount or need an equivalent Role rule. Record this on #127 when this lands.

---

## 6. Implementation plan

### Global Constraints

- Go module `github.com/cnmsql/cnmsql`. Do not edit `config/rbac/role.yaml`, `config/crd/bases/*`, `zz_generated.*` or `PROJECT` by hand. Run `make manifests generate` if markers change (they should not).
- Log messages follow the Kubernetes style: a capital first letter, no trailing period, past tense, balanced key/value pairs. **Never log a password value**, and never put one in an error string.
- The Secret key is `password` (`corev1.Secret.Data["password"]`), as `ensurePasswordSecret` writes it.
- Conventional commits: lowercase, no body, no co-author. The Pod spec change commit uses `feat(instance)!:` so git-cliff marks it **breaking** in the release notes.
- After Go edits: `make lint-fix` and `make test`. After doc edits: `npm run build` in `docs/`.
- Existing constants to reuse: `controlUser`, `backupUser`, `replicationUser`, `managerInstanceCmd`, `managerBinary`, `socketPath` in `internal/controller`; `engine.DumpAccountName` in `pkg/engine`.

### Review Focus

1. **A dump request before the manager has read `<cluster>-dump` must fail cleanly** (a 4xx with `ErrInvalidDumpRequest`, no dump client started, no credentials file left behind). Covered in Task 6 (`TestStartDumpRejectsMissingPassword`).
2. **A missing optional Secret must not block startup.** On a recovery cluster there is no app Secret, and on the first reconcile the dump Secret may not exist yet. `run` must start, and `Password(Dump)` returns `ErrNotLoaded` until the Secret appears. Covered in Task 3 (`TestProviderRunPicksUpLateSecret`).
3. **A Secret edited to an empty password must not wipe the known value.** Keep the last good value and log it. Covered in Task 3 (`TestProviderIgnoresEmptyPassword`).
4. **A watch event for a different Secret must not overwrite the account.** The fake clientset (and a misbehaving proxy) ignores field selectors. Covered in Task 3 (`TestProviderFiltersForeignSecret`).
5. **The Role must never grant `list`, and never name object-store or replication Secrets.** Covered in Task 2.
6. **`prestop` must return promptly when the API server is unreachable.** Covered in Task 7 (`TestPrestopProceedsWithoutCredentials`).

---

### Task 1: Credential Secret name helpers on the Cluster type

**Files:**
- Modify: `api/v1alpha1/cluster_funcs.go` (append)
- Test: `api/v1alpha1/cluster_funcs_test.go` (append)
- Modify: `internal/controller/cluster_plan.go:242-272` (use the helpers)
- Modify: `internal/controller/cluster_dump_account.go:37-40` (delegate)

**Interfaces:**
- Produces: `(*Cluster).RootSecretName() string`, `AppSecretName() string` (`""` when `spec.bootstrap.initdb` is nil), `ControlSecretName() string`, `BackupSecretName() string`, `DumpSecretName() string`.

- [ ] **Step 1: Write the failing test** (match the style of the existing file: plain `testing` or Ginkgo, whichever `cluster_funcs_test.go` uses. The Go below uses `testing`.)

```go
func TestCredentialSecretNames(t *testing.T) {
	c := &Cluster{ObjectMeta: metav1.ObjectMeta{Name: "demo"}}
	if got := c.RootSecretName(); got != "demo-root" {
		t.Fatalf("root = %q", got)
	}
	if got := c.AppSecretName(); got != "" {
		t.Fatalf("app without initdb = %q, want empty", got)
	}
	if c.ControlSecretName() != "demo-control" || c.BackupSecretName() != "demo-backup" || c.DumpSecretName() != "demo-dump" {
		t.Fatal("system secret names do not follow <cluster>-<account>")
	}

	c.Spec.Bootstrap = &BootstrapConfiguration{InitDB: &BootstrapInitDB{}}
	if got := c.AppSecretName(); got != "demo-app" {
		t.Fatalf("app with initdb = %q", got)
	}
	c.Spec.Bootstrap.InitDB.Secret = &LocalObjectReference{Name: "mine"}
	c.Spec.RootPasswordSecret = &LocalObjectReference{Name: "my-root"}
	if c.AppSecretName() != "mine" || c.RootSecretName() != "my-root" {
		t.Fatal("user-provided secret names are not honoured")
	}
}
```

- [ ] **Step 2: Run it and check that it fails**

Run: `go test ./api/v1alpha1/ -run TestCredentialSecretNames -v`
Expected: a compile error (`c.RootSecretName undefined`).

- [ ] **Step 3: Implement**

```go
// RootSecretName is the Secret holding root@localhost's password.
func (cluster *Cluster) RootSecretName() string {
	if ref := cluster.Spec.RootPasswordSecret; ref != nil && ref.Name != "" {
		return ref.Name
	}
	return cluster.Name + "-root"
}

// AppSecretName is the Secret holding the application owner's password. It is
// empty when the cluster is not bootstrapped with initdb: a recovered cluster
// takes its application user from the restored data and has no such Secret.
func (cluster *Cluster) AppSecretName() string {
	if cluster.Spec.Bootstrap == nil || cluster.Spec.Bootstrap.InitDB == nil {
		return ""
	}
	if ref := cluster.Spec.Bootstrap.InitDB.Secret; ref != nil && ref.Name != "" {
		return ref.Name
	}
	return cluster.Name + "-app"
}

// ControlSecretName is the Secret holding the instance manager's control account password.
func (cluster *Cluster) ControlSecretName() string { return cluster.Name + "-control" }

// BackupSecretName is the Secret holding the physical backup account's password.
func (cluster *Cluster) BackupSecretName() string { return cluster.Name + "-backup" }

// DumpSecretName is the Secret holding the cnmsql_dump account's password.
func (cluster *Cluster) DumpSecretName() string { return cluster.Name + "-dump" }
```

Then in `cluster_plan.go`, set `RootSecretName: cluster.RootSecretName()`, `ControlSecretName: cluster.ControlSecretName()` and `BackupSecretName: cluster.BackupSecretName()` in the literal, and delete the `RootPasswordSecret` override block. Keep the `AppSecretName` default and override exactly as they are: the plan must keep `<name>-app` when there is no initdb, because `ensureCredentials` and the tests rely on it. Replace only the override block with:

```go
	if name := cluster.AppSecretName(); name != "" {
		plan.AppSecretName = name
	}
```

In `cluster_dump_account.go`: `func dumpAccountSecretName(cluster *mysqlv1alpha1.Cluster) string { return cluster.DumpSecretName() }`.

- [ ] **Step 4: Run the tests**

Run: `go test ./api/v1alpha1/ ./internal/controller/ -count=1`
Expected: PASS (a pure refactor on the controller side).

- [ ] **Step 5: Commit**

```bash
git add api/v1alpha1/cluster_funcs.go api/v1alpha1/cluster_funcs_test.go internal/controller/cluster_plan.go internal/controller/cluster_dump_account.go
git commit -m "refactor(api): name credential secrets from the cluster"
```

---

### Task 2: Instance Role grants get/watch on the cluster's credential Secrets

**Files:**
- Modify: `internal/controller/cluster_rbac.go:46-56`
- Test: `internal/controller/cluster_rbac_test.go` (append)

**Interfaces:**
- Consumes: Task 1 helpers, `clusterPlan.{RootSecretName,ControlSecretName,BackupSecretName,AppSecretName}`.
- Produces: `func instanceSecretNames(cluster *mysqlv1alpha1.Cluster, plan clusterPlan) []string` (sorted and de-duplicated).

- [ ] **Step 1: Write the failing test**

```go
func TestEnsureInstanceRBACScopesCredentialSecrets(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster() // has initdb
	plan := testPlan()
	scheme := testScheme(t)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme: scheme,
	}
	if err := r.ensureInstanceRBAC(ctx, cluster, plan); err != nil {
		t.Fatal(err)
	}
	role := &rbacv1.Role{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + "-instance"}, role); err != nil {
		t.Fatal(err)
	}
	var secretRules []rbacv1.PolicyRule
	for _, rule := range role.Rules {
		if slices.Contains(rule.Resources, "secrets") {
			secretRules = append(secretRules, rule)
		}
	}
	if len(secretRules) != 1 {
		t.Fatalf("want exactly one secrets rule, got %+v", secretRules)
	}
	rule := secretRules[0]
	if !slices.Equal(rule.Verbs, []string{"get", "watch"}) {
		t.Fatalf("secrets verbs = %v, want [get watch] (never list)", rule.Verbs)
	}
	want := []string{plan.AppSecretName, plan.BackupSecretName, plan.ControlSecretName, cluster.DumpSecretName(), plan.RootSecretName}
	slices.Sort(want)
	if !slices.Equal(rule.ResourceNames, want) {
		t.Fatalf("secret names = %v, want %v", rule.ResourceNames, want)
	}
	for _, name := range rule.ResourceNames {
		if name == plan.ReplicationSecret {
			t.Fatal("replication secret must not be readable by instances")
		}
	}
}

func TestEnsureInstanceRBACOmitsAppSecretWithoutInitDB(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Bootstrap = &mysqlv1alpha1.BootstrapConfiguration{Recovery: &mysqlv1alpha1.BootstrapRecovery{}}
	plan := testPlan()
	scheme := testScheme(t)
	r := &ClusterReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(), Scheme: scheme}
	if err := r.ensureInstanceRBAC(ctx, cluster, plan); err != nil {
		t.Fatal(err)
	}
	role := &rbacv1.Role{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + "-instance"}, role); err != nil {
		t.Fatal(err)
	}
	for _, rule := range role.Rules {
		if slices.Contains(rule.Resources, "secrets") && slices.Contains(rule.ResourceNames, plan.AppSecretName) {
			t.Fatalf("app secret granted on a recovery cluster: %+v", rule)
		}
	}
}
```

(If `BootstrapRecovery` needs required fields to build, fill in the minimum the type demands. The test only needs `InitDB == nil`.)

- [ ] **Step 2: Run it and check that it fails**

Run: `go test ./internal/controller/ -run 'TestEnsureInstanceRBAC(ScopesCredentialSecrets|OmitsAppSecret)' -v`
Expected: FAIL (`want exactly one secrets rule, got []`).

- [ ] **Step 3: Implement.** In `ensureInstanceRBAC`, after the `clusters` rule:

```go
			{
				// The instance manager reads its account passwords through the API
				// (design 030). Named Secrets only, and never list: list would
				// expose every Secret in the namespace.
				APIGroups:     []string{""},
				Resources:     []string{"secrets"},
				Verbs:         []string{"get", "watch"},
				ResourceNames: instanceSecretNames(cluster, plan),
			},
```

and add to the same file:

```go
// instanceSecretNames lists the credential Secrets an instance manager reads.
// Object-store and replication Secrets are deliberately absent.
func instanceSecretNames(cluster *mysqlv1alpha1.Cluster, plan clusterPlan) []string {
	names := []string{plan.RootSecretName, plan.ControlSecretName, plan.BackupSecretName, cluster.DumpSecretName()}
	if cluster.AppSecretName() != "" {
		names = append(names, plan.AppSecretName)
	}
	slices.Sort(names)
	return slices.Compact(names)
}
```

Check that the operator's ClusterRole already allows `get`/`watch` on secrets. `config/rbac/role.yaml` lists every verb for `secrets`, so the escalation check passes. Also check the namespaced overlay from design 021 (`config/` namespaced RBAC): it needs the same verbs, so grep for it.

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/controller/ -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/controller/cluster_rbac.go internal/controller/cluster_rbac_test.go
git commit -m "feat(rbac): let instances read their own credential secrets"
```

---

### Task 3: Credentials package: Source, Static, Provider

**Files:**
- Create: `pkg/management/mysql/credentials/credentials.go` (Account, Source, errors, Static, FromEnv, Getter)
- Create: `pkg/management/mysql/credentials/provider.go` (Provider)
- Test: `pkg/management/mysql/credentials/credentials_test.go`, `pkg/management/mysql/credentials/provider_test.go`

**Interfaces:**
- Produces:

```go
type Account string
const (Root, App, Control, Backup, Dump Account = "root", "app", "control", "backup", "dump")
var ErrUnknown = errors.New("no credential secret for this account")
var ErrNotLoaded = errors.New("credential secret not read yet")
type Source interface{ Password(Account) (string, error) }
type Static map[Account]string
func FromEnv() Static
func Getter(s Source, a Account) func() string
type Provider struct{ /* unexported */ }
func NewProvider(c kubernetes.Interface, namespace string, secrets map[Account]string) *Provider
func (p *Provider) Load(ctx context.Context, required ...Account) error
func (p *Provider) Run(ctx context.Context)
func (p *Provider) Password(a Account) (string, error)
```

- [ ] **Step 1: Write the failing tests for Static and FromEnv** (`credentials_test.go`)

```go
package credentials

import (
	"errors"
	"testing"
)

func TestStaticPassword(t *testing.T) {
	s := Static{Control: "c"}
	if p, err := s.Password(Control); err != nil || p != "c" {
		t.Fatalf("Password(Control) = %q, %v", p, err)
	}
	if _, err := s.Password(Dump); !errors.Is(err, ErrUnknown) {
		t.Fatalf("Password(Dump) err = %v, want ErrUnknown", err)
	}
}

func TestFromEnv(t *testing.T) {
	t.Setenv("MYSQL_ROOT_PASSWORD", "r")
	t.Setenv("MYSQL_CONTROL_PASSWORD", "c")
	t.Setenv("MYSQL_REPLICATION_PASSWORD", "rep") // no longer an account: must be ignored
	s := FromEnv()
	if s[Root] != "r" || s[Control] != "c" || len(s) != 2 {
		t.Fatalf("FromEnv = %v", s)
	}
	if _, ok := s[Backup]; ok {
		t.Fatal("unset env vars must be absent, not empty")
	}
}

func TestGetterSwallowsErrors(t *testing.T) {
	if got := Getter(Static{}, Control)(); got != "" {
		t.Fatalf("Getter on missing account = %q", got)
	}
}
```

- [ ] **Step 2: Write the failing Provider tests** (`provider_test.go`). The fake clientset is `k8s.io/client-go/kubernetes/fake`. Watches are driven by a `watch.FakeWatcher` injected through a reactor, so the tests are deterministic.

```go
package credentials

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const ns = "default"

func secret(name, password string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, ResourceVersion: "1"},
		Data:       map[string][]byte{"password": []byte(password)},
	}
}

// fakeWatches makes every Secret watch return the watcher for its field
// selector's name, created on demand, so tests can push events.
func fakeWatches(cs *fake.Clientset) func(name string) *watch.FakeWatcher {
	watchers := map[string]*watch.FakeWatcher{}
	get := func(name string) *watch.FakeWatcher {
		if w, ok := watchers[name]; ok {
			return w
		}
		w := watch.NewFakeWithChanSize(10, false)
		watchers[name] = w
		return w
	}
	cs.PrependWatchReactor("secrets", func(action k8stesting.Action) (bool, watch.Interface, error) {
		sel := action.(k8stesting.WatchAction).GetWatchRestrictions().Fields
		name, _ := sel.RequiresExactMatch("metadata.name")
		return true, get(name), nil
	})
	return get
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestProviderLoad(t *testing.T) {
	cs := fake.NewClientset(secret("demo-control", "c1"))
	p := NewProvider(cs, ns, map[Account]string{Control: "demo-control", Dump: "demo-dump"})
	if err := p.Load(context.Background(), Control); err != nil {
		t.Fatal(err)
	}
	if got, _ := p.Password(Control); got != "c1" {
		t.Fatalf("control = %q", got)
	}
	if _, err := p.Password(Dump); !errors.Is(err, ErrNotLoaded) {
		t.Fatalf("dump err = %v, want ErrNotLoaded", err)
	}
	if _, err := p.Password(Root); !errors.Is(err, ErrUnknown) {
		t.Fatalf("root err = %v, want ErrUnknown", err)
	}
}

func TestProviderLoadRetriesUntilContextEnds(t *testing.T) {
	cs := fake.NewClientset() // secret missing
	p := NewProvider(cs, ns, map[Account]string{Control: "demo-control"})
	p.backoffBase = time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := p.Load(ctx, Control); err == nil {
		t.Fatal("Load succeeded without the secret")
	}
	if n := len(cs.Actions()); n < 2 {
		t.Fatalf("Load tried %d times, want retries", n)
	}
}

func TestProviderWatchRotates(t *testing.T) {
	cs := fake.NewClientset(secret("demo-control", "c1"))
	watches := fakeWatches(cs)
	p := NewProvider(cs, ns, map[Account]string{Control: "demo-control"})
	p.resync = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := p.Load(ctx, Control); err != nil {
		t.Fatal(err)
	}
	go p.Run(ctx)
	watches("demo-control").Modify(secret("demo-control", "c2"))
	waitFor(t, func() bool { v, _ := p.Password(Control); return v == "c2" })
}

func TestProviderFiltersForeignSecret(t *testing.T) {
	cs := fake.NewClientset(secret("demo-control", "c1"))
	watches := fakeWatches(cs)
	p := NewProvider(cs, ns, map[Account]string{Control: "demo-control"})
	p.resync = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = p.Load(ctx, Control)
	go p.Run(ctx)
	w := watches("demo-control")
	w.Modify(secret("someone-else", "evil"))
	w.Modify(secret("demo-control", "c2"))
	waitFor(t, func() bool { v, _ := p.Password(Control); return v == "c2" })
}

func TestProviderIgnoresEmptyPassword(t *testing.T) {
	cs := fake.NewClientset(secret("demo-control", "c1"))
	p := NewProvider(cs, ns, map[Account]string{Control: "demo-control"})
	ctx := context.Background()
	if err := p.Load(ctx, Control); err != nil {
		t.Fatal(err)
	}
	if p.store(ctx, Control, secret("demo-control", "")) {
		t.Fatal("store accepted an empty password")
	}
	if v, _ := p.Password(Control); v != "c1" {
		t.Fatalf("control = %q after an empty update, want c1 kept", v)
	}
}

func TestProviderResyncCatchesMissedUpdate(t *testing.T) {
	cs := fake.NewClientset(secret("demo-control", "c1"))
	fakeWatches(cs) // watches never deliver
	p := NewProvider(cs, ns, map[Account]string{Control: "demo-control"})
	p.resync = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = p.Load(ctx, Control)
	go p.Run(ctx)
	_, err := cs.CoreV1().Secrets(ns).Update(ctx, secret("demo-control", "c2"), metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { v, _ := p.Password(Control); return v == "c2" })
}

func TestProviderRunPicksUpLateSecret(t *testing.T) {
	cs := fake.NewClientset()
	watches := fakeWatches(cs)
	p := NewProvider(cs, ns, map[Account]string{Dump: "demo-dump"})
	p.resync = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)
	watches("demo-dump").Add(secret("demo-dump", "d1"))
	waitFor(t, func() bool { v, _ := p.Password(Dump); return v == "d1" })
}
```

- [ ] **Step 3: Run them and check that they fail**

Run: `go test ./pkg/management/mysql/credentials/ -v`
Expected: a compile error (the package does not exist).

- [ ] **Step 4: Implement `credentials.go`**

```go
// Package credentials gives instance manager commands the passwords of the
// MySQL accounts they drive. In a Pod they come from the cluster's credential
// Secrets through the Kubernetes API (design 030); outside Kubernetes (the
// integration tests) from the legacy MYSQL_*_PASSWORD environment variables.
package credentials

import (
	"errors"
	"os"
)

// Account names one MySQL account whose password the instance manager needs.
type Account string

const (
	Root    Account = "root"
	App     Account = "app"
	Control Account = "control"
	Backup  Account = "backup"
	Dump    Account = "dump"
)

var (
	// ErrUnknown means this cluster has no Secret for the account.
	ErrUnknown = errors.New("no credential secret for this account")
	// ErrNotLoaded means the account's Secret has not been read yet.
	ErrNotLoaded = errors.New("credential secret not read yet")
)

// Source hands out the current password of an account.
type Source interface {
	Password(a Account) (string, error)
}

// Static is a fixed set of passwords.
type Static map[Account]string

func (s Static) Password(a Account) (string, error) {
	if p, ok := s[a]; ok {
		return p, nil
	}
	return "", ErrUnknown
}

var envNames = map[Account]string{
	Root:    "MYSQL_ROOT_PASSWORD",
	App:     "MYSQL_APP_PASSWORD",
	Control: "MYSQL_CONTROL_PASSWORD",
	Backup:  "MYSQL_BACKUP_PASSWORD",
}

// FromEnv reads the legacy MYSQL_*_PASSWORD variables. Unset ones are absent.
func FromEnv() Static {
	s := Static{}
	for a, name := range envNames {
		if v, ok := os.LookupEnv(name); ok {
			s[a] = v
		}
	}
	return s
}

// Getter adapts a Source to the func() string long-lived consumers call before
// each use. An unavailable password reads as empty, which the consumer reports.
func Getter(s Source, a Account) func() string {
	return func() string {
		p, _ := s.Password(a)
		return p
	}
}
```

- [ ] **Step 5: Implement `provider.go`**

```go
package credentials

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	passwordKey       = "password"
	defaultResync     = 5 * time.Minute
	defaultBackoff    = time.Second
	maxBackoff        = 30 * time.Second
)

// Provider reads account passwords from named Secrets and keeps them current.
// It never lists Secrets: the instance Role grants only get and watch by name.
type Provider struct {
	client    kubernetes.Interface
	namespace string
	secrets   map[Account]string

	resync      time.Duration
	backoffBase time.Duration

	mu       sync.RWMutex
	values   map[Account]string
	versions map[Account]string
}

// NewProvider reads the given account → Secret name mapping in namespace.
func NewProvider(c kubernetes.Interface, namespace string, secrets map[Account]string) *Provider {
	return &Provider{
		client: c, namespace: namespace, secrets: secrets,
		resync: defaultResync, backoffBase: defaultBackoff,
		values: map[Account]string{}, versions: map[Account]string{},
	}
}

// Password returns the account's last known password.
func (p *Provider) Password(a Account) (string, error) {
	if _, ok := p.secrets[a]; !ok {
		return "", ErrUnknown
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	v, ok := p.values[a]
	if !ok {
		return "", ErrNotLoaded
	}
	return v, nil
}

// Load reads each required account's Secret, retrying with backoff until all
// have been read once or ctx ends.
func (p *Provider) Load(ctx context.Context, required ...Account) error {
	log := logf.FromContext(ctx).WithName("credentials")
	for _, a := range required {
		if _, ok := p.secrets[a]; !ok {
			return fmt.Errorf("credentials: %s: %w", a, ErrUnknown)
		}
		if err := retry(ctx, p.backoffBase, func() error {
			err := p.refresh(ctx, a)
			if err != nil {
				log.Info("Could not read credential Secret, retrying", "account", string(a),
					"secret", p.secrets[a], "error", err.Error())
			}
			return err
		}); err != nil {
			return fmt.Errorf("credentials: reading %s secret: %w", a, err)
		}
	}
	return nil
}

// retry calls fn until it succeeds or ctx ends, waiting base, then doubling up
// to maxBackoff between attempts. It returns fn's last error on cancellation.
func retry(ctx context.Context, base time.Duration, fn func() error) error {
	delay := base
	for {
		err := fn()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(delay):
		}
		delay = min(delay*2, maxBackoff)
	}
}

// Run keeps every account current until ctx ends: one watch per Secret,
// restarted when it closes, and a re-get of all of them every resync period.
// Errors are logged; the last value read stays in place.
func (p *Provider) Run(ctx context.Context) {
	for a := range p.secrets {
		go p.watch(ctx, a)
	}
	ticker := time.NewTicker(p.resync)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for a := range p.secrets {
				if err := p.refresh(ctx, a); err != nil {
					logf.FromContext(ctx).WithName("credentials").V(1).Info("Could not re-read credential Secret",
						"account", string(a), "error", err.Error())
				}
			}
		}
	}
}

func (p *Provider) watch(ctx context.Context, a Account) {
	name := p.secrets[a]
	log := logf.FromContext(ctx).WithName("credentials").WithValues("account", string(a), "secret", name)
	delay := p.backoffBase
	for ctx.Err() == nil {
		p.mu.RLock()
		rv := p.versions[a]
		p.mu.RUnlock()
		w, err := p.client.CoreV1().Secrets(p.namespace).Watch(ctx, metav1.ListOptions{
			FieldSelector:   fields.OneTermEqualSelector("metadata.name", name).String(),
			ResourceVersion: rv,
		})
		if err != nil {
			log.V(1).Info("Could not watch credential Secret", "error", err.Error())
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			delay = min(delay*2, maxBackoff)
			continue
		}
		delay = p.backoffBase
		p.consume(ctx, a, w)
		w.Stop()
		// The watch closed or expired (410 Gone): re-read to get a fresh
		// resourceVersion before watching again.
		_ = p.refresh(ctx, a)
	}
}

func (p *Provider) consume(ctx context.Context, a Account, w watch.Interface) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-w.ResultChan():
			if !ok {
				return
			}
			if ev.Type == watch.Error {
				return
			}
			if s, isSecret := ev.Object.(*corev1.Secret); isSecret && (ev.Type == watch.Added || ev.Type == watch.Modified) {
				p.store(ctx, a, s)
			}
		}
	}
}

func (p *Provider) refresh(ctx context.Context, a Account) error {
	s, err := p.client.CoreV1().Secrets(p.namespace).Get(ctx, p.secrets[a], metav1.GetOptions{})
	if err != nil {
		return err
	}
	if !p.store(ctx, a, s) {
		return fmt.Errorf("secret %s has no %q key", s.Name, passwordKey)
	}
	return nil
}

// store records s's password for a. It ignores Secrets with another name (a
// watch that does not honour its field selector) and empty passwords (keeping
// the last good value), and reports whether it stored anything.
func (p *Provider) store(ctx context.Context, a Account, s *corev1.Secret) bool {
	if s.Name != p.secrets[a] {
		return false
	}
	v := string(s.Data[passwordKey])
	if v == "" {
		logf.FromContext(ctx).WithName("credentials").Info("Ignored credential Secret without a password",
			"account", string(a), "secret", s.Name)
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if old, ok := p.values[a]; ok && old != v {
		logf.FromContext(ctx).WithName("credentials").Info("Picked up rotated credential Secret",
			"account", string(a), "secret", s.Name)
	}
	p.values[a] = v
	p.versions[a] = s.ResourceVersion
	return true
}
```

- [ ] **Step 6: Run the tests**

Run: `go test ./pkg/management/mysql/credentials/ -race -count=3 -v`
Expected: PASS on all three runs. If `watch.NewFakeWithChanSize` or `GetWatchRestrictions` differ in client-go v0.37, adapt the test helper, not the Provider.

- [ ] **Step 7: Commit**

```bash
git add pkg/management/mysql/credentials/
git commit -m "feat(credentials): read account passwords from named secrets"
```

---

### Task 4: `Open`, `SecretNames` and the shared flag

**Files:**
- Create: `pkg/management/mysql/credentials/open.go`
- Test: `pkg/management/mysql/credentials/open_test.go`

**Interfaces:**
- Consumes: Task 1 helpers, Task 3 types.
- Produces:

```go
type Mode string
const (ModeSecrets Mode = "secrets"; ModeEnv Mode = "env")
type Options struct {
	Mode        Mode
	Namespace   string
	ClusterName string
	RestConfig  *rest.Config         // nil: in-cluster
	Cluster     client.Reader        // test seam; nil: built from RestConfig
	Secrets     kubernetes.Interface // test seam; nil: built from RestConfig
}
func AddFlags(fs *pflag.FlagSet, o *Options)
func SecretNames(c *mysqlv1alpha1.Cluster) map[Account]string
func Open(ctx context.Context, o Options, required ...Account) (Source, error)
```

- [ ] **Step 1: Write the failing tests**

```go
package credentials

import (
	"context"
	"os"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

func TestSecretNames(t *testing.T) {
	c := &mysqlv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "demo"}}
	got := SecretNames(c)
	if _, ok := got[App]; ok {
		t.Fatal("app must be absent without initdb")
	}
	if len(got) != 4 {
		t.Fatalf("SecretNames = %v, want exactly root, control, backup, dump", got)
	}
	if got[Root] != "demo-root" || got[Control] != "demo-control" || got[Backup] != "demo-backup" || got[Dump] != "demo-dump" {
		t.Fatalf("SecretNames = %v", got)
	}
}

func TestOpenSecretsMode(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = mysqlv1alpha1.AddToScheme(scheme)
	cluster := &mysqlv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: ns}}
	src, err := Open(context.Background(), Options{
		Mode: ModeSecrets, Namespace: ns, ClusterName: "demo",
		Cluster: crfake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Secrets: fake.NewClientset(secret("demo-control", "c1")),
	}, Control)
	if err != nil {
		t.Fatal(err)
	}
	if p, _ := src.Password(Control); p != "c1" {
		t.Fatalf("control = %q", p)
	}
	if _, ok := src.(*Provider); !ok {
		t.Fatal("secrets mode must return a *Provider so run can start its watch")
	}
}

func TestOpenEnvMode(t *testing.T) {
	t.Setenv("MYSQL_ROOT_PASSWORD", "r")
	src, err := Open(context.Background(), Options{Mode: ModeEnv}, Root)
	if err != nil {
		t.Fatal(err)
	}
	if p, _ := src.Password(Root); p != "r" {
		t.Fatalf("root = %q", p)
	}
	t.Setenv("MYSQL_BACKUP_PASSWORD", "") // restores the original on cleanup
	_ = os.Unsetenv("MYSQL_BACKUP_PASSWORD")
	if _, err := Open(context.Background(), Options{Mode: ModeEnv}, Backup); err == nil {
		t.Fatal("env mode must fail when a required variable is unset")
	}
}

func TestOpenSecretsModeNeedsCluster(t *testing.T) {
	if _, err := Open(context.Background(), Options{Mode: ModeSecrets, Namespace: ns}); err == nil {
		t.Fatal("secrets mode without --cluster-name must fail")
	}
}
```

- [ ] **Step 2: Run them and check that they fail**

Run: `go test ./pkg/management/mysql/credentials/ -run 'TestSecretNames|TestOpen' -v`
Expected: a compile error.

- [ ] **Step 3: Implement `open.go`**

```go
package credentials

import (
	"context"
	"fmt"

	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// Mode selects where a manager command reads its passwords.
type Mode string

const (
	ModeSecrets Mode = "secrets"
	ModeEnv     Mode = "env"
)

// Options configures Open.
type Options struct {
	Mode        Mode
	Namespace   string
	ClusterName string
	RestConfig  *rest.Config
	Cluster     client.Reader
	Secrets     kubernetes.Interface
}

// AddFlags registers --credentials-source.
func AddFlags(fs *pflag.FlagSet, o *Options) {
	fs.StringVar((*string)(&o.Mode), "credentials-source", string(ModeSecrets),
		"Where to read MySQL passwords: secrets (the Cluster's credential Secrets through the Kubernetes API) "+
			"or env (MYSQL_{ROOT,APP,CONTROL,BACKUP}_PASSWORD, for tests and standalone runs)")
}

// SecretNames maps each account to its Secret on cluster.
func SecretNames(c *mysqlv1alpha1.Cluster) map[Account]string {
	names := map[Account]string{
		Root:    c.RootSecretName(),
		Control: c.ControlSecretName(),
		Backup:  c.BackupSecretName(),
		Dump:    c.DumpSecretName(),
	}
	if app := c.AppSecretName(); app != "" {
		names[App] = app
	}
	return names
}

// Open returns the command's credential source with the required accounts
// already read. In secrets mode it is a *Provider; long-running callers start
// its Run.
func Open(ctx context.Context, o Options, required ...Account) (Source, error) {
	switch o.Mode {
	case ModeEnv:
		s := FromEnv()
		for _, a := range required {
			if _, ok := s[a]; !ok {
				return nil, fmt.Errorf("credentials: %s must be set", envNames[a])
			}
		}
		return s, nil
	case ModeSecrets, "":
	default:
		return nil, fmt.Errorf("credentials: unknown --credentials-source %q", o.Mode)
	}
	if o.ClusterName == "" || o.Namespace == "" {
		return nil, fmt.Errorf("credentials: --cluster-name and POD_NAMESPACE are required to read credential Secrets")
	}
	if err := o.complete(); err != nil {
		return nil, err
	}
	cluster := &mysqlv1alpha1.Cluster{}
	// The API server may still be starting; retry like Load does.
	if err := retry(ctx, defaultBackoff, func() error {
		return o.Cluster.Get(ctx, types.NamespacedName{Namespace: o.Namespace, Name: o.ClusterName}, cluster)
	}); err != nil {
		return nil, fmt.Errorf("credentials: reading cluster %s: %w", o.ClusterName, err)
	}
	p := NewProvider(o.Secrets, o.Namespace, SecretNames(cluster))
	if err := p.Load(ctx, required...); err != nil {
		return nil, err
	}
	return p, nil
}

func (o *Options) complete() error {
	if o.Cluster != nil && o.Secrets != nil {
		return nil
	}
	cfg := o.RestConfig
	if cfg == nil {
		var err error
		if cfg, err = rest.InClusterConfig(); err != nil {
			return fmt.Errorf("credentials: loading in-cluster config: %w", err)
		}
	}
	if o.Secrets == nil {
		cs, err := kubernetes.NewForConfig(cfg)
		if err != nil {
			return err
		}
		o.Secrets = cs
	}
	if o.Cluster == nil {
		scheme := runtime.NewScheme()
		if err := mysqlv1alpha1.AddToScheme(scheme); err != nil {
			return err
		}
		c, err := client.New(cfg, client.Options{Scheme: scheme})
		if err != nil {
			return err
		}
		o.Cluster = c
	}
	return nil
}
```

- [ ] **Step 4: Run the tests**

Run: `go test ./pkg/management/mysql/credentials/ -race -count=1 -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/management/mysql/credentials/
git commit -m "feat(credentials): open a source from the cluster or the env"
```

---

### Task 5: Control pool uses the current password on each new connection

**Files:**
- Modify: `pkg/management/mysql/pool/pool.go` (Config, Open)
- Modify: `pkg/management/mysql/pool/control.go` (ControlParams, ControlConfig)
- Test: `pkg/management/mysql/pool/pool_test.go`, `pkg/management/mysql/pool/control_test.go`

**Interfaces:**
- Produces: `pool.Config.PasswordFunc func() string`; `pool.ControlParams.PasswordFunc func() string`; `func (p ControlParams) CurrentPassword() string`; unexported `passwordHook(f func() string) func(context.Context, *mysql.Config) error`.

- [ ] **Step 1: Write the failing tests**

```go
// pool_test.go
func TestPasswordHookSetsCurrentPassword(t *testing.T) {
	current := "old"
	hook := passwordHook(func() string { return current })
	cfg := mysql.NewConfig()
	current = "new"
	if err := hook(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Passwd != "new" {
		t.Fatalf("Passwd = %q, want the value at connect time", cfg.Passwd)
	}
}

// control_test.go
func TestControlConfigCarriesPasswordFunc(t *testing.T) {
	f := func() string { return "x" }
	cfg := ControlConfig(true, ControlParams{User: "u", PasswordFunc: f})
	if cfg.PasswordFunc == nil || cfg.PasswordFunc() != "x" {
		t.Fatal("PasswordFunc not propagated")
	}
	if (ControlParams{Password: "s"}).CurrentPassword() != "s" {
		t.Fatal("static password fallback broken")
	}
	if (ControlParams{Password: "s", PasswordFunc: f}).CurrentPassword() != "x" {
		t.Fatal("PasswordFunc must win over Password")
	}
}
```

(`pool_test.go` imports `github.com/go-sql-driver/mysql` as `mysql`.)

- [ ] **Step 2: Run them and check that they fail**

Run: `go test ./pkg/management/mysql/pool/ -v`
Expected: a compile error.

- [ ] **Step 3: Implement.** In `pool.go`, replace the blank import with `"github.com/go-sql-driver/mysql"`, then add to `Config`:

```go
	// PasswordFunc, when set, is called before every new connection and wins
	// over Password, so a rotated credential Secret reaches the next connection
	// without closing the ones already open.
	PasswordFunc func() string
```

Rewrite `Open` to go through a connector:

```go
func Open(ctx context.Context, c Config) (*sql.DB, error) {
	dsn, err := c.DSN()
	if err != nil {
		return nil, err
	}
	mc, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("pool: parsing dsn: %w", err)
	}
	if c.PasswordFunc != nil {
		if err := mc.Apply(mysql.BeforeConnect(passwordHook(c.PasswordFunc))); err != nil {
			return nil, fmt.Errorf("pool: %w", err)
		}
	}
	connector, err := mysql.NewConnector(mc)
	if err != nil {
		return nil, fmt.Errorf("pool: opening connection: %w", err)
	}
	db := sql.OpenDB(connector)
	// ...keep the existing SetConnMaxLifetime / SetMaxOpenConns / SetMaxIdleConns / Ping block unchanged
}

func passwordHook(f func() string) func(context.Context, *mysql.Config) error {
	return func(_ context.Context, cfg *mysql.Config) error {
		cfg.Passwd = f()
		return nil
	}
}
```

In `control.go`, add `PasswordFunc func() string` to `ControlParams` (with a comment like the one above), copy it in `ControlConfig` (`PasswordFunc: p.PasswordFunc`), and add:

```go
// CurrentPassword is the control password to use now.
func (p ControlParams) CurrentPassword() string {
	if p.PasswordFunc != nil {
		return p.PasswordFunc()
	}
	return p.Password
}
```

In `pkg/management/mysql/instance/runner.go`, replace the two `opts.Control.Password` reads (the `runUpgrade` call and the `LoadConfig` literal) with `opts.Control.CurrentPassword()`. Task 6 changes the `LoadConfig` one again.

- [ ] **Step 4: Run the tests**

Run: `go test ./pkg/management/mysql/pool/ ./pkg/management/mysql/instance/ -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/management/mysql/pool/ pkg/management/mysql/instance/runner.go
git commit -m "feat(pool): read the password before each new connection"
```

---

### Task 6: Backup, load and dump read the current password; the dump password leaves the worker

**Files:**
- Modify: `pkg/management/mysql/instance/backup.go` (BackupConfig, use at `:96`)
- Modify: `pkg/management/mysql/instance/load.go` (LoadConfig, uses at `:100-131`)
- Modify: `pkg/management/mysql/instance/dump.go` (DumpConfig, StartDump `:117-146`)
- Modify: `pkg/management/mysql/instance/runner.go` (RunOptions gets `DumpPassword func() string`; `SetDumpConfig` / `SetLoadConfig` literals)
- Modify: `pkg/management/mysql/webserver/dump.go:29-35` (delete `DumpRequest.Password`; the type comment should now say the request is a POST because the body carries the database list and extra arguments, not a password)
- Modify: `pkg/management/mysql/backupworker/contract.go:27-29` (delete `EnvDumpPassword`)
- Modify: `internal/controller/backup_controller.go:475` (delete the `secretEnv(backupworker.EnvDumpPassword, ...)` line)
- Modify: `internal/cmd/manager/instance/backup/upload.go:120-125` and `internal/cmd/manager/instance/backup/logical.go:~183` (stop reading and sending the password; `runLogicalUpload` loses its `password` parameter)
- Test: `pkg/management/mysql/instance/dump_test.go`, `load_test.go`, and the backup test file that covers `SetBackupConfig` (find it with `grep -ln SetBackupConfig pkg/management/mysql/instance/*_test.go`)
- Test (update): `pkg/management/mysql/webserver/dump_test.go:91`, `internal/cmd/manager/instance/backup/logical_test.go:204`, `internal/controller/backup_logical_test.go:135,161-175`

**Interfaces:**
- Produces: `BackupConfig.PasswordFunc`, `LoadConfig.PasswordFunc`, `DumpConfig.PasswordFunc` (all `func() string`), each with an unexported `password() string` method (the func wins when set, else the static field; `DumpConfig` has no static field). Also `RunOptions.DumpPassword func() string`. `webserver.DumpRequest` becomes `{Databases, ExtraArgs}`.

- [ ] **Step 1: Write the failing tests.** In `dump_test.go`, find the existing test that checks the credentials file written by `StartDump` (it reads `client.cnf` from the session dir, or stubs the dump binary). Copy its setup and add:

```go
func TestStartDumpUsesManagerPassword(t *testing.T) {
	// setup: same controller + fake dump binary as the existing StartDump test
	c.SetDumpConfig(DumpConfig{Engine: eng, Socket: sock, WorkDir: dir, DumpPath: fakeDump,
		PasswordFunc: func() string { return "from-secret" }})
	session, err := c.StartDump(ctx, webserver.DumpRequest{})
	// assert: err == nil and the credentials file contains "from-secret"
}

func TestStartDumpRejectsMissingPassword(t *testing.T) {
	// PasswordFunc returns "" (the dump Secret has not been read yet)
	// assert: errors.Is(err, webserver.ErrInvalidDumpRequest),
	// the dump slot is released (a second StartDump does not get ErrDumpInProgress),
	// and no cnmsql-dump-* directory is left in WorkDir
}
```

Fill in the setup and the assertions with the exact helpers the existing `dump_test.go` uses. Read it first; the credentials-file assertion pattern already exists there. Existing tests that pass `Password` in `DumpRequest` switch to `PasswordFunc`. Add the same kind of "func wins over static" test for `LoadConfig` in `load_test.go`. For `BackupConfig`, add a small table test of its `password()` method. In `backup_logical_test.go`, flip the logical-job assertion at `:135` so that **no** container env references the `<cluster>-dump` Secret, and delete `TestPhysicalBackupJobHasNoDumpPassword` (it becomes redundant once the constant is gone).

- [ ] **Step 2: Run them and check that they fail**

Run: `go test ./pkg/management/mysql/instance/ ./internal/controller/ -run 'StartDump|Load|Backup|Logical' -v`
Expected: a compile error (`PasswordFunc` unknown).

- [ ] **Step 3: Implement.** For each of `BackupConfig`, `LoadConfig` and `DumpConfig`:

```go
	// PasswordFunc, when set, returns the account's current password and wins
	// over Password, so a rotated credential Secret applies to the next use.
	PasswordFunc func() string
```

```go
func (c *BackupConfig) password() string {
	if c.PasswordFunc != nil {
		return c.PasswordFunc()
	}
	return c.Password
}
```

(`LoadConfig` works the same way. `DumpConfig` has no static `Password` field, so its method returns `""` when the func is nil.) Replace `c.backup.Password` at `backup.go:96` with `c.backup.password()`. In `load.go`, read `pw := cfg.password()` once at the top of the load and use `pw` in the validation (`:100`, `:103`) and in `clientDefaultsFile` (`:131`). In `StartDump`:

```go
	// The dump account's password comes from the <cluster>-dump Secret the
	// manager watches (design 030); until it has been read, refuse the dump.
	password := cfg.password()
	if password == "" {
		return nil, fmt.Errorf("%w: the dump account password has not been read from its Secret yet",
			webserver.ErrInvalidDumpRequest)
	}
	if strings.ContainsAny(password, "\n\r") {
```

Place this check where the current `req.Password` line-break check is (before the dump slot is taken), and use `password` in `clientDefaultsFile(...)`.

In `runner.go`:
- `controller.SetDumpConfig(DumpConfig{..., PasswordFunc: opts.DumpPassword})`, with the comment "the dump account's password comes from its Secret".
- `SetLoadConfig(LoadConfig{..., PasswordFunc: opts.Control.PasswordFunc, Password: opts.Control.Password})`.
- Add `DumpPassword func() string` to `RunOptions` with a doc comment.

On the worker side, delete `EnvDumpPassword`, the `secretEnv` line in `backup_controller.go`, the env read in `upload.go`, and the `Password:` field in the `DumpRequest` built in `logical.go`. Then follow the compile errors. Update the `DumpRequest` type comment in `webserver/dump.go`. The `ensureCredentials` comment in `internal/controller/cluster_resources.go:79-82` ("logical backup worker Jobs carry it instead") becomes "the instance managers read it through the API (design 030)".

- [ ] **Step 4: Run the tests**

Run: `go test ./pkg/... ./internal/... -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/management/mysql/ internal/controller/ internal/cmd/manager/instance/backup/
git commit -m "feat(instance): read backup, load and dump passwords on each use"
```

---

### Task 7: `run` and `prestop` read credentials through the provider

**Files:**
- Modify: `internal/cmd/manager/instance/run/cmd.go`
- Modify: `internal/cmd/manager/instance/prestop/cmd.go`
- Modify: `internal/controller/cluster_pod.go:52-62` (the prestop command gets `--cluster-name`)
- Test: `internal/cmd/manager/instance/prestop/cmd_test.go` (create it if absent; match the package's existing test style)
- Test: `internal/controller/backup_controller_test.go` or a new `internal/controller/cluster_pod_test.go` (assert the prestop args)

**Interfaces:**
- Consumes: `credentials.Options`, `credentials.AddFlags`, `credentials.Open`, `credentials.Getter`, `(*credentials.Provider).Run`, `pool.ControlParams.PasswordFunc`, `instance.BackupConfig.PasswordFunc`, `instance.RunOptions.DumpPassword`.
- Produces: `prestop` flags `--cluster-name` and `--credentials-source`; `run` flag `--credentials-source`.

- [ ] **Step 1: Write the failing tests**

```go
// prestop/cmd_test.go
func TestPrestopProceedsWithoutCredentials(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "default")
	t.Setenv("KUBERNETES_SERVICE_HOST", "") // not in a cluster: in-cluster config fails fast
	cmd := NewCommand()
	cmd.SetArgs([]string{"--cluster-name=demo", "--socket=/nonexistent.sock"})
	start := time.Now()
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("prestop must not fail the hook: %v", err)
	}
	if time.Since(start) > 10*time.Second {
		t.Fatal("prestop blocked on credentials")
	}
}
```

```go
// controller test
func TestPrestopHookNamesCluster(t *testing.T) {
	cluster := baseCluster()
	// enable switchover-on-drain the way the existing tests do (grep IsSwitchoverOnDrainEnabled in *_test.go)
	plan := testPlan()
	spec := (&ClusterReconciler{}).podSpec(cluster, plan, plan.instanceFor(cluster, 1))
	cmd := spec.Containers[0].Lifecycle.PreStop.Exec.Command
	if !slices.Contains(cmd, "--cluster-name="+cluster.Name) {
		t.Fatalf("prestop command %v lacks --cluster-name", cmd)
	}
}
```

- [ ] **Step 2: Run them and check that they fail**

Run: `go test ./internal/cmd/manager/instance/prestop/ ./internal/controller/ -run 'Prestop' -v`
Expected: FAIL (unknown flag `--cluster-name`, and the arg is missing).

- [ ] **Step 3: Implement `prestop`**

```go
	var creds credentials.Options
	// ...
		RunE: func(cmd *cobra.Command, _ []string) error {
			creds.Namespace = os.Getenv("POD_NAMESPACE")
			// preStop must never hold up a drain: a bounded read, and on failure
			// proceed exactly as when mysqld is unreachable.
			readCtx, cancel := context.WithTimeout(cmd.Context(), 5*time.Second)
			src, err := credentials.Open(readCtx, creds, credentials.Control)
			cancel()
			if err != nil {
				fmt.Fprintf(os.Stderr, "prestop: cannot read the control password (%v); proceeding with shutdown\n", err)
				return nil
			}
			password, _ := src.Password(credentials.Control)
			cfg := pool.Config{Socket: socket, User: user, Password: password, MaxOpenConns: 1}
			// ...rest unchanged
		},
	// flags:
	cmd.Flags().StringVar(&creds.ClusterName, "cluster-name", "", "Owning Cluster name; locates the credential Secrets")
	credentials.AddFlags(cmd.Flags(), &creds)
```

In `cluster_pod.go`, add `"--cluster-name=" + cluster.Name,` to the prestop `Command` slice after `managerInstanceCmd, "prestop",`.

- [ ] **Step 4: Implement `run`.** Add `var creds credentials.Options`, register `credentials.AddFlags(cmd.Flags(), &creds)`, and fix the `Long` text ("Passwords are read from the cluster's credential Secrets through the Kubernetes API"). At the top of `RunE`, after `namespace` is resolved:

```go
			creds.Namespace, creds.ClusterName = namespace, clusterName
			required := []credentials.Account{credentials.Control}
			if backupUser != "" {
				required = append(required, credentials.Backup)
			}
			src, err := credentials.Open(cmd.Context(), creds, required...)
			if err != nil {
				return err
			}
			if p, ok := src.(*credentials.Provider); ok {
				go p.Run(cmd.Context())
			}
```

Then:
- `sourceTemplate.Password`: delete the line (C6: `run` never reads a replication password).
- `backup.Password`: replace with `PasswordFunc: credentials.Getter(src, credentials.Backup)`.
- `Control`: replace `Password: os.Getenv(...)` with `PasswordFunc: credentials.Getter(src, credentials.Control)`.
- Add `DumpPassword: credentials.Getter(src, credentials.Dump)` to `instance.RunOptions`.
- Fix the `--backup-user` flag help ("password from the backup credential Secret").

`run` is also the target of in-place re-exec (`syscall.Exec` with the same argv). Check `pkg/management/mysql/webserver/upgrade.go` and `instance/runner.go` (grep `syscall.Exec`) to confirm the argv and env are passed through unchanged, so `--credentials-source` and `--cluster-name` survive the re-exec. An old Pod's argv has no `--credentials-source`, so it gets the default `secrets`, which is correct.

- [ ] **Step 5: Run the tests**

Run: `go test ./internal/cmd/... ./internal/controller/ -count=1`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/cmd/manager/instance/run/ internal/cmd/manager/instance/prestop/ internal/controller/cluster_pod.go internal/controller/*_test.go
git commit -m "feat(instance): run and prestop read credentials from secrets"
```

---

### Task 8: Init commands (`initdb`, `join`, `restore`, `import`) read credentials through the provider

**Files:**
- Modify: `internal/cmd/manager/instance/initdb/cmd.go`
- Modify: `internal/cmd/manager/instance/join/cmd.go`
- Modify: `internal/cmd/manager/instance/restore/cmd.go`
- Modify: `internal/cmd/manager/instance/importdump/cmd.go`
- Modify: `internal/controller/cluster_pod.go` (`initdbArgs` `:259`, `joinArgs` `:295`, `restoreArgs` `:225`)
- Modify: `internal/controller/cluster_import.go` (`importArgs` `:179`)
- Modify: `test/integration/{initdb,join,run,logical,groupreplication,replication}_integration_test.go`: every shell invocation of `manager instance initdb|join|restore|import|run` gets `--credentials-source=env`
- Test: a controller test asserting `--cluster-name=<name>` in all four arg builders

**Interfaces:**
- Consumes: `credentials.Open`, `credentials.AddFlags`.
- Produces: `--cluster-name` and `--credentials-source` on all four commands.

The replication password is left alone in this task (the commands keep reading `MYSQL_REPLICATION_PASSWORD` for now). Task 9 removes it, so this task stays a pure credential-source swap.

- [ ] **Step 1: Write the failing controller test**

```go
func TestBootstrapArgsNameCluster(t *testing.T) {
	cluster := baseCluster()
	plan := testPlan()
	r := &ClusterReconciler{}
	want := "--cluster-name=" + cluster.Name
	for name, args := range map[string][]string{
		"initdb": r.initdbArgs(cluster, cluster.Spec.Bootstrap.InitDB),
		"join":   joinArgs(cluster, plan),
	} {
		if !slices.Contains(args, want) {
			t.Fatalf("%s args %v lack %s", name, args, want)
		}
	}
}
```

`restoreArgs(plan)` and `importArgs(plan)` take only the plan. Give `clusterPlan` a `ClusterName string` field set in the plan builder (`ClusterName: cluster.Name`), have both use `"--cluster-name=" + plan.ClusterName`, and extend the test with `testPlan()` plus `plan.ClusterName = cluster.Name`, including `restoreArgs` and `importArgs` in the map. (Changing the signatures instead would touch more callers.)

- [ ] **Step 2: Run it and check that it fails**

Run: `go test ./internal/controller/ -run TestBootstrapArgsNameCluster -v`
Expected: FAIL.

- [ ] **Step 3: Implement the operator side.** Append `"--cluster-name=" + cluster.Name` (or `plan.ClusterName`) to each of the four arg builders.

- [ ] **Step 4: Implement the commands.** The pattern is the same in all four. Add `var creds credentials.Options`, register `--cluster-name` into `creds.ClusterName`, call `credentials.AddFlags`, and in `RunE` set `creds.Namespace = os.Getenv("POD_NAMESPACE")`, then `src, err := credentials.Open(cmd.Context(), creds, <required>...)`. Read each password with `src.Password(...)`. Required accounts per command:

| Command | Required |
|---|---|
| initdb | Root, plus App when `database != ""`, plus Control when `controlUser != ""`, plus Backup when `backupUser != ""` |
| join | Root |
| restore | Root, plus Control when `controlUser != ""`, plus Backup when `backupUser != ""` |
| import | Root |

Since `Open` checked them, read each password with `p, _ := src.Password(credentials.X)`. In `initdb`, delete the `MYSQL_ROOT_PASSWORD must be set` check, because `Open` reports it. Update each command's `Long` text and flag help: say "from the cluster's credential Secrets", and drop the `MYSQL_{ROOT,APP,CONTROL,BACKUP}_PASSWORD` mentions except in the `--credentials-source` help.

- [ ] **Step 5: Update the integration tests.** For every `manager instance <cmd>` shell line in `test/integration/*.go` that relies on `MYSQL_*_PASSWORD`, append ` --credentials-source=env`. Run `grep -n "manager instance" test/integration/*.go` for the list.

- [ ] **Step 6: Run the tests**

Run: `go test ./internal/... ./pkg/... -count=1` and then `make test-integration` (needs Docker).
Expected: PASS. If the integration tests are too slow to run locally, run at least `go test ./test/integration/ -run 'Initdb|Join' -v`.

- [ ] **Step 7: Commit**

```bash
git add internal/cmd/manager/instance/ internal/controller/ test/integration/
git commit -m "feat(instance): bootstrap commands read credentials from secrets"
```

---

### Task 9: Remove the replication password path

**Files:**
- Modify: `pkg/management/mysql/instance/bootstrap.go` (delete the `ReplicationPassword` field `:38-41`; `Validate` `:109`; the replication account statement `:158-163`)
- Modify: `pkg/management/mysql/instance/bootstrap_test.go` (cases that set `ReplicationPassword`)
- Modify: `internal/cmd/manager/instance/initdb/cmd.go` (drop `ReplicationPassword: os.Getenv(...)` and the `MYSQL_REPLICATION_PASSWORD` mention in `Long`)
- Modify: `internal/cmd/manager/instance/join/cmd.go` (drop `Password: os.Getenv("MYSQL_REPLICATION_PASSWORD")`; fix the `Long` text at `:59-60`)
- Modify: `test/integration/join_integration_test.go` (switch to X.509 replication)
- Create: `test/integration/tls_helpers_test.go` (test CA and certificates)

**Interfaces:**
- Consumes: nothing new.
- Produces: `BootstrapParams` without `ReplicationPassword`. `Validate` rejects `ReplicationUser != "" && !ReplicationRequireX509`. `replication.SourceOptions.Password` stays (library API, used directly by the replication and group replication integration tests).

- [ ] **Step 1: Write the failing unit tests** (`bootstrap_test.go`, in the file's existing style):

```go
func TestBootstrapReplicationUserRequiresX509(t *testing.T) {
	p := BootstrapParams{RootPassword: "r", ReplicationUser: "repl"}
	if err := p.Validate(); err == nil {
		t.Fatal("a replication user without REQUIRE X509 must be rejected")
	}
	p.ReplicationRequireX509 = true
	stmts, err := BootstrapStatements(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range stmts {
		if strings.Contains(s, "'repl'") && strings.HasPrefix(s, "CREATE USER") &&
			(!strings.Contains(s, "REQUIRE X509") || strings.Contains(s, "IDENTIFIED BY")) {
			t.Fatalf("replication account is not X.509-only: %s", s)
		}
	}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	p.ReplicationRequireX509 = false
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "X.509") {
		t.Fatalf("err = %v, want the X.509 requirement named", err)
	}
}
```

Delete the existing cases that build a replication account with a password. There's one per flavor dialect, so grep `ReplicationPassword` in the test file.

- [ ] **Step 2: Run them and check that they fail**

Run: `go test ./pkg/management/mysql/instance/ -run Bootstrap -v`
Expected: FAIL on the last assertion. Today's message is "needs a password or requireX509", which doesn't name X.509 the new way.

- [ ] **Step 3: Implement.**
  - Delete the `ReplicationPassword` field.
  - `Validate`: `if p.ReplicationUser != "" && !p.ReplicationRequireX509 { return fmt.Errorf("bootstrap: the replication user requires X.509 (replication is mTLS-only)") }`
  - Statement: always `d.createUser(p.ReplicationUser, "REQUIRE X509")`.
  - Remove the `MYSQL_REPLICATION_PASSWORD` reads in `initdb` and `join` (Task 7 already removed the one in `run`) and fix the help text.
  - In `join`, keep `--source-get-public-key` (harmless, and useful for a non-X.509 source outside the operator), but nothing sets a password any more.

- [ ] **Step 4: Move the join integration test to X.509.** In `tls_helpers_test.go`, write `func writeTestPKI(t *testing.T, dir string)`. Using `crypto/x509` + `crypto/ecdsa`, it generates a self-signed CA, a server certificate (SAN `127.0.0.1`, `localhost`) and a client certificate (CN `repl`), and writes `ca.crt`, `server.crt`, `server.key`, `client.crt` and `client.key` (PEM, 0644/0600, because mysqld runs as uid 1001 inside the image) into `dir`. Mount `dir` into the container with testcontainers `Files` (`testcontainers.ContainerFile{HostFilePath: ..., ContainerFilePath: "/pki/...", FileMode: 0o644}`). Then change the script:

```bash
export MYSQL_ROOT_PASSWORD=rootpass MYSQL_APP_PASSWORD=%s
...
manager instance initdb --credentials-source=env --mysqld=/usr/sbin/mysqld --config='' \
  --data-dir=$SRC --socket=/tmp/src.sock \
  --database=app --owner=%s --replication-user=repl --replication-require-x509 --server-version=%s
/usr/sbin/mysqld --datadir=$SRC --socket=/tmp/src.sock --port=3306 --server-id=1 $GA \
  --ssl-ca=/pki/ca.crt --ssl-cert=/pki/server.crt --ssl-key=/pki/server.key >/tmp/src.log 2>&1 &
...
manager instance join --credentials-source=env --xtrabackup=xtrabackup --mysqld=/usr/sbin/mysqld --config='' \
  --backup-dir=$BK --data-dir=$REP --socket=/tmp/reptemp.sock \
  --server-version=%s --source-host=127.0.0.1 --source-port=3306 \
  --replication-user=repl --source-ssl \
  --source-ssl-ca=/pki/ca.crt --source-ssl-cert=/pki/client.crt --source-ssl-key=/pki/client.key
```

On MariaDB flavors, check the `ssl` option names the image's server accepts (`f.name` tells you the flavor). MariaDB takes the same `--ssl-ca/--ssl-cert/--ssl-key`. If `f.joinSupported` is false for MariaDB, the test already skips it.

- [ ] **Step 5: Run the tests**

Run: `go test ./pkg/... ./internal/... -count=1` and `go test ./test/integration/ -run 'Join|Initdb' -v` (Docker).
Expected: PASS. The row written on the source must replicate over the X.509 channel.

- [ ] **Step 6: Commit**

```bash
git add pkg/management/mysql/instance/bootstrap.go pkg/management/mysql/instance/bootstrap_test.go internal/cmd/manager/instance/ test/integration/
git commit -m "feat(instance)!: drop the replication password path"
```

---

### Task 10: Drop the Secret env vars from instance Pods

**Files:**
- Modify: `internal/controller/cluster_pod.go:383-437` (`initEnv`, `runEnv`; keep `secretEnv`, which `backup_controller.go:475` still uses)
- Modify: `internal/controller/cluster_import_test.go:377` (expects `MYSQL_ROOT_PASSWORD`)
- Modify: `internal/controller/backup_controller_test.go:358` if it depends on the env layout
- Test: `internal/controller/cluster_pod_env_test.go` (new)

**Interfaces:**
- Consumes: Tasks 7, 8 and 9 (every command reads from Secrets first, and nothing reads the replication password).

- [ ] **Step 1: Write the failing test**

```go
package controller

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestInstancePodCarriesNoPasswordEnv(t *testing.T) {
	cluster := baseCluster()
	plan := testPlan()
	spec := (&ClusterReconciler{}).podSpec(cluster, plan, plan.instanceFor(cluster, 1))
	containers := append(append([]corev1.Container{}, spec.InitContainers...), spec.Containers...)
	for _, c := range containers {
		for _, env := range c.Env {
			if strings.HasSuffix(env.Name, "_PASSWORD") {
				t.Fatalf("container %s still carries %s", c.Name, env.Name)
			}
			if env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil &&
				!strings.HasPrefix(env.Name, "CNMSQL_S3_") && !strings.HasPrefix(env.Name, "AWS_") {
				t.Fatalf("container %s reads secret %s through %s", c.Name, env.ValueFrom.SecretKeyRef.Name, env.Name)
			}
		}
	}
}
```

Check the object-store env var names in `backupObjectStoreEnv` and adjust the two allowed prefixes to match. Object-store Secret refs are expected (archiving, recovery, import). Also add an import-cluster case (reuse `importCluster(...)` from `cluster_import_test.go`) so the `import` init container is covered.

- [ ] **Step 2: Run it and check that it fails**

Run: `go test ./internal/controller/ -run TestInstancePodCarriesNoPasswordEnv -v`
Expected: FAIL (`container bootstrap still carries MYSQL_CONTROL_PASSWORD`).

- [ ] **Step 3: Implement.** Delete the two `secretEnv` lines from `runEnv`. In `initEnv`, delete the root and app `secretEnv` lines and the comment about the app Secret wedging the Pod (the Role handles it now). `initEnv` becomes `return runEnv(nil, plan)`. Keep the function for readability, or inline it. Update the `initEnv` doc comment. In `cluster_import_test.go:377`, drop `"MYSQL_ROOT_PASSWORD"` from the expected list.

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/controller/ -count=1`
Expected: PASS. Tests that pin the Pod template hash to a literal will fail. Update those literals: the change is intended (C7).

- [ ] **Step 5: Commit** (breaking marker for the release notes)

```bash
git add internal/controller/
git commit -m "feat(instance)!: stop passing passwords to instance pods as env vars"
```

---

### Task 11: E2E coverage

**Files:**
- Create: `test/e2e/credentials_test.go`
- Read first: `test/e2e/helpers.go`, `test/e2e/logical_backup_test.go` (the cluster manifest and backup helpers they share, per commit `a638832`), and `test/e2e/e2e_suite_test.go` (labels and tiers)

**Interfaces:**
- Consumes: the existing e2e helpers for creating a cluster, waiting for Ready, running `kubectl`, and taking a logical backup. Use their real names; do not add new helpers when one already exists.

- [ ] **Step 1: Write the spec.** Label it like the other core specs (`Label("core")` or whatever the suite uses). One cluster, `instances: 2`, then these `It` blocks:

1. **No password env.** For each instance Pod, run `kubectl get pod <pod> -o jsonpath='{range .spec.initContainers[*]}{.env[*].name}{" "}{end}{range .spec.containers[*]}{.env[*].name}{end}'` and expect no `_PASSWORD` in the output.
2. **RBAC is scoped.** As `system:serviceaccount:<ns>:<cluster>-1-instance`:
   - `kubectl auth can-i get secret/<cluster>-control` → `yes`
   - `kubectl auth can-i watch secret/<cluster>-dump` → `yes`
   - `kubectl auth can-i list secrets` → `no`
   - `kubectl auth can-i get secret/<cluster>-replication` → `no`
   - For the object-store Secret the suite's MinIO uses: `kubectl auth can-i get secret/<that one>` → `no`
3. **Dump password rotation without a restart.** Record each Pod's `.metadata.uid` and container `restartCount`. Patch `<cluster>-dump` with a new `password` (`kubectl patch secret ... --type merge -p '{"stringData":{"password":"rotated-<random>"}}'`). Wait for `DumpAccountReady` to be `True` with `status.dumpAccountSecretVersion` equal to the Secret's new `resourceVersion` (the operator re-applies the account). Then take a logical backup and expect it to complete. Assert the Pod UIDs and restart counts did not change. The worker no longer carries the password (C5), so a completed backup proves the manager picked up the new value through the watch. Also check the source manager's logs for `Picked up rotated credential Secret` with `account=dump`.
4. **Worker carries no dump password.** The logical backup's worker Job spec has no env var that references `<cluster>-dump`.

- [ ] **Step 2: Run it on a dedicated Kind cluster**

Run: `./hack/e2e.sh --focus 'credentials'` (or the suite's documented focus invocation. Check `hack/e2e.sh --help`.)
Expected: PASS.

- [ ] **Step 3: Run the operator-upgrade specs** (`inplace_operator_upgrade_test.go`), since this change deliberately rolls Pods once on upgrade. If a spec asserts that an upgrade does not roll Pods, adjust its expectation only where the hash change from this release is the cause, and leave a comment pointing to design 030 C7.

- [ ] **Step 4: Commit**

```bash
git add test/e2e/credentials_test.go test/e2e/
git commit -m "test(e2e): cover credential secrets read through the api"
```

---

### Task 12: Docs, design index, decision record

**Files:**
- Modify: `docs/src/security-model.md` (§Database accounts, §Kubernetes RBAC › Per-instance identity, §Current limits and follow-ups)
- Modify: `docs/src/operator-upgrades.md` (§Version-specific upgrade notes: new subsection)
- Modify: `docs/src/logical-backups.md` (where it says the worker carries the dump password)
- Modify: `design/028-logical-backups.md` (LB15 row and §5.10 lines ~683-689: the workaround is gone; the manager reads `<cluster>-dump` itself, and `POST /cluster/dump` takes no password. Point to design 030)
- Modify: `design/INDEX.md` (status of 030 → `accepted` when implementation starts, `done` when merged; add 030 to "Status Authorization & Security" in Quick Navigation)
- Modify: `INSTRUCTION.md` (§Key Decisions: add D17; amend D15's "the worker Job (not the instance Pod) carries its password")

- [ ] **Step 1: Upgrade note** in `operator-upgrades.md` under "Version-specific upgrade notes":

```markdown
### Instances roll once: passwords move from env vars to the API

Instance managers now read their MySQL account passwords from the cluster's
credential Secrets through the Kubernetes API, instead of from `MYSQL_*_PASSWORD`
environment variables. The instance Pods no longer carry those variables, so the
Pod template changes once: after upgrading the operator, every instance restarts
once through the normal rolling update (replicas first, then a switchover, then
the primary), even with `inPlaceInstanceManagerUpdates` enabled.

Each instance's ServiceAccount can `get` and `watch` only its own cluster's
credential Secrets, by name, and cannot `list` Secrets.

A changed credential Secret is now picked up without a restart. Changing the
Secret does not change the MySQL account: run `ALTER USER` first, then update the
Secret.

Logical backups whose source instance has not rolled yet fail during the
rollout, because the backup worker no longer sends the dump account's password.
Avoid taking logical backups until every instance has restarted. Scheduled ones
simply run again at their next slot.

The replication account is now X.509-only: the unused replication password code
path is gone. Clusters managed by the operator already replicate over mTLS, so
this needs no action.
```

- [ ] **Step 2: Security model.** Under Per-instance identity, add the Secret rule (verbs, the names, no `list`, no object-store or replication Secrets). Under Database accounts, say where the passwords come from and how rotation behaves (the out-of-scope note from §2). Remove any statement that passwords are in the Pod env.

- [ ] **Step 3: INSTRUCTION.md D17**

```markdown
| D17 | The instance manager reads its account passwords from the cluster's credential Secrets through the Kubernetes API (`get`/`watch` by name, never `list`), learning the names from the Cluster object; instance Pods carry no password env vars | Adding an account changes the Role, not the Pod spec, so no instance restarts; rotated Secrets apply without a restart; passwords stay out of `/proc/*/environ` and child processes. See `design/030-instance-credentials-from-api.md` |
```

- [ ] **Step 4: Build the docs**

Run: `cd docs && npm run build`
Expected: the build succeeds with no broken links.

- [ ] **Step 5: Commit**

```bash
git add docs/src/ design/ INSTRUCTION.md
git commit -m "docs: instance credentials come from secrets through the api"
```

---

### Final verification (whole branch)

- [ ] `make manifests generate` produces no diff (no markers changed).
- [ ] `make lint` is clean. `make test` passes.
- [ ] `make test-integration` passes.
- [ ] The Task 11 e2e spec plus the operator upgrade specs pass on a dedicated Kind cluster.
- [ ] `grep -rn "MYSQL_.*_PASSWORD" --include='*.go' internal pkg | grep -v _test` shows only `credentials.go` (`envNames`) and help text.
- [ ] `grep -rn -E "MYSQL_REPLICATION_PASSWORD|ReplicationPassword|EnvDumpPassword|CNMSQL_DUMP_PASSWORD" --include='*.go' internal pkg cmd` returns only the ignore-it assertion in `credentials_test.go`.
