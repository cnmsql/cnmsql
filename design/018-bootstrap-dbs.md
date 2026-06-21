# 018 — Manager Binary Injection at Pod Startup

**Status:** done
**Milestone:** M18

Inject the manager binary from the operator image into each instance Pod via a `bootstrap-controller` init container and shared `scratch-data` emptyDir, following the CNPG pattern.

## Goal

Remove the manager binary from the Percona instance image (`Dockerfile.instance`) so
the operator version and the instance manager version are always identical — the
operator injects its own binary into each MySQL pod via a self-copying init
container and a shared EmptyDir volume.

```
Pod:
  initContainer: bootstrap-controller (operator image)
    /manager bootstrap /controller/manager   → copies binary to shared volume
  initContainer: bootstrap (instance image)
    /controller/manager instance initdb/join/restore  → same binary, instance mode
  container: mysql (instance image)
    /controller/manager instance run          → same binary, instance mode
  volume: scratch-data (EmptyDir) at /controller
```

## Architecture change

Today there are **two separate Go binaries**:
| Binary | Entrypoint | Role |
|---|---|---|
| `cmd/main.go` | `/manager` (in `Dockerfile`) | Operator controller |
| `cmd/manager/main.go` | `/usr/local/bin/manager` (in `Dockerfile.instance`) | Instance manager |

After this change, there is **one binary** that serves all three roles:
| Invocation | Role |
|---|---|
| `/manager` (no args) | Operator controller |
| `/manager bootstrap <dest>` | Copy self to `<dest>` |
| `/manager instance <sub>` | Instance manager (initdb, join, run, etc.) |

`cmd/manager/main.go` is **kept** as a convenience wrapper (e.g. for integration
tests that build the instance manager binary in isolation), but the operator
image contains a single unified binary built from `cmd/main.go`.

---

## Step 1 — Bootstrap subcommand

**New file:** `internal/cmd/manager/bootstrap/cmd.go`

A cobra `Command` (package `bootstrap`) that:
1. Resolves the running binary via `os.Executable()`.
2. Copies it to the destination path given as the first positional arg.
3. Calls `os.Chmod(dest, 0750)`.

Errors are fatal (`os.Exit(1)`).

```go
package bootstrap

import (
    "io"
    "os"

    "github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
    return &cobra.Command{
        Use:   "bootstrap <destination>",
        Short: "Copy the running manager binary to a shared volume",
        Args:  cobra.ExactArgs(1),
        RunE: func(cmd *cobra.Command, args []string) error {
            src, err := os.Executable()
            if err != nil { return err }
            sf, err := os.Open(src)
            if err != nil { return err }
            defer sf.Close()
            df, err := os.OpenFile(args[0], os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0)
            if err != nil { return err }
            defer df.Close()
            if _, err := io.Copy(df, sf); err != nil {
                return err
            }
            return os.Chmod(args[0], 0750)
        },
    }
}
```

---

## Step 2 — Merge instance manager commands into the operator binary

**Modify:** `cmd/main.go`

Currently `cmd/main.go` is a flat `main()` that calls `ctrl.NewManager`. It needs
a cobra root command that dispatches to three branches:

```
/manager
  ├── bootstrap /controller/manager      # new: copy self
  ├── instance [subcommand...]           # existing: instance manager CLI
  └── (default / no matching sub)        # existing: start operator controller
```

The `instance` sub-tree is already built by `internal/cmd/manager/instance.NewCommand()`.
Register both `bootstrap.NewCommand()` and `instance.NewCommand()` on the root.

When the root command's `Run` is reached (no subcommand), run the current
controller-runtime boot sequence.

**Important:** `cmd/manager/main.go` remains unchanged. The `instance` command
package (`internal/cmd/manager/instance`) is already a reusable cobra command
tree — both entrypoints (`cmd/main.go` and `cmd/manager/main.go`) can register it.

The operator's cobra root must accept all the existing operator flags
(`--metrics-bind-address`, `--leader-elect`, etc.) which today are defined as
`flag` package globals. Convert them to cobra persistent flags or bind them as
`flag.CommandLine` flags before `cmd.Execute()`.

Pseudocode for `cmd/main.go`:

```go
func main() {
    root := &cobra.Command{
        Use: "manager",
        RunE: func(cmd *cobra.Command, args []string) error {
            // existing controller-runtime setup...
            mgr, err := ctrl.NewManager(...)
            // register controllers...
            return mgr.Start(ctx)
        },
    }
    // register persistent flags (metrics-bind-address, leader-elect, ...)
    root.AddCommand(bootstrap.NewCommand())
    root.AddCommand(instance.NewCommand())
    if err := root.Execute(); err != nil {
        os.Exit(1)
    }
}
```

---

## Step 3 — Pass the operator image name to the controller

The controller needs to know which image to use for the `bootstrap-controller` init
container. Add a command-line flag:

```go
// in cmd/main.go
root.PersistentFlags().String("operator-image", "", "Operator image name (used for bootstrap-controller init container)")
```

**Modify:** `internal/controller/cluster_controller.go`

Add `OperatorImageName string` field to `ClusterReconciler`:

```go
type ClusterReconciler struct {
    // ... existing fields ...
    OperatorImageName string
}
```

When registering the reconciler, pass the flag value:

```go
&controller.ClusterReconciler{
    // ... existing fields ...
    OperatorImageName: operatorImageName,
}
```

The operator deployment is responsible for setting the flag to its own image
(e.g. via the downward API or a build-time default).

**Modify:** `config/manager/manager.yaml` and `config/default/manager_auth_proxy_patch.yaml`
Add `--operator-image=$(OPERATOR_IMAGE)` to the manager container args,
and set `OPERATOR_IMAGE` via the downward API or an env substitution.

Alternatively, use the same approach as CNPG: read the image from the operator
pod's own container status at startup.

---

## Step 4 — Extend the cluster plan

**Modify:** `internal/controller/cluster_plan.go`

Add `OperatorImage string` to `clusterPlan`:

```go
type clusterPlan struct {
    // ... existing fields ...
    OperatorImage string
}
```

Populate it in `buildPlan()`:

```go
plan.OperatorImage = r.OperatorImageName
```

If `OperatorImageName` is empty, fall back to the instance image (graceful
degradation for development / bare `make run`).

---

## Step 5 — Modify the pod spec

**Modify:** `internal/controller/cluster_pod.go`

### 5a — Add scratch-data volume

Add a new EmptyDir volume to the pod spec, placed **before** the bootstrap-controller
init container references it:

```go
{Name: "scratch-data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
```

### 5b — Add bootstrap-controller init container (first in order)

Insert before the existing `"bootstrap"` init container. This container uses the
**operator image** (not `plan.Image`):

```go
{
    Name:            "bootstrap-controller",
    Image:           plan.OperatorImage,
    ImagePullPolicy: cluster.Spec.ImagePullPolicy,
    Command:         []string{"/manager"},
    Args:            []string{"bootstrap", "/controller/manager"},
    VolumeMounts: []corev1.VolumeMount{
        {Name: "scratch-data", MountPath: "/controller"},
    },
    SecurityContext: cluster.Spec.SecurityContext,
},
```

### 5c — Bootstrap init container: switch to the shared binary

The existing `"bootstrap"` init container currently uses `plan.Image` (the
Percona image) and its entrypoint is `/usr/local/bin/manager` (built into the
image). After removing the binary from the instance image, the bootstrap
container needs to use the shared binary from the EmptyDir:

- Add `Command: []string{"/controller/manager"}` (explicitly override the image's
  absent/unconfigured entrypoint).
- Add the `scratch-data` volume mount:
  ```go
  {Name: "scratch-data", MountPath: "/controller"},
  ```

### 5d — MySQL container: switch to the shared binary

- Add `Command: []string{"/controller/manager"}` (explicitly override the entrypoint).
- Add the `scratch-data` volume mount:
  ```go
  {Name: "scratch-data", MountPath: "/controller"},
  ```

The container `Args` (`instance run ...`) are unchanged.

### 5e — volumeMounts()

Add the `scratch-data` mount to the shared `volumeMounts()` helper:

```go
func volumeMounts() []corev1.VolumeMount {
    return []corev1.VolumeMount{
        // ... existing mounts ...
        {Name: "scratch-data", MountPath: "/controller"},
    }
}
```

Then use `volumeMounts()` for all three containers (bootstrap-controller,
bootstrap, mysql). The bootstrap-controller only really needs `scratch-data`, but
sharing the same mount list is harmless and cleaner.

---

## Step 6 — Strip the manager binary from `Dockerfile.instance`

**Modify:** `Dockerfile.instance`

Remove everything related to the Go builder:

1. Remove the `FROM golang:${GO_VERSION} AS builder` stage entirely (lines 28–39).
2. Remove the `GO_VERSION` ARG (line 22) — no longer needed.
3. Remove the `COPY --from=builder /out/manager /usr/local/bin/manager` (line 120).
4. Remove the `ENTRYPOINT ["/usr/local/bin/manager"]` (line 124).
5. Remove the `CMD ["instance", "run"]` (line 125) — the pod spec fully controls
   the container command now.

The image now contains only Percona Server, XtraBackup, and the trimmed OS
dependencies. No Go toolchain, no module cache, no compiled binary.

The images build script (`images/build.sh`) does not need changes — it calls
`Dockerfile.instance` with build args that are still valid (only `PS_REPO`,
`PXB_REPO`, `PXB_PACKAGE`, `REPO_COMPONENT` remain).

---

## Step 7 — RBAC adjustments

**Modify:** `internal/controller/cluster_controller.go`

Add an RBAC marker so the operator can read its own Pod (for self-image
discovery, if we implement that later) — or add nothing if we use a flag/env.
The flag-based approach requires no new RBAC.

If using the pod-based self-discovery approach (optional future enhancement):

```go
// +kubebuilder:rbac:groups="",resources=pods,verbs=get
```

(This is already covered by the existing Pod RBAC markers.)

---

## Step 8 — Update deployment manifests

**Modify:** `config/manager/manager.yaml`

Add the `--operator-image` flag to the deployment. Use a placeholder that
kustomize or Helm replaces at deploy time.

```yaml
args:
  - --operator-image=$(OPERATOR_IMAGE)
env:
  - name: OPERATOR_IMAGE
    value: "cnmsql:latest"  # placeholder, overridden by kustomize/Helm
```

**Modify:** `config/default/kustomization.yaml`

Set `OPERATOR_IMAGE` via a `configMapGenerator` / `secretGenerator` or an
images override, or rely on Helm to inject it.

For the kustomize (dist) path, patch the deployment with the image:

```yaml
# config/default/manager_image_patch.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: controller-manager
  namespace: system
spec:
  template:
    spec:
      containers:
        - name: manager
          env:
            - name: OPERATOR_IMAGE
              valueFrom:
                fieldRef:
                  fieldPath: spec.containers[0].image  # or a hardcoded value via kustomize images
```

**Note:** `fieldRef` referencing container images is not supported natively.
Use kustomize's `images` transformer or Helm templating. The simplest approach
is to accept the flag at deploy time and set it to match `$IMG`.

---

## Step 9 — Remove / keep `cmd/manager/main.go`

`cmd/manager/main.go` is **kept** but its purpose is now a developer convenience:
it builds a standalone instance manager binary (e.g. for `make test-integration`).

No changes required to `cmd/manager/main.go` or the packages under
`internal/cmd/manager/`. They continue to work as before.

---

## Implementation order

| # | What | Risk |
|---|------|------|
| 1 | Create `internal/cmd/manager/bootstrap/cmd.go` | Low — new file, no dependencies |
| 2 | Convert `cmd/main.go` to cobra with operator flags + instance subcommand | Medium — touches operator entrypoint |
| 3 | Add `--operator-image` flag to `cmd/main.go` and wire into reconcile | Low |
| 4 | Add `OperatorImage` to `clusterPlan` + `ClusterReconciler` | Low |
| 5 | Modify `podSpec()`: add volume, bootstrap-controller init container, Command overrides | Medium — pod spec generation |
| 6 | Strip `Dockerfile.instance` | Low — removal only |
| 7 | Update deployment manifests (`config/`) | Low |
| 8 | Run `make manifests generate test lint-fix` | Verification |

Steps 1–2 are the core refactoring. Steps 3–5 are the actual injection logic.
Steps 6–7 clean up the build and deployment. Step 8 validates everything.

## Rollback strategy

If something goes wrong, the `Dockerfile.instance` and pod spec changes can be
reverted independently of the cobra refactoring (steps 1–2). Keep `cmd/manager/main.go`
functional throughout so the instance image build path still works during
development.
