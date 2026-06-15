# kubectl-cloudnative-mysql

A `kubectl` plugin for managing and inspecting cloudnative-mysql (Percona Server) clusters.
The binary is `kubectl-cloudnative-mysql`; once on your `PATH` it is invoked as
`kubectl cloudnative-mysql ...`.

## Install

```sh
make install-plugin   # builds and installs into ~/.local/bin
```

This installs two files (make sure `~/.local/bin` is on your `PATH`):

- `kubectl-cloudnative-mysql` — the plugin binary
- `kubectl_complete-cloudnative-mysql` — the shell-completion shim (see below)

Verify:

```sh
kubectl cloudnative-mysql version
kubectl plugin list | grep cloudnative-mysql
```

## Commands

Most commands take an optional `CLUSTER` argument. When omitted, the plugin
defaults to the only cluster in the current namespace (and warns if there are
several).

| Command | Tier | Description |
| --- | --- | --- |
| `status [CLUSTER]` | API | Topology, phase and per-instance health |
| `logs [CLUSTER] [INSTANCE]` | API | Stream pod logs (merged with a prefix) |
| `promote CLUSTER INSTANCE` | API | Planned switchover |
| `fence on\|off CLUSTER INSTANCE` | API | Isolate / restore an instance |
| `restart [CLUSTER] [INSTANCE]` | API | Rolling restart, or one Pod |
| `reload [CLUSTER]` | API | Re-apply dynamic `my.cnf` params (no restart) |
| `backup [CLUSTER]` | API | Create a `Backup` |
| `maintenance set\|unset [CLUSTER]` | API | Toggle the node maintenance window |
| `destroy CLUSTER INSTANCE` | API | Delete a Pod and its PVC |
| `user create\|alter\|drop\|list [CLUSTER]` | control | Manage MySQL users |
| `database create\|drop\|list [CLUSTER]` | control | Manage MySQL schemas |
| `metrics [CLUSTER] [INSTANCE]` | control | Scrape an instance's Prometheus metrics |

"control"-tier commands open an mTLS port-forward to the instance manager.

`status` and `metrics` support `--watch`/`-w` (with `--watch-interval`, default
2s) to refresh continuously until interrupted, like `watch(1)`:

```sh
kubectl cloudnative-mysql status -w
kubectl cloudnative-mysql metrics -w --watch-interval=5s --filter=mysql_global_status_threads
```

### Passwords

`user create`/`user alter` never accept a password as a flag. Use
`--password-stdin` (e.g. piping from a secret) or let the plugin prompt on the
terminal with echo disabled:

```sh
printf '%s' "$PASSWORD" | kubectl cloudnative-mysql user create mydb --name=app --password-stdin
```

## Shell completion

Dynamic completion is supported for `CLUSTER` and `INSTANCE` arguments (it lists
clusters/pods in the current namespace).

### As a kubectl plugin (`kubectl cloudnative-mysql <TAB>`)

kubectl (>= 1.26) delegates plugin completion to an executable named
`kubectl_complete-cloudnative-mysql` on your `PATH`. `make install-plugin` installs it.
Once kubectl's own completion is enabled, `kubectl cloudnative-mysql <TAB>` just works:

```sh
# if not already set up:
source <(kubectl completion zsh)   # or bash
```

### Standalone (`kubectl-cloudnative-mysql <TAB>`)

The binary also generates standard cobra completion scripts:

```sh
source <(kubectl-cloudnative-mysql completion zsh)    # or bash / fish / powershell
```
