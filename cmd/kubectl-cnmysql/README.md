# kubectl-cnmysql

A `kubectl` plugin for managing and inspecting cloudnative-mysql (Percona Server) clusters.
The binary is `kubectl-cnmysql`; once on your `PATH` it is invoked as
`kubectl cnmysql ...`.

## Install

**From a release:**

```sh
curl -sSfL https://github.com/CloudNative-MySQL/cloudnative-mysql/raw/main/hack/install-cnmysql-plugin.sh | sh -s -- -b ~/.local/bin
```

The script downloads the latest release binary for your platform, verifies its checksum,
and installs the plugin plus a tab-completion shim.

**From the repo (development):**

```sh
make install-plugin   # builds and installs into ~/.local/bin
```

This installs two files (make sure `~/.local/bin` is on your `PATH`):

- `kubectl-cnmysql` — the plugin binary
- `kubectl_complete-cnmysql` — the shell-completion shim (see below)

Verify:

```sh
kubectl cnmysql version
kubectl plugin list | grep cnmysql
```

## Commands

Most commands take an optional `CLUSTER` argument. When omitted, the plugin
defaults to the only cluster in the current namespace (and warns if there are
several).

| Command | Tier | Description |
| --- | --- | --- |
| `status [CLUSTER]` | API | Topology, phase and per-instance health |
| `group status [CLUSTER]` | API | Group Replication view: members, roles, quorum |
| `group recover [CLUSTER]` | API | Request a guarded quorum recovery (last resort) |
| `logs [CLUSTER] [INSTANCE]` | API | Stream pod logs (merged with a prefix) |
| `promote CLUSTER INSTANCE` | API | Planned switchover |
| `fence on\|off CLUSTER INSTANCE` | API | Isolate / restore an instance |
| `restart [CLUSTER] [INSTANCE]` | API | Rolling restart, or one Pod |
| `reinit CLUSTER INSTANCE` | API | Re-init a replica from scratch (destroys data, re-clones) |
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
kubectl cnmysql status -w
kubectl cnmysql metrics -w --watch-interval=5s --filter=mysql_global_status_threads
```

### Group Replication

`group` commands apply only to clusters with
`spec.replication.mode: groupReplication`; they refuse to run against async
clusters. `group status` shows the operator's cross-validated group view —
group name, whether the group is bootstrapped, whether it currently holds
quorum, the elected primary, and a per-member table of state/role/reachability:

```sh
kubectl cnmysql group status -w
```

`group recover` is the one destructive GR command and is gated accordingly:

- **What it does:** stamps the `force-quorum-recovery` annotation on the
  Cluster, asking the operator to force a new membership
  (`group_replication_force_members`) from the most-advanced surviving member.
- **Consequence:** this overrides Paxos consensus. If any member that was
  partitioned away is still running, forcing a new membership can cause
  **split-brain and permanent data loss**.
- **Safety bar:** the plugin refuses unless the group is bootstrapped and has
  *provably lost quorum*, and prints a consequence summary requiring an explicit
  confirmation (`--yes`/`-y` to skip). The annotation is only a request — the
  operator independently re-verifies quorum loss and proves a single safe
  survivor (GTID-dominating every other reachable member) before acting, and
  otherwise leaves the cluster `Blocked`.

#### Command safety matrix

| Command | Mutates | Consequence | Confirmation |
| --- | --- | --- | --- |
| `group status` | no | none (read-only) | — |
| `group recover` | annotates Cluster | forces new membership; split-brain risk if a lost member is still live | prompt unless `--yes` |

### Passwords

`user create`/`user alter` never accept a password as a flag. Use
`--password-stdin` (e.g. piping from a secret) or let the plugin prompt on the
terminal with echo disabled:

```sh
printf '%s' "$PASSWORD" | kubectl cnmysql user create mydb --name=app --password-stdin
```

## Shell completion

Dynamic completion is supported for `CLUSTER` and `INSTANCE` arguments (it lists
clusters/pods in the current namespace).

### As a kubectl plugin (`kubectl cnmysql <TAB>`)

kubectl (>= 1.26) delegates plugin completion to an executable named
`kubectl_complete-cnmysql` on your `PATH`. `make install-plugin` installs it.
Once kubectl's own completion is enabled, `kubectl cnmysql <TAB>` just works:

```sh
# if not already set up:
source <(kubectl completion zsh)   # or bash
```

### Standalone (`kubectl-cnmysql <TAB>`)

The binary also generates standard cobra completion scripts:

```sh
source <(kubectl-cnmysql completion zsh)    # or bash / fish / powershell
```
