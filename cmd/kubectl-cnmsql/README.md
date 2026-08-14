# kubectl-cnmsql

A `kubectl` plugin for managing and inspecting cnmsql (Percona Server) clusters.
The binary is `kubectl-cnmsql`; once on your `PATH` it is invoked as
`kubectl cnmsql ...`.

## Install

**From a release:**

```sh
curl -sSfL https://github.com/cnmsql/cnmsql/raw/main/hack/install-cnmsql-plugin.sh | sh -s -- -b ~/.local/bin
```

The script downloads the latest release binary for your platform, verifies its checksum,
and installs the plugin plus a tab-completion shim.

**From the repo (development):**

```sh
make install-plugin   # builds and installs into ~/.local/bin
```

This installs two files (make sure `~/.local/bin` is on your `PATH`):

- `kubectl-cnmsql` — the plugin binary
- `kubectl_complete-cnmsql` — the shell-completion shim (see below)

Verify:

```sh
kubectl cnmsql version
kubectl plugin list | grep cnmsql
```

## Commands

Most commands take an optional `CLUSTER` argument. When omitted, the plugin
defaults to the only cluster in the current namespace (and warns if there are
several). Commands are grouped under `--help` headings:

- **Cluster Administration:** `status`, `group`, `promote`, `fence`, `restart`,
  `restart-inplace`, `reinit`, `reload`, `backup`, `maintenance`, `destroy`
- **Database Administration:** `user`, `database`, `databaseuser`, `shell`
- **Troubleshooting:** `logs`, `metrics`, `report`, `bench`
- **Miscellaneous:** `version`, `certificate`

| Command | Tier | Description |
| --- | --- | --- |
| `status [CLUSTER]` | API+control | Topology, phase, per-instance health, GTID/lag/uptime, archiving, backups, certs, services, PDBs |
| `group status [CLUSTER]` | API | Group Replication view: members, roles, quorum |
| `group recover [CLUSTER]` | API | Request a guarded quorum recovery (last resort) |
| `logs cluster [CLUSTER] [INSTANCE]` | API | Stream pod logs (merged with a prefix) |
| `logs pretty` | — | Pretty-print structured JSON logs from stdin |
| `promote CLUSTER INSTANCE` | API | Planned switchover |
| `fence on\|off CLUSTER INSTANCE` | API | Isolate / restore an instance |
| `restart [CLUSTER] [INSTANCE]` | API | Rolling restart, or one Pod |
| `restart-inplace [CLUSTER] [INSTANCE]` | control | Re-exec the instance manager in place |
| `reinit CLUSTER INSTANCE` | API | Re-init a replica from scratch (destroys data, re-clones) |
| `reload [CLUSTER]` | API | Re-apply dynamic `my.cnf` params (no restart) |
| `backup [CLUSTER]` | API | Create a `Backup` |
| `maintenance set\|unset [CLUSTER]` | API | Toggle the node maintenance window |
| `destroy CLUSTER INSTANCE` | API | Delete a Pod and its PVC |
| `user create\|alter\|drop\|list [CLUSTER]` | control | Manage MySQL users |
| `database create\|drop\|list [CLUSTER]` | control | Manage MySQL schemas |
| `databaseuser ...` | control | Manage installation-wide `DatabaseUser` resources |
| `shell [CLUSTER]` | control | Open a database client shell on the primary |
| `metrics [CLUSTER] [INSTANCE]` | control | Scrape an instance's Prometheus metrics |
| `report cluster CLUSTER` | API | Collect a diagnostic ZIP for a cluster (read-only; redacts by default) |
| `report operator` | API | Collect a diagnostic ZIP for the operator (read-only; redacts by default) |
| `certificate [SECRET]` | API | Generate a client cert signed by the cluster CA (`--dry-run` to print) |
| `version` | — | Print plugin version information |

"control"-tier commands open an mTLS port-forward to the instance manager.

`status` and `metrics` support `--watch`/`-w` (with `--watch-interval`, default
2s) to refresh continuously until interrupted, like `watch(1)`:

```sh
kubectl cnmsql status -w
kubectl cnmsql metrics -w --watch-interval=5s --filter=mysql_global_status_threads
```

### Color output

All human-readable output respects a global `--color=always|auto|never` flag
(default `auto`, colorizing only when stdout is a terminal). Machine-readable
output (`-o json|yaml`) is never colorized.

```sh
kubectl cnmsql status --color=always
kubectl cnmsql report cluster mydb | kubectl cnmsql logs pretty
```

### Group Replication

`group` commands apply only to clusters with
`spec.replication.mode: groupReplication`; they refuse to run against async
clusters. `group status` shows the operator's cross-validated group view —
group name, whether the group is bootstrapped, whether it currently holds
quorum, the elected primary, and a per-member table of state/role/reachability:

```sh
kubectl cnmsql group status -w
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
| `report cluster` | no | none (read-only; secrets redacted unless `--stop-redaction` only on operator) | — |
| `report operator` | no | none (read-only; secrets/configmaps redacted by default, `--stop-redaction` to keep) | — |
| `certificate` | creates a Secret | adds a client cert Secret | `--dry-run` to print instead |

### Reports

`report cluster` and `report operator` gather diagnostic manifests (and
optional pod logs) into a timestamped ZIP file for support:

```sh
kubectl cnmsql report cluster mydb                # report_cluster_mydb_<ts>.zip
kubectl cnmsql report cluster mydb -o json -l      # JSON manifests + logs
kubectl cnmsql report operator                     # report_operator_<ts>.zip
kubectl cnmsql report operator -S                  # do NOT redact secrets (use with caution)
```

By default secrets and configmaps are redacted (keys kept, values blanked)
and webhook CA bundles are obfuscated. Use `-S/--stop-redaction` on `report
operator` to include raw material.

### Passwords

`user create`/`user alter` never accept a password as a flag. Use
`--password-stdin` (e.g. piping from a secret) or let the plugin prompt on the
terminal with echo disabled:

```sh
printf '%s' "$PASSWORD" | kubectl cnmsql user create mydb --name=app --password-stdin
```

## Shell completion

Dynamic completion is supported for `CLUSTER` and `INSTANCE` arguments (it lists
clusters/pods in the current namespace).

### As a kubectl plugin (`kubectl cnmsql <TAB>`)

kubectl (>= 1.26) delegates plugin completion to an executable named
`kubectl_complete-cnmsql` on your `PATH`. `make install-plugin` installs it.
Once kubectl's own completion is enabled, `kubectl cnmsql <TAB>` just works:

```sh
# if not already set up:
source <(kubectl completion zsh)   # or bash
```

### Standalone (`kubectl-cnmsql <TAB>`)

The binary also generates standard cobra completion scripts:

```sh
source <(kubectl-cnmsql completion zsh)    # or bash / fish / powershell
```
