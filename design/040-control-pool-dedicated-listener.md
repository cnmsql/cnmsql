# 040 — Control Pool on a Dedicated Listener (MariaDB `extra_port`)

- **Status:** accepted
- **Milestone:** M-MDB follow-up
- **Supersedes:** none (extends [026 §13](026-mariadb-support.md#13-instance-manager-control-plane))

## 1. Problem

The instance manager reaches its local server through a privileged control
pool (`pool.ControlConfig`). Two shortcomings:

1. **MariaDB has no admin interface.** MySQL 8.0.14+ gives the manager a
   loopback `admin_address`/`admin_port` listener that is exempt from
   `max_connections`. MariaDB has no equivalent, so the manager falls back to
   the unix socket and the single reserved `SUPER`/`CONNECTION ADMIN` slot.
   That slot is shared with every other privileged account: if one of them
   holds it while client connections are exhausted, the manager is locked out
   (probes, failover, fencing).
2. **The control pool is capped at one connection everywhere.** The cap exists
   to keep the one reserved slot owned by the manager, but it also applies on
   the admin interface, where it buys nothing. Every control statement —
   `/status`, heartbeat, archiver probes, role transitions, long
   `WAIT_FOR_EXECUTED_GTID_SET` waits — is serialized through one session, so a
   slow statement stalls everything else.

The single shared session also hides a latent bug: `configureSemiSync` runs
`SET SESSION SQL_LOG_BIN=0` on the pool and relies on later statements landing
on the same session. That only holds because the pool has one connection, and
it also means any concurrent control statement in that window runs with
binary logging off.

## 2. MariaDB `extra_port` (verified)

Probed on MariaDB 10.11.19, 11.4.12 and 12.3.2 (`one-thread-per-connection`),
with `max_connections` exhausted by a non-privileged user:

| Probe | Result |
|---|---|
| Main port, non-privileged | `ERROR 1040 Too many connections` |
| `extra_port`, any user | connects — separate budget of `extra_max_connections` |
| `extra_port` with `extra_max_connections` full, `CONNECTION ADMIN` user | connects — the extra port has its **own** reserved privileged slot |
| `extra_port` with `extra_max_connections` full, non-privileged user | `ERROR 1040` |
| Listener address | `bind_address` (no separate address variable) |

Differences from MySQL's admin interface: no privilege check on connect, and it
listens on `bind_address` (pod IP) rather than loopback only.

## 3. Design

### 3.1 Server configuration

- `Engine.HasExtraPort() bool` — `true` for MariaDB, `false` for MySQL.
- `ServerConfig.HasExtraPort` (set by `renderMyCnf` from the engine) renders
  `extra_port = 33062` (reuses `DefaultAdminPort`) and
  `extra_max_connections = pool.ControlPoolSize`.
- `extra_port`/`extra_max_connections` join the operator-owned keys a user
  parameter cannot override.

`extra_port` is not dynamic: existing MariaDB clusters pick it up through the
normal config-hash rolling restart after the operator upgrade.

### 3.2 Control pool

- `pool.ControlPoolSize = 4`. Dedicated listeners (MySQL admin interface,
  MariaDB `extra_port`) get a pool of that size. The socket fallback keeps
  `MaxOpenConns = 1` to own the reserved slot.
- On MariaDB the manager opens the socket pool first, reads
  `@@GLOBAL.extra_port` and, when non-zero, opens a TCP pool to
  `127.0.0.1:<extra_port>` and closes the socket pool. If the variable is zero
  or the TCP pool cannot be opened, it keeps the socket pool and logs why.

  Asking the server rather than trusting the flavor matters because the
  manager and the server config can disagree: an in-place manager upgrade
  re-execs the new manager against a mysqld that was started from the old
  config (the `my.cnf` `subPath` mount is fixed for the Pod's lifetime).
  Trusting the flavor there would make the manager dial a port nobody listens
  on and fail the container, restarting the primary.

### 3.3 Session-dependent code

With more than one connection, anything that relies on session state must pin
a connection (`db.Conn`). Audit of `pkg/` and `internal/`:

- `configureSemiSync` (`SET SESSION SQL_LOG_BIN`) — **fixed**: builds its
  replication manager on a pinned `*sql.Conn`.
- `pool.RunIsolated`, PITR verification, custom-metrics transactions — already
  pin a connection or transaction.
- No `GET_LOCK`, temporary tables, `LAST_INSERT_ID` or other session state on
  the control pool.

## 4. Risks

- **Extra-port squatting.** Any account that can reach the pod IP on 33062 can
  fill `extra_max_connections`. The manager still gets the extra port's
  reserved privileged slot, so it is never locked out, but pool connections
  beyond the first fail with `1040` until the squatters leave — degraded, not
  blind. Restricting access needs a NetworkPolicy; out of scope.
- **Exposure.** The extra port is reachable from the pod network like 3306.
  Authentication is unchanged and the control user's privileges are unchanged,
  so this adds no new credential surface.

## 5. Testing

- Unit: engine capability, config rendering + reserved keys, pool configs
  (sizes, extra-port target), the socket→extra-port switch (sqlmock), semi-sync
  on a pinned connection.
- E2E (MariaDB): the control user's sessions come from `127.0.0.1` (TCP), not
  `localhost` (socket).
