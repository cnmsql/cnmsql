# 043 — Runtime application of `spec.mysql.parameters` changes

Status: accepted (2026-11-24)

Related: 002 (rendered my.cnf), 040 (control pool), 037 (slow log).

## Problem

Changing `spec.mysql.parameters` re-renders my.cnf, the rendered config is
hashed into the pod template hash, and every instance is deleted and recreated.
Most MySQL settings are dynamic and can be applied with `SET GLOBAL`; rolling a
busy cluster for them drops every connection and empties the buffer pool. The
runtime path already exists — `POST /reload` runs `SET GLOBAL` per parameter
and reports per-parameter `Applied`/`Skipped`, and `reconcileReload` drives it
from the `cnmsql.cnmsql.co/reload` annotation with per-pod
`reload-applied` tokens — but it is manual and does not stop the roll.

## Decision

**The probe is the classifier.** No per-version table of dynamic variables.
The instance manager probes each changed parameter with `SET GLOBAL` and the
server's own refusal decides the classification:

- applied → runtime-settable; the value is absorbed into the Runtime record
  and the pod template hash does not move;
- skipped (read-only variable, rejected value, anything) → restart-required;
  the value is folded into the Template record so the template hash moves and
  the existing serialized roll applies it from my.cnf — exactly today's
  behavior for anything the server refuses;
- a **removed** parameter is always restart-required: a global cannot be unset
  without knowing its default, so removals converge through the roll.

The roll decision never depends on server state observed after the fact: the
probe result arrives before the hash is recomputed, because the parameter pass
runs before `reconcileInstances` in every reconcile.

**State is one Cluster annotation**, `cnmsql.cnmsql.co/parameters-state`
(JSON), holding:

- `runtime` — values known applied to running instances (via `SET GLOBAL` or
  baked into templates at creation/roll). It is the diff basis for the spec;
- `template` — values the pod template hash is rendered from. It only moves
  for restart-required changes, mirroring the seed-list trick that keeps the
  hash stable across scale changes;
- `pending` — the change set currently being probed (token, fingerprint,
  per-parameter classification). A new or edited spec produces a new
  fingerprint and a fresh reload token, so partial evidence for an abandoned
  change is discarded.

**The automatic path reuses the manual reload pass.** When a diff appears, the
operator stamps `reloadAnnotation` with a fresh RFC3339 token (the same
annotation `kubectl cnmsql reload` stamps) and runs the shared
`applyReload` pass — same endpoint, same per-pod tokens, same not-ready
skip/catch-up. The request content depends on who stamped the token: a token
matching the pending change set sends **only the changed parameters**, so
manual `SET GLOBAL` drift on unchanged parameters survives until an explicit
reload; any other token sends the full spec (drift correction, unchanged
semantics). Outcomes are recorded into the pending record from any driver, so
a manual reload that happens to run first still classifies the change.

**Managed keys are structurally excluded.** The diff source is
`spec.mysql.parameters`, which the validating webhook rejects when it contains
operator-managed keys (`mysqlconfig.IsDeniedKey`); the instance manager
additionally refuses them at the endpoint. A managed key can therefore never
appear in a probe request.

**Partially available clusters.** With zero ready instances the decision
defers: the change is kept pending (no roll, no probe) until an instance
reports, then classified from its evidence. Instances that are down during a
probe are skipped by the pass and caught up once ready — with the changed
parameters only, because the pending record is retired only after every
instance recorded the token.

**Runtime rejections are visible.** Absorbing the verdicts emits
`ParametersAppliedAtRuntime` (Normal) and `ParametersRequireRestart` (Warning,
listing refused keys) events; the per-instance skips are also logged.

## Consequences

- A dynamic-only change no longer rolls any instance; a mixed change applies
  the dynamic part immediately and rolls for the rest; a restart-required-only
  change behaves exactly as before.
- The template hash is a roll-decision fingerprint, not a content hash of the
  mounted my.cnf: after a dynamic-only change, a freshly created Pod mounts the
  new my.cnf but carries the stable hash. This is consistent by construction —
  the hash only needs to be equal for Pods that need no roll.
- A cluster that is entirely down when a dynamic-only change lands rolls on
  recovery only if the probe is never run before the instances come back
  ready; otherwise it is caught up by reload. The conservative default (roll)
  is avoided deliberately: rolling a down cluster is needless churn.
- The reload token mechanism gains one rule: a Pod stamped by a *newer* token
  is never re-probed by an older pass (tokens are RFC3339 stamps), which
  prevents the automatic pass and a concurrent manual reload from fighting
  over the pod annotations.

## Alternatives considered

- **Status quo** (manual reload, always roll): the pieces exist, but the
  default path is destructive and the manual step is easy to forget.
- **Static dynamic/restart table per series and flavor**: a deterministic
  pre-probe decision, but a large maintenance surface that diverges across
  MySQL 8.0/8.4/9.x and MariaDB; wrong entries would silently skip needed
  rolls or force needless ones. The server is the authority.
- **`SET PERSIST`**: would make mysqld-auto.cnf a second config source that
  fights my.cnf and the operator's ownership model.
