# 026 — MariaDB Support: Execution Plans

These are **work orders** for the MariaDB milestones in
[../026-mariadb-support.md](../026-mariadb-support.md). One file per milestone.
Each is written to be executed by an agent with no prior context on this design —
read the design section it cites, then follow the steps.

## How to pick up work

1. Open the **lowest-numbered** plan whose `Status:` is `ready` (not `blocked`).
2. Check its **Depends on** line — every listed milestone must be `done`.
3. Read the design sections it cites before touching code.
4. Work the **Tasks** checklist top-to-bottom. Do not skip ahead across a
   `— checkpoint —` line without the checkpoint's command passing.
5. Update the **Status log** at the bottom of that plan file as you go (see
   protocol below). This is how you report — the file is the source of truth.

## The single hard rule

**MySQL behaviour must not change.** Every existing test must stay green with the
same expected values. If a golden file (`*_test.go` with literal expected config
or SQL strings) would have to change to make your code compile or pass, you have
broken this rule — stop and re-read the plan. The whole point of the `Engine`
abstraction is that the MySQL path produces byte-identical output before and
after. The only commits allowed to change MySQL output are ones the plan
explicitly calls out (there are none in M-MDB.1–.6).

## Dependency graph

```
M-MDB.1 (engine extraction) ──┬──> M-MDB.2 (api + webhook) ──> M-MDB.3 (bootstrap+config)
                              │                                      │
                              └──────────────────────────────────> M-MDB.4 (replication+gtid)
                                                                     │
                                                     M-MDB.5 (backups+pitr) ─> M-MDB.6 (ecosystem)
```

- **M-MDB.1** is partially done (the `pkg/engine` foundation exists; see design
  §18). Its plan covers the *remaining* threading work.
- **M-MDB.3** needs .2 (the `spec.flavor` field) to select an engine per cluster.
- **M-MDB.4** needs .3 (a MariaDB instance must boot before replication matters).
- **M-MDB.5** needs .4 (clone/PITR reuse the GTID model + dialect).
- **M-MDB.6** is last (docs/samples/CI reflect finished behaviour).

## Verification commands (every plan uses these)

```bash
go build ./...                         # compiles
go test ./pkg/engine/... ./pkg/management/... ./internal/... ./api/...   # unit
gofmt -l pkg/ internal/ api/           # must print nothing
go vet ./...                           # must be clean
```

`make lint` uses a custom-built golangci-lint binary (`.custom-gcl.yml`, needs
the `logcheck` plugin) — run it if `make` is available; if it fails with
`plugin "logcheck" not found`, that is a local toolchain gap, not your code —
note it and rely on `go vet` + `gofmt` + CI.

## Status protocol

Each plan ends with a **Status log**. Append one dated entry per work session.
Never delete prior entries. Format:

```
### <YYYY-MM-DD> — <agent/handle>
- state: ready | in-progress | blocked | done
- did: <what you completed this session, referencing task numbers>
- next: <the next concrete step>
- blockers: <none | what you need and from whom>
- verify: <which commands you ran and their result>
```

Set the plan's top-of-file `Status:` field to match your latest `state`.

## Commit conventions

Conventional Commits, scope `(gr)`, **no** `Co-Authored-By` trailer. One
milestone may span several commits — keep each commit green.
