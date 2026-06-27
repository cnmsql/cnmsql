# Contributing to CNMSQL

Thanks for taking the time to contribute. CNMSQL is a Kubernetes operator for Percona Server for MySQL, and it gets better when people report problems, propose changes, and send patches. This guide covers how to do that.

## Before you start

- Read the [Code of Conduct](CODE_OF_CONDUCT.md). It applies to every space connected to the project.
- Search the [issue tracker](https://github.com/cnmsql/cnmsql/issues) before opening something new. Someone may already be on it.
- For anything large, open an issue first and agree on the approach before you write code. It saves rework on both sides.

## Reporting bugs

Open an issue with the bug report template. A report we can act on usually has:

- What you expected to happen and what happened instead.
- The CNMSQL version, Kubernetes version, and MySQL major version.
- The `Cluster` (or other resource) manifest that triggers it, with secrets removed.
- Operator logs and, where relevant, `kubectl cnmsql status` output.
- The smallest set of steps that reproduces it.

## Suggesting changes

Use the feature request template. Describe the problem you are trying to solve, not just the solution you have in mind. That gives reviewers room to suggest alternatives.

## Development setup

The project is built with [Kubebuilder](https://book.kubebuilder.io). You need `go`, `docker`, `kubectl`, `kind`, and `make`, plus `cert-manager` in any cluster you deploy to.

```bash
make manifests generate   # Regenerate CRDs, RBAC, and DeepCopy after editing API types
make lint-fix             # Auto-fix style
make test                 # Unit tests (Ginkgo + Gomega on envtest)
make run                  # Run the controller against your current kubeconfig
```

Run `make help` for the full list of targets. The [README](README.md) has a Kind-based quickstart.

End-to-end tests run against a real cluster and take longer. `./hack/e2e.sh` is
the one-command entrypoint: it creates a Kind cluster, builds and loads the
operator image, runs the suite, and tears the cluster down.

```bash
make test-e2e                              # whole suite (alias for ./hack/e2e.sh)
./hack/e2e.sh --tier smoke                 # critical path only, fast
./hack/e2e.sh --focus 'switchover' --keep  # one spec, reuse the cluster
./hack/e2e.sh --mysql 9.x --tier flavor    # version-sensitive specs on 9.x
./hack/e2e.sh --help                       # all flags (--k8s, --procs, --junit, --fresh, ...)
```

Specs carry tier labels (`core`/`feature`/`flavor`/`heavy`/`disruptive`); CI runs
them as separate lanes so version-agnostic specs run once and only `flavor` specs
run across every MySQL version. Operator-disrupting specs each provision their own
ephemeral Kind cluster, so they never destabilise the shared suite operator.

## Pull requests

1. Fork the repo and branch off `main`.
2. Make your change. Keep commits small and focused.
3. Run `make lint test` and make sure both pass.
4. If you touched API types, run `make manifests generate` and commit the regenerated files.
5. Open the PR against `main` and fill in the template.

Keep the PR scoped to one thing. If you find an unrelated issue along the way, note it or open a separate PR rather than folding it in.

### Commit messages

We follow [Conventional Commits](https://www.conventionalcommits.org). The subject is a single lowercase line, for example:

```
fix(gr): rejoin former primary as replica after failover
```

Use a scope that matches the area you touched (`gr` for group replication, and so on). 

### Sign your commits

Every commit must carry a [Developer Certificate of Origin](https://developercertificate.org) sign-off. It states that you wrote the change or have the right to submit it under the project license. Add it with:

```bash
git commit -s
```

That appends a `Signed-off-by` line with your name and email. The line has to match the commit author.

## Reviews and merging

A maintainer reviews every PR. CI has to be green before merge. Expect questions and requested changes; that is the normal path, not a sign anything is wrong. Once a maintainer approves and CI passes, they merge it.

## License

By contributing you agree that your work is licensed under the [Apache License 2.0](LICENSE), the same license as the rest of the project.
