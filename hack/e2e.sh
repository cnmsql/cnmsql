#!/usr/bin/env bash
#
# e2e.sh — single entrypoint for the cnmsql end-to-end suite.
#
# It owns the whole lifecycle: ensure a Kind cluster, build & load the manager
# image once (outside Ginkgo), run the suite with the requested focus / labels /
# parallelism, and tear the cluster down on success. The operator, instance
# images, cert-manager and MinIO are still set up by the suite's
# SynchronizedBeforeSuite.
#
# See design/025-e2e-testing-overhaul.md.
#
# Usage: hack/e2e.sh [flags]   (run with --help for the full list)

set -euo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# ---------------------------------------------------------------------------
# Defaults (env wins over built-in default; an explicit flag wins over env).
# ---------------------------------------------------------------------------
KIND="${KIND:-kind}"
CLUSTER="${KIND_CLUSTER:-cnmsql-test-e2e}"
MANAGER_IMAGE="${E2E_MANAGER_IMAGE:-example.com/cnmsql:v0.0.1}"
TIMEOUT="${GINKGO_TIMEOUT:-120m}"

K8S="${K8S_VERSION:-}"
MYSQL="${E2E_MYSQL_VERSION:-}"
FOCUS="${E2E_FOCUS:-}"
LABEL=""                       # set only by --label
TIER=""                        # set only by --tier
PROCS="${GINKGO_PROCS:-}"      # empty => auto-size
ENV_LABEL="${GINKGO_LABEL_FILTER:-}"
JUNIT="${E2E_JUNIT:-}"
KEEP=false
FRESH=false
DRYRUN=false

usage() {
	cat <<'EOF'
hack/e2e.sh — run the cnmsql e2e suite end to end.

Flags:
  --focus <regex>    Ginkgo --focus: run specs whose text matches the regex.
  --label <filter>   Ginkgo --label-filter (e.g. 'feature && !heavy'). Overrides --tier.
  --tier <name>      Preset label filter:
                       smoke       -> core
                       feature     -> (core || feature) && !heavy
                       flavor      -> flavor
                       heavy       -> heavy
                       disruptive  -> disruptive
                       flaky       -> flaky (quarantined; non-blocking)
                       all         -> no filter (default)
                     Set GINKGO_EXTRA_ARGS for extra ginkgo flags
                     (e.g. --repeat=2, --until-it-fails, --allow-empty).
  --k8s <version>    kindest/node version, e.g. v1.36.1   (sets K8S_VERSION).
  --mysql <version>  Pin one MySQL flavor: 8.0 | 8.4 | 9.x (sets E2E_MYSQL_VERSION).
  --procs <N>        Parallel Ginkgo processes (default: auto from CPU/RAM, capped at 3).
  --junit <path>     Write a JUnit report to <path>.
  --keep             Do not delete the Kind cluster afterwards (fast inner loop).
  --fresh            Delete and recreate the Kind cluster before running.
  --dry-run          Print the resolved plan (cluster/procs/labels/ginkgo args) and exit.
  -h, --help         Show this help.

Examples:
  hack/e2e.sh                              # whole suite, auto-sized, hermetic
  hack/e2e.sh --tier smoke                 # critical path only, fast
  hack/e2e.sh --focus 'switchover' --keep  # iterate on one spec, reuse cluster
  hack/e2e.sh --mysql 9.x --tier flavor    # version-sensitive specs on 9.x
EOF
}

# ---------------------------------------------------------------------------
# Parse flags.
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
	case "$1" in
	--focus) FOCUS="$2"; shift 2 ;;
	--label) LABEL="$2"; shift 2 ;;
	--tier) TIER="$2"; shift 2 ;;
	--k8s) K8S="$2"; shift 2 ;;
	--mysql) MYSQL="$2"; shift 2 ;;
	--procs) PROCS="$2"; shift 2 ;;
	--junit) JUNIT="$2"; shift 2 ;;
	--keep) KEEP=true; shift ;;
	--fresh) FRESH=true; shift ;;
	--dry-run) DRYRUN=true; shift ;;
	-h | --help) usage; exit 0 ;;
	*) echo "unknown flag: $1" >&2; usage >&2; exit 2 ;;
	esac
done

# ---------------------------------------------------------------------------
# Resolve the label filter. Precedence: --label > --tier (non-all) >
# GINKGO_LABEL_FILTER env > none.
# ---------------------------------------------------------------------------
if [[ -z "$LABEL" ]]; then
	if [[ -n "$TIER" && "$TIER" != "all" ]]; then
		case "$TIER" in
		smoke) LABEL="core" ;;
		feature) LABEL="(core || feature) && !heavy" ;;
		flavor) LABEL="flavor" ;;
		heavy) LABEL="heavy" ;;
		disruptive) LABEL="disruptive" ;;
		flaky) LABEL="flaky" ;;
		*) echo "unknown --tier: $TIER (want smoke|feature|flavor|heavy|disruptive|flaky|all)" >&2; exit 2 ;;
		esac
	elif [[ -n "$ENV_LABEL" ]]; then
		LABEL="$ENV_LABEL"
	fi
fi

# ---------------------------------------------------------------------------
# Auto-size parallelism for the constrained single-node test cluster: bounded by
# CPUs and by ~4 GiB of headroom per parallel 3-instance cluster, capped at 3.
# ---------------------------------------------------------------------------
auto_procs() {
	local cpus mem_gib by_mem n
	cpus="$(nproc 2>/dev/null || echo 2)"
	mem_gib="$(awk '/MemTotal/{printf "%d", $2/1024/1024}' /proc/meminfo 2>/dev/null || echo 4)"
	by_mem=$((mem_gib / 4)); ((by_mem < 1)) && by_mem=1
	n="$cpus"; ((by_mem < n)) && n="$by_mem"
	((n > 3)) && n=3
	((n < 1)) && n=1
	echo "$n"
}
# Tier-aware parallelism ceiling, matching the resource-budget partitions in
# design/025: heavy and disruptive specs are too large to run many at once on the
# single-node test cluster (disruptive ones each provision a whole Kind cluster).
# An explicit --procs or GINKGO_PROCS overrides this.
tier_cap() {
	case "$1" in
	heavy | disruptive | flaky) echo 1 ;;
	flavor) echo 2 ;;
	*) echo 3 ;;
	esac
}

if [[ -z "$PROCS" ]]; then
	cap="$(tier_cap "${TIER:-all}")"
	auto="$(auto_procs)"
	PROCS=$((auto < cap ? auto : cap))
fi

# ---------------------------------------------------------------------------
# Export the knobs the suite and the make targets read.
# ---------------------------------------------------------------------------
export KIND KIND_CLUSTER="$CLUSTER" E2E_MANAGER_IMAGE="$MANAGER_IMAGE"
export K8S_VERSION="$K8S" E2E_MYSQL_VERSION="$MYSQL"
export E2E_SKIP_IMAGE_BUILD=true

echo "==> e2e: cluster=$CLUSTER procs=$PROCS k8s=${K8S:-default} mysql=${MYSQL:-default} label='${LABEL:-<all>}' focus='${FOCUS:-<none>}'"

# Assemble the ginkgo arguments up front so --dry-run can show the exact plan.
args=(-procs="$PROCS" -tags=e2e --timeout="$TIMEOUT" -v)
[[ -n "$LABEL" ]] && args+=(--label-filter="$LABEL")
[[ -n "$FOCUS" ]] && args+=(--focus="$FOCUS")
[[ -n "$JUNIT" ]] && args+=(--junit-report="$JUNIT")
# Escape hatch for extra ginkgo flags (e.g. --repeat=N, --until-it-fails,
# --allow-empty) — used by the flake-hunt workflow. Intentionally word-split.
# shellcheck disable=SC2206
[[ -n "${GINKGO_EXTRA_ARGS:-}" ]] && args+=(${GINKGO_EXTRA_ARGS})
args+=(./test/e2e/)

if [[ "$DRYRUN" == true ]]; then
	echo "==> dry-run; would run: ginkgo ${args[*]}"
	exit 0
fi

# ---------------------------------------------------------------------------
# Tooling + cluster lifecycle.
# ---------------------------------------------------------------------------
make ginkgo
GINKGO_BIN="${GINKGO:-./bin/ginkgo}"

if [[ "$FRESH" == true ]]; then
	echo "==> recreating Kind cluster (--fresh)"
	make cleanup-test-e2e KIND_CLUSTER="$CLUSTER" || true
fi
make setup-test-e2e KIND_CLUSTER="$CLUSTER" K8S_VERSION="$K8S"

echo "==> building and loading the manager image ($MANAGER_IMAGE)"
make e2e-build-images IMG="$MANAGER_IMAGE" KIND_CLUSTER="$CLUSTER"

[[ -n "$JUNIT" ]] && mkdir -p "$(dirname "$JUNIT")"

# ---------------------------------------------------------------------------
# Run the suite.
# ---------------------------------------------------------------------------
echo "==> $GINKGO_BIN ${args[*]}"
set +e
"$GINKGO_BIN" "${args[@]}"
rc=$?
set -e

# ---------------------------------------------------------------------------
# Teardown. Keep the cluster on failure (for inspection) and on --keep.
# ---------------------------------------------------------------------------
if [[ "$KEEP" == true ]]; then
	echo "==> keeping Kind cluster $CLUSTER (--keep)"
elif [[ $rc -ne 0 ]]; then
	echo "==> suite failed (rc=$rc); keeping Kind cluster $CLUSTER for inspection."
	echo "    delete it with: make cleanup-test-e2e KIND_CLUSTER=$CLUSTER"
else
	make cleanup-test-e2e KIND_CLUSTER="$CLUSTER"
fi

exit "$rc"
