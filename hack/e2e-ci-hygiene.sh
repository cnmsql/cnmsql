#!/usr/bin/env bash
#
# e2e-ci-hygiene.sh — keep a persistent self-hosted runner host clean between
# e2e jobs, while other e2e jobs may be running on the same host.
#
# Several runners can share one host (and its Docker daemon), so a job must only
# touch its own Kind clusters: its shared cluster <KIND_CLUSTER> and the dedicated
# clusters the disruptive specs create (<KIND_CLUSTER>-<slug>). Clusters left by
# a crashed job are only recognized by age, and pruning is age-filtered for the
# same reason: a running job's containers, images and build cache are recent.
#
# Usage:
#   hack/e2e-ci-hygiene.sh prepare <cluster>   before a job: sweep, prune
#   hack/e2e-ci-hygiene.sh cleanup <cluster>   after a job: delete its clusters

set -euo pipefail

KIND="${KIND:-kind}"
# Longer than any e2e job can run (GitHub's default job timeout is 6h).
STALE_AFTER_HOURS="${E2E_STALE_AFTER_HOURS:-8}"

# own_clusters lists the clusters that belong to job cluster $1.
own_clusters() {
	local c
	for c in $("$KIND" get clusters 2>/dev/null || true); do
		case "$c" in "$1" | "$1"-*) echo "$c" ;; esac
	done
}

delete_clusters() {
	local c
	for c in "$@"; do
		echo "==> deleting Kind cluster $c"
		"$KIND" delete cluster --name "$c" || true
	done
}

# sweep_stale deletes cnmsql clusters whose control plane is older than
# STALE_AFTER_HOURS: no running job can still own them.
sweep_stale() {
	local c created cutoff
	cutoff=$(($(date +%s) - STALE_AFTER_HOURS * 3600))
	for c in $("$KIND" get clusters 2>/dev/null || true); do
		case "$c" in *cnmsql*) ;; *) continue ;; esac
		created=$(docker inspect -f '{{.Created}}' "$c-control-plane" 2>/dev/null) || continue
		if (($(date -d "$created" +%s) < cutoff)); then
			delete_clusters "$c"
		fi
	done
}

prepare() {
	# Leftovers of an earlier attempt of this same job.
	# shellcheck disable=SC2046
	delete_clusters $(own_clusters "$1")
	sweep_stale
	# Never `docker system prune`: it would race other jobs on this host (a node
	# container created but not yet started, a build in flight).
	docker container prune -f --filter "until=${STALE_AFTER_HOURS}h" >/dev/null || true
	docker image prune -f --filter "until=${STALE_AFTER_HOURS}h" >/dev/null || true
	docker builder prune -f --filter "until=72h" >/dev/null || true
	df -h / || true
}

cleanup() {
	# shellcheck disable=SC2046
	delete_clusters $(own_clusters "$1")
}

case "${1:-}" in
prepare | cleanup)
	[[ -n "${2:-}" ]] || { echo "usage: $0 $1 <cluster>" >&2; exit 2; }
	"$1" "$2"
	;;
*) echo "usage: $0 prepare|cleanup <cluster>" >&2; exit 2 ;;
esac
