#!/usr/bin/env bash
#
# e2e-registry-mirrors.sh — pull-through registry caches for the e2e Kind nodes.
#
# Every e2e Kind cluster is new, so without a cache each node pulls cert-manager,
# SeaweedFS, rclone, curl, VPA and metrics-server from the internet again. This
# runs one `registry` proxy per upstream as a long-lived container on the host and
# points the nodes' containerd at them, so each image crosses the network once per
# host. containerd falls back to the upstream when a mirror is unreachable.
#
# The Kind configs in test/e2e enable containerd's registry config_path; this
# script only fills it in, so a cluster created without it ignores the mirrors.
#
# Usage:
#   hack/e2e-registry-mirrors.sh up                 start the mirrors (idempotent)
#   hack/e2e-registry-mirrors.sh configure <cluster> point a Kind cluster's nodes at them

set -euo pipefail

KIND="${KIND:-kind}"
# renovate: datasource=docker depName=registry
REGISTRY_IMAGE="${E2E_REGISTRY_MIRROR_IMAGE:-docker.io/library/registry:3.0.0}"

# <registry host> <upstream URL>. The container and its cache volume are named
# cnmsql-mirror-<host with dots replaced>.
UPSTREAMS=(
	"docker.io https://registry-1.docker.io"
	"quay.io https://quay.io"
	"ghcr.io https://ghcr.io"
	"registry.k8s.io https://registry.k8s.io"
)

mirror_name() { echo "cnmsql-mirror-${1//./-}"; }

up() {
	local entry host url name
	for entry in "${UPSTREAMS[@]}"; do
		read -r host url <<<"$entry"
		name="$(mirror_name "$host")"
		if [[ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" == true ]]; then
			continue
		fi
		docker rm -f "$name" >/dev/null 2>&1 || true
		echo "==> starting registry mirror $name for $host"
		# A concurrent lane may win the race to create it; that is fine as long as
		# it ends up running.
		docker run -d --restart=always --name "$name" \
			-v "$name:/var/lib/registry" \
			-e REGISTRY_PROXY_REMOTEURL="$url" \
			-e REGISTRY_LOG_LEVEL=info \
			-e OTEL_TRACES_EXPORTER=none \
			"$REGISTRY_IMAGE" >/dev/null ||
			[[ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" == true ]]
	done
}

configure() {
	local cluster="$1" entry host url name node
	for entry in "${UPSTREAMS[@]}"; do
		read -r host url <<<"$entry"
		name="$(mirror_name "$host")"
		# Kind nodes resolve the mirrors by container name on the `kind` network,
		# which exists once the first cluster is created.
		if ! docker inspect -f '{{json .NetworkSettings.Networks}}' "$name" | grep -q '"kind"'; then
			docker network connect kind "$name" 2>/dev/null ||
				docker inspect -f '{{json .NetworkSettings.Networks}}' "$name" | grep -q '"kind"'
		fi
	done
	for node in $("$KIND" get nodes --name "$cluster"); do
		for entry in "${UPSTREAMS[@]}"; do
			read -r host url <<<"$entry"
			name="$(mirror_name "$host")"
			docker exec -i "$node" sh -c "mkdir -p /etc/containerd/certs.d/$host && cat >/etc/containerd/certs.d/$host/hosts.toml" <<EOF
server = "$url"

[host."http://$name:5000"]
  capabilities = ["pull", "resolve"]
EOF
		done
	done
	echo "==> Kind cluster $cluster pulls through the registry mirrors"
}

case "${1:-}" in
up) up ;;
configure)
	[[ -n "${2:-}" ]] || { echo "usage: $0 configure <cluster>" >&2; exit 2; }
	configure "$2"
	;;
*) echo "usage: $0 up | configure <cluster>" >&2; exit 2 ;;
esac
