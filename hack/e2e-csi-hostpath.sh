#!/usr/bin/env bash
# hack/e2e-csi-hostpath.sh — install CSI volume snapshot support on the e2e
# Kind cluster: the VolumeSnapshot CRDs, the snapshot controller, and
# csi-driver-host-path with its StorageClass (csi-hostpath-sc) and
# VolumeSnapshotClass (csi-hostpath-snapclass). The volumeSnapshot backup specs
# run it on demand, the way the suite installs cert-manager; it is a no-op when
# the StorageClass is already there.
set -euo pipefail

SNAPSHOTTER_VERSION="${SNAPSHOTTER_VERSION:-v8.7.1}"
CSI_HOSTPATH_VERSION="${CSI_HOSTPATH_VERSION:-v1.19.0}"

if kubectl get storageclass csi-hostpath-sc >/dev/null 2>&1 &&
	kubectl get volumesnapshotclass csi-hostpath-snapclass >/dev/null 2>&1; then
	echo "==> csi-driver-host-path is already installed"
	exit 0
fi

echo "==> installing the VolumeSnapshot CRDs and controller (${SNAPSHOTTER_VERSION})"
kubectl apply -k "https://github.com/kubernetes-csi/external-snapshotter/client/config/crd?ref=${SNAPSHOTTER_VERSION}"
kubectl apply -k "https://github.com/kubernetes-csi/external-snapshotter/deploy/kubernetes/snapshot-controller?ref=${SNAPSHOTTER_VERSION}"
kubectl -n kube-system rollout status deployment/snapshot-controller --timeout=5m

echo "==> installing csi-driver-host-path (${CSI_HOSTPATH_VERSION})"
workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT
git clone --quiet --depth 1 --branch "${CSI_HOSTPATH_VERSION}" \
	https://github.com/kubernetes-csi/csi-driver-host-path "${workdir}/csi-driver-host-path"
# deploy.sh applies the driver and its sidecars and waits for them.
"${workdir}/csi-driver-host-path/deploy/kubernetes-latest/deploy.sh"
kubectl apply -f "${workdir}/csi-driver-host-path/examples/csi-storageclass.yaml"
kubectl apply -f "${workdir}/csi-driver-host-path/examples/csi-volumesnapshotclass.yaml"
echo "==> csi-driver-host-path is ready"
