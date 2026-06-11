#!/usr/bin/env bash
# Build the cnmysql slim instance image(s) from images/versions.json.
#
# Usage:
#   images/build.sh                 # build every version in the matrix
#   images/build.sh 8.0 8.4         # build only the named versions
#
# Environment:
#   REGISTRY   image name prefix      (default: cnmysql-instance)
#   PUSH       set to 1 to push       (default: unset)
#   CONTAINER_TOOL                    (default: docker)
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${here}/.." && pwd)"
versions_json="${here}/versions.json"

REGISTRY="${REGISTRY:-cnmysql-instance}"
CONTAINER_TOOL="${CONTAINER_TOOL:-docker}"

# Print "version base ps pxb pxbPackage" for each requested version (or all).
select_versions() {
  python3 - "$versions_json" "$@" <<'PY'
import json, sys
path, *want = sys.argv[1], *sys.argv[2:]
with open(path) as fh:
    rows = json.load(fh)
want = set(want)
for r in rows:
    if want and r["version"] not in want:
        continue
    print(r["version"], r["base"], r["ps"], r["pxb"], r["pxbPackage"], r.get("component", "release"))
PY
}

build_one() {
  local version="$1" base="$2" ps="$3" pxb="$4" pxbPkg="$5" component="$6"
  local tag="${REGISTRY}:${version}"
  echo ">> building ${tag} (base=${base} ps=${ps} pxb=${pxb} component=${component})"
  "${CONTAINER_TOOL}" build \
    -f "${repo_root}/Dockerfile.instance" \
    --build-arg "BASE_IMAGE=${base}" \
    --build-arg "PS_REPO=${ps}" \
    --build-arg "PXB_REPO=${pxb}" \
    --build-arg "PXB_PACKAGE=${pxbPkg}" \
    --build-arg "REPO_COMPONENT=${component}" \
    -t "${tag}" \
    "${repo_root}"
  if [ "${PUSH:-}" = "1" ]; then
    echo ">> pushing ${tag}"
    "${CONTAINER_TOOL}" push "${tag}"
  fi
}

rc=0
while read -r version base ps pxb pxbPkg component; do
  [ -z "${version}" ] && continue
  build_one "${version}" "${base}" "${ps}" "${pxb}" "${pxbPkg}" "${component}" || rc=1
done < <(select_versions "$@")
exit "${rc}"
