#!/usr/bin/env python3
#
# Build the e2e *lane* matrix for cnmsql (design/025-e2e-testing-overhaul.md).
#
# Instead of running the whole suite once per MySQL version, the suite is split
# into resource-budgeted lanes selected by Ginkgo labels. Version-agnostic specs
# run once (on the latest MySQL); only `flavor` specs run across every MySQL
# version. Disruptive specs each provision their own ephemeral Kind cluster and
# run a single Ginkgo process at a time. This shrinks both the wall-clock on the
# single self-hosted runner and the flake surface, since most specs no longer run
# three times.
#
# Lanes (each becomes one matrix entry == one status check == one runner job):
#   - core-feature       (core || feature) && !heavy   latest MySQL   procs 3
#   - heavy              heavy                          latest MySQL   procs 1
#   - operator-upgrade   disruptive (op-lifecycle)      latest MySQL   procs 1
#   - major-upgrade      major-upgrade                  latest MySQL   procs 1
#   - node-failure       node-failure                   latest MySQL   procs 1
#   - flavor-MySQL-<v>   flavor && !heavy               each MySQL     procs 2
#
# k8s versions come from .github/kind_versions.json filtered by the e2e_test
# range in .github/k8s_versions_scope.json (latest is used for every lane).
# MySQL versions are read from .github/mysql_versions.json, newest-first.

import argparse
import json
import os
import re
import sys
from operator import itemgetter
from typing import List

KIND_VERSIONS_FILE = ".github/kind_versions.json"
VERSION_SCOPE_FILE = ".github/k8s_versions_scope.json"
MYSQL_VERSIONS_FILE = ".github/mysql_versions.json"


class VersionList(list):
    """An ordered (newest-first) list of versions."""

    @property
    def latest(self):
        return self[0]

    @property
    def oldest(self):
        return self[-1]


def filter_version(versions_list, version_range):
    """Keep only k8s versions whose major.minor is within [min, max]."""
    min_version = version_range["min"]
    max_version = version_range["max"] or "99.99"
    return [
        v
        for v in versions_list
        if max_version >= re.sub(r"v", "", v)[0:4] >= min_version
    ]


def load_json(path):
    try:
        with open(path) as fh:
            return json.load(fh)
    except Exception as exc:  # noqa: BLE001
        print(f"Failed opening file {path}: {exc}", file=sys.stderr)
        raise SystemExit(1)


def _server_version_key(row):
    # "8.0.46" -> (8, 0, 46) so 9.6.0 sorts above 8.4.0 above 8.0.46.
    parts = re.findall(r"\d+", row.get("serverVersion", "0"))
    return tuple(int(p) for p in parts) if parts else (0,)


scope = load_json(VERSION_SCOPE_FILE)["e2e_test"]
KIND_K8S = VersionList(filter_version(load_json(KIND_VERSIONS_FILE), scope))

mysql_rows = sorted(load_json(MYSQL_VERSIONS_FILE), key=_server_version_key, reverse=True)
MYSQL = VersionList([row["version"] for row in mysql_rows])


def lane(lane_id, label_filter, mysql_version, procs, major_upgrade=False):
    """One matrix entry. The workflow maps these fields onto the env vars the
    suite and hack/e2e.sh consume (GINKGO_LABEL_FILTER, E2E_MYSQL_VERSION,
    GINKGO_PROCS, E2E_MAJOR_UPGRADE).

    Every gate lane excludes `flaky`: quarantined specs never block a merge. They
    are exercised by the non-blocking flake-hunt workflow (e2e-flake-hunt.yml)
    instead. See design/025-e2e-testing-overhaul.md."""
    return {
        "id": lane_id,
        "label_filter": f"({label_filter}) && !flaky",
        "k8s_version": KIND_K8S.latest,
        "mysql_version": mysql_version,
        "procs": procs,
        "major_upgrade": major_upgrade,
    }


def build_lanes():
    latest = MYSQL.latest
    lanes = [
        lane("core-feature", "(core || feature) && !heavy", latest, 3),
        lane("heavy", "heavy", latest, 1),
        # Disruptive operator-lifecycle specs (each provisions its own ephemeral
        # cluster); major-upgrade and node-failure get their own lanes below.
        lane("operator-upgrade", "disruptive && !major-upgrade && !node-failure", latest, 1),
        # major-upgrade co-loads every supported series image (E2E_MAJOR_UPGRADE).
        lane("major-upgrade", "major-upgrade", latest, 1, major_upgrade=True),
        lane("node-failure", "node-failure", latest, 1),
    ]
    # Only version-sensitive specs run across the whole MySQL axis.
    for mysql_version in MYSQL:
        lanes.append(lane(f"flavor-MySQL-{mysql_version}", "flavor && !heavy", mysql_version, 2))
    return lanes


# Every triggering event currently runs the same full lane set; the mode argument
# is kept so a cheaper set can be reintroduced per event without touching the
# workflow wiring (and so register-e2e-pending / authorize-e2e keep working).
MODES = {
    mode: build_lanes
    for mode in ("push", "pull_request", "workflow_dispatch", "main", "schedule")
}


def main():
    parser = argparse.ArgumentParser(description="Create the e2e lane matrix")
    parser.add_argument(
        "-m",
        "--mode",
        choices=list(MODES),
        default="push",
        help="triggering event",
    )
    args = parser.parse_args()

    include: List[dict] = sorted(MODES[args.mode](), key=itemgetter("id"))
    for job in include:
        print(f"Generating lane: {job['id']}", file=sys.stderr)

    output = os.getenv("GITHUB_OUTPUT")
    payload = json.dumps({"include": include})
    if output:
        with open(output, "a") as github_output:
            print(f"kindMatrix={payload}", file=github_output)
            print(f"kindEnabled={str(len(include) > 0).lower()}", file=github_output)
    else:
        print(f"kindMatrix={payload}")
        print(f"kindEnabled={str(len(include) > 0).lower()}")


if __name__ == "__main__":
    main()
