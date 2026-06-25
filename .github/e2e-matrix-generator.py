#!/usr/bin/env python3
#
# Build the e2e job matrix for cnmsql, in the style of CloudNativePG.
#
# The matrix has two axes:
#   - k8s_version   the kindest/node image tag the cluster runs on
#   - mysql_version the cnmsql instance flavor (8.0 / 8.4 / 9.x) under test
#
# To keep the job count bounded we never run the full cartesian product. We run
# a "cross": the two diagonal corners, plus the full k8s axis at the newest
# MySQL and the full MySQL axis at the newest k8s. Every triggering event runs
# this same cross (build_push remains as the corner helper build_pull_request
# composes on top of).
#
# k8s versions come from .github/kind_versions.json, filtered by the e2e_test
# range in .github/k8s_versions_scope.json. MySQL versions are read from
# .github/mysql_versions.json — kept in sync with the flavor matrix in
# test/integration/flavors_test.go and the external containers repo that builds
# the instance images — ordered newest-first by serverVersion.

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


class E2EJob(dict):
    """A single matrix entry."""

    def __init__(self, k8s_version, mysql_version):
        super().__init__(
            {
                "id": f"{k8s_version}-MySQL-{mysql_version}",
                "k8s_version": k8s_version,
                "mysql_version": mysql_version,
                "major_upgrade": False,
            }
        )

    def __hash__(self):
        return hash(self["id"])


class MajorUpgradeE2EJob(E2EJob):
    """Latest-Kubernetes job that co-loads every MySQL upgrade-series image.

    The job carries major_upgrade=true, which makes the e2e suite load all
    instance images (8.0, 8.4, 9.x) and run only the heavy multi-image
    upgrade specs (selected by the "major-upgrade" Ginkgo label): the full
    multi-hop Group Replication rollout plus the defensive and backup-gate
    scenarios. Those specs pin their own 8.0/8.4/9.x images, so a single job
    exercises every adjacent-series hop regardless of the suite's pinned
    MySQL version; running it once (at MYSQL.latest) is sufficient.
    """

    def __init__(self):
        super().__init__(KIND_K8S.latest, MYSQL.latest)
        self["id"] = f"{KIND_K8S.latest}-MySQL-major-upgrade"
        self["major_upgrade"] = True


def build_push():
    """Smoke set: just the two diagonal corners."""
    return {
        E2EJob(KIND_K8S.latest, MYSQL.latest),
        E2EJob(KIND_K8S.oldest, MYSQL.oldest),
    }


def build_pull_request():
    """Corners + full k8s axis at newest MySQL + full MySQL axis at newest k8s +
    a single dedicated major-upgrade job that exercises every adjacent-series
    hop."""
    result = build_push()
    for k8s_version in KIND_K8S:
        result.add(E2EJob(k8s_version, MYSQL.latest))
    for mysql_version in MYSQL:
        result.add(E2EJob(KIND_K8S.latest, mysql_version))
    # One dedicated major-upgrade job: its specs pin their own series images, so
    # a second MySQL version would re-run identical upgrade scenarios.
    result.add(MajorUpgradeE2EJob())
    return result


MODES = {
    "push": build_pull_request,
    "pull_request": build_pull_request,
    "workflow_dispatch": build_pull_request,
    "main": build_pull_request,
    "schedule": build_pull_request,
}


def main():
    parser = argparse.ArgumentParser(description="Create the e2e job matrix")
    parser.add_argument(
        "-m",
        "--mode",
        choices=list(MODES),
        default="push",
        help="set of tests to run",
    )
    args = parser.parse_args()

    include: List[dict] = sorted(MODES[args.mode](), key=itemgetter("id"))
    for job in include:
        print(f"Generating kind: {job['id']}", file=sys.stderr)

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
