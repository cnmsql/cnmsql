//go:build e2e
// +build e2e

package e2e

import (
	"os"
	"strings"
)

// This file is the single source of truth for the container image references and
// the MySQL version selection the e2e suite uses. Specs and helpers must read
// image refs and versions from here rather than re-literalling them, so a version
// bump or a registry move is a one-line change.
//
// See design/025-e2e-testing-overhaul.md.

// managerImage is the operator manager image built and loaded for testing.
// Overridable with E2E_MANAGER_IMAGE so hack/e2e.sh and the suite agree on the
// tag when the image is built outside the suite (E2E_SKIP_IMAGE_BUILD=true).
var managerImage = func() string {
	if v := strings.TrimSpace(os.Getenv("E2E_MANAGER_IMAGE")); v != "" {
		return v
	}
	return "example.com/cnmsql:v0.0.1"
}()

// instanceImage is the local instance image consumed by the sample Cluster. It
// tracks sampleVersion so a matrix job pinning E2E_MYSQL_VERSION runs the whole
// suite against that one version.
var instanceImage = instanceImageFor(sampleVersion())

// instanceImageRepo is the GHCR repository the containers repo publishes the
// slim instance images to. Override with E2E_INSTANCE_IMAGE_REPO to test against
// a fork or a private mirror.
var instanceImageRepo = func() string {
	if v := strings.TrimSpace(os.Getenv("E2E_INSTANCE_IMAGE_REPO")); v != "" {
		return v
	}
	return "ghcr.io/cnmsql/cnmsql-instance"
}()

// instanceImageFor returns the published slim instance image reference for a
// version. The images are built and pushed from the separate containers repo;
// the suite pulls them and loads them into Kind (see pullAndLoadInstanceImage).
func instanceImageFor(version string) string {
	return instanceImageRepo + ":" + version
}

// sampleVersion is the MySQL version used by the non-archiving sample Clusters
// (the bulk of the suite). Under the CI matrix model it follows
// E2E_MYSQL_VERSION so every spec runs against the job's pinned version;
// otherwise it defaults to 8.4 (the historical sample version).
func sampleVersion() string {
	if v := strings.TrimSpace(os.Getenv("E2E_MYSQL_VERSION")); v != "" {
		return v
	}
	return "8.4"
}

// archiveVersions is the set of Percona versions the continuous-archiving specs
// run against. Precedence:
//   - E2E_MYSQL_VERSION (single version): the CI matrix model — each job pins
//     one MySQL version and the whole suite runs against it.
//   - E2E_ARCHIVE_VERSIONS (comma-separated list): explicit local override to
//     exercise several versions in one cluster.
//   - default: the full supported matrix, so a bare local run proves archiving
//     broadly compatible.
func archiveVersions() []string {
	if v := strings.TrimSpace(os.Getenv("E2E_MYSQL_VERSION")); v != "" {
		return []string{v}
	}
	if raw := strings.TrimSpace(os.Getenv("E2E_ARCHIVE_VERSIONS")); raw != "" {
		var out []string
		for v := range strings.SplitSeq(raw, ",") {
			if v = strings.TrimSpace(v); v != "" {
				out = append(out, v)
			}
		}
		return out
	}
	return []string{"8.0", "8.4", "9.x"}
}
