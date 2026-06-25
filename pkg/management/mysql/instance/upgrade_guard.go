/*
Copyright 2026 The CNMSQL - CloudNative for MySQL Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package instance

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/version"
)

// versionMarkerName is the file in the data directory recording the MySQL server
// version that last ran against it. The upgrade guard compares it to the
// incoming image version to refuse an unsupported major-version transition
// before mysqld touches the (irreversibly upgraded) data dictionary.
const versionMarkerName = ".cnmsql-mysql-version"

func versionMarkerPath(dataDir string) string {
	return filepath.Join(dataDir, versionMarkerName)
}

// readVersionMarker returns the recorded previous server version, with ok=false
// when no marker exists — a fresh data directory or a cluster that predates the
// marker, in which case the prior version is unknown and the guard cannot apply.
func readVersionMarker(dataDir string) (version.Version, bool, error) {
	data, err := os.ReadFile(versionMarkerPath(dataDir))
	if errors.Is(err, os.ErrNotExist) {
		return version.Version{}, false, nil
	}
	if err != nil {
		return version.Version{}, false, fmt.Errorf("reading mysql version marker: %w", err)
	}
	v, err := version.Parse(strings.TrimSpace(string(data)))
	if err != nil {
		return version.Version{}, false, fmt.Errorf("parsing mysql version marker %q: %w", string(data), err)
	}
	return v, true, nil
}

// writeVersionMarker records the server version now running against dataDir, so
// the next start can validate its transition. It is a no-op for an empty
// dataDir. A write failure is reported but is not fatal to the caller.
func writeVersionMarker(dataDir, ver string) error {
	if dataDir == "" {
		return nil
	}
	if err := os.WriteFile(versionMarkerPath(dataDir), []byte(ver+"\n"), 0o600); err != nil {
		return fmt.Errorf("writing mysql version marker: %w", err)
	}
	return nil
}

// guardDataDirUpgrade refuses to start a server whose version is an unsupported
// transition from the version that last ran against the data directory. It is a
// defense-in-depth backstop for the admission webhook: it reads the actual
// on-disk version, so it catches a downgrade or skipped series that bypassed
// admission (a disabled webhook, a hand-edited object, a restored backup). A
// fresh data directory (no marker) or an empty dataDir is always allowed.
func guardDataDirUpgrade(dataDir, targetVersion string) error {
	if dataDir == "" {
		return nil
	}
	from, ok, err := readVersionMarker(dataDir)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	to, err := version.Parse(targetVersion)
	if err != nil {
		return fmt.Errorf("parsing target MySQL version %q: %w", targetVersion, err)
	}
	if err := version.CheckUpgrade(from, to); err != nil {
		return fmt.Errorf("refusing to start mysqld on this data directory: %w", err)
	}
	return nil
}
