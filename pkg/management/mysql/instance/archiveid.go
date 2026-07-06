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
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"

	"github.com/cnmsql/cnmsql/pkg/engine"
)

// archiveIDFileName holds a data directory's per-incarnation archive identity. It
// lives in the data dir (wiped when the data dir is re-inited, preserved across
// restarts), a sibling of .cnmsql-bootstrapped. The continuous archiver uses it as
// the object-key partition for MariaDB, where @@server_id is config-assigned and
// therefore reused verbatim across a re-init — which would otherwise collide the new
// incarnation's binlog.000001 with the old one's in the archive.
const archiveIDFileName = ".cnmsql-archive-id"

// autoCnfFileName is MySQL's server_uuid store. Deleting it makes mysqld mint a fresh
// server_uuid on next start; a physical restore/clone copies it from the source, so a
// restored MySQL server would otherwise reuse the source's identity.
const autoCnfFileName = "auto.cnf"

// EnsureArchiveID returns the data directory's archive identity, generating and
// persisting a fresh one if none exists yet. It is idempotent: a subsequent call on
// the same data dir returns the same value, so restarts keep a stable identity.
func EnsureArchiveID(dataDir string) (string, error) {
	path := filepath.Join(dataDir, archiveIDFileName)
	content, err := os.ReadFile(path) //nolint:gosec // path derived from operator-provided data dir
	if err == nil {
		if id := strings.TrimSpace(string(content)); id != "" {
			return id, nil
		}
		// Empty/corrupt file: fall through and regenerate.
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("reading archive id: %w", err)
	}

	id := uuid.NewString()
	if err := os.WriteFile(path, []byte(id+"\n"), 0o600); err != nil {
		return "", fmt.Errorf("writing archive id: %w", err)
	}
	return id, nil
}

// ReadArchiveID returns the data directory's persisted archive identity without
// generating one when absent (unlike EnsureArchiveID). It reports "" for a missing
// or empty file, so a caller that only wants to record an existing identity (e.g. a
// backup annotating its source incarnation) never mints a spurious token.
func ReadArchiveID(dataDir string) (string, error) {
	//nolint:gosec // path derived from operator-provided data dir
	content, err := os.ReadFile(filepath.Join(dataDir, archiveIDFileName))
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("reading archive id: %w", err)
	}
	return strings.TrimSpace(string(content)), nil
}

// ResetArchiveID discards any existing archive identity and generates a fresh one. It
// is called whenever a data directory is laid down from an external source (restore,
// replica join), so the new incarnation never inherits the source's archive identity.
func ResetArchiveID(dataDir string) (string, error) {
	if err := os.Remove(filepath.Join(dataDir, archiveIDFileName)); err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("removing archive id: %w", err)
	}
	return EnsureArchiveID(dataDir)
}

// removeAutoCnf deletes MySQL's auto.cnf so mysqld regenerates a fresh server_uuid on
// next start. Used at restore/join for MySQL, where the physical copy carried the
// source's auto.cnf. A missing file is not an error.
func removeAutoCnf(dataDir string) error {
	if err := os.Remove(filepath.Join(dataDir, autoCnfFileName)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing auto.cnf: %w", err)
	}
	return nil
}

// resetArchiveIdentity gives a freshly laid-down data directory a new archive
// identity so the incarnation never reuses the source's: MariaDB regenerates the
// persisted token (a backup carries the source's), MySQL drops auto.cnf so mysqld
// mints a new server_uuid. Called by restore and replica join.
func resetArchiveIdentity(dataDir string, flavor engine.Flavor) error {
	if flavor == engine.FlavorMariaDB {
		_, err := ResetArchiveID(dataDir)
		return err
	}
	return removeAutoCnf(dataDir)
}
