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
	"os"
	"path/filepath"
	"testing"

	"github.com/cnmsql/cnmsql/pkg/engine"
)

func TestEnsureArchiveID(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	id, err := EnsureArchiveID(dir)
	if err != nil {
		t.Fatalf("EnsureArchiveID: %v", err)
	}
	if id == "" {
		t.Fatal("EnsureArchiveID returned an empty id")
	}

	// It is persisted with 0600 perms.
	info, err := os.Stat(filepath.Join(dir, archiveIDFileName))
	if err != nil {
		t.Fatalf("stat archive id file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("archive id file perm = %o, want 600", perm)
	}

	// A second call returns the same id (stable across restarts).
	again, err := EnsureArchiveID(dir)
	if err != nil {
		t.Fatalf("EnsureArchiveID (2nd): %v", err)
	}
	if again != id {
		t.Fatalf("EnsureArchiveID not stable: %q then %q", id, again)
	}
}

func TestEnsureArchiveIDRegeneratesEmptyFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, archiveIDFileName)
	if err := os.WriteFile(path, []byte("  \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	id, err := EnsureArchiveID(dir)
	if err != nil {
		t.Fatalf("EnsureArchiveID: %v", err)
	}
	if id == "" {
		t.Fatal("expected a fresh id for an empty/corrupt file")
	}
}

func TestReadArchiveID(t *testing.T) {
	t.Parallel()

	// Missing file: empty id, no error, and no file minted (unlike EnsureArchiveID).
	dir := t.TempDir()
	id, err := ReadArchiveID(dir)
	if err != nil {
		t.Fatalf("ReadArchiveID on missing file: %v", err)
	}
	if id != "" {
		t.Fatalf("ReadArchiveID on missing file = %q, want empty", id)
	}
	if _, err := os.Stat(filepath.Join(dir, archiveIDFileName)); !os.IsNotExist(err) {
		t.Fatal("ReadArchiveID must not create the archive-id file")
	}

	// Existing file: returns the trimmed persisted id.
	want, err := EnsureArchiveID(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ReadArchiveID(dir)
	if err != nil {
		t.Fatalf("ReadArchiveID: %v", err)
	}
	if got != want {
		t.Fatalf("ReadArchiveID = %q, want %q", got, want)
	}
}

func TestResetArchiveID(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	first, err := EnsureArchiveID(dir)
	if err != nil {
		t.Fatalf("EnsureArchiveID: %v", err)
	}
	second, err := ResetArchiveID(dir)
	if err != nil {
		t.Fatalf("ResetArchiveID: %v", err)
	}
	if second == first {
		t.Fatalf("ResetArchiveID returned the same id %q; expected a fresh one", first)
	}

	// After reset the persisted id is the new one.
	got, err := EnsureArchiveID(dir)
	if err != nil {
		t.Fatalf("EnsureArchiveID after reset: %v", err)
	}
	if got != second {
		t.Fatalf("persisted id = %q, want %q", got, second)
	}
}

func TestResetArchiveIDOnFreshDir(t *testing.T) {
	t.Parallel()
	// Reset with no pre-existing file simply mints one (no error on missing file).
	id, err := ResetArchiveID(t.TempDir())
	if err != nil {
		t.Fatalf("ResetArchiveID on fresh dir: %v", err)
	}
	if id == "" {
		t.Fatal("expected a fresh id")
	}
}

func TestRemoveAutoCnf(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Idempotent on a missing file.
	if err := removeAutoCnf(dir); err != nil {
		t.Fatalf("removeAutoCnf on missing file: %v", err)
	}

	path := filepath.Join(dir, autoCnfFileName)
	if err := os.WriteFile(path, []byte("[auto]\nserver-uuid=deadbeef\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := removeAutoCnf(dir); err != nil {
		t.Fatalf("removeAutoCnf: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("auto.cnf still present after removeAutoCnf (err=%v)", err)
	}
}

func TestResetArchiveIdentity(t *testing.T) {
	t.Parallel()

	// MariaDB: resets the persisted token, leaving auto.cnf untouched.
	t.Run("mariadb resets token", func(t *testing.T) {
		dir := t.TempDir()
		before, err := EnsureArchiveID(dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := resetArchiveIdentity(dir, engine.FlavorMariaDB); err != nil {
			t.Fatalf("resetArchiveIdentity: %v", err)
		}
		after, err := EnsureArchiveID(dir)
		if err != nil {
			t.Fatal(err)
		}
		if after == before {
			t.Fatal("expected the MariaDB token to change after reset")
		}
	})

	// MySQL: drops auto.cnf, does not create an archive-id token.
	t.Run("mysql removes auto.cnf", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, autoCnfFileName), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := resetArchiveIdentity(dir, engine.FlavorMySQL); err != nil {
			t.Fatalf("resetArchiveIdentity: %v", err)
		}
		if _, err := os.Stat(filepath.Join(dir, autoCnfFileName)); !os.IsNotExist(err) {
			t.Fatalf("auto.cnf still present (err=%v)", err)
		}
		if _, err := os.Stat(filepath.Join(dir, archiveIDFileName)); !os.IsNotExist(err) {
			t.Fatal("MySQL path should not create an archive-id token")
		}
	})
}
