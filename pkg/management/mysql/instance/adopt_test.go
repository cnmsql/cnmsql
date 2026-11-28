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
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cnmsql/cnmsql/pkg/engine"
)

// snapshotDataDir lays down what a data directory restored from a snapshot
// looks like: the system schema, the source's bootstrap sentinel, and files
// that must not survive the clone.
func snapshotDataDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "mysql"), 0o750); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{bootstrapSentinel, "demo-2.pid", "mysqld.sock", "mysqld.sock.lock",
		corruptionMarker, pitrSentinelFile, adoptSentinel, "binlog.000003", "ibdata1"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("source-uid\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestRemoveAdoptLeftoversKeepsTheData(t *testing.T) {
	dir := snapshotDataDir(t)
	if err := removeAdoptLeftovers(dir); err != nil {
		t.Fatal(err)
	}
	for _, gone := range []string{"demo-2.pid", "mysqld.sock", "mysqld.sock.lock", corruptionMarker, pitrSentinelFile, adoptSentinel} {
		if _, err := os.Stat(filepath.Join(dir, gone)); !os.IsNotExist(err) {
			t.Errorf("%s survived the adoption cleanup", gone)
		}
	}
	for _, kept := range []string{bootstrapSentinel, "binlog.000003", "ibdata1", "mysql"} {
		if _, err := os.Stat(filepath.Join(dir, kept)); err != nil {
			t.Errorf("%s was removed: %v", kept, err)
		}
	}
}

func TestAdoptRefusesAVolumeWithoutADataDirectory(t *testing.T) {
	err := adoptVolume(context.Background(), RestoreOptions{DataDir: t.TempDir(), AdoptID: "uid", BackupName: "b"},
		engine.MustForFlavor(engine.FlavorMySQL))
	if err == nil || !strings.Contains(err.Error(), "holds no MySQL data directory") {
		t.Fatalf("error = %v", err)
	}
}

// A source cluster that was itself adopted carries its own sentinel: only the
// adopting cluster's UID skips the adoption.
func TestAdoptSkipsOnlyItsOwnCompletedAdoption(t *testing.T) {
	dir := snapshotDataDir(t)
	if got := adoptedBy(dir); got != "source-uid" {
		t.Fatalf("adoptedBy = %q", got)
	}
	if err := os.WriteFile(filepath.Join(dir, adoptSentinel), []byte("our-uid\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Already adopted by us and no replay: nothing to start, nothing to do.
	if err := adoptVolume(context.Background(), RestoreOptions{DataDir: dir, AdoptID: "our-uid", BackupName: "b"},
		engine.MustForFlavor(engine.FlavorMySQL)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "demo-2.pid")); err != nil {
		t.Fatal("a completed adoption ran its cleanup again")
	}
}

func TestAdoptReplayNeedsThisBackupsAnchor(t *testing.T) {
	dir := snapshotDataDir(t)
	if err := os.WriteFile(filepath.Join(dir, adoptSentinel), []byte("our-uid\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A stale manifest from an earlier snapshot of the source instance.
	if err := WriteSnapshotManifest(dir, SnapshotManifest{BackupName: "older", AnchorGTID: "u:1-3"}); err != nil {
		t.Fatal(err)
	}
	err := adoptVolume(context.Background(), RestoreOptions{
		DataDir: dir, AdoptID: "our-uid", BackupName: "nightly", SourceCluster: "src",
	}, engine.MustForFlavor(engine.FlavorMySQL))
	if err == nil || !strings.Contains(err.Error(), `anchor of backup "nightly"`) {
		t.Fatalf("error = %v, want a refusal to replay from another backup's anchor", err)
	}
}

func TestEnsureAdoptAnchorKeepsTheOnlineManifest(t *testing.T) {
	dir := snapshotDataDir(t)
	want := SnapshotManifest{BackupName: "nightly", AnchorGTID: "u:1-9", BinlogFile: "binlog.000003", BinlogPos: 157, Online: true}
	if err := WriteSnapshotManifest(dir, want); err != nil {
		t.Fatal(err)
	}
	// No server is started: the manifest names this Backup.
	opts := RestoreOptions{DataDir: dir, BackupName: "nightly", MysqldPath: "/nonexistent/mysqld"}
	if err := ensureAdoptAnchor(context.Background(), &opts, engine.MustForFlavor(engine.FlavorMySQL)); err != nil {
		t.Fatal(err)
	}
	got, err := ReadSnapshotManifest(dir)
	if err != nil || *got != want {
		t.Fatalf("manifest = %+v, %v", got, err)
	}
}
