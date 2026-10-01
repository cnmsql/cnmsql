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

package binlog

import (
	"context"
	"database/sql"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-logr/logr"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/engine"
)

// staticFloor is a ReplicaFloor with canned positions.
type staticFloor struct {
	positions map[string]string
	unknown   []string
	observed  bool
}

func (f staticFloor) Positions() (map[string]string, []string, bool) {
	return f.positions, f.unknown, f.observed
}

// purgeLogs are the files a primary with the purge gate on holds: five rotated
// and archived, the sixth active. The archive bound alone would purge up to
// binlog.000004 (everything before the file preceding the frontier).
var purgeLogs = []string{
	"binlog.000001", "binlog.000002", "binlog.000003",
	"binlog.000004", "binlog.000005", "binlog.000006",
}

var purgeSets = map[string]string{
	"binlog.000001": testUUID + ":1-3",
	"binlog.000002": testUUID + ":4-6",
	"binlog.000003": testUUID + ":7-9",
	"binlog.000004": testUUID + ":10-12",
	"binlog.000005": testUUID + ":13-15",
}

func purgeFixture(t *testing.T, sets map[string]string, newSet func() GTIDOps) (*sql.DB, sqlmock.Sqlmock, *Archiver) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	dir := t.TempDir()
	for _, name := range purgeLogs {
		writeBinlog(t, dir, name, name)
	}
	arch, err := NewArchiver(ArchiverOptions{
		Store:        newMemStore(),
		ObjectStore:  mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "cnmsql"},
		ClusterName:  "demo",
		InstanceName: "demo-1",
		ServerUUID:   testUUID,
		BinlogDir:    dir,
		Scan:         staticScan(sets),
		NewSet:       newSet,
	})
	if err != nil {
		t.Fatal(err)
	}
	return db, mock, arch
}

func expectPrimaryPass(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("super_read_only").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow("0"))
	rows := sqlmock.NewRows([]string{"Log_name", "File_size"})
	for _, name := range purgeLogs {
		rows.AddRow(name, "100")
	}
	mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(rows)
}

func expectPurgeTo(mock sqlmock.Sqlmock, file string) {
	mock.ExpectExec(regexp.QuoteMeta("PURGE BINARY LOGS TO '" + file + "'")).
		WillReturnResult(sqlmock.NewResult(0, 0))
}

func runPurgeTick(t *testing.T, db *sql.DB, arch *Archiver, floor ReplicaFloor) *Loop {
	t.Helper()
	loop := NewLoop(LoopOptions{
		Reader: NewReader(db), Archiver: arch, Logger: logr.Discard(),
		Purge: true, Floor: floor,
	})
	tick(loop)
	return loop
}

func tick(loop *Loop) {
	var lastFlush time.Time
	var lastSize int64
	loop.tick(context.Background(), &lastFlush, &lastSize)
}

func TestPurgeGate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		floor    ReplicaFloor
		purgeTo  string
		heldFile string
		heldBy   []string
	}{
		{
			// Every replica has everything archived: the archive bound decides,
			// as before the replica floor existed.
			name: "replicas caught up",
			floor: staticFloor{observed: true, positions: map[string]string{
				"demo-2": testUUID + ":1-15", "demo-3": testUUID + ":1-20",
			}},
			purgeTo: "binlog.000004",
		},
		{
			// demo-3 has applied up to 5, so binlog.000002 (4-6) is the first file
			// it still needs: the purge stops there and keeps it.
			name: "a replica is behind",
			floor: staticFloor{observed: true, positions: map[string]string{
				"demo-2": testUUID + ":1-15", "demo-3": testUUID + ":1-5",
			}},
			purgeTo:  "binlog.000002",
			heldFile: "binlog.000002",
			heldBy:   []string{"demo-3"},
		},
		{
			// Every replica needs the oldest file: nothing goes.
			name: "the oldest file is needed",
			floor: staticFloor{observed: true, positions: map[string]string{
				"demo-2": testUUID + ":1-2", "demo-3": testUUID + ":1",
			}},
			heldFile: "binlog.000001",
			heldBy:   []string{"demo-2", "demo-3"},
		},
		{
			// A joining instance has no known position yet: fail closed.
			name: "a position is unknown",
			floor: staticFloor{observed: true,
				positions: map[string]string{"demo-2": testUUID + ":1-15"},
				unknown:   []string{"demo-4"},
			},
			heldFile: "binlog.000001",
			heldBy:   []string{"demo-4"},
		},
		{
			// Nothing observed about the cluster yet: fail closed, and nobody to
			// blame.
			name:  "cluster not observed",
			floor: staticFloor{},
		},
		{
			// A single-instance cluster has no replica to wait for.
			name:    "no replicas",
			floor:   staticFloor{observed: true},
			purgeTo: "binlog.000004",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, mock, arch := purgeFixture(t, purgeSets, nil)
			expectPrimaryPass(mock)
			if tc.purgeTo != "" {
				expectPurgeTo(mock, tc.purgeTo)
			}

			loop := runPurgeTick(t, db, arch, tc.floor)

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
			state := loop.State()
			if state.LastError != "" {
				t.Fatalf("unexpected error: %q", state.LastError)
			}
			if state.purgeHeldFile != tc.heldFile {
				t.Fatalf("held file = %q, want %q", state.purgeHeldFile, tc.heldFile)
			}
			if !slices.Equal(state.PurgeHeldBy, tc.heldBy) {
				t.Fatalf("held by = %v, want %v", state.PurgeHeldBy, tc.heldBy)
			}
			if (tc.heldFile != "") == state.PurgeHeldSince.IsZero() {
				t.Fatalf("held since = %v with held file %q", state.PurgeHeldSince, tc.heldFile)
			}
		})
	}
}

// Without a floor the gate cannot know what the replicas need, so it keeps
// everything rather than fall back to the archive bound alone.
func TestPurgeGateWithoutFloorPurgesNothing(t *testing.T) {
	t.Parallel()
	db, mock, arch := purgeFixture(t, purgeSets, nil)
	expectPrimaryPass(mock)

	loop := runPurgeTick(t, db, arch, nil)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if got := loop.State().LastError; got != "" {
		t.Fatalf("unexpected error: %q", got)
	}
}

// A file with no transactions holds nothing any replica could miss.
func TestPurgeGateEmptyFileIsCovered(t *testing.T) {
	t.Parallel()
	sets := map[string]string{
		"binlog.000001": testUUID + ":1-3",
		"binlog.000002": "",
		"binlog.000003": testUUID + ":4-6",
		"binlog.000004": testUUID + ":7-9",
		"binlog.000005": testUUID + ":10-12",
	}
	db, mock, arch := purgeFixture(t, sets, nil)
	expectPrimaryPass(mock)
	expectPurgeTo(mock, "binlog.000003")

	runPurgeTick(t, db, arch, staticFloor{observed: true, positions: map[string]string{
		"demo-2": testUUID + ":1-3",
	}})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// PurgeHeldSince measures how long the same file has been kept, so it survives
// passes that hold that file and restarts when the floor moves on.
func TestPurgeGateHeldSinceTracksTheSameFile(t *testing.T) {
	t.Parallel()
	db, mock, arch := purgeFixture(t, purgeSets, nil)
	floor := &movingFloor{position: testUUID + ":1"}
	loop := NewLoop(LoopOptions{
		Reader: NewReader(db), Archiver: arch, Logger: logr.Discard(),
		Purge: true, Floor: floor,
	})

	expectPrimaryPass(mock)
	tick(loop)
	first := loop.State()
	if first.purgeHeldFile != "binlog.000001" || first.PurgeHeldSince.IsZero() {
		t.Fatalf("first pass held %q since %v", first.purgeHeldFile, first.PurgeHeldSince)
	}

	expectPrimaryPass(mock)
	tick(loop)
	if got := loop.State().PurgeHeldSince; !got.Equal(first.PurgeHeldSince) {
		t.Fatalf("held since moved from %v to %v while the same file was held", first.PurgeHeldSince, got)
	}

	// The replica applied binlog.000001 and 000002: the floor now holds 000003.
	floor.position = testUUID + ":1-6"
	expectPrimaryPass(mock)
	expectPurgeTo(mock, "binlog.000003")
	tick(loop)
	moved := loop.State()
	if moved.purgeHeldFile != "binlog.000003" || !moved.PurgeHeldSince.After(first.PurgeHeldSince) {
		t.Fatalf("after the floor moved: held %q since %v", moved.purgeHeldFile, moved.PurgeHeldSince)
	}

	floor.position = testUUID + ":1-15"
	expectPrimaryPass(mock)
	expectPurgeTo(mock, "binlog.000004")
	tick(loop)
	released := loop.State()
	if released.purgeHeldFile != "" || !released.PurgeHeldSince.IsZero() || released.PurgeHeldBy != nil {
		t.Fatalf("nothing is held, got %q since %v by %v",
			released.purgeHeldFile, released.PurgeHeldSince, released.PurgeHeldBy)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

type movingFloor struct{ position string }

func (f *movingFloor) Positions() (map[string]string, []string, bool) {
	return map[string]string{"demo-2": f.position}, nil, true
}

// A position the engine cannot parse is not evidence of anything: the pass
// fails and nothing is purged.
func TestPurgeGateUnparsablePositionFails(t *testing.T) {
	t.Parallel()
	db, mock, arch := purgeFixture(t, purgeSets, nil)
	expectPrimaryPass(mock)

	loop := runPurgeTick(t, db, arch, staticFloor{observed: true, positions: map[string]string{
		"demo-2": "not a gtid set",
	}})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if loop.State().LastError == "" {
		t.Fatal("an unparsable position must fail the pass")
	}
}

// MariaDB compares positions per replication domain, by sequence number. A
// replica ahead in one domain and behind in another still holds the file.
func TestPurgeGateMariaDBDomains(t *testing.T) {
	t.Parallel()
	eng, err := engine.ForFlavor(engine.FlavorMariaDB)
	if err != nil {
		t.Fatal(err)
	}
	model := eng.GTID()
	sets := map[string]string{
		"binlog.000001": "0-1-10",
		"binlog.000002": "0-1-20,1-1-5",
		"binlog.000003": "0-1-30,1-1-9",
		"binlog.000004": "0-1-40",
		"binlog.000005": "0-1-50",
	}
	db, mock, arch := purgeFixture(t, sets, func() GTIDOps { return NewMariadbGTIDSet(model) })
	expectPrimaryPass(mock)
	expectPurgeTo(mock, "binlog.000003")

	loop := runPurgeTick(t, db, arch, staticFloor{observed: true, positions: map[string]string{
		// Domain 0 is well past every file; domain 1 stops at 5.
		"demo-2": "0-1-100,1-1-5",
	}})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if got := loop.State().PurgeHeldBy; !slices.Equal(got, []string{"demo-2"}) {
		t.Fatalf("held by = %v", got)
	}
}
