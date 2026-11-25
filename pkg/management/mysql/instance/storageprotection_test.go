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
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-logr/logr"
	"github.com/go-sql-driver/mysql"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/diskusage"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// usageAt builds a data-volume snapshot whose used share is the given percent
// of a 1,000,000-byte capacity, so ratios compare exactly against thresholds.
func usageAt(usedPercent float64) diskusage.Usage {
	const capacity = int64(1_000_000)
	used := int64(usedPercent / 100 * float64(capacity))
	return diskusage.Usage{
		UsedBytes:      used,
		CapacityBytes:  capacity,
		AvailableBytes: capacity - used,
	}
}

// usageNoAvailable is a snapshot whose ratio is below the threshold but whose
// space free to an unprivileged writer is exhausted: root-reserved blocks can
// hide a full volume behind a low used%.
func usageNoAvailable(usedPercent float64) diskusage.Usage {
	usage := usageAt(usedPercent)
	usage.AvailableBytes = 0
	return usage
}

func TestStorageDecision(t *testing.T) {
	t.Parallel()
	const threshold = 0.90
	tests := []struct {
		name            string
		usage           diskusage.Usage
		readOnlyAlready bool
		writable        bool
		engaged         bool
		wantSet         bool
		wantClear       bool
		wantLatch       bool
	}{
		{
			name:     "pressure at the threshold engages",
			usage:    usageAt(90),
			writable: true,
			wantSet:  true, wantLatch: true,
		},
		{
			name:            "pressure holds when already read-only",
			usage:           usageAt(95),
			readOnlyAlready: true,
			writable:        true,
			engaged:         true,
			wantLatch:       true,
		},
		{
			name:     "exhausted unprivileged space engages below the threshold",
			usage:    usageNoAvailable(85),
			writable: true,
			wantSet:  true, wantLatch: true,
		},
		{
			name:            "hysteresis band holds the engagement",
			usage:           usageAt(87),
			readOnlyAlready: true,
			writable:        true,
			engaged:         true,
			wantLatch:       true,
		},
		{
			name:            "recovered volume clears the engagement",
			usage:           usageAt(84),
			engaged:         true,
			readOnlyAlready: true,
			writable:        true,
			wantClear:       true,
		},
		{
			name:            "recovered volume without engagement does nothing",
			usage:           usageAt(84),
			writable:        true,
			readOnlyAlready: true,
		},
		{
			name:            "demoted instance drops the latch without touching the flags",
			usage:           usageAt(95),
			engaged:         true,
			writable:        false,
			readOnlyAlready: true,
		},
		{
			name:     "unwritable healthy instance does nothing",
			usage:    usageAt(50),
			writable: false,
		},
		{
			name:     "unreadable volume (zero capacity) is not pressure",
			usage:    diskusage.Usage{},
			writable: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			set, clear, latch := storageDecision(tc.usage, tc.readOnlyAlready, tc.writable, tc.engaged, threshold)
			if set != tc.wantSet || clear != tc.wantClear || latch != tc.wantLatch {
				t.Fatalf("storageDecision = (set=%v, clear=%v, latch=%v), want (set=%v, clear=%v, latch=%v)",
					set, clear, latch, tc.wantSet, tc.wantClear, tc.wantLatch)
			}
		})
	}
}

// newProtectionController builds a controller over a sqlmock connection, the
// same way startStorageProtection wires the watchdog, with a fixed usage
// snapshot in place of the statfs.
func newProtectionController(
	t *testing.T, role webserver.Role, flavor engine.Flavor, usage diskusage.Usage,
) (*StorageProtector, *Controller, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	versionStr := "8.0.36"
	if flavor == engine.FlavorMariaDB {
		versionStr = "11.4.5"
	}
	controller, err := NewController("cluster-1", db, versionStr, role, nil, engine.MustForFlavor(flavor))
	if err != nil {
		t.Fatalf("NewController: %v", err)
	}
	p := newStorageProtector(controller,
		func() (diskusage.Usage, error) { return usage, nil },
		90, newStorageProtectionClusterView("cluster-1"))
	p.log = logr.Discard()
	return p, controller, mock
}

// errUnknownSystemVariable is ER_UNKNOWN_SYSTEM_VARIABLE (1193).
var errUnknownSystemVariable = &mysql.MySQLError{Number: 1193, Message: "Unknown system variable"}

// expectReadOnly registers the reads the live read-only check issues: both
// flags on MySQL, read_only alone on engines without super_read_only.
func expectReadOnly(mock sqlmock.Sqlmock, readOnly, superReadOnly string) {
	mock.ExpectQuery(regexp.QuoteMeta("SELECT @@GLOBAL.read_only")).
		WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(readOnly))
	if superReadOnly != "" {
		mock.ExpectQuery(regexp.QuoteMeta("SELECT @@GLOBAL.super_read_only")).
			WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(superReadOnly))
	}
}

func TestStorageProtectionEngagesOnPressure(t *testing.T) {
	t.Parallel()
	p, controller, mock := newProtectionController(t, webserver.RolePrimary, engine.FlavorMySQL, usageAt(90))

	// The writable primary is at the threshold: read_only first, then
	// super_read_only, in the order demotion uses.
	expectReadOnly(mock, "0", "0")
	mock.ExpectExec(regexp.QuoteMeta("SET GLOBAL read_only = ON")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("SET GLOBAL super_read_only = ON")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	p.Tick(context.Background())
	if !controller.StorageProtectionEngaged() {
		t.Fatal("protection should be engaged after a pass at the threshold")
	}
	if !p.engaged {
		t.Fatal("protector latch should be set")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStorageProtectionEngagesReadOnlyOnlyOnMariaDB(t *testing.T) {
	t.Parallel()
	p, _, mock := newProtectionController(t, webserver.RolePrimary, engine.FlavorMariaDB, usageAt(90))

	// MariaDB has no super_read_only: the live read never asks for it, and the
	// engagement sets read_only alone.
	mock.ExpectQuery(regexp.QuoteMeta("SELECT @@GLOBAL.read_only")).
		WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("0"))
	mock.ExpectExec(regexp.QuoteMeta("SET GLOBAL read_only = ON")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	p.Tick(context.Background())
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStorageProtectionReleasesAfterRecovery(t *testing.T) {
	t.Parallel()
	p, controller, mock := newProtectionController(t, webserver.RolePrimary, engine.FlavorMySQL, usageAt(84))
	p.engaged = true

	// Usage recovered below the hysteresis margin: super_read_only clears
	// before read_only, in the order promotion uses.
	expectReadOnly(mock, "1", "1")
	mock.ExpectExec(regexp.QuoteMeta("SET GLOBAL super_read_only = OFF")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("SET GLOBAL read_only = OFF")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	p.Tick(context.Background())
	if controller.StorageProtectionEngaged() {
		t.Fatal("protection should be released after recovery")
	}
	if p.engaged {
		t.Fatal("protector latch should be clear")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStorageProtectionLeavesTheFlagsToTheSemiSyncFence(t *testing.T) {
	t.Parallel()
	p, controller, mock := newProtectionController(t, webserver.RolePrimary, engine.FlavorMySQL, usageAt(84))
	p.engaged = true
	controller.setSemiSyncReadOnlyEngaged(true)

	// Usage recovered, but the semi-sync fence (design 044) holds the primary
	// read-only too: the latch drops, the flags stay.
	expectReadOnly(mock, "1", "1")

	p.Tick(context.Background())
	if p.engaged || controller.StorageProtectionEngaged() {
		t.Fatal("protector latch should be clear")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStorageProtectionHoldsWithoutServerChanges(t *testing.T) {
	t.Parallel()
	p, _, mock := newProtectionController(t, webserver.RolePrimary, engine.FlavorMySQL, usageAt(50))

	// A healthy volume: the pass only reads the flags and asserts nothing.
	expectReadOnly(mock, "0", "0")

	p.Tick(context.Background())
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStorageProtectionNeverTouchesReplicas(t *testing.T) {
	t.Parallel()
	p, _, mock := newProtectionController(t, webserver.RoleReplica, engine.FlavorMySQL, usageAt(90))

	// A replica holds no writable-primary intent; the pass reads the flags and
	// stops there.
	expectReadOnly(mock, "1", "1")

	p.Tick(context.Background())
	if p.engaged {
		t.Fatal("a replica must never report protection engagement")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStorageProtectionSurvivesAReadFailure(t *testing.T) {
	t.Parallel()
	p, controller, mock := newProtectionController(t, webserver.RolePrimary, engine.FlavorMySQL, usageAt(90))

	// mysqld is not answering: the pass aborts before touching anything, and
	// the next tick retries.
	mock.ExpectQuery(regexp.QuoteMeta("SELECT @@GLOBAL.read_only")).
		WillReturnError(errUnknownSystemVariable)

	p.Tick(context.Background())
	if controller.StorageProtectionEngaged() {
		t.Fatal("a failed pass must not report engagement")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
