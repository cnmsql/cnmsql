/*
Copyright 2026 The CNMySQL Authors.

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

package replication

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

func newManager(t *testing.T, ver string) (*Manager, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewManager(db, mustParse(t, ver)), mock
}

func TestConfigureSourceOrdering(t *testing.T) {
	m, mock := newManager(t, "8.0.36")

	mock.ExpectExec("STOP REPLICA").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CHANGE REPLICATION SOURCE TO").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("START REPLICA").WillReturnResult(sqlmock.NewResult(0, 0))

	err := m.ConfigureSource(context.Background(), SourceOptions{
		Host: "primary", Port: 3306, User: "repl", AutoPosition: true,
	})
	if err != nil {
		t.Fatalf("ConfigureSource: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestProvisionFromBackupOrdering(t *testing.T) {
	m, mock := newManager(t, "8.0.36")

	mock.ExpectExec("RESET MASTER").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("SET GLOBAL gtid_purged = 'uuid:1-10'")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("STOP REPLICA").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CHANGE REPLICATION SOURCE TO").WillReturnResult(sqlmock.NewResult(0, 0))
	// No START REPLICA: provisioning configures only; the real server resumes.

	err := m.ProvisionFromBackup(context.Background(), "uuid:1-10", SourceOptions{
		Host: "primary", User: "repl", AutoPosition: true,
	})
	if err != nil {
		t.Fatalf("ProvisionFromBackup: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestProvisionFromBackupSkipsEmptyGTID(t *testing.T) {
	m, mock := newManager(t, "8.0.36")

	mock.ExpectExec("RESET MASTER").WillReturnResult(sqlmock.NewResult(0, 0))
	// No SET GLOBAL gtid_purged expected when the set is empty.
	mock.ExpectExec("STOP REPLICA").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CHANGE REPLICATION SOURCE TO").WillReturnResult(sqlmock.NewResult(0, 0))
	// No START REPLICA: provisioning configures only; the real server resumes.

	if err := m.ProvisionFromBackup(context.Background(), "", SourceOptions{Host: "p", User: "r"}); err != nil {
		t.Fatalf("ProvisionFromBackup: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestEnsureReplicaStartedNoopOnPrimary(t *testing.T) {
	m, mock := newManager(t, "8.0.36")

	mock.ExpectQuery("SHOW REPLICA STATUS").
		WillReturnRows(sqlmock.NewRows([]string{"Source_Host", "Replica_IO_Running", "Replica_SQL_Running"}))

	if err := m.EnsureReplicaStarted(context.Background()); err != nil {
		t.Fatalf("EnsureReplicaStarted: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestEnsureReplicaStartedNoopWhenRunning(t *testing.T) {
	m, mock := newManager(t, "8.0.36")

	mock.ExpectQuery("SHOW REPLICA STATUS").
		WillReturnRows(sqlmock.NewRows([]string{"Source_Host", "Replica_IO_Running", "Replica_SQL_Running"}).
			AddRow("primary.svc", "Yes", "Yes"))

	if err := m.EnsureReplicaStarted(context.Background()); err != nil {
		t.Fatalf("EnsureReplicaStarted: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestEnsureReplicaStartedStartsStoppedReplica(t *testing.T) {
	m, mock := newManager(t, "8.0.36")

	mock.ExpectQuery("SHOW REPLICA STATUS").
		WillReturnRows(sqlmock.NewRows([]string{"Source_Host", "Replica_IO_Running", "Replica_SQL_Running"}).
			AddRow("primary.svc", "No", "Yes"))
	mock.ExpectExec("START REPLICA").WillReturnResult(sqlmock.NewResult(0, 0))

	if err := m.EnsureReplicaStarted(context.Background()); err != nil {
		t.Fatalf("EnsureReplicaStarted: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestEnsureReplicaConfiguredConfiguresMissingSource(t *testing.T) {
	m, mock := newManager(t, "8.0.36")

	mock.ExpectQuery("SHOW REPLICA STATUS").
		WillReturnRows(sqlmock.NewRows([]string{"Source_Host", "Replica_IO_Running", "Replica_SQL_Running"}))
	mock.ExpectExec("STOP REPLICA").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CHANGE REPLICATION SOURCE TO").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("START REPLICA").WillReturnResult(sqlmock.NewResult(0, 0))

	err := m.EnsureReplicaConfigured(context.Background(), SourceOptions{
		Host: "primary", Port: 3306, User: "repl", AutoPosition: true,
	})
	if err != nil {
		t.Fatalf("EnsureReplicaConfigured: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestPromoteOrdering(t *testing.T) {
	m, mock := newManager(t, "8.0.36")

	mock.ExpectExec("STOP REPLICA").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("RESET REPLICA ALL").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("SET GLOBAL super_read_only = OFF")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("SET GLOBAL read_only = OFF")).WillReturnResult(sqlmock.NewResult(0, 0))

	if err := m.Promote(context.Background()); err != nil {
		t.Fatalf("Promote: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestDemoteOrdering(t *testing.T) {
	m, mock := newManager(t, "8.0.36")

	mock.ExpectExec(regexp.QuoteMeta("SET GLOBAL read_only = ON")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("SET GLOBAL super_read_only = ON")).WillReturnResult(sqlmock.NewResult(0, 0))

	if err := m.Demote(context.Background()); err != nil {
		t.Fatalf("Demote: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestSetSuperReadOnlyNoopOnLegacy(t *testing.T) {
	m, mock := newManager(t, "5.6.51")
	// No expectations registered: a 5.6 server must not receive super_read_only.
	if err := m.SetSuperReadOnly(context.Background(), true); err != nil {
		t.Fatalf("SetSuperReadOnly: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestInstallSemiSyncIdempotent(t *testing.T) {
	m, mock := newManager(t, "8.0.36")

	mock.ExpectExec("INSTALL PLUGIN rpl_semi_sync_source").
		WillReturnError(&mysql.MySQLError{Number: 1968, Message: "plugin already installed"})

	if err := m.InstallSemiSyncSource(context.Background()); err != nil {
		t.Fatalf("InstallSemiSyncSource should ignore 'already installed': %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestVersionAwareLegacyKeywords(t *testing.T) {
	m, mock := newManager(t, "5.7.44")

	mock.ExpectExec("STOP SLAVE").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CHANGE MASTER TO").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("START SLAVE").WillReturnResult(sqlmock.NewResult(0, 0))

	err := m.ConfigureSource(context.Background(), SourceOptions{Host: "p", User: "r"})
	if err != nil {
		t.Fatalf("ConfigureSource: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
