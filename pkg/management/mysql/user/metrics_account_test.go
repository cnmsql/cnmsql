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

package user

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

const (
	metricsExistsQuery = "SELECT COUNT(*) FROM mysql.user WHERE User = ? AND Host = ?"
	lowerCaseQuery     = "SELECT @@GLOBAL.lower_case_table_names"
)

func expectLowerCase(mock sqlmock.Sqlmock, value int) {
	mock.ExpectQuery(regexp.QuoteMeta(lowerCaseQuery)).WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(value))
}

func TestEnsureMetricsAccountCreatesAndGrants(t *testing.T) {
	m, mock := newManager(t)
	expectLowerCase(mock, 0)
	mock.ExpectQuery(regexp.QuoteMeta(metricsExistsQuery)).WithArgs("cnmsql_metrics", "localhost").
		WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(0))
	mock.ExpectExec(regexp.QuoteMeta("CREATE USER IF NOT EXISTS 'cnmsql_metrics'@'localhost'")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SHOW GRANTS FOR 'cnmsql_metrics'@'localhost'")).
		WillReturnRows(sqlmock.NewRows([]string{"g"}).AddRow("GRANT USAGE ON *.* TO `cnmsql_metrics`@`localhost`"))
	mock.ExpectExec(regexp.QuoteMeta(
		"GRANT PROCESS, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'cnmsql_metrics'@'localhost'")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("GRANT SELECT ON performance_schema.* TO 'cnmsql_metrics'@'localhost'")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("GRANT SELECT ON `app`.* TO 'cnmsql_metrics'@'localhost'")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	resp, err := m.EnsureMetricsAccount(context.Background(), "cnmsql_metrics", MetricsAccountRequest{
		Privileges: []Privilege{{Privileges: []string{"select"}, On: "app.*"}},
	})
	if err != nil {
		t.Fatalf("EnsureMetricsAccount: %v", err)
	}
	if !resp.Created || len(resp.Granted) != 5 || len(resp.Revoked) != 0 {
		t.Fatalf("unexpected response %+v", resp)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestEnsureMetricsAccountInSyncWritesNothing(t *testing.T) {
	m, mock := newManager(t)
	expectLowerCase(mock, 0)
	mock.ExpectQuery(regexp.QuoteMeta(metricsExistsQuery)).WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
	mock.ExpectQuery(regexp.QuoteMeta("SHOW GRANTS")).WillReturnRows(sqlmock.NewRows([]string{"g"}).
		AddRow("GRANT PROCESS, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `cnmsql_metrics`@`localhost`").
		AddRow("GRANT SELECT ON `performance_schema`.* TO `cnmsql_metrics`@`localhost`"))

	resp, err := m.EnsureMetricsAccount(context.Background(), "cnmsql_metrics", MetricsAccountRequest{})
	if err != nil {
		t.Fatalf("EnsureMetricsAccount: %v", err)
	}
	if resp.Created || len(resp.Granted) != 0 || len(resp.Revoked) != 0 {
		t.Fatalf("unexpected response %+v", resp)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestEnsureMetricsAccountRejectsInvalidRequest(t *testing.T) {
	for name, req := range map[string]MetricsAccountRequest{
		"write privilege": {Privileges: []Privilege{{Privileges: []string{"INSERT"}, On: "app.*"}}},
		"global target":   {Privileges: []Privilege{{Privileges: []string{"SELECT"}, On: "*.*"}}},
		"mysql schema":    {Privileges: []Privilege{{Privileges: []string{"SELECT"}, On: "mysql.*"}}},
		"injection": {Privileges: []Privilege{
			{Privileges: []string{"SELECT"}, On: "app.* TO x; DROP DATABASE app; --"},
		}},
	} {
		m, mock := newManager(t)
		_, err := m.EnsureMetricsAccount(context.Background(), "cnmsql_metrics", req)
		if _, ok := errors.AsType[*InvalidRequestError](err); !ok {
			t.Errorf("%s: expected InvalidRequestError, got %v", name, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestEnsureMetricsAccountNeedsAName(t *testing.T) {
	m, _ := newManager(t)
	if _, err := m.EnsureMetricsAccount(context.Background(), "", MetricsAccountRequest{}); err == nil {
		t.Fatal("expected an error without a metrics account name")
	}
}

const (
	metricsBaseGlobal = "GRANT PROCESS, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `cnmsql_metrics`@`localhost`"
	metricsBasePS     = "GRANT SELECT ON `performance_schema`.* TO `cnmsql_metrics`@`localhost`"
)

func TestEnsureMetricsAccountFoldsCaseLikeTheServer(t *testing.T) {
	// With lower_case_table_names=1 the server stores and prints App.* as
	// app.*, so the declared target must compare in lower case too.
	m, mock := newManager(t)
	expectLowerCase(mock, 1)
	mock.ExpectQuery(regexp.QuoteMeta(metricsExistsQuery)).WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
	mock.ExpectQuery(regexp.QuoteMeta("SHOW GRANTS")).WillReturnRows(sqlmock.NewRows([]string{"g"}).
		AddRow(metricsBaseGlobal).AddRow(metricsBasePS).
		AddRow("GRANT SELECT ON `app`.* TO `cnmsql_metrics`@`localhost`"))

	resp, err := m.EnsureMetricsAccount(context.Background(), "cnmsql_metrics", MetricsAccountRequest{
		Privileges: []Privilege{{Privileges: []string{"SELECT"}, On: "App.*"}},
	})
	if err != nil {
		t.Fatalf("EnsureMetricsAccount: %v", err)
	}
	if len(resp.Granted) != 0 || len(resp.Revoked) != 0 {
		t.Fatalf("expected no change, got %+v", resp)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestEnsureMetricsAccountKeepsGoingAfterAFailedGrant(t *testing.T) {
	// A table grant declared before the table exists fails. The out-of-band
	// grant must still be revoked and the other declared grants applied.
	m, mock := newManager(t)
	expectLowerCase(mock, 0)
	mock.ExpectQuery(regexp.QuoteMeta(metricsExistsQuery)).WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
	mock.ExpectQuery(regexp.QuoteMeta("SHOW GRANTS")).WillReturnRows(sqlmock.NewRows([]string{"g"}).
		AddRow(metricsBaseGlobal).AddRow(metricsBasePS).
		AddRow("GRANT INSERT ON `app`.* TO `cnmsql_metrics`@`localhost`"))
	mock.ExpectExec(regexp.QuoteMeta("REVOKE INSERT ON `app`.* FROM 'cnmsql_metrics'@'localhost'")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("GRANT SELECT ON `app`.`nosuch` TO 'cnmsql_metrics'@'localhost'")).
		WillReturnError(&mysql.MySQLError{Number: 1146, Message: "Table 'app.nosuch' doesn't exist"})
	mock.ExpectExec(regexp.QuoteMeta("GRANT SELECT ON `app`.* TO 'cnmsql_metrics'@'localhost'")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	_, err := m.EnsureMetricsAccount(context.Background(), "cnmsql_metrics", MetricsAccountRequest{
		Privileges: []Privilege{
			{Privileges: []string{"SELECT"}, On: "app.nosuch"},
			{Privileges: []string{"SELECT"}, On: "app.*"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "nosuch") {
		t.Fatalf("expected the failed grant to be reported, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
