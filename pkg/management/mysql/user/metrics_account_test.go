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
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

const metricsExistsQuery = "SELECT COUNT(*) FROM mysql.user WHERE User = ? AND Host = ?"

func TestEnsureMetricsAccountCreatesAndGrants(t *testing.T) {
	m, mock := newManager(t)
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
