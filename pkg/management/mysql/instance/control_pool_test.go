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
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/pool"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/version"
)

func TestMoveControlToExtraPort(t *testing.T) {
	socketDB, socketMock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	socketMock.ExpectQuery("SELECT @@GLOBAL.extra_port").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(33062))
	socketMock.ExpectClose()
	tcpDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tcpDB.Close() })

	var got pool.Config
	db := moveControlToExtraPort(context.Background(), socketDB, pool.ControlParams{User: "control"},
		func(_ context.Context, cfg pool.Config) (*sql.DB, error) {
			got = cfg
			return tcpDB, nil
		})

	if db != tcpDB {
		t.Fatal("expected the extra port pool to replace the socket pool")
	}
	if got.Host != "127.0.0.1" || got.Port != 33062 || got.Socket != "" || got.User != "control" {
		t.Errorf("extra port pool config = %+v", got)
	}
	if err := socketMock.ExpectationsWereMet(); err != nil {
		t.Errorf("socket pool must be closed after the switch: %v", err)
	}
}

func TestMoveControlToExtraPortKeepsSocket(t *testing.T) {
	cases := map[string]struct {
		expect func(sqlmock.Sqlmock)
		open   error
	}{
		// mysqld still runs a config without extra_port, e.g. after an in-place
		// manager upgrade.
		"extra port disabled": {
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT @@GLOBAL.extra_port").
					WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(0))
			},
		},
		"variable unreadable": {
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT @@GLOBAL.extra_port").WillReturnError(errors.New("boom"))
			},
		},
		"extra port unreachable": {
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT @@GLOBAL.extra_port").
					WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(33062))
			},
			open: errors.New("connection refused"),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			socketDB, socketMock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = socketDB.Close() })
			tc.expect(socketMock)

			opened := false
			db := moveControlToExtraPort(context.Background(), socketDB, pool.ControlParams{User: "control"},
				func(context.Context, pool.Config) (*sql.DB, error) {
					opened = true
					return nil, tc.open
				})

			if db != socketDB {
				t.Fatal("expected the socket pool to be kept")
			}
			if tc.open == nil && opened {
				t.Error("extra port pool must not be opened")
			}
			if err := socketDB.PingContext(context.Background()); err != nil {
				t.Errorf("socket pool must stay open: %v", err)
			}
			if err := socketMock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

// sessionDriver is a database/sql driver where every connection is a distinct,
// numbered server session. It refuses to hand a connection back out of the pool
// (ResetSession reports it bad), so any statement not pinned to a connection
// lands on a fresh session — which is how a multi-connection pool behaves
// under concurrent use.
type sessionDriver struct {
	next   atomic.Int64
	mu     sync.Mutex
	stmts  []sessionStmt
	closed map[int64]bool
	// failOn makes Exec fail for statements with this prefix.
	failOn string
}

type sessionStmt struct {
	session int64
	query   string
}

func (d *sessionDriver) Open(string) (driver.Conn, error) {
	return &sessionConn{d: d, id: d.next.Add(1)}, nil
}

func (d *sessionDriver) record(id int64, q string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stmts = append(d.stmts, sessionStmt{session: id, query: q})
}

type sessionConn struct {
	d  *sessionDriver
	id int64
}

func (c *sessionConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unsupported") }
func (c *sessionConn) Begin() (driver.Tx, error)           { return nil, errors.New("unsupported") }
func (c *sessionConn) ResetSession(context.Context) error  { return driver.ErrBadConn }

func (c *sessionConn) Close() error {
	c.d.mu.Lock()
	defer c.d.mu.Unlock()
	c.d.closed[c.id] = true
	return nil
}

func (c *sessionConn) ExecContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
	c.d.record(c.id, q)
	if c.d.failOn != "" && strings.HasPrefix(q, c.d.failOn) {
		return nil, fmt.Errorf("refused %q", q)
	}
	return driver.RowsAffected(0), nil
}

func (c *sessionConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	c.d.record(c.id, q)
	return &singleValueRows{value: "1"}, nil
}

type singleValueRows struct {
	value string
	done  bool
}

func (r *singleValueRows) Columns() []string { return []string{"v"} }
func (r *singleValueRows) Close() error      { return nil }
func (r *singleValueRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = r.value
	return nil
}

type sessionConnector struct{ d *sessionDriver }

func (c sessionConnector) Connect(context.Context) (driver.Conn, error) { return c.d.Open("") }
func (c sessionConnector) Driver() driver.Driver                        { return c.d }

func newSessionDB(t *testing.T, failOn string) (*sql.DB, *sessionDriver) {
	t.Helper()
	d := &sessionDriver{closed: map[int64]bool{}, failOn: failOn}
	db := sql.OpenDB(sessionConnector{d: d})
	db.SetMaxOpenConns(pool.ControlPoolSize)
	t.Cleanup(func() { _ = db.Close() })
	return db, d
}

func TestConfigureSemiSyncPinsOneSession(t *testing.T) {
	db, d := newSessionDB(t, "")

	err := configureSemiSync(context.Background(), db, mustVersion(t, "8.0.36"), RunOptions{
		SemiSyncWaitCount:     1,
		SemiSyncTimeoutMillis: 10000,
	}, engine.MustForFlavor(engine.FlavorMySQL))
	if err != nil {
		t.Fatalf("configureSemiSync: %v", err)
	}

	if len(d.stmts) == 0 {
		t.Fatal("no statements recorded")
	}
	// SQL_LOG_BIN is session state: every statement it is meant to cover must
	// run on the session that disabled it.
	session := d.stmts[0].session
	for _, s := range d.stmts {
		if s.session != session {
			t.Errorf("%q ran on session %d, want %d", s.query, s.session, session)
		}
	}
	if d.closed[session] {
		t.Error("a session with binary logging restored should go back to the pool")
	}
}

func TestConfigureSemiSyncDiscardsSessionWithBinlogOff(t *testing.T) {
	db, d := newSessionDB(t, "SET SESSION SQL_LOG_BIN=1")

	err := configureSemiSync(context.Background(), db, mustVersion(t, "8.0.36"), RunOptions{},
		engine.MustForFlavor(engine.FlavorMySQL))
	if err == nil {
		t.Fatal("expected the failed SQL_LOG_BIN restore to surface")
	}

	// The session still has binary logging off: returning it to the pool would
	// let a later control statement skip the binlog.
	if session := d.stmts[0].session; !d.closed[session] {
		t.Errorf("session %d with SQL_LOG_BIN=0 was returned to the pool", session)
	}
}

func mustVersion(t *testing.T, s string) version.Version {
	t.Helper()
	v, err := version.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
