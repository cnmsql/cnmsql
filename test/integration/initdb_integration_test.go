//go:build integration

/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/pool"
)

// TestInitdbBootstrapsWorkingServer runs `manager instance initdb` to initialise
// a fresh data directory and create the application account, then starts mysqld
// and verifies the account works. It runs across every supported MySQL flavor.
func TestInitdbBootstrapsWorkingServer(t *testing.T) {
	for _, f := range flavors {
		t.Run(f.name, func(t *testing.T) {
			t.Parallel()
			runInitdbTest(t, f)
		})
	}
}

func runInitdbTest(t *testing.T, f flavor) {
	ctx := context.Background()

	const (
		appDB   = "app"
		appUser = "appuser"
		appPass = "apppass"
	)

	script := fmt.Sprintf(`set -e
export MYSQL_ROOT_PASSWORD=rootpass MYSQL_APP_PASSWORD=%s
manager instance initdb --mysqld=/usr/sbin/mysqld --config='' \
  --data-dir=/var/lib/mysql --socket=/var/run/mysqld/mysqld.sock \
  --database=%s --owner=%s --server-version=%s
exec /usr/sbin/mysqld --datadir=/var/lib/mysql --socket=/var/run/mysqld/mysqld.sock
`, appPass, appDB, appUser, f.version)

	req := testcontainers.ContainerRequest{
		Image:        ensureInstanceImage(t, f),
		ExposedPorts: []string{"3306/tcp"},
		Env:          map[string]string{"MYSQL_ROOT_PASSWORD": rootPassword, "MYSQL_APP_PASSWORD": appPass},
		Entrypoint:   []string{"bash", "-lc"},
		Cmd:          []string{script},
		WaitingFor:   wait.ForLog("ready for connections").WithStartupTimeout(5 * time.Minute),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("starting instance container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mapped, err := container.MappedPort(ctx, "3306")
	if err != nil {
		t.Fatal(err)
	}

	cfg := pool.Config{
		Host:     host,
		Port:     int(mapped.Num()),
		User:     appUser,
		Password: appPass,
		Database: appDB,
	}
	// The container log "ready for connections" appears during startup but the
	// connection may still be briefly refused; retry for up to 90 seconds.
	var db *sql.DB
	deadline := time.Now().Add(90 * time.Second)
	for {
		db, err = pool.Open(ctx, cfg)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("connecting as application user: %v", err)
		}
		time.Sleep(time.Second)
	}
	t.Cleanup(func() { _ = db.Close() })

	mustExec(ctx, t, db, "CREATE TABLE t (id INT PRIMARY KEY)")
	mustExec(ctx, t, db, "INSERT INTO t VALUES (1)")

	var id int
	if err := db.QueryRowContext(ctx, "SELECT id FROM t WHERE id = 1").Scan(&id); err != nil {
		t.Fatalf("querying application table: %v", err)
	}
	if id != 1 {
		t.Errorf("unexpected row: %d", id)
	}

	var gtidMode string
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_mode").Scan(&gtidMode); err != nil {
		t.Fatalf("reading gtid_mode: %v", err)
	}
}
