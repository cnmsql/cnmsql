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

//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/yyewolf/cnmysql/pkg/management/mysql/pool"
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
		Image:          ensureInstanceImage(t, f),
		ExposedPorts: []string{"3306/tcp"},
		Env:          map[string]string{"MYSQL_ROOT_PASSWORD": rootPassword, "MYSQL_APP_PASSWORD": appPass},
		Entrypoint:   []string{"bash", "-lc"},
		Cmd:          []string{script},
		WaitingFor:   wait.ForListeningPort("3306/tcp").WithStartupTimeout(5 * time.Minute),
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

	db, err := pool.Open(ctx, pool.Config{
		Host:     host,
		Port:     int(mapped.Num()),
		User:     appUser,
		Password: appPass,
		Database: appDB,
	})
	if err != nil {
		t.Fatalf("connecting as application user: %v", err)
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
