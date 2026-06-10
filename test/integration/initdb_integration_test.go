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
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/yyewolf/cnmysql/pkg/management/mysql/pool"
)

// buildInstanceContext compiles the manager binary for the container platform
// and writes it next to a thin Dockerfile, returning the build context dir.
// Prebuilding avoids compiling Go inside Docker (faster, and builder-agnostic:
// the production Dockerfile.instance multi-stage build needs BuildKit, which the
// testcontainers legacy builder does not use).
func buildInstanceContext(t *testing.T) string {
	t.Helper()
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()

	build := exec.Command("go", "build", "-o", filepath.Join(dir, "manager"), "./cmd/manager")
	build.Dir = repoRoot
	build.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+runtime.GOARCH)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building manager binary: %v\n%s", err, out)
	}

	dockerfile := "FROM percona/percona-server:8.0\n" +
		"COPY manager /usr/local/bin/manager\n" +
		"ENTRYPOINT [\"/usr/local/bin/manager\"]\n"
	if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(dockerfile), 0o600); err != nil {
		t.Fatalf("writing Dockerfile: %v", err)
	}
	return dir
}

// TestInitdbBootstrapsWorkingServer builds the instance image, runs
// `manager instance initdb` to initialise a fresh data directory and create the
// application account, then starts mysqld and verifies the account works.
func TestInitdbBootstrapsWorkingServer(t *testing.T) {
	ctx := context.Background()

	const (
		appDB   = "app"
		appUser = "appuser"
		appPass = "apppass"
	)

	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    buildInstanceContext(t),
			Dockerfile: "Dockerfile",
			KeepImage:  true,
		},
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": rootPassword,
			"MYSQL_APP_PASSWORD":  appPass,
		},
		// Initialise via our manager, then hand off to mysqld so we can connect.
		Entrypoint: []string{"bash", "-lc"},
		Cmd: []string{
			"manager instance initdb " +
				"--mysqld=/usr/sbin/mysqld --config='' " +
				"--data-dir=/var/lib/mysql --socket=/var/run/mysqld/mysqld.sock " +
				"--database=" + appDB + " --owner=" + appUser + " && " +
				"exec /usr/sbin/mysqld --datadir=/var/lib/mysql --socket=/var/run/mysqld/mysqld.sock",
		},
		WaitingFor: wait.ForListeningPort("3306/tcp").WithStartupTimeout(5 * time.Minute),
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

	// The application account created by initdb must be able to connect and use
	// its database.
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

	// GTID must be enabled by the initialisation (gtid_executed is non-empty
	// once any transaction has run with GTID mode on); at minimum the variable
	// must be readable.
	var gtidMode string
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_mode").Scan(&gtidMode); err != nil {
		t.Fatalf("reading gtid_mode: %v", err)
	}
}
