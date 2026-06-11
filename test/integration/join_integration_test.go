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

// TestJoinProvisionsReplica verifies that `instance join` clones a populated
// primary via XtraBackup and resumes GTID replication: the pre-existing row is
// present on the replica, and a subsequent write on the source propagates. It
// runs across every supported MySQL flavor.
func TestJoinProvisionsReplica(t *testing.T) {
	for _, f := range flavors {
		t.Run(f.name, func(t *testing.T) {
			t.Parallel()
			runJoinTest(t, f)
		})
	}
}

func runJoinTest(t *testing.T, f flavor) {
	if !f.joinSupported {
		t.Skip("XtraBackup-based replica provisioning is not supported on this flavor's image")
	}

	ctx := context.Background()

	const (
		appUser = "appuser"
		appPass = "apppass"
	)

	// One container drives the whole flow: initialise a source, seed data, back
	// it up, provision a replica via join, then run the replica in foreground.
	// The operator performs this across pods in M3.
	script := fmt.Sprintf(`set -e
export MYSQL_ROOT_PASSWORD=rootpass MYSQL_APP_PASSWORD=%s MYSQL_REPLICATION_PASSWORD=replpass
SRC=/tmp/source REP=/tmp/replica BK=/tmp/backup
GA="%s"
manager instance initdb --mysqld=/usr/sbin/mysqld --config='' \
  --data-dir=$SRC --socket=/tmp/src.sock \
  --database=app --owner=%s --replication-user=repl --server-version=%s
/usr/sbin/mysqld --datadir=$SRC --socket=/tmp/src.sock --port=3306 --server-id=1 $GA >/tmp/src.log 2>&1 &
until mysqladmin --socket=/tmp/src.sock -uroot -prootpass ping >/dev/null 2>&1; do sleep 1; done
mysql --socket=/tmp/src.sock -uroot -prootpass app -e "CREATE TABLE t (id INT PRIMARY KEY); INSERT INTO t VALUES (1);"
xtrabackup --backup --target-dir=$BK --datadir=$SRC --socket=/tmp/src.sock --user=root --password=rootpass
manager instance join --xtrabackup=xtrabackup --mysqld=/usr/sbin/mysqld --config='' \
  --backup-dir=$BK --data-dir=$REP --socket=/tmp/reptemp.sock \
  --server-version=%s --source-host=127.0.0.1 --source-port=3306 \
  --replication-user=repl --source-get-public-key
exec /usr/sbin/mysqld --datadir=$REP --socket=/tmp/rep.sock --port=3307 --server-id=2 $GA
`, appPass, f.gtidArgs(t), appUser, f.version, f.version)

	req := testcontainers.ContainerRequest{
		Image:          ensureInstanceImage(t, f),
		ExposedPorts: []string{"3306/tcp", "3307/tcp"},
		Entrypoint:   []string{"bash", "-lc"},
		Cmd:          []string{script},
		WaitingFor:   wait.ForListeningPort("3307/tcp").WithStartupTimeout(5 * time.Minute),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("starting container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}

	open := func(port string) pool.Config {
		mapped, err := container.MappedPort(ctx, port)
		if err != nil {
			t.Fatalf("mapped port %s: %v", port, err)
		}
		return pool.Config{Host: host, Port: int(mapped.Num()), User: appUser, Password: appPass, Database: "app"}
	}

	replicaDB, err := pool.Open(ctx, open("3307"))
	if err != nil {
		t.Fatalf("connecting to replica: %v", err)
	}
	t.Cleanup(func() { _ = replicaDB.Close() })

	// The row that existed before the backup must be present on the replica.
	waitFor(t, 30*time.Second, func() bool {
		var id int
		err := replicaDB.QueryRowContext(ctx, "SELECT id FROM t WHERE id = 1").Scan(&id)
		return err == nil && id == 1
	})

	// A new write on the source must replicate to the joined replica.
	sourceDB, err := pool.Open(ctx, open("3306"))
	if err != nil {
		t.Fatalf("connecting to source: %v", err)
	}
	t.Cleanup(func() { _ = sourceDB.Close() })

	mustExec(ctx, t, sourceDB, "INSERT INTO t VALUES (2)")
	waitFor(t, 30*time.Second, func() bool {
		var id int
		err := replicaDB.QueryRowContext(ctx, "SELECT id FROM t WHERE id = 2").Scan(&id)
		return err == nil && id == 2
	})
}
