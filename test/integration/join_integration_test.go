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
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/yyewolf/cnmysql/pkg/management/mysql/pool"
)

// joinScript drives a full XtraBackup-based replica provisioning inside one
// container: it initialises a source via `manager instance initdb`, seeds data,
// backs it up, provisions a replica via `manager instance join`, then runs the
// replica in the foreground. The operator performs this orchestration across
// pods in M3; here it is collapsed onto a single host to validate the manager's
// join path against real Percona + XtraBackup.
const joinScript = `set -e
export MYSQL_ROOT_PASSWORD=rootpass MYSQL_APP_PASSWORD=apppass MYSQL_REPLICATION_PASSWORD=replpass
SRC=/tmp/source REP=/tmp/replica BK=/tmp/backup
GTID_ARGS="--gtid-mode=ON --enforce-gtid-consistency=ON --log-bin=binlog --log-replica-updates=ON --binlog-format=ROW"

manager instance initdb --mysqld=/usr/sbin/mysqld --config='' \
  --data-dir=$SRC --socket=/tmp/src.sock \
  --database=app --owner=appuser --replication-user=repl

/usr/sbin/mysqld --datadir=$SRC --socket=/tmp/src.sock --port=3306 --server-id=1 $GTID_ARGS &
until mysqladmin --socket=/tmp/src.sock -uroot -p$MYSQL_ROOT_PASSWORD ping >/dev/null 2>&1; do sleep 1; done

mysql --socket=/tmp/src.sock -uroot -p$MYSQL_ROOT_PASSWORD app \
  -e "CREATE TABLE t (id INT PRIMARY KEY); INSERT INTO t VALUES (1);"

xtrabackup --backup --target-dir=$BK --datadir=$SRC \
  --socket=/tmp/src.sock --user=root --password=$MYSQL_ROOT_PASSWORD

manager instance join --xtrabackup=xtrabackup --mysqld=/usr/sbin/mysqld --config='' \
  --backup-dir=$BK --data-dir=$REP --socket=/tmp/reptemp.sock \
  --server-version=8.0.36 --source-host=127.0.0.1 --source-port=3306 \
  --replication-user=repl --source-get-public-key

exec /usr/sbin/mysqld --datadir=$REP --socket=/tmp/rep.sock --port=3307 --server-id=2 $GTID_ARGS
`

// TestJoinProvisionsReplica verifies that `instance join` clones a populated
// primary via XtraBackup and resumes GTID replication: the pre-existing row is
// present on the replica, and a subsequent write on the source propagates.
func TestJoinProvisionsReplica(t *testing.T) {
	ctx := context.Background()

	const (
		appUser = "appuser"
		appPass = "apppass"
	)

	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    buildInstanceContext(t),
			Dockerfile: "Dockerfile",
			KeepImage:  true,
		},
		ExposedPorts: []string{"3306/tcp", "3307/tcp"},
		Entrypoint:   []string{"bash", "-lc"},
		Cmd:          []string{joinScript},
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
