//go:build integration

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

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/user"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// logicalImage is one instance image the logical backup round trip runs on.
type logicalImage struct {
	name    string
	image   string
	version string
	flavor  engine.Flavor
	cnf     string
}

func mariadbImageRepo() string {
	if v := strings.TrimSpace(os.Getenv("MARIADB_INSTANCE_IMAGE_REPO")); v != "" {
		return v
	}
	return "ghcr.io/cnmsql/cnmsql-mariadb-instance"
}

const mariadbLogicalCnf = `[mysqld]
server-id=1
log_bin=binlog
gtid_strict_mode=ON
binlog_format=ROW
`

// mariadbSeries is the MariaDB half of the round-trip matrix, in upgrade
// order. The version matrix mirrors the containers repo's images/versions.json
// — keep the two in sync.
var mariadbSeries = []struct{ tag, version string }{
	{"10.11", "10.11.19"}, {"11.4", "11.4.13"}, {"11.8", "11.8.9"}, {"12.3", "12.3.3"},
}

// selectedMariaDBSeries returns the MariaDB series to exercise. By default it
// is the full list; setting E2E_MARIADB_VERSION pins a single series so a CI
// matrix job can run one flavor per job, mirroring selectedFlavors.
func selectedMariaDBSeries(t *testing.T) []struct{ tag, version string } {
	t.Helper()
	want := strings.TrimSpace(os.Getenv("E2E_MARIADB_VERSION"))
	if want == "" {
		return mariadbSeries
	}
	for _, m := range mariadbSeries {
		if m.tag == want {
			return []struct{ tag, version string }{m}
		}
	}
	t.Fatalf("E2E_MARIADB_VERSION=%q matches no known MariaDB series", want)
	return nil
}

// logicalImages is the images the round trip runs on, in upgrade order per
// flavor: the MySQL half from selectedFlavors and the MariaDB half from
// selectedMariaDBSeries. Each dump is loaded into the next series of the same
// flavor, so the round trip also covers the cross-series moves logical
// backups exist for.
func logicalImages(t *testing.T) []logicalImage {
	t.Helper()
	var out []logicalImage
	for _, f := range selectedFlavors(t) {
		out = append(out, logicalImage{
			name: "mysql-" + f.name, image: instanceImage(f), version: f.version,
			flavor: engine.FlavorMySQL, cnf: f.myCnf(t, 1),
		})
	}
	for _, m := range selectedMariaDBSeries(t) {
		out = append(out, logicalImage{
			name: "mariadb-" + m.tag, image: mariadbImageRepo() + ":" + m.tag, version: m.version,
			flavor: engine.FlavorMariaDB, cnf: mariadbLogicalCnf,
		})
	}
	return out
}

var (
	managerOnce sync.Once
	managerPath string
	managerErr  error
)

// buildManager compiles the instance manager for the container platform. The
// published images do not ship it (the operator copies it into each Pod), so
// the test copies it in the same way.
func buildManager(t *testing.T) string {
	t.Helper()
	managerOnce.Do(func() {
		dir, err := os.MkdirTemp("", "cnmsql-manager-")
		if err != nil {
			managerErr = err
			return
		}
		managerPath = filepath.Join(dir, "manager")
		cmd := exec.Command("go", "build", "-o", managerPath, "./cmd/manager")
		cmd.Dir = filepath.Join("..", "..")
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+runtime.GOARCH)
		if out, err := cmd.CombinedOutput(); err != nil {
			managerErr = fmt.Errorf("building manager: %v\n%s", err, out)
		}
	})
	if managerErr != nil {
		t.Fatal(managerErr)
	}
	return managerPath
}

type logicalNode struct {
	img       logicalImage
	container testcontainers.Container
	baseURL   string
}

// startLogicalNode runs `instance initdb` then `instance run` in the image, as
// an instance Pod does, and waits for the control API.
func startLogicalNode(ctx context.Context, t *testing.T, img logicalImage) *logicalNode {
	t.Helper()
	script := fmt.Sprintf(`set -e
export MYSQL_ROOT_PASSWORD=rootpass MYSQL_CONTROL_PASSWORD=ctlpass MYSQL_APP_PASSWORD=apppass
export CNMSQL_FLAVOR=%[1]s
cat > /tmp/my.cnf <<'CFG'
%[2]sCFG
manager instance initdb --mysqld=/usr/sbin/mysqld --config=/tmp/my.cnf \
  --data-dir=/var/lib/mysql --socket=/tmp/mysql.sock \
  --database=app --owner=appuser --control-user=control --server-version=%[3]s
exec manager instance run --mysqld=/usr/sbin/mysqld --config=/tmp/my.cnf \
  --data-dir=/var/lib/mysql --socket=/tmp/mysql.sock --server-version=%[3]s \
  --instance-name=%[4]s --control-user=control --web-addr=:8080
`, img.flavor, img.cnf, img.version, img.name)

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        img.image,
			ExposedPorts: []string{"8080/tcp"},
			Entrypoint:   []string{"bash", "-c"},
			Cmd:          []string{script},
			Files: []testcontainers.ContainerFile{{
				HostFilePath: buildManager(t), ContainerFilePath: "/usr/local/bin/manager", FileMode: 0o755,
			}},
			WaitingFor: wait.ForHTTP("/readyz").WithPort("8080/tcp").
				WithStatusCodeMatcher(func(status int) bool { return status == http.StatusOK }).
				WithStartupTimeout(5 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		if container != nil {
			if logs, lerr := container.Logs(ctx); lerr == nil {
				out, _ := io.ReadAll(logs)
				t.Logf("%s logs:\n%s", img.name, tail(string(out), 40))
			}
			_ = container.Terminate(ctx)
		}
		t.Fatalf("starting %s: %v", img.name, err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "8080")
	if err != nil {
		t.Fatal(err)
	}
	return &logicalNode{img: img, container: container, baseURL: fmt.Sprintf("http://%s:%d", host, port.Num())}
}

// sql runs statements as root over the socket and returns the output.
func (n *logicalNode) sql(ctx context.Context, t *testing.T, stmts string) string {
	t.Helper()
	client := engine.MustForFlavor(n.img.flavor).Logical().LoadBinary()
	return n.exec(ctx, t, fmt.Sprintf("MYSQL_PWD=rootpass %s -uroot --socket=/tmp/mysql.sock -N -B <<'SQL'\n%s\nSQL", client, stmts))
}

func (n *logicalNode) exec(ctx context.Context, t *testing.T, script string) string {
	t.Helper()
	code, reader, err := n.container.Exec(ctx, []string{"bash", "-c", script}, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("exec on %s: %v", n.img.name, err)
	}
	out, _ := io.ReadAll(reader)
	if code != 0 {
		t.Fatalf("exec on %s exited %d:\n%s", n.img.name, code, out)
	}
	return string(out)
}

func (n *logicalNode) post(ctx context.Context, t *testing.T, path string, body any) *http.Response {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	// Each request carries its own generous deadline (dumps stream for a
	// while), covering the body read that happens after this helper returns:
	// the caller's context alone has no deadline. The cancel is released with
	// the test, since responses are consumed by the callers.
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, n.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	return resp
}

// createDumpAccount does what the operator's dump account reconcile does:
// create cnmsql_dump with the facet's grants through the user API, then set its
// password.
func (n *logicalNode) createDumpAccount(ctx context.Context, t *testing.T, password string) {
	t.Helper()
	v, err := engine.MustForFlavor(n.img.flavor).ParseServerVersion(n.img.version)
	if err != nil {
		t.Fatal(err)
	}
	logical := engine.MustForFlavor(n.img.flavor).Logical()
	privs := func(grants []engine.AccountGrant) []user.Privilege {
		var out []user.Privilege
		for _, g := range grants {
			out = append(out, user.Privilege{Privileges: g.Privileges, On: g.On})
		}
		return out
	}
	for path, body := range map[string]any{
		"/user/create": user.CreateUserRequest{
			Name: engine.DumpAccountName, Host: engine.DumpAccountHost, Password: password, RequireTLS: "none",
			Privileges: privs(logical.DumpAccountGrants(v)), Revokes: privs(logical.DumpAccountRevokes(v)),
		},
	} {
		resp := n.post(ctx, t, path, body)
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s on %s = %d: %s", path, n.img.name, resp.StatusCode, b)
		}
	}
	resp := n.post(ctx, t, "/user/alter", user.AlterUserRequest{
		Name: engine.DumpAccountName, Host: engine.DumpAccountHost, Password: &password,
	})
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/user/alter on %s = %d", n.img.name, resp.StatusCode)
	}
}

const seedSQL = `
CREATE DATABASE shop CHARACTER SET utf8mb4;
CREATE TABLE shop.items (
  id INT PRIMARY KEY,
  name VARCHAR(64) NOT NULL,
  price DECIMAL(10,2) NOT NULL,
  price_with_tax DECIMAL(10,2) AS (price * 1.2) STORED,
  payload BLOB,
  note TEXT
) CHARACTER SET utf8mb4;
INSERT INTO shop.items (id, name, price, payload, note) VALUES
  (1, 'café ☕', 10.00, UNHEX('00FF10E2'), 'line one\nline two'),
  (2, 'emoji 🎉', 3.50, NULL, CONCAT('-- Current Database: ', CHAR(96), 'billing', CHAR(96)));
CREATE VIEW shop.cheap AS SELECT id, name FROM shop.items WHERE price < 5;
CREATE PROCEDURE shop.count_items(OUT n INT) SELECT COUNT(*) INTO n FROM shop.items;
CREATE FUNCTION shop.double_it(x INT) RETURNS INT DETERMINISTIC RETURN x * 2;
CREATE TRIGGER shop.items_bi BEFORE INSERT ON shop.items FOR EACH ROW SET NEW.name = TRIM(NEW.name);
CREATE EVENT shop.nightly ON SCHEDULE EVERY 1 DAY DISABLE DO DELETE FROM shop.items WHERE id < 0;
CREATE DATABASE billing;
CREATE TABLE billing.invoices (id INT PRIMARY KEY, total INT);
INSERT INTO billing.invoices VALUES (1, 100), (2, 250);
`

type dumpResponse struct {
	status    int
	header    http.Header
	trailer   http.Header
	body      string
	errorBody webserver.DumpErrorBody
}

func (n *logicalNode) dump(ctx context.Context, t *testing.T, req webserver.DumpRequest) dumpResponse {
	t.Helper()
	resp := n.post(ctx, t, "/cluster/dump", req)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading dump from %s: %v", n.img.name, err)
	}
	out := dumpResponse{status: resp.StatusCode, header: resp.Header, trailer: resp.Trailer, body: string(body)}
	if resp.StatusCode != http.StatusOK {
		_ = json.Unmarshal(body, &out.errorBody)
	}
	return out
}

// TestLogicalBackupRoundTrip dumps each image through the real POST
// /cluster/dump as cnmsql_dump with the engine's grants, checks the stream,
// then loads it into the next series of the same flavor.
func TestLogicalBackupRoundTrip(t *testing.T) {
	images := logicalImages(t)
	for i, src := range images {
		dst := src
		if i+1 < len(images) && images[i+1].flavor == src.flavor {
			dst = images[i+1]
		}
		t.Run(src.name+"-to-"+dst.name, func(t *testing.T) {
			t.Parallel()
			runLogicalRoundTrip(t, src, dst)
		})
	}
}

func runLogicalRoundTrip(t *testing.T, srcImg, dstImg logicalImage) {
	ctx := context.Background()
	const password = `dump-p"a\ss`
	src := startLogicalNode(ctx, t, srcImg)
	src.sql(ctx, t, seedSQL)

	// Before the account exists, the instance refuses with a retryable 503.
	if r := src.dump(ctx, t, webserver.DumpRequest{Password: password}); r.status != http.StatusServiceUnavailable ||
		r.errorBody.Reason != webserver.DumpReasonAccountMissing {
		t.Fatalf("dump without account = %d %+v", r.status, r.errorBody)
	}
	src.createDumpAccount(ctx, t, password)

	// A wrong password fails before any byte is streamed.
	if r := src.dump(ctx, t, webserver.DumpRequest{Password: "wrong"}); r.status != http.StatusInternalServerError ||
		!strings.Contains(r.errorBody.Error, "Access denied") {
		t.Fatalf("dump with a wrong password = %d %+v", r.status, r.errorBody)
	}
	if r := src.dump(ctx, t, webserver.DumpRequest{Password: password, Databases: []string{"nope"}}); r.status != http.StatusUnprocessableEntity {
		t.Fatalf("dump of a missing database = %d %+v", r.status, r.errorBody)
	}

	full := src.dump(ctx, t, webserver.DumpRequest{Password: password})
	if full.status != http.StatusOK {
		t.Fatalf("dump = %d %+v", full.status, full.errorBody)
	}
	if e := full.trailer.Get(webserver.DumpErrorTrailer); e != "" {
		t.Fatalf("dump failed mid-stream: %s", e)
	}
	logical := engine.MustForFlavor(srcImg.flavor).Logical()
	if tool := full.header.Get(webserver.DumpToolHeader); tool != logical.DumpBinary() {
		t.Errorf("tool = %q", tool)
	}
	dbs, _ := webserver.DecodeDumpDatabases(full.header.Get(webserver.DumpDatabasesHeader))
	if !slices.Equal(dbs, []string{"app", "billing", "shop"}) {
		t.Errorf("databases = %v, want app, billing, shop", dbs)
	}
	if full.trailer.Get(webserver.DumpSnapshotBinlogTrailer) == "" {
		t.Errorf("no snapshot binlog position in trailers %v", full.trailer)
	}
	if srcImg.flavor == engine.FlavorMariaDB && full.trailer.Get(webserver.DumpSnapshotGTIDTrailer) == "" {
		t.Errorf("no snapshot GTID from mariadb-dump in trailers %v", full.trailer)
	}
	for _, want := range []string{"-- Dump completed on", "-- Current Database: `shop`", "-- Current Database: `billing`"} {
		if !strings.Contains(full.body, want) {
			t.Errorf("dump is missing %q", want)
		}
	}
	for _, unwanted := range []string{
		"insufficient privileges", // a missing grant shows up as a comment, not an error
		"Current Database: `mysql`",
		"GTID_PURGED",
		"SQL_LOG_BIN",
	} {
		if strings.Contains(full.body, unwanted) {
			t.Errorf("dump contains %q", unwanted)
		}
	}

	partial := src.dump(ctx, t, webserver.DumpRequest{Password: password, Databases: []string{"billing"}})
	if partial.status != http.StatusOK || strings.Contains(partial.body, "`shop`") ||
		!strings.Contains(partial.body, "invoices") {
		t.Errorf("partial dump = %d, contains shop: %v", partial.status, strings.Contains(partial.body, "`shop`"))
	}

	// Load into a fresh server of the next series, as an import will.
	dst := startLogicalNode(ctx, t, dstImg)
	if err := dst.container.CopyToContainer(ctx, []byte(full.body), "/tmp/dump.sql", 0o644); err != nil {
		t.Fatal(err)
	}
	gtidBefore := dst.gtidState(ctx, t)
	loader := engine.MustForFlavor(dstImg.flavor).Logical().LoadBinary()
	dst.exec(ctx, t, "MYSQL_PWD=rootpass "+loader+" -uroot --socket=/tmp/mysql.sock < /tmp/dump.sql")
	if after := dst.gtidState(ctx, t); after != gtidBefore {
		t.Errorf("loading the dump changed the GTID state from %q to %q", gtidBefore, after)
	}

	checks := map[string]string{
		"SELECT name FROM shop.items WHERE id = 1":                                       "café ☕",
		"SELECT HEX(payload) FROM shop.items WHERE id = 1":                               "00FF10E2",
		"SELECT price_with_tax FROM shop.items WHERE id = 2":                             "4.20",
		"SELECT note FROM shop.items WHERE id = 2":                                       "-- Current Database: `billing`",
		"SELECT name FROM shop.cheap":                                                    "emoji 🎉",
		"SELECT shop.double_it(21)":                                                      "42",
		"CALL shop.count_items(@n); SELECT @n":                                           "2",
		"SELECT SUM(total) FROM billing.invoices":                                        "350",
		"SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = 'shop'": "1",
		"SELECT COUNT(*) FROM information_schema.EVENTS WHERE EVENT_SCHEMA = 'shop'":     "1",
	}
	for query, want := range checks {
		if got := strings.TrimSpace(dst.sql(ctx, t, query+";")); got != want {
			t.Errorf("%s on %s = %q, want %q", query, dstImg.name, got, want)
		}
	}
	// The multi-line value survives as one value.
	if got := dst.sql(ctx, t, "SELECT REPLACE(note, '\\n', '|') FROM shop.items WHERE id = 1;"); strings.TrimSpace(got) != "line one|line two" {
		t.Errorf("multi-line value = %q", got)
	}
}

// gtidState is what a load must leave alone: gtid_purged on MySQL,
// gtid_slave_pos on MariaDB.
func (n *logicalNode) gtidState(ctx context.Context, t *testing.T) string {
	t.Helper()
	if n.img.flavor == engine.FlavorMariaDB {
		return strings.TrimSpace(n.sql(ctx, t, "SELECT @@GLOBAL.gtid_slave_pos;"))
	}
	return strings.TrimSpace(n.sql(ctx, t, "SELECT @@GLOBAL.gtid_purged;"))
}

func tail(s string, lines int) string {
	parts := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(parts) > lines {
		parts = parts[len(parts)-lines:]
	}
	return strings.Join(parts, "\n")
}
