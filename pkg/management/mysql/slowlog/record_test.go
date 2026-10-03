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

package slowlog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	mysqllog "github.com/percona/go-mysql/log"
)

func parseFixture(t *testing.T, path string) []*mysqllog.Event {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var events []*mysqllog.Event
	if err := parseFrom(f, 0, func(e *mysqllog.Event) bool {
		events = append(events, e)
		return true
	}); err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	return events
}

func record(e *mysqllog.Event) map[string]any {
	kv := keysAndValues(e)
	m := make(map[string]any, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i].(string)] = kv[i+1]
	}
	return m
}

func findRecord(t *testing.T, events []*mysqllog.Event, match func(map[string]any) bool) map[string]any {
	t.Helper()
	for _, e := range events {
		if r := record(e); match(r) {
			return r
		}
	}
	t.Fatal("no record matched")
	return nil
}

func queryIs(q string) func(map[string]any) bool {
	return func(r map[string]any) bool { return r["query"] == q }
}

// TestEveryFixtureEntryBecomesARecord parses the slow logs captured from every
// supported image (hack/slowlog-fixtures.sh) and expects one record per entry.
func TestEveryFixtureEntryBecomesARecord(t *testing.T) {
	t.Parallel()
	paths, err := filepath.Glob("testdata/*/*.log")
	if err != nil || len(paths) != 12 {
		t.Fatalf("fixtures = %v (%v), want 12 files", paths, err)
	}
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		want := strings.Count(string(raw), "# User@Host:")
		if got := len(parseFixture(t, path)); got != want {
			t.Errorf("%s: %d records, want %d", path, got, want)
		}
	}
}

func TestMySQLRecordFields(t *testing.T) {
	t.Parallel()
	for _, series := range []string{"mysql-8.0", "mysql-8.4", "mysql-9.7"} {
		for _, kind := range []string{"plain", "verbose"} {
			path := filepath.Join("testdata", series, kind+".log")
			r := findRecord(t, parseFixture(t, path), queryIs("SELECT * FROM t WHERE v = 'a'"))
			if r["user"] != "root" || r["host"] != "localhost" || r["db"] != "shop" {
				t.Errorf("%s: user/host/db = %v/%v/%v", path, r["user"], r["host"], r["db"])
			}
			ts, ok := r["time"].(string)
			if _, err := time.Parse(time.RFC3339Nano, ts); !ok || err != nil {
				t.Errorf("%s: time = %v, want RFC 3339", path, r["time"])
			}
			if _, ok := r["query_time"].(float64); !ok {
				t.Errorf("%s: query_time = %#v, want float64", path, r["query_time"])
			}
			if r["rows_examined"] != uint64(2) || r["rows_sent"] != uint64(1) {
				t.Errorf("%s: rows_examined/rows_sent = %v/%v", path, r["rows_examined"], r["rows_sent"])
			}
		}
	}
}

func TestMariaDBRecordFields(t *testing.T) {
	t.Parallel()
	for _, series := range []string{"mariadb-10.11", "mariadb-11.4", "mariadb-12.3"} {
		for _, kind := range []string{"plain", "verbose"} {
			path := filepath.Join("testdata", series, kind+".log")
			r := findRecord(t, parseFixture(t, path), queryIs("SELECT * FROM t WHERE v = 'a'"))
			if r["user"] != "root" || r["db"] != "shop" {
				t.Errorf("%s: user/db = %v/%v", path, r["user"], r["db"])
			}
			if _, ok := r["thread_id"].(uint64); !ok {
				t.Errorf("%s: thread_id = %#v, want uint64", path, r["thread_id"])
			}
			if q, _ := r["query"].(string); strings.Contains(q, "explain") {
				t.Errorf("%s: EXPLAIN rows leaked into the query: %q", path, q)
			}
		}
	}
}

func TestAdminCommandRecord(t *testing.T) {
	t.Parallel()
	events := parseFixture(t, filepath.Join("testdata", "mysql-8.4", "plain.log"))
	r := findRecord(t, events, func(r map[string]any) bool { return r["admin"] == true })
	if r["query"] != "Quit" {
		t.Errorf("admin query = %v, want Quit", r["query"])
	}
}

func TestUnparsedExtraMetricsAreLeftOut(t *testing.T) {
	t.Parallel()
	e := mysqllog.NewEvent()
	e.Query = "SELECT 1"
	e.NumberMetrics["Start"] = 0
	e.NumberMetrics["End"] = 0
	e.NumberMetrics["Errno"] = 0
	attrs, _ := record(e)["attributes"].(map[string]any)
	if _, ok := attrs["Start"]; ok {
		t.Error("attributes carry Start, which the parser cannot read")
	}
	if _, ok := attrs["Errno"]; !ok {
		t.Error("attributes lost Errno")
	}
}

func TestCapQuery(t *testing.T) {
	t.Parallel()
	q, truncated := capQuery("SELECT 1")
	if q != "SELECT 1" || truncated {
		t.Errorf("short query changed: %q %v", q, truncated)
	}
	long := strings.Repeat("é", MaxQueryBytes) // 2 bytes per rune
	q, truncated = capQuery(long)
	if !truncated || len(q) > MaxQueryBytes || !utf8.ValidString(q) {
		t.Errorf("capQuery: len=%d truncated=%v valid=%v", len(q), truncated, utf8.ValidString(q))
	}
	e := mysqllog.NewEvent()
	e.Query = long
	if r := record(e); r["query_truncated"] != true {
		t.Error("record lacks query_truncated for a capped query")
	}
}

// TestRedactSecrets covers the statements that carry a password. MySQL
// rewrites them before logging; MariaDB logs them verbatim, including the
// ones the operator sends for managed roles and replication.
func TestRedactSecrets(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"CREATE USER 'a'@'%' IDENTIFIED BY 's3cr''et'":                                     "CREATE USER 'a'@'%' IDENTIFIED BY <secret>",
		`ALTER USER 'a'@'%' IDENTIFIED BY "p\"w"`:                                          "ALTER USER 'a'@'%' IDENTIFIED BY <secret>",
		"create user a identified with caching_sha2_password by 'pw'":                      "create user a identified with caching_sha2_password by <secret>",
		"CREATE USER a IDENTIFIED BY PASSWORD '*94BDCEBE19083CE2A1F959FD02F964C7AF4CFC29'": "CREATE USER a IDENTIFIED BY PASSWORD <secret>",
		"CREATE USER a IDENTIFIED VIA ed25519 USING PASSWORD('pw')":                        "CREATE USER a IDENTIFIED VIA ed25519 USING PASSWORD(<secret>)",
		"CREATE USER a IDENTIFIED VIA mysql_native_password USING 'hash'":                  "CREATE USER a IDENTIFIED VIA mysql_native_password USING <secret>",
		"SET PASSWORD FOR 'a'@'%' = PASSWORD('pw')":                                        "SET PASSWORD FOR 'a'@'%' = PASSWORD(<secret>)",
		"SET PASSWORD = 'pw'": "SET PASSWORD = <secret>",
		"CHANGE MASTER TO MASTER_HOST='h', MASTER_PASSWORD='pw', MASTER_PORT=3306": "CHANGE MASTER TO MASTER_HOST='h', MASTER_PASSWORD=<secret>, MASTER_PORT=3306",
		"CHANGE REPLICATION SOURCE TO SOURCE_PASSWORD = 'pw'":                      "CHANGE REPLICATION SOURCE TO SOURCE_PASSWORD = <secret>",
		"START REPLICA USER='r' PASSWORD='pw'":                                     "START REPLICA USER='r' PASSWORD=<secret>",
		"CREATE USER 'a'@'%' IDENTIFIED BY <secret>":                               "CREATE USER 'a'@'%' IDENTIFIED BY <secret>",
		"SELECT id FROM t WHERE password = 'plain'":                                "SELECT id FROM t WHERE password = <secret>",
		"SELECT 'IDENTIFIED', name FROM t WHERE id = 'x'":                          "SELECT 'IDENTIFIED', name FROM t WHERE id = 'x'",
	} {
		if got := redactSecrets(in); got != want {
			t.Errorf("redactSecrets(%q)\n got %q\nwant %q", in, got, want)
		}
	}
	e := mysqllog.NewEvent()
	e.Query = "CREATE USER a IDENTIFIED BY 'pw'"
	if q := record(e)["query"]; q != "CREATE USER a IDENTIFIED BY <secret>" {
		t.Errorf("record query = %q, want the password redacted", q)
	}
}
