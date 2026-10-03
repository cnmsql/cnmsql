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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cnmsql/cnmsql/pkg/engine"
)

// TestSlowLogReachesManagerOutput runs the instance manager on every MySQL and
// MariaDB image with the slow log on and a tiny rotation threshold. Every slow
// query must reach the manager's output as one record across rotations, and
// rotating must not touch the GTID state (design 037).
func TestSlowLogReachesManagerOutput(t *testing.T) {
	for _, img := range logicalImages(t) {
		t.Run(img.name, func(t *testing.T) {
			t.Parallel()
			runSlowLogTest(t, img)
		})
	}
}

const (
	slowLogCnf = `slow_query_log=ON
long_query_time=0
slow_query_log_file=/tmp/mysqld-slow.log
log_output=FILE
`
	slowLogQueries = 300
	slowLogSecret  = "slowlog-secret-pw"
)

func runSlowLogTest(t *testing.T, img logicalImage) {
	ctx := context.Background()
	img.cnf += slowLogCnf
	img.runArgs = " --slow-log-rotate-bytes=16384"
	n := startLogicalNode(ctx, t, img, nil)

	n.sql(ctx, t, fmt.Sprintf("CREATE USER 'pwcheck'@'localhost' IDENTIFIED BY '%s';", slowLogSecret))
	before := n.gtidExecuted(ctx, t)

	var b strings.Builder
	for i := range slowLogQueries {
		fmt.Fprintf(&b, "SELECT 'slowlog-it-%03d', REPEAT('x', 200);\n", i)
	}
	n.sql(ctx, t, b.String())

	var records []map[string]any
	deadline := time.Now().Add(2 * time.Minute)
	for {
		records = n.slowQueryRecords(ctx, t)
		missing := missingSlowLogMarkers(records)
		if len(missing) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d slow queries never reached the manager output, e.g. %v", len(missing), missing[:min(5, len(missing))])
		}
		time.Sleep(2 * time.Second)
	}

	if after := n.gtidExecuted(ctx, t); after != before {
		t.Errorf("GTID state changed across slow log rotations: %q -> %q", before, after)
	}
	metrics := n.exec(ctx, t, `exec 3<>/dev/tcp/127.0.0.1/9187; printf 'GET /metrics HTTP/1.0\r\n\r\n' >&3; cat <&3`)
	if v := metricValue(metrics, "mysql_instance_slow_log_rotations_total"); v < 1 {
		t.Errorf("rotations_total = %v, want at least 1", v)
	}
	if v := metricValue(metrics, "mysql_instance_slow_log_dropped_bytes_total"); v != 0 {
		t.Errorf("dropped_bytes_total = %v, want 0", v)
	}
	for _, r := range records {
		q, _ := r["query"].(string)
		if strings.Contains(q, "CREATE USER") && strings.Contains(q, slowLogSecret) {
			t.Errorf("%s: the slow log carries a CREATE USER password: %q", img.name, q)
		}
	}
}

// slowQueryRecords returns every "Slow query" record in the container output.
func (n *logicalNode) slowQueryRecords(ctx context.Context, t *testing.T) []map[string]any {
	t.Helper()
	logs, err := n.container.Logs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = logs.Close() }()
	raw, _ := io.ReadAll(logs)
	var out []map[string]any
	sc := bufio.NewScanner(strings.NewReader(string(raw)))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if i := strings.IndexByte(line, '{'); i >= 0 {
			line = line[i:]
		}
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) == nil && m["msg"] == "Slow query" {
			out = append(out, m)
		}
	}
	return out
}

func missingSlowLogMarkers(records []map[string]any) []string {
	seen := map[string]bool{}
	for _, r := range records {
		if q, ok := r["query"].(string); ok {
			seen[q] = true
		}
	}
	var missing []string
	for i := range slowLogQueries {
		marker := fmt.Sprintf("slowlog-it-%03d", i)
		found := false
		for q := range seen {
			if strings.Contains(q, marker) {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, marker)
		}
	}
	return missing
}

func (n *logicalNode) gtidExecuted(ctx context.Context, t *testing.T) string {
	t.Helper()
	if n.img.flavor == engine.FlavorMariaDB {
		return strings.TrimSpace(n.sql(ctx, t, "SELECT @@GLOBAL.gtid_binlog_pos;"))
	}
	return strings.TrimSpace(n.sql(ctx, t, "SELECT @@GLOBAL.gtid_executed;"))
}

func metricValue(exposition, name string) float64 {
	for line := range strings.SplitSeq(exposition, "\n") {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), name+" "); ok {
			v, _ := strconv.ParseFloat(strings.TrimSpace(rest), 64)
			return v
		}
	}
	return -1
}
