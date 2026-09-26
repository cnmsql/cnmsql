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
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// definerSQL gives shop objects whose definer is another account. With binary
// logging on, loading them needs SUPER: this is what rules out a
// least-privilege load account (design 029 §2.1), so every series must show
// that the control account loads them.
const definerSQL = `
CREATE USER 'app'@'%' IDENTIFIED BY 'app-pass';
GRANT ALL ON shop.* TO 'app'@'%';
CREATE DEFINER='app'@'%' VIEW shop.named AS SELECT id, name FROM shop.items;
CREATE DEFINER='app'@'%' TRIGGER shop.items_bu BEFORE UPDATE ON shop.items FOR EACH ROW SET NEW.note = NEW.note;
CREATE DEFINER='app'@'%' FUNCTION shop.triple_it(x INT) RETURNS INT DETERMINISTIC RETURN x * 3;
CREATE DEFINER='app'@'%' EVENT shop.weekly ON SCHEDULE EVERY 1 WEEK DISABLE DO DELETE FROM shop.items WHERE id < 0;
`

type loadResponse struct {
	status  int
	result  webserver.LoadResult
	refusal webserver.DumpErrorBody
}

// load posts body to POST /cluster/load, as the restore worker does.
func (n *logicalNode) load(ctx context.Context, t *testing.T, databases []string, policy, body string) loadResponse {
	t.Helper()
	q := url.Values{webserver.LoadDatabaseParam: databases, webserver.LoadPolicyParam: {policy}}
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, n.baseURL+"/cluster/load?"+q.Encode(),
		strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/sql")
	req.Header.Set("Expect", "100-continue")
	resp, err := (&http.Client{Transport: &http.Transport{ExpectContinueTimeout: time.Minute}}).Do(req)
	if err != nil {
		t.Fatalf("POST /cluster/load on %s: %v", n.img.name, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	out := loadResponse{status: resp.StatusCode}
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(raw, &out.result); err != nil {
			t.Fatalf("load result %q: %v", raw, err)
		}
	} else {
		_ = json.Unmarshal(raw, &out.refusal)
	}
	return out
}

// TestLogicalRestoreIntoRunningInstance loads a dump into a running server
// through the real POST /cluster/load, on every image: the policies, the
// read-only refusal, a partial selection, and a load that goes through the
// binary log.
func TestLogicalRestoreIntoRunningInstance(t *testing.T) {
	for _, img := range logicalImages(t) {
		t.Run(img.name, func(t *testing.T) {
			t.Parallel()
			runLogicalRestore(t, img)
		})
	}
}

func runLogicalRestore(t *testing.T, img logicalImage) {
	ctx := context.Background()
	const password = "dump-pass"
	node := startLogicalNode(ctx, t, img, nil)
	node.sql(ctx, t, seedSQL+definerSQL)
	node.createDumpAccount(ctx, t, password)
	dump := node.dump(ctx, t, webserver.DumpRequest{Password: password})
	if dump.status != http.StatusOK {
		t.Fatalf("dump = %d %+v", dump.status, dump.errorBody)
	}

	// Damage both databases after the dump.
	node.sql(ctx, t, `
DELETE FROM shop.items;
DROP VIEW shop.cheap;
DROP FUNCTION shop.triple_it;
UPDATE billing.invoices SET total = 0;
`)
	// Row events do not show their values, so the binlog check counts the
	// CREATE TABLE, which is logged as a statement.
	beforeRestore := node.binlogMentions(ctx, t, "price_with_tax")

	// FailIfExists refuses a database that holds objects and changes nothing.
	r := node.load(ctx, t, []string{"shop"}, webserver.LoadPolicyFailIfExists, dump.body)
	if r.status != http.StatusConflict || r.refusal.Reason != webserver.LoadReasonDatabaseNotEmpty ||
		!strings.Contains(r.refusal.Error, "shop") {
		t.Fatalf("FailIfExists on a non-empty database = %d %+v", r.status, r.refusal)
	}
	if got := strings.TrimSpace(node.sql(ctx, t, "SELECT COUNT(*) FROM shop.items;")); got != "0" {
		t.Fatalf("a refused load changed shop.items: %s rows", got)
	}

	// A read-only instance (a replica, a demoted primary) refuses the load.
	node.sql(ctx, t, "SET GLOBAL read_only = ON;")
	r = node.load(ctx, t, []string{"shop"}, webserver.LoadPolicyDropAndRecreate, dump.body)
	node.sql(ctx, t, "SET GLOBAL read_only = OFF;")
	if r.status != http.StatusConflict || r.refusal.Reason != webserver.LoadReasonNotPrimary {
		t.Fatalf("load on a read-only instance = %d %+v", r.status, r.refusal)
	}
	if got := strings.TrimSpace(node.sql(ctx, t,
		"SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = 'shop';")); got != "1" {
		t.Fatal("a refused DropAndRecreate dropped shop")
	}

	// A system schema is never loaded.
	if r := node.load(ctx, t, []string{"mysql"}, webserver.LoadPolicyDropAndRecreate, dump.body); r.status != http.StatusUnprocessableEntity {
		t.Fatalf("load of the mysql schema = %d %+v", r.status, r.refusal)
	}

	// DropAndRecreate replaces shop, and only shop, as the control account.
	r = node.load(ctx, t, []string{"shop"}, webserver.LoadPolicyDropAndRecreate, dump.body)
	if r.status != http.StatusOK {
		t.Fatalf("DropAndRecreate = %d %+v", r.status, r.refusal)
	}
	if !slices.Equal(r.result.Databases, []string{"shop"}) || r.result.Bytes != int64(len(dump.body)) {
		t.Errorf("load result = %+v", r.result)
	}
	checks := map[string]string{
		"SELECT name FROM shop.items WHERE id = 1":                                       "café ☕",
		"SELECT HEX(payload) FROM shop.items WHERE id = 1":                               "00FF10E2",
		"SELECT name FROM shop.cheap":                                                    "emoji 🎉",
		"SELECT shop.triple_it(3)":                                                       "9",
		"SELECT COUNT(*) FROM shop.named":                                                "2",
		"SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = 'shop'": "2",
		"SELECT DEFINER FROM information_schema.VIEWS WHERE TABLE_NAME = 'named'":        "app@%",
		"SELECT DEFINER FROM information_schema.EVENTS WHERE EVENT_NAME = 'weekly'":      "app@%",
		// billing was not selected: its damage stays.
		"SELECT SUM(total) FROM billing.invoices": "0",
	}
	for query, want := range checks {
		if got := strings.TrimSpace(node.sql(ctx, t, query+";")); got != want {
			t.Errorf("%s on %s = %q, want %q", query, img.name, got, want)
		}
	}

	// The load went through the binary log, so replicas and the archive
	// follow it (LB17).
	if after := node.binlogMentions(ctx, t, "price_with_tax"); after <= beforeRestore {
		t.Errorf("the restore is not in the binlog: %d mentions before, %d after", beforeRestore, after)
	}

	// With shop empty, FailIfExists loads into it: the Database-CR flow.
	node.sql(ctx, t, "DROP DATABASE billing; CREATE DATABASE billing;")
	r = node.load(ctx, t, []string{"billing"}, webserver.LoadPolicyFailIfExists, dump.body)
	if r.status != http.StatusOK {
		t.Fatalf("FailIfExists into an empty database = %d %+v", r.status, r.refusal)
	}
	if got := strings.TrimSpace(node.sql(ctx, t, "SELECT SUM(total) FROM billing.invoices;")); got != "350" {
		t.Errorf("billing after restore = %s, want 350", got)
	}

	// A broken statement fails the load with the client's error.
	broken := strings.Replace(dump.body, "CREATE TABLE `invoices`", "CREATE TABLEX `invoices`", 1)
	r = node.load(ctx, t, []string{"billing"}, webserver.LoadPolicyDropAndRecreate, broken)
	if r.status != http.StatusInternalServerError || r.refusal.Reason != webserver.LoadReasonFailed ||
		!strings.Contains(r.refusal.Error, "ERROR") {
		t.Errorf("load of a broken dump = %d %+v", r.status, r.refusal)
	}
}
