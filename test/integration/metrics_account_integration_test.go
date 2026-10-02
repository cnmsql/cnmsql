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
	"strings"
	"testing"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/user"
)

// TestMetricsAccountGrants drives POST /monitoring/account on every flavor:
// the account is created when missing, declared grants are applied, grants
// made by hand are revoked, base grants survive, and a pass with nothing to
// change writes no binlog event. The node's initdb runs without
// --metrics-user, so the account starts out missing, as on a cluster
// bootstrapped before it existed.
func TestMetricsAccountGrants(t *testing.T) {
	for _, img := range logicalImages(t) {
		t.Run(img.name, func(t *testing.T) {
			t.Parallel()
			runMetricsAccountTest(t, img)
		})
	}
}

func runMetricsAccountTest(t *testing.T, img logicalImage) {
	ctx := context.Background()
	n := startLogicalNode(ctx, t, img, nil)
	n.sql(ctx, t, "CREATE TABLE app.items (id INT PRIMARY KEY); INSERT INTO app.items VALUES (1), (2), (3);")

	declare := func(want int, privileges ...user.Privilege) user.MetricsAccountResponse {
		t.Helper()
		resp := n.post(ctx, t, "/monitoring/account", user.MetricsAccountRequest{Privileges: privileges})
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != want {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("POST /monitoring/account = %d, want %d: %s", resp.StatusCode, want, body)
		}
		var out user.MetricsAccountResponse
		if want == http.StatusOK {
			if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
				t.Fatal(err)
			}
		}
		return out
	}
	grants := func() string {
		return n.sql(ctx, t, "SHOW GRANTS FOR 'cnmsql_metrics'@'localhost';")
	}
	readsItems := func() string {
		client := engine.MustForFlavor(img.flavor).Logical().LoadBinary()
		return strings.TrimSpace(n.exec(ctx, t, client+
			" -ucnmsql_metrics --socket=/tmp/mysql.sock -N -B -e 'SELECT COUNT(*) FROM app.items' 2>&1 || true"))
	}
	gtid := func() string {
		if img.flavor == engine.FlavorMariaDB {
			return strings.TrimSpace(n.sql(ctx, t, "SELECT @@GLOBAL.gtid_binlog_pos;"))
		}
		return strings.TrimSpace(n.sql(ctx, t, "SELECT @@GLOBAL.gtid_executed;"))
	}
	selectApp := user.Privilege{Privileges: []string{"SELECT"}, On: "app.*"}

	// Created when missing, with base and declared grants.
	first := declare(http.StatusOK, selectApp)
	if !first.Created {
		t.Fatalf("expected the account to be created: %+v", first)
	}
	if got := readsItems(); got != "3" {
		t.Fatalf("metrics account could not read app.items: %q\n%s", got, grants())
	}

	// In sync: nothing written, no new GTID.
	before := gtid()
	again := declare(http.StatusOK, selectApp)
	if again.Created || len(again.Granted) != 0 || len(again.Revoked) != 0 {
		t.Fatalf("second pass changed something: %+v\n%s", again, grants())
	}
	if after := gtid(); after != before {
		t.Fatalf("an in-sync pass wrote a binlog event: %q -> %q", before, after)
	}

	// Grants made by hand are revoked; base grants stay.
	n.sql(ctx, t, "GRANT INSERT ON app.* TO 'cnmsql_metrics'@'localhost' WITH GRANT OPTION;"+
		" GRANT SELECT (id) ON app.items TO 'cnmsql_metrics'@'localhost';"+
		" GRANT SELECT ON sys.* TO 'cnmsql_metrics'@'localhost';")
	cleaned := declare(http.StatusOK, selectApp)
	if len(cleaned.Revoked) == 0 {
		t.Fatalf("expected revokes, got %+v", cleaned)
	}
	g := grants()
	for _, gone := range []string{"INSERT", "GRANT OPTION", "`sys`", "(`id`)"} {
		if strings.Contains(g, gone) {
			t.Fatalf("%s survived:\n%s", gone, g)
		}
	}
	for _, kept := range []string{"PROCESS", "REPLICATION SLAVE", "`performance_schema`"} {
		if !strings.Contains(g, kept) {
			t.Fatalf("base grant %s was revoked:\n%s", kept, g)
		}
	}
	if again := declare(http.StatusOK, selectApp); len(again.Granted)+len(again.Revoked) != 0 {
		t.Fatalf("pass after cleanup not idempotent: %+v\n%s", again, grants())
	}

	// Declaring a base grant and then dropping it keeps it.
	declare(http.StatusOK, selectApp, user.Privilege{Privileges: []string{"SELECT"}, On: "performance_schema.*"})
	declare(http.StatusOK)
	if g := grants(); !strings.Contains(g, "`performance_schema`") || strings.Contains(g, "`app`") {
		t.Fatalf("unexpected grants after dropping the list:\n%s", g)
	}

	// A grant that cannot apply yet (its table does not exist) does not hold
	// back the revokes or the other grants.
	n.sql(ctx, t, "GRANT INSERT ON app.* TO 'cnmsql_metrics'@'localhost';")
	declare(http.StatusInternalServerError,
		user.Privilege{Privileges: []string{"SELECT"}, On: "app.nosuch"}, selectApp)
	if g := grants(); strings.Contains(g, "INSERT") || !strings.Contains(g, "`app`.*") {
		t.Fatalf("a failed grant held back the others:\n%s", g)
	}

	// A PROXY grant with its grant option gets a REVOKE the server parses.
	// The control account holds no PROXY privilege, so the server refuses
	// it (access denied, not a syntax error), and the failure is reported
	// without holding back the other statements.
	proxyTarget := "''@''"
	if img.flavor == engine.FlavorMariaDB {
		proxyTarget = "''@'%'"
	}
	n.sql(ctx, t, "GRANT PROXY ON "+proxyTarget+" TO 'cnmsql_metrics'@'localhost' WITH GRANT OPTION;"+
		" GRANT INSERT ON app.* TO 'cnmsql_metrics'@'localhost';")
	resp := n.post(ctx, t, "/monitoring/account", user.MetricsAccountRequest{Privileges: []user.Privilege{selectApp}})
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError || !strings.Contains(string(body), "REVOKE PROXY ON") ||
		strings.Contains(string(body), "1064") {
		t.Fatalf("unexpected PROXY revoke outcome %d: %s", resp.StatusCode, body)
	}
	if g := grants(); strings.Contains(g, "INSERT") {
		t.Fatalf("the PROXY failure held back the INSERT revoke:\n%s", g)
	}
	n.sql(ctx, t, "REVOKE PROXY ON "+proxyTarget+" FROM 'cnmsql_metrics'@'localhost';")

	// The endpoint refuses write privileges and runs nothing.
	declare(http.StatusBadRequest, user.Privilege{Privileges: []string{"INSERT"}, On: "app.*"})
}
