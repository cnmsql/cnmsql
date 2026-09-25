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

package importdump

import (
	"slices"
	"testing"
)

// The SQL client flag must default to empty so the engine picks the flavor's
// client; a "mysql" default would break MariaDB images.
func TestSQLClientFlagDefaultsEmpty(t *testing.T) {
	f := NewCommand().Flags().Lookup("mysql")
	if f == nil || f.DefValue != "" {
		t.Fatalf("--mysql must exist and default to empty, got %+v", f)
	}
}

// Each --database and --post-import-sql is one value, commas included.
func TestRepeatableFlagsKeepCommas(t *testing.T) {
	cmd := NewCommand()
	if err := cmd.ParseFlags([]string{
		"--database=a,b", "--database=c",
		"--post-import-sql=INSERT INTO t VALUES (1, 2)", "--post-import-sql=SELECT 1",
	}); err != nil {
		t.Fatal(err)
	}
	dbs, _ := cmd.Flags().GetStringArray("database")
	sql, _ := cmd.Flags().GetStringArray("post-import-sql")
	if !slices.Equal(dbs, []string{"a,b", "c"}) {
		t.Errorf("databases = %q", dbs)
	}
	if !slices.Equal(sql, []string{"INSERT INTO t VALUES (1, 2)", "SELECT 1"}) {
		t.Errorf("post-import SQL = %q", sql)
	}
}
