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

package initdb

import (
	"slices"
	"testing"
)

// Each --post-init-sql is one value, commas included: a statement may hold a
// comma, and each flag is one statement (mirrors --post-import-sql).
func TestRepeatablePostInitSQLFlagKeepsCommas(t *testing.T) {
	cmd := NewCommand()
	if err := cmd.ParseFlags([]string{
		"--post-init-sql=CREATE TABLE t (a INT, b INT)", "--post-init-sql=SELECT 1",
	}); err != nil {
		t.Fatal(err)
	}
	sql, err := cmd.Flags().GetStringArray("post-init-sql")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(sql, []string{"CREATE TABLE t (a INT, b INT)", "SELECT 1"}) {
		t.Errorf("post-init SQL = %q", sql)
	}
}
