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

package join

import "testing"

// TestXtrabackupFlagDefaultsEmpty guards the regression that broke MariaDB
// clones: the flag must default to empty so Join falls back to the engine's
// binary (mariabackup on MariaDB) instead of pinning to xtrabackup.
func TestXtrabackupFlagDefaultsEmpty(t *testing.T) {
	f := NewCommand().Flags().Lookup("xtrabackup")
	if f == nil {
		t.Fatal("flag --xtrabackup not found")
	}
	if f.DefValue != "" {
		t.Errorf("--xtrabackup default = %q, want empty (engine selects the tool)", f.DefValue)
	}
}
