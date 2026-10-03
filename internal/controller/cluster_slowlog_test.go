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

package controller

import (
	"strings"
	"testing"
)

// TestRenderedConfigPinsSlowLogToRunVolume asserts the operator points the slow
// log at the run volume, where the instance manager tails it (design 037).
func TestRenderedConfigPinsSlowLogToRunVolume(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	plan := testPlan()
	out, err := (&ClusterReconciler{}).renderMyCnf(cluster, plan, plan.instanceFor(cluster, 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"slow_query_log_file = /var/run/mysqld/mysqld-slow.log", "log_output = FILE"} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered config lacks %q:\n%s", want, out)
		}
	}
}
