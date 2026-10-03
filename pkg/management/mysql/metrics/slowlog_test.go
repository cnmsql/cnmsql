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

package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/slowlog"
)

func TestSlowLogCollector(t *testing.T) {
	t.Parallel()
	c := NewSlowLogCollector(func() slowlog.Stats {
		return slowlog.Stats{Bytes: 1024, Entries: 7, DroppedBytes: 3, Rotations: 2}
	})
	if err := testutil.CollectAndCompare(c, strings.NewReader(`
# HELP mysql_instance_slow_log_bytes Bytes the slow log files use on the run volume.
# TYPE mysql_instance_slow_log_bytes gauge
mysql_instance_slow_log_bytes 1024
# HELP mysql_instance_slow_log_dropped_bytes_total Slow log bytes deleted or truncated before they were read.
# TYPE mysql_instance_slow_log_dropped_bytes_total counter
mysql_instance_slow_log_dropped_bytes_total 3
# HELP mysql_instance_slow_log_entries_total Slow log entries emitted as log records.
# TYPE mysql_instance_slow_log_entries_total counter
mysql_instance_slow_log_entries_total 7
# HELP mysql_instance_slow_log_rotations_total Slow log rotations completed.
# TYPE mysql_instance_slow_log_rotations_total counter
mysql_instance_slow_log_rotations_total 2
`)); err != nil {
		t.Fatal(err)
	}
}
