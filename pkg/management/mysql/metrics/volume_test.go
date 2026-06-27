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
)

func TestVolumeCollectorReportsUsage(t *testing.T) {
	t.Parallel()
	c := NewVolumeCollector(t.TempDir())

	// A readable volume emits the three gauges plus a zero scrape_error.
	if got := testutil.CollectAndCount(c); got != 4 {
		t.Fatalf("CollectAndCount = %d, want 4", got)
	}
	if err := testutil.CollectAndCompare(c, strings.NewReader(`
# HELP mysql_instance_data_volume_scrape_error Whether reading the instance data volume usage failed (1 for error, 0 for success).
# TYPE mysql_instance_data_volume_scrape_error gauge
mysql_instance_data_volume_scrape_error 0
`), "mysql_instance_data_volume_scrape_error"); err != nil {
		t.Fatalf("scrape_error mismatch: %v", err)
	}
}

func TestVolumeCollectorReportsErrorOnMissingPath(t *testing.T) {
	t.Parallel()
	c := NewVolumeCollector(t.TempDir() + "/missing")

	// An unreadable volume emits only the scrape_error gauge, set to 1.
	if got := testutil.CollectAndCount(c); got != 1 {
		t.Fatalf("CollectAndCount = %d, want 1", got)
	}
	if err := testutil.CollectAndCompare(c, strings.NewReader(`
# HELP mysql_instance_data_volume_scrape_error Whether reading the instance data volume usage failed (1 for error, 0 for success).
# TYPE mysql_instance_data_volume_scrape_error gauge
mysql_instance_data_volume_scrape_error 1
`), "mysql_instance_data_volume_scrape_error"); err != nil {
		t.Fatalf("scrape_error mismatch: %v", err)
	}
}
