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

package diskusage

import "testing"

func TestOfReturnsConsistentSnapshot(t *testing.T) {
	t.Parallel()
	u, err := Of(t.TempDir())
	if err != nil {
		t.Fatalf("Of: %v", err)
	}
	if u.CapacityBytes <= 0 {
		t.Fatalf("CapacityBytes = %d, want > 0", u.CapacityBytes)
	}
	if u.UsedBytes < 0 || u.UsedBytes > u.CapacityBytes {
		t.Fatalf("UsedBytes = %d, want within [0, %d]", u.UsedBytes, u.CapacityBytes)
	}
	// Available is the unprivileged free space, never more than the raw free space.
	if u.AvailableBytes < 0 || u.AvailableBytes > u.CapacityBytes-u.UsedBytes {
		t.Fatalf("AvailableBytes = %d, want within [0, %d]", u.AvailableBytes, u.CapacityBytes-u.UsedBytes)
	}
	if r := u.Ratio(); r < 0 || r > 1 {
		t.Fatalf("Ratio = %v, want within [0,1]", r)
	}
}

func TestOfMissingPathErrors(t *testing.T) {
	t.Parallel()
	if _, err := Of(t.TempDir() + "/does-not-exist"); err == nil {
		t.Fatal("Of(missing path) = nil error, want error")
	}
}

func TestRatioZeroCapacity(t *testing.T) {
	t.Parallel()
	if r := (Usage{}).Ratio(); r != 0 {
		t.Fatalf("Ratio of zero-capacity usage = %v, want 0", r)
	}
}
