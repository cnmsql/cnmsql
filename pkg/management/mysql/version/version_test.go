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

package version

import "testing"

func TestParse(t *testing.T) {
	cases := []struct {
		in                  string
		major, minor, patch int
		wantErr             bool
	}{
		{"8.0.36", 8, 0, 36, false},
		{"5.7.44-48", 5, 7, 44, false},
		{"8.4", 8, 4, 0, false},
		{"v9.0.1", 9, 0, 1, false},
		{"8.0.23", 8, 0, 23, false},
		{"26.7.0", 26, 7, 0, false},   // calendar version YY.M.P
		{"26.10.1", 26, 10, 1, false}, // two-digit month is a whole minor, not 26.1
		{"26.7-1", 26, 7, 0, false},   // image tag: build suffix dropped, series kept
		{"", 0, 0, 0, true},
		{"abc", 0, 0, 0, true},
		{"8.x.1", 0, 0, 0, true},
	}
	for _, tc := range cases {
		v, err := Parse(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("Parse(%q): expected error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("Parse(%q): unexpected error %v", tc.in, err)
			continue
		}
		if v.Major != tc.major || v.Minor != tc.minor || v.Patch != tc.patch {
			t.Errorf("Parse(%q) = %d.%d.%d, want %d.%d.%d",
				tc.in, v.Major, v.Minor, v.Patch, tc.major, tc.minor, tc.patch)
		}
	}
}

func TestAtLeast(t *testing.T) {
	v := Version{Major: 8, Minor: 0, Patch: 23}
	cases := []struct {
		major, minor, patch int
		want                bool
	}{
		{5, 7, 8, true},
		{8, 0, 22, true},
		{8, 0, 23, true},
		{8, 0, 24, false},
		{8, 4, 0, false},
		{9, 0, 0, false},
	}
	for _, tc := range cases {
		if got := v.AtLeast(tc.major, tc.minor, tc.patch); got != tc.want {
			t.Errorf("8.0.23.AtLeast(%d.%d.%d) = %v, want %v",
				tc.major, tc.minor, tc.patch, got, tc.want)
		}
	}
}

func TestFeatureGates(t *testing.T) {
	cases := []struct {
		ver          string
		replicaTerms bool
		superRO      bool
		logReplica   bool
	}{
		{"5.7.7", false, false, false},
		{"5.7.8", false, true, false},
		{"5.7.44", false, true, false},
		{"8.0.22", false, true, true},
		{"8.0.23", true, true, true},
		{"8.4.0", true, true, true},
		{"9.0.1", true, true, true},
	}
	for _, tc := range cases {
		v, err := Parse(tc.ver)
		if err != nil {
			t.Fatalf("Parse(%q): %v", tc.ver, err)
		}
		if got := v.UsesReplicaTerminology(); got != tc.replicaTerms {
			t.Errorf("%s UsesReplicaTerminology = %v, want %v", tc.ver, got, tc.replicaTerms)
		}
		if got := v.HasSuperReadOnly(); got != tc.superRO {
			t.Errorf("%s HasSuperReadOnly = %v, want %v", tc.ver, got, tc.superRO)
		}
		if got := v.HasLogReplicaUpdates(); got != tc.logReplica {
			t.Errorf("%s HasLogReplicaUpdates = %v, want %v", tc.ver, got, tc.logReplica)
		}
	}
}

func TestCheckUpgrade(t *testing.T) {
	mustParse := func(s string) Version {
		v, err := Parse(s)
		if err != nil {
			t.Fatalf("Parse(%q): %v", s, err)
		}
		return v
	}
	cases := []struct {
		from, to string
		wantErr  bool
	}{
		{"8.0.36", "8.0.40", false}, // patch bump within a series
		{"8.0.36", "8.4.3", false},  // single hop forward
		{"8.4.3", "9.7.1", false},   // single hop to the 9.7 LTS
		{"8.4.3", "9.6.0", true},    // 9.x innovation is no longer a supported target
		{"9.6.0", "9.7.1", true},    // hard cut: legacy 9.x innovation cannot upgrade in place
		{"9.0.1", "9.7.1", true},    // hard cut: legacy 9.x innovation cannot upgrade in place
		{"8.0.36", "9.7.1", true},   // skips 8.4
		{"8.0.36", "9.0.1", true},   // 9.x innovation is not a supported target
		{"9.7.1", "9.6.0", true},    // downgrade / unsupported target
		{"9.0.1", "8.4.3", true},    // unsupported source (legacy innovation)
		{"8.4.3", "8.0.36", true},   // downgrade
		{"5.7.44", "8.0.36", true},  // source series outside chain
		{"8.4.3", "10.0.0", true},   // target series outside chain
		// Calendar versioning: 26.7 is an Innovation release, so it is not in the
		// chain (D20) and no 9.7 -> 26.7 hop is offered.
		{"9.7.1", "26.7.0", true},
		{"26.7.0", "26.7.1", false}, // patch bump within a calendar series is a no-op
	}
	for _, tc := range cases {
		err := CheckUpgrade(mustParse(tc.from), mustParse(tc.to))
		if tc.wantErr && err == nil {
			t.Errorf("CheckUpgrade(%s -> %s): expected error", tc.from, tc.to)
		}
		if !tc.wantErr && err != nil {
			t.Errorf("CheckUpgrade(%s -> %s): unexpected error: %v", tc.from, tc.to, err)
		}
	}
}

func TestCalendarSeriesOrdering(t *testing.T) {
	// YY.M compares month numerically: 26.10 is after 26.4, and every calendar
	// release is past every 8.x / 9.x feature gate.
	oct := Version{Major: 26, Minor: 10}
	if !oct.AtLeast(26, 4, 0) {
		t.Error("26.10 should be at least 26.4")
	}
	if !oct.AtLeast(9, 7, 0) || !oct.HasAdminInterface() || !oct.UsesResetBinaryLogsAndGtids() {
		t.Error("26.10 should pass the 8.x / 9.x feature gates")
	}
	if got := (Version{Major: 26, Minor: 10, Patch: 3}).Series(); got != oct {
		t.Errorf("Series() = %v, want 26.10", got)
	}
}
