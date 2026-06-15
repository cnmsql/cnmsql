/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
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
