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

package instance

import (
	"testing"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/version"
)

func TestGuardDataDirUpgrade(t *testing.T) {
	eng := engine.MustForFlavor(engine.FlavorMySQL)

	t.Run("allows a fresh data directory with no marker", func(t *testing.T) {
		if err := guardDataDirUpgrade(t.TempDir(), "8.4.3", eng); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("allows an empty data dir", func(t *testing.T) {
		if err := guardDataDirUpgrade("", "8.4.3", eng); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	cases := []struct {
		name    string
		marker  string
		target  string
		wantErr bool
	}{
		{"same series patch bump", "8.0.36", "8.0.40", false},
		{"single hop forward", "8.0.36", "8.4.3", false},
		{"second hop forward", "8.4.3", "9.0.1", false},
		{"second hop to current 9.x", "8.4.3", "9.6.0", false},
		{"skips a series", "8.0.36", "9.0.1", true},
		{"downgrade", "8.4.3", "8.0.36", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := writeVersionMarker(dir, tc.marker); err != nil {
				t.Fatalf("writeVersionMarker: %v", err)
			}
			err := guardDataDirUpgrade(dir, tc.target, eng)
			if tc.wantErr && err == nil {
				t.Errorf("guardDataDirUpgrade(%s -> %s): expected error", tc.marker, tc.target)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("guardDataDirUpgrade(%s -> %s): unexpected error: %v", tc.marker, tc.target, err)
			}
		})
	}
}

func TestNeedsUpgrade(t *testing.T) {
	mustParse := func(s string) version.Version {
		v, err := version.Parse(s)
		if err != nil {
			t.Fatalf("version.Parse(%q): %v", s, err)
		}
		return v
	}

	t.Run("fresh data dir (no marker) needs no upgrade", func(t *testing.T) {
		if needsUpgrade(t.TempDir(), mustParse("11.4.3")) {
			t.Error("fresh data dir should not need an upgrade")
		}
	})

	cases := []struct {
		name    string
		marker  string
		current string
		want    bool
	}{
		{"same version", "11.4.3", "11.4.3", false},
		{"patch bump", "11.4.2", "11.4.3", true},
		{"minor bump", "11.2.0", "11.4.3", true},
		{"major bump", "10.11.6", "11.4.3", true},
		{"downgrade never upgrades", "11.4.3", "11.4.2", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := writeVersionMarker(dir, tc.marker); err != nil {
				t.Fatalf("writeVersionMarker: %v", err)
			}
			if got := needsUpgrade(dir, mustParse(tc.current)); got != tc.want {
				t.Errorf("needsUpgrade(%s, %s) = %v, want %v", tc.marker, tc.current, got, tc.want)
			}
		})
	}
}

func TestVersionMarkerRoundTrip(t *testing.T) {
	dir := t.TempDir()
	if _, ok, err := readVersionMarker(dir); err != nil || ok {
		t.Fatalf("expected no marker, got ok=%v err=%v", ok, err)
	}
	if err := writeVersionMarker(dir, "8.4.3"); err != nil {
		t.Fatalf("writeVersionMarker: %v", err)
	}
	v, ok, err := readVersionMarker(dir)
	if err != nil || !ok {
		t.Fatalf("expected marker, got ok=%v err=%v", ok, err)
	}
	if v.Major != 8 || v.Minor != 4 {
		t.Errorf("marker = %d.%d, want 8.4", v.Major, v.Minor)
	}
}
