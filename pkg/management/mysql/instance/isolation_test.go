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

package instance

import (
	"testing"
	"time"
)

func TestIsolationDetectorHealthyAfterContact(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 0)
	d := NewIsolationDetector(30 * time.Second)
	d.now = func() time.Time { return now }
	d.lastContact = now // re-seed against the fake clock

	// Freshly built: seeded as just-contacted, so healthy.
	if err := d.Check(); err != nil {
		t.Fatalf("fresh detector = %v, want healthy", err)
	}

	// Within the timeout: still healthy.
	now = now.Add(29 * time.Second)
	if err := d.Check(); err != nil {
		t.Fatalf("at 29s = %v, want healthy", err)
	}

	// Past the timeout without contact: isolated.
	now = now.Add(2 * time.Second)
	if err := d.Check(); err == nil {
		t.Fatal("at 31s = nil, want isolation error")
	}

	// A fresh contact clears isolation.
	d.RecordContact()
	if err := d.Check(); err != nil {
		t.Fatalf("after RecordContact = %v, want healthy", err)
	}
}

func TestIsolationDetectorNilSafe(t *testing.T) {
	t.Parallel()
	var d *IsolationDetector
	d.RecordContact() // must not panic
	if err := d.Check(); err != nil {
		t.Fatalf("nil detector Check = %v, want nil", err)
	}
}
