/*
Copyright 2026 The CloudNative MySQL Authors.

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
	d.RestoreContact(time.Unix(0, 0))
	if !d.LastContact().IsZero() {
		t.Error("nil detector LastContact = non-zero, want zero")
	}
	if err := d.Check(); err != nil {
		t.Fatalf("nil detector Check = %v, want nil", err)
	}
}

// RestoreContact resumes the isolation clock from a pre-upgrade contact instead
// of the re-exec instant: a primary already most of the way to the timeout must
// trip on schedule (measured from its true last contact), not get a fresh full
// budget just because the manager was swapped.
func TestIsolationDetectorRestoreContactResumesClock(t *testing.T) {
	t.Parallel()
	now := time.Unix(100, 0)
	d := NewIsolationDetector(30 * time.Second)
	d.now = func() time.Time { return now }
	// The previous image last reached the API server 25s ago; the re-exec must not
	// reset that to "now".
	prior := now.Add(-25 * time.Second)
	d.RestoreContact(prior)
	if !d.LastContact().Equal(prior) {
		t.Fatalf("LastContact = %v, want %v", d.LastContact(), prior)
	}

	// 5s later we are at 30s since the true last contact: still healthy.
	now = now.Add(5 * time.Second)
	if err := d.Check(); err != nil {
		t.Fatalf("at 30s since contact = %v, want healthy", err)
	}
	// 1s more and the real timeout is exceeded: isolated, despite the recent swap.
	now = now.Add(1 * time.Second)
	if err := d.Check(); err == nil {
		t.Fatal("at 31s since contact = nil, want isolation error")
	}
}

// A zero time is a no-op so a re-exec without a carried contact keeps the fresh
// "just contacted" seed rather than zeroing the clock into instant isolation.
func TestIsolationDetectorRestoreContactZeroIsNoop(t *testing.T) {
	t.Parallel()
	now := time.Unix(100, 0)
	d := NewIsolationDetector(30 * time.Second)
	d.now = func() time.Time { return now }
	d.lastContact = now // fresh seed against the fake clock

	d.RestoreContact(time.Time{})
	if err := d.Check(); err != nil {
		t.Fatalf("after zero RestoreContact = %v, want healthy", err)
	}
}
