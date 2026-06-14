/*
Copyright 2026 The CNMySQL Authors.

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
	if err := d.Check(); err != nil {
		t.Fatalf("nil detector Check = %v, want nil", err)
	}
}
