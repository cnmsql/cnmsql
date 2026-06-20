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
	"fmt"
	"sync"
	"time"
)

const (
	// DefaultIsolationTimeout is how long the instance tolerates losing contact
	// with the Kubernetes API server before declaring itself isolated. A
	// partitioned instance cannot be coordinated by the operator (and a
	// partitioned primary is a split-brain risk), so once this elapses the
	// liveness probe fails and the kubelet restarts the container.
	DefaultIsolationTimeout = 30 * time.Second
	// DefaultIsolationProbeInterval is how often the API-server reachability
	// prober runs. It must be comfortably below DefaultIsolationTimeout so a
	// single missed probe does not trip isolation.
	DefaultIsolationProbeInterval = 5 * time.Second
)

// IsolationDetector tracks the last time the instance successfully reached the
// Kubernetes API server. When that contact goes stale beyond the configured
// timeout the instance considers itself network-isolated. It is safe for
// concurrent use and nil-safe: a nil detector never reports isolation, so
// non-cluster-managed instances (dev, bootstrap servers) are unaffected.
type IsolationDetector struct {
	timeout time.Duration
	now     func() time.Time

	mu          sync.Mutex
	lastContact time.Time
}

// NewIsolationDetector returns a detector seeded as "just contacted" so a
// freshly started instance is not immediately considered isolated before the
// first probe runs.
func NewIsolationDetector(timeout time.Duration) *IsolationDetector {
	if timeout <= 0 {
		timeout = DefaultIsolationTimeout
	}
	d := &IsolationDetector{timeout: timeout, now: time.Now}
	d.lastContact = d.now()
	return d
}

// RecordContact marks the API server as reachable right now.
func (d *IsolationDetector) RecordContact() {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastContact = d.now()
}

// LastContact returns the time of the most recent successful API-server contact.
// It is carried across an in-place manager re-exec so the replacement image
// resumes the isolation clock instead of resetting it; a nil detector returns
// the zero time.
func (d *IsolationDetector) LastContact() time.Time {
	if d == nil {
		return time.Time{}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lastContact
}

// RestoreContact seeds the last-contact time from a value carried across a
// manager re-exec, so an in-place upgrade does not reset the isolation clock to
// "now": a primary that was in steady API-server contact stays non-isolated
// across the swap, while a genuinely partitioned one still trips at the real
// timeout measured from its last true contact. A zero time or nil detector is a
// no-op, leaving the fresh "just contacted" seed in place.
func (d *IsolationDetector) RestoreContact(t time.Time) {
	if d == nil || t.IsZero() {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastContact = t
}

// Check returns a non-nil error when the API server has been unreachable for
// longer than the configured timeout. A nil detector is always healthy.
func (d *IsolationDetector) Check() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if since := d.now().Sub(d.lastContact); since > d.timeout {
		return fmt.Errorf("isolated from Kubernetes API server for %s (> %s)",
			since.Truncate(time.Second), d.timeout)
	}
	return nil
}
