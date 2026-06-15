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
