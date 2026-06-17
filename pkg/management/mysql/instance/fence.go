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
	"context"
	"sync"
)

// FenceGate coordinates the intentional stop of mysqld while the instance
// manager (PID 1) stays alive. When an instance is fenced the operator wants
// mysqld stopped so it can inspect or maintain the data, but the Pod must keep
// running and answering its control and liveness APIs (matching CloudNativePG,
// whose liveness probe does not depend on the database being up).
//
// The mysqld supervisor goroutine in the run loop consults the gate to tell an
// intentional fence-stop, which it must not treat as fatal, from a crash, which
// it must. On unfence the gate wakes the supervisor so it restarts mysqld.
type FenceGate struct {
	mu      sync.Mutex
	fenced  bool
	waiters chan struct{} // closed and replaced on each unfence to wake waiters
}

// NewFenceGate returns an unfenced gate.
func NewFenceGate() *FenceGate {
	return &FenceGate{waiters: make(chan struct{})}
}

// Fence marks the instance as intentionally stopped.
func (g *FenceGate) Fence() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.fenced = true
}

// Unfence clears the intentional stop and wakes any waiter so the supervisor
// can restart mysqld.
func (g *FenceGate) Unfence() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.fenced {
		return
	}
	g.fenced = false
	close(g.waiters)
	g.waiters = make(chan struct{})
}

// IsFenced reports the current intentional-stop state.
func (g *FenceGate) IsFenced() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.fenced
}

// WaitUntilUnfenced blocks until the gate is unfenced or the context is done. It
// returns the context error if the wait was cut short by cancellation (run-loop
// teardown), nil once unfenced.
func (g *FenceGate) WaitUntilUnfenced(ctx context.Context) error {
	for {
		g.mu.Lock()
		if !g.fenced {
			g.mu.Unlock()
			return nil
		}
		ch := g.waiters
		g.mu.Unlock()

		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
