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
