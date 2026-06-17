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
	"testing"
	"time"
)

func TestFenceGateUnfencedReturnsImmediately(t *testing.T) {
	g := NewFenceGate()
	if g.IsFenced() {
		t.Fatal("a fresh gate must be unfenced")
	}
	if err := g.WaitUntilUnfenced(context.Background()); err != nil {
		t.Fatalf("WaitUntilUnfenced on an unfenced gate: %v", err)
	}
}

func TestFenceGateWaitWakesOnUnfence(t *testing.T) {
	g := NewFenceGate()
	g.Fence()
	if !g.IsFenced() {
		t.Fatal("expected gate to be fenced")
	}

	woke := make(chan struct{})
	go func() {
		_ = g.WaitUntilUnfenced(context.Background())
		close(woke)
	}()

	// The waiter must stay blocked while fenced.
	select {
	case <-woke:
		t.Fatal("WaitUntilUnfenced returned while still fenced")
	case <-time.After(20 * time.Millisecond):
	}

	g.Unfence()
	select {
	case <-woke:
	case <-time.After(time.Second):
		t.Fatal("WaitUntilUnfenced did not wake after Unfence")
	}
	if g.IsFenced() {
		t.Fatal("gate should be unfenced after Unfence")
	}
}

func TestFenceGateWaitRespectsContext(t *testing.T) {
	g := NewFenceGate()
	g.Fence()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := g.WaitUntilUnfenced(ctx); err == nil {
		t.Fatal("expected WaitUntilUnfenced to return the context error")
	}
}

func TestControllerFenceStopsMysqld(t *testing.T) {
	sup := &fakeSupervisor{}
	c, _ := newController(t, sup)
	c.SetFenceGate(NewFenceGate())

	if err := c.Fence(context.Background()); err != nil {
		t.Fatalf("Fence: %v", err)
	}
	if !sup.called {
		t.Error("Fence should stop mysqld via the supervisor")
	}
	if !c.fence.IsFenced() {
		t.Error("Fence should mark the gate fenced")
	}

	if err := c.Unfence(context.Background()); err != nil {
		t.Fatalf("Unfence: %v", err)
	}
	if c.fence.IsFenced() {
		t.Error("Unfence should clear the gate")
	}
}

func TestControllerFenceUnavailableWithoutGate(t *testing.T) {
	sup := &fakeSupervisor{}
	c, _ := newController(t, sup)
	if err := c.Fence(context.Background()); err == nil {
		t.Error("Fence should fail when no gate is configured")
	}
	// Unfence is a tolerant no-op without a gate.
	if err := c.Unfence(context.Background()); err != nil {
		t.Errorf("Unfence without a gate should be a no-op: %v", err)
	}
}
