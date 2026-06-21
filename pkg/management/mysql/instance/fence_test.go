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
