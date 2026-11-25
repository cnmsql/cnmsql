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
	"errors"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
)

func TestSemiSyncFenceDecision(t *testing.T) {
	const after = 30 * time.Second
	cases := []struct {
		name                          string
		in                            semiSyncFenceInput
		wantSet, wantClear, wantLatch bool
		wantProbe                     bool
	}{
		{name: "fence off, not engaged: nothing",
			in: semiSyncFenceInput{active: false, inactiveFor: time.Hour}},
		{name: "fence switched off while engaged: lift it",
			in:        semiSyncFenceInput{engaged: true},
			wantClear: true},
		{name: "active: nothing",
			in: semiSyncFenceInput{configured: true, after: after, active: true}},
		{name: "inactive below the delay: wait",
			in: semiSyncFenceInput{configured: true, after: after, inactiveFor: after - time.Second}},
		{name: "inactive past the delay: engage",
			in:      semiSyncFenceInput{configured: true, after: after, inactiveFor: after},
			wantSet: true, wantLatch: true},
		{name: "inactive past the delay, already read-only: latch without touching",
			in:        semiSyncFenceInput{configured: true, after: after, inactiveFor: after, readOnlyAlready: true},
			wantLatch: true},
		{name: "engaged and still inactive: hold",
			in:        semiSyncFenceInput{configured: true, after: after, engaged: true, inactiveFor: time.Hour},
			wantLatch: true},
		{name: "engaged and active again: release",
			in:        semiSyncFenceInput{configured: true, after: after, engaged: true, active: true},
			wantClear: true},
		{name: "engaged, replicas reconnected: probe release",
			in: semiSyncFenceInput{configured: true, after: after, engaged: true,
				enoughClients: true, reconnected: true},
			wantClear: true, wantProbe: true},
		{name: "engaged, replicas connected all along: hold for the flag",
			in: semiSyncFenceInput{configured: true, after: after, engaged: true,
				enoughClients: true, reconnected: false},
			wantLatch: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			set, clear, latch, probe := semiSyncFenceDecision(tc.in)
			if set != tc.wantSet || clear != tc.wantClear || latch != tc.wantLatch || probe != tc.wantProbe {
				t.Fatalf("got set=%v clear=%v latch=%v probe=%v, want set=%v clear=%v latch=%v probe=%v",
					set, clear, latch, probe, tc.wantSet, tc.wantClear, tc.wantLatch, tc.wantProbe)
			}
		})
	}
}

// fakeSemiSyncServer is the server and cluster state a monitor test drives.
type fakeSemiSyncServer struct {
	now       time.Time
	live      replication.SemiSyncSourceLive
	liveErr   error
	readOnly  bool
	writable  bool
	settings  *semiSyncSettings
	other     bool
	sample    *semiSyncSample
	engaged   bool
	waitNoSet int
	applied   []bool
}

func newFakeMonitor(f *fakeSemiSyncServer) *SemiSyncMonitor {
	return &SemiSyncMonitor{
		log: logr.Discard(),
		now: func() time.Time { return f.now },
		live: func(context.Context) (replication.SemiSyncSourceLive, error) {
			return f.live, f.liveErr
		},
		setWaitNoReplica: func(context.Context) error {
			f.waitNoSet++
			f.live.WaitNoReplica = true
			return nil
		},
		liveReadOnly: func(context.Context) (readOnlyState, error) {
			return readOnlyState{ReadOnly: f.readOnly, SuperReadOnly: f.readOnly}, nil
		},
		applyReadOnly: func(_ context.Context, on bool) error {
			f.applied = append(f.applied, on)
			f.readOnly = on
			return nil
		},
		writable: func(context.Context) bool { return f.writable },
		settings: func() (semiSyncSettings, bool) {
			if f.settings == nil {
				return semiSyncSettings{}, false
			}
			return *f.settings, true
		},
		otherHolds:       func() bool { return f.other },
		publish:          func(s *semiSyncSample) { f.sample = s },
		report:           func(engaged bool) { f.engaged = engaged },
		hasSuperReadOnly: true,
	}
}

func degradedPrimary(settings semiSyncSettings) *fakeSemiSyncServer {
	return &fakeSemiSyncServer{
		now: time.Unix(1_000_000, 0),
		live: replication.SemiSyncSourceLive{
			Observed: true, Active: true, Clients: 2, WaitForReplicaCount: 1, WaitNoReplica: true,
		},
		writable: true,
		settings: &settings,
	}
}

func TestSemiSyncMonitorTracksTheInactiveWindow(t *testing.T) {
	f := degradedPrimary(semiSyncSettings{})
	m := newFakeMonitor(f)
	ctx := context.Background()

	m.Tick(ctx)
	if f.sample == nil || !f.sample.Active || !f.sample.InactiveSince.IsZero() {
		t.Fatalf("active primary: sample = %+v", f.sample)
	}

	start := f.now
	f.live.Active, f.live.Clients = false, 0
	m.Tick(ctx)
	if f.sample == nil || f.sample.Active || !f.sample.InactiveSince.Equal(start) {
		t.Fatalf("fallback: sample = %+v, want inactive since %v", f.sample, start)
	}

	// The window start is kept across passes, not reset each tick.
	f.now = f.now.Add(10 * time.Second)
	m.Tick(ctx)
	if !f.sample.InactiveSince.Equal(start) {
		t.Fatalf("window start moved to %v", f.sample.InactiveSince)
	}

	f.live.Active, f.live.Clients = true, 1
	m.Tick(ctx)
	if !f.sample.Active || !f.sample.InactiveSince.IsZero() {
		t.Fatalf("resumed: sample = %+v", f.sample)
	}
	if len(f.applied) != 0 {
		t.Fatalf("no fence configured, but read-only was switched: %v", f.applied)
	}
}

func TestSemiSyncMonitorPublishesNothingOffThePrimary(t *testing.T) {
	f := degradedPrimary(semiSyncSettings{})
	f.writable = false
	f.live.Active = false
	m := newFakeMonitor(f)
	m.Tick(context.Background())
	if f.sample != nil {
		t.Fatalf("replica published a sample: %+v", f.sample)
	}
}

func TestSemiSyncMonitorReassertsWaitNoReplicaInRequiredMode(t *testing.T) {
	f := degradedPrimary(semiSyncSettings{required: true})
	f.writable = false // every instance, not only the primary
	f.live.WaitNoReplica = false
	m := newFakeMonitor(f)
	m.Tick(context.Background())
	if f.waitNoSet != 1 {
		t.Fatalf("wait-no-replica set %d times, want 1", f.waitNoSet)
	}
	m.Tick(context.Background())
	if f.waitNoSet != 1 {
		t.Fatalf("wait-no-replica re-set while already ON")
	}

	p := degradedPrimary(semiSyncSettings{required: false})
	p.live.WaitNoReplica = false
	newFakeMonitor(p).Tick(context.Background())
	if p.waitNoSet != 0 {
		t.Fatalf("preferred mode touched wait-no-replica")
	}
}

func TestSemiSyncMonitorFencesAndReleasesOnTheFlag(t *testing.T) {
	f := degradedPrimary(semiSyncSettings{required: true, readOnlyAfter: 30 * time.Second})
	m := newFakeMonitor(f)
	ctx := context.Background()

	f.live.Active, f.live.Clients = false, 1 // connected but not acknowledging
	m.Tick(ctx)
	f.now = f.now.Add(29 * time.Second)
	m.Tick(ctx)
	if f.engaged || len(f.applied) != 0 {
		t.Fatalf("fenced before the delay: engaged=%v applied=%v", f.engaged, f.applied)
	}
	f.now = f.now.Add(time.Second)
	m.Tick(ctx)
	if !f.engaged || !f.readOnly {
		t.Fatalf("not fenced after the delay: engaged=%v readOnly=%v", f.engaged, f.readOnly)
	}

	// Replicas were connected throughout: no reconnect, so hold for the flag.
	f.now = f.now.Add(time.Minute)
	m.Tick(ctx)
	if !f.engaged || !f.readOnly {
		t.Fatalf("released without the flag or a reconnect")
	}

	f.live.Active = true
	m.Tick(ctx)
	if f.engaged || f.readOnly {
		t.Fatalf("not released after the flag came back: engaged=%v readOnly=%v", f.engaged, f.readOnly)
	}
}

func TestSemiSyncMonitorProbesOnReconnectAndRearmsTheDelay(t *testing.T) {
	f := degradedPrimary(semiSyncSettings{required: true, readOnlyAfter: 30 * time.Second})
	m := newFakeMonitor(f)
	ctx := context.Background()

	f.live.Active, f.live.Clients = false, 0
	m.Tick(ctx)
	f.now = f.now.Add(30 * time.Second)
	m.Tick(ctx)
	if !f.engaged {
		t.Fatalf("not fenced")
	}

	// A replica reconnects already caught up: the flag stays OFF because the
	// read-only primary commits nothing, so the reconnect releases the fence.
	f.live.Clients = 1
	f.now = f.now.Add(2 * time.Second)
	m.Tick(ctx)
	if f.engaged || f.readOnly {
		t.Fatalf("reconnect did not release the fence")
	}

	// The probe commit timed out: the delay restarts from the release instead
	// of re-fencing on the next tick, and the same connection does not
	// release twice.
	f.now = f.now.Add(2 * time.Second)
	m.Tick(ctx)
	if f.engaged {
		t.Fatalf("re-fenced right after the probe release")
	}
	f.now = f.now.Add(28 * time.Second)
	m.Tick(ctx)
	if !f.engaged || !f.readOnly {
		t.Fatalf("not re-fenced a full delay after the probe release")
	}
	f.now = f.now.Add(10 * time.Second)
	m.Tick(ctx)
	if !f.engaged {
		t.Fatalf("released again without a new reconnect")
	}
}

func TestSemiSyncMonitorLeavesTheFlagsToStorageProtection(t *testing.T) {
	f := degradedPrimary(semiSyncSettings{required: true, readOnlyAfter: time.Second})
	m := newFakeMonitor(f)
	ctx := context.Background()

	f.live.Active = false
	m.Tick(ctx)
	f.now = f.now.Add(time.Second)
	m.Tick(ctx)
	if !f.engaged {
		t.Fatalf("not fenced")
	}
	f.other = true
	f.live.Active = true
	m.Tick(ctx)
	if f.engaged {
		t.Fatalf("latch not dropped")
	}
	if !f.readOnly {
		t.Fatalf("cleared read-only while storage protection holds it")
	}
}

func TestSemiSyncMonitorDropsTheFenceOnDemotion(t *testing.T) {
	f := degradedPrimary(semiSyncSettings{required: true, readOnlyAfter: time.Second})
	m := newFakeMonitor(f)
	ctx := context.Background()

	f.live.Active = false
	m.Tick(ctx)
	f.now = f.now.Add(time.Second)
	m.Tick(ctx)
	f.writable = false
	m.Tick(ctx)
	if f.engaged {
		t.Fatalf("latch kept after demotion")
	}
	if len(f.applied) != 1 || !f.readOnly {
		t.Fatalf("demotion owns the flags, but the monitor touched them: %v", f.applied)
	}
}

func TestSemiSyncMonitorLiftsTheFenceWhenSwitchedOff(t *testing.T) {
	f := degradedPrimary(semiSyncSettings{required: true, readOnlyAfter: time.Second})
	m := newFakeMonitor(f)
	ctx := context.Background()

	f.live.Active = false
	m.Tick(ctx)
	f.now = f.now.Add(time.Second)
	m.Tick(ctx)
	f.settings = &semiSyncSettings{required: true}
	m.Tick(ctx)
	if f.engaged || f.readOnly {
		t.Fatalf("fence kept after it was switched off: engaged=%v readOnly=%v", f.engaged, f.readOnly)
	}
}

func TestSemiSyncMonitorDoesNothingOnAReadFailure(t *testing.T) {
	f := degradedPrimary(semiSyncSettings{required: true, readOnlyAfter: time.Second})
	f.liveErr = errors.New("mysqld is restarting")
	m := newFakeMonitor(f)
	m.engaged = true
	m.Tick(context.Background())
	if !m.engaged || len(f.applied) != 0 || f.sample != nil {
		t.Fatalf("acted on a failed read: engaged=%v applied=%v", m.engaged, f.applied)
	}
}

func TestSemiSyncMonitorWaitsForTheClusterBeforeFencing(t *testing.T) {
	f := degradedPrimary(semiSyncSettings{})
	f.settings = nil
	f.live.Active = false
	m := newFakeMonitor(f)
	m.Tick(context.Background())
	f.now = f.now.Add(time.Hour)
	m.Tick(context.Background())
	if f.engaged || len(f.applied) != 0 {
		t.Fatalf("fenced without knowing the settings")
	}
	if f.sample == nil || f.sample.Active {
		t.Fatalf("the live state is still published: %+v", f.sample)
	}
}
