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
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
)

// The semi-sync monitor watches the primary's live semi-sync flag (design
// 044-semisync-degradation.md). MySQL and MariaDB fall back to asynchronous
// replication when an acknowledgement wait times out, and switch back once a
// replica catches up, without telling anyone: the configuration flag
// rpl_semi_sync_*_enabled stays ON throughout. The monitor:
//
//   - samples the live flag on the writable primary, logs both transitions and
//     publishes how long it has been OFF, which the operator turns into the
//     SemiSyncDegraded condition;
//   - keeps the wait-no-replica variable ON in "required" mode on every
//     instance, so a primary with no connected replica still waits for the
//     timeout instead of falling back at once;
//   - optionally (readOnlyAfterDegradedSeconds) holds the primary read-only
//     once the flag has been OFF for that long, so writes stall instead of
//     continuing unprotected.
//
// Like the storage-protection watchdog (design 042) it lives in the instance
// manager, so a wedged operator cannot disable the fence, and it shares the
// read-only flags with that watchdog: neither clears them while the other
// holds them.

const (
	// semiSyncMonitorInterval is how often the monitor samples the live flag.
	// It bounds how far the read-only fence can trail its deadline, and the
	// precision of the reported inactive duration.
	semiSyncMonitorInterval = 2 * time.Second

	// semiSyncMonitorTickTimeout bounds every server interaction of one pass.
	// SET GLOBAL read_only pends on the GLOBAL metadata lock behind queued DML.
	semiSyncMonitorTickTimeout = 15 * time.Second
)

// semiSyncSettings is the part of the followed Cluster the monitor needs.
type semiSyncSettings struct {
	// required is true for dataDurability "required".
	required bool
	// readOnlyAfter is the fence delay; zero when the fence is off.
	readOnlyAfter time.Duration
}

// semiSyncSample is the monitor's latest view of the primary's live state,
// published for the instance status.
type semiSyncSample struct {
	Active  bool
	Clients int
	// InactiveSince is when the monitor first saw the flag OFF in the current
	// window; zero while it is ON.
	InactiveSince time.Time
}

// SemiSyncMonitor is the semi-sync observation and fence state machine for one
// instance. The run loop drives it with Tick.
type SemiSyncMonitor struct {
	// mu serializes passes: the run loop ticks on its own goroutine, and the
	// first Cluster read runs one pass on the role reconciler's (Observe).
	mu sync.Mutex
	// observed flips on the first Cluster read.
	observed atomic.Bool

	log logr.Logger
	now func() time.Time

	// live reads the source's live semi-sync state.
	live func(ctx context.Context) (replication.SemiSyncSourceLive, error)
	// position reads gtid_executed, to tell whether the primary committed
	// anything since a release on reconnected replicas.
	position func(ctx context.Context) (string, error)
	// setWaitNoReplica turns the wait-no-replica variable ON.
	setWaitNoReplica func(ctx context.Context) error
	// liveReadOnly and applyReadOnly read and switch the read-only flags, as
	// the storage-protection watchdog does.
	liveReadOnly  func(ctx context.Context) (readOnlyState, error)
	applyReadOnly func(ctx context.Context, on bool) error
	// writable reports whether the instance holds the writable-primary intent.
	writable func(ctx context.Context) bool
	// settings returns the Cluster's semi-sync settings, and false until the
	// first Cluster read lands.
	settings func() (semiSyncSettings, bool)
	// otherHolds reports whether another watchdog (storage protection) holds
	// the read-only flags, in which case releasing the fence must leave them.
	otherHolds func() bool
	// publish reports the latest sample (nil when there is none) and report
	// the fence engagement, for the instance status.
	publish func(*semiSyncSample)
	report  func(engaged bool)

	hasSuperReadOnly bool

	// inactiveSince is when the current OFF window was first seen; zero while
	// the flag is ON or the instance is not the writable primary.
	inactiveSince time.Time
	// settled is set after the first pass that knew the Cluster settings. The
	// pass before it may adopt a fence the server still holds from before an
	// instance-manager restart (the latch lives in memory).
	settled bool
	// probing is set by a release on reconnected replicas: the fence then
	// re-engages only once a commit stays unacknowledged for the semi-sync
	// timeout. A primary that commits nothing writes nothing unprotected.
	probing bool
	// probeGTID is gtid_executed at the release, and probeCommitAt when a pass
	// first saw it move.
	probeGTID     string
	probeCommitAt time.Time
	// clientsWereShort records that, since the fence engaged or last released
	// on reconnected replicas, a pass saw fewer semi-sync replicas than the
	// wait count. Only then does enough replicas being connected count as a
	// reconnect worth releasing for.
	clientsWereShort bool
	// engaged is the fence latch.
	engaged bool
}

// semiSyncFenceInput is everything the pure fence decision reads.
type semiSyncFenceInput struct {
	// configured is true when the fence is on (required mode, delay set).
	configured bool
	// due is true when the fence deadline has passed: the flag has been OFF
	// for the configured delay or, after a release on reconnected replicas, a
	// commit went unacknowledged for the semi-sync timeout.
	due bool
	// adopt is true on the first pass that knows the settings.
	adopt           bool
	active          bool
	enoughClients   bool
	reconnected     bool
	readOnlyAlready bool
	engaged         bool
}

// semiSyncFenceDecision is the pure fence state machine. It returns whether to
// assert or clear read-only, the resulting latch, and whether the release was
// on reconnected replicas (which arms the probe).
//
// Release has two triggers. The live flag coming back ON is the obvious one.
// But the server only switches back to semi-sync when a replica acknowledges a
// position at or past the last commit, and a read-only primary does not commit,
// so a replica that reconnects already caught up may never be asked for an
// acknowledgement. Enough semi-sync replicas reconnecting therefore releases
// the fence too, once per reconnect, as a probe: the next commit either gets
// its acknowledgement (the flag turns ON) or the fence re-engages one timeout
// later.
//
// The fence latch lives in memory, so an instance-manager restart (an in-place
// upgrade re-execs it while mysqld keeps running) loses it. On the first pass
// that knows the settings, a primary that is still read-only with the flag OFF
// is adopted as fenced instead of waiting a full delay, during which the role
// reconciler would make it writable again.
func semiSyncFenceDecision(in semiSyncFenceInput) (set, clear, latch, probe bool) {
	if !in.configured {
		// The fence was switched off while engaged: lift it.
		return false, in.engaged, false, false
	}
	if in.engaged {
		if in.active {
			return false, true, false, false
		}
		if in.enoughClients && in.reconnected {
			return false, true, false, true
		}
		return false, false, true, false
	}
	if in.active {
		return false, false, false, false
	}
	if in.adopt && in.readOnlyAlready {
		return false, false, true, false
	}
	if in.due {
		return !in.readOnlyAlready, false, true, false
	}
	return false, false, false, false
}

// Observe runs one pass synchronously on the first Cluster read. The role
// reconciler calls it before it reconciles, so a fence the server still holds
// from before an instance-manager restart is adopted before the reconciler
// could make the primary writable again.
func (m *SemiSyncMonitor) Observe(ctx context.Context) {
	if m.observed.CompareAndSwap(false, true) {
		m.Tick(ctx)
	}
}

// Tick takes one sample and applies the state machine. A pass that cannot read
// the server leaves every latch untouched for the next tick.
func (m *SemiSyncMonitor) Tick(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tickCtx, cancel := context.WithTimeout(ctx, semiSyncMonitorTickTimeout)
	defer cancel()

	live, err := m.live(tickCtx)
	if err != nil {
		m.log.V(1).Info("Semi-sync monitor: could not read the live semi-sync state", "error", err.Error())
		return
	}
	if !live.Observed {
		// The source plugin is not loaded (yet): nothing to observe.
		m.publish(nil)
		return
	}
	settings, known := m.settings()
	adopt := false
	if known {
		adopt = !m.settled
		m.settled = true
	}
	if known && settings.required {
		m.ensureWaitNoReplica(tickCtx, live)
	}
	if !m.writable(tickCtx) {
		m.leavePrimary()
		return
	}
	now := m.now()
	m.trackWindow(now, live)
	if known {
		m.fence(tickCtx, now, live, settings, adopt)
	}
}

// ensureWaitNoReplica keeps the wait-no-replica variable ON in "required"
// mode. It runs on every instance, not only the primary: a promoted replica
// must already wait on a replica-less primary, and a manual SET GLOBAL is
// repaired.
func (m *SemiSyncMonitor) ensureWaitNoReplica(ctx context.Context, live replication.SemiSyncSourceLive) {
	if live.WaitNoReplica {
		return
	}
	if err := m.setWaitNoReplica(ctx); err != nil {
		m.log.Error(err, "Semi-sync monitor: could not turn the wait-no-replica variable ON")
		return
	}
	m.log.Info("Turned the semi-sync wait-no-replica variable back ON for required data durability")
}

// leavePrimary resets the source-side state on a replica, a fenced instance
// or one shutting down: there is no window to track, and the read-only flags
// belong to demotion now, so an engaged fence is dropped without touching
// them.
func (m *SemiSyncMonitor) leavePrimary() {
	m.inactiveSince = time.Time{}
	m.probing = false
	m.clientsWereShort = false
	if m.engaged {
		m.engaged = false
		m.report(false)
	}
	m.publish(nil)
}

// trackWindow follows the live flag's OFF windows, logs both transitions and
// publishes the sample.
func (m *SemiSyncMonitor) trackWindow(now time.Time, live replication.SemiSyncSourceLive) {
	switch {
	case !live.Active && m.inactiveSince.IsZero():
		m.inactiveSince = now
		m.log.Info("Semi-sync fell back to asynchronous replication; commits are not acknowledged by any replica",
			"connectedReplicas", live.Clients, "waitForReplicaCount", live.WaitForReplicaCount)
	case live.Active && !m.inactiveSince.IsZero():
		m.log.Info("Semi-sync resumed; commits are acknowledged by replicas again",
			"inactiveFor", now.Sub(m.inactiveSince).Round(time.Second).String(),
			"connectedReplicas", live.Clients)
		m.inactiveSince = time.Time{}
		m.probing = false
	}
	m.publish(&semiSyncSample{Active: live.Active, Clients: live.Clients, InactiveSince: m.inactiveSince})
}

// inactiveFor is how long the current OFF window has lasted.
func (m *SemiSyncMonitor) inactiveFor(now time.Time) time.Duration {
	if m.inactiveSince.IsZero() {
		return 0
	}
	return now.Sub(m.inactiveSince)
}

// fenceDue reports whether the fence deadline has passed. Normally that is
// the flag staying OFF for the configured delay. After a release on
// reconnected replicas it is a commit staying unacknowledged for the semi-sync
// timeout (plus one pass, for the acknowledgement to show): the release lets
// the next commit prove the replica acknowledges, and a primary that commits
// nothing writes nothing unprotected, so it is never re-fenced for idling.
func (m *SemiSyncMonitor) fenceDue(
	ctx context.Context, now time.Time, live replication.SemiSyncSourceLive, after time.Duration,
) bool {
	if !m.probing {
		return m.inactiveFor(now) >= after
	}
	if m.probeCommitAt.IsZero() {
		gtid, err := m.position(ctx)
		if err != nil {
			m.log.V(1).Info("Semi-sync monitor: could not read gtid_executed", "error", err.Error())
			return false
		}
		if gtid == m.probeGTID {
			return false
		}
		m.probeCommitAt = now
	}
	grace := max(time.Duration(live.TimeoutMillis)*time.Millisecond, 0) + semiSyncMonitorInterval
	return now.Sub(m.probeCommitAt) >= grace
}

// fence applies the read-only fence state machine for one pass.
func (m *SemiSyncMonitor) fence(
	ctx context.Context, now time.Time, live replication.SemiSyncSourceLive, settings semiSyncSettings, adopt bool,
) {
	enoughClients := live.Clients >= max(1, live.WaitForReplicaCount)
	if !enoughClients {
		m.clientsWereShort = true
	}
	inactiveFor := m.inactiveFor(now)
	configured := settings.required && settings.readOnlyAfter > 0
	due := false
	if configured && !live.Active && !m.engaged {
		due = m.fenceDue(ctx, now, live, settings.readOnlyAfter)
	}
	readOnlyAlready := false
	if !live.Active || m.engaged {
		ro, err := m.liveReadOnly(ctx)
		if err != nil {
			m.log.V(1).Info("Semi-sync monitor: could not read the read-only flags", "error", err.Error())
			return
		}
		readOnlyAlready = ro.ReadOnly && (!m.hasSuperReadOnly || ro.SuperReadOnly)
	}

	set, clear, latch, probe := semiSyncFenceDecision(semiSyncFenceInput{
		configured:      configured,
		due:             due,
		adopt:           adopt,
		active:          live.Active,
		enoughClients:   enoughClients,
		reconnected:     m.clientsWereShort,
		readOnlyAlready: readOnlyAlready,
		engaged:         m.engaged,
	})
	switch {
	case set:
		if err := m.applyReadOnly(ctx, true); err != nil {
			m.log.Error(err, "Semi-sync monitor: could not switch the primary to read-only")
			return
		}
		m.log.Info("Semi-sync read-only fence engaged: the primary ran asynchronously too long; writes are paused",
			"inactiveFor", inactiveFor.Round(time.Second).String(), "readOnlyAfter", settings.readOnlyAfter.String(),
			"connectedReplicas", live.Clients)
	case latch && !m.engaged && !set:
		m.log.Info("Semi-sync read-only fence holds: the primary is still read-only with semi-sync OFF",
			"inactiveFor", inactiveFor.Round(time.Second).String(), "connectedReplicas", live.Clients)
	case clear && m.otherHolds():
		m.log.Info("Semi-sync read-only fence released; storage protection still holds the primary read-only")
	case clear:
		if err := m.applyReadOnly(ctx, false); err != nil {
			m.log.Error(err, "Semi-sync monitor: could not lift the read-only fence")
			return
		}
		m.log.Info("Semi-sync read-only fence released; writes resume",
			"semiSyncActive", live.Active, "connectedReplicas", live.Clients)
	}
	if latch && !m.engaged {
		// A fresh engagement: only a reconnect from here on counts.
		m.clientsWereShort = !enoughClients
	}
	if latch && !m.engaged {
		m.probing = false
	}
	if probe {
		m.clientsWereShort = false
		m.probing = true
		m.probeCommitAt = time.Time{}
		m.probeGTID = ""
		if gtid, err := m.position(ctx); err == nil {
			m.probeGTID = gtid
		}
	}
	if latch != m.engaged {
		m.engaged = latch
		m.report(latch)
	}
}

// startSemiSyncMonitor runs the monitor for the life of the manager: one pass
// immediately, then every semiSyncMonitorInterval.
func startSemiSyncMonitor(
	ctx context.Context, controller *Controller, view *storageProtectionClusterView,
) *SemiSyncMonitor {
	m := newSemiSyncMonitor(controller, view)
	go func() {
		m.Tick(ctx)
		ticker := time.NewTicker(semiSyncMonitorInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.Tick(ctx)
			}
		}
	}()
	return m
}

// newSemiSyncMonitor wires the monitor to the controller's live state, the
// same way newStorageProtector does.
func newSemiSyncMonitor(controller *Controller, view *storageProtectionClusterView) *SemiSyncMonitor {
	ops := controllerReadOnlyOps(controller)
	return &SemiSyncMonitor{
		log:      logf.Log.WithName("semi-sync-monitor").WithValues("instance", controller.name),
		now:      time.Now,
		live:     controller.repl.SemiSyncSourceLive,
		position: controller.repl.GTIDExecuted,
		setWaitNoReplica: func(ctx context.Context) error {
			return controller.repl.SetSemiSyncWaitNoReplica(ctx, true)
		},
		liveReadOnly:  ops.live,
		applyReadOnly: ops.apply,
		writable: func(ctx context.Context) bool {
			return controller.storageProtectionWritable(ctx, view)
		},
		settings:         view.semiSync,
		otherHolds:       controller.StorageProtectionEngaged,
		publish:          controller.setSemiSyncSample,
		report:           controller.setSemiSyncReadOnlyEngaged,
		hasSuperReadOnly: !controller.mariadb && controller.version.HasSuperReadOnly(),
	}
}
