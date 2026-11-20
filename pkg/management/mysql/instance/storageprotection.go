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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/diskusage"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/groupreplication"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// Storage protection holds the writable primary read-only while its data
// volume is at (or past) the configured usage threshold, so a nearly-full
// disk cannot corrupt data or crash-loop the server (design
// 042-storage-full-read-only.md). Reads and replication continue; the pause
// lifts once usage recovers below the threshold minus hysteresis — after a
// volume expansion, for example.
//
// The watchdog lives in the instance manager on purpose: detection is a local
// statfs the manager already takes for the storage metrics, a wedged operator
// must not be able to disable the protection, and the manager is the one
// component that can act before ENOSPC. The operator only observes and
// reports (the StorageReadOnly condition); it never commands.
//
// The state machine is deliberately narrow. It acts only on an instance that
// holds the writable-primary intent: the expected role is primary, the
// instance is neither fenced nor shutting down, and the topology's own
// authority agrees — status.targetPrimary for asynchronous clusters, the
// local group view's PRIMARY for Group Replication. Replicas are permanently
// read-only and fencing stops mysqld outright, so the only writable surface a
// full disk can corrupt is the primary. When the designation is not readable
// the watchdog does nothing: it fails safe towards read-only.

const (
	// storageProtectionInterval is how often the watchdog samples the data
	// volume. statfs is cheap, and the interval bounds how far a fast fill can
	// outrun the protection.
	storageProtectionInterval = 10 * time.Second

	// storageProtectionTickTimeout bounds every server interaction of one
	// pass. SET GLOBAL read_only pends on the GLOBAL metadata lock behind
	// queued DML, and the protection is worth exactly as much as it is prompt.
	storageProtectionTickTimeout = 15 * time.Second

	// storageProtectionHysteresis is the percentage-point drop below the
	// engagement threshold required before read-only is lifted, so a volume
	// hovering at the threshold does not flap the server in and out of
	// read-only.
	storageProtectionHysteresis = 5
)

// StorageProtector is the read-only-on-storage-pressure state machine for one
// instance. The run loop drives it with Tick; the engagement is reported
// through report so the control API shows it in /status.
type StorageProtector struct {
	log logr.Logger

	// usage takes one data-volume snapshot. An error means the sample is not
	// trustworthy and the pass must not act on it.
	usage func() (diskusage.Usage, error)
	// liveReadOnly reads the server's live read-only flags. An error means
	// mysqld is not answering and the pass must not act either.
	liveReadOnly func(ctx context.Context) (readOnlyState, error)
	// applyReadOnly switches the server into (on) or out of (off) read-only
	// mode — read_only plus, when the engine supports it, super_read_only, in
	// the order demotion and promotion use.
	applyReadOnly func(ctx context.Context, on bool) error
	// writable reports whether the instance holds the writable-primary intent
	// right now.
	writable func(ctx context.Context) bool
	// report publishes the engagement for the instance status.
	report func(engaged bool)

	// hasSuperReadOnly reports whether the engine and version support
	// super_read_only (MySQL does; MariaDB does not). It decides when the
	// flags count as fully asserted.
	hasSuperReadOnly bool
	// threshold is the engagement ratio (thresholdPercent / 100).
	threshold float64

	// engaged is the latch: true from the pass that asserted read-only until
	// the pass that clears it or sees the designation move away. It keeps the
	// watchdog from ever clearing a flag it did not set — one left by demotion,
	// promotion or the user — and it is rebuilt from live facts after a
	// manager restart: a still-full volume re-engages on the first tick.
	engaged bool
}

// readOnlyState mirrors replication's read-only reading; the indirection keeps
// the state machine unit-testable without a server.
type readOnlyState struct {
	ReadOnly      bool
	SuperReadOnly bool
}

// storageDecision is the pure protection state machine. usage is the
// data-volume snapshot, readOnlyAlready whether the server is fully read-only,
// writable whether the writable-primary intent holds, engaged the previous
// latch, threshold the engagement ratio. It returns whether to apply or clear
// the flags and the resulting latch.
//
// Engagement fires at the threshold or when the space actually available to an
// unprivileged writer is gone: root-reserved blocks can keep used% below any
// chosen threshold while mysqld is already getting ENOSPC. Release waits for
// the hysteresis margin.
func storageDecision(
	usage diskusage.Usage, readOnlyAlready, writable, engaged bool, threshold float64,
) (set, clear, latch bool) {
	if !writable {
		// The flags belong to demotion and to the replica configuration now;
		// a stale engagement is dropped without touching them.
		return false, false, false
	}
	ratio := usage.Ratio()
	pressured := ratio >= threshold ||
		(usage.CapacityBytes > 0 && usage.AvailableBytes <= 0)
	switch {
	case pressured && !readOnlyAlready:
		return true, false, true
	case pressured:
		return false, false, true
	case engaged && ratio <= threshold-float64(storageProtectionHysteresis)/100:
		return false, true, false
	default:
		return false, false, engaged
	}
}

// Tick takes one sample and applies the state machine. Every server
// interaction of the pass is bounded; a pass that cannot complete leaves the
// latch untouched for the next tick to retry.
func (p *StorageProtector) Tick(ctx context.Context) {
	tickCtx, cancel := context.WithTimeout(ctx, storageProtectionTickTimeout)
	defer cancel()

	usage, err := p.usage()
	if err != nil {
		p.log.Error(err, "Storage protection: could not read data-volume usage")
		return
	}
	ro, err := p.liveReadOnly(tickCtx)
	if err != nil {
		// mysqld is not answering (down, restarting, upgrading): nothing to
		// assert this pass. The next pass covers it.
		p.log.V(1).Info("Storage protection: could not read the read-only flags", "error", err.Error())
		return
	}
	readOnlyAlready := ro.ReadOnly && (!p.hasSuperReadOnly || ro.SuperReadOnly)

	set, clear, latch := storageDecision(usage, readOnlyAlready, p.writable(tickCtx), p.engaged, p.threshold)
	switch {
	case set:
		if err := p.applyReadOnly(tickCtx, true); err != nil {
			p.log.Error(err, "Storage protection: could not switch the instance to read-only")
			return
		}
		p.log.Info("Storage protection engaged: data volume reached the read-only threshold; writes are paused",
			"usedPercent", fmt.Sprintf("%.1f", usage.Ratio()*100),
			"thresholdPercent", fmt.Sprintf("%.1f", p.threshold*100),
			"availableBytes", usage.AvailableBytes)
	case clear:
		if err := p.applyReadOnly(tickCtx, false); err != nil {
			p.log.Error(err, "Storage protection: could not lift the read-only pause")
			return
		}
		p.log.Info("Storage protection released: data-volume usage recovered below the threshold; writes resume",
			"usedPercent", fmt.Sprintf("%.1f", usage.Ratio()*100))
	}
	p.engaged = latch
	p.report(p.engaged)
}

// startStorageProtection runs the watchdog for the life of the manager. It
// takes one pass immediately — a manager that boots into a still-full volume
// re-engages before the control API starts serving, not one interval later —
// and then every storageProtectionInterval.
func startStorageProtection(
	ctx context.Context,
	controller *Controller,
	dataDir string,
	thresholdPercent int,
	view *storageProtectionClusterView,
) {
	p := newStorageProtector(controller,
		func() (diskusage.Usage, error) { return diskusage.Of(dataDir) },
		thresholdPercent, view)
	go func() {
		p.Tick(ctx)
		ticker := time.NewTicker(storageProtectionInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.Tick(ctx)
			}
		}
	}()
}

// newStorageProtector wires the watchdog to the controller's live state: the
// same control connection demotion and promotion speak through, the same
// expected-role belief the role reconciler drives, and the same fence gate the
// supervisor honours. usage is injected so tests can feed fixed snapshots.
func newStorageProtector(
	controller *Controller,
	usage func() (diskusage.Usage, error),
	thresholdPercent int,
	view *storageProtectionClusterView,
) *StorageProtector {
	return &StorageProtector{
		log:   logf.Log.WithName("storage-protection").WithValues("instance", controller.name),
		usage: usage,
		liveReadOnly: func(ctx context.Context) (readOnlyState, error) {
			ro, err := controller.repl.ReadOnly(ctx)
			if err != nil {
				return readOnlyState{}, err
			}
			return readOnlyState{ReadOnly: ro.ReadOnly, SuperReadOnly: ro.SuperReadOnly}, nil
		},
		applyReadOnly: func(ctx context.Context, on bool) error {
			if on {
				if err := controller.repl.SetReadOnly(ctx, true); err != nil {
					return err
				}
				// No-op on engines (MariaDB) or versions without the variable.
				return controller.repl.SetSuperReadOnly(ctx, true)
			}
			if err := controller.repl.SetSuperReadOnly(ctx, false); err != nil {
				return err
			}
			return controller.repl.SetReadOnly(ctx, false)
		},
		writable: func(ctx context.Context) bool {
			return controller.storageProtectionWritable(ctx, view)
		},
		report:           controller.setStorageProtectionEngaged,
		hasSuperReadOnly: !controller.mariadb && controller.version.HasSuperReadOnly(),
		threshold:        float64(thresholdPercent) / 100,
		engaged:          controller.StorageProtectionEngaged(),
	}
}

// storageProtectionWritable reports whether this instance holds the
// writable-primary intent: the expected role is primary, the instance is
// neither fenced nor shutting down, and the topology's own designation
// agrees. When the designation is not (yet) readable the answer is no — the
// watchdog never acts on a guess.
func (c *Controller) storageProtectionWritable(ctx context.Context, view *storageProtectionClusterView) bool {
	if c.expected != webserver.RolePrimary {
		return false
	}
	if c.fence != nil && c.fence.IsFenced() {
		return false
	}
	if c.shuttingDown.Load() {
		return false
	}
	if !view.managed() {
		// No followed Cluster (a standalone boot with a static --role): the
		// expected role is the only authority there is.
		return true
	}
	if view.groupReplication() {
		// The group elects the primary; its own view is the authority.
		gr := c.groupReplicationStatus(ctx)
		if gr == nil {
			return false
		}
		return gr.Role == groupreplication.MemberRolePrimary
	}
	target, ok := view.targetPrimary()
	if !ok {
		return false
	}
	return target == c.name
}

// storageProtectionClusterView is the part of the followed Cluster the
// watchdog reads: whether this instance is still the designated primary, and
// whether the topology is Group Replication (whose primary the group elects,
// not the operator). It rides the role reconciler's OnCluster feed, so the
// watchdog needs no watch of its own.
type storageProtectionClusterView struct {
	instance string
	// target is status.targetPrimary from the latest Cluster read; nil until
	// the first read lands.
	target atomic.Pointer[string]
	// isGroupReplication marks a Group Replication cluster.
	isGroupReplication atomic.Bool
	// observed reports that at least one Cluster read has landed.
	observed atomic.Bool
}

func newStorageProtectionClusterView(instance string) *storageProtectionClusterView {
	return &storageProtectionClusterView{instance: instance}
}

// Observe records the latest Cluster read by the role reconciler.
func (v *storageProtectionClusterView) Observe(cluster *mysqlv1alpha1.Cluster) {
	target := cluster.Status.TargetPrimary
	v.target.Store(&target)
	v.isGroupReplication.Store(cluster.IsGroupReplication())
	v.observed.Store(true)
}

// managed reports whether the instance runs under cluster management, i.e.
// whether a followed Cluster feeds this view. A view that never observed a
// Cluster in a managed boot simply has not read one yet.
func (v *storageProtectionClusterView) managed() bool {
	return v.instance != "" && v.observed.Load()
}

func (v *storageProtectionClusterView) groupReplication() bool {
	return v.isGroupReplication.Load()
}

func (v *storageProtectionClusterView) targetPrimary() (string, bool) {
	if target := v.target.Load(); target != nil {
		return *target, true
	}
	return "", false
}
