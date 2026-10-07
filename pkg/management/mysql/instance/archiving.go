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
	"database/sql"
	"maps"
	"slices"
	"sync/atomic"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/binlog"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// replicaProbe adapts the replication manager to binlog.ReplicationProbe. It
// answers the one question the archiver's drain needs: has the source accepted
// this instance's GTID position and started streaming? Under
// MASTER_USE_GTID=current_pos a diverged instance is refused (error 1236) and so
// never reports streaming, which is what makes the drain safe.
type replicaProbe struct {
	repl *replication.Manager
}

func (p replicaProbe) Streaming(ctx context.Context) (bool, error) {
	state, err := p.repl.ReplicaState(ctx)
	if err != nil {
		return false, err
	}
	if state == nil || !state.Configured {
		return false, nil
	}
	return state.IORunning && state.SQLRunning, nil
}

// clusterFloor is the purge gate's replica floor, read from the Cluster the role
// reconciler already follows. The operator keeps status.gtidExecutedByInstance
// to the names in status.instanceNames and rewrites both in the same patch, so a
// scaled-down instance stops holding the purge as soon as it leaves the list.
//
// The positions are the operator's throttled snapshot, not live reads. That is
// safe in the one direction that matters: gtid_executed only grows, and a new or
// re-cloned instance starts from a copy of one whose set already contains any
// earlier floor, so an old snapshot can only understate what the replicas have
// and purge less.
type clusterFloor struct {
	instance string
	latest   atomic.Pointer[floorView]
}

// floorView is the part of a Cluster the floor reads.
type floorView struct {
	instances      []string
	diverged       []string
	positions      map[string]string
	currentPrimary string
	generation     int64
	timeline       engine.MariaDBTimeline
	epochs         []objectstore.ArchiveEpoch
}

func newClusterFloor(instance string) *clusterFloor {
	return &clusterFloor{instance: instance}
}

// Observe records the latest Cluster read by the role reconciler.
func (f *clusterFloor) Observe(cluster *mysqlv1alpha1.Cluster) {
	timeline := make(engine.MariaDBTimeline, 0, len(cluster.Status.MariaDBTimeline))
	epochs := make([]objectstore.ArchiveEpoch, 0, len(cluster.Status.MariaDBTimeline))
	for _, epoch := range cluster.Status.MariaDBTimeline {
		timeline = append(timeline, engine.MariaDBEpoch{ServerID: epoch.ServerID, Handoff: epoch.Handoff})
		epochs = append(epochs, objectstore.ArchiveEpoch{Instance: epoch.Instance, ServerID: epoch.ServerID, Handoff: epoch.Handoff})
	}
	f.latest.Store(&floorView{
		instances:      slices.Clone(cluster.Status.InstanceNames),
		diverged:       slices.Clone(cluster.Status.DivergedInstances),
		positions:      maps.Clone(cluster.Status.GTIDExecutedByInstance),
		currentPrimary: cluster.Status.CurrentPrimary,
		generation:     cluster.Status.CurrentPrimaryGeneration,
		timeline:       timeline,
		epochs:         epochs,
	})
}

// Primary implements binlog.ClusterView: status.currentPrimary and the
// position the operator last recorded for it. The record only understates
// what the primary holds (gtid_executed only grows), which is the safe
// direction for the drain gate.
func (f *clusterFloor) Primary() (string, string, bool) {
	view := f.latest.Load()
	if view == nil || view.currentPrimary == "" {
		return "", "", false
	}
	position, ok := view.positions[view.currentPrimary]
	return view.currentPrimary, position, ok
}

// Authority implements binlog.AuthoritySource: the primary generation, while
// the Cluster names this instance status.currentPrimary.
func (f *clusterFloor) Authority() (int64, bool) {
	view := f.latest.Load()
	if view == nil || view.currentPrimary != f.instance {
		return 0, false
	}
	return view.generation, true
}

// Epochs returns status.mariadbTimeline with instance names, as the archive
// stores it.
func (f *clusterFloor) Epochs() []objectstore.ArchiveEpoch {
	view := f.latest.Load()
	if view == nil {
		return nil
	}
	return view.epochs
}

// Diverged implements binlog.ClusterView.
func (f *clusterFloor) Diverged(name string) bool {
	view := f.latest.Load()
	return view != nil && slices.Contains(view.diverged, name)
}

// Timeline implements binlog.ClusterView: status.mariadbTimeline, absent on
// MySQL and replica clusters.
func (f *clusterFloor) Timeline() (engine.MariaDBTimeline, bool) {
	view := f.latest.Load()
	if view == nil || len(view.timeline) == 0 {
		return nil, false
	}
	return view.timeline, true
}

// Positions implements binlog.ReplicaFloor. Every expected instance but this
// one counts, fenced ones included since they come back. Diverged instances do
// not: the source refuses them whatever binlogs it keeps, so they are re-cloned
// anyway, and waiting for them would stop purging for good.
func (f *clusterFloor) Positions() (map[string]string, []string, bool) {
	view := f.latest.Load()
	if view == nil {
		return nil, nil, false
	}
	positions := make(map[string]string, len(view.instances))
	var unknown []string
	for _, name := range view.instances {
		if name == f.instance || slices.Contains(view.diverged, name) {
			continue
		}
		position, ok := view.positions[name]
		if !ok {
			unknown = append(unknown, name)
			continue
		}
		positions[name] = position
	}
	return positions, unknown, true
}

// ArchivingConfig configures the in-Pod continuous binlog archiver.
type ArchivingConfig struct {
	// Enabled turns the archiver on. The loop still only ships from the primary.
	Enabled bool
	// ObjectStore is the destination bucket + key prefix. Credentials/endpoint
	// come from the cnmsql_S3_* environment.
	ObjectStore mysqlv1alpha1.S3ObjectStore
	// ClusterName and InstanceName identify this segment of the archive.
	ClusterName  string
	InstanceName string
	// BinlogDir is the directory holding the local binary-log files (the
	// datadir, where log_bin writes them).
	BinlogDir string
	// MysqlbinlogPath is the mysqlbinlog/mariadb-binlog binary (defaults to PATH lookup).
	MysqlbinlogPath string
	// FlushInterval bounds the time-based RPO via forced FLUSH BINARY LOGS.
	FlushInterval time.Duration
	// Purge enables the active purge gate.
	Purge bool
	// MariaDB selects MariaDB GTID format for the binlog scanner and archiver.
	MariaDB bool
	// MariaDBGTIDModel provides MariaDB GTID operations for the archiver's
	// cumulative set tracking. Nil for MySQL.
	MariaDBGTIDModel binlog.MariaDBGTIDModel
	// ArchiveIdentity, when set, is the archive-partition key instead of the server's
	// server_uuid/server_id. MariaDB passes its persisted per-incarnation token so a
	// re-inited server (which reuses the config-assigned server_id) does not collide
	// with the previous incarnation's objects. Empty means query the server identity.
	ArchiveIdentity string
}

// newGTIDSet returns a factory for the binlog accumulation set. For MariaDB it
// uses the MariaDBGTIDModel; for MySQL it returns the default (replication.GTIDSet).
func (c *ArchivingConfig) newGTIDSet() func() binlog.GTIDOps {
	if c.MariaDB && c.MariaDBGTIDModel != nil {
		return func() binlog.GTIDOps {
			return binlog.NewMariadbGTIDSet(c.MariaDBGTIDModel)
		}
	}
	return nil // fallback to default in archiver (newMysqlGTIDSet)
}

// startArchiver builds and runs the continuous binlog archiver loop against the
// given control connection. It returns the loop (so its state can be surfaced in
// status) and a channel that receives the loop's terminal error. It blocks only
// long enough to read the server identity; the loop itself runs in a goroutine.
// identityQuery selects the flavor's archive-partition identity (MySQL
// server_uuid; MariaDB server_id).
func startArchiver(
	ctx context.Context,
	cfg ArchivingConfig,
	db *sql.DB,
	identityQuery string,
	repl *replication.Manager,
	floor *clusterFloor,
) (*binlog.Loop, <-chan error, error) {
	log := logf.FromContext(ctx).WithName("archiver")
	store, err := objectstore.NewClientFromEnv()
	if err != nil {
		return nil, nil, err
	}
	reader := binlog.NewReaderWithIdentityQuery(db, identityQuery)
	serverUUID := cfg.ArchiveIdentity
	if serverUUID == "" {
		var err error
		serverUUID, err = reader.ServerUUID(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	archiver, err := binlog.NewArchiver(binlog.ArchiverOptions{
		Store:        store,
		ObjectStore:  cfg.ObjectStore,
		ClusterName:  cfg.ClusterName,
		InstanceName: cfg.InstanceName,
		ServerUUID:   serverUUID,
		BinlogDir:    cfg.BinlogDir,
		Scan:         binlog.MysqlbinlogScanner(cfg.MysqlbinlogPath, cfg.MariaDB),
		NewSet:       cfg.newGTIDSet(),
		Forks:        forkSource(reader, floor, cfg.MariaDB),
		Authority:    floor.Authority,
	})
	if err != nil {
		return nil, nil, err
	}

	loop := binlog.NewLoop(binlog.LoopOptions{
		Reader:        reader,
		Archiver:      archiver,
		Logger:        log,
		FlushInterval: cfg.FlushInterval,
		Purge:         cfg.Purge,
		Floor:         floor,
		Replication:   replicaProbe{repl: repl},
		Cluster:       floor,
		Instance:      cfg.InstanceName,
		MariaDB:       cfg.MariaDB,
	})

	errCh := make(chan error, 1)
	go func() { errCh <- loop.Run(ctx) }()

	log.Info("Started continuous binlog archiver",
		"serverUUID", serverUUID,
		"bucket", cfg.ObjectStore.Bucket,
		"binlogDir", cfg.BinlogDir,
		"purgeGate", cfg.Purge)
	return loop, errCh, nil
}

// forkSource returns the authority the primary's fork check compares archive
// segments against: MySQL gtid_executed, or the MariaDB primary timeline from
// the Cluster (with the primary's own position recorded for audit). Before the
// operator has recorded a timeline there is no MariaDB authority, and the
// check is skipped rather than guessed.
func forkSource(reader *binlog.Reader, floor *clusterFloor, mariadb bool) binlog.ForkSource {
	if !mariadb {
		return func(ctx context.Context) (binlog.ForkJudge, error) {
			// A server that is no longer writable no longer speaks for the
			// surviving timeline, whatever pass it is finishing.
			if writable, err := reader.Writable(ctx); err != nil || !writable {
				return nil, err
			}
			executed, err := reader.ExecutedGTIDSet(ctx)
			if err != nil {
				return nil, err
			}
			return binlog.NewMySQLForkJudge(executed)
		}
	}
	return func(ctx context.Context) (binlog.ForkJudge, error) {
		timeline, ok := floor.Timeline()
		if !ok {
			return nil, nil
		}
		if writable, err := reader.Writable(ctx); err != nil || !writable {
			return nil, err
		}
		position, err := reader.CurrentPosition(ctx)
		if err != nil {
			return nil, err
		}
		if epochs := floor.Epochs(); len(epochs) == len(timeline) {
			return binlog.NewMariaDBArchiveJudge(epochs, position), nil
		}
		return binlog.NewMariaDBForkJudge(timeline, position), nil
	}
}

// archivingStatusProvider adapts a Loop's State to the webserver status shape.
func archivingStatusProvider(loop *binlog.Loop) func() *webserver.ArchivingStatus {
	return func() *webserver.ArchivingStatus { return archivingStatus(loop.State()) }
}

// archivingStatus converts an archiver state to the webserver status shape.
func archivingStatus(s binlog.State) *webserver.ArchivingStatus {
	out := &webserver.ArchivingStatus{
		Active:                s.Active,
		LastArchivedBinlog:    s.LastArchivedBinlog,
		LastArchivedGTID:      s.LastArchivedGTID,
		PendingFiles:          s.PendingFiles,
		LastError:             s.LastError,
		PurgeHeldBy:           s.PurgeHeldBy,
		OldestSegmentPosition: s.OldestSegmentPosition,
		DeferredFile:          s.DeferredFile,
		DisownedGTIDs:         s.Disowned,
		Gaps:                  s.Gaps,
		CoveredGTIDSet:        s.Covered,
	}
	for _, e := range s.Timeline {
		out.MariaDBTimeline = append(out.MariaDBTimeline, webserver.ArchiveEpochStatus{
			Instance: e.Instance, ServerID: e.ServerID, Handoff: e.Handoff,
		})
	}
	if !s.PurgeHeldSince.IsZero() {
		out.PurgeHeldSince = rfc3339(s.PurgeHeldSince)
	}
	if !s.LastArchivedTime.IsZero() {
		out.LastArchivedTime = rfc3339(s.LastArchivedTime)
	}
	if !s.LastErrorTime.IsZero() {
		out.LastErrorTime = rfc3339(s.LastErrorTime)
	}
	if !s.ForkCheckedAt.IsZero() {
		out.ForkCheckedAt = rfc3339(s.ForkCheckedAt)
	}
	for _, fork := range s.Forks {
		entry := webserver.ArchiveForkStatus{Segment: fork.ServerUUID, InstanceName: fork.InstanceName, GTIDs: fork.GTIDs}
		if !fork.DetectedAt.IsZero() {
			entry.DetectedAt = rfc3339(fork.DetectedAt)
		}
		out.Forks = append(out.Forks, entry)
	}
	return out
}

func rfc3339(t time.Time) string { return t.UTC().Format(time.RFC3339) }
