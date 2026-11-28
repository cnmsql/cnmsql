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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/version"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// coldSnapshotFile records, on the data directory, a cold snapshot's anchor
// capture (design 045). A cold snapshot is cut from a cleanly stopped server,
// so the position the server reports on its next start, before replication
// runs, is the snapshot's. The operator arms the capture once mysqld has
// stopped; the next start holds replication (--skip-replica-start), the
// manager reads the position into the record and resumes replication, and the
// operator copies the anchor to the Backup and clears the record.
//
// The record is on the data directory so it survives a manager restart
// between the cut and the start. It is armed before the cut, so the snapshot
// carries it, and adoption removes it.
const coldSnapshotFile = ".cnmsql-cold-snapshot"

// maxColdCaptureAttempts bounds the failed reads of a cold anchor. Past it the
// capture gives up and replication resumes: the restore derives the anchor
// from the snapshot instead, so a lost capture costs only the checks that read
// the anchor from the Backup.
const maxColdCaptureAttempts = 5

// coldSnapshotRecord is the content of coldSnapshotFile.
type coldSnapshotRecord struct {
	BackupName string                    `json:"backupName"`
	ArmedAt    string                    `json:"armedAt"`
	Anchor     *webserver.SnapshotAnchor `json:"anchor,omitempty"`
	Error      string                    `json:"error,omitempty"`
}

// pending reports whether the record still waits for its anchor.
func (r *coldSnapshotRecord) pending() bool {
	return r != nil && r.Anchor == nil && r.Error == ""
}

// readColdSnapshotRecord reads coldSnapshotFile, (nil, nil) when absent.
func readColdSnapshotRecord(dataDir string) (*coldSnapshotRecord, error) {
	content, err := os.ReadFile(filepath.Join(dataDir, coldSnapshotFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var record coldSnapshotRecord
	if err := json.Unmarshal(content, &record); err != nil {
		return nil, fmt.Errorf("decoding %s: %w", coldSnapshotFile, err)
	}
	return &record, nil
}

func writeColdSnapshotRecord(dataDir string, record coldSnapshotRecord) error {
	content, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return writeDataDirFile(dataDir, coldSnapshotFile, append(content, '\n'))
}

// ColdSnapshotStartArgs returns the mysqld arguments for the next start: the
// flag that keeps replication from starting while a cold capture is pending,
// none otherwise. An unreadable record holds replication too: the capture then
// records the failure and resumes it.
func ColdSnapshotStartArgs(dataDir string, eng engine.Engine, v version.Version) []string {
	if dataDir == "" {
		return nil
	}
	record, err := readColdSnapshotRecord(dataDir)
	if err == nil && !record.pending() {
		return nil
	}
	return []string{skipReplicaStartArg(eng, v)}
}

// skipReplicaStartArg is the mysqld flag that keeps replication from starting
// with the server. The option was renamed slave→replica in MySQL 8.0.26.
func skipReplicaStartArg(eng engine.Engine, v version.Version) string {
	if eng.UsesReplicaTerminology(v) {
		return "--skip-replica-start"
	}
	return "--skip-slave-start"
}

// armColdSnapshot records a pending capture for backupName. mysqld must be
// stopped by a fence: a capture armed on a running server would read a
// position past the cut on the next restart.
func (c *Controller) armColdSnapshot(ctx context.Context, backupName string) error {
	if c.fence == nil || c.supervisor == nil || !c.fence.IsFenced() || c.supervisor.Pid() != 0 {
		return webserver.ErrSnapshotNotStopped
	}
	c.coldMu.Lock()
	defer c.coldMu.Unlock()
	dataDir := c.snapshot.DataDir
	record, err := readColdSnapshotRecord(dataDir)
	if err == nil && record != nil && record.BackupName == backupName {
		return nil
	}
	// A record left by an earlier Backup is replaced.
	if err := writeColdSnapshotRecord(dataDir, coldSnapshotRecord{
		BackupName: backupName,
		ArmedAt:    time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		return fmt.Errorf("writing %s: %w", coldSnapshotFile, err)
	}
	c.coldAttempts = 0
	logf.FromContext(ctx).WithName("snapshot").Info("Armed the anchor capture of a cold volume snapshot",
		"backup", backupName)
	return nil
}

// coldSnapshotStatus reports the cold capture, nil when none.
func (c *Controller) coldSnapshotStatus() *webserver.ColdSnapshotStatus {
	if c.snapshot == nil {
		return nil
	}
	c.coldMu.Lock()
	defer c.coldMu.Unlock()
	record, err := readColdSnapshotRecord(c.snapshot.DataDir)
	if err != nil {
		return &webserver.ColdSnapshotStatus{Error: err.Error()}
	}
	if record == nil {
		return nil
	}
	return &webserver.ColdSnapshotStatus{BackupName: record.BackupName, Anchor: record.Anchor, Error: record.Error}
}

// clearColdSnapshot removes the cold capture of backupName. It reports
// whether there was one.
func (c *Controller) clearColdSnapshot(ctx context.Context, backupName string) (bool, error) {
	if c.snapshot == nil {
		return false, nil
	}
	c.coldMu.Lock()
	defer c.coldMu.Unlock()
	record, err := readColdSnapshotRecord(c.snapshot.DataDir)
	if err != nil || record == nil || record.BackupName != backupName {
		return false, nil
	}
	err = os.Remove(filepath.Join(c.snapshot.DataDir, coldSnapshotFile))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return true, err
	}
	logf.FromContext(ctx).WithName("snapshot").Info("Cleared the anchor capture of a cold volume snapshot",
		"backup", backupName)
	return true, nil
}

// captureColdAnchor reads a pending cold capture's anchor from the running
// server. It reports whether replication must stay held: true only while the
// capture is pending and has attempts left. Every other outcome (no capture,
// anchor read, capture given up) lets replication resume.
func (c *Controller) captureColdAnchor(ctx context.Context) bool {
	if c.snapshot == nil {
		return false
	}
	c.coldMu.Lock()
	defer c.coldMu.Unlock()
	log := logf.FromContext(ctx).WithName("snapshot")
	dataDir := c.snapshot.DataDir
	record, err := readColdSnapshotRecord(dataDir)
	if err != nil {
		// The start held replication for it; nothing can be captured from a
		// record that cannot be read, so drop it and resume.
		log.Error(err, "Could not read the cold volume snapshot record; resuming replication without its anchor")
		_ = os.Remove(filepath.Join(dataDir, coldSnapshotFile))
		return false
	}
	if !record.pending() {
		return false
	}
	// A server that is not up yet (or fenced again) costs no attempt: the
	// position is read once it answers.
	if (c.supervisor != nil && c.supervisor.Pid() == 0) || c.conn.PingContext(ctx) != nil {
		return true
	}
	anchor, err := c.readSnapshotPosition(ctx, c.conn)
	if err == nil {
		record.Anchor = &anchor
		if err = writeColdSnapshotRecord(dataDir, *record); err == nil {
			c.coldAttempts = 0
			log.Info("Read the anchor of a cold volume snapshot", "backup", record.BackupName,
				"anchorGTID", anchor.AnchorGTID, "binlogFile", anchor.BinlogFile, "binlogPos", anchor.BinlogPos)
			return false
		}
	}
	c.coldAttempts++
	if c.coldAttempts < maxColdCaptureAttempts {
		log.Info("Could not read the anchor of a cold volume snapshot; holding replication and retrying",
			"backup", record.BackupName, "attempt", c.coldAttempts, "error", err.Error())
		return true
	}
	record.Error = fmt.Sprintf("reading the position after %d attempts: %v", c.coldAttempts, err)
	if werr := writeColdSnapshotRecord(dataDir, *record); werr != nil {
		_ = os.Remove(filepath.Join(dataDir, coldSnapshotFile))
	}
	log.Error(err, "Gave up reading the anchor of a cold volume snapshot; resuming replication",
		"backup", record.BackupName, "attempts", c.coldAttempts)
	return false
}

// coldCaptureRetry is the wait between reads of a cold anchor after a start.
const coldCaptureRetry = 2 * time.Second

// ResumeAfterColdSnapshot runs after a mysqld start. While a cold capture is
// pending it reads the anchor (retrying while the server comes up), then
// starts the replication the start held. It is a no-op when no capture is
// pending, so it runs after every start.
func (c *Controller) ResumeAfterColdSnapshot(ctx context.Context) {
	if c.snapshot == nil {
		return
	}
	if record, err := readColdSnapshotRecord(c.snapshot.DataDir); err == nil && !record.pending() {
		return
	}
	if !c.coldResuming.CompareAndSwap(false, true) {
		return
	}
	defer c.coldResuming.Store(false)
	log := logf.FromContext(ctx).WithName("snapshot")
	ticker := time.NewTicker(coldCaptureRetry)
	defer ticker.Stop()
	for {
		if !c.captureColdAnchor(ctx) {
			if err := c.repl.EnsureReplicaStarted(ctx); err != nil {
				// The role reconciler starts it on its next pass.
				log.Error(err, "Could not resume replication after a cold volume snapshot")
			}
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
