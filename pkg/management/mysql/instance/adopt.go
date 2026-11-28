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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cnmsql/cnmsql/pkg/engine"
)

// adoptSentinel marks, on the data directory, that a volume created from a
// VolumeSnapshot was adopted. It holds the adopting cluster's UID: the
// snapshot's own data directory may carry the sentinel of an earlier adoption
// (a snapshot of a cluster that was itself recovered from one), which must not
// skip this one. .cnmsql-bootstrapped cannot serve: it comes with the source.
const adoptSentinel = ".cnmsql-adopted"

// adoptLeftovers are the files of the source instance that must not survive a
// clone: its process and socket files, its corruption record, and its replay
// and adoption sentinels, which would skip this restore's own steps.
var adoptLeftovers = []string{"*.pid", "*.sock", "*.sock.lock", corruptionMarker, pitrSentinelFile, adoptSentinel}

// adoptVolume bootstraps a data directory restored from a VolumeSnapshot
// (design 045). The copy is already in place, so unlike Restore nothing is
// downloaded, prepared or copied back: the source's leftovers are removed, the
// anchor is read from the snapshot's manifest (or, for a cold snapshot,
// derived from the stopped server's state), replication and the internal
// credentials are reset, the archive identity is renewed, and archived
// binlogs are replayed from the anchor when a recovery target is set.
func adoptVolume(ctx context.Context, opts RestoreOptions, eng engine.Engine) error {
	log := logf.FromContext(ctx).WithName("instance-adopt").WithValues("dataDir", opts.DataDir, "backup", opts.BackupName)
	if opts.DataDir == "" {
		return errors.New("adopt: data dir is required")
	}
	if opts.AdoptID == "" {
		return errors.New("adopt: the adopting cluster's UID is required")
	}
	if !hasSystemSchema(opts.DataDir) {
		return fmt.Errorf("adopt: %s holds no MySQL data directory; the snapshot did not hold one", opts.DataDir)
	}

	if adoptedBy(opts.DataDir) == opts.AdoptID {
		log.Info("Data directory already adopted; skipping adoption")
	} else {
		log.Info("Adopting a data directory restored from a volume snapshot")
		if err := removeAdoptLeftovers(opts.DataDir); err != nil {
			return err
		}
		if err := ensureAdoptAnchor(ctx, &opts, eng); err != nil {
			if opts.SourceCluster != "" {
				return err
			}
			log.Error(err, "Could not determine the snapshot's anchor; continuing without point-in-time recovery")
		}
		if err := opts.reconcileRestoredServer(ctx, eng); err != nil {
			return fmt.Errorf("reconciling the adopted server: %w", err)
		}
		if err := resetArchiveIdentity(opts.DataDir, eng.Flavor()); err != nil {
			return fmt.Errorf("resetting archive identity: %w", err)
		}
		if err := os.WriteFile(filepath.Join(opts.DataDir, adoptSentinel), []byte(opts.AdoptID+"\n"), 0o600); err != nil {
			return fmt.Errorf("marking the data directory adopted: %w", err)
		}
	}

	if opts.SourceCluster == "" {
		log.Info("Completed adoption of the volume snapshot")
		return nil
	}
	manifest, err := ReadSnapshotManifest(opts.DataDir)
	if err != nil {
		return fmt.Errorf("reading the snapshot anchor: %w", err)
	}
	if manifest == nil || manifest.BackupName != opts.BackupName {
		return fmt.Errorf("adopt: point-in-time recovery needs the anchor of backup %q, and the data directory has none",
			opts.BackupName)
	}
	opts.Anchor = &engine.BinlogInfo{File: manifest.BinlogFile, Position: manifest.BinlogPos, GTIDSet: manifest.AnchorGTID}
	if err := opts.maybeReplay(ctx, eng.Backup(), eng); err != nil {
		return fmt.Errorf("replaying archived binlogs: %w", err)
	}
	log.Info("Completed adoption of the volume snapshot")
	return nil
}

// adoptedBy returns the UID recorded by a completed adoption, empty when none.
func adoptedBy(dataDir string) string {
	content, err := os.ReadFile(filepath.Join(dataDir, adoptSentinel))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(content))
}

// removeAdoptLeftovers deletes the source instance's files that must not
// survive the clone.
func removeAdoptLeftovers(dataDir string) error {
	for _, pattern := range adoptLeftovers {
		matches, err := filepath.Glob(filepath.Join(dataDir, pattern))
		if err != nil {
			return err
		}
		for _, match := range matches {
			if err := os.Remove(match); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("removing %s: %w", match, err)
			}
		}
	}
	return nil
}

// ensureAdoptAnchor makes sure the data directory holds a manifest for this
// Backup. An online snapshot carries the one its hold wrote. A cold snapshot
// carries none for it (at most a stale one from an earlier online snapshot of
// the same instance), and its anchor is read from the stopped server itself:
// a temporary server is started with the binary log on, so crash recovery
// consults it and gtid_binlog_pos is known on MariaDB, and its position is
// written as the manifest.
func ensureAdoptAnchor(ctx context.Context, opts *RestoreOptions, eng engine.Engine) error {
	manifest, err := ReadSnapshotManifest(opts.DataDir)
	if err != nil {
		return err
	}
	if manifest != nil && manifest.BackupName == opts.BackupName {
		return nil
	}
	anchor, version, err := deriveAnchor(ctx, opts, eng)
	if err != nil {
		return fmt.Errorf("deriving the anchor of a cold snapshot: %w", err)
	}
	return WriteSnapshotManifest(opts.DataDir, SnapshotManifest{
		BackupName:    opts.BackupName,
		AnchorGTID:    anchor.GTIDSet,
		BinlogFile:    anchor.File,
		BinlogPos:     anchor.Position,
		Flavor:        string(eng.Flavor()),
		ServerVersion: version,
		TakenAt:       time.Now().UTC().Format(time.RFC3339),
		Online:        false,
	})
}

// deriveAnchor starts a temporary socket-only server over the data directory
// and reads its GTID position and binary-log coordinates. No statement that
// writes runs, so the server records nothing new.
func deriveAnchor(ctx context.Context, opts *RestoreOptions, eng engine.Engine) (engine.BinlogInfo, string, error) {
	log := logf.FromContext(ctx).WithName("instance-adopt")
	v := restoredServerVersion(opts.Version)
	skipStart := "--skip-slave-start"
	if eng.UsesReplicaTerminology(v) {
		skipStart = "--skip-replica-start"
	}
	args := append(temporaryServerArgs(opts.ConfigFile, opts.DataDir, opts.Socket), "--skip-grant-tables", skipStart)
	stdout, stderr := newProcessLogWriters(log.WithName("temporary-mysqld"))
	sup := NewProcessSupervisor(opts.MysqldPath, args, WithShutdownTimeout(opts.ReadyTimeout), WithOutput(stdout, stderr))
	log.Info("Starting temporary mysqld to read a cold snapshot's anchor", "socket", opts.Socket)
	if err := sup.Start(ctx); err != nil {
		return engine.BinlogInfo{}, "", fmt.Errorf("starting temporary server: %w", err)
	}
	defer func() { _ = sup.Shutdown(ctx) }()
	db, err := waitForSocket(ctx, opts.Socket, "root", "", opts.ReadyTimeout)
	if err != nil {
		return engine.BinlogInfo{}, "", err
	}
	defer func() { _ = db.Close() }()

	gtidQuery, statusQuery := "SELECT @@GLOBAL.gtid_executed", "SHOW MASTER STATUS"
	if eng.Flavor() == engine.FlavorMariaDB {
		gtidQuery = "SELECT @@GLOBAL.gtid_binlog_pos"
	} else if v.AtLeast(8, 2, 0) {
		statusQuery = "SHOW BINARY LOG STATUS"
	}
	var gtid string
	if err := db.QueryRowContext(ctx, gtidQuery).Scan(&gtid); err != nil {
		return engine.BinlogInfo{}, "", fmt.Errorf("reading the GTID position: %w", err)
	}
	file, pos, err := readBinlogStatus(ctx, db, statusQuery)
	if err != nil {
		return engine.BinlogInfo{}, "", err
	}
	var version string
	_ = db.QueryRowContext(ctx, "SELECT @@GLOBAL.version").Scan(&version)
	anchor := engine.BinlogInfo{File: file, Position: pos, GTIDSet: strings.NewReplacer("\n", "", " ", "").Replace(gtid)}
	log.Info("Read the cold snapshot's anchor", "anchorGTID", anchor.GTIDSet, "binlogFile", file, "binlogPos", pos)
	return anchor, version, nil
}
