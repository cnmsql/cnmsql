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

package controller

import (
	"fmt"
	"sort"
	"time"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
)

// selectRecoveryBackup picks the base backup a recovery with no named backup
// starts from: the newest one that can reach the target.
//
//   - targetTime: completed at or before the target, since a base backup
//     already holds everything up to its completion and replay cannot undo it.
//   - targetGTID: an anchor the target contains, when the anchor is known.
//   - targetTime: an anchor holding nothing the archive recorded as
//     disowned, since replay cannot remove a dead branch the backup holds.
//
// Without a target nothing is replayed, so the newest backup is the answer.
// Backups whose anchor the metadata does not record are not judged on it.
func selectRecoveryBackup(
	entries []objectstore.BackupEntry, target *mysqlv1alpha1.RecoveryTarget,
	index *objectstore.ArchiveIndex, mariadb bool,
) (objectstore.BackupEntry, error) {
	if len(entries) == 0 {
		return objectstore.BackupEntry{}, fmt.Errorf("no base backups found in object store")
	}
	sorted := append([]objectstore.BackupEntry(nil), entries...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Meta.CompletedAt.After(sorted[j].Meta.CompletedAt)
	})

	var targetTime *time.Time
	targetGTID := ""
	replays := replaysBinlogs(target)
	if target != nil {
		targetGTID = target.TargetGTID
		if target.TargetTime != "" {
			t, err := time.Parse(time.RFC3339, target.TargetTime)
			if err != nil {
				return objectstore.BackupEntry{}, fmt.Errorf("invalid recovery targetTime %q: %w", target.TargetTime, err)
			}
			targetTime = &t
		}
	}
	model := engine.MustForFlavor(engine.FlavorMySQL).GTID()
	if mariadb {
		model = engine.MustForFlavor(engine.FlavorMariaDB).GTID()
	}

	var skipped []string
	for _, entry := range sorted {
		anchor := entry.Meta.AnchorGTID
		switch {
		case targetTime != nil && entry.Meta.CompletedAt.After(*targetTime):
			skipped = append(skipped, entry.Meta.BackupID+" (completed after the target)")
			continue
		case targetGTID != "" && anchor != "":
			if contains, err := model.Contains(targetGTID, anchor); err != nil || !contains {
				skipped = append(skipped, entry.Meta.BackupID+" (anchor not contained in the target)")
				continue
			}
		case replays && targetGTID == "" && anchorOnDeadBranch(anchor, index, mariadb):
			skipped = append(skipped, entry.Meta.BackupID+" (taken on a dead branch)")
			continue
		}
		return entry, nil
	}
	return objectstore.BackupEntry{}, fmt.Errorf("no base backup can reach the recovery target: %v", skipped)
}

// replaysBinlogs reports whether a recovery target replays the archive past
// the base backup: an absent or immediate target restores the backup alone.
func replaysBinlogs(target *mysqlv1alpha1.RecoveryTarget) bool {
	return target != nil && (target.TargetImmediate == nil || !*target.TargetImmediate)
}

// anchorOnDeadBranch reports whether a known anchor holds a transaction the
// archive recorded as disowned.
func anchorOnDeadBranch(anchor string, index *objectstore.ArchiveIndex, mariadb bool) bool {
	if anchor == "" || index == nil {
		return false
	}
	if !mariadb {
		disowned := []string{}
		if index.Disowned != nil {
			disowned = append(disowned, index.Disowned.GTIDSet)
		}
		for _, seg := range index.Segments {
			if seg.Fork != nil {
				disowned = append(disowned, seg.Fork.GTIDSet)
			}
		}
		all, err := replication.UnionGTIDStrings(disowned...)
		if err != nil || all == "" {
			return false
		}
		held, err := replication.IntersectsGTIDStrings(anchor, all)
		return err == nil && held
	}
	gtids, err := engine.ParseMariaDBPosition(anchor)
	if err != nil {
		return false
	}
	for _, g := range gtids {
		if index.Disowned != nil {
			for _, r := range index.Disowned.Ranges {
				if r.Holds(g.Domain, g.Server, g.Seq) {
					return true
				}
			}
		}
		for _, seg := range index.Segments {
			if seg.Fork == nil {
				continue
			}
			cut, ok := seg.Fork.AfterSeq[g.Domain]
			if !ok {
				continue
			}
			position, err := engine.ParseMariaDBPosition(seg.GTIDSet)
			if err != nil {
				continue
			}
			for _, last := range position {
				if last.Domain == g.Domain && last.Server == g.Server && g.Seq > cut {
					return true
				}
			}
		}
	}
	return false
}

// checkNamedBackupTarget refuses a targetTime before a named Backup
// completed: the backup already holds transactions committed after the target.
func checkNamedBackupTarget(backup *mysqlv1alpha1.Backup, target *mysqlv1alpha1.RecoveryTarget) error {
	if target == nil || target.TargetTime == "" || backup.Status.StoppedAt == nil {
		return nil
	}
	t, err := time.Parse(time.RFC3339, target.TargetTime)
	if err != nil {
		return fmt.Errorf("invalid recovery targetTime %q: %w", target.TargetTime, err)
	}
	if t.Before(backup.Status.StoppedAt.Time) {
		return fmt.Errorf("recovery targetTime %s is before backup %q completed (%s): "+
			"the backup already holds later transactions; name an older backup",
			target.TargetTime, backup.Name, backup.Status.StoppedAt.UTC().Format(time.RFC3339))
	}
	return nil
}
