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

package binlog

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
)

// drainCoverageRefresh is how long a drain reuses the archive coverage it
// read. A former primary judges every closed binlog it holds against it on
// every pass, and the index is too large an object to read each time.
const drainCoverageRefresh = 5 * time.Minute

// drainVerdict is what a draining former primary does with a closed binlog it
// has not shipped.
type drainVerdict int

const (
	// drainShip: the archive lacks transactions only this file can give it.
	drainShip drainVerdict = iota
	// drainRedundant: the archive holds every transaction of the file.
	drainRedundant
	// drainWait: the file is ahead of the archive; the primary ships those
	// transactions itself.
	drainWait
)

// judgeDrainFile decides whether a former primary's closed binlog is worth
// shipping, given what the archive covers.
//
// A demoted primary keeps writing binary logs: as a replica it re-logs every
// transaction the primary commits, and the primary archives those itself.
// Shipping the copy too stores the timeline twice. But the copy is also the
// only other source of those transactions, so it is shipped when the archive
// lacks some of them and will not get them otherwise:
//
//   - transactions of its own (ownUUID): the tail it stranded when it was
//     demoted, which nobody else logs unless a successor re-logged it;
//   - transactions the archive has already moved past: a hole, e.g. an index
//     entry lost to a stale write or a binlog the primary never shipped.
//
// Transactions ahead of the archive for their server are simply not archived
// yet, and the file waits.
func judgeDrainFile(fileGTIDSet, covered, ownUUID string) (drainVerdict, error) {
	file, err := replication.ParseGTIDSet(fileGTIDSet)
	if err != nil {
		return drainWait, fmt.Errorf("binlog: parsing file gtid set: %w", err)
	}
	held, err := replication.ParseGTIDSet(covered)
	if err != nil {
		return drainWait, fmt.Errorf("binlog: parsing archive coverage: %w", err)
	}
	missing := file.Difference(held)
	if missing.IsEmpty() {
		return drainRedundant, nil
	}
	for uuid, intervals := range missing {
		if len(intervals) == 0 {
			continue
		}
		if uuid == strings.ToLower(ownUUID) {
			return drainShip, nil
		}
		reached := held[uuid]
		if len(reached) > 0 && reached[len(reached)-1].End > intervals[0].Start {
			return drainShip, nil
		}
	}
	return drainWait, nil
}

// drainCoverage returns what the archive index covers, read at most once per
// drainCoverageRefresh.
func (a *Archiver) drainCoverage(ctx context.Context) (string, error) {
	if !a.coverageAt.IsZero() && a.now().Sub(a.coverageAt) < drainCoverageRefresh {
		return a.coverage, nil
	}
	var index objectstore.ArchiveIndex
	key := objectstore.ArchiveIndexKey(a.objectStore, a.clusterName)
	if _, _, err := a.store.GetJSONVersion(ctx, a.objectStore.Bucket, key, &index); err != nil {
		return "", fmt.Errorf("binlog: reading archive index: %w", err)
	}
	a.coverage, a.coverageAt = index.CoveredGTIDSet, a.now()
	return a.coverage, nil
}

// worthDraining reports whether a drain should ship a file with this GTID set.
func (a *Archiver) worthDraining(ctx context.Context, fileGTIDSet string) (bool, error) {
	covered, err := a.drainCoverage(ctx)
	if err != nil {
		return false, err
	}
	verdict, err := judgeDrainFile(fileGTIDSet, covered, a.serverUUID)
	return verdict == drainShip, err
}
