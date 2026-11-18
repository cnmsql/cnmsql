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
	"os"
	"time"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
)

// coverageRefresh is how long the primary reuses the archive coverage it
// read. It judges every closed binlog it left out against it on every pass,
// and the index is too large an object to read each time.
const coverageRefresh = 5 * time.Minute

// archiveCovers reports whether the archive index already covers every
// transaction of a file (design 041).
//
// A replica re-logs what it replicates, so a promoted one holds closed binlogs
// full of transactions the old primary archived. Shipping them stores the
// timeline twice. A file the archive covers only in part is shipped whole: it
// carries the old primary's unarchived tail.
//
// The index copy read may be stale. A stale copy lists less than the archive
// holds, so it can ship a file that was not needed and never leaves out one
// that was.
func (a *Archiver) archiveCovers(ctx context.Context, fileGTIDSet string) (bool, error) {
	if a.coverageAt.IsZero() || a.now().Sub(a.coverageAt) >= coverageRefresh {
		var index objectstore.ArchiveIndex
		key := objectstore.ArchiveIndexKey(a.objectStore, a.clusterName)
		if _, _, err := a.store.GetJSONVersion(ctx, a.objectStore.Bucket, key, &index); err != nil {
			return false, fmt.Errorf("binlog: reading archive index: %w", err)
		}
		a.coverage, a.coverageAt = index.CoveredGTIDSet, a.now()
	}
	held, err := replication.GTIDContains(a.coverage, fileGTIDSet)
	if err != nil {
		return false, fmt.Errorf("binlog: comparing a file with the archive coverage: %w", err)
	}
	return held, nil
}

// stillCovered judges again a file an earlier pass left out. It returns
// errCovered, with a manifest that carries only the file's GTID set, while the
// archive still holds its transactions. Once it does not, the file goes on to
// be shipped without being decoded again.
func (a *Archiver) stillCovered(ctx context.Context, name string, st os.FileInfo) (objectstore.BinlogMetadata, error) {
	left, ok := a.covered[name]
	delete(a.covered, name)
	if !ok || !left.matches(st) {
		return objectstore.BinlogMetadata{}, nil
	}
	if meta, err := a.leaveOutIfCovered(ctx, name, left); err != nil {
		return meta, err
	}
	a.deferredScans[name] = left
	return objectstore.BinlogMetadata{}, nil
}

// leaveOutIfCovered returns errCovered, with a manifest that carries only the
// file's GTID set, when the archive holds every transaction of a scanned
// file, and remembers the scan. A file with no transactions is never left
// out: there is nothing to judge.
func (a *Archiver) leaveOutIfCovered(
	ctx context.Context, name string, file scannedStamp,
) (objectstore.BinlogMetadata, error) {
	if file.scan.GTIDSet == "" {
		return objectstore.BinlogMetadata{}, nil
	}
	held, err := a.archiveCovers(ctx, file.scan.GTIDSet)
	if err != nil || !held {
		return objectstore.BinlogMetadata{}, err
	}
	a.covered[name] = file
	return objectstore.BinlogMetadata{GTIDSet: file.scan.GTIDSet}, errCovered
}

// ForgetCoverage drops the coverage read as a primary. An instance that is
// promoted again must not judge its files against what the archive held
// before it was demoted.
func (a *Archiver) ForgetCoverage() {
	a.coverage, a.coverageAt = "", time.Time{}
}

// forgetCovered drops the memo of the files left out that are no longer
// listed.
func (a *Archiver) forgetCovered(listed []BinaryLog) {
	if len(a.covered) == 0 {
		return
	}
	names := make(map[string]struct{}, len(listed))
	for _, l := range listed {
		names[l.Name] = struct{}{}
	}
	for name := range a.covered {
		if _, ok := names[name]; !ok {
			delete(a.covered, name)
		}
	}
}
