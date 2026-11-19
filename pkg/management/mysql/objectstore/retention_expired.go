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

package objectstore

import (
	"context"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// uploadClockMargin is how long before the horizon a file must have been
// uploaded for its upload time to count as proof that it is old. The upload
// time is the store's clock and the horizon the database's.
const uploadClockMargin = 10 * time.Minute

// ArchivedBinlog is one archived binlog as the archive's listing shows it.
type ArchivedBinlog struct {
	// Keys are the binlog file and manifest object keys.
	Keys BinlogKeys
	// ServerUUID and Name are the segment and file basename the keys spell.
	ServerUUID string
	Name       string
	// UploadedAt is when the store says the manifest was written, zero when the
	// listing does not say.
	UploadedAt time.Time
}

// ManifestReader reads the per-file manifest of an archived binlog.
type ManifestReader func(ctx context.Context, keys BinlogKeys) (BinlogMetadata, error)

// ExpiredBinlogs returns the archived files whose last event predates horizon,
// reading as few manifests as it can.
//
// A file is uploaded after its last event was written, so only the files
// uploaded before the horizon can have expired, and the listing already says
// which those are. Among them, in a server's sequence order, the files whose
// last event predates the horizon form a prefix, so its end is found by
// binary search: a dozen reads for a segment of thousands.
//
// Neither clue is trusted alone. An event timestamp is whatever a session set
// it to, so a file uploaded after the horizon is kept whatever its manifest
// says; the store's clock may run behind, so a file uploaded before the
// horizon is kept unless the search also places it before the boundary. A
// manifest the search reads that carries no last-event time counts as not
// expired, which keeps that file and the files after it.
//
// A file the listing gives no upload time for, or whose name has no sequence,
// is judged by its own manifest.
func ExpiredBinlogs(
	ctx context.Context, read ManifestReader, files []ArchivedBinlog, horizon time.Time,
) ([]ArchivedBinlog, error) {
	expiredAt := func(f ArchivedBinlog) (bool, error) {
		meta, err := read(ctx, f.Keys)
		if err != nil {
			return false, err
		}
		return !meta.LastEventTime.IsZero() && meta.LastEventTime.Before(horizon), nil
	}

	type sequenced struct {
		file ArchivedBinlog
		seq  int64
	}
	searchable := map[string][]sequenced{}
	var unordered []ArchivedBinlog
	for _, f := range files {
		seq, ok := binlogSequence(f.Name)
		switch {
		case f.UploadedAt.IsZero() || !ok:
			unordered = append(unordered, f)
		case f.UploadedAt.Before(horizon.Add(-uploadClockMargin)):
			searchable[f.ServerUUID] = append(searchable[f.ServerUUID], sequenced{file: f, seq: seq})
		}
	}

	var expired []ArchivedBinlog
	for _, uuid := range slices.Sorted(maps.Keys(searchable)) {
		segment := searchable[uuid]
		sort.Slice(segment, func(i, j int) bool { return segment[i].seq < segment[j].seq })
		var readErr error
		kept := sort.Search(len(segment), func(i int) bool {
			if readErr != nil {
				return true
			}
			old, err := expiredAt(segment[i].file)
			if err != nil {
				readErr = err
				return true
			}
			return !old
		})
		if readErr != nil {
			return nil, readErr
		}
		for _, s := range segment[:kept] {
			expired = append(expired, s.file)
		}
	}
	for _, f := range unordered {
		old, err := expiredAt(f)
		if err != nil {
			return nil, err
		}
		if old {
			expired = append(expired, f)
		}
	}
	return expired, nil
}

// binlogSequence returns the number a binlog basename ends in
// ("binlog.000004" is 4).
func binlogSequence(name string) (int64, bool) {
	_, digits, ok := strings.CutLast(name, ".")
	if !ok {
		return 0, false
	}
	seq, err := strconv.ParseInt(digits, 10, 64)
	return seq, err == nil
}
