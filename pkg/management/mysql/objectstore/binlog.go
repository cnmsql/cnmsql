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
	"fmt"
	"strings"
	"time"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

const (
	// BinlogPrefixComponent is the path segment under a cluster prefix beneath
	// which all continuously-archived binary logs live.
	BinlogPrefixComponent = "binlogs"
	// ArchiveStatusName is the per-server-uuid status object name, holding the
	// last archived file and the GTID set covered by that segment.
	ArchiveStatusName = "_archive_status.json"
	// ArchiveIndexName is the cluster-level index object name, recording the
	// ordered list of timeline segments across however many server UUIDs the
	// cluster has produced over its failover history.
	ArchiveIndexName = "_index.json"
	// BinlogManifestSuffix is appended to a binlog object key to name its
	// per-file manifest.
	BinlogManifestSuffix = ".json"
)

// BinlogKeys contains the object keys for an archived binary-log file and its
// per-file manifest.
type BinlogKeys struct {
	// BinlogKey is the object key of the raw binary-log file.
	BinlogKey string
	// ManifestKey is the object key of the per-file JSON manifest.
	ManifestKey string
}

// BinlogPrefix returns the object-store key prefix under which a cluster's
// continuous binlog archive lives (trailing slash, so it cannot match sibling
// clusters whose names share this one as a prefix).
func BinlogPrefix(store mysqlv1alpha1.S3ObjectStore, clusterName string) string {
	return ClusterPrefix(store, clusterName) + BinlogPrefixComponent + "/"
}

// ServerPrefix returns the object-store key prefix for one server UUID's
// segment of the archive (trailing slash).
func ServerPrefix(store mysqlv1alpha1.S3ObjectStore, clusterName, serverUUID string) string {
	return BinlogPrefix(store, clusterName) + serverUUID + "/"
}

// BuildBinlogKeys returns deterministic object-store keys for an archived binlog
// file and its manifest. Keys are partitioned by server UUID so two primaries'
// like-named files (both produce a binlog.000004) never collide.
func BuildBinlogKeys(
	store mysqlv1alpha1.S3ObjectStore, clusterName, serverUUID, binlogName string,
) (BinlogKeys, error) {
	if store.Bucket == "" {
		return BinlogKeys{}, fmt.Errorf("object store bucket is required")
	}
	if clusterName == "" || serverUUID == "" || binlogName == "" {
		return BinlogKeys{}, fmt.Errorf("cluster name, server uuid and binlog name are required")
	}
	if strings.ContainsAny(serverUUID, "/") || strings.ContainsAny(binlogName, "/") {
		return BinlogKeys{}, fmt.Errorf("server uuid and binlog name must not contain a path separator")
	}
	base := ServerPrefix(store, clusterName, serverUUID) + binlogName
	return BinlogKeys{
		BinlogKey:   base,
		ManifestKey: base + BinlogManifestSuffix,
	}, nil
}

// ArchiveStatusKey returns the object key of a server UUID segment's archive
// status object.
func ArchiveStatusKey(store mysqlv1alpha1.S3ObjectStore, clusterName, serverUUID string) string {
	return ServerPrefix(store, clusterName, serverUUID) + ArchiveStatusName
}

// ArchiveIndexKey returns the object key of the cluster-level archive index.
func ArchiveIndexKey(store mysqlv1alpha1.S3ObjectStore, clusterName string) string {
	return BinlogPrefix(store, clusterName) + ArchiveIndexName
}

// BinlogMetadata is the inspectable per-file manifest written next to an
// archived binary-log file. It lets recovery order and verify the stream by
// GTID without parsing every file. It mirrors BackupMetadata's stance: the
// manifest, not provider ETag, is the integrity source of truth.
type BinlogMetadata struct {
	// ClusterName is the cluster the binlog was archived from.
	ClusterName string `json:"clusterName"`
	// ServerUUID is the server_uuid of the producing instance; it partitions the
	// key space so like-named files from different primaries never collide.
	ServerUUID string `json:"serverUUID"`
	// InstanceName is the instance the binlog was archived from.
	InstanceName string `json:"instanceName,omitempty"`
	// BinlogName is the binary-log file basename (e.g. "binlog.000004").
	BinlogName string `json:"binlogName"`
	// Sequence is the binlog index sequence parsed from the basename.
	Sequence int64 `json:"sequence"`
	// FirstGTID and LastGTID bound this file's contribution; GTIDSet is the full
	// set of GTIDs this file contributes, used to order and de-duplicate replay.
	FirstGTID string `json:"firstGTID,omitempty"`
	LastGTID  string `json:"lastGTID,omitempty"`
	GTIDSet   string `json:"gtidSet,omitempty"`
	// FirstEventTime and LastEventTime bound the file in wall-clock time, for
	// targetTime recovery.
	FirstEventTime time.Time `json:"firstEventTime,omitempty"`
	LastEventTime  time.Time `json:"lastEventTime,omitempty"`
	// SizeBytes and SHA256 verify the uploaded file.
	SizeBytes int64  `json:"sizeBytes"`
	SHA256    string `json:"sha256,omitempty"`
	// ArchivedAt is when the file landed in the object store.
	ArchivedAt time.Time `json:"archivedAt"`
}

// ArchiveStatus is the per-server-uuid status object: the cheap per-segment
// entry point recording how far this segment's archive has advanced.
type ArchiveStatus struct {
	// ClusterName and ServerUUID identify the segment.
	ClusterName string `json:"clusterName"`
	ServerUUID  string `json:"serverUUID"`
	// InstanceName is the instance owning this segment.
	InstanceName string `json:"instanceName,omitempty"`
	// LastArchivedBinlog is the most recent file fully archived in this segment.
	LastArchivedBinlog string `json:"lastArchivedBinlog,omitempty"`
	// LastArchivedGTID is the last GTID covered by this segment's archive.
	LastArchivedGTID string `json:"lastArchivedGTID,omitempty"`
	// FirstGTID is the first GTID this segment archived, set once and never
	// overwritten. It becomes the segment's StartGTIDSet in the index, giving
	// recovery the segment's per-domain range start.
	FirstGTID string `json:"firstGTID,omitempty"`
	// CoveredGTIDSet is the cumulative GTID set this segment has archived.
	CoveredGTIDSet string `json:"coveredGTIDSet,omitempty"`
	// UpdatedAt is when the status was last rewritten.
	UpdatedAt time.Time `json:"updatedAt"`
}

// ArchiveSegment is one timeline segment in the cluster-level index: a single
// server UUID's contiguous contribution to the logical timeline.
type ArchiveSegment struct {
	// ServerUUID is the producing instance's server_uuid for this segment.
	ServerUUID string `json:"serverUUID"`
	// InstanceName is the instance that produced the segment.
	InstanceName string `json:"instanceName,omitempty"`
	// Binlogs is the ordered list of file basenames archived in this segment.
	Binlogs []string `json:"binlogs,omitempty"`
	// GTIDSet is the full GTID set the segment contributes (its last/covered
	// position).
	GTIDSet string `json:"gtidSet,omitempty"`
	// StartGTIDSet is the segment's first archived GTID position. With GTIDSet it
	// bounds the segment's per-domain sequence range [start, end], which recovery
	// uses to stitch a gap-free replay from segments that individually have gaps
	// (e.g. after a re-init clone that reset the binlog history). A single
	// incarnation is contiguous per domain, so one interval per domain suffices.
	StartGTIDSet string `json:"startGTIDSet,omitempty"`
	// Fork records the transactions this segment archived that the surviving
	// timeline does not hold. Whichever primary detects them writes it; it only
	// ever grows, and it leaves the index with the segment when retention drops
	// the segment.
	Fork *ArchiveFork `json:"fork,omitempty"`
	// StartedAt and EndedAt bound the segment in wall-clock time.
	StartedAt time.Time `json:"startedAt,omitempty"`
	EndedAt   time.Time `json:"endedAt,omitempty"`
}

// ArchiveIndex is the cluster-level archive index: the MySQL-GTID analog of a
// PostgreSQL timeline-history file. It records the ordered list of timeline
// segments so recovery can discover and order the archive across many server
// UUIDs without brute-listing every prefix. GTID handles de-duplication; this
// index handles discovery and ordering.
type ArchiveIndex struct {
	// ClusterName is the cluster this index describes.
	ClusterName string `json:"clusterName"`
	// Segments is the ordered timeline, oldest first.
	Segments []ArchiveSegment `json:"segments"`
	// CoveredGTIDSet is the cumulative GTID set across the whole archive.
	CoveredGTIDSet string `json:"coveredGTIDSet,omitempty"`
	// UpdatedAt is when the index was last rewritten.
	UpdatedAt time.Time `json:"updatedAt"`
	// ForkCheck records the last fork check a primary ran over the whole index.
	// nil means no primary has checked this index since fork checks shipped.
	ForkCheck *ArchiveForkCheck `json:"forkCheck,omitempty"`
	// Generation is the highest status.currentPrimaryGeneration of a primary
	// that wrote the index. A writer whose generation is lower is a demoted
	// primary finishing a pass, and never judges the segments.
	Generation int64 `json:"generation,omitempty"`
	// Disowned accumulates every transaction recorded as disowned, by a fork
	// record or by a base backup found on a dead branch. Unlike fork records it
	// is not attached to a segment, so retention dropping the segment that held
	// a dead branch does not erase it: a base backup holding that branch is
	// still refused for a time or latest recovery.
	Disowned *ArchiveDisowned `json:"disowned,omitempty"`
	// ArchivedThrough is the time before which every transaction the primary
	// committed is archived. A targetTime after it cannot be proven
	// recoverable: transactions committed in between may sit in a binary log
	// the archive never received.
	ArchivedThrough time.Time `json:"archivedThrough,omitempty"`
}

// ArchiveDisowned is the index-level record of disowned transactions.
type ArchiveDisowned struct {
	// GTIDSet (MySQL) is the disowned set.
	GTIDSet string `json:"gtidSet,omitempty"`
	// Ranges (MariaDB) name, per domain and authoring server, the disowned
	// stretches of sequence numbers.
	Ranges []ArchiveDisownedRange `json:"ranges,omitempty"`
}

// ArchiveDisownedRange is a MariaDB dead branch: the transactions server
// authored in domain with a sequence in (After, Through].
type ArchiveDisownedRange struct {
	Domain  uint32 `json:"domain"`
	Server  uint32 `json:"server"`
	After   uint64 `json:"after"`
	Through uint64 `json:"through"`
}

// Holds reports whether the MariaDB range holds the transaction d-s-n.
func (r ArchiveDisownedRange) Holds(domain, server uint32, seq uint64) bool {
	return r.Domain == domain && r.Server == server && seq > r.After && seq <= r.Through
}

// AddRange folds a MariaDB dead branch into the record, merging it with a
// range of the same domain and server that it overlaps. It reports whether the
// record grew.
func (d *ArchiveDisowned) AddRange(r ArchiveDisownedRange) bool {
	if r.Through <= r.After {
		return false
	}
	for i := range d.Ranges {
		cur := &d.Ranges[i]
		if cur.Domain != r.Domain || cur.Server != r.Server || r.After > cur.Through || r.Through < cur.After {
			continue
		}
		grew := false
		if r.After < cur.After {
			cur.After, grew = r.After, true
		}
		if r.Through > cur.Through {
			cur.Through, grew = r.Through, true
		}
		return grew
	}
	d.Ranges = append(d.Ranges, r)
	return true
}

// ArchiveFork names the disowned part of a segment: transactions it archived
// that the surviving timeline never executed (a dead branch). Replay of a
// targetTime or latest recovery leaves them out.
type ArchiveFork struct {
	// GTIDSet (MySQL) is the disowned subset of the segment's GTIDSet.
	GTIDSet string `json:"gtidSet,omitempty"`
	// AfterSeq (MariaDB) maps a replication domain to the sequence the
	// surviving timeline inherited: every transaction this segment holds in
	// that domain with a higher sequence is disowned.
	AfterSeq map[uint32]uint64 `json:"afterSeq,omitempty"`
	// AuthorityGTIDSet is what the check compared against (MySQL
	// gtid_executed, MariaDB the primary's position). Audit only; replay never
	// reads it.
	AuthorityGTIDSet string `json:"authorityGTIDSet,omitempty"`
	// DetectedAt is when the record was first written.
	DetectedAt time.Time `json:"detectedAt"`
	// DetectedBy is the instance name and archive identity of the primary
	// whose check found it.
	DetectedBy string `json:"detectedBy"`
}

// ArchiveForkCheck stamps the last fork check run over an index.
type ArchiveForkCheck struct {
	CheckedAt time.Time `json:"checkedAt"`
	CheckedBy string    `json:"checkedBy"`
}

// Segment returns the segment for the given server UUID and whether it exists.
func (idx *ArchiveIndex) Segment(serverUUID string) (*ArchiveSegment, bool) {
	for i := range idx.Segments {
		if idx.Segments[i].ServerUUID == serverUUID {
			return &idx.Segments[i], true
		}
	}
	return nil, false
}
