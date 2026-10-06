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

package engine

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// MariaDBTimelineCeiling bounds how many epochs the operator keeps in
// status.mariadbTimeline, whatever still references the oldest ones.
const MariaDBTimelineCeiling = 256

// MariaDBGTID is one MariaDB transaction identifier: <domain>-<server>-<seq>.
type MariaDBGTID struct {
	Domain uint32
	Server uint32
	Seq    uint64
}

// String renders the GTID as domain-server-seq.
func (g MariaDBGTID) String() string {
	return strconv.FormatUint(uint64(g.Domain), 10) + "-" +
		strconv.FormatUint(uint64(g.Server), 10) + "-" +
		strconv.FormatUint(g.Seq, 10)
}

// ParseMariaDBGTID parses a single domain-server-seq triple.
func ParseMariaDBGTID(s string) (MariaDBGTID, error) {
	parts := strings.Split(strings.TrimSpace(s), "-")
	if len(parts) != 3 {
		return MariaDBGTID{}, fmt.Errorf("invalid mariadb gtid %q: want domain-server-seq", s)
	}
	domain, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return MariaDBGTID{}, fmt.Errorf("invalid mariadb gtid domain in %q: %w", s, err)
	}
	server, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return MariaDBGTID{}, fmt.Errorf("invalid mariadb gtid server_id in %q: %w", s, err)
	}
	seq, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return MariaDBGTID{}, fmt.Errorf("invalid mariadb gtid sequence in %q: %w", s, err)
	}
	return MariaDBGTID{Domain: uint32(domain), Server: uint32(server), Seq: seq}, nil
}

// ParseMariaDBPosition parses a MariaDB GTID position into one GTID per domain,
// ordered by domain. The empty position parses to no GTIDs.
func ParseMariaDBPosition(pos string) ([]MariaDBGTID, error) {
	parsed, err := parseMariaPos(pos)
	if err != nil {
		return nil, err
	}
	out := make([]MariaDBGTID, 0, len(parsed))
	for domain, e := range parsed {
		out = append(out, MariaDBGTID{Domain: domain, Server: e.server, Seq: e.seq})
	}
	slices.SortFunc(out, func(a, b MariaDBGTID) int { return int(a.Domain) - int(b.Domain) })
	return out, nil
}

// MariaDBEpoch is one change of primary on a MariaDB cluster: from Handoff on,
// ServerID authors every domain's transactions until the next epoch's Handoff.
type MariaDBEpoch struct {
	// ServerID is the @@server_id of the epoch's primary.
	ServerID uint32
	// Handoff is the primary's @@gtid_slave_pos when it took authority: per
	// domain, the last transaction it inherited.
	Handoff string
}

// MariaDBTimeline is the history of primaries of a MariaDB cluster, oldest
// first: the record of who authored each stretch of sequence numbers that a
// MariaDB GTID position, which names only the author of its last transaction,
// cannot give. It plays the part of a PostgreSQL timeline history file.
//
// In domain d, epoch i authored (start_i, start_{i+1}], where start_i is the
// sequence its Handoff holds in d (0 when absent) and the newest epoch's range
// is open-ended. Anything at or below the oldest epoch's start predates the
// timeline and gets no verdict. A stretch is unknown, and gets no verdict
// either, when the next epoch inherited from a server that is not this epoch's
// author: some primary the operator never observed wrote in between.
type MariaDBTimeline []MariaDBEpoch

// domainView is a timeline projected onto one domain.
type domainView struct {
	starts []uint64
	// handoffServer[i] is the author of the transaction epoch i inherited in
	// the domain; valid only when starts[i] > 0.
	handoffServer []uint32
	valid         bool
}

func (t MariaDBTimeline) domain(d uint32) domainView {
	v := domainView{
		starts:        make([]uint64, len(t)),
		handoffServer: make([]uint32, len(t)),
	}
	for i, epoch := range t {
		pos, err := parseMariaPos(epoch.Handoff)
		if err != nil {
			return domainView{}
		}
		if e, ok := pos[d]; ok {
			v.starts[i] = e.seq
			v.handoffServer[i] = e.server
		}
		if i > 0 && v.starts[i] < v.starts[i-1] {
			// The history is no longer a single chain in this domain (a
			// successor inherited less than its predecessor did), so no stretch
			// in it can be attributed.
			return domainView{}
		}
	}
	v.valid = true
	return v
}

// containing returns the epoch whose range in the domain holds seq, or -1 when
// seq predates the timeline.
func (v domainView) containing(seq uint64) int {
	idx := -1
	for i, start := range v.starts {
		if start < seq {
			idx = i
		}
	}
	return idx
}

// Verdict says whether g is a transaction of the surviving timeline. known is
// false when the timeline cannot tell: g predates it, falls in a stretch
// authored by a primary the operator never observed, or the timeline is empty
// or unreadable in g's domain.
func (t MariaDBTimeline) Verdict(g MariaDBGTID) (onTimeline, known bool) {
	if len(t) == 0 || g.Seq == 0 {
		return false, false
	}
	v := t.domain(g.Domain)
	if !v.valid {
		return false, false
	}
	i := v.containing(g.Seq)
	if i < 0 {
		return false, false
	}
	if next := i + 1; next < len(t) && v.handoffServer[next] != t[i].ServerID {
		// The successor inherited from someone other than this epoch's author:
		// an unobserved primary wrote part of this stretch. Only the handoff
		// transaction itself is certain, and so is any other author at its
		// sequence.
		if g.Seq == v.starts[next] {
			return g.Server == v.handoffServer[next], true
		}
		return false, false
	}
	return g.Server == t[i].ServerID, true
}

// Judge applies Verdict to every domain of a position. Because a domain's
// history is one linear chain, the last GTID per domain settles the whole
// prefix: the position is off the timeline as soon as one domain is known to be
// off, and on it only when every domain is known to be on.
func (t MariaDBTimeline) Judge(pos string) (onTimeline, known bool, err error) {
	gtids, err := ParseMariaDBPosition(pos)
	if err != nil {
		return false, false, err
	}
	allKnown := true
	for _, g := range gtids {
		on, k := t.Verdict(g)
		if k && !on {
			return false, true, nil
		}
		if !k {
			allKnown = false
		}
	}
	if !allKnown {
		return false, false, nil
	}
	return true, true, nil
}

// DeadAfter returns, for a transaction off the timeline, the sequence past
// which everything its author holds in the domain is disowned. ok is false
// when g is on the timeline or has no verdict.
//
// That sequence is the latest point the author could legitimately have reached
// before g: the end of the last epoch it authored, or of the last stretch whose
// successor inherited from it (an unobserved primary's stretch may be its own).
// Without either, it is the timeline's floor: every sequence above it is
// attributed to another author. The floor case is what keeps a dead branch
// detectable once the operator pruned the epoch its author held.
func (t MariaDBTimeline) DeadAfter(g MariaDBGTID) (uint64, bool) {
	on, known := t.Verdict(g)
	if on || !known {
		return 0, false
	}
	v := t.domain(g.Domain)
	i := v.containing(g.Seq)
	for k := i - 1; k >= 0; k-- {
		if t[k].ServerID == g.Server || v.handoffServer[k+1] == g.Server {
			return v.starts[k+1], true
		}
	}
	return v.starts[0], true
}

// PruneResult is the outcome of PruneMariaDBTimeline.
type PruneResult struct {
	// Timeline is the pruned timeline.
	Timeline MariaDBTimeline
	// Dropped is how many of the oldest entries were removed.
	Dropped int
	// Truncated is true when the ceiling forced out an entry something still
	// referenced; PinnedBy names those references.
	Truncated bool
	PinnedBy  []string
}

// PruneMariaDBTimeline drops the oldest epochs nothing still needs. Dropping the
// oldest entry raises the timeline's floor to the next entry's handoff, so it
// goes only when no reference position sits at or below that handoff in any
// domain the reference carries. refs maps a name (an instance, or the archive)
// to a position the operator still has to judge. Past ceiling entries the
// oldest goes regardless, and the result says what it still pinned. The input
// is left unmodified.
func PruneMariaDBTimeline(t MariaDBTimeline, refs map[string]string, ceiling int) PruneResult {
	res := PruneResult{Timeline: slices.Clone(t)}
	for len(res.Timeline) > 1 && len(pinners(res.Timeline[1], refs)) == 0 {
		res.Timeline = res.Timeline[1:]
		res.Dropped++
	}
	for ceiling > 0 && len(res.Timeline) > ceiling {
		for _, name := range pinners(res.Timeline[1], refs) {
			if !slices.Contains(res.PinnedBy, name) {
				res.PinnedBy = append(res.PinnedBy, name)
			}
		}
		res.Timeline = res.Timeline[1:]
		res.Dropped++
		res.Truncated = true
	}
	slices.Sort(res.PinnedBy)
	return res
}

// pinners returns the references a timeline would stop judging if next became
// its oldest entry. An unreadable handoff or reference pins, since nothing
// proves it safe to drop.
func pinners(next MariaDBEpoch, refs map[string]string) []string {
	floor, floorErr := parseMariaPos(next.Handoff)
	var out []string
	for name, raw := range refs {
		pos, err := parseMariaPos(raw)
		if err != nil || floorErr != nil {
			out = append(out, name)
			continue
		}
		for domain, e := range pos {
			if f, ok := floor[domain]; ok && e.seq <= f.seq {
				out = append(out, name)
				break
			}
		}
	}
	slices.Sort(out)
	return out
}
