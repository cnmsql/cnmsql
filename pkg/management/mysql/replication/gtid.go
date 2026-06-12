/*
Copyright 2026 The CNMySQL Authors.

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

package replication

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// GTIDInterval is an inclusive transaction-number range [Start, End].
type GTIDInterval struct {
	Start int64
	End   int64
}

// GTIDSet is a parsed MySQL GTID set keyed by source UUID. Each source's
// intervals are kept sorted and coalesced so containment checks are simple.
type GTIDSet map[string][]GTIDInterval

// ParseGTIDSet parses a MySQL GTID set such as
// "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5:8-10,uuid2:1-3". Whitespace and
// newlines (mysqld wraps long sets) are ignored. An empty string parses to an
// empty set.
func ParseGTIDSet(raw string) (GTIDSet, error) {
	set := GTIDSet{}
	cleaned := strings.Map(func(r rune) rune {
		switch r {
		case ' ', '\n', '\r', '\t':
			return -1
		default:
			return r
		}
	}, raw)
	if cleaned == "" {
		return set, nil
	}
	for uuidSet := range strings.SplitSeq(cleaned, ",") {
		parts := strings.Split(uuidSet, ":")
		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid gtid set %q: missing intervals in %q", raw, uuidSet)
		}
		uuid := strings.ToLower(parts[0])
		if uuid == "" {
			return nil, fmt.Errorf("invalid gtid set %q: empty uuid", raw)
		}
		for _, ivStr := range parts[1:] {
			iv, err := parseInterval(ivStr)
			if err != nil {
				return nil, fmt.Errorf("invalid gtid set %q: %w", raw, err)
			}
			set[uuid] = append(set[uuid], iv)
		}
	}
	for uuid := range set {
		set[uuid] = normalizeIntervals(set[uuid])
	}
	return set, nil
}

func parseInterval(s string) (GTIDInterval, error) {
	lo, hi, ranged := strings.Cut(s, "-")
	start, err := strconv.ParseInt(lo, 10, 64)
	if err != nil {
		return GTIDInterval{}, fmt.Errorf("bad interval %q", s)
	}
	end := start
	if ranged {
		end, err = strconv.ParseInt(hi, 10, 64)
		if err != nil {
			return GTIDInterval{}, fmt.Errorf("bad interval %q", s)
		}
	}
	if start < 1 || end < start {
		return GTIDInterval{}, fmt.Errorf("bad interval %q", s)
	}
	return GTIDInterval{Start: start, End: end}, nil
}

// normalizeIntervals sorts intervals by start and coalesces overlapping or
// adjacent ranges so each transaction range is represented exactly once.
func normalizeIntervals(in []GTIDInterval) []GTIDInterval {
	if len(in) <= 1 {
		return in
	}
	sort.Slice(in, func(i, j int) bool { return in[i].Start < in[j].Start })
	out := []GTIDInterval{in[0]}
	for _, iv := range in[1:] {
		last := &out[len(out)-1]
		if iv.Start <= last.End+1 {
			if iv.End > last.End {
				last.End = iv.End
			}
			continue
		}
		out = append(out, iv)
	}
	return out
}

// IsEmpty reports whether the set holds no transactions.
func (s GTIDSet) IsEmpty() bool {
	for _, intervals := range s {
		if len(intervals) > 0 {
			return false
		}
	}
	return true
}

// Contains reports whether s is a superset of other: every transaction in
// other is also present in s.
func (s GTIDSet) Contains(other GTIDSet) bool {
	for uuid, intervals := range other {
		mine, ok := s[uuid]
		if !ok {
			return len(intervals) == 0
		}
		for _, iv := range intervals {
			if !intervalsCover(mine, iv) {
				return false
			}
		}
	}
	return true
}

// Equal reports whether the two sets contain exactly the same transactions.
func (s GTIDSet) Equal(other GTIDSet) bool {
	return s.Contains(other) && other.Contains(s)
}

// intervalsCover reports whether iv is fully covered by a single coalesced
// interval. Because mine is normalized, a range is covered iff one interval
// covers it.
func intervalsCover(mine []GTIDInterval, iv GTIDInterval) bool {
	for _, m := range mine {
		if m.Start <= iv.Start && iv.End <= m.End {
			return true
		}
	}
	return false
}

// Clone returns a deep copy of the set so the original can be mutated
// independently.
func (s GTIDSet) Clone() GTIDSet {
	out := make(GTIDSet, len(s))
	for uuid, intervals := range s {
		cp := make([]GTIDInterval, len(intervals))
		copy(cp, intervals)
		out[uuid] = cp
	}
	return out
}

// AddInterval merges an interval for the given source UUID into the set,
// re-normalizing so the result stays sorted and coalesced.
func (s GTIDSet) AddInterval(uuid string, iv GTIDInterval) {
	uuid = strings.ToLower(uuid)
	s[uuid] = normalizeIntervals(append(s[uuid], iv))
}

// Union merges every transaction in other into s.
func (s GTIDSet) Union(other GTIDSet) {
	for uuid, intervals := range other {
		for _, iv := range intervals {
			s.AddInterval(uuid, iv)
		}
	}
}

// String renders the set in canonical MySQL form
// ("uuid:1-5:8-10,uuid2:1-3"), sources sorted by UUID. An empty set renders to
// the empty string.
func (s GTIDSet) String() string {
	uuids := make([]string, 0, len(s))
	for uuid, intervals := range s {
		if len(intervals) > 0 {
			uuids = append(uuids, uuid)
		}
	}
	sort.Strings(uuids)
	var b strings.Builder
	for i, uuid := range uuids {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(uuid)
		for _, iv := range s[uuid] {
			b.WriteByte(':')
			if iv.Start == iv.End {
				b.WriteString(strconv.FormatInt(iv.Start, 10))
			} else {
				b.WriteString(strconv.FormatInt(iv.Start, 10))
				b.WriteByte('-')
				b.WriteString(strconv.FormatInt(iv.End, 10))
			}
		}
	}
	return b.String()
}

// UnionGTIDStrings parses and merges any number of GTID set strings into a
// single canonical set string. A parse error on any input is returned.
func UnionGTIDStrings(sets ...string) (string, error) {
	union := GTIDSet{}
	for _, raw := range sets {
		parsed, err := ParseGTIDSet(raw)
		if err != nil {
			return "", err
		}
		union.Union(parsed)
	}
	return union.String(), nil
}

// GTIDContains reports whether the superset GTID string fully contains the
// subset string. Both are parsed; a parse error is returned.
func GTIDContains(superset, subset string) (bool, error) {
	super, err := ParseGTIDSet(superset)
	if err != nil {
		return false, err
	}
	sub, err := ParseGTIDSet(subset)
	if err != nil {
		return false, err
	}
	return super.Contains(sub), nil
}
