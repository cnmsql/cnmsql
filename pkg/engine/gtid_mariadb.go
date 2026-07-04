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

// mariadbGTID is the MariaDB GTID model.
//
// A MariaDB GTID position (the value of @@gtid_binlog_pos, @@gtid_current_pos
// or @@gtid_slave_pos) is a comma-separated list of <domain>-<server>-<seq>
// triples with at most one entry per replication domain — the highest sequence
// number reached in that domain. Example: "0-1-100,1-5-42".
//
// Unlike MySQL there is no server-side GTID_SUBSET(); MariaDB orders positions
// per domain by sequence number, and so does this model. Two positions that
// share a domain with the same sequence number represent the same amount of
// history in that domain even if the server-id component differs (the server-id
// records who wrote the latest event, not a distinct transaction), which
// matches MariaDB's own MASTER_GTID_WAIT / slave_pos comparison semantics.
type mariadbGTID struct{}

// mariaPos is a parsed MariaDB position: the reached sequence number per domain
// (plus the server-id, retained only for canonical rendering).
type mariaPos map[uint32]mariaEntry

type mariaEntry struct {
	server uint32
	seq    uint64
}

// parseMariaPos parses a MariaDB GTID position string. Whitespace and newlines
// are ignored; an empty string parses to the empty position. When a domain
// appears more than once the entry with the higher sequence number wins, so the
// result is always a canonical one-per-domain position.
func parseMariaPos(raw string) (mariaPos, error) {
	pos := mariaPos{}
	cleaned := strings.Map(func(r rune) rune {
		switch r {
		case ' ', '\n', '\r', '\t':
			return -1
		default:
			return r
		}
	}, raw)
	if cleaned == "" {
		return pos, nil
	}
	for triple := range strings.SplitSeq(cleaned, ",") {
		parts := strings.Split(triple, "-")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid mariadb gtid %q in position %q: want domain-server-seq", triple, raw)
		}
		domain, err := strconv.ParseUint(parts[0], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid mariadb gtid domain in %q: %w", triple, err)
		}
		server, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid mariadb gtid server_id in %q: %w", triple, err)
		}
		seq, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid mariadb gtid sequence in %q: %w", triple, err)
		}
		if existing, ok := pos[uint32(domain)]; !ok || seq >= existing.seq {
			pos[uint32(domain)] = mariaEntry{server: uint32(server), seq: seq}
		}
	}
	return pos, nil
}

// contains reports whether super includes every transaction in sub: for each
// domain in sub, super must have reached at least the same sequence number.
func (super mariaPos) contains(sub mariaPos) bool {
	for domain, e := range sub {
		mine, ok := super[domain]
		if !ok || mine.seq < e.seq {
			return false
		}
	}
	return true
}

func (p mariaPos) isEmpty() bool { return len(p) == 0 }

// string renders the position in canonical form: domains ascending, joined by
// commas. The empty position renders to the empty string.
func (p mariaPos) string() string {
	domains := make([]uint32, 0, len(p))
	for d := range p {
		domains = append(domains, d)
	}
	slices.Sort(domains)
	var b strings.Builder
	for i, d := range domains {
		if i > 0 {
			b.WriteByte(',')
		}
		e := p[d]
		b.WriteString(strconv.FormatUint(uint64(d), 10))
		b.WriteByte('-')
		b.WriteString(strconv.FormatUint(uint64(e.server), 10))
		b.WriteByte('-')
		b.WriteString(strconv.FormatUint(e.seq, 10))
	}
	return b.String()
}

func (mariadbGTID) Contains(superset, subset string) (bool, error) {
	super, err := parseMariaPos(superset)
	if err != nil {
		return false, err
	}
	sub, err := parseMariaPos(subset)
	if err != nil {
		return false, err
	}
	return super.contains(sub), nil
}

func (mariadbGTID) Equal(a, b string) (bool, error) {
	posA, err := parseMariaPos(a)
	if err != nil {
		return false, err
	}
	posB, err := parseMariaPos(b)
	if err != nil {
		return false, err
	}
	return posA.contains(posB) && posB.contains(posA), nil
}

func (mariadbGTID) Compare(a, b string) (Ordering, error) {
	posA, err := parseMariaPos(a)
	if err != nil {
		return OrderingEqual, err
	}
	posB, err := parseMariaPos(b)
	if err != nil {
		return OrderingEqual, err
	}
	return orderingFromContainment(posA.contains(posB), posB.contains(posA)), nil
}

func (mariadbGTID) IsEmpty(raw string) (bool, error) {
	pos, err := parseMariaPos(raw)
	if err != nil {
		return false, err
	}
	return pos.isEmpty(), nil
}

func (mariadbGTID) Canonical(raw string) (string, error) {
	pos, err := parseMariaPos(raw)
	if err != nil {
		return "", err
	}
	return pos.string(), nil
}
