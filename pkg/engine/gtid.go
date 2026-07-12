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

// Ordering is the relationship of one GTID position to another, as computed by
// GTIDModel.Compare. It is the primitive behind two safety-critical decisions:
//   - failover candidate selection: pick the replica that is Ahead of / not
//     Behind the others (the most-advanced survivor, smallest data loss);
//   - errant-transaction / diverged-instance detection: a replica that is
//     Ahead of or Diverged from the primary has transactions the primary lacks
//     and cannot safely rejoin.
type Ordering int

const (
	// OrderingEqual: a and b hold exactly the same transactions.
	OrderingEqual Ordering = iota
	// OrderingAhead: a strictly contains b (a has transactions b lacks; b has
	// none a lacks).
	OrderingAhead
	// OrderingBehind: b strictly contains a.
	OrderingBehind
	// OrderingDiverged: neither contains the other — each side has at least one
	// transaction the other lacks (errant transactions).
	OrderingDiverged
)

// String renders the ordering for logs and status messages.
func (o Ordering) String() string {
	switch o {
	case OrderingEqual:
		return "equal"
	case OrderingAhead:
		return "ahead"
	case OrderingBehind:
		return "behind"
	case OrderingDiverged:
		return "diverged"
	default:
		return "unknown"
	}
}

// GTIDModel interprets an engine's GTID position/set strings. The two engines
// use structurally different formats:
//
//   - MySQL:   UUID-keyed transaction-interval sets, e.g.
//     "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5:8-10,<uuid2>:1-3".
//   - MariaDB: domain-keyed <domain>-<server>-<seq> triples, one per domain,
//     e.g. "0-1-100,1-5-42". There is no server-side GTID_SUBSET() function, so
//     containment is computed here, per domain, by sequence number.
//
// Every method treats "" as the empty position. Strings are stored engine-
// opaque in status (status.gtidExecutedByInstance): only the GTIDModel for the
// cluster's flavor interprets them, so the status schema is flavor-agnostic.
type GTIDModel interface {
	// Contains reports whether superset includes every transaction in subset.
	Contains(superset, subset string) (bool, error)

	// MissingCount returns how many transactions in want are absent from have.
	// Contains reports whether promoting a replica would lose anything at all;
	// this reports how much, which is what the failover bound needs.
	MissingCount(have, want string) (int64, error)

	// Union merges positions into the set of every transaction any of them holds.
	Union(sets ...string) (string, error)

	// Equal reports whether a and b hold exactly the same transactions.
	Equal(a, b string) (bool, error)

	// Compare returns a's relationship to b.
	Compare(a, b string) (Ordering, error)

	// IsEmpty reports whether raw holds no transactions.
	IsEmpty(raw string) (bool, error)

	// Canonical re-parses and re-renders raw in the engine's canonical,
	// comparable form so equal positions have equal strings (stable status,
	// sound string equality).
	Canonical(raw string) (string, error)
}

// orderingFromContainment derives an Ordering from the two directional
// containment results. It is shared by both engines because, given a correct
// per-flavor Contains, the four-way relationship is identical across engines.
func orderingFromContainment(aContainsB, bContainsA bool) Ordering {
	switch {
	case aContainsB && bContainsA:
		return OrderingEqual
	case aContainsB:
		return OrderingAhead
	case bContainsA:
		return OrderingBehind
	default:
		return OrderingDiverged
	}
}
