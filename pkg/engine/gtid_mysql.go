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

import "github.com/cnmsql/cnmsql/pkg/management/mysql/replication"

// mysqlGTID is the MySQL GTID model. MySQL positions are UUID-keyed interval
// sets; the operator already has a well-tested operator-side implementation in
// pkg/management/mysql/replication, so this delegates to it rather than
// duplicating the parser. (The later "engine extraction" milestone may relocate
// that code under engine; keeping a single implementation now is the low-risk
// path.)
type mysqlGTID struct{}

func (mysqlGTID) Contains(superset, subset string) (bool, error) {
	return replication.GTIDContains(superset, subset)
}

func (mysqlGTID) MissingCount(have, want string) (int64, error) {
	setHave, err := replication.ParseGTIDSet(have)
	if err != nil {
		return 0, err
	}
	setWant, err := replication.ParseGTIDSet(want)
	if err != nil {
		return 0, err
	}
	return setHave.MissingCount(setWant), nil
}

func (mysqlGTID) Union(sets ...string) (string, error) {
	return replication.UnionGTIDStrings(sets...)
}

func (mysqlGTID) Equal(a, b string) (bool, error) {
	setA, err := replication.ParseGTIDSet(a)
	if err != nil {
		return false, err
	}
	setB, err := replication.ParseGTIDSet(b)
	if err != nil {
		return false, err
	}
	return setA.Equal(setB), nil
}

func (m mysqlGTID) Compare(a, b string) (Ordering, error) {
	setA, err := replication.ParseGTIDSet(a)
	if err != nil {
		return OrderingEqual, err
	}
	setB, err := replication.ParseGTIDSet(b)
	if err != nil {
		return OrderingEqual, err
	}
	return orderingFromContainment(setA.Contains(setB), setB.Contains(setA)), nil
}

func (mysqlGTID) IsEmpty(raw string) (bool, error) {
	set, err := replication.ParseGTIDSet(raw)
	if err != nil {
		return false, err
	}
	return set.IsEmpty(), nil
}

func (mysqlGTID) Canonical(raw string) (string, error) {
	set, err := replication.ParseGTIDSet(raw)
	if err != nil {
		return "", err
	}
	return set.String(), nil
}
