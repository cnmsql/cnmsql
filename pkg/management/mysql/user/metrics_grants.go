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

package user

import (
	"fmt"
	"slices"
	"strings"
)

// MetricsGrantPlan is what it takes to bring the metrics account's grants to
// base ∪ declared: the statements to run, grants first, and a short summary
// of each change for events and logs.
type MetricsGrantPlan struct {
	Grants  []string
	Revokes []string
	Granted []string
	Revoked []string
}

// privilegeAliases maps the names a server prints for a base privilege to the
// name the base list uses. MariaDB 10.5+ prints REPLICATION CLIENT as BINLOG
// MONITOR and may add SLAVE MONITOR; MySQL accepts REPLICATION REPLICA.
var privilegeAliases = map[string]string{
	"binlog monitor":      "replication client",
	"slave monitor":       "replication client",
	"replication replica": "replication slave",
}

// grantPair is one privilege on one target. The text fields keep what the
// server printed (or what the caller declared) so a REVOKE reuses it exactly;
// key is the normalized form used for comparison. Privilege names compare
// case-insensitively, targets case-sensitively, as identifiers are on Linux.
type grantPair struct {
	privilege string
	target    string
	key       string
}

func newGrantPair(privilege, target string) grantPair {
	priv := strings.ToLower(strings.Join(strings.Fields(strings.ReplaceAll(privilege, "`", "")), " "))
	if alias, ok := privilegeAliases[priv]; ok {
		priv = alias
	}
	target = strings.TrimSpace(target)
	return grantPair{
		privilege: strings.TrimSpace(privilege),
		target:    target,
		key:       priv + "@" + strings.ReplaceAll(target, "`", ""),
	}
}

// parsedGrant is one SHOW GRANTS line: privileges on a target, or roles.
type parsedGrant struct {
	privileges  []string
	target      string
	roles       string
	grantOption bool
}

// parseShowGrant parses a "GRANT ... TO ..." line. Other lines (partial
// revokes, SET DEFAULT ROLE) are reported as not ok.
func parseShowGrant(line string) (parsedGrant, bool) {
	line = strings.TrimSpace(line)
	if len(line) < len("GRANT ") || !strings.EqualFold(line[:len("GRANT ")], "GRANT ") {
		return parsedGrant{}, false
	}
	body := line[len("GRANT "):]
	to := indexTopLevel(body, " TO ")
	if to < 0 {
		return parsedGrant{}, false
	}
	head, tail := body[:to], body[to+len(" TO "):]
	g := parsedGrant{grantOption: strings.HasSuffix(strings.ToUpper(tail), " WITH GRANT OPTION")}
	on := indexTopLevel(head, " ON ")
	if on < 0 {
		g.roles = strings.TrimSpace(head)
		return g, true
	}
	g.target = strings.TrimSpace(head[on+len(" ON "):])
	for _, p := range splitTopLevel(head[:on], ',') {
		if p = strings.TrimSpace(p); p != "" {
			g.privileges = append(g.privileges, p)
		}
	}
	return g, true
}

// indexTopLevel returns the index of the first case-insensitive occurrence of
// sep outside backticks, quotes and parentheses, or -1.
func indexTopLevel(s, sep string) int {
	var quote byte
	depth := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '`' || c == '\'' || c == '"':
			quote = c
		case c == '(':
			depth++
		case c == ')':
			depth--
		case depth == 0 && i+len(sep) <= len(s) && strings.EqualFold(s[i:i+len(sep)], sep):
			return i
		}
	}
	return -1
}

// splitTopLevel splits s on sep outside backticks, quotes and parentheses.
func splitTopLevel(s string, sep byte) []string {
	var parts []string
	var quote byte
	depth, start := 0, 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '`' || c == '\'' || c == '"':
			quote = c
		case c == '(':
			depth++
		case c == ')':
			depth--
		case c == sep && depth == 0:
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	return append(parts, s[start:])
}

// PlanMetricsGrants compares the account's SHOW GRANTS output with base ∪
// declared. Missing pairs are granted; observed pairs that are neither base
// nor declared are revoked, and so are granted roles. Base pairs are never
// revoked. USAGE and non-GRANT lines are ignored. Statements are grouped per
// target in first-seen order.
func PlanMetricsGrants(name, host string, observed []string, base, declared []Privilege) MetricsGrantPlan {
	acct := account(name, host)
	baseKeys := pairKeys(base)
	wantKeys := pairKeys(declared)

	have := map[string]bool{}
	var extras []grantPair
	var roles []string
	for _, line := range observed {
		g, ok := parseShowGrant(line)
		if !ok {
			continue
		}
		if g.roles != "" {
			roles = append(roles, g.roles)
			continue
		}
		privs := g.privileges
		if g.grantOption {
			privs = append(slices.Clone(privs), "GRANT OPTION")
		}
		for _, priv := range privs {
			pair := newGrantPair(priv, g.target)
			if strings.HasPrefix(pair.key, "usage@") {
				continue
			}
			have[pair.key] = true
			if !baseKeys[pair.key] && !wantKeys[pair.key] {
				extras = append(extras, pair)
			}
		}
	}

	var missing []grantPair
	for _, p := range append(slices.Clone(base), declared...) {
		for _, priv := range p.Privileges {
			pair := newGrantPair(priv, p.On)
			if !have[pair.key] {
				have[pair.key] = true
				missing = append(missing, pair)
			}
		}
	}

	var plan MetricsGrantPlan
	for _, group := range groupByTarget(missing) {
		plan.Grants = append(plan.Grants, fmt.Sprintf("GRANT %s ON %s TO %s",
			strings.Join(group.privileges, ", "), group.target, acct))
		plan.Granted = append(plan.Granted, group.summaries()...)
	}
	for _, group := range groupByTarget(extras) {
		plan.Revokes = append(plan.Revokes, fmt.Sprintf("REVOKE %s ON %s FROM %s",
			strings.Join(group.privileges, ", "), group.target, acct))
		plan.Revoked = append(plan.Revoked, group.summaries()...)
	}
	for _, r := range roles {
		plan.Revokes = append(plan.Revokes, fmt.Sprintf("REVOKE %s FROM %s", r, acct))
		plan.Revoked = append(plan.Revoked, "role "+r)
	}
	return plan
}

func pairKeys(privileges []Privilege) map[string]bool {
	keys := map[string]bool{}
	for _, p := range privileges {
		for _, priv := range p.Privileges {
			keys[newGrantPair(priv, p.On).key] = true
		}
	}
	return keys
}

type targetGroup struct {
	target     string
	privileges []string
}

// groupByTarget gathers pairs per target, keeping first-seen order.
func groupByTarget(pairs []grantPair) []targetGroup {
	var groups []targetGroup
	index := map[string]int{}
	for _, p := range pairs {
		i, ok := index[p.target]
		if !ok {
			i = len(groups)
			index[p.target] = i
			groups = append(groups, targetGroup{target: p.target})
		}
		groups[i].privileges = append(groups[i].privileges, p.privilege)
	}
	return groups
}

func (g targetGroup) summaries() []string {
	out := make([]string, 0, len(g.privileges))
	for _, p := range g.privileges {
		out = append(out, p+" ON "+g.target)
	}
	return out
}
