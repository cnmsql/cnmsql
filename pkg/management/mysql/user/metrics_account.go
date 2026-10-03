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
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/cnmsql/cnmsql/pkg/engine"
)

// MetricsAccountRequest is the body of POST /monitoring/account: the extra
// grants the metrics account should hold on top of its base grants.
type MetricsAccountRequest struct {
	Privileges []Privilege `json:"privileges,omitempty"`
}

// MetricsAccountResponse reports what POST /monitoring/account changed.
type MetricsAccountResponse struct {
	Created bool     `json:"created"`
	Granted []string `json:"granted,omitempty"`
	Revoked []string `json:"revoked,omitempty"`
}

// InvalidRequestError marks a request the server refused without running any
// SQL. The control API answers it with 400.
type InvalidRequestError struct {
	Err error
}

func (e *InvalidRequestError) Error() string { return e.Err.Error() }

func (e *InvalidRequestError) Unwrap() error { return e.Err }

// EnsureMetricsAccount makes the local metrics account exist and hold exactly
// its base grants plus req.Privileges. It creates the account when missing,
// compares SHOW GRANTS with the desired set, and runs only the GRANT and
// REVOKE statements needed, so an account already in sync costs no binlog
// event. Base grants are never revoked.
func (m *Manager) EnsureMetricsAccount(
	ctx context.Context,
	name string,
	req MetricsAccountRequest,
) (*MetricsAccountResponse, error) {
	if name == "" {
		return nil, errors.New("no metrics account is configured on this instance")
	}
	if _, err := metricsDeclaredGrants(req.Privileges, false); err != nil {
		return nil, &InvalidRequestError{Err: err}
	}
	host := engine.MetricsAccountHost
	resp := &MetricsAccountResponse{}

	// With lower_case_table_names set, the server stores and prints names in
	// lower case, so the declared targets have to compare that way too.
	var lowerCaseTableNames int
	if err := m.conn.QueryRowContext(ctx,
		"SELECT @@GLOBAL.lower_case_table_names").Scan(&lowerCaseTableNames); err != nil {
		return nil, fmt.Errorf("reading lower_case_table_names: %w", err)
	}
	declared, err := metricsDeclaredGrants(req.Privileges, lowerCaseTableNames != 0)
	if err != nil {
		return nil, &InvalidRequestError{Err: err}
	}

	var count int
	if err := m.conn.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM mysql.user WHERE User = ? AND Host = ?", name, host).Scan(&count); err != nil {
		return nil, fmt.Errorf("looking up the metrics account: %w", err)
	}
	if count == 0 {
		if err := m.exec(ctx, "CREATE USER IF NOT EXISTS "+account(name, host)); err != nil {
			return nil, err
		}
		resp.Created = true
	}

	observed, err := m.showGrants(ctx, name, host)
	if err != nil {
		return nil, err
	}
	var base []Privilege
	for _, g := range engine.MetricsAccountBaseGrants() {
		base = append(base, Privilege{Privileges: g.Privileges, On: g.On})
	}
	plan := PlanMetricsGrants(name, host, observed, base, declared)
	// Revokes run first and every statement is tried: a grant that cannot be
	// applied yet, such as one on a table that does not exist, must not keep
	// an out-of-band grant in place or hold back the other grants.
	var errs []error
	for _, stmt := range append(plan.Revokes, plan.Grants...) {
		if err := m.execAll(ctx, []string{stmt}); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	resp.Granted, resp.Revoked = plan.Granted, plan.Revoked
	return resp, nil
}

// metricsDeclaredGrants validates the requested grants with the same rules as
// the Cluster webhook, upper-cases the privilege names and renders the
// targets as quoted SQL, in lower case when lowerCase is set.
func metricsDeclaredGrants(in []Privilege, lowerCase bool) ([]Privilege, error) {
	out := make([]Privilege, 0, len(in))
	for i, p := range in {
		if err := engine.ValidateMetricsPrivilegeNames(p.Privileges); err != nil {
			return nil, fmt.Errorf("privileges[%d]: %w", i, err)
		}
		target, err := engine.ParseMetricsGrantTarget(p.On)
		if err != nil {
			return nil, fmt.Errorf("privileges[%d]: %w", i, err)
		}
		if lowerCase {
			target.Database, target.Table = strings.ToLower(target.Database), strings.ToLower(target.Table)
		}
		names := make([]string, 0, len(p.Privileges))
		for _, name := range p.Privileges {
			names = append(names, strings.ToUpper(strings.Join(strings.Fields(name), " ")))
		}
		out = append(out, Privilege{Privileges: names, On: target.SQL()})
	}
	return out, nil
}
