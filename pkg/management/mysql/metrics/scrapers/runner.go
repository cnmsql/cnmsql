/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package scrapers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

// Default is the set of scrapers run on every collection. They are all
// flavor-safe across the Percona MySQL versions we support; version gating in
// Run skips any whose minimum version exceeds the connected server.
var Default = []Scraper{
	ScrapeGlobalStatus{},
	ScrapeGlobalVariables{},
	ScrapeSlaveStatus{},
	ScrapeBinlogSize{},
	ScrapeInnodbCmp{},
	ScrapeInnodbCmpMem{},
	ScrapeQueryResponseTime{},
	ScrapePerfReplicationGroupMemberStats{},
	ScrapePerfReplicationApplierStatsByWorker{},
}

// Run detects the server flavor/version over db, then runs each scraper,
// emitting metrics to ch. db is owned by the caller and is never closed here.
// A scraper whose minimum MySQL version exceeds the server is skipped. Errors
// from individual scrapers are collected and joined so one failing query does
// not suppress the others.
func Run(
	ctx context.Context,
	db *sql.DB,
	ch chan<- prometheus.Metric,
	logger *slog.Logger,
	enabled []Scraper,
) error {
	inst, err := newInstance(db)
	if err != nil {
		return fmt.Errorf("detect instance: %w", err)
	}

	var errs []error
	for _, s := range enabled {
		if s.Version() > inst.versionMajorMinor {
			continue
		}
		if err := s.Scrape(ctx, inst, ch, logger); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", s.Name(), err))
		}
	}
	return errors.Join(errs...)
}
