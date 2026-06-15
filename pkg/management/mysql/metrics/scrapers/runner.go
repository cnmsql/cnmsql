/*
Copyright 2026 The cloudnative-mysql Authors.

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
