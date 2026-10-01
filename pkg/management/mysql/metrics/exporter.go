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

// Package metrics exposes MySQL instance metrics in Prometheus format. The
// per-metric collectors are vendored from github.com/prometheus/mysqld_exporter
// (see the scrapers subpackage); this file orchestrates them as a single
// prometheus.Collector backed by the instance manager's connection.
package metrics

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/metrics/scrapers"
)

const namespace = "mysql"

var scrapeErrorDesc = prometheus.NewDesc(
	prometheus.BuildFQName(namespace, "exporter", "last_scrape_error"),
	"Whether the last scrape of MySQL metrics resulted in an error (1 for error, 0 for success).",
	nil, nil,
)

// Config selects what the exporter runs on a scrape.
type Config struct {
	// DisableDefaultQueries turns off the built-in scrapers.
	DisableDefaultQueries bool
	// CustomQueries run after the built-in scrapers.
	CustomQueries []CustomQuery
	// TTL is the minimum interval between two runs of the queries. Scrapes
	// inside it are served the previous results. Zero runs them every scrape.
	TTL time.Duration
}

// Exporter collects MySQL metrics from a local mysqld connection by running the
// vendored mysqld_exporter scrapers, then any custom queries, on Prometheus
// scrapes.
type Exporter struct {
	db       *sql.DB
	scrapers []scrapers.Scraper
	logger   *slog.Logger
	now      func() time.Time

	// mu serialises scrapes, so concurrent ones share one run within the TTL.
	mu       sync.Mutex
	config   Config
	cached   []prometheus.Metric
	cachedAt time.Time
}

// NewExporter builds a Prometheus collector backed by db, running the default
// scraper set until SetConfig says otherwise.
func NewExporter(db *sql.DB) *Exporter {
	return &Exporter{
		db:       db,
		scrapers: scrapers.Default,
		logger:   slog.Default(),
	}
}

// SetConfig replaces what the next scrape runs and drops any cached results.
func (e *Exporter) SetConfig(c Config) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.config = c
	e.cached = nil
}

// Describe implements prometheus.Collector. The scrapers emit dynamic metrics
// discovered from MySQL rows, so the exporter is intentionally unchecked; only
// the fixed scrape-status descriptors are advertised.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- scrapeErrorDesc
}

// Collect implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now
	if e.now != nil {
		now = e.now
	}
	if e.cached == nil || e.config.TTL <= 0 || now().Sub(e.cachedAt) >= e.config.TTL {
		e.cached = e.scrape()
		e.cachedAt = now()
	}
	for _, m := range e.cached {
		ch <- m
	}
}

// scrape runs every enabled query and returns what they emitted, ending with
// the scrape-error gauge.
func (e *Exporter) scrape() []prometheus.Metric {
	ctx := context.Background()
	buf := make(chan prometheus.Metric)
	done := make(chan []prometheus.Metric)
	go func() {
		var out []prometheus.Metric
		for m := range buf {
			out = append(out, m)
		}
		done <- out
	}()

	var errs []error
	if !e.config.DisableDefaultQueries {
		errs = append(errs, scrapers.Run(ctx, e.db, buf, e.logger, e.scrapers))
	}
	for _, q := range e.config.CustomQueries {
		if err := q.Scrape(ctx, e.db, buf); err != nil {
			errs = append(errs, fmt.Errorf("custom query %s: %w", q.Name, err))
		}
	}
	close(buf)
	out := <-done

	scrapeError := 0.0
	if err := errors.Join(errs...); err != nil {
		scrapeError = 1
		e.logger.Error("MySQL metrics scrape failed", "err", err)
	}
	return append(out, prometheus.MustNewConstMetric(scrapeErrorDesc, prometheus.GaugeValue, scrapeError))
}
