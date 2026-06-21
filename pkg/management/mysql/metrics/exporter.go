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
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/metrics/scrapers"
)

const namespace = "mysql"

var scrapeErrorDesc = prometheus.NewDesc(
	prometheus.BuildFQName(namespace, "exporter", "last_scrape_error"),
	"Whether the last scrape of MySQL metrics resulted in an error (1 for error, 0 for success).",
	nil, nil,
)

// Exporter collects MySQL metrics from a local mysqld connection by running the
// vendored mysqld_exporter scrapers on every Prometheus scrape.
type Exporter struct {
	db       *sql.DB
	scrapers []scrapers.Scraper
	logger   *slog.Logger
}

// NewExporter builds a Prometheus collector backed by db, running the default
// scraper set.
func NewExporter(db *sql.DB) *Exporter {
	return &Exporter{
		db:       db,
		scrapers: scrapers.Default,
		logger:   slog.Default(),
	}
}

// Describe implements prometheus.Collector. The scrapers emit dynamic metrics
// discovered from MySQL rows, so the exporter is intentionally unchecked; only
// the fixed scrape-status descriptors are advertised.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- scrapeErrorDesc
}

// Collect implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	err := scrapers.Run(context.Background(), e.db, ch, e.logger, e.scrapers)
	scrapeError := 0.0
	if err != nil {
		scrapeError = 1
		e.logger.Error("MySQL metrics scrape failed", "err", err)
	}
	ch <- prometheus.MustNewConstMetric(scrapeErrorDesc, prometheus.GaugeValue, scrapeError)
}
