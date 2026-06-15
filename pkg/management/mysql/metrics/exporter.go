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

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/metrics/scrapers"
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
