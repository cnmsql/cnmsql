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

package metrics

import (
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/metrics/scrapers"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestExporterCollectsGlobalStatus drives the exporter through the vendored
// ScrapeGlobalStatus collector and asserts the well-known global-status metrics
// surface, plus the exporter's own scrape-error gauge.
func TestExporterCollectsGlobalStatus(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Run() first detects the server version, then the single scraper runs.
	mock.ExpectQuery("SELECT @@version").
		WillReturnRows(sqlmock.NewRows([]string{"@@version"}).AddRow("8.0.36-28.1"))
	mock.ExpectQuery("SHOW GLOBAL STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("Threads_connected", "12").
			AddRow("Uptime", "300"))

	exp := &Exporter{db: db, scrapers: []scrapers.Scraper{scrapers.ScrapeGlobalStatus{}}, logger: discardLogger()}

	ch := make(chan prometheus.Metric, 32)
	exp.Collect(ch)
	close(ch)

	got := map[string]float64{}
	for metric := range ch {
		var dtoMetric dto.Metric
		if err := metric.Write(&dtoMetric); err != nil {
			t.Fatal(err)
		}
		desc := metric.Desc().String()
		switch {
		case strings.Contains(desc, "mysql_global_status_threads_connected"):
			got["threads"] = dtoMetric.GetUntyped().GetValue()
		case strings.Contains(desc, "mysql_global_status_uptime"):
			got["uptime"] = dtoMetric.GetUntyped().GetValue()
		case strings.Contains(desc, "mysql_exporter_last_scrape_error"):
			got["error"] = dtoMetric.GetGauge().GetValue()
		}
	}

	if got["threads"] != 12 {
		t.Fatalf("threads metric = %v, want 12", got["threads"])
	}
	if got["uptime"] != 300 {
		t.Fatalf("uptime metric = %v, want 300", got["uptime"])
	}
	if got["error"] != 0 {
		t.Fatalf("scrape-error metric = %v, want 0", got["error"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestExporterReportsScrapeError verifies the exporter still emits the
// scrape-error gauge set to 1 when version detection fails.
func TestExporterReportsScrapeError(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery("SELECT @@version").WillReturnError(errors.New("connection refused"))

	exp := &Exporter{db: db, scrapers: scrapers.Default, logger: discardLogger()}
	ch := make(chan prometheus.Metric, 8)
	exp.Collect(ch)
	close(ch)

	var sawError bool
	for metric := range ch {
		if strings.Contains(metric.Desc().String(), "mysql_exporter_last_scrape_error") {
			var dtoMetric dto.Metric
			if err := metric.Write(&dtoMetric); err != nil {
				t.Fatal(err)
			}
			if dtoMetric.GetGauge().GetValue() != 1 {
				t.Fatalf("scrape-error gauge = %v, want 1", dtoMetric.GetGauge().GetValue())
			}
			sawError = true
		}
	}
	if !sawError {
		t.Fatal("expected a scrape-error metric")
	}
}
