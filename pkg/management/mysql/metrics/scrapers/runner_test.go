/*
Copyright 2026 The CNMySQL Authors.

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
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus"
)

// fakeScraper records whether it ran and lets a test set its minimum version
// and a Scrape error. It is in-package because Scraper.Scrape takes the
// unexported *instance type.
type fakeScraper struct {
	name    string
	version float64
	err     error
	ran     *bool
}

func (f fakeScraper) Name() string     { return f.name }
func (f fakeScraper) Help() string     { return f.name }
func (f fakeScraper) Version() float64 { return f.version }

func (f fakeScraper) Scrape(_ context.Context, _ *instance, _ chan<- prometheus.Metric, _ *slog.Logger) error {
	*f.ran = true
	return f.err
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestRunVersionGatingAndErrorJoin(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery("SELECT @@version").
		WillReturnRows(sqlmock.NewRows([]string{"@@version"}).AddRow("8.0.36-28.1"))

	var supportedRan, futureRan bool
	supported := fakeScraper{name: "supported", version: 5.7, err: errors.New("boom"), ran: &supportedRan}
	future := fakeScraper{name: "future", version: 99.0, ran: &futureRan}

	ch := make(chan prometheus.Metric, 1)
	runErr := Run(context.Background(), db, ch, discardLogger(), []Scraper{supported, future})

	if !supportedRan {
		t.Fatal("supported scraper should have run on 8.0")
	}
	if futureRan {
		t.Fatal("future scraper (v99) should have been gated out on 8.0")
	}
	if runErr == nil || !errors.Is(runErr, supported.err) {
		t.Fatalf("Run error = %v, want it to wrap the supported scraper error", runErr)
	}
}
