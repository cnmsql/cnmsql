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
