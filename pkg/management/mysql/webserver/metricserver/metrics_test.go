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

package metricserver

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// brokenCollector emits one good metric and one that fails to encode.
type brokenCollector struct{}

var goodDesc = prometheus.NewDesc("test_good", "A metric that encodes.", nil, nil)

func (brokenCollector) Describe(chan<- *prometheus.Desc) {}

func (brokenCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(goodDesc, prometheus.GaugeValue, 1)
	ch <- prometheus.NewInvalidMetric(prometheus.NewDesc("test_bad", "A metric that fails.", nil, nil),
		errors.New("broken"))
}

// One collector failing must not blank the page: the other metrics are
// still served.
func TestMetricsServeDespiteAFailingCollector(t *testing.T) {
	t.Parallel()
	srv := New(":0", nil, brokenCollector{})
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	if rec.Code != http.StatusOK || !strings.Contains(string(body), "test_good 1") {
		t.Fatalf("status %d, body:\n%s\nwant 200 with test_good", rec.Code, body)
	}
}
