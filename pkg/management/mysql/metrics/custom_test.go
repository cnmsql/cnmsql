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

package metrics

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/metrics/scrapers"
)

const tableRowsDoc = `
table_rows:
  query: "SELECT table_schema, table_name, table_rows, data_length FROM information_schema.tables"
  metrics:
    - table_schema:
        usage: LABEL
    - table_name:
        usage: LABEL
    - table_rows:
        usage: GAUGE
        description: Estimated rows per table
    - data_length:
        usage: COUNTER
`

type sample struct {
	labels map[string]string
	value  float64
	kind   string
}

// collect drains c and indexes what it emitted by metric name.
func collect(t *testing.T, c prometheus.Collector) map[string][]sample {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	c.Collect(ch)
	close(ch)
	out := map[string][]sample{}
	for m := range ch {
		var d dto.Metric
		if err := m.Write(&d); err != nil {
			t.Fatal(err)
		}
		desc := m.Desc().String()
		name := desc[strings.Index(desc, `fqName: "`)+len(`fqName: "`):]
		name = name[:strings.Index(name, `"`)]
		s := sample{labels: map[string]string{}}
		for _, l := range d.GetLabel() {
			s.labels[l.GetName()] = l.GetValue()
		}
		switch {
		case d.Gauge != nil:
			s.value, s.kind = d.GetGauge().GetValue(), "gauge"
		case d.Counter != nil:
			s.value, s.kind = d.GetCounter().GetValue(), "counter"
		default:
			s.value, s.kind = d.GetUntyped().GetValue(), "untyped"
		}
		out[name] = append(out[name], s)
	}
	return out
}

func TestParseCustomQueries(t *testing.T) {
	t.Parallel()
	queries, err := ParseCustomQueries([]byte(tableRowsDoc))
	if err != nil {
		t.Fatal(err)
	}
	if len(queries) != 1 || queries[0].Name != "table_rows" {
		t.Fatalf("queries = %+v", queries)
	}
	q := queries[0]
	if strings.Join(q.labels, ",") != "table_schema,table_name" {
		t.Fatalf("labels = %v, want declared order", q.labels)
	}
	if got := strings.Join(q.metricNames(), ","); got != "mysql_table_rows_table_rows,mysql_table_rows_data_length" {
		t.Fatalf("metric names = %s", got)
	}
}

func TestParseCustomQueriesRejectsBadDocuments(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"bad usage":       "q:\n  query: SELECT 1 AS v\n  metrics:\n    - v:\n        usage: HISTOGRAM\n",
		"no value column": "q:\n  query: SELECT 1 AS v\n  metrics:\n    - v:\n        usage: LABEL\n",
		"empty query":     "q:\n  query: ''\n  metrics:\n    - v:\n        usage: GAUGE\n",
		"bad name":        "my-query:\n  query: SELECT 1 AS v\n  metrics:\n    - v:\n        usage: GAUGE\n",
		"reserved label": "q:\n  query: SELECT 1 AS v, 'x' AS __name\n  metrics:\n" +
			"    - __name:\n        usage: LABEL\n    - v:\n        usage: GAUGE\n",
		// mysql_global_status_threads is a built-in family.
		"built-in prefix": "global:\n  query: SELECT 1 AS status_threads\n  metrics:\n" +
			"    - status_threads:\n        usage: GAUGE\n",
		"built-in exporter": "exporter:\n  query: SELECT 1 AS last_scrape_error\n  metrics:\n" +
			"    - last_scrape_error:\n        usage: GAUGE\n",
		"duplicate col": "q:\n  query: SELECT 1 AS v\n  metrics:\n" +
			"    - v:\n        usage: GAUGE\n    - v:\n        usage: LABEL\n",
		"unknown field": "q:\n  query: SELECT 1 AS v\n  primary: true\n  metrics:\n    - v:\n        usage: GAUGE\n",
		"not yaml map":  "- just a list\n",
	}
	for name, doc := range cases {
		if _, err := ParseCustomQueries([]byte(doc)); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestCustomQueryScrape(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	queries, err := ParseCustomQueries([]byte(tableRowsDoc))
	if err != nil {
		t.Fatal(err)
	}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT table_schema").WillReturnRows(
		sqlmock.NewRows([]string{"table_schema", "table_name", "table_rows", "data_length"}).
			AddRow("app", "users", "42", "16384").
			AddRow("app", "orders", nil, "8192").
			AddRow("app", "users", "7", "1")) // repeated labels: dropped
	mock.ExpectRollback()

	exp := &Exporter{customDB: db, logger: discardLogger(), config: Config{
		DisableDefaultQueries: true,
		CustomQueries:         queries,
	}}
	got := collect(t, exp)

	rows := got["mysql_table_rows_table_rows"]
	if len(rows) != 1 || rows[0].value != 42 || rows[0].kind != "gauge" ||
		rows[0].labels["table_schema"] != "app" || rows[0].labels["table_name"] != "users" {
		t.Fatalf("table_rows = %+v, want one gauge of 42 for app.users (NULL skipped)", rows)
	}
	if lengths := got["mysql_table_rows_data_length"]; len(lengths) != 2 || lengths[0].kind != "counter" {
		t.Fatalf("data_length = %+v, want two counters", lengths)
	}
	if e := got["mysql_exporter_last_scrape_error"]; len(e) != 1 || e[0].value != 0 {
		t.Fatalf("scrape error = %+v, want 0", e)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCustomQueryMissingColumnFlagsScrapeError(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	queries, err := ParseCustomQueries([]byte(tableRowsDoc))
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT table_schema").WillReturnRows(
		sqlmock.NewRows([]string{"table_schema", "table_rows"}).AddRow("app", "1"))
	mock.ExpectRollback()

	exp := &Exporter{customDB: db, logger: discardLogger(),
		config: Config{DisableDefaultQueries: true, CustomQueries: queries}}
	got := collect(t, exp)
	if e := got["mysql_exporter_last_scrape_error"]; len(e) != 1 || e[0].value != 1 {
		t.Fatalf("scrape error = %+v, want 1", e)
	}
	if len(got["mysql_table_rows_table_rows"]) != 0 {
		t.Fatal("want no metrics from a query missing a label column")
	}
}

// TestExporterTTLReusesResults checks that scrapes inside the TTL replay the
// previous run instead of querying again, and that only a change of queries
// drops the cache.
func TestExporterTTLReusesResults(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	now := time.Unix(1000, 0)
	exp := &Exporter{db: db, scrapers: []scrapers.Scraper{scrapers.ScrapeGlobalStatus{}}, logger: discardLogger(),
		now: func() time.Time { return now }}
	exp.SetConfig(Config{TTL: 30 * time.Second})

	expectStatus := func(threads string) {
		mock.ExpectQuery("SELECT @@version").
			WillReturnRows(sqlmock.NewRows([]string{"@@version"}).AddRow("8.0.36-28.1"))
		mock.ExpectQuery("SHOW GLOBAL STATUS").WillReturnRows(
			sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("Threads_connected", threads))
	}
	threads := func() float64 {
		s := collect(t, exp)["mysql_global_status_threads_connected"]
		if len(s) != 1 {
			t.Fatalf("threads_connected = %+v", s)
		}
		return s[0].value
	}

	expectStatus("1")
	if v := threads(); v != 1 {
		t.Fatalf("first scrape = %v, want 1", v)
	}
	now = now.Add(10 * time.Second)
	if v := threads(); v != 1 {
		t.Fatalf("scrape inside the TTL = %v, want the cached 1", v)
	}
	now = now.Add(30 * time.Second)
	expectStatus("2")
	if v := threads(); v != 2 {
		t.Fatalf("scrape after the TTL = %v, want 2", v)
	}
	// Re-applying the same queries, as the minute resync does, or changing only
	// the TTL keeps the cached run.
	exp.SetConfig(Config{TTL: 30 * time.Second})
	exp.SetConfig(Config{TTL: time.Hour})
	if v := threads(); v != 2 {
		t.Fatalf("scrape after re-applying the config = %v, want the cached 2", v)
	}
	// Changing what runs drops it.
	exp.SetConfig(Config{TTL: time.Hour, DisableDefaultQueries: true})
	if s := collect(t, exp)["mysql_global_status_threads_connected"]; len(s) != 0 {
		t.Fatalf("threads_connected = %+v after disabling the defaults, want none", s)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestExporterDisableDefaultQueries(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	exp := &Exporter{db: db, scrapers: scrapers.Default, logger: discardLogger()}
	exp.SetConfig(Config{DisableDefaultQueries: true})

	got := collect(t, exp)
	if len(got) != 1 || got["mysql_exporter_last_scrape_error"][0].value != 0 {
		t.Fatalf("got %v, want only a clean scrape-error gauge", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestQuerySourceLoadsReferencedDocuments(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	other := "other:\n  query: SELECT 1 AS v\n  metrics:\n    - v:\n        usage: GAUGE\n"
	// Publishes mysql_table_rows_data_length, already owned by table_rows.
	clash := "table:\n  query: SELECT 1 AS rows_data_length\n  metrics:\n    - rows_data_length:\n        usage: GAUGE\n"
	cs := fake.NewClientset(
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "queries", Namespace: "ns"},
			Data: map[string]string{"tables.yaml": tableRowsDoc, "broken.yaml": "q: [", "clash.yaml": clash}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "secret-queries", Namespace: "ns"},
			Data: map[string][]byte{"other.yaml": []byte(other)}},
	)
	exp := NewExporter(nil, nil)
	src := NewQuerySource(cs, "ns", exp)

	cluster := &mysqlv1alpha1.Cluster{}
	cluster.Spec.Monitoring = &mysqlv1alpha1.MonitoringConfiguration{
		CustomQueriesConfigMap: []mysqlv1alpha1.ConfigMapKeySelector{
			{Name: "queries", Key: "tables.yaml"},
			{Name: "queries", Key: "broken.yaml"},
			{Name: "queries", Key: "clash.yaml"},
			{Name: "missing", Key: "x.yaml"},
		},
		CustomQueriesSecret:   []mysqlv1alpha1.SecretKeySelector{{Name: "secret-queries", Key: "other.yaml"}},
		DisableDefaultQueries: new(true),
		MetricsQueriesTTL:     &metav1.Duration{Duration: time.Minute},
	}
	src.Observe(cluster)
	src.apply(ctx)

	cfg := exp.config
	if !cfg.DisableDefaultQueries || cfg.TTL != time.Minute {
		t.Fatalf("config = %+v, want defaults off and a 1m TTL", cfg)
	}
	names := make([]string, 0, len(cfg.CustomQueries))
	for _, q := range cfg.CustomQueries {
		names = append(names, q.Name)
	}
	if strings.Join(names, ",") != "table_rows,other" {
		t.Fatalf("queries = %v, want table_rows,other (broken, clashing and missing sources skipped)", names)
	}

	// A source that stops parsing keeps its last good queries.
	cm, err := cs.CoreV1().ConfigMaps("ns").Get(ctx, "queries", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cm.Data["tables.yaml"] = "not: [valid"
	if _, err := cs.CoreV1().ConfigMaps("ns").Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	src.apply(ctx)
	if q := exp.config.CustomQueries; len(q) != 2 || q[0].Name != "table_rows" {
		t.Fatalf("queries after a bad edit = %+v, want table_rows kept", q)
	}

	// Re-reading unchanged documents keeps the exporter's cached results.
	exp.cached = []prometheus.Metric{}
	src.apply(ctx)
	if exp.cached == nil {
		t.Fatal("resync with unchanged queries dropped the cached results")
	}

	// A deleted source stops publishing instead of keeping its last queries.
	if err := cs.CoreV1().Secrets("ns").Delete(ctx, "secret-queries", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	src.apply(ctx)
	if q := exp.config.CustomQueries; len(q) != 1 || q[0].Name != "table_rows" {
		t.Fatalf("queries after deleting the Secret = %+v, want only table_rows", q)
	}

	// Dropping the references returns to the defaults only.
	src.Observe(&mysqlv1alpha1.Cluster{})
	src.apply(ctx)
	if c := exp.config; c.DisableDefaultQueries || c.TTL != 0 || len(c.CustomQueries) != 0 {
		t.Fatalf("config = %+v, want the defaults", c)
	}
}

// A label value that is not valid UTF-8 (a BINARY column, say) must not fail
// the whole gather: the row is dropped and the scrape is flagged.
func TestCustomQueryInvalidLabelValue(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	queries, err := ParseCustomQueries([]byte(tableRowsDoc))
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT table_schema").WillReturnRows(
		sqlmock.NewRows([]string{"table_schema", "table_name", "table_rows", "data_length"}).
			AddRow("app", []byte{0xff, 0xfe}, "1", "1").
			AddRow("app", "users", "42", "1"))
	mock.ExpectRollback()

	exp := &Exporter{customDB: db, logger: discardLogger(),
		config: Config{DisableDefaultQueries: true, CustomQueries: queries}}
	reg := prometheus.NewRegistry()
	reg.MustRegister(exp)
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather failed: %v", err)
	}
	got := map[string]*dto.MetricFamily{}
	for _, f := range families {
		got[f.GetName()] = f
	}
	if rows := got["mysql_table_rows_table_rows"].GetMetric(); len(rows) != 1 || rows[0].GetGauge().GetValue() != 42 {
		t.Fatalf("table_rows = %v, want only the valid row", rows)
	}
	if e := got["mysql_exporter_last_scrape_error"].GetMetric(); len(e) != 1 || e[0].GetGauge().GetValue() != 1 {
		t.Fatalf("scrape error = %v, want 1", e)
	}
}

// Custom queries never fall back to the control connection.
func TestCustomQueriesNeedTheirOwnConnection(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	queries, err := ParseCustomQueries([]byte(tableRowsDoc))
	if err != nil {
		t.Fatal(err)
	}
	exp := NewExporter(db, nil)
	exp.logger = discardLogger()
	exp.SetConfig(Config{DisableDefaultQueries: true, CustomQueries: queries})
	if e := collect(t, exp)["mysql_exporter_last_scrape_error"]; len(e) != 1 || e[0].value != 1 {
		t.Fatalf("scrape error = %+v, want 1", e)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
