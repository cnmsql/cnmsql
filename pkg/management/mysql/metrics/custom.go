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
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/yaml"
)

// ColumnUsage says what a custom query does with a result column.
type ColumnUsage string

const (
	// UsageLabel turns the column into a label on every metric of the row.
	UsageLabel ColumnUsage = "LABEL"
	// UsageGauge publishes the column as a gauge.
	UsageGauge ColumnUsage = "GAUGE"
	// UsageCounter publishes the column as a counter.
	UsageCounter ColumnUsage = "COUNTER"
	// UsageDiscard ignores the column. Unlisted columns are ignored too.
	UsageDiscard ColumnUsage = "DISCARD"
)

var metricNamePart = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// builtinSubsystems are the metric name prefixes, after mysql_, that the
// built-in scrapers and collectors publish. A custom metric under one of them
// could collide with a built-in one, and the registry would then reject the
// whole scrape.
var builtinSubsystems = []string{
	"binlog", "exporter", "galera", "global_status", "global_variables", "info_schema",
	"instance", "perf_schema", "slave_status", "transaction", "version",
}

// ColumnMapping describes one result column of a custom query.
type ColumnMapping struct {
	Usage       ColumnUsage `json:"usage"`
	Description string      `json:"description,omitempty"`
}

// QuerySpec is one entry of a custom queries document. Metrics is a list of
// single-key maps, column name to mapping, so the label order is the order the
// user wrote.
type QuerySpec struct {
	Query   string                     `json:"query"`
	Metrics []map[string]ColumnMapping `json:"metrics"`
}

// CustomQuery is a parsed, validated custom query, ready to run.
type CustomQuery struct {
	Name   string
	query  string
	labels []string
	values []customValue
}

type customValue struct {
	column    string
	valueType prometheus.ValueType
	desc      *prometheus.Desc
}

// ParseCustomQueries reads a custom queries document: a YAML map of query name
// to QuerySpec. Each GAUGE or COUNTER column becomes the metric
// mysql_<query name>_<column>, labelled by the query's LABEL columns. The
// result is sorted by name.
func ParseCustomQueries(data []byte) ([]CustomQuery, error) {
	var doc map[string]QuerySpec
	if err := yaml.UnmarshalStrict(data, &doc); err != nil {
		return nil, err
	}
	queries := make([]CustomQuery, 0, len(doc))
	for name, spec := range doc {
		q, err := buildCustomQuery(name, spec)
		if err != nil {
			return nil, fmt.Errorf("query %q: %w", name, err)
		}
		queries = append(queries, q)
	}
	slices.SortFunc(queries, func(a, b CustomQuery) int { return strings.Compare(a.Name, b.Name) })
	return queries, nil
}

func buildCustomQuery(name string, spec QuerySpec) (CustomQuery, error) {
	if !metricNamePart.MatchString(name) {
		return CustomQuery{}, fmt.Errorf("name must match %s", metricNamePart)
	}
	if strings.TrimSpace(spec.Query) == "" {
		return CustomQuery{}, errors.New("query is empty")
	}
	type column struct {
		name string
		ColumnMapping
	}
	var columns []column
	for _, entry := range spec.Metrics {
		if len(entry) != 1 {
			return CustomQuery{}, errors.New("each metrics entry must map exactly one column")
		}
		for col, m := range entry {
			if !metricNamePart.MatchString(col) {
				return CustomQuery{}, fmt.Errorf("column %q must match %s", col, metricNamePart)
			}
			if m.Usage == UsageLabel && strings.HasPrefix(col, "__") {
				return CustomQuery{}, fmt.Errorf("label column %q: names starting with __ are reserved", col)
			}
			if slices.ContainsFunc(columns, func(c column) bool { return c.name == col }) {
				return CustomQuery{}, fmt.Errorf("column %q is listed twice", col)
			}
			columns = append(columns, column{name: col, ColumnMapping: m})
		}
	}

	q := CustomQuery{Name: name, query: spec.Query}
	for _, c := range columns {
		if c.Usage == UsageLabel {
			q.labels = append(q.labels, c.name)
		}
	}
	for _, c := range columns {
		var vt prometheus.ValueType
		switch c.Usage {
		case UsageLabel, UsageDiscard:
			continue
		case UsageGauge:
			vt = prometheus.GaugeValue
		case UsageCounter:
			vt = prometheus.CounterValue
		default:
			return CustomQuery{}, fmt.Errorf("column %q: unknown usage %q", c.name, c.Usage)
		}
		fqName := prometheus.BuildFQName(namespace, name, c.name)
		for _, sub := range builtinSubsystems {
			if strings.HasPrefix(fqName, namespace+"_"+sub+"_") {
				return CustomQuery{}, fmt.Errorf("metric %s would share the built-in %s_%s_ prefix", fqName, namespace, sub)
			}
		}
		help := c.Description
		if help == "" {
			help = fmt.Sprintf("Column %s of custom query %s.", c.name, name)
		}
		q.values = append(q.values, customValue{
			column:    c.name,
			valueType: vt,
			desc:      prometheus.NewDesc(fqName, help, q.labels, nil),
		})
	}
	if len(q.values) == 0 {
		return CustomQuery{}, errors.New("no GAUGE or COUNTER column")
	}
	return q, nil
}

// equal reports whether q and o are the same query publishing the same
// metrics.
func (q CustomQuery) equal(o CustomQuery) bool {
	return q.Name == o.Name && q.query == o.query && slices.Equal(q.labels, o.labels) &&
		slices.EqualFunc(q.values, o.values, func(a, b customValue) bool {
			return a.column == b.column && a.valueType == b.valueType && a.desc.String() == b.desc.String()
		})
}

// metricNames returns the fully qualified names the query publishes.
func (q CustomQuery) metricNames() []string {
	names := make([]string, len(q.values))
	for i, v := range q.values {
		names[i] = prometheus.BuildFQName(namespace, q.Name, v.column)
	}
	return names
}

// Scrape runs the query in a read-only transaction and sends one metric per
// row and value column. NULL values are skipped. A row repeating an earlier
// row's labels, or with a label value that is not valid UTF-8, is dropped and
// reported, since the registry would reject the whole scrape.
func (q CustomQuery) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx, q.query)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	index := make(map[string]int, len(cols))
	for i, c := range cols {
		index[c] = i
	}
	for _, l := range q.labels {
		if _, ok := index[l]; !ok {
			return fmt.Errorf("result has no label column %q", l)
		}
	}
	for _, v := range q.values {
		if _, ok := index[v.column]; !ok {
			return fmt.Errorf("result has no value column %q", v.column)
		}
	}

	raw := make([]sql.NullString, len(cols))
	dest := make([]any, len(cols))
	for i := range raw {
		dest[i] = &raw[i]
	}
	seen := map[string]bool{}
	var errs []error
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return err
		}
		labelValues := make([]string, len(q.labels))
		for i, l := range q.labels {
			labelValues[i] = raw[index[l]].String
		}
		key := strings.Join(labelValues, "\x00")
		if seen[key] {
			continue
		}
		seen[key] = true
		for _, v := range q.values {
			cell := raw[index[v.column]]
			if !cell.Valid {
				continue
			}
			f, err := strconv.ParseFloat(cell.String, 64)
			if err != nil {
				errs = append(errs, fmt.Errorf("column %q: %w", v.column, err))
				continue
			}
			m, err := prometheus.NewConstMetric(v.desc, v.valueType, f, labelValues...)
			if err != nil {
				errs = append(errs, fmt.Errorf("column %q: %w", v.column, err))
				continue
			}
			ch <- m
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return errors.Join(errs...)
}
