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
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/slowlog"
)

const slowLogSubsystem = "instance_slow_log"

// SlowLogCollector exports the slow log tailer's counters (design 037).
type SlowLogCollector struct {
	stats                                              func() slowlog.Stats
	bytesDesc, entriesDesc, droppedDesc, rotationsDesc *prometheus.Desc
}

// NewSlowLogCollector builds a collector reading stats on every scrape.
func NewSlowLogCollector(stats func() slowlog.Stats) *SlowLogCollector {
	desc := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc(prometheus.BuildFQName(namespace, slowLogSubsystem, name), help, nil, nil)
	}
	return &SlowLogCollector{
		stats:         stats,
		bytesDesc:     desc("bytes", "Bytes the slow log files use on the run volume."),
		entriesDesc:   desc("entries_total", "Slow log entries emitted as log records."),
		droppedDesc:   desc("dropped_bytes_total", "Slow log bytes deleted or truncated before they were read."),
		rotationsDesc: desc("rotations_total", "Slow log rotations completed."),
	}
}

// Describe implements prometheus.Collector.
func (c *SlowLogCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.bytesDesc
	ch <- c.entriesDesc
	ch <- c.droppedDesc
	ch <- c.rotationsDesc
}

// Collect implements prometheus.Collector.
func (c *SlowLogCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.stats()
	ch <- prometheus.MustNewConstMetric(c.bytesDesc, prometheus.GaugeValue, float64(s.Bytes))
	ch <- prometheus.MustNewConstMetric(c.entriesDesc, prometheus.CounterValue, float64(s.Entries))
	ch <- prometheus.MustNewConstMetric(c.droppedDesc, prometheus.CounterValue, float64(s.DroppedBytes))
	ch <- prometheus.MustNewConstMetric(c.rotationsDesc, prometheus.CounterValue, float64(s.Rotations))
}
