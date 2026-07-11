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

package heartbeat

import "github.com/prometheus/client_golang/prometheus"

var (
	lagDesc = prometheus.NewDesc(
		"cnmsql_replication_lag_seconds",
		"Age of the newest heartbeat stamp this instance has applied. On a replica this is the replication delay. "+
			"It is absent when no heartbeat reading could be taken, and it climbs with the clock once the primary "+
			"stops stamping, so alert on it together with the primary's health rather than on its own.",
		nil, nil,
	)
	writerDesc = prometheus.NewDesc(
		"cnmsql_replication_heartbeat_writer",
		"1 when this instance is the one stamping the heartbeat table, which only the writable primary does.",
		nil, nil,
	)
)

// Collector exposes the loop's latest reading to Prometheus.
//
// The lag gauge is deliberately absent rather than zero when there is no
// reading: a missing series is something an alert can be written against, while
// a zero would read as a replica in perfect sync, which is the opposite of what
// an unreadable heartbeat means.
type Collector struct {
	loop *Loop
}

// NewCollector builds a Prometheus collector over a heartbeat loop.
func NewCollector(loop *Loop) *Collector {
	return &Collector{loop: loop}
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- lagDesc
	ch <- writerDesc
}

// Collect implements prometheus.Collector.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	state := c.loop.State()
	writing := 0.0
	if state.Writing {
		writing = 1
	}
	ch <- prometheus.MustNewConstMetric(writerDesc, prometheus.GaugeValue, writing)
	if state.LagKnown {
		ch <- prometheus.MustNewConstMetric(lagDesc, prometheus.GaugeValue, state.Lag.Seconds())
	}
}
