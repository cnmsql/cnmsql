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
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/diskusage"
)

const volumeSubsystem = "instance_data_volume"

// VolumeCollector exports the instance data volume's space usage as Prometheus
// gauges. Unlike the SQL scrapers it does not touch mysqld: it statfs's the data
// directory on each scrape, so it keeps reporting even while the server is down.
// It is the continuous-gauge counterpart to the operator's StoragePressure
// condition; both read the same diskusage snapshot.
type VolumeCollector struct {
	dataDir       string
	logger        *slog.Logger
	usedDesc      *prometheus.Desc
	capacityDesc  *prometheus.Desc
	availableDesc *prometheus.Desc
	errorDesc     *prometheus.Desc
}

// NewVolumeCollector builds a collector that reports usage for the filesystem
// backing dataDir.
func NewVolumeCollector(dataDir string) *VolumeCollector {
	return &VolumeCollector{
		dataDir: dataDir,
		logger:  slog.Default(),
		usedDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, volumeSubsystem, "used_bytes"),
			"Bytes used on the instance data volume.", nil, nil),
		capacityDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, volumeSubsystem, "capacity_bytes"),
			"Total size of the instance data volume in bytes.", nil, nil),
		availableDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, volumeSubsystem, "available_bytes"),
			"Bytes available to mysqld on the instance data volume.", nil, nil),
		errorDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, volumeSubsystem, "scrape_error"),
			"Whether reading the instance data volume usage failed (1 for error, 0 for success).",
			nil, nil),
	}
}

// Describe implements prometheus.Collector.
func (c *VolumeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.usedDesc
	ch <- c.capacityDesc
	ch <- c.availableDesc
	ch <- c.errorDesc
}

// Collect implements prometheus.Collector.
func (c *VolumeCollector) Collect(ch chan<- prometheus.Metric) {
	usage, err := diskusage.Of(c.dataDir)
	if err != nil {
		c.logger.Error("Data volume usage scrape failed", "dataDir", c.dataDir, "err", err)
		ch <- prometheus.MustNewConstMetric(c.errorDesc, prometheus.GaugeValue, 1)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.usedDesc, prometheus.GaugeValue, float64(usage.UsedBytes))
	ch <- prometheus.MustNewConstMetric(c.capacityDesc, prometheus.GaugeValue, float64(usage.CapacityBytes))
	ch <- prometheus.MustNewConstMetric(c.availableDesc, prometheus.GaugeValue, float64(usage.AvailableBytes))
	ch <- prometheus.MustNewConstMetric(c.errorDesc, prometheus.GaugeValue, 0)
}
