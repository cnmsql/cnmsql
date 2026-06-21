/*
Copyright 2026 The CloudNative MySQL Authors.

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

package controller

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	mysqlgr "github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/groupreplication"
)

// grMetricNamespace is the Prometheus namespace for operator-level Group
// Replication metrics. It is distinct from the in-pod "mysql" exporter
// namespace so the two never collide on a name.
const grMetricNamespace = "cnmysql"

// grMemberStates enumerates the member states the collector reports, so that a
// state that drops to zero members is published as 0 rather than disappearing
// from the series (which would make absence indistinguishable from "unscraped").
var grMemberStates = []string{
	mysqlgr.MemberStateOnline,
	mysqlgr.MemberStateRecovering,
	mysqlgr.MemberStateOffline,
	mysqlgr.MemberStateError,
	mysqlgr.MemberStateUnreachable,
}

// grCollector is a Prometheus collector that publishes each Group Replication
// cluster's authoritative status (status.groupReplication) as operator metrics.
// It reads from the manager's cached client at scrape time, so the values track
// the operator's own cross-validated view without a separate reconcile or
// in-pod query.
type grCollector struct {
	reader client.Reader

	quorum       *prometheus.Desc
	bootstrapped *prometheus.Desc
	viewSize     *prometheus.Desc
	members      *prometheus.Desc
}

// RegisterGRMetrics registers the Group Replication collector on the
// controller-runtime global registry, so it is served on the operator's
// existing /metrics endpoint. The reader is typically the manager's cached
// client.
func RegisterGRMetrics(reader client.Reader) {
	ctrlmetrics.Registry.MustRegister(newGRCollector(reader))
}

// newGRCollector builds the collector over a cluster-listing reader, typically
// the manager's cached client.
func newGRCollector(reader client.Reader) *grCollector {
	labels := []string{"namespace", "cluster"}
	return &grCollector{
		reader: reader,
		quorum: prometheus.NewDesc(
			prometheus.BuildFQName(grMetricNamespace, "cluster", "gr_has_quorum"),
			"Whether the operator sees a Group Replication quorum (1) or not (0).",
			labels, nil),
		bootstrapped: prometheus.NewDesc(
			prometheus.BuildFQName(grMetricNamespace, "cluster", "gr_bootstrapped"),
			"Whether the Group Replication group has been bootstrapped at least once (1) or not (0).",
			labels, nil),
		viewSize: prometheus.NewDesc(
			prometheus.BuildFQName(grMetricNamespace, "cluster", "gr_view_size"),
			"Group size used as the quorum denominator (the sticky maximum observed view, clamped to spec.instances).",
			labels, nil),
		members: prometheus.NewDesc(
			prometheus.BuildFQName(grMetricNamespace, "cluster", "gr_members"),
			"Number of Group Replication members in each state.",
			append(append([]string{}, labels...), "state"), nil),
	}
}

// Describe implements prometheus.Collector.
func (c *grCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.quorum
	ch <- c.bootstrapped
	ch <- c.viewSize
	ch <- c.members
}

// grSample is the flattened, prometheus-free view of one cluster's GR status,
// extracted so the value math can be unit-tested without a metrics registry.
type grSample struct {
	namespace      string
	name           string
	hasQuorum      bool
	bootstrapped   bool
	viewSize       int
	membersByState map[string]int
}

// collectGRSamples lists clusters and returns one sample per Group Replication
// cluster that has reported status. If the listing fails (e.g. the cache has
// not synced yet) it returns nil rather than erroring the scrape.
func collectGRSamples(reader client.Reader) []grSample {
	var list mysqlv1alpha1.ClusterList
	if err := reader.List(context.Background(), &list); err != nil {
		return nil
	}
	var samples []grSample
	for i := range list.Items {
		cluster := &list.Items[i]
		if !cluster.IsGroupReplication() {
			continue
		}
		status := cluster.Status.GroupReplication
		if status == nil {
			continue
		}
		// Seed every known state with zero so a state that drops to no members
		// is published as 0 rather than vanishing from the series.
		byState := map[string]int{}
		for _, state := range grMemberStates {
			byState[state] = 0
		}
		for _, m := range status.Members {
			byState[m.State]++
		}
		samples = append(samples, grSample{
			namespace:      cluster.Namespace,
			name:           cluster.Name,
			hasQuorum:      status.HasQuorum,
			bootstrapped:   status.Bootstrapped,
			viewSize:       status.ObservedViewMax,
			membersByState: byState,
		})
	}
	return samples
}

// Collect implements prometheus.Collector.
func (c *grCollector) Collect(ch chan<- prometheus.Metric) {
	for _, s := range collectGRSamples(c.reader) {
		ch <- prometheus.MustNewConstMetric(c.quorum, prometheus.GaugeValue, boolToFloat(s.hasQuorum), s.namespace, s.name)
		ch <- prometheus.MustNewConstMetric(c.bootstrapped, prometheus.GaugeValue, boolToFloat(s.bootstrapped), s.namespace, s.name)
		ch <- prometheus.MustNewConstMetric(c.viewSize, prometheus.GaugeValue, float64(s.viewSize), s.namespace, s.name)
		for _, state := range grMemberStates {
			ch <- prometheus.MustNewConstMetric(
				c.members, prometheus.GaugeValue, float64(s.membersByState[state]), s.namespace, s.name, state)
		}
	}
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
