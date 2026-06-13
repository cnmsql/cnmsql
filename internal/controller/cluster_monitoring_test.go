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

package controller

import (
	"context"
	"testing"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
)

func TestBuildPodMonitor(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	cluster.Spec.Monitoring = &mysqlv1alpha1.MonitoringConfiguration{
		EnablePodMonitor: true,
		TLSConfig:        &mysqlv1alpha1.ClusterMonitoringTLSConfig{Enabled: true},
	}

	podMonitor := buildPodMonitor(cluster)

	if podMonitor.Name != cluster.Name {
		t.Fatalf("pod monitor name = %q, want %q", podMonitor.Name, cluster.Name)
	}
	if podMonitor.Labels[podMonitorClusterLabel] != cluster.Name {
		t.Fatalf("pod monitor cluster label = %q, want %q", podMonitor.Labels[podMonitorClusterLabel], cluster.Name)
	}
	if podMonitor.Spec.Selector.MatchLabels[podMonitorClusterLabel] != cluster.Name {
		t.Fatalf("selector = %#v, want %s=%s", podMonitor.Spec.Selector.MatchLabels, podMonitorClusterLabel, cluster.Name)
	}
	if got := len(podMonitor.Spec.PodMetricsEndpoints); got != 1 {
		t.Fatalf("endpoints = %d, want 1", got)
	}
	endpoint := podMonitor.Spec.PodMetricsEndpoints[0]
	if endpoint.Port == nil || *endpoint.Port != metricsPortName {
		t.Fatalf("endpoint port = %v, want %q", endpoint.Port, metricsPortName)
	}
	if endpoint.Path != "/metrics" {
		t.Fatalf("endpoint path = %q, want /metrics", endpoint.Path)
	}
	if endpoint.Scheme == nil || *endpoint.Scheme != monitoringv1.SchemeHTTPS {
		t.Fatalf("endpoint scheme = %v, want %s", endpoint.Scheme, monitoringv1.SchemeHTTPS)
	}
}

func TestReconcilePodMonitorCreateAndDelete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Monitoring = &mysqlv1alpha1.MonitoringConfiguration{EnablePodMonitor: true}
	scheme := testScheme(t)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(cluster).
			Build(),
		Scheme:              scheme,
		podMonitorAvailable: true,
	}

	if err := reconciler.reconcilePodMonitor(ctx, cluster); err != nil {
		t.Fatal(err)
	}

	podMonitor := &monitoringv1.PodMonitor{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
	if err := reconciler.Get(ctx, key, podMonitor); err != nil {
		t.Fatal(err)
	}
	if !metav1.IsControlledBy(podMonitor, cluster) {
		t.Fatalf("pod monitor owner references = %#v, want controlled by cluster", podMonitor.OwnerReferences)
	}
	if endpoint := podMonitor.Spec.PodMetricsEndpoints[0]; endpoint.Port == nil || *endpoint.Port != metricsPortName {
		t.Fatalf("endpoint port = %v, want %q", endpoint.Port, metricsPortName)
	}

	cluster.Spec.Monitoring.EnablePodMonitor = false
	if err := reconciler.reconcilePodMonitor(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	err := reconciler.Get(ctx, key, podMonitor)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("pod monitor get err = %v, want not found", err)
	}
}

// TestReconcilePodMonitorCRDAbsent ensures PodMonitor reconciliation is a no-op
// (no error, no API calls) when the Prometheus Operator CRD is not installed,
// even with enablePodMonitor set.
func TestReconcilePodMonitorCRDAbsent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Monitoring = &mysqlv1alpha1.MonitoringConfiguration{EnablePodMonitor: true}
	scheme := testScheme(t)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(cluster).
			Build(),
		Scheme:              scheme,
		Recorder:            record.NewFakeRecorder(10),
		podMonitorAvailable: false,
	}

	if err := reconciler.reconcilePodMonitor(ctx, cluster); err != nil {
		t.Fatalf("reconcilePodMonitor with CRD absent = %v, want nil", err)
	}

	podMonitor := &monitoringv1.PodMonitor{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
	if err := reconciler.Get(ctx, key, podMonitor); !apierrors.IsNotFound(err) {
		t.Fatalf("pod monitor get err = %v, want not found (none created)", err)
	}
}
