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
	"testing"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

func TestBuildPodMonitor(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	cluster.Spec.Monitoring = &mysqlv1alpha1.MonitoringConfiguration{
		EnablePodMonitor: true,
		TLSConfig:        &mysqlv1alpha1.ClusterMonitoringTLSConfig{Enabled: true},
	}

	plan := clusterPlan{
		ServerCASecretName: cluster.Name + "-ca",
		ClientTLSSecret:    cluster.Name + "-client-tls",
		RServiceName:       cluster.Name + "-r",
	}
	podMonitor := buildPodMonitor(cluster, plan)

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
	if endpoint.TLSConfig == nil {
		t.Fatal("endpoint TLS config = nil, want mutual TLS config")
	}
	if ca := endpoint.TLSConfig.CA.Secret; ca == nil || ca.Name != plan.ServerCASecretName || ca.Key != "ca.crt" {
		t.Fatalf("endpoint CA = %#v, want secret %s/ca.crt", ca, plan.ServerCASecretName)
	}
	if cert := endpoint.TLSConfig.Cert.Secret; cert == nil || cert.Name != plan.ClientTLSSecret || cert.Key != "tls.crt" {
		t.Fatalf("endpoint cert = %#v, want secret %s/tls.crt", cert, plan.ClientTLSSecret)
	}
	if key := endpoint.TLSConfig.KeySecret; key == nil || key.Name != plan.ClientTLSSecret || key.Key != "tls.key" {
		t.Fatalf("endpoint key = %#v, want secret %s/tls.key", key, plan.ClientTLSSecret)
	}
	wantServerName := plan.RServiceName + "." + cluster.Namespace + ".svc"
	if sn := endpoint.TLSConfig.ServerName; sn == nil || *sn != wantServerName {
		t.Fatalf("endpoint serverName = %v, want %q", sn, wantServerName)
	}
}

// TestBuildPodMonitorPlainHTTP verifies that without monitoring TLS the endpoint
// stays on plain HTTP with no scrape-side TLS config.
func TestBuildPodMonitorPlainHTTP(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	cluster.Spec.Monitoring = &mysqlv1alpha1.MonitoringConfiguration{EnablePodMonitor: true}

	endpoint := buildPodMonitor(cluster, clusterPlan{}).Spec.PodMetricsEndpoints[0]
	if endpoint.Scheme != nil {
		t.Fatalf("endpoint scheme = %v, want nil (plain HTTP)", endpoint.Scheme)
	}
	if endpoint.TLSConfig != nil {
		t.Fatalf("endpoint TLS config = %#v, want nil", endpoint.TLSConfig)
	}
}

// TestRunArgsMetricsTLS checks that the run command opts into serving metrics
// over mutual TLS exactly when spec.monitoring.tls.enabled is set.
func TestRunArgsMetricsTLS(t *testing.T) {
	t.Parallel()

	off := (&ClusterReconciler{}).runArgs(baseCluster(), testPlan(), instancePlan{})
	if containsArg(off, "--metrics-tls") {
		t.Fatalf("--metrics-tls present without monitoring TLS: %v", off)
	}

	cluster := baseCluster()
	cluster.Spec.Monitoring = &mysqlv1alpha1.MonitoringConfiguration{
		TLSConfig: &mysqlv1alpha1.ClusterMonitoringTLSConfig{Enabled: true},
	}
	on := (&ClusterReconciler{}).runArgs(cluster, testPlan(), instancePlan{})
	if !containsArg(on, "--metrics-tls") {
		t.Fatalf("missing --metrics-tls with monitoring TLS enabled: %v", on)
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

	if err := reconciler.reconcilePodMonitor(ctx, cluster, clusterPlan{}); err != nil {
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
	if err := reconciler.reconcilePodMonitor(ctx, cluster, clusterPlan{}); err != nil {
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

	if err := reconciler.reconcilePodMonitor(ctx, cluster, clusterPlan{}); err != nil {
		t.Fatalf("reconcilePodMonitor with CRD absent = %v, want nil", err)
	}

	podMonitor := &monitoringv1.PodMonitor{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
	if err := reconciler.Get(ctx, key, podMonitor); !apierrors.IsNotFound(err) {
		t.Fatalf("pod monitor get err = %v, want not found (none created)", err)
	}
}
