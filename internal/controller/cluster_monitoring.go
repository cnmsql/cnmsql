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

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
)

const metricsPortName = "metrics"

func (r *ClusterReconciler) reconcilePodMonitor(ctx context.Context, cluster *mysqlv1alpha1.Cluster) error {
	// PodMonitor support is opt-in and requires the Prometheus Operator CRD. When
	// it is not installed there is nothing to create or clean up, so skip
	// entirely rather than erroring on a no-matches API call.
	if !r.podMonitorAvailable {
		if cluster.Spec.Monitoring != nil && cluster.Spec.Monitoring.EnablePodMonitor {
			r.Recorder.Eventf(cluster, corev1.EventTypeWarning, "PodMonitorUnavailable",
				"spec.monitoring.enablePodMonitor is set but the PodMonitor CRD (Prometheus Operator) is not installed")
		}
		return nil
	}

	name := podMonitorName(cluster)
	if cluster.Spec.Monitoring == nil || !cluster.Spec.Monitoring.EnablePodMonitor {
		podMonitor := &monitoringv1.PodMonitor{}
		err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, podMonitor)
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		return r.Delete(ctx, podMonitor)
	}

	podMonitor := &monitoringv1.PodMonitor{ObjectMeta: metav1.ObjectMeta{
		Name:      name,
		Namespace: cluster.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, podMonitor, func() error {
		desired := buildPodMonitor(cluster)
		podMonitor.Labels = desired.Labels
		podMonitor.Spec = desired.Spec
		return controllerutil.SetControllerReference(cluster, podMonitor, r.Scheme)
	})
	return err
}

func buildPodMonitor(cluster *mysqlv1alpha1.Cluster) *monitoringv1.PodMonitor {
	labels := labelsFor(cluster, "", "")
	labels[podMonitorClusterLabel] = cluster.Name

	metricsPort := new(string)
	*metricsPort = metricsPortName
	endpoint := monitoringv1.PodMetricsEndpoint{
		Port:     metricsPort,
		Path:     "/metrics",
		Interval: "30s",
	}
	if cluster.Spec.Monitoring != nil &&
		cluster.Spec.Monitoring.TLSConfig != nil &&
		cluster.Spec.Monitoring.TLSConfig.Enabled {
		scheme := new(monitoringv1.Scheme)
		*scheme = monitoringv1.SchemeHTTPS
		endpoint.Scheme = scheme
	}

	return &monitoringv1.PodMonitor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podMonitorName(cluster),
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: monitoringv1.PodMonitorSpec{
			Selector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					podMonitorClusterLabel: cluster.Name,
				},
			},
			PodMetricsEndpoints: []monitoringv1.PodMetricsEndpoint{endpoint},
			NamespaceSelector: monitoringv1.NamespaceSelector{
				MatchNames: []string{cluster.Namespace},
			},
			PodTargetLabels: []string{
				clusterLabel,
				instanceLabel,
				roleLabel,
				podMonitorClusterLabel,
			},
			JobLabel: podMonitorClusterLabel,
		},
	}
}

func podMonitorName(cluster *mysqlv1alpha1.Cluster) string {
	return cluster.Name
}
