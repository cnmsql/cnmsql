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
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/webserver"
)

type observedCluster struct {
	Phase       string
	PhaseReason string
	Ready       bool
	Progressing bool
	Plan        clusterPlan
	Status      *webserver.Status
}

func (r *ClusterReconciler) observe(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) (observedCluster, error) {
	observed := observedCluster{
		Phase:       phasePending,
		PhaseReason: "Waiting for Pod",
		Ready:       false,
		Progressing: true,
		Plan:        plan,
	}
	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: plan.InstanceName}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return observed, nil
		}
		return observedCluster{}, err
	}
	if !podReady(pod) {
		observed.Phase = phaseProvisioning
		observed.PhaseReason = "Waiting for Pod readiness"
		return observed, nil
	}

	statusClient := r.StatusClient
	if statusClient == nil {
		statusClient = &HTTPStatusClient{Client: r.Client}
	}
	status, err := statusClient.Status(ctx, cluster, plan.InstanceName)
	if err != nil {
		observed.Phase = phaseProvisioning
		observed.PhaseReason = "Waiting for instance status: " + err.Error()
		return observed, nil
	}
	observed.Status = status
	observed.Ready = status.IsReady
	observed.Progressing = !status.IsReady
	if status.IsReady {
		observed.Phase = phaseReady
		observed.PhaseReason = "Instance is ready"
	} else {
		observed.Phase = phaseProvisioning
		observed.PhaseReason = "Instance manager reported not ready"
	}
	return observed, nil
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (r *ClusterReconciler) patchStatus(ctx context.Context, cluster *mysqlv1alpha1.Cluster, observed observedCluster) error {
	latest := &mysqlv1alpha1.Cluster{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
	if err := r.Get(ctx, key, latest); err != nil {
		return err
	}
	before := latest.DeepCopy()
	if observed.Plan.InstanceName != "" {
		latest.Status.Instances = 1
		latest.Status.InstanceNames = []string{observed.Plan.InstanceName}
		latest.Status.CurrentPrimary = observed.Plan.InstanceName
		latest.Status.LatestGeneratedNode = 1
		latest.Status.Image = observed.Plan.Image
	} else {
		latest.Status.Instances = latest.Spec.Instances
		latest.Status.InstanceNames = nil
		latest.Status.CurrentPrimary = ""
		latest.Status.LatestGeneratedNode = 0
		latest.Status.Image = ""
	}
	latest.Status.ObservedGeneration = latest.Generation
	latest.Status.Phase = observed.Phase
	latest.Status.PhaseReason = observed.PhaseReason
	if observed.Ready {
		latest.Status.ReadyInstances = 1
		now := metav1.Now().Format(time.RFC3339)
		if latest.Status.CurrentPrimaryTimestamp == "" {
			latest.Status.CurrentPrimaryTimestamp = now
		}
	} else {
		latest.Status.ReadyInstances = 0
	}
	if observed.Status != nil {
		latest.Status.GTIDExecutedByInstance = map[string]string{
			observed.Plan.InstanceName: observed.Status.GTIDExecuted,
		}
	}
	apimeta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             conditionStatus(observed.Ready),
		Reason:             observed.Phase,
		Message:            observed.PhaseReason,
		ObservedGeneration: latest.Generation,
	})
	apimeta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
		Type:               conditionProgressing,
		Status:             conditionStatus(observed.Progressing),
		Reason:             observed.Phase,
		Message:            observed.PhaseReason,
		ObservedGeneration: latest.Generation,
	})
	r.recordPhaseTransition(latest, before.Status.Phase, observed)
	return r.Status().Patch(ctx, latest, client.MergeFrom(before))
}

// recordPhaseTransition emits an Event only when the phase actually changes, so
// steady-state resyncs do not spam the event stream.
func (r *ClusterReconciler) recordPhaseTransition(cluster *mysqlv1alpha1.Cluster, previousPhase string, observed observedCluster) {
	if r.Recorder == nil || observed.Phase == previousPhase {
		return
	}
	eventType := corev1.EventTypeNormal
	if observed.Phase == phaseBlocked {
		eventType = corev1.EventTypeWarning
	}
	r.Recorder.Event(cluster, eventType, observed.Phase, observed.PhaseReason)
}

func conditionStatus(ok bool) metav1.ConditionStatus {
	if ok {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}
