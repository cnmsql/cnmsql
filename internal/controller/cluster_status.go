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
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
)

type observedCluster struct {
	Phase       string
	PhaseReason string
	Ready       bool
	Progressing bool
	Plan        clusterPlan
	// PrimaryName is the instance acting as primary (fixed to ordinal 1 in M4).
	PrimaryName string
	// ReadyInstances is the number of instances reporting ready.
	ReadyInstances int
	// InstanceNames are the desired instance names, in ordinal order.
	InstanceNames []string
	// GTIDByInstance maps instance name to its gtid_executed set.
	GTIDByInstance map[string]string
}

// observe polls every desired instance and aggregates cluster-level readiness.
// The cluster is Ready when all desired instances report ready.
func (r *ClusterReconciler) observe(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) (observedCluster, error) {
	statusClient := r.StatusClient
	if statusClient == nil {
		statusClient = &HTTPStatusClient{Client: r.Client}
	}

	observed := observedCluster{
		Plan:           plan,
		PrimaryName:    plan.primaryName(cluster),
		InstanceNames:  plan.instanceNames(cluster),
		GTIDByInstance: map[string]string{},
		Progressing:    true,
	}

	for i := 1; i <= plan.Instances; i++ {
		inst := plan.instanceFor(cluster, i)
		pod := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.Name}, pod); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return observedCluster{}, err
		}
		if !podReady(pod) {
			continue
		}
		status, err := statusClient.Status(ctx, cluster, inst.Name)
		if err != nil {
			continue
		}
		if status.GTIDExecuted != "" {
			observed.GTIDByInstance[inst.Name] = status.GTIDExecuted
		}
		if status.IsReady {
			observed.ReadyInstances++
		}
	}

	observed.Ready = observed.ReadyInstances == plan.Instances
	observed.Progressing = !observed.Ready
	switch {
	case observed.Ready:
		observed.Phase = phaseReady
		observed.PhaseReason = "All instances are ready"
	case observed.ReadyInstances == 0:
		observed.Phase = phasePending
		observed.PhaseReason = "Waiting for the primary instance"
	default:
		observed.Phase = phaseProvisioning
		observed.PhaseReason = fmt.Sprintf("%d/%d instances ready", observed.ReadyInstances, plan.Instances)
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
	if len(observed.InstanceNames) > 0 {
		latest.Status.Instances = observed.Plan.Instances
		latest.Status.InstanceNames = observed.InstanceNames
		latest.Status.CurrentPrimary = observed.PrimaryName
		latest.Status.LatestGeneratedNode = observed.Plan.Instances
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
	latest.Status.ReadyInstances = observed.ReadyInstances
	if observed.ReadyInstances > 0 && latest.Status.CurrentPrimaryTimestamp == "" {
		latest.Status.CurrentPrimaryTimestamp = metav1.Now().Format(time.RFC3339)
	}
	if len(observed.GTIDByInstance) > 0 {
		latest.Status.GTIDExecutedByInstance = observed.GTIDByInstance
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
