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

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
)

// reconcilePDB keeps the cluster's PodDisruptionBudgets in step with the spec.
//
// When spec.enablePDB is true (the default) the operator maintains two PDBs:
//
//   - {cluster}-primary: maxUnavailable=1, matches the pod holding role=primary.
//   - {cluster}-replicas: maxUnavailable=floor(N/2) for N replicas, matches pods
//     with role=replica. Single-instance clusters (no replicas) skip it.
//
// During a node maintenance window (spec.nodeMaintenanceWindow.inProgress with
// reusePVC) the relevant PDBs are torn down so the kubelet can drain the node;
// they are restored once the window closes. When enablePDB is false (or the
// cluster is deleted) both PDBs are removed.
func (r *ClusterReconciler) reconcilePDB(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) error {
	pdbEnabled := cluster.Spec.EnablePDB == nil || *cluster.Spec.EnablePDB

	// During a node maintenance window we must let nodes drain, so the PDBs that
	// would block eviction are removed for the duration of the window. The
	// replica PDB always goes; the primary PDB only goes for a single-instance
	// cluster, where the lone pod must be allowed to move with its (reused) PVC.
	maintenance := inNodeMaintenance(cluster)
	singleInstance := plan.Instances <= 1

	wantPrimary := pdbEnabled && (!maintenance || !singleInstance)
	wantReplica := pdbEnabled && !singleInstance && !maintenance

	if err := r.reconcileOnePDB(ctx, cluster, primaryPDBName(cluster), wantPrimary, func() *policyv1.PodDisruptionBudget {
		return buildPDB(cluster, primaryPDBName(cluster), rolePrimary, intstr.FromInt32(1))
	}); err != nil {
		return err
	}

	// Replicas tolerate losing up to half their number at once; the rest keep the
	// cluster serving reads and available as failover candidates.
	replicas := plan.Instances - 1
	return r.reconcileOnePDB(ctx, cluster, replicaPDBName(cluster), wantReplica, func() *policyv1.PodDisruptionBudget {
		return buildPDB(cluster, replicaPDBName(cluster), roleReplica, intstr.FromInt32(int32(replicas/2)))
	})
}

// reconcileOnePDB creates/updates the named PDB when want is true, or deletes it
// (if present) otherwise.
func (r *ClusterReconciler) reconcileOnePDB(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	name string,
	want bool,
	build func() *policyv1.PodDisruptionBudget,
) error {
	if !want {
		pdb := &policyv1.PodDisruptionBudget{}
		err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, pdb)
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		return r.Delete(ctx, pdb)
	}

	pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{
		Name:      name,
		Namespace: cluster.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		desired := build()
		pdb.Labels = desired.Labels
		// Selector is immutable once set; CreateOrUpdate re-applies it on create
		// and leaves the (identical) value untouched on update.
		pdb.Spec = desired.Spec
		return controllerutil.SetControllerReference(cluster, pdb, r.Scheme)
	})
	return err
}

// buildPDB returns a PodDisruptionBudget that selects the cluster's pods holding
// the given role, allowing at most maxUnavailable of them to be voluntarily
// disrupted at a time.
func buildPDB(cluster *mysqlv1alpha1.Cluster, name, role string, maxUnavailable intstr.IntOrString) *policyv1.PodDisruptionBudget {
	labels := labelsFor(cluster, "", "")
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &maxUnavailable,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					clusterLabel: cluster.Name,
					roleLabel:    role,
				},
			},
		},
	}
}

// inNodeMaintenance reports whether a node maintenance window is active for the
// cluster. The window only relaxes PDBs when PVCs are reused, otherwise draining
// a node would discard the instance's data.
func inNodeMaintenance(cluster *mysqlv1alpha1.Cluster) bool {
	window := cluster.Spec.NodeMaintenanceWindow
	if window == nil || !window.InProgress {
		return false
	}
	// ReusePVC defaults to true.
	return window.ReusePVC == nil || *window.ReusePVC
}

func primaryPDBName(cluster *mysqlv1alpha1.Cluster) string {
	return cluster.Name + "-primary"
}

func replicaPDBName(cluster *mysqlv1alpha1.Cluster) string {
	return cluster.Name + "-replicas"
}

// isPodFenced reports whether an instance Pod carries the fencing annotation set
// to "true".
func isPodFenced(pod *corev1.Pod) bool {
	return pod.Annotations[fencingAnnotation] == "true"
}

// reconcileFencing keeps each instance Pod's routable label in step with its
// fencing annotation. A fenced Pod gets routable=false, which drops it from every
// routing Service (their selectors require routable=true); clearing the
// annotation restores routable=true. The in-Pod reconciler separately reads
// status.fencedInstances and holds the fenced instance read-only.
func (r *ClusterReconciler) reconcileFencing(ctx context.Context, cluster *mysqlv1alpha1.Cluster, observed observedCluster) error {
	fenced := map[string]bool{}
	for _, name := range observed.FencedInstances {
		fenced[name] = true
	}
	for _, name := range observed.InstanceNames {
		pod := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, pod); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		if pod.DeletionTimestamp != nil {
			continue
		}
		desired := routableTrue
		if fenced[name] {
			desired = routableFalse
		}
		if pod.Labels[routableLabel] == desired {
			continue
		}
		before := pod.DeepCopy()
		if pod.Labels == nil {
			pod.Labels = map[string]string{}
		}
		pod.Labels[routableLabel] = desired
		if err := r.Patch(ctx, pod, client.MergeFrom(before)); err != nil {
			return err
		}
		if r.Recorder != nil {
			verb := "Unfenced"
			if fenced[name] {
				verb = "Fenced"
			}
			r.Recorder.Event(cluster, corev1.EventTypeWarning, verb,
				fmt.Sprintf("Instance %s %s", name, verb))
		}
	}
	return nil
}

