/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package controller

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

// reinitRequestedInstances parses the comma-separated instance list from the
// Cluster's reinit annotation, dropping blanks and surrounding whitespace.
func reinitRequestedInstances(cluster *mysqlv1alpha1.Cluster) []string {
	raw := cluster.Annotations[reinitAnnotation]
	if raw == "" {
		return nil
	}
	var out []string
	for part := range strings.SplitSeq(raw, ",") {
		if name := strings.TrimSpace(part); name != "" {
			out = append(out, name)
		}
	}
	return out
}

// reinitRequested reports whether the named instance is listed in the Cluster's
// reinit annotation.
func reinitRequested(cluster *mysqlv1alpha1.Cluster, name string) bool {
	return slices.Contains(reinitRequestedInstances(cluster), name)
}

// reconcileReinit drives a requested re-initialisation of a single instance. It
// returns handled=true while the instance's Pod and PVC are being torn down, so
// the caller skips the normal ensure* pass and does not recreate them until the
// teardown completes. When both are gone it clears the request and returns
// handled=false, letting the normal reconcile recreate them empty — the
// bootstrap init-container then re-clones a fresh copy from a backup and rejoins
// replication, preserving the instance's name/ordinal (hence server_id).
//
// The current primary is never re-initialised: it is the replication source, so
// wiping it would destroy the cluster's data. Such a request is refused and
// dropped.
func (r *ClusterReconciler) reconcileReinit(ctx context.Context, cluster *mysqlv1alpha1.Cluster, inst instancePlan) (bool, error) {
	if !reinitRequested(cluster, inst.Name) {
		return false, nil
	}
	log := logf.FromContext(ctx).WithName("reinit").WithValues("instance", inst.Name)

	if inst.IsPrimary {
		log.Info("Refusing to re-initialise the primary instance; dropping request")
		if r.Recorder != nil {
			r.Recorder.Event(cluster, corev1.EventTypeWarning, "ReinitRefused",
				fmt.Sprintf("Refusing to re-initialise %s: it is the primary (re-cloning the replication source would destroy the cluster's data)", inst.Name))
		}
		return false, r.clearReinitRequest(ctx, cluster, inst.Name)
	}

	// Tear down the Pod first, then the PVC. The PVC stays Terminating until the
	// Pod releases its mount, so this typically spans several reconciles; the
	// owned-object watches re-trigger us as each object disappears.
	pod := &corev1.Pod{}
	podGone := false
	switch err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.Name}, pod); {
	case apierrors.IsNotFound(err):
		podGone = true
	case err != nil:
		return false, err
	case pod.DeletionTimestamp == nil:
		log.Info("Deleting Pod and PVC to re-initialise instance from a fresh clone")
		if r.Recorder != nil {
			r.Recorder.Event(cluster, corev1.EventTypeWarning, "Reinitializing",
				fmt.Sprintf("Re-initialising %s: deleting its Pod and PVC to re-clone from a fresh backup", inst.Name))
		}
		if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
	}

	pvc := &corev1.PersistentVolumeClaim{}
	pvcGone := false
	switch err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.PVCName}, pvc); {
	case apierrors.IsNotFound(err):
		pvcGone = true
	case err != nil:
		return false, err
	case pvc.DeletionTimestamp == nil:
		if err := r.Delete(ctx, pvc); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
	}

	if !podGone || !pvcGone {
		// Still tearing down: do not recreate the instance yet.
		return true, nil
	}

	log.Info("Re-initialisation teardown complete; clearing request and recreating instance")
	if err := r.clearReinitRequest(ctx, cluster, inst.Name); err != nil {
		return false, err
	}
	return false, nil
}

// clearReinitRequest removes the named instance from the Cluster's reinit
// annotation (deleting the annotation entirely when it was the last entry) and
// persists the change. It mutates the passed Cluster in place so subsequent
// reinitRequested checks in this reconcile see the cleared state.
func (r *ClusterReconciler) clearReinitRequest(ctx context.Context, cluster *mysqlv1alpha1.Cluster, name string) error {
	var remaining []string
	for _, n := range reinitRequestedInstances(cluster) {
		if n != name {
			remaining = append(remaining, n)
		}
	}
	before := cluster.DeepCopy()
	if len(remaining) == 0 {
		delete(cluster.Annotations, reinitAnnotation)
	} else {
		cluster.Annotations[reinitAnnotation] = strings.Join(remaining, ",")
	}
	if reflect.DeepEqual(before.Annotations, cluster.Annotations) {
		return nil
	}
	return r.Patch(ctx, cluster, client.MergeFrom(before))
}
