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
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

const primaryLeaseDuration = 15 * time.Second

func primaryLeaseName(cluster *mysqlv1alpha1.Cluster) string {
	return cluster.Name + "-primary"
}

func primaryLeaseEnabled(cluster *mysqlv1alpha1.Cluster) bool {
	return cluster.Spec.EnablePrimaryLease == nil || *cluster.Spec.EnablePrimaryLease
}

func (r *ClusterReconciler) ensurePrimaryLease(ctx context.Context, cluster *mysqlv1alpha1.Cluster) error {
	// The primary lease is the async split-brain guard (the in-Pod async strategy
	// anchors writes on it). Group Replication provides quorum-based split-brain
	// safety itself, and its in-Pod strategy never touches the lease, so it is
	// unused under GR.
	if isGroupReplication(cluster) || !primaryLeaseEnabled(cluster) {
		return nil
	}
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{
		Name:      primaryLeaseName(cluster),
		Namespace: cluster.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, lease, func() error {
		seconds := int32(primaryLeaseDuration / time.Second)
		lease.Spec.LeaseDurationSeconds = &seconds
		return controllerutil.SetControllerReference(cluster, lease, r.Scheme)
	})
	return err
}

func (r *ClusterReconciler) isPrimaryLeaseHeld(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	holder string,
) (bool, error) {
	if !primaryLeaseEnabled(cluster) {
		return false, nil
	}
	lease := &coordinationv1.Lease{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: primaryLeaseName(cluster)}
	if err := r.Get(ctx, key, lease); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != holder {
		return false, nil
	}
	if lease.Spec.RenewTime == nil {
		return false, nil
	}
	duration := primaryLeaseDuration
	if lease.Spec.LeaseDurationSeconds != nil {
		duration = time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second
	}
	return time.Since(lease.Spec.RenewTime.Time) <= duration, nil
}
