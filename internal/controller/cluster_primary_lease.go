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
	if !primaryLeaseEnabled(cluster) {
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
