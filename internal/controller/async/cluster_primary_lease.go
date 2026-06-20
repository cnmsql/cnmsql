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

package async

import (
	"context"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
)

const primaryLeaseDuration = 15 * time.Second

func primaryLeaseName(cluster *mysqlv1alpha1.Cluster) string {
	return cluster.Name + "-primary"
}

// EnsurePrimaryLease reconciles the split-brain guard used by async instances.
func (r *Reconciler) EnsurePrimaryLease(ctx context.Context, cluster *mysqlv1alpha1.Cluster) error {
	if !cluster.IsPrimaryLeaseEnabled() {
		return nil
	}
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{
		Name:      primaryLeaseName(cluster),
		Namespace: cluster.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.client, lease, func() error {
		seconds := int32(primaryLeaseDuration / time.Second)
		lease.Spec.LeaseDurationSeconds = &seconds
		return controllerutil.SetControllerReference(cluster, lease, r.scheme)
	})
	return err
}

// PrimaryLeaseStatus reports whether holder still owns an unexpired Lease.
func (r *Reconciler) PrimaryLeaseStatus(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	holder string,
) (topology.PrimaryLeaseStatus, error) {
	if !cluster.IsPrimaryLeaseEnabled() {
		return topology.PrimaryLeaseStatus{}, nil
	}
	lease := &coordinationv1.Lease{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: primaryLeaseName(cluster)}
	if err := r.client.Get(ctx, key, lease); err != nil {
		if apierrors.IsNotFound(err) {
			return topology.PrimaryLeaseStatus{}, nil
		}
		return topology.PrimaryLeaseStatus{}, err
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != holder || lease.Spec.RenewTime == nil {
		return topology.PrimaryLeaseStatus{}, nil
	}
	duration := primaryLeaseDuration
	if lease.Spec.LeaseDurationSeconds != nil {
		duration = time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second
	}
	if time.Since(lease.Spec.RenewTime.Time) > duration {
		return topology.PrimaryLeaseStatus{}, nil
	}
	return topology.PrimaryLeaseStatus{Held: true, RetryAfter: duration}, nil
}
