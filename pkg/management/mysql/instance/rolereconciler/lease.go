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

package rolereconciler

import (
	"context"
	"errors"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const defaultLeaseDuration = 15 * time.Second

var errPrimaryLeaseHeld = errors.New("primary lease is held by another instance")

func (r *Reconciler) primaryLeaseName() string {
	return r.ClusterKey.Name + "-primary"
}

func (r *Reconciler) acquireOrRenewLease(ctx context.Context) error {
	if !r.primaryLeaseEnabled {
		return nil
	}
	key := types.NamespacedName{Namespace: r.ClusterKey.Namespace, Name: r.primaryLeaseName()}
	lease := &coordinationv1.Lease{}
	create := false
	if err := r.Get(ctx, key, lease); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		create = true
		seconds := int32(defaultLeaseDuration / time.Second)
		lease = &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
			Spec: coordinationv1.LeaseSpec{
				LeaseDurationSeconds: &seconds,
			},
		}
	}
	previousHolder := ""
	if lease.Spec.HolderIdentity != nil {
		previousHolder = *lease.Spec.HolderIdentity
	}
	if previousHolder != "" && previousHolder != r.InstanceName && !leaseExpired(lease) {
		return errPrimaryLeaseHeld
	}
	now := metav1.MicroTime{Time: time.Now()}
	holder := r.InstanceName
	lease.Spec.HolderIdentity = &holder
	lease.Spec.RenewTime = &now
	if lease.Spec.AcquireTime == nil || previousHolder != holder {
		lease.Spec.AcquireTime = &now
		transitions := int32(0)
		if lease.Spec.LeaseTransitions != nil {
			transitions = *lease.Spec.LeaseTransitions
		}
		transitions++
		lease.Spec.LeaseTransitions = &transitions
	}
	if lease.Spec.LeaseDurationSeconds == nil {
		seconds := int32(defaultLeaseDuration / time.Second)
		lease.Spec.LeaseDurationSeconds = &seconds
	}
	if create {
		return r.Create(ctx, lease)
	}
	return r.Update(ctx, lease)
}

func leaseExpired(lease *coordinationv1.Lease) bool {
	if lease.Spec.RenewTime == nil {
		return true
	}
	duration := defaultLeaseDuration
	if lease.Spec.LeaseDurationSeconds != nil {
		duration = time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second
	}
	return time.Since(lease.Spec.RenewTime.Time) > duration
}

func (r *Reconciler) releaseLease(ctx context.Context) error {
	if !r.primaryLeaseEnabled {
		return nil
	}
	lease := &coordinationv1.Lease{}
	key := types.NamespacedName{Namespace: r.ClusterKey.Namespace, Name: r.primaryLeaseName()}
	if err := r.Get(ctx, key, lease); err != nil {
		return client.IgnoreNotFound(err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != r.InstanceName {
		return nil
	}
	return client.IgnoreNotFound(r.Delete(ctx, lease))
}
