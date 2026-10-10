/*
Copyright 2026 The CNMSQL - CloudNative for MySQL Authors.

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

// handoffPositionAnnotation records, on the primary Lease, the executed GTID
// set an outgoing primary had once it was read-only. The instance taking over
// does not promote until it has applied that set, so a transaction the old
// primary committed just before stepping down, which the target may not have
// received yet, is not lost.
const handoffPositionAnnotation = "cnmsql.cnmsql.co/handoff-position"

var errPrimaryLeaseHeld = errors.New("primary lease is held by another instance")

func (r *Reconciler) primaryLeaseName() string {
	return r.ClusterKey.Name + "-primary"
}

// acquireOrRenewLease takes or renews the primary Lease for this instance and
// returns the handoff position the previous holder recorded, if any. A settled
// (writable) primary passes settled=true to drop that position, which only
// matters to the instance taking over.
func (r *Reconciler) acquireOrRenewLease(ctx context.Context, settled bool) (string, error) {
	if !r.primaryLeaseEnabled {
		return "", nil
	}
	key := types.NamespacedName{Namespace: r.ClusterKey.Namespace, Name: r.primaryLeaseName()}
	lease := &coordinationv1.Lease{}
	create := false
	if err := r.Get(ctx, key, lease); err != nil {
		if !apierrors.IsNotFound(err) {
			return "", err
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
		return "", errPrimaryLeaseHeld
	}
	handoff := lease.Annotations[handoffPositionAnnotation]
	if settled && handoff != "" {
		delete(lease.Annotations, handoffPositionAnnotation)
		handoff = ""
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
		return handoff, r.Create(ctx, lease)
	}
	return handoff, r.Update(ctx, lease)
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

// releaseLease gives up the primary Lease if this instance holds it, recording
// handoff as the position the next primary must apply first. An empty handoff
// keeps whatever position is already recorded: a target that held the Lease
// while waiting for one must not erase it for the next candidate. The Lease is
// kept and only its holder cleared: the instance's RBAC cannot create it again,
// only the operator can.
func (r *Reconciler) releaseLease(ctx context.Context, handoff string) error {
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
	lease.Spec.HolderIdentity = nil
	lease.Spec.RenewTime = nil
	if handoff != "" {
		if lease.Annotations == nil {
			lease.Annotations = map[string]string{}
		}
		lease.Annotations[handoffPositionAnnotation] = handoff
	}
	return client.IgnoreNotFound(r.Update(ctx, lease))
}
