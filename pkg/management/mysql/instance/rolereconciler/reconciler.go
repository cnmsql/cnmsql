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

// Package rolereconciler implements the in-Pod controller that watches the
// owning Cluster and drives the local mysqld to match status.targetPrimary /
// status.currentPrimary. It is the CNPG "pull" model: the operator decides who
// should be primary by writing status.targetPrimary; each instance promotes
// itself or follows the current primary on its own. This removes the immutable
// --role flag and keeps role correct across Pod restarts.
package rolereconciler

import (
	"context"
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/groupreplication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/replication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
)

// LocalInstance drives the local mysqld. It is implemented by
// *instance.Controller.
type LocalInstance interface {
	// Status reports the live role, GTID and replication state.
	Status(ctx context.Context) (*webserver.Status, error)
	// Promote turns a replica into a writable primary.
	Promote(ctx context.Context) error
	// Demote makes the instance read-only (without touching replication config).
	Demote(ctx context.Context) error
	// EnsureReplicaConfigured points the instance at the given source and starts
	// replication.
	EnsureReplicaConfigured(ctx context.Context, source replication.SourceOptions) error
	// Shutdown stops mysqld so the Pod restarts clean (demotion fallback).
	Shutdown(ctx context.Context) error
	// Fence stops mysqld while keeping the manager alive (fenced instance).
	Fence(ctx context.Context) error
	// Unfence restarts mysqld after a fence is cleared. It is a no-op when the
	// instance is not fenced.
	Unfence(ctx context.Context) error

	// GroupView reports the local member's view of the Group Replication group.
	// Used only by the group role strategy.
	GroupView(ctx context.Context) (groupreplication.GroupView, error)
	// PrepareGroupJoin readies the member for distributed recovery (clears local
	// GTIDs and forces a clone for a fresh member, sets the recovery-channel
	// account) before joining with StartGroupReplication.
	PrepareGroupJoin(ctx context.Context, user, password string) error
	// StartGroupReplication joins an existing group (no bootstrap).
	StartGroupReplication(ctx context.Context) error
	// BootstrapGroup runs the exactly-once group-creation sequence on the
	// designated bootstrap member.
	BootstrapGroup(ctx context.Context) error
}

const (
	// waitRequeue paces reconciles while waiting for a transition (catch-up,
	// promotion by another instance).
	waitRequeue = 2 * time.Second
	// steadyRequeue re-checks role periodically even without a Cluster event.
	steadyRequeue = 30 * time.Second
	// groupObservationRequeue watches the cheap local GR snapshot closely enough
	// to turn elections and membership changes into Kubernetes Pod events.
	groupObservationRequeue = time.Second
)

// Reconciler keeps the local instance's role in sync with the Cluster status.
type Reconciler struct {
	client.Client
	// DoorbellClient bypasses the informer cache when patching this instance's
	// Pod. The role-manager cache is deliberately scoped to Cluster and Lease.
	DoorbellClient client.Client
	// ClusterKey identifies the owning Cluster.
	ClusterKey types.NamespacedName
	// InstanceName is this instance's Pod/instance name.
	InstanceName string
	// ServiceDomain is appended to the primary name to build its host, e.g.
	// "<namespace>.svc".
	ServiceDomain string
	// SourceTemplate holds the static replication connection parameters (user,
	// port, TLS). The source host is filled dynamically from currentPrimary.
	SourceTemplate replication.SourceOptions
	// Local drives the local mysqld.
	Local LocalInstance
	// primaryLeaseEnabled controls the optional primary Lease fencing layer.
	primaryLeaseEnabled bool
}

// Reconcile drives one role reconciliation pass.
func (r *Reconciler) Reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	cluster := &mysqlv1alpha1.Cluster{}
	if err := r.Get(ctx, r.ClusterKey, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	r.primaryLeaseEnabled = cluster.Spec.EnablePrimaryLease == nil || *cluster.Spec.EnablePrimaryLease

	me := r.InstanceName

	// Fencing is handled before anything else: a fenced instance has mysqld
	// stopped, so the Status call below would fail. The operator has already
	// pulled this Pod out of routing; here the in-Pod side releases the primary
	// lease (so a fenced primary stops anchoring writes) and stops mysqld while
	// the manager stays alive. Fence is idempotent, so this re-asserts the
	// stopped state on every resync until the fence is cleared.
	if isFenced(cluster, me) {
		_ = r.releaseLease(ctx)
		if err := r.Local.Fence(ctx); err != nil {
			log.Error(err, "Could not fence instance; will retry", "instance", me)
			return ctrl.Result{RequeueAfter: waitRequeue}, nil
		}
		log.Info("Instance is fenced; mysqld stopped", "instance", me)
		return ctrl.Result{RequeueAfter: steadyRequeue}, nil
	}

	// Not fenced: if a prior fence stopped mysqld, restart it before reconciling
	// role. Unfence is a no-op when the instance was never fenced.
	if err := r.Local.Unfence(ctx); err != nil {
		log.Error(err, "Could not unfence instance; will retry", "instance", me)
		return ctrl.Result{RequeueAfter: waitRequeue}, nil
	}

	status, err := r.Local.Status(ctx)
	if err != nil {
		// mysqld not reachable yet; try again shortly.
		return ctrl.Result{RequeueAfter: waitRequeue}, nil //nolint:nilerr // transient, retried
	}

	// Topology strategy split: under Group Replication the group elects the
	// primary and the operator reflects it; the in-Pod side only ensures
	// membership and never self-promotes or writes Cluster status. The async path
	// below is unchanged.
	if cluster.ReplicationMode() == mysqlv1alpha1.ReplicationModeGroupReplication {
		return r.reconcileGroupRole(ctx, cluster, status)
	}
	return r.reconcileAsyncRole(ctx, cluster, status)
}

// reconcileAsyncRole is the asynchronous topology role strategy: the CNPG
// pull-model where an instance promotes itself when it is the target and follows
// the current primary otherwise. Its behaviour is unchanged from before the
// topology split.
func (r *Reconciler) reconcileAsyncRole(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	status *webserver.Status,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	me := r.InstanceName

	target := cluster.Status.TargetPrimary
	current := cluster.Status.CurrentPrimary
	amPrimary := status.Role == webserver.RolePrimary

	// I am the designated primary.
	if target == me {
		// Already a writable primary: just keep currentPrimary in step.
		if amPrimary && !status.ReadOnly && !status.SuperReadOnly {
			if err := r.acquireOrRenewLease(ctx); err != nil {
				log.Error(err, "Could not secure primary lease; will retry", "instance", me)
				return ctrl.Result{RequeueAfter: waitRequeue}, nil
			}
			if current != me {
				return ctrl.Result{RequeueAfter: steadyRequeue}, r.setCurrentPrimary(ctx, me)
			}
			return ctrl.Result{RequeueAfter: steadyRequeue}, nil
		}
		// A replica must drain its relay log before promoting so we do not lose
		// received transactions. For a switchover the old primary is read-only and
		// this converges; for a failover the source is gone and the relay drains.
		if !amPrimary && !caughtUp(status) {
			log.Info("Waiting to catch up before promotion", "instance", me)
			return ctrl.Result{RequeueAfter: waitRequeue}, nil
		}
		if err := r.acquireOrRenewLease(ctx); err != nil {
			log.Error(err, "Could not secure primary lease; will retry", "instance", me)
			return ctrl.Result{RequeueAfter: waitRequeue}, nil
		}
		// Promote: stop/reset any replication and clear read-only. Idempotent on a
		// primary that merely booted read-only.
		if err := r.Local.Promote(ctx); err != nil {
			return ctrl.Result{}, err
		}
		log.Info("Ensured self is the writable primary", "instance", me)
		return ctrl.Result{RequeueAfter: waitRequeue}, r.setCurrentPrimary(ctx, me)
	}

	// I am not the designated primary: I must be a replica of the current
	// primary. A diverged former primary stays read-only and does not follow.
	if isDiverged(cluster, me) {
		if amPrimary {
			_ = r.releaseLease(ctx)
			_ = r.Local.Demote(ctx)
		}
		log.Info("Instance is diverged; staying read-only, not following", "instance", me)
		return ctrl.Result{RequeueAfter: steadyRequeue}, nil
	}

	// The new primary is not known yet, or I am still the current primary waiting
	// to be superseded: stop accepting writes and wait.
	if current == "" || current == me {
		if amPrimary {
			if err := r.releaseLease(ctx); err != nil {
				log.Error(err, "Could not release primary lease during demotion", "instance", me)
			}
			if err := r.Local.Demote(ctx); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: waitRequeue}, nil
	}

	// Follow the current primary.
	source := r.sourceFor(current)
	if amPrimary {
		// Former primary: demote then follow live. If live demotion fails, fall
		// back to a restart so the Pod comes back clean as a replica.
		if err := r.releaseLease(ctx); err != nil {
			log.Error(err, "Could not release primary lease during demotion", "instance", me)
		}
		if err := r.Local.Demote(ctx); err != nil {
			log.Error(err, "Live demotion failed; requesting shutdown to rejoin clean", "instance", me)
			return ctrl.Result{}, r.Local.Shutdown(ctx)
		}
	}
	if err := r.Local.EnsureReplicaConfigured(ctx, source); err != nil {
		if amPrimary {
			log.Error(err, "Configuring replication failed; requesting shutdown to rejoin clean", "instance", me)
			return ctrl.Result{}, r.Local.Shutdown(ctx)
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: steadyRequeue}, nil
}

// setCurrentPrimary patches status.currentPrimary to this instance. Only the
// promoting instance writes currentPrimary, so there is a single writer.
func (r *Reconciler) setCurrentPrimary(ctx context.Context, me string) error {
	cluster := &mysqlv1alpha1.Cluster{}
	if err := r.Get(ctx, r.ClusterKey, cluster); err != nil {
		return err
	}
	if cluster.Status.CurrentPrimary == me {
		return nil
	}
	before := cluster.DeepCopy()
	cluster.Status.CurrentPrimary = me
	cluster.Status.CurrentPrimaryTimestamp = metav1.Now().Format(time.RFC3339)
	return r.Status().Patch(ctx, cluster, client.MergeFrom(before))
}

func (r *Reconciler) sourceFor(primary string) replication.SourceOptions {
	source := r.SourceTemplate
	source.Host = primary + "." + r.ServiceDomain
	return source
}

// caughtUp reports whether the replica has applied everything it has received
// and is not lagging, so it is safe to promote.
func caughtUp(status *webserver.Status) bool {
	repl := status.Replication
	if repl == nil {
		// Not configured as a replica: nothing to wait for.
		return true
	}
	if !repl.SQLRunning {
		return false
	}
	applied, err := replication.GTIDContains(status.GTIDExecuted, repl.RetrievedGTIDSet)
	if err != nil || !applied {
		return false
	}
	if repl.SecondsBehindSource != nil && *repl.SecondsBehindSource > 0 {
		return false
	}
	return true
}

func isDiverged(cluster *mysqlv1alpha1.Cluster, name string) bool {
	return slices.Contains(cluster.Status.DivergedInstances, name)
}

func isFenced(cluster *mysqlv1alpha1.Cluster, name string) bool {
	return slices.Contains(cluster.Status.FencedInstances, name)
}

// SetupWithManager wires the reconciler to watch only the owning Cluster.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mysqlv1alpha1.Cluster{}).
		Named("instance-role").
		Complete(r)
}
