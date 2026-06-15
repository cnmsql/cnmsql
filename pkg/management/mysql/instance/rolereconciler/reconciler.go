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
}

const (
	// waitRequeue paces reconciles while waiting for a transition (catch-up,
	// promotion by another instance).
	waitRequeue = 2 * time.Second
	// steadyRequeue re-checks role periodically even without a Cluster event.
	steadyRequeue = 30 * time.Second
)

// Reconciler keeps the local instance's role in sync with the Cluster status.
type Reconciler struct {
	client.Client
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

	status, err := r.Local.Status(ctx)
	if err != nil {
		// mysqld not reachable yet; try again shortly.
		return ctrl.Result{RequeueAfter: waitRequeue}, nil //nolint:nilerr // transient, retried
	}

	me := r.InstanceName
	target := cluster.Status.TargetPrimary
	current := cluster.Status.CurrentPrimary
	amPrimary := status.Role == webserver.RolePrimary

	// I am fenced: stay read-only regardless of role so I accept no writes and the
	// continuous archiver (which only ships from a writable primary) stands down.
	// The operator has already pulled me out of routing. Clearing the fence lets a
	// later reconcile resume normal role convergence.
	if isFenced(cluster, me) {
		if amPrimary {
			_ = r.releaseLease(ctx)
			_ = r.Local.Demote(ctx)
		}
		log.Info("Instance is fenced; staying read-only", "instance", me)
		return ctrl.Result{RequeueAfter: steadyRequeue}, nil
	}

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
