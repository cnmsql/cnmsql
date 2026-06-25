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

package v1alpha1

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

var clusterlog = logf.Log.WithName("cluster-status-validator")

var clusterspeclog = logf.Log.WithName("cluster-spec-validator")

// +kubebuilder:webhook:path=/validate-mysql-cnmsql-io-v1alpha1-cluster-status,mutating=false,failurePolicy=fail,sideEffects=None,admissionReviewVersions=v1,groups=mysql.cnmsql.co,resources=clusters,verbs=update,versions=v1alpha1,name=vclusterstatus-v1alpha1.cnmsql.co
// +kubebuilder:webhook:path=/validate-mysql-cnmsql-io-v1alpha1-cluster-spec,mutating=false,failurePolicy=fail,sideEffects=None,admissionReviewVersions=v1,groups=mysql.cnmsql.co,resources=clusters,verbs=create;update,versions=v1alpha1,name=vclustervalidation-v1alpha1.cnmsql.co

// SetupClusterWebhookWithManager registers the validating webhooks for Cluster
// status updates (least-privilege per-instance authz) and for Cluster spec
// create/update (field and upgrade-transition validation).
func SetupClusterWebhookWithManager(mgr ctrl.Manager) error {
	mgr.GetWebhookServer().Register(
		"/validate-mysql-cnmsql-io-v1alpha1-cluster-status",
		&admission.Webhook{
			Handler: &ClusterStatusValidator{Decoder: admission.NewDecoder(mgr.GetScheme())},
		},
	)
	mgr.GetWebhookServer().Register(
		"/validate-mysql-cnmsql-io-v1alpha1-cluster-spec",
		&admission.Webhook{
			Handler: &ClusterSpecValidator{Decoder: admission.NewDecoder(mgr.GetScheme())},
		},
	)
	return nil
}

// ClusterSpecValidator validates Cluster spec on create and update: the
// field-level checks in Cluster.Validate plus, on update, the immutability and
// MySQL series-transition guards in Cluster.ValidateUpdate (no downgrade, no
// skipped series, major changes only through imageCatalogRef).
type ClusterSpecValidator struct {
	Decoder admission.Decoder
}

// Handle implements admission.Handler.
func (v *ClusterSpecValidator) Handle(_ context.Context, req admission.Request) admission.Response {
	// Status subresource updates are governed by ClusterStatusValidator; the spec
	// is unchanged there, so there is nothing for this webhook to validate.
	if req.SubResource == "status" {
		return admission.Allowed("")
	}

	newCluster := &mysqlv1alpha1.Cluster{}
	if err := v.Decoder.Decode(req, newCluster); err != nil {
		return admission.Errored(400, fmt.Errorf("could not decode Cluster object: %w", err))
	}

	allErrs := newCluster.Validate()
	if req.Operation == admissionv1.Update && len(req.OldObject.Raw) > 0 {
		oldCluster := &mysqlv1alpha1.Cluster{}
		if err := v.Decoder.DecodeRaw(req.OldObject, oldCluster); err != nil {
			return admission.Errored(400, fmt.Errorf("could not decode old Cluster object: %w", err))
		}
		allErrs = append(allErrs, newCluster.ValidateUpdate(oldCluster)...)
	}
	if len(allErrs) > 0 {
		clusterspeclog.V(1).Info("Rejecting Cluster spec", "cluster", req.Name, "errors", allErrs.ToAggregate().Error())
		return admission.Denied(allErrs.ToAggregate().Error())
	}
	return admission.Allowed("")
}

// ClusterStatusValidator validates Cluster status updates from instance
// ServiceAccounts. Each instance may only modify status.currentPrimary and
// status.currentPrimaryTimestamp, and only to promote itself.
type ClusterStatusValidator struct {
	Decoder admission.Decoder
}

// Handle implements admission.Handler.
func (v *ClusterStatusValidator) Handle(ctx context.Context, req admission.Request) admission.Response {
	clusterlog.V(1).Info("Validating Cluster status update", "cluster", req.Name, "user", req.UserInfo.Username)

	if len(req.Object.Raw) == 0 {
		return admission.Errored(400, fmt.Errorf("admission request did not contain a Cluster object"))
	}
	oldCluster := &mysqlv1alpha1.Cluster{}
	if len(req.OldObject.Raw) > 0 {
		if err := v.Decoder.DecodeRaw(req.OldObject, oldCluster); err != nil {
			return admission.Errored(400, fmt.Errorf("could not decode old Cluster object: %w", err))
		}
	}
	newCluster := &mysqlv1alpha1.Cluster{}
	if err := v.Decoder.DecodeRaw(req.Object, newCluster); err != nil {
		return admission.Errored(400, fmt.Errorf("could not decode new Cluster object: %w", err))
	}

	oldStatus := oldCluster.Status
	newStatus := newCluster.Status

	// Monotonic invariants on the two split-brain-critical Group Replication
	// fields are enforced for EVERY caller — the operator included — as defence in
	// depth against a bug or a compromised operator token. Re-arming bootstrap or
	// changing the group name is the path to a second, forked group.
	if msg := validateGroupReplicationMonotonic(oldStatus, newStatus); msg != "" {
		return admission.Denied(msg)
	}

	instanceName, isInstance := instanceIdentity(req.UserInfo.Username, req.Name, req.Namespace)
	if !isInstance {
		// Non-instance callers (notably the operator) are subject to normal RBAC
		// plus the monotonic invariants checked above.
		return admission.Allowed("")
	}

	if req.SubResource != "status" {
		return admission.Denied(fmt.Sprintf("instance %q is not allowed to modify Cluster %q subresource %q", instanceName, req.Name, req.SubResource))
	}

	// Under Group Replication the group elects the primary and the operator is the
	// sole writer of status (currentPrimary and the whole groupReplication block,
	// cross-validated across the group view). There is no self-promotion, so an
	// instance identity may write NOTHING to status: old and new must be identical.
	if newCluster.ReplicationMode() == mysqlv1alpha1.ReplicationModeGroupReplication {
		if !reflect.DeepEqual(&oldStatus, &newStatus) {
			return admission.Denied(fmt.Sprintf(
				"instance %q may not modify Cluster status under group replication (the operator is the sole writer)", instanceName))
		}
		return admission.Allowed("")
	}

	// Asynchronous topology: an instance may set its own currentPrimary (the
	// pull-model self-promotion path), gated on the operator-designated target.
	if newStatus.CurrentPrimary != oldStatus.CurrentPrimary ||
		newStatus.CurrentPrimaryTimestamp != oldStatus.CurrentPrimaryTimestamp {
		if newStatus.CurrentPrimary != instanceName {
			return admission.Denied(fmt.Sprintf("instance %q may only set status.currentPrimary to itself", instanceName))
		}
		if newStatus.CurrentPrimaryTimestamp == "" {
			return admission.Denied(fmt.Sprintf("instance %q must set status.currentPrimaryTimestamp when updating status.currentPrimary", instanceName))
		}
		if oldStatus.TargetPrimary != "" && newStatus.CurrentPrimary != oldStatus.TargetPrimary {
			return admission.Denied(fmt.Sprintf("instance %q may only promote itself when it is the designated target primary (%q), not %q",
				instanceName, oldStatus.TargetPrimary, newStatus.CurrentPrimary))
		}
	}

	// Strip the instance-owned fields and ensure nothing else changed.
	oldCopy := oldStatus.DeepCopy()
	newCopy := newStatus.DeepCopy()
	oldCopy.CurrentPrimary = ""
	oldCopy.CurrentPrimaryTimestamp = ""
	newCopy.CurrentPrimary = ""
	newCopy.CurrentPrimaryTimestamp = ""

	if !reflect.DeepEqual(oldCopy, newCopy) {
		return admission.Denied(fmt.Sprintf("instance %q is only allowed to modify status.currentPrimary and status.currentPrimaryTimestamp", instanceName))
	}

	return admission.Allowed("")
}

// validateGroupReplicationMonotonic enforces the two invariants that protect the
// group against a split-brain second group, for every caller. It returns a
// non-empty denial message when an invariant is violated.
//   - groupReplication.bootstrapped: false→true allowed, true→false denied
//     (total-outage recovery re-bootstraps the SAME group without clearing it).
//   - groupReplication.groupName: ""→value allowed, value→different denied.
func validateGroupReplicationMonotonic(oldStatus, newStatus mysqlv1alpha1.ClusterStatus) string {
	oldGR := oldStatus.GroupReplication
	newGR := newStatus.GroupReplication
	if oldGR == nil {
		// Nothing was set before; any initial value is allowed.
		return ""
	}
	if oldGR.Bootstrapped && (newGR == nil || !newGR.Bootstrapped) {
		return "status.groupReplication.bootstrapped is monotonic: it may not be cleared once set"
	}
	if oldGR.GroupName != "" {
		newName := ""
		if newGR != nil {
			newName = newGR.GroupName
		}
		if newName != "" && newName != oldGR.GroupName {
			return "status.groupReplication.groupName is immutable once set"
		}
	}
	return ""
}

// instanceIdentity extracts the instance name from a ServiceAccount identity of
// the form system:serviceaccount:<namespace>:<instance>-instance. The returned
// instance name is valid only if it belongs to the requested Cluster.
func instanceIdentity(username, clusterName, namespace string) (string, bool) {
	parts := strings.Split(username, ":")
	if len(parts) != 4 || parts[0] != "system" || parts[1] != "serviceaccount" {
		return "", false
	}
	if parts[2] != namespace {
		return "", false
	}
	account := parts[3]
	if !strings.HasSuffix(account, "-instance") {
		return "", false
	}
	instanceName := strings.TrimSuffix(account, "-instance")
	if !strings.HasPrefix(instanceName, clusterName+"-") {
		return "", false
	}
	// The suffix after "<cluster>-" must be a plain numeric ordinal, so that an
	// arbitrarily named ServiceAccount (e.g. "<cluster>-evil-instance") cannot
	// masquerade as a legitimate instance identity.
	ordinal := strings.TrimPrefix(instanceName, clusterName+"-")
	if ordinal == "" || strings.ContainsFunc(ordinal, func(r rune) bool { return r < '0' || r > '9' }) {
		return "", false
	}
	return instanceName, true
}
