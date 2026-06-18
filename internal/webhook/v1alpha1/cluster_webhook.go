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

package v1alpha1

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

var clusterlog = logf.Log.WithName("cluster-status-validator")

// +kubebuilder:webhook:path=/validate-mysql-cloudnative-mysql-io-v1alpha1-cluster-status,mutating=false,failurePolicy=fail,sideEffects=None,admissionReviewVersions=v1,groups=mysql.cloudnative-mysql.io,resources=clusters,verbs=update,versions=v1alpha1,name=vclusterstatus-v1alpha1.cloudnative-mysql.io

// SetupClusterWebhookWithManager registers the validating webhook for Cluster status updates.
func SetupClusterWebhookWithManager(mgr ctrl.Manager) error {
	mgr.GetWebhookServer().Register(
		"/validate-mysql-cloudnative-mysql-io-v1alpha1-cluster-status",
		&admission.Webhook{
			Handler: &ClusterStatusValidator{Decoder: admission.NewDecoder(mgr.GetScheme())},
		},
	)
	return nil
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

	instanceName, isInstance := instanceIdentity(req.UserInfo.Username, req.Name, req.Namespace)
	if !isInstance {
		// Non-instance callers are subject to normal RBAC only.
		return admission.Allowed("")
	}

	if req.SubResource != "status" {
		return admission.Denied(fmt.Sprintf("instance %q is not allowed to modify Cluster %q subresource %q", instanceName, req.Name, req.SubResource))
	}

	oldCluster := &mysqlv1alpha1.Cluster{}
	if len(req.OldObject.Raw) > 0 {
		if err := v.Decoder.DecodeRaw(req.OldObject, oldCluster); err != nil {
			return admission.Errored(400, fmt.Errorf("could not decode old Cluster object: %w", err))
		}
	}
	if len(req.Object.Raw) == 0 {
		return admission.Errored(400, fmt.Errorf("admission request did not contain a Cluster object"))
	}
	newCluster := &mysqlv1alpha1.Cluster{}
	if err := v.Decoder.DecodeRaw(req.Object, newCluster); err != nil {
		return admission.Errored(400, fmt.Errorf("could not decode new Cluster object: %w", err))
	}

	oldStatus := oldCluster.Status
	newStatus := newCluster.Status

	// If currentPrimary or its timestamp are touched, the new primary must be the caller
	// and must match the operator-designated target.
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
