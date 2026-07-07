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
	"maps"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

var podlog = logf.Log.WithName("instance-pod-validator")

// These annotation keys mirror the constants in internal/controller: the in-Pod
// Group Replication reconciler is the only thing an instance identity is allowed
// to change on its own Pod. groupObservationAnnotation is the doorbell an
// instance rings after a local group-view change; the two force-* keys are
// operator-issued commands the instance may only *acknowledge* by clearing them.
const (
	groupObservationAnnotation      = "mysql.cnmsql.co/gr-observed"
	forceQuorumMembersAnnotation    = "cnmsql.cnmsql.co/force-quorum-members"
	forceGroupRebootstrapAnnotation = "cnmsql.cnmsql.co/force-group-rebootstrap"
)

// +kubebuilder:webhook:path=/validate--v1-pod,mutating=false,failurePolicy=fail,sideEffects=None,admissionReviewVersions=v1,groups="",resources=pods,verbs=update,versions=v1,name=vinstancepod-v1alpha1.cnmsql.co

// InstancePodValidator constrains what an instance ServiceAccount may change on
// its own Pod. The per-instance Role grants get/patch on the Pod that bears the
// instance's name (via resourceNames), so RBAC already prevents an instance from
// patching *another* Pod. RBAC cannot express field-level access, so this webhook
// is the field-level gate: an instance may only touch the Group Replication
// doorbell annotations on its own Pod, and nothing else — not the container
// images, not ephemeral containers or any other spec field, not labels,
// ownerReferences, finalizers, nor any operator-trusted annotation.
type InstancePodValidator struct {
	Decoder admission.Decoder
}

// Handle implements admission.Handler.
func (v *InstancePodValidator) Handle(_ context.Context, req admission.Request) admission.Response {
	podlog.V(1).Info("Validating Pod update", "pod", req.Name, "user", req.UserInfo.Username)

	// The webhook is scoped to the pods resource (never pods/status), so the
	// kubelet's high-frequency status heartbeats do not reach here. A stray call
	// for a subresource is not something an instance identity may perform.
	newPod := &corev1.Pod{}
	if err := v.Decoder.DecodeRaw(req.Object, newPod); err != nil {
		return admission.Errored(400, fmt.Errorf("could not decode Pod object: %w", err))
	}
	oldPod := &corev1.Pod{}
	if len(req.OldObject.Raw) > 0 {
		if err := v.Decoder.DecodeRaw(req.OldObject, oldPod); err != nil {
			return admission.Errored(400, fmt.Errorf("could not decode old Pod object: %w", err))
		}
	}

	// Derive the cluster from the stored (old) object; the requester cannot forge
	// it because the old object is what the API server already persisted.
	clusterName := oldPod.Labels[mysqlv1alpha1.ClusterLabelName]
	instanceName, isInstance := instanceIdentity(req.UserInfo.Username, clusterName, req.Namespace)
	if !isInstance {
		// Non-instance callers (the operator reconciling annotations/labels, the
		// kubelet, humans) are governed by normal Kubernetes RBAC.
		return admission.Allowed("")
	}

	// Defence in depth: RBAC resourceNames already binds the instance identity to
	// the Pod of the same name, but re-assert it here so the field rules below can
	// never be reasoned about against someone else's Pod.
	if instanceName != req.Name {
		return admission.Denied(fmt.Sprintf("instance %q may only patch its own Pod, not %q", instanceName, req.Name))
	}

	// An instance may not touch the Pod spec at all: this blocks container image
	// swaps, ephemeral containers, and every other spec mutation.
	if !reflect.DeepEqual(oldPod.Spec, newPod.Spec) {
		return admission.Denied(fmt.Sprintf("instance %q may not modify its Pod spec", instanceName))
	}
	// Nor the identity-bearing metadata the operator and Services rely on.
	if !reflect.DeepEqual(oldPod.Labels, newPod.Labels) {
		return admission.Denied(fmt.Sprintf("instance %q may not modify its Pod labels", instanceName))
	}
	if !reflect.DeepEqual(oldPod.OwnerReferences, newPod.OwnerReferences) {
		return admission.Denied(fmt.Sprintf("instance %q may not modify its Pod ownerReferences", instanceName))
	}
	if !reflect.DeepEqual(oldPod.Finalizers, newPod.Finalizers) {
		return admission.Denied(fmt.Sprintf("instance %q may not modify its Pod finalizers", instanceName))
	}

	if msg := validateInstanceAnnotations(instanceName, oldPod.Annotations, newPod.Annotations); msg != "" {
		return admission.Denied(msg)
	}

	return admission.Allowed("")
}

// validateInstanceAnnotations enforces the only annotation deltas an instance may
// make on its own Pod. It returns a non-empty denial message on violation.
//
//   - groupObservationAnnotation: freely writable — it is a low-trust doorbell the
//     operator reads and then re-derives ground truth from MySQL.
//   - force-* command annotations: the instance may only *clear* them (present →
//     absent/empty) to acknowledge an operator command it has executed. It may not
//     set or change them, which would otherwise let a compromised member trigger
//     its own quorum-force or group re-bootstrap and fork the group.
//   - every other annotation must be byte-identical.
func validateInstanceAnnotations(instanceName string, oldAnn, newAnn map[string]string) string {
	old := copyAnnotations(oldAnn)
	updated := copyAnnotations(newAnn)

	// The doorbell is fully owned by the instance; ignore it in the diff.
	delete(old, groupObservationAnnotation)
	delete(updated, groupObservationAnnotation)

	for _, key := range []string{forceQuorumMembersAnnotation, forceGroupRebootstrapAnnotation} {
		if newVal, ok := updated[key]; ok && newVal != "" && newVal != old[key] {
			return fmt.Sprintf("instance %q may not set operator command annotation %q; it may only clear it", instanceName, key)
		}
		delete(old, key)
		delete(updated, key)
	}

	if !reflect.DeepEqual(old, updated) {
		return fmt.Sprintf("instance %q may only modify the Group Replication doorbell annotations on its own Pod", instanceName)
	}
	return ""
}

func copyAnnotations(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	maps.Copy(out, in)
	return out
}
