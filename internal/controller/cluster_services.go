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
	"fmt"
	"maps"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

// ensureDefaultServices reconciles the rw/ro/r traffic-routing Services,
// honouring spec.managed.services.disabledDefaultServices and applying the
// optional default-service template on top of each role's base spec.
func (r *ClusterReconciler) ensureDefaultServices(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) error {
	services := []struct {
		selector mysqlv1alpha1.ServiceSelectorType
		name     string
	}{
		{mysqlv1alpha1.ServiceSelectorTypeRW, plan.RWServiceName},
		{mysqlv1alpha1.ServiceSelectorTypeRO, plan.ROServiceName},
		{mysqlv1alpha1.ServiceSelectorTypeR, plan.RServiceName},
	}
	for _, svc := range services {
		if plan.DisabledServices[svc.selector] {
			if err := r.deleteService(ctx, cluster.Namespace, svc.name); err != nil {
				return err
			}
			continue
		}
		// Default services always use the patch strategy with the shared template.
		if err := r.ensureRoutingService(
			ctx, cluster, svc.name, svc.selector,
			plan.ServiceTemplate, mysqlv1alpha1.ServiceUpdateStrategyPatch,
		); err != nil {
			return err
		}
	}
	return r.ensureAdditionalServices(ctx, cluster, plan)
}

// ensureAdditionalServices reconciles the user-declared additional services,
// each fully-qualified as <cluster>-<entry.Name>. Collisions with the default
// service names are rejected.
func (r *ClusterReconciler) ensureAdditionalServices(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) error {
	defaults := map[string]bool{
		plan.RWServiceName: true,
		plan.ROServiceName: true,
		plan.RServiceName:  true,
	}
	for i := range plan.AdditionalServices {
		entry := &plan.AdditionalServices[i]
		name := fmt.Sprintf("%s-%s", cluster.Name, entry.Name)
		if defaults[name] {
			return fmt.Errorf("additional service %q collides with a default service name", name)
		}
		strategy := entry.UpdateStrategy
		if strategy == "" {
			strategy = mysqlv1alpha1.ServiceUpdateStrategyPatch
		}
		if err := r.ensureRoutingService(
			ctx, cluster, name, entry.SelectorType,
			&entry.ServiceTemplate, strategy,
		); err != nil {
			return err
		}
	}
	return nil
}

func (r *ClusterReconciler) ensureRoutingService(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	name string,
	role mysqlv1alpha1.ServiceSelectorType,
	template *mysqlv1alpha1.ServiceTemplateSpec,
	strategy mysqlv1alpha1.ServiceUpdateStrategy,
) error {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		desired := r.buildRoutingService(cluster, name, role, template, strategy)
		service.Labels = desired.Labels
		service.Annotations = desired.Annotations
		// Preserve the cluster-assigned ClusterIP across updates.
		clusterIP := service.Spec.ClusterIP
		clusterIPs := service.Spec.ClusterIPs
		service.Spec = desired.Spec
		service.Spec.ClusterIP = clusterIP
		service.Spec.ClusterIPs = clusterIPs
		return controllerutil.SetControllerReference(cluster, service, r.Scheme)
	})
	return err
}

// roleSelector returns the Pod selector for the given service role. The
// operator owns the selector; users cannot override it.
func roleSelector(cluster *mysqlv1alpha1.Cluster, role mysqlv1alpha1.ServiceSelectorType) map[string]string {
	// routable=true gates every routing Service so a fenced Pod (routable=false)
	// is dropped from rw/ro/r and user-defined services alike.
	selector := map[string]string{
		clusterLabel:  cluster.Name,
		routableLabel: routableTrue,
	}
	switch role {
	case mysqlv1alpha1.ServiceSelectorTypeRW:
		selector[roleLabel] = rolePrimary
	case mysqlv1alpha1.ServiceSelectorTypeRO:
		selector[roleLabel] = roleReplica
	}
	return selector
}

// buildRoutingService builds the desired Service for a routing role, applying
// the optional template with the given update strategy. The selector, ports and
// the mandatory operator labels are always operator-controlled.
func (r *ClusterReconciler) buildRoutingService(
	cluster *mysqlv1alpha1.Cluster,
	name string,
	role mysqlv1alpha1.ServiceSelectorType,
	template *mysqlv1alpha1.ServiceTemplateSpec,
	strategy mysqlv1alpha1.ServiceUpdateStrategy,
) *corev1.Service {
	selector := roleSelector(cluster, role)
	ports := []corev1.ServicePort{
		{Name: "mysql", Port: 3306, TargetPort: intstr.FromString("mysql")},
	}
	// The rw Service must never publish a not-ready primary; under async, ro/r
	// tolerate in-progress replicas so clients can discover them as they catch up.
	// Under Group Replication readiness tracks the member's group state (ONLINE),
	// and a non-ONLINE member (RECOVERING/ERROR/UNREACHABLE) does not serve
	// consistent reads, so ro/r must exclude not-ready members too — routing by
	// group role falls out of the readiness bridge.
	publishNotReady := r.topologyReconciler(cluster).PublishNotReadyAddresses(role)

	var labels, annotations map[string]string
	if strategy == mysqlv1alpha1.ServiceUpdateStrategyReplace {
		// Replace keeps only the mandatory operator labels for owner tracking.
		labels = map[string]string{
			clusterLabel: cluster.Name,
			roleLabel:    string(role),
		}
	} else {
		labels = labelsFor(cluster, "", "")
		labels[roleLabel] = string(role)
	}

	spec := corev1.ServiceSpec{
		Type:                     corev1.ServiceTypeClusterIP,
		Selector:                 selector,
		Ports:                    ports,
		PublishNotReadyAddresses: publishNotReady,
	}

	if template != nil {
		if meta := template.ObjectMeta; meta != nil {
			labels = mergeStringMaps(labels, meta.Labels)
			annotations = mergeStringMaps(annotations, meta.Annotations)
		}
		applyServiceSpecTemplate(&spec, template.Spec)
	}
	// The operator always owns the selector and ports; never user-settable.
	spec.Selector = selector
	spec.Ports = ports

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   cluster.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: spec,
	}
}

// applyServiceSpecTemplate overlays the user-customisable spec fields onto base.
func applyServiceSpecTemplate(base *corev1.ServiceSpec, t *mysqlv1alpha1.ServiceTemplateServiceSpec) {
	if t == nil {
		return
	}
	if t.Type != nil {
		base.Type = *t.Type
	}
	if t.ExternalTrafficPolicy != nil {
		base.ExternalTrafficPolicy = *t.ExternalTrafficPolicy
	}
	if t.SessionAffinity != nil {
		base.SessionAffinity = *t.SessionAffinity
	}
	if len(t.LoadBalancerSourceRanges) > 0 {
		base.LoadBalancerSourceRanges = t.LoadBalancerSourceRanges
	}
	if t.ExternalName != "" {
		base.ExternalName = t.ExternalName
	}
	if t.HealthCheckNodePort != nil {
		base.HealthCheckNodePort = *t.HealthCheckNodePort
	}
}

// mergeStringMaps returns base with overlay applied on top (overlay wins). A nil
// base is initialised when overlay is non-empty.
func mergeStringMaps(base, overlay map[string]string) map[string]string {
	if len(overlay) == 0 {
		return base
	}
	if base == nil {
		base = map[string]string{}
	}
	maps.Copy(base, overlay)
	return base
}

func (r *ClusterReconciler) deleteService(ctx context.Context, namespace, name string) error {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	if err := r.Delete(ctx, service); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}
