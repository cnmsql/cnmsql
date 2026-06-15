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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

var _ = Describe("buildRoutingService", func() {
	cluster := &mysqlv1alpha1.Cluster{}
	cluster.Name = scheduledTestCluster
	cluster.Namespace = "ns"

	It("builds a default rw service with the primary selector", func() {
		svc := buildRoutingService(cluster, scheduledTestCluster+"-rw", mysqlv1alpha1.ServiceSelectorTypeRW, nil, mysqlv1alpha1.ServiceUpdateStrategyPatch)
		Expect(svc.Spec.Type).To(Equal(corev1.ServiceTypeClusterIP))
		Expect(svc.Spec.Selector).To(HaveKeyWithValue(roleLabel, rolePrimary))
		Expect(svc.Spec.PublishNotReadyAddresses).To(BeFalse())
		Expect(svc.Spec.Ports).To(HaveLen(1))
		Expect(svc.Spec.Ports[0].Port).To(Equal(int32(3306)))
	})

	It("publishes not-ready addresses for ro/r services", func() {
		ro := buildRoutingService(cluster, scheduledTestCluster+"-ro", mysqlv1alpha1.ServiceSelectorTypeRO, nil, mysqlv1alpha1.ServiceUpdateStrategyPatch)
		Expect(ro.Spec.Selector).To(HaveKeyWithValue(roleLabel, roleReplica))
		Expect(ro.Spec.PublishNotReadyAddresses).To(BeTrue())

		r := buildRoutingService(cluster, scheduledTestCluster+"-r", mysqlv1alpha1.ServiceSelectorTypeR, nil, mysqlv1alpha1.ServiceUpdateStrategyPatch)
		Expect(r.Spec.Selector).NotTo(HaveKey(roleLabel))
		Expect(r.Spec.PublishNotReadyAddresses).To(BeTrue())
	})

	It("patches template type, labels and annotations onto defaults", func() {
		lb := corev1.ServiceTypeLoadBalancer
		template := &mysqlv1alpha1.ServiceTemplateSpec{
			ObjectMeta: &mysqlv1alpha1.ObjectMetaTemplate{
				Labels:      map[string]string{"pool": "rw"},
				Annotations: map[string]string{"a": "b"},
			},
			Spec: &mysqlv1alpha1.ServiceTemplateServiceSpec{Type: &lb},
		}
		svc := buildRoutingService(cluster, scheduledTestCluster+"-rw", mysqlv1alpha1.ServiceSelectorTypeRW, template, mysqlv1alpha1.ServiceUpdateStrategyPatch)
		Expect(svc.Spec.Type).To(Equal(corev1.ServiceTypeLoadBalancer))
		Expect(svc.Labels).To(HaveKeyWithValue("pool", "rw"))
		// Operator labels survive a patch.
		Expect(svc.Labels).To(HaveKeyWithValue(clusterLabel, "demo"))
		Expect(svc.Annotations).To(HaveKeyWithValue("a", "b"))
	})

	It("replace strategy drops operator labels except owner-tracking ones", func() {
		template := &mysqlv1alpha1.ServiceTemplateSpec{
			ObjectMeta: &mysqlv1alpha1.ObjectMetaTemplate{Labels: map[string]string{"pool": "reporting"}},
		}
		svc := buildRoutingService(cluster, scheduledTestCluster+"-x", mysqlv1alpha1.ServiceSelectorTypeRO, template, mysqlv1alpha1.ServiceUpdateStrategyReplace)
		Expect(svc.Labels).To(HaveKeyWithValue("pool", "reporting"))
		Expect(svc.Labels).To(HaveKeyWithValue(clusterLabel, "demo"))
		Expect(svc.Labels).To(HaveKeyWithValue(roleLabel, "ro"))
		Expect(svc.Labels).NotTo(HaveKey("app.kubernetes.io/name"))
		// Selector is still operator-controlled.
		Expect(svc.Spec.Selector).To(HaveKeyWithValue(roleLabel, roleReplica))
	})
})
