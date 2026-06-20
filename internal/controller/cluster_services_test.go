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

	It("excludes not-ready members from ro/r under Group Replication", func() {
		// Under GR, readiness tracks the member's ONLINE state, so ro/r must not
		// publish a non-ONLINE (not-ready) member that cannot serve consistent reads.
		grCluster := &mysqlv1alpha1.Cluster{}
		grCluster.Name = scheduledTestCluster
		grCluster.Namespace = "ns"
		grCluster.Spec.Replication = &mysqlv1alpha1.ReplicationConfiguration{
			Mode: mysqlv1alpha1.ReplicationModeGroupReplication,
		}

		ro := buildRoutingService(grCluster, scheduledTestCluster+"-ro", mysqlv1alpha1.ServiceSelectorTypeRO, nil, mysqlv1alpha1.ServiceUpdateStrategyPatch)
		Expect(ro.Spec.PublishNotReadyAddresses).To(BeFalse())

		r := buildRoutingService(grCluster, scheduledTestCluster+"-r", mysqlv1alpha1.ServiceSelectorTypeR, nil, mysqlv1alpha1.ServiceUpdateStrategyPatch)
		Expect(r.Spec.PublishNotReadyAddresses).To(BeFalse())

		rw := buildRoutingService(grCluster, scheduledTestCluster+"-rw", mysqlv1alpha1.ServiceSelectorTypeRW, nil, mysqlv1alpha1.ServiceUpdateStrategyPatch)
		Expect(rw.Spec.PublishNotReadyAddresses).To(BeFalse())
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
