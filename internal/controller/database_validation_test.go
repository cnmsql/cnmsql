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

package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// Database has no webhook, so the inline-user name rule lives in the CRD
// schema (CEL) and is checked against a real API server.
var _ = Describe("Database admission", func() {
	ctx := context.Background()

	database := func(name, userName string) *mysqlv1alpha1.Database {
		return &mysqlv1alpha1.Database{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: mysqlv1alpha1.DatabaseSpec{
				Cluster: mysqlv1alpha1.LocalObjectReference{Name: "demo"},
				Name:    "app",
				Users:   []mysqlv1alpha1.InlineUser{{Name: userName}},
			},
		}
	}

	It("accepts an ordinary inline user", func() {
		db := database("inline-user-ok", "app")
		Expect(k8sClient.Create(ctx, db)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, db) })
	})

	DescribeTable("rejects reserved inline user names",
		func(name, userName string) {
			err := k8sClient.Create(ctx, database(name, userName))
			Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected Invalid, got %v", err)
			Expect(err.Error()).To(ContainSubstring("user name is reserved"))
		},
		Entry("operator dump account", "inline-user-dump", "cnmsql_dump"),
		Entry("any cnmsql_ account", "inline-user-control", "cnmsql_control"),
		Entry("root", "inline-user-root", "root"),
		Entry("mysql.* system account", "inline-user-sys", "mysql.sys"),
	)
})
