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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ImageCatalog lookup", func() {
	spec := ImageCatalogSpec{
		Images: []CatalogImage{
			{Major: 8, Image: "percona/percona-server:8.0"},
			{Major: 9, Image: "percona/percona-server:9.0"},
		},
	}

	It("finds an image for a known major version", func() {
		image, found := spec.FindImageForMajor(8)
		Expect(found).To(BeTrue())
		Expect(image).To(Equal("percona/percona-server:8.0"))
	})

	It("reports a missing major version", func() {
		_, found := spec.FindImageForMajor(5)
		Expect(found).To(BeFalse())
	})

	It("exposes the spec through the generic interface", func() {
		catalogs := []GenericImageCatalog{
			&ImageCatalog{Spec: spec},
			&ClusterImageCatalog{Spec: spec},
		}
		for _, catalog := range catalogs {
			image, found := catalog.GetSpec().FindImageForMajor(9)
			Expect(found).To(BeTrue())
			Expect(image).To(Equal("percona/percona-server:9.0"))
		}
	})
})
