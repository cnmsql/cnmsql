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
