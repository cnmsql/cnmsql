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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// +kubebuilder:object:generate=false

// GenericImageCatalog is implemented by both ImageCatalog and
// ClusterImageCatalog so that callers can resolve an image for a major version
// regardless of the catalog scope.
type GenericImageCatalog interface {
	runtime.Object
	metav1.Object
	// GetSpec returns the shared catalog spec.
	GetSpec() *ImageCatalogSpec
}

// GetSpec returns the catalog spec.
func (c *ImageCatalog) GetSpec() *ImageCatalogSpec {
	return &c.Spec
}

// GetSpec returns the catalog spec.
func (c *ClusterImageCatalog) GetSpec() *ImageCatalogSpec {
	return &c.Spec
}

// FindImageForMajor returns the image mapped to the given major version and
// whether it was found.
func (spec *ImageCatalogSpec) FindImageForMajor(major int) (string, bool) {
	for _, entry := range spec.Images {
		if entry.Major == major {
			return entry.Image, true
		}
	}
	return "", false
}

var (
	_ GenericImageCatalog = &ImageCatalog{}
	_ GenericImageCatalog = &ClusterImageCatalog{}
)
