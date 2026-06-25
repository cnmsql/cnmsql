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

// FindImageForSeries returns the image mapped to the given MySQL series (e.g.
// "8.4") and whether it was found.
func (spec *ImageCatalogSpec) FindImageForSeries(series string) (string, bool) {
	for _, entry := range spec.Images {
		if entry.Series == series {
			return entry.Image, true
		}
	}
	return "", false
}

var (
	_ GenericImageCatalog = &ImageCatalog{}
	_ GenericImageCatalog = &ClusterImageCatalog{}
)
