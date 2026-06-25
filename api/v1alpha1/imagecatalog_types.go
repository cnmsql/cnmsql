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
)

// ImageCatalogSpec is the shared spec for ImageCatalog and ClusterImageCatalog.
type ImageCatalogSpec struct {
	// Images is the list of MySQL series to container image mappings. Each
	// series must appear at most once.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=8
	// +listType=map
	// +listMapKey=series
	Images []CatalogImage `json:"images"`
}

// CatalogImage maps a MySQL series to a container image. The series, not the
// integer major, is the upgrade unit: MySQL 8.0 and 8.4 are distinct upgrade
// targets that both live under integer major 8, so a catalog keyed by integer
// major could not express the 8.0 -> 8.4 hop.
type CatalogImage struct {
	// Series is the MySQL release series in "major.minor" form (e.g. "8.0",
	// "8.4", "9.0"). It must match the image's server version line.
	// +kubebuilder:validation:Pattern=`^[0-9]+\.[0-9]+$`
	// +kubebuilder:validation:Required
	Series string `json:"series"`

	// Image is the fully qualified Percona Server for MySQL image reference.
	// +kubebuilder:validation:Required
	Image string `json:"image"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=myimagecatalog

// ImageCatalog is the Schema for the imagecatalogs API (namespaced).
type ImageCatalog struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of ImageCatalog
	// +required
	Spec ImageCatalogSpec `json:"spec"`
}

// +kubebuilder:object:root=true

// ImageCatalogList contains a list of ImageCatalog.
type ImageCatalogList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ImageCatalog `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ImageCatalog{}, &ImageCatalogList{})
}
