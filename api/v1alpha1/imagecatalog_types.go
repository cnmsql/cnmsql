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
)

// ImageCatalogSpec is the shared spec for ImageCatalog and ClusterImageCatalog.
type ImageCatalogSpec struct {
	// Images is the list of major version to container image mappings. Each
	// major version must appear at most once.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=8
	// +listType=map
	// +listMapKey=major
	Images []CatalogImage `json:"images"`
}

// CatalogImage maps a MySQL major version to a container image.
type CatalogImage struct {
	// Major is the MySQL major version (e.g. 8 for 8.0/8.4 lines uses the full
	// version where needed; values map to the image's server version).
	// +kubebuilder:validation:Minimum=5
	// +kubebuilder:validation:Required
	Major int `json:"major"`

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
