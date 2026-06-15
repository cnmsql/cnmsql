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

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=myclusterimagecatalog

// ClusterImageCatalog is the Schema for the clusterimagecatalogs API
// (cluster-scoped).
type ClusterImageCatalog struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of ClusterImageCatalog
	// +required
	Spec ImageCatalogSpec `json:"spec"`
}

// +kubebuilder:object:root=true

// ClusterImageCatalogList contains a list of ClusterImageCatalog.
type ClusterImageCatalogList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterImageCatalog `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ClusterImageCatalog{}, &ClusterImageCatalogList{})
}
