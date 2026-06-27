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
	corev1 "k8s.io/api/core/v1"
)

// LocalObjectReference contains the reference to a Kubernetes object in the same
// namespace, identified by name.
type LocalObjectReference struct {
	// Name of the referent
	// +kubebuilder:validation:Required
	Name string `json:"name"`
}

// SecretKeySelector selects a key from a Secret in the same namespace.
type SecretKeySelector struct {
	// Name of the Secret
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Key within the Secret to select
	// +kubebuilder:validation:Required
	Key string `json:"key"`
}

// ConfigMapKeySelector selects a key from a ConfigMap in the same namespace.
type ConfigMapKeySelector struct {
	// Name of the ConfigMap
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Key within the ConfigMap to select
	// +kubebuilder:validation:Required
	Key string `json:"key"`
}

// Metadata is a structure similar to the metav1.ObjectMeta, but still
// parseable by controller-gen to create a suitable CRD for the user. The
// comment of PodTemplateSpec has an explanation of why we are not using the
// core data types.
type Metadata struct {
	// The name of the resource. Only supported for certain types
	// +optional
	Name string `json:"name,omitempty"`

	// Map of string keys and values that can be used to organize and categorize
	// (scope and select) objects. May match selectors of replication controllers
	// and services.
	// More info: http://kubernetes.io/docs/user-guide/labels
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// Annotations is an unstructured key value map stored with a resource that
	// may be set by external tools to store and retrieve arbitrary metadata. They
	// are not queryable and should be preserved when modifying objects.
	// More info: http://kubernetes.io/docs/user-guide/annotations
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// EmbeddedObjectMetadata contains metadata to be inherited by all resources
// related to a Cluster.
type EmbeddedObjectMetadata struct {
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// StorageConfiguration is the configuration used to create and reconcile PVCs,
// usable for the instance data directory or for separate binlog storage.
type StorageConfiguration struct {
	// StorageClass to use for PVCs. Applied after evaluating the PVC template,
	// if available. If not specified, the generated PVCs will use the default
	// storage class.
	// +optional
	StorageClass *string `json:"storageClass,omitempty"`

	// Size of the storage. Required if not already specified in the PVC template.
	// Changes to this field are automatically reapplied to the created PVCs.
	// Size cannot be decreased.
	// +optional
	Size string `json:"size,omitempty"`

	// ResizeInUseVolumes, when true (the default), grows mounted PVCs in place and
	// relies on the storage backend to expand the filesystem online. Set it to
	// false when the backend cannot expand a volume in use: the operator then
	// completes a resize by recycling the instance Pod (serialised
	// replica-by-replica, primary last) so the volume is detached and remounted.
	// +optional
	// +kubebuilder:default:=true
	ResizeInUseVolumes *bool `json:"resizeInUseVolumes,omitempty"`

	// Template to be used to generate the Persistent Volume Claim
	// +optional
	PersistentVolumeClaimTemplate *corev1.PersistentVolumeClaimSpec `json:"pvcTemplate,omitempty"`
}

// AffinityConfiguration contains the info we need to create the affinity rules
// for Pods.
type AffinityConfiguration struct {
	// Activates anti-affinity for the pods. The operator will define pods
	// anti-affinity unless this field is explicitly set to false
	// +optional
	EnablePodAntiAffinity *bool `json:"enablePodAntiAffinity,omitempty"`

	// TopologyKey to use for anti-affinity configuration. See Kubernetes
	// documentation for more information on how this works
	// +optional
	TopologyKey string `json:"topologyKey,omitempty"`

	// NodeSelector is map of key-value pairs used to define the nodes on which
	// the pods can run.
	// More info: https://kubernetes.io/docs/concepts/configuration/assign-pod-node/
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// NodeAffinity describes node affinity scheduling rules for the pod.
	// More info: https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#node-affinity
	// +optional
	NodeAffinity *corev1.NodeAffinity `json:"nodeAffinity,omitempty"`

	// Tolerations is a list of Tolerations that should be set for all the pods,
	// in order to allow them to run on tainted nodes.
	// More info: https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// PodAntiAffinityType allows the user to decide whether pod anti-affinity
	// between cluster instances should be considered a strong requirement
	// ("required") during scheduling or not ("preferred", default).
	// +kubebuilder:validation:Enum=preferred;required
	// +kubebuilder:default:=preferred
	// +optional
	PodAntiAffinityType string `json:"podAntiAffinityType,omitempty"`

	// AdditionalPodAntiAffinity allows to specify pod anti-affinity terms to be
	// added to the ones generated by the operator if EnablePodAntiAffinity is
	// set to true (default) or to be used exclusively if set to false.
	// +optional
	AdditionalPodAntiAffinity *corev1.PodAntiAffinity `json:"additionalPodAntiAffinity,omitempty"`

	// AdditionalPodAffinity allows to specify pod affinity terms to be passed to
	// all the cluster's pods.
	// +optional
	AdditionalPodAffinity *corev1.PodAffinity `json:"additionalPodAffinity,omitempty"`
}

// ServiceAccountTemplate contains the template needed to generate the service
// accounts.
type ServiceAccountTemplate struct {
	// Metadata are the metadata to be used for the generated service account
	// +optional
	Metadata Metadata `json:"metadata,omitempty"`
}

// CertificatesConfiguration contains the needed configurations to handle server
// and client certificates for TLS and mTLS communication.
type CertificatesConfiguration struct {
	// The secret containing the Server CA certificate. If not defined, a new
	// secret will be created with a self-signed CA and will be used to generate
	// the TLS certificate ServerTLSSecret.
	// +optional
	ServerCASecret string `json:"serverCASecret,omitempty"`

	// The secret of type kubernetes.io/tls containing the server TLS certificate
	// and key that will be set as ssl-cert and ssl-key. Should be signed by the
	// CA in ServerCASecret.
	// +optional
	ServerTLSSecret string `json:"serverTLSSecret,omitempty"`

	// The secret of type kubernetes.io/tls containing the client certificate to
	// authenticate as the replication user. Should be signed by the CA in
	// ClientCASecret.
	// +optional
	ReplicationTLSSecret string `json:"replicationTLSSecret,omitempty"`

	// The secret containing the Client CA certificate. If not defined, a new
	// secret will be created with a self-signed CA and will be used to generate
	// all the client certificates.
	// +optional
	ClientCASecret string `json:"clientCASecret,omitempty"`

	// The list of additional Subject Alternative Names (SANs) to be added to the
	// server certificate generated by the operator.
	// +optional
	ServerAltDNSNames []string `json:"serverAltDNSNames,omitempty"`
}

// CertificatesStatus contains configuration certificates and related expiration
// dates.
type CertificatesStatus struct {
	// CertificatesConfiguration mirrors the resolved certificates configuration.
	CertificatesConfiguration `json:",inline"`

	// Expiration dates for all certificates.
	// +optional
	Expirations map[string]string `json:"expirations,omitempty"`
}

// S3SignatureVersion is the AWS Signature version used to sign object-store
// requests.
// +kubebuilder:validation:Enum=s3v4;s3v2
type S3SignatureVersion string

const (
	// SignatureVersionV4 is the default AWS Signature V4.
	SignatureVersionV4 S3SignatureVersion = "s3v4"

	// SignatureVersionV2 is the legacy AWS Signature V2, kept for older
	// S3-compatible providers.
	SignatureVersionV2 S3SignatureVersion = "s3v2"
)

// S3Credentials holds the references to the secrets containing the credentials
// used to access an S3-compatible object store.
type S3Credentials struct {
	// The reference to the secret key containing the access key id.
	// +optional
	AccessKeyID *SecretKeySelector `json:"accessKeyId,omitempty"`

	// The reference to the secret key containing the secret access key.
	// +optional
	SecretAccessKey *SecretKeySelector `json:"secretAccessKey,omitempty"`

	// The reference to the secret key containing the session token, used for
	// temporary credentials.
	// +optional
	SessionToken *SecretKeySelector `json:"sessionToken,omitempty"`

	// InheritFromIAMRole, when true, makes the credentials be retrieved from the
	// pod's environment (IRSA / instance profile) instead of from a secret.
	// +optional
	// +kubebuilder:default:=false
	InheritFromIAMRole bool `json:"inheritFromIAMRole,omitempty"`
}

// S3TLSConfig configures TLS verification against the object-store endpoint.
type S3TLSConfig struct {
	// InsecureSkipVerify disables TLS certificate verification against the
	// endpoint. Use only for testing.
	// +optional
	// +kubebuilder:default:=false
	InsecureSkipVerify bool `json:"insecureSkipVerify,omitempty"`

	// CABundleSecret references a secret key holding a PEM CA bundle used to
	// verify the endpoint certificate (for private CAs / self-signed endpoints).
	// +optional
	CABundleSecret *SecretKeySelector `json:"caBundleSecret,omitempty"`
}

// S3ObjectStore describes an S3-compatible object store, designed to be
// compatible with as many providers as possible (AWS S3, MinIO, Ceph RGW,
// Wasabi, Backblaze B2, etc.).
type S3ObjectStore struct {
	// Endpoint is the URL of the S3-compatible service. Leave empty to target
	// AWS S3 with the region's default endpoint.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// Region is the bucket region. Required by most providers; for AWS it
	// selects the regional endpoint.
	// +optional
	Region string `json:"region,omitempty"`

	// Bucket is the destination bucket name.
	// +kubebuilder:validation:Required
	Bucket string `json:"bucket"`

	// Path is the key prefix (folder) inside the bucket under which backups are
	// stored.
	// +optional
	Path string `json:"path,omitempty"`

	// ForcePathStyle uses path-style addressing (endpoint/bucket/key) instead of
	// virtual-hosted style (bucket.endpoint/key). Required by MinIO, Ceph and
	// most non-AWS providers; defaults to true for maximum compatibility.
	// +optional
	// +kubebuilder:default:=true
	ForcePathStyle *bool `json:"forcePathStyle,omitempty"`

	// SignatureVersion selects the request signing scheme. Defaults to s3v4;
	// set to s3v2 for legacy providers.
	// +optional
	// +kubebuilder:default:=s3v4
	SignatureVersion S3SignatureVersion `json:"signatureVersion,omitempty"`

	// ServerSideEncryption sets the SSE algorithm (e.g. "AES256" or "aws:kms").
	// +optional
	ServerSideEncryption *string `json:"serverSideEncryption,omitempty"`

	// StorageClass sets the object storage class (e.g. "STANDARD_IA").
	// +optional
	StorageClass *string `json:"storageClass,omitempty"`

	// Credentials to access the object store.
	// +kubebuilder:validation:Required
	Credentials S3Credentials `json:"credentials"`

	// TLS configuration for the endpoint connection.
	// +optional
	TLS *S3TLSConfig `json:"tls,omitempty"`
}

// Condition types and reasons shared across the API.
const (
	// ConditionReady indicates that the resource is fully functional.
	ConditionReady = "Ready"

	// ConditionProgressing indicates that the resource is being created or
	// updated.
	ConditionProgressing = "Progressing"

	// ConditionDegraded indicates that the resource failed to reach or maintain
	// its desired state.
	ConditionDegraded = "Degraded"
)
