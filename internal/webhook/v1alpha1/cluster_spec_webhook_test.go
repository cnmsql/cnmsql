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
	"encoding/json"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

func TestClusterSpecValidator(t *testing.T) {
	d := admission.NewDecoder(schemeForTests())
	validator := &ClusterSpecValidator{Decoder: d}

	catalogCluster := func(series string) *mysqlv1alpha1.Cluster {
		c := &mysqlv1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
			Spec: mysqlv1alpha1.ClusterSpec{
				ImageCatalogRef: &mysqlv1alpha1.ImageCatalogRef{Series: series},
				Instances:       3,
				Storage:         mysqlv1alpha1.StorageConfiguration{Size: "1Gi"},
			},
		}
		c.SetDefaults()
		return c
	}
	imageCluster := func(image string) *mysqlv1alpha1.Cluster {
		c := &mysqlv1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
			Spec: mysqlv1alpha1.ClusterSpec{
				ImageName: image,
				Instances: 3,
				Storage:   mysqlv1alpha1.StorageConfiguration{Size: "1Gi"},
			},
		}
		c.SetDefaults()
		return c
	}
	mustRaw := func(c *mysqlv1alpha1.Cluster) []byte {
		b, err := json.Marshal(c)
		if err != nil {
			t.Fatalf("marshal Cluster: %v", err)
		}
		return b
	}

	cases := []struct {
		name    string
		op      admissionv1.Operation
		old     *mysqlv1alpha1.Cluster
		new     *mysqlv1alpha1.Cluster
		allowed bool
	}{
		{
			name:    "create a valid cluster",
			op:      admissionv1.Create,
			new:     catalogCluster("8.0"),
			allowed: true,
		},
		{
			name: "create an invalid cluster (both image sources)",
			op:   admissionv1.Create,
			new: func() *mysqlv1alpha1.Cluster {
				c := catalogCluster("8.0")
				c.Spec.ImageName = "percona/percona-server:8.0"
				return c
			}(),
			allowed: false,
		},
		{
			name:    "allow a single supported hop",
			op:      admissionv1.Update,
			old:     catalogCluster("8.0"),
			new:     catalogCluster("8.4"),
			allowed: true,
		},
		{
			name:    "reject a skipped series",
			op:      admissionv1.Update,
			old:     catalogCluster("8.0"),
			new:     catalogCluster("9.0"),
			allowed: false,
		},
		{
			name:    "reject a downgrade",
			op:      admissionv1.Update,
			old:     catalogCluster("8.4"),
			new:     catalogCluster("8.0"),
			allowed: false,
		},
		{
			name:    "reject a major change via imageName",
			op:      admissionv1.Update,
			old:     imageCluster("percona/percona-server:8.0"),
			new:     imageCluster("percona/percona-server:8.4"),
			allowed: false,
		},
		{
			name: "mariadb flavor with mariadb series is allowed",
			op:   admissionv1.Create,
			new: func() *mysqlv1alpha1.Cluster {
				c := catalogCluster("10.6")
				c.Spec.Flavor = mysqlv1alpha1.FlavorMariaDB
				return c
			}(),
			allowed: true,
		},
		{
			name: "flavor is immutable (mysql to mariadb rejected)",
			op:   admissionv1.Update,
			old:  catalogCluster("8.0"),
			new: func() *mysqlv1alpha1.Cluster {
				c := catalogCluster("8.0")
				c.Spec.Flavor = mysqlv1alpha1.FlavorMariaDB
				return c
			}(),
			allowed: false,
		},
		{
			name: "flavor empty to mysql allowed (same resolved value)",
			op:   admissionv1.Update,
			old:  catalogCluster("8.0"),
			new: func() *mysqlv1alpha1.Cluster {
				c := catalogCluster("8.0")
				c.Spec.Flavor = mysqlv1alpha1.FlavorMySQL
				return c
			}(),
			allowed: true,
		},
		{
			name: "mariadb does not support group replication",
			op:   admissionv1.Create,
			new: func() *mysqlv1alpha1.Cluster {
				c := catalogCluster("10.6")
				c.Spec.Flavor = mysqlv1alpha1.FlavorMariaDB
				c.Spec.Replication = &mysqlv1alpha1.ReplicationConfiguration{
					Mode: mysqlv1alpha1.ReplicationModeGroupReplication,
				}
				return c
			}(),
			allowed: false,
		},
		{
			name: "mariadb cannot use MySQL series",
			op:   admissionv1.Create,
			new: func() *mysqlv1alpha1.Cluster {
				c := catalogCluster("8.0")
				c.Spec.Flavor = mysqlv1alpha1.FlavorMariaDB
				return c
			}(),
			allowed: false,
		},
		{
			name: "mysql cannot use MariaDB series",
			op:   admissionv1.Create,
			new: func() *mysqlv1alpha1.Cluster {
				c := catalogCluster("10.11")
				c.Spec.Flavor = mysqlv1alpha1.FlavorMySQL
				return c
			}(),
			allowed: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					Name:      "demo",
					Namespace: "default",
					Operation: tc.op,
					Object:    runtime.RawExtension{Raw: mustRaw(tc.new)},
				},
			}
			if tc.old != nil {
				req.OldObject = runtime.RawExtension{Raw: mustRaw(tc.old)}
			}
			resp := validator.Handle(t.Context(), req)
			if resp.Allowed != tc.allowed {
				t.Errorf("allowed = %v, want %v (reason: %s)", resp.Allowed, tc.allowed, resp.Result.Message)
			}
		})
	}
}

func TestClusterSpecValidatorIgnoresStatusSubresource(t *testing.T) {
	d := admission.NewDecoder(schemeForTests())
	validator := &ClusterSpecValidator{Decoder: d}
	req := admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Name:        "demo",
			Namespace:   "default",
			Operation:   admissionv1.Update,
			SubResource: "status",
		},
	}
	if resp := validator.Handle(t.Context(), req); !resp.Allowed {
		t.Errorf("status subresource updates should be allowed by the spec validator")
	}
}
