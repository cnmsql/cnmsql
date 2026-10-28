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

func TestDatabaseValidatorHandle(t *testing.T) {
	scheme := schemeForTests()

	mkDatabase := func(grant mysqlv1alpha1.DatabaseGrant) *mysqlv1alpha1.Database {
		return &mysqlv1alpha1.Database{
			ObjectMeta: metav1.ObjectMeta{Name: "tenant-a", Namespace: "shared"},
			Spec: mysqlv1alpha1.DatabaseSpec{
				Cluster: mysqlv1alpha1.LocalObjectReference{Name: "shared"},
				Name:    "tenant_a",
				Users:   []mysqlv1alpha1.InlineUser{{Name: "tenant_a_app", Grants: []mysqlv1alpha1.DatabaseGrant{grant}}},
			},
		}
	}
	mustRaw := func(db *mysqlv1alpha1.Database) []byte {
		b, err := json.Marshal(db)
		if err != nil {
			t.Fatalf("marshal Database: %v", err)
		}
		return b
	}
	scoped := mysqlv1alpha1.DatabaseGrant{Privileges: []string{"SELECT", "INSERT"}}
	global := mysqlv1alpha1.DatabaseGrant{Privileges: []string{"ALL"}, On: "*.*"}
	withFinalizer := func(db *mysqlv1alpha1.Database) *mysqlv1alpha1.Database {
		db.Finalizers = []string{"mysql.cnmsql.co/database"}
		return db
	}

	cases := []struct {
		name    string
		op      admissionv1.Operation
		obj     *mysqlv1alpha1.Database
		old     *mysqlv1alpha1.Database
		allowed bool
	}{
		{"create scoped grant", admissionv1.Create, mkDatabase(scoped), nil, true},
		{"create ALL on *.*", admissionv1.Create, mkDatabase(global), nil, false},
		{"create injected target", admissionv1.Create, mkDatabase(mysqlv1alpha1.DatabaseGrant{
			Privileges: []string{"SELECT"}, On: "tenant_a.* TO 'other'@'%' WITH GRANT OPTION #",
		}), nil, false},
		{"update widening to *.*", admissionv1.Update, mkDatabase(global), mkDatabase(scoped), false},
		{"update narrowing a stored invalid grant", admissionv1.Update, mkDatabase(scoped), mkDatabase(global), true},
		{"metadata-only update of a stored invalid grant", admissionv1.Update,
			withFinalizer(mkDatabase(global)), mkDatabase(global), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			validator := &DatabaseValidator{Decoder: admission.NewDecoder(scheme)}
			req := admission.Request{}
			req.Operation = tc.op
			req.Name = tc.obj.Name
			req.Namespace = tc.obj.Namespace
			req.Object = runtime.RawExtension{Raw: mustRaw(tc.obj)}
			if tc.old != nil {
				req.OldObject = runtime.RawExtension{Raw: mustRaw(tc.old)}
			}

			resp := validator.Handle(t.Context(), req)
			if resp.Allowed != tc.allowed {
				t.Fatalf("expected allowed=%v, got allowed=%v (%s)", tc.allowed, resp.Allowed, resp.Result.Message)
			}
		})
	}
}
