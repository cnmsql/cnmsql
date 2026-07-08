package v1alpha1

import (
	"encoding/json"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

func TestDatabaseUserValidatorRevokesOnMariaDB(t *testing.T) {
	scheme := schemeForTests()

	mkCluster := func(flavor mysqlv1alpha1.Flavor) *mysqlv1alpha1.Cluster {
		return &mysqlv1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "default"},
			Spec:       mysqlv1alpha1.ClusterSpec{Flavor: flavor},
		}
	}

	mkUser := func(revokes bool) *mysqlv1alpha1.DatabaseUser {
		du := &mysqlv1alpha1.DatabaseUser{
			ObjectMeta: metav1.ObjectMeta{Name: "tenant-admin", Namespace: "default"},
			Spec: mysqlv1alpha1.DatabaseUserSpec{
				Cluster: mysqlv1alpha1.LocalObjectReference{Name: "shared"},
			},
		}
		if revokes {
			du.Spec.Revokes = []mysqlv1alpha1.DatabaseUserRevoke{
				{Privileges: []string{"INSERT", "UPDATE"}, On: "mysql.*"},
			}
		}
		return du
	}

	mustRaw := func(du *mysqlv1alpha1.DatabaseUser) []byte {
		b, err := json.Marshal(du)
		if err != nil {
			t.Fatalf("marshal DatabaseUser: %v", err)
		}
		return b
	}

	cases := []struct {
		name         string
		revokes      bool
		clusterInAPI client.Object // nil => cluster absent
		allowed      bool
	}{
		{"no revokes: always allowed", false, mkCluster(mysqlv1alpha1.FlavorMariaDB), true},
		{"revokes on mysql: allowed", true, mkCluster(mysqlv1alpha1.FlavorMySQL), true},
		{"revokes on default flavor (mysql): allowed", true, mkCluster(""), true},
		{"revokes on mariadb: denied", true, mkCluster(mysqlv1alpha1.FlavorMariaDB), false},
		{"revokes but cluster absent: allowed (controller guards)", true, nil, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tc.clusterInAPI != nil {
				builder = builder.WithObjects(tc.clusterInAPI)
			}
			validator := &DatabaseUserValidator{
				Decoder: admission.NewDecoder(scheme),
				Client:  builder.Build(),
			}

			req := admission.Request{}
			req.Name = "tenant-admin"
			req.Namespace = "default"
			req.Object = runtime.RawExtension{Raw: mustRaw(mkUser(tc.revokes))}

			resp := validator.Handle(t.Context(), req)
			if resp.Allowed != tc.allowed {
				t.Fatalf("expected allowed=%v, got allowed=%v (%s)",
					tc.allowed, resp.Allowed, resp.Result.Message)
			}
		})
	}
}
