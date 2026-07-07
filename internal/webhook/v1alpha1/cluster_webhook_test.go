package v1alpha1

import (
	"encoding/json"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

func TestClusterStatusValidator(t *testing.T) {
	d := admission.NewDecoder(schemeForTests())
	validator := &ClusterStatusValidator{Decoder: d}

	mkCluster := func(current, timestamp, target string) *mysqlv1alpha1.Cluster {
		return &mysqlv1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
			Spec: mysqlv1alpha1.ClusterSpec{
				Storage: mysqlv1alpha1.StorageConfiguration{},
			},
			Status: mysqlv1alpha1.ClusterStatus{
				CurrentPrimary:          current,
				CurrentPrimaryTimestamp: timestamp,
				TargetPrimary:           target,
			},
		}
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
		user    string
		subRes  string
		old     *mysqlv1alpha1.Cluster
		new     *mysqlv1alpha1.Cluster
		allowed bool
	}{
		{
			name:    "operator may change status fields",
			user:    "system:serviceaccount:cnm-system:controller-manager",
			subRes:  "status",
			old:     mkCluster("", "", ""),
			new:     func() *mysqlv1alpha1.Cluster { c := mkCluster("demo-1", "", ""); c.Status.Phase = "Ready"; return c }(),
			allowed: true,
		},
		{
			name:    "instance may promote itself when it is the target",
			user:    "system:serviceaccount:default:demo-1-instance",
			subRes:  "status",
			old:     mkCluster("", "", "demo-1"),
			new:     func() *mysqlv1alpha1.Cluster { c := mkCluster("demo-1", "now", "demo-1"); return c }(),
			allowed: true,
		},
		{
			name:    "instance may not promote itself when not the target",
			user:    "system:serviceaccount:default:demo-1-instance",
			subRes:  "status",
			old:     mkCluster("demo-2", "then", "demo-2"),
			new:     func() *mysqlv1alpha1.Cluster { c := mkCluster("demo-1", "now", "demo-2"); return c }(),
			allowed: false,
		},
		{
			name:    "instance may not promote another instance",
			user:    "system:serviceaccount:default:demo-1-instance",
			subRes:  "status",
			old:     mkCluster("demo-1", "then", "demo-1"),
			new:     func() *mysqlv1alpha1.Cluster { c := mkCluster("demo-2", "now", "demo-1"); return c }(),
			allowed: false,
		},
		{
			name:   "instance may not change phase",
			user:   "system:serviceaccount:default:demo-1-instance",
			subRes: "status",
			old:    mkCluster("demo-1", "then", "demo-1"),
			new: func() *mysqlv1alpha1.Cluster {
				c := mkCluster("demo-1", "then", "demo-1")
				c.Status.Phase = "Broken"
				return c
			}(),
			allowed: false,
		},
		{
			name:    "instance may only access status subresource",
			user:    "system:serviceaccount:default:demo-1-instance",
			subRes:  "",
			old:     mkCluster("demo-1", "then", "demo-1"),
			new:     mkCluster("demo-1", "then", "demo-1"),
			allowed: false,
		},
		{
			name:    "instance must set timestamp when promoting itself",
			user:    "system:serviceaccount:default:demo-1-instance",
			subRes:  "status",
			old:     mkCluster("", "", "demo-1"),
			new:     mkCluster("demo-1", "", "demo-1"),
			allowed: false,
		},
		{
			name:    "instance may not clear currentPrimary",
			user:    "system:serviceaccount:default:demo-1-instance",
			subRes:  "status",
			old:     mkCluster("demo-1", "then", "demo-1"),
			new:     mkCluster("", "", "demo-1"),
			allowed: false,
		},
		{
			name:   "instance may not change other status fields while currentPrimary unchanged",
			user:   "system:serviceaccount:default:demo-1-instance",
			subRes: "status",
			old:    mkCluster("demo-1", "then", "demo-1"),
			new: func() *mysqlv1alpha1.Cluster {
				c := mkCluster("demo-1", "then", "demo-1")
				c.Status.ReadyInstances = 3
				return c
			}(),
			allowed: false,
		},
		{
			name:   "non-instance service account in cluster namespace is allowed",
			user:   "system:serviceaccount:default:some-random-sa",
			subRes: "status",
			old:    mkCluster("demo-1", "then", "demo-1"),
			new: func() *mysqlv1alpha1.Cluster {
				c := mkCluster("demo-1", "then", "demo-1")
				c.Status.Phase = "Ready"
				return c
			}(),
			allowed: true,
		},
		{
			name:    "service account named like instance but with non-numeric ordinal is not an instance",
			user:    "system:serviceaccount:default:demo-evil-instance",
			subRes:  "status",
			old:     mkCluster("demo-1", "then", "demo-1"),
			new:     func() *mysqlv1alpha1.Cluster { c := mkCluster("demo-evil", "now", "demo-1"); return c }(),
			allowed: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					Name:        "demo",
					Namespace:   "default",
					SubResource: tc.subRes,
					UserInfo:    authenticationv1.UserInfo{Username: tc.user},
					Object:      runtime.RawExtension{Raw: mustRaw(tc.new)},
					OldObject:   runtime.RawExtension{Raw: mustRaw(tc.old)},
				},
			}
			resp := validator.Handle(t.Context(), req)
			if resp.Allowed != tc.allowed {
				t.Fatalf("expected allowed=%v, got allowed=%v (%s)", tc.allowed, resp.Allowed, resp.Result.Message)
			}
		})
	}
}

func TestClusterStatusValidatorGroupReplication(t *testing.T) {
	d := admission.NewDecoder(schemeForTests())
	validator := &ClusterStatusValidator{Decoder: d}

	mkGR := func(mutate func(*mysqlv1alpha1.Cluster)) *mysqlv1alpha1.Cluster {
		c := &mysqlv1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
			Spec: mysqlv1alpha1.ClusterSpec{
				Replication: &mysqlv1alpha1.ReplicationConfiguration{
					Mode: mysqlv1alpha1.ReplicationModeGroupReplication,
				},
			},
			Status: mysqlv1alpha1.ClusterStatus{
				GroupReplication: &mysqlv1alpha1.GroupReplicationStatus{
					GroupName:     "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
					Bootstrapped:  true,
					PrimaryMember: "demo-1",
				},
			},
		}
		if mutate != nil {
			mutate(c)
		}
		return c
	}

	mustRaw := func(c *mysqlv1alpha1.Cluster) []byte {
		b, err := json.Marshal(c)
		if err != nil {
			t.Fatalf("marshal Cluster: %v", err)
		}
		return b
	}

	const operator = "system:serviceaccount:cnm-system:controller-manager"
	const instance = "system:serviceaccount:default:demo-1-instance"

	cases := []struct {
		name    string
		user    string
		subRes  string
		old     *mysqlv1alpha1.Cluster
		new     *mysqlv1alpha1.Cluster
		allowed bool
	}{
		{
			name:   "operator writes the group view",
			user:   operator,
			subRes: "status",
			old:    mkGR(nil),
			new: mkGR(func(c *mysqlv1alpha1.Cluster) {
				c.Status.GroupReplication.PrimaryMember = "demo-2"
				c.Status.CurrentPrimary = "demo-2"
			}),
			allowed: true,
		},
		{
			name:    "instance may not write currentPrimary under GR",
			user:    instance,
			subRes:  "status",
			old:     mkGR(nil),
			new:     mkGR(func(c *mysqlv1alpha1.Cluster) { c.Status.CurrentPrimary = "demo-1" }),
			allowed: false,
		},
		{
			name:   "instance may not forge the group view",
			user:   instance,
			subRes: "status",
			old:    mkGR(nil),
			new: mkGR(func(c *mysqlv1alpha1.Cluster) {
				c.Status.GroupReplication.HasQuorum = true
				c.Status.GroupReplication.PrimaryMember = "demo-1"
			}),
			allowed: false,
		},
		{
			name:    "instance making no status change is allowed under GR",
			user:    instance,
			subRes:  "status",
			old:     mkGR(nil),
			new:     mkGR(nil),
			allowed: true,
		},
		{
			name:    "operator may not clear bootstrapped",
			user:    operator,
			subRes:  "status",
			old:     mkGR(nil),
			new:     mkGR(func(c *mysqlv1alpha1.Cluster) { c.Status.GroupReplication.Bootstrapped = false }),
			allowed: false,
		},
		{
			name:   "operator may not change the group name",
			user:   operator,
			subRes: "status",
			old:    mkGR(nil),
			new: mkGR(func(c *mysqlv1alpha1.Cluster) {
				c.Status.GroupReplication.GroupName = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
			}),
			allowed: false,
		},
		{
			name:   "first bootstrap sets bootstrapped true",
			user:   operator,
			subRes: "status",
			old: mkGR(func(c *mysqlv1alpha1.Cluster) {
				c.Status.GroupReplication = &mysqlv1alpha1.GroupReplicationStatus{}
			}),
			new:     mkGR(nil),
			allowed: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					Name:        "demo",
					Namespace:   "default",
					SubResource: tc.subRes,
					UserInfo:    authenticationv1.UserInfo{Username: tc.user},
					Object:      runtime.RawExtension{Raw: mustRaw(tc.new)},
					OldObject:   runtime.RawExtension{Raw: mustRaw(tc.old)},
				},
			}
			resp := validator.Handle(t.Context(), req)
			if resp.Allowed != tc.allowed {
				t.Fatalf("expected allowed=%v, got allowed=%v (%s)", tc.allowed, resp.Allowed, resp.Result.Message)
			}
		})
	}
}

func schemeForTests() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = mysqlv1alpha1.AddToScheme(s)
	_ = corev1.AddToScheme(s)
	return s
}
