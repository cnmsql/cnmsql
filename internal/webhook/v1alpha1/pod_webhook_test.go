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

func TestInstancePodValidator(t *testing.T) {
	d := admission.NewDecoder(schemeForTests())
	validator := &InstancePodValidator{Decoder: d}

	const operator = "system:serviceaccount:default:controller-manager"
	const instance = "system:serviceaccount:default:demo-1-instance"
	const otherInstance = "demo-2"

	// basePod is a well-formed instance Pod as the operator would create it.
	basePod := func(mutate func(*corev1.Pod)) *corev1.Pod {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "demo-1",
				Namespace: "default",
				Labels: map[string]string{
					mysqlv1alpha1.ClusterLabelName: "demo",
					"mysql.cnmsql.co/instance":     "demo-1",
				},
				Annotations: map[string]string{
					"cnmsql.cnmsql.co/config-hash": "abc",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "mysql", Image: "mysql:8.4.0"}},
			},
		}
		if mutate != nil {
			mutate(p)
		}
		return p
	}

	mustRaw := func(p *corev1.Pod) []byte {
		b, err := json.Marshal(p)
		if err != nil {
			t.Fatalf("marshal Pod: %v", err)
		}
		return b
	}

	cases := []struct {
		name    string
		user    string
		podName string
		old     *corev1.Pod
		new     *corev1.Pod
		allowed bool
	}{
		{
			name:    "operator may change any field",
			user:    operator,
			podName: "demo-1",
			old:     basePod(nil),
			new:     basePod(func(p *corev1.Pod) { p.Spec.Containers[0].Image = "mysql:8.4.1" }),
			allowed: true,
		},
		{
			name:    "instance may ring the observation doorbell",
			user:    instance,
			podName: "demo-1",
			old:     basePod(nil),
			new: basePod(func(p *corev1.Pod) {
				p.Annotations[groupObservationAnnotation] = "fingerprint-2"
			}),
			allowed: true,
		},
		{
			name:    "instance may clear an operator force command",
			user:    instance,
			podName: "demo-1",
			old: basePod(func(p *corev1.Pod) {
				p.Annotations[forceGroupRebootstrapAnnotation] = "yes"
			}),
			new:     basePod(nil),
			allowed: true,
		},
		{
			name:    "instance may not set an operator force command",
			user:    instance,
			podName: "demo-1",
			old:     basePod(nil),
			new: basePod(func(p *corev1.Pod) {
				p.Annotations[forceGroupRebootstrapAnnotation] = "yes"
			}),
			allowed: false,
		},
		{
			name:    "instance may not swap its container image",
			user:    instance,
			podName: "demo-1",
			old:     basePod(nil),
			new:     basePod(func(p *corev1.Pod) { p.Spec.Containers[0].Image = "evil:latest" }),
			allowed: false,
		},
		{
			name:    "instance may not add an ephemeral container",
			user:    instance,
			podName: "demo-1",
			old:     basePod(nil),
			new: basePod(func(p *corev1.Pod) {
				p.Spec.EphemeralContainers = []corev1.EphemeralContainer{{
					EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug", Image: "evil:latest"},
				}}
			}),
			allowed: false,
		},
		{
			name:    "instance may not hijack labels",
			user:    instance,
			podName: "demo-1",
			old:     basePod(nil),
			new: basePod(func(p *corev1.Pod) {
				p.Labels["mysql.cnmsql.co/instance"] = otherInstance
			}),
			allowed: false,
		},
		{
			name:    "instance may not inject a finalizer",
			user:    instance,
			podName: "demo-1",
			old:     basePod(nil),
			new:     basePod(func(p *corev1.Pod) { p.Finalizers = []string{"evil.io/block"} }),
			allowed: false,
		},
		{
			name:    "instance may not spoof an operator-trusted annotation",
			user:    instance,
			podName: "demo-1",
			old:     basePod(nil),
			new: basePod(func(p *corev1.Pod) {
				p.Annotations["mysql.cnmsql.co/fenced"] = "true"
			}),
			allowed: false,
		},
		{
			name:    "instance may not patch another instance's Pod",
			user:    instance,
			podName: otherInstance,
			old: basePod(func(p *corev1.Pod) {
				p.Name = otherInstance
				p.Labels["mysql.cnmsql.co/instance"] = otherInstance
			}),
			new: basePod(func(p *corev1.Pod) {
				p.Name = otherInstance
				p.Labels["mysql.cnmsql.co/instance"] = otherInstance
				p.Annotations[groupObservationAnnotation] = "fingerprint-2"
			}),
			allowed: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					Name:      tc.podName,
					Namespace: "default",
					UserInfo:  authenticationv1.UserInfo{Username: tc.user},
					Object:    runtime.RawExtension{Raw: mustRaw(tc.new)},
					OldObject: runtime.RawExtension{Raw: mustRaw(tc.old)},
				},
			}
			resp := validator.Handle(t.Context(), req)
			if resp.Allowed != tc.allowed {
				t.Fatalf("expected allowed=%v, got allowed=%v (%s)", tc.allowed, resp.Allowed, resp.Result.Message)
			}
		})
	}
}
