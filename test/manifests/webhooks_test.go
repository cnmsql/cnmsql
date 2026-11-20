// Package manifests checks the rendered kustomize overlays that ship the
// operator, where a mistake reaches every install without failing a build.
package manifests

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	"sigs.k8s.io/kustomize/api/krusty"
	"sigs.k8s.io/kustomize/kyaml/filesys"
	"sigs.k8s.io/yaml"
)

const (
	podWebhook           = "vinstancepod-v1alpha1.cnmsql.co"
	clusterStatusWebhook = "vclusterstatus-v1alpha1.cnmsql.co"
	instanceLabel        = "mysql.cnmsql.co/cluster"
	namespaceNameLabel   = "kubernetes.io/metadata.name"
)

// renderWebhooks builds a kustomize overlay under config/ and returns its
// validating webhooks by name, plus the webhook Service's namespace.
func renderWebhooks(t *testing.T, overlay string) (map[string]admissionregistrationv1.ValidatingWebhook, string) {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	dir := filepath.Join(filepath.Dir(file), "..", "..", "config", overlay)

	resMap, err := krusty.MakeKustomizer(krusty.MakeDefaultOptions()).Run(filesys.MakeFsOnDisk(), dir)
	if err != nil {
		t.Fatalf("kustomize build %s: %v", overlay, err)
	}

	webhooks := map[string]admissionregistrationv1.ValidatingWebhook{}
	serviceNamespace := ""
	configs := 0
	for _, res := range resMap.Resources() {
		switch res.GetKind() {
		case "Service":
			if strings.HasSuffix(res.GetName(), "webhook-service") {
				serviceNamespace = res.GetNamespace()
			}
		case "ValidatingWebhookConfiguration":
			configs++
			raw, err := res.AsYAML()
			if err != nil {
				t.Fatalf("serialize %s: %v", res.GetName(), err)
			}
			var cfg admissionregistrationv1.ValidatingWebhookConfiguration
			if err := yaml.Unmarshal(raw, &cfg); err != nil {
				t.Fatalf("decode %s: %v", res.GetName(), err)
			}
			for _, wh := range cfg.Webhooks {
				webhooks[wh.Name] = wh
			}
		}
	}
	if configs != 1 {
		t.Fatalf("%s renders %d ValidatingWebhookConfigurations, want 1", overlay, configs)
	}
	if len(webhooks) == 0 {
		t.Fatalf("%s renders no validating webhooks", overlay)
	}
	if serviceNamespace == "" {
		t.Fatalf("%s renders no webhook Service", overlay)
	}
	return webhooks, serviceNamespace
}

// checkCommonScoping asserts what every overlay must do: the Pod webhook, which
// fails closed, only sees instance Pods; no other webhook is narrowed by object
// labels (users do not label Databases or DatabaseUsers, so a selector there
// switches the webhook off); and the status webhook sees only the subresource.
func checkCommonScoping(t *testing.T, webhooks map[string]admissionregistrationv1.ValidatingWebhook) {
	t.Helper()
	for name, wh := range webhooks {
		if name == podWebhook {
			sel := wh.ObjectSelector
			if sel == nil || len(sel.MatchExpressions) != 1 ||
				sel.MatchExpressions[0].Key != instanceLabel ||
				sel.MatchExpressions[0].Operator != "Exists" {
				t.Errorf("%s: objectSelector = %+v, want %s Exists", name, sel, instanceLabel)
			}
			continue
		}
		if wh.ObjectSelector != nil {
			t.Errorf("%s: unexpected objectSelector %+v; it would skip objects without that label", name, wh.ObjectSelector)
		}
	}
	if _, ok := webhooks[podWebhook]; !ok {
		t.Errorf("webhook %s not rendered", podWebhook)
	}
	status, ok := webhooks[clusterStatusWebhook]
	if !ok {
		t.Fatalf("webhook %s not rendered", clusterStatusWebhook)
	}
	if len(status.Rules) != 1 || len(status.Rules[0].Resources) != 1 || status.Rules[0].Resources[0] != "clusters/status" {
		t.Errorf("%s: rules = %+v, want only clusters/status", clusterStatusWebhook, status.Rules)
	}
}

func TestDefaultOverlayWebhookScoping(t *testing.T) {
	webhooks, _ := renderWebhooks(t, "default")
	checkCommonScoping(t, webhooks)
	for name, wh := range webhooks {
		if wh.NamespaceSelector != nil {
			t.Errorf("%s: unexpected namespaceSelector %+v in the cluster-wide overlay", name, wh.NamespaceSelector)
		}
	}
}

// TestNamespacedOverlayWebhookScoping guards cohabiting namespaced operators:
// a webhook left unscoped receives every other tenant's objects, and the Pod
// webhook fails closed, so one operator outage would block Pod updates in every
// namespace.
func TestNamespacedOverlayWebhookScoping(t *testing.T) {
	webhooks, namespace := renderWebhooks(t, "namespaced")
	checkCommonScoping(t, webhooks)
	for name, wh := range webhooks {
		sel := wh.NamespaceSelector
		if sel == nil || len(sel.MatchExpressions) != 0 || len(sel.MatchLabels) != 1 ||
			sel.MatchLabels[namespaceNameLabel] != namespace {
			t.Errorf("%s: namespaceSelector = %+v, want only %s=%s", name, sel, namespaceNameLabel, namespace)
		}
	}
}
