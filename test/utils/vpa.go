package utils

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// This file installs the Vertical Pod Autoscaler and metrics-server into an
// ephemeral e2e cluster via pinned upstream manifests, mirroring the
// kubectl-apply install pattern used for cert-manager (see InstallCertManager).
// It is only ever called from a Label("disruptive") spec that owns a throwaway
// Kind cluster, because the VPA admission-controller registers a *cluster-wide*
// mutating webhook that would otherwise intercept every other spec's Pods.
//
// The whole Kind cluster is deleted on spec teardown, so there is deliberately
// no uninstall counterpart: tearing the cluster down reclaims everything.

const (
	// vpaVersion pins the kubernetes/autoscaler vertical-pod-autoscaler release.
	// Bump in lockstep with the e2e Kubernetes version (.github/kind_versions.json).
	vpaVersion = "1.4.1"
	vpaTagTmpl = "https://raw.githubusercontent.com/kubernetes/autoscaler/" +
		"vertical-pod-autoscaler-%s/vertical-pod-autoscaler/deploy/%s"

	// metricsServerVersion pins the metrics-server release. The VPA recommender
	// reads usage from the metrics.k8s.io API metrics-server serves, so it must be
	// installed and Available before recommendations appear.
	metricsServerVersion = "v0.7.2"
	metricsServerURLTmpl = "https://github.com/kubernetes-sigs/metrics-server/" +
		"releases/download/%s/components.yaml"

	// vpaWebhookDNSName is the SAN the admission-controller serves under and the
	// name its self-registered webhook points at (Service vpa-webhook in
	// kube-system). The generated server certificate must carry it or the API
	// server rejects the webhook's TLS handshake.
	vpaWebhookDNSName = "vpa-webhook.kube-system.svc"
)

// InstallMetricsServer installs metrics-server and patches it for Kind, where the
// kubelet serves a self-signed certificate that metrics-server would otherwise
// refuse (--kubelet-insecure-tls). The VPA recommender needs it for usage data.
func InstallMetricsServer() error {
	url := fmt.Sprintf(metricsServerURLTmpl, metricsServerVersion)
	if _, err := Run(exec.Command("kubectl", "apply", "-f", url)); err != nil {
		return fmt.Errorf("applying metrics-server: %w", err)
	}

	// Kind nodes present a self-signed kubelet serving cert; without this flag
	// metrics-server never reaches Ready and the recommender gets no metrics.
	patch := `[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]`
	if _, err := Run(exec.Command("kubectl", "patch", "deployment", "metrics-server",
		"-n", "kube-system", "--type=json", "-p", patch)); err != nil {
		return fmt.Errorf("patching metrics-server for Kind: %w", err)
	}

	if _, err := Run(exec.Command("kubectl", "wait", "deployment/metrics-server",
		"--for=condition=Available", "--namespace", "kube-system", "--timeout", "5m")); err != nil {
		return fmt.Errorf("waiting for metrics-server: %w", err)
	}
	return nil
}

// InstallVPA installs the full Vertical Pod Autoscaler stack (CRDs, RBAC,
// recommender, updater, admission-controller) from pinned upstream manifests.
//
// The admission-controller serves a mutating webhook over TLS and self-registers
// it using the CA from the vpa-tls-certs secret. Upstream generates that secret
// with an openssl script (gencerts.sh); to avoid a new toolchain dependency in
// CI we mint the same CA/server certificate in-process and create the secret
// before applying the manifests, so the controller finds it on first start.
func InstallVPA() error {
	if err := createVPACertSecret(); err != nil {
		return err
	}
	for _, manifest := range []string{
		"vpa-v1-crd-gen.yaml",
		"vpa-rbac.yaml",
		"recommender-deployment.yaml",
		"updater-deployment.yaml",
		"admission-controller-deployment.yaml",
	} {
		url := fmt.Sprintf(vpaTagTmpl, vpaVersion, manifest)
		if _, err := Run(exec.Command("kubectl", "apply", "-f", url)); err != nil {
			return fmt.Errorf("applying %s: %w", manifest, err)
		}
	}
	for _, deploy := range []string{"vpa-recommender", "vpa-updater", "vpa-admission-controller"} {
		if _, err := Run(exec.Command("kubectl", "wait", "deployment/"+deploy,
			"--for=condition=Available", "--namespace", "kube-system", "--timeout", "5m")); err != nil {
			return fmt.Errorf("waiting for %s: %w", deploy, err)
		}
	}
	return nil
}

// createVPACertSecret mints a self-signed CA and a server certificate for the
// vpa-webhook Service and stores them in the kube-system/vpa-tls-certs secret
// under the file keys the admission-controller expects (caCert.pem,
// serverCert.pem, serverKey.pem, plus caKey.pem for fidelity with gencerts.sh).
func createVPACertSecret() error {
	caCert, caKey, err := selfSignedCA()
	if err != nil {
		return fmt.Errorf("generating VPA CA: %w", err)
	}
	serverCert, serverKey, err := serverCertFor(vpaWebhookDNSName, caCert, caKey)
	if err != nil {
		return fmt.Errorf("generating VPA server cert: %w", err)
	}

	dir, err := os.MkdirTemp("", "vpa-certs")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()

	files := map[string][]byte{
		"caCert.pem":     certPEM(caCert),
		"caKey.pem":      keyPEM(caKey),
		"serverCert.pem": certPEM(serverCert),
		"serverKey.pem":  keyPEM(serverKey),
	}
	args := []string{"create", "secret", "generic", "vpa-tls-certs", "--namespace=kube-system"}
	for name, data := range files {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			return err
		}
		args = append(args, fmt.Sprintf("--from-file=%s=%s", name, path))
	}

	// Make the create idempotent: re-render to YAML and apply, so a re-run of the
	// spec on a reused cluster does not fail on an existing secret.
	args = append(args, "--dry-run=client", "-o", "yaml")
	out, err := Run(exec.Command("kubectl", args...))
	if err != nil {
		return fmt.Errorf("rendering vpa-tls-certs secret: %w", err)
	}
	apply := exec.Command("kubectl", "apply", "-f", "-")
	apply.Stdin = strings.NewReader(out)
	if _, err := Run(apply); err != nil {
		return fmt.Errorf("applying vpa-tls-certs secret: %w", err)
	}
	return nil
}

func selfSignedCA() (*x509.Certificate, *rsa.PrivateKey, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "vpa-webhook-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, err
	}
	return cert, key, nil
}

func serverCertFor(
	dnsName string, ca *x509.Certificate, caKey *rsa.PrivateKey,
) (*x509.Certificate, *rsa.PrivateKey, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: dnsName},
		DNSNames:     []string{dnsName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, err
	}
	return cert, key, nil
}

func certPEM(cert *x509.Certificate) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
}

func keyPEM(key *rsa.PrivateKey) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}
