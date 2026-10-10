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

package controller

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// testPKI issues the certificates of an instance's control API: a CA, the
// server certificate for <instance>.<namespace>.svc, and client certificates.
type testPKI struct {
	t      *testing.T
	caCert *x509.Certificate
	caKey  *ecdsa.PrivateKey
	caPEM  []byte
	serial int64
}

func newTestPKI(t *testing.T) *testPKI {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &testPKI{
		t: t, caCert: cert, caKey: key, serial: 1,
		caPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}
}

// issue returns a PEM certificate and key signed by the CA.
func (p *testPKI) issue(cn string, dnsNames []string, usage x509.ExtKeyUsage) (certPEM, keyPEM []byte) {
	p.t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		p.t.Fatal(err)
	}
	p.serial++
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(p.serial),
		Subject:      pkix.Name{CommonName: cn},
		DNSNames:     dnsNames,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.caCert, &key.PublicKey, p.caKey)
	if err != nil {
		p.t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		p.t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

// controlServer is an instance control API that requires a client certificate
// and records every connection it accepts and the client certificate of every
// request.
type controlServer struct {
	*httptest.Server
	mu          sync.Mutex
	newConns    int
	clientCerts []string
}

func newControlServer(t *testing.T, pki *testPKI, serverName string) *controlServer {
	t.Helper()
	certPEM, keyPEM := pki.issue(serverName, []string{serverName}, x509.ExtKeyUsageServerAuth)
	serverCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	clientCAs := x509.NewCertPool()
	clientCAs.AddCert(pki.caCert)

	s := &controlServer{}
	s.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.clientCerts = append(s.clientCerts, r.TLS.PeerCertificates[0].Subject.CommonName)
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"instanceName":"demo-1","role":"primary","isReady":true}`))
	}))
	s.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			s.mu.Lock()
			s.newConns++
			s.mu.Unlock()
		}
	}
	s.TLS = &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    clientCAs,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
	s.StartTLS()
	t.Cleanup(s.Close)
	return s
}

func (s *controlServer) connections() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.newConns
}

func (s *controlServer) lastClientCert() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clientCerts[len(s.clientCerts)-1]
}

// newTestControlClient returns a control client whose every dial reaches
// server, with the cluster's CA and client TLS Secrets in a fake API.
func newTestControlClient(t *testing.T, pki *testPKI, server *controlServer, clientCN string) (*HTTPControlClient, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	certPEM, keyPEM := pki.issue(clientCN, nil, x509.ExtKeyUsageClientAuth)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "demo-ca", Namespace: "default"},
			Data:       map[string][]byte{"ca.crt": pki.caPEM},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "demo-client-tls", Namespace: "default"},
			Data:       map[string][]byte{corev1.TLSCertKey: certPEM, corev1.TLSPrivateKeyKey: keyPEM},
		},
	).Build()
	addr := server.Listener.Addr().String()
	c := &HTTPControlClient{Client: kube}
	c.dialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, network, addr)
	}
	return c, kube
}

func testControlCluster() *mysqlv1alpha1.Cluster {
	return &mysqlv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}}
}

// TestStatusReusesOneConnectionAcrossCalls proves repeated calls to an
// instance share a connection. A client that built a new Transport per call
// left every connection open for good (the instance server has no idle
// timeout), and in run 38050924292 the operator held thousands of them to the
// primary until it was OOMKilled.
func TestStatusReusesOneConnectionAcrossCalls(t *testing.T) {
	pki := newTestPKI(t)
	server := newControlServer(t, pki, "demo-1.default.svc")
	c, _ := newTestControlClient(t, pki, server, "operator")
	cluster := testControlCluster()

	for range 10 {
		status, err := c.Status(context.Background(), cluster, "demo-1")
		if err != nil {
			t.Fatal(err)
		}
		if status.Role != "primary" {
			t.Fatalf("role = %q, want primary", status.Role)
		}
	}
	if got := server.connections(); got != 1 {
		t.Fatalf("server accepted %d connections for 10 calls, want 1 reused connection", got)
	}
}

// TestStatusPicksUpARotatedClientCertificate proves a reused connection does
// not pin the client certificate: once cert-manager rotates the client TLS
// Secret, the next call presents the new certificate.
func TestStatusPicksUpARotatedClientCertificate(t *testing.T) {
	pki := newTestPKI(t)
	server := newControlServer(t, pki, "demo-1.default.svc")
	c, kube := newTestControlClient(t, pki, server, "operator-before")
	cluster := testControlCluster()
	ctx := context.Background()

	if _, err := c.Status(ctx, cluster, "demo-1"); err != nil {
		t.Fatal(err)
	}
	if got := server.lastClientCert(); got != "operator-before" {
		t.Fatalf("client certificate = %q, want operator-before", got)
	}

	certPEM, keyPEM := pki.issue("operator-after", nil, x509.ExtKeyUsageClientAuth)
	secret := &corev1.Secret{}
	if err := kube.Get(ctx, client.ObjectKey{Namespace: "default", Name: "demo-client-tls"}, secret); err != nil {
		t.Fatal(err)
	}
	secret.Data = map[string][]byte{corev1.TLSCertKey: certPEM, corev1.TLSPrivateKeyKey: keyPEM}
	if err := kube.Update(ctx, secret); err != nil {
		t.Fatal(err)
	}

	if _, err := c.Status(ctx, cluster, "demo-1"); err != nil {
		t.Fatal(err)
	}
	if got := server.lastClientCert(); got != "operator-after" {
		t.Fatalf("client certificate = %q after rotation, want operator-after", got)
	}
}

// TestTransportClosesIdleConnections proves the transport does not keep idle
// connections forever. The instance server sets no idle timeout, so the
// client side has to.
func TestTransportClosesIdleConnections(t *testing.T) {
	pki := newTestPKI(t)
	server := newControlServer(t, pki, "demo-1.default.svc")
	c, _ := newTestControlClient(t, pki, server, "operator")

	transport, err := c.transport(context.Background(), "default", statusTLS{
		ServiceName: "demo-1", CASecretName: "demo-ca", ClientTLSSecret: "demo-client-tls",
	})
	if err != nil {
		t.Fatal(err)
	}
	if transport.IdleConnTimeout <= 0 {
		t.Fatalf("IdleConnTimeout = %s, want a bound", transport.IdleConnTimeout)
	}
}
