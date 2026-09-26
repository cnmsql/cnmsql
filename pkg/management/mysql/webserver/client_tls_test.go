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

package webserver

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeCert writes a self-signed certificate for name, usable as a server,
// client and CA certificate, and returns its cert and key paths.
func writeCert(t *testing.T, dir, name string) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		DNSNames:              []string{name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPath, keyPath := filepath.Join(dir, name+".crt"), filepath.Join(dir, name+".key")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}

func TestClientTLSConfigAuthenticatesBothWays(t *testing.T) {
	dir := t.TempDir()
	serverCert, serverKey := writeCert(t, dir, "prod-1.default.svc")
	clientCert, clientKey := writeCert(t, dir, "worker")

	pair, err := tls.LoadX509KeyPair(serverCert, serverKey)
	if err != nil {
		t.Fatal(err)
	}
	clientPEM, err := os.ReadFile(clientCert)
	if err != nil {
		t.Fatal(err)
	}
	clientCAs := x509.NewCertPool()
	clientCAs.AppendCertsFromPEM(clientPEM)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(r.TLS.PeerCertificates[0].Subject.CommonName))
	}))
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{pair},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	}
	srv.StartTLS()
	defer srv.Close()

	get := func(serverName string) (string, error) {
		cfg, err := ClientTLSConfig(ClientTLSOptions{
			CertFile: clientCert, KeyFile: clientKey, CAFile: serverCert, ServerName: serverName,
		})
		if err != nil {
			t.Fatal(err)
		}
		client := &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}, Timeout: 10 * time.Second}
		resp, err := client.Get(srv.URL)
		if err != nil {
			return "", err
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		return string(body), err
	}
	if peer, err := get("prod-1.default.svc"); err != nil || peer != "worker" {
		t.Fatalf("peer = %q, err = %v", peer, err)
	}
	if _, err := get("prod-2.default.svc"); err == nil {
		t.Fatal("a server with another name was trusted")
	}
}

func TestClientTLSConfigRejectsAnEmptyCA(t *testing.T) {
	dir := t.TempDir()
	cert, key := writeCert(t, dir, "worker")
	empty := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := ClientTLSConfig(ClientTLSOptions{CertFile: cert, KeyFile: key, CAFile: empty})
	if err == nil || !strings.Contains(err.Error(), "no certificates") {
		t.Fatalf("err = %v", err)
	}
}
