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
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func requestAs(method, path string, commonName string) *http.Request {
	req := httptest.NewRequest(method, path, nil)
	if commonName != "" {
		leaf := &x509.Certificate{Subject: pkix.Name{CommonName: commonName}}
		req.TLS = &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{leaf},
			VerifiedChains:   [][]*x509.Certificate{{leaf}},
		}
	}
	return req
}

func TestAuthorizeClients(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	handler := AuthorizeClients(ok)
	peer := "prod-2.default.svc"
	cases := []struct {
		name, method, path, commonName string
		want                           int
	}{
		{"operator calls status", http.MethodGet, "/status", OperatorCommonName, http.StatusOK},
		{"operator upgrades the manager", http.MethodPost, "/instance/manager/upgrade", OperatorCommonName, http.StatusOK},
		{"operator streams a backup", http.MethodGet, "/cluster/backup", OperatorCommonName, http.StatusOK},
		{"peer streams a backup to join", http.MethodGet, "/cluster/backup", peer, http.StatusOK},
		{"peer cannot upgrade the manager", http.MethodPost, "/instance/manager/upgrade", peer, http.StatusForbidden},
		{"peer cannot promote", http.MethodPost, "/promote", peer, http.StatusForbidden},
		{"peer cannot load SQL", http.MethodPost, "/cluster/load", peer, http.StatusForbidden},
		{"peer cannot dump", http.MethodPost, "/cluster/dump", peer, http.StatusForbidden},
		{"peer cannot create users", http.MethodPost, "/user/create", peer, http.StatusForbidden},
		{"peer cannot read status", http.MethodGet, "/status", peer, http.StatusForbidden},
		{"peer cannot post to the backup route", http.MethodPost, "/cluster/backup", peer, http.StatusForbidden},
		{"lookalike common name is a peer", http.MethodPost, "/promote", OperatorCommonName + " ", http.StatusForbidden},
		{"no client certificate", http.MethodGet, "/cluster/backup", "", http.StatusUnauthorized},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, requestAs(tc.method, tc.path, tc.commonName))
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d", rec.Code, tc.want)
			}
		})
	}
}

// An unverified peer certificate (one the server did not chain to its client
// CA) must never be trusted, even when it names the operator.
func TestAuthorizeClientsIgnoresUnverifiedCertificates(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	req := httptest.NewRequest(http.MethodPost, "/promote", nil)
	req.TLS = &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{{Subject: pkix.Name{CommonName: OperatorCommonName}}},
	}
	rec := httptest.NewRecorder()
	AuthorizeClients(ok).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// Over a real mutual-TLS handshake, an instance certificate under the cluster
// CA reaches the join route but not the manager upgrade.
func TestAuthorizeClientsOverMutualTLS(t *testing.T) {
	dir := t.TempDir()
	serverCert, serverKey := writeCert(t, dir, "prod-1.default.svc")
	peerCert, peerKey := writeCert(t, dir, "prod-2.default.svc")
	opCert, opKey := writeCert(t, dir, OperatorCommonName)

	pair, err := tls.LoadX509KeyPair(serverCert, serverKey)
	if err != nil {
		t.Fatal(err)
	}
	clientCAs := x509.NewCertPool()
	for _, path := range []string{peerCert, opCert} {
		pem, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		clientCAs.AppendCertsFromPEM(pem)
	}
	srv := httptest.NewUnstartedServer(AuthorizeClients(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})))
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{pair},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	}
	srv.StartTLS()
	defer srv.Close()

	call := func(cert, key, method, path string) int {
		t.Helper()
		cfg, err := ClientTLSConfig(ClientTLSOptions{
			CertFile: cert, KeyFile: key, CAFile: serverCert, ServerName: "prod-1.default.svc",
		})
		if err != nil {
			t.Fatal(err)
		}
		client := &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}, Timeout: 10 * time.Second}
		req, err := http.NewRequest(method, srv.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	if got := call(peerCert, peerKey, http.MethodGet, "/cluster/backup"); got != http.StatusOK {
		t.Fatalf("peer join stream: status = %d", got)
	}
	if got := call(peerCert, peerKey, http.MethodPost, "/instance/manager/upgrade"); got != http.StatusForbidden {
		t.Fatalf("peer manager upgrade: status = %d", got)
	}
	if got := call(opCert, opKey, http.MethodPost, "/instance/manager/upgrade"); got != http.StatusOK {
		t.Fatalf("operator manager upgrade: status = %d", got)
	}
}
