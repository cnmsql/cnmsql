//go:build integration

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

package integration

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// writeTestPKI generates a throwaway PKI into dir: a self-signed CA, a server
// certificate (SAN 127.0.0.1, localhost) for the source mysqld and a client
// certificate (CN repl) for the replica's IO thread. It writes ca.crt,
// server.crt, server.key, client.crt and client.key as PEM — certificates at
// 0644 and keys at 0600 on the host.
func writeTestPKI(t *testing.T, dir string) {
	t.Helper()

	caKey := mustECDSAKey(t)
	caDER := mustCertificate(t, &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}, nil, caKey, caKey)
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}

	serverKey := mustECDSAKey(t)
	serverDER := mustCertificate(t, &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}, caCert, serverKey, caKey)

	clientKey := mustECDSAKey(t)
	clientDER := mustCertificate(t, &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "repl"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}, caCert, clientKey, caKey)

	mustWritePEM(t, filepath.Join(dir, "ca.crt"), "CERTIFICATE", caDER, 0o644)
	mustWritePEM(t, filepath.Join(dir, "server.crt"), "CERTIFICATE", serverDER, 0o644)
	mustWritePEM(t, filepath.Join(dir, "client.crt"), "CERTIFICATE", clientDER, 0o644)
	mustWritePEM(t, filepath.Join(dir, "server.key"), "PRIVATE KEY", mustPKCS8(t, serverKey), 0o0600)
	mustWritePEM(t, filepath.Join(dir, "client.key"), "PRIVATE KEY", mustPKCS8(t, clientKey), 0o0600)
}

// pkiContainerFiles generates the test PKI into a temp dir and returns the
// testcontainers mounts exposing it at /pki. Every file is mounted 0644,
// because mysqld runs as uid 1001 inside the image and must be able to read
// the private keys.
func pkiContainerFiles(t *testing.T) []testcontainers.ContainerFile {
	t.Helper()

	dir := t.TempDir()
	writeTestPKI(t, dir)

	var files []testcontainers.ContainerFile
	for _, name := range []string{"ca.crt", "server.crt", "server.key", "client.crt", "client.key"} {
		files = append(files, testcontainers.ContainerFile{
			HostFilePath:      filepath.Join(dir, name),
			ContainerFilePath: "/pki/" + name,
			FileMode:          0o644,
		})
	}
	return files
}

func mustECDSAKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

// mustCertificate signs template with signer (a nil parent means self-signed).
func mustCertificate(t *testing.T, template *x509.Certificate, parent *x509.Certificate, key *ecdsa.PrivateKey, signer *ecdsa.PrivateKey) []byte {
	t.Helper()
	if parent == nil {
		parent = template
	}
	der, err := x509.CreateCertificate(rand.Reader, template, parent, &key.PublicKey, signer)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

func mustPKCS8(t *testing.T, key *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

func mustWritePEM(t *testing.T, path, blockType string, der []byte, mode os.FileMode) {
	t.Helper()
	buf := pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der})
	if err := os.WriteFile(path, buf, mode); err != nil {
		t.Fatal(err)
	}
}
