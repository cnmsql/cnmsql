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
	"fmt"
	"os"
)

// ClientTLSOptions locate what a worker presents to an instance manager, and
// what it trusts from it.
type ClientTLSOptions struct {
	CertFile string
	KeyFile  string
	CAFile   string
	// ServerName is the instance manager's name in its certificate.
	ServerName string
}

// ClientTLSConfig builds the TLS config of a client that mutually
// authenticates to an instance manager: its certificate, the CA that signed
// the manager's, and the manager's expected name.
func ClientTLSConfig(o ClientTLSOptions) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(o.CertFile, o.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("loading client certificate: %w", err)
	}
	caPEM, err := os.ReadFile(o.CAFile)
	if err != nil {
		return nil, fmt.Errorf("reading CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("CA file %s contains no certificates", o.CAFile)
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		ServerName:   o.ServerName,
		Certificates: []tls.Certificate{cert},
		RootCAs:      roots,
	}, nil
}
