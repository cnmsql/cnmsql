/*
Copyright 2026 The cloudnative-mysql Authors.

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
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

// TLSOptions configures the mutual-TLS server.
type TLSOptions struct {
	// ServerCertFile and ServerKeyFile are the server's certificate and key.
	ServerCertFile string
	ServerKeyFile  string
	// ClientCAFile is the CA bundle used to verify operator client certs.
	ClientCAFile string
}

// MTLSConfig builds a tls.Config that requires and verifies a client
// certificate signed by the configured client CA. It is exported so other
// servers (notably the Prometheus metrics endpoint) can adopt the same
// mutual-TLS posture as the control API rather than re-implementing it.
func (o TLSOptions) MTLSConfig() (*tls.Config, error) {
	return o.mtlsConfig()
}

// mtlsConfig builds a tls.Config that requires and verifies a client
// certificate signed by the configured client CA.
func (o TLSOptions) mtlsConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(o.ServerCertFile, o.ServerKeyFile)
	if err != nil {
		return nil, fmt.Errorf("loading server keypair: %w", err)
	}

	caPEM, err := os.ReadFile(o.ClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("reading client CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("no valid certificates found in client CA %q", o.ClientCAFile)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// TLSCertManager provides a dynamically reloadable mutual-TLS configuration.
// Each new TLS connection receives the latest cert/key/CA material via
// GetConfigForClient, so certificate rotations take effect without a server
// restart.
type TLSCertManager struct {
	serverCertFile string
	serverKeyFile  string
	clientCAFile   string

	mu     sync.RWMutex
	config *tls.Config

	// OnReload is called after the cert manager successfully reloads its
	// own key material. Use it to notify other components (e.g. mysqld)
	// to reload their TLS configuration.
	OnReload func(ctx context.Context) error
}

// NewTLSCertManager loads the initial mTLS configuration from the given files
// and returns a manager that can reload on cert-file changes.
func NewTLSCertManager(opts TLSOptions) (*TLSCertManager, error) {
	m := &TLSCertManager{
		serverCertFile: opts.ServerCertFile,
		serverKeyFile:  opts.ServerKeyFile,
		clientCAFile:   opts.ClientCAFile,
	}
	if err := m.load(); err != nil {
		return nil, err
	}
	return m, nil
}

// load reads the current certs from disk and atomically swaps the cached
// configuration. If loading fails the existing configuration is preserved.
func (m *TLSCertManager) load() error {
	cert, err := tls.LoadX509KeyPair(m.serverCertFile, m.serverKeyFile)
	if err != nil {
		return fmt.Errorf("loading server keypair: %w", err)
	}

	caPEM, err := os.ReadFile(m.clientCAFile)
	if err != nil {
		return fmt.Errorf("reading client CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return fmt.Errorf("no valid certificates found in client CA %q", m.clientCAFile)
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}

	m.mu.Lock()
	m.config = cfg
	m.mu.Unlock()
	return nil
}

// Reload re-reads the key material from disk and, if configured, calls OnReload
// so other components (e.g. mysqld) can refresh their TLS configuration. An
// error means the reload failed and the previous configuration continues to be
// served.
func (m *TLSCertManager) Reload(ctx context.Context) error {
	if err := m.load(); err != nil {
		return err
	}
	if m.OnReload != nil {
		return m.OnReload(ctx)
	}
	return nil
}

// TLSConfig returns a *tls.Config suitable for an http.Server. Each new TLS
// handshake receives a fresh clone of the latest certificate configuration,
// allowing the server to serve renewed certificates without restarting.
func (m *TLSCertManager) TLSConfig() *tls.Config {
	return &tls.Config{
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			m.mu.RLock()
			cfg := m.config.Clone()
			m.mu.RUnlock()
			return cfg, nil
		},
		MinVersion: tls.VersionTLS13,
	}
}

// WatchedFiles returns the file paths whose changes should trigger a
// certificate reload.
func (m *TLSCertManager) WatchedFiles() []string {
	return []string{m.serverCertFile, m.serverKeyFile, m.clientCAFile}
}

// NewServer builds an http.Server that serves the control API over mTLS on the
// given address.
func NewServer(addr string, controller InstanceController, opts TLSOptions) (*http.Server, error) {
	tlsConfig, err := opts.mtlsConfig()
	if err != nil {
		return nil, err
	}

	return &http.Server{
		Addr:              addr,
		Handler:           Handler(controller),
		TLSConfig:         tlsConfig,
		ReadHeaderTimeout: 10 * time.Second,
	}, nil
}

// NewServerDynamic builds an http.Server that serves the control API over mTLS
// with dynamic certificate reloading. The manager's TLSConfig is consulted for
// every new connection.
func NewServerDynamic(addr string, controller InstanceController, mgr *TLSCertManager) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           Handler(controller),
		TLSConfig:         mgr.TLSConfig(),
		ReadHeaderTimeout: 10 * time.Second,
	}
}
