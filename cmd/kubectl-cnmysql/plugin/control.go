/*
Copyright 2026 The CNMySQL Authors.

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

package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
)

// Instance manager ports (kept in sync with the controller's Pod spec).
const (
	ControlPort = 8080
	MetricsPort = 9187
)

// ControlClient talks to a single instance's manager over its mTLS control API,
// tunnelled through a port-forward. Construct it with DialControl and always
// Close it to tear the tunnel down.
type ControlClient struct {
	cluster  *mysqlv1alpha1.Cluster
	instance string
	scheme   string
	forward  *PortForward
	http     *http.Client
}

// DialControl opens a port-forward to the instance's control port and prepares
// an mTLS HTTP client. The caller owns the returned client and must Close it.
func (e *Env) DialControl(
	ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string,
) (*ControlClient, error) {
	// The control API is always mutually authenticated.
	return e.dial(ctx, cluster, instanceName, ControlPort, true)
}

// DialMetrics opens a port-forward to the instance's metrics port. The metrics
// endpoint is served over mTLS only when monitoring TLS is enabled on the
// cluster (spec.monitoring.tlsConfig.enabled); otherwise it is plain HTTP, so
// the scheme is chosen to match.
func (e *Env) DialMetrics(
	ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string,
) (*ControlClient, error) {
	return e.dial(ctx, cluster, instanceName, MetricsPort, MonitoringTLSEnabled(cluster))
}

func (e *Env) dial(
	ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string, port int, useTLS bool,
) (*ControlClient, error) {
	transport := &http.Transport{}
	scheme := "http"
	if useTLS {
		tlsConfig, err := e.controlTLSConfig(ctx, cluster, instanceName)
		if err != nil {
			return nil, err
		}
		transport.TLSClientConfig = tlsConfig
		scheme = "https"
	}
	fw, err := e.ForwardPod(ctx, cluster.Namespace, instanceName, port)
	if err != nil {
		return nil, err
	}
	return &ControlClient{
		cluster:  cluster,
		instance: instanceName,
		scheme:   scheme,
		forward:  fw,
		http: &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
		},
	}, nil
}

// Close tears down the underlying port-forward.
func (c *ControlClient) Close() {
	if c != nil {
		c.forward.Close()
	}
}

// do issues a request to the forwarded control API. path must start with "/".
func (c *ControlClient) do(ctx context.Context, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(payload)
	}
	url := fmt.Sprintf("%s://%s%s", c.scheme, c.forward.Local, path)
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return c.statusError(resp, path)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

// statusError turns a non-200 control response into an error, surfacing the
// JSON {"error": ...} body the instance manager returns on failures.
func (c *ControlClient) statusError(resp *http.Response, path string) error {
	data, _ := io.ReadAll(resp.Body)
	var payload struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(data, &payload) == nil && payload.Error != "" {
		return fmt.Errorf("%s on %s: %s", resp.Status, path, payload.Error)
	}
	return fmt.Errorf("%s on %s", resp.Status, path)
}

// Get issues a GET and decodes the JSON response into out.
func (c *ControlClient) Get(ctx context.Context, path string, out any) error {
	return c.do(ctx, http.MethodGet, path, nil, out)
}

// Post issues a POST with an optional JSON body, decoding the response into out
// when non-nil.
func (c *ControlClient) Post(ctx context.Context, path string, body, out any) error {
	return c.do(ctx, http.MethodPost, path, body, out)
}

// GetText issues a GET and returns the raw response body, used for non-JSON
// endpoints such as the Prometheus /metrics scrape.
func (c *ControlClient) GetText(ctx context.Context, path string) (string, error) {
	url := fmt.Sprintf("%s://%s%s", c.scheme, c.forward.Local, path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", c.statusError(resp, path)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
