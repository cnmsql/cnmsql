/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package controller

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/user"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
)

// HTTPControlClient drives the mTLS control API exposed by the instance manager.
type HTTPControlClient struct {
	Client     client.Client
	HTTPClient *http.Client
}

// HTTPStatusClient reads instance status through the mTLS control API exposed
// by the instance manager.
type HTTPStatusClient = HTTPControlClient

// Status fetches /status from the per-instance Service.
func (c *HTTPControlClient) Status(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string) (*webserver.Status, error) {
	resp, err := c.do(ctx, cluster, instanceName, http.MethodGet, "/status", nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("instance status returned %s", resp.Status)
	}
	var status webserver.Status
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, err
	}
	return &status, nil
}

// CreateUser creates a MySQL user on the named instance.
func (c *HTTPControlClient) CreateUser(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string, req user.CreateUserRequest) error {
	return c.action(ctx, cluster, instanceName, "/user/create", req)
}

// AlterUser mutates a MySQL user on the named instance.
func (c *HTTPControlClient) AlterUser(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string, req user.AlterUserRequest) error {
	return c.action(ctx, cluster, instanceName, "/user/alter", req)
}

// DropUser removes a MySQL user on the named instance.
func (c *HTTPControlClient) DropUser(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string, req user.DropUserRequest) error {
	return c.action(ctx, cluster, instanceName, "/user/drop", req)
}

// ListUsers reads the managed MySQL users from the named instance.
func (c *HTTPControlClient) ListUsers(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string) (*user.ListUsersResponse, error) {
	var result user.ListUsersResponse
	if err := c.fetch(ctx, cluster, instanceName, "/user/list", &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// CreateDatabase creates a MySQL schema on the named instance.
func (c *HTTPControlClient) CreateDatabase(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string, req user.CreateDatabaseRequest) error {
	return c.action(ctx, cluster, instanceName, "/database/create", req)
}

// DropDatabase drops a MySQL schema on the named instance.
func (c *HTTPControlClient) DropDatabase(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string, req user.DropDatabaseRequest) error {
	return c.action(ctx, cluster, instanceName, "/database/drop", req)
}

// Reload re-applies dynamic configuration parameters to the named instance via
// its control API and returns the per-parameter outcome.
func (c *HTTPControlClient) Reload(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string, req webserver.ReloadRequest) (*webserver.ReloadResponse, error) {
	resp, err := c.do(ctx, cluster, instanceName, http.MethodPost, "/reload", req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("instance /reload returned %s", resp.Status)
	}
	var result webserver.ReloadResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// SetSemiSyncWaitForReplicaCount adjusts the semi-sync acknowledgement count on
// the named instance (used to self-heal semi-sync under "preferred" durability).
func (c *HTTPControlClient) SetSemiSyncWaitForReplicaCount(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string, count int) error {
	return c.action(ctx, cluster, instanceName, "/semisync/wait", webserver.SemiSyncWaitRequest{Count: count})
}

// ListDatabases reads the user-managed MySQL schemas from the named instance.
func (c *HTTPControlClient) ListDatabases(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string) (*user.ListDatabasesResponse, error) {
	var result user.ListDatabasesResponse
	if err := c.fetch(ctx, cluster, instanceName, "/database/list", &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// action POSTs a JSON body to an instance control endpoint and treats any
// non-200 status as an error.
func (c *HTTPControlClient) action(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName, path string, body any) error {
	resp, err := c.do(ctx, cluster, instanceName, http.MethodPost, path, body)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("instance %s returned %s", path, resp.Status)
	}
	return nil
}

// fetch GETs a JSON result from an instance control endpoint.
func (c *HTTPControlClient) fetch(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName, path string, result any) error {
	resp, err := c.do(ctx, cluster, instanceName, http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("instance %s returned %s", path, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(result)
}

func (c *HTTPControlClient) do(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName, method, path string, body any) (*http.Response, error) {
	conn := statusTLS{
		ServiceName:     instanceName,
		CASecretName:    cluster.Name + "-ca",
		ClientTLSSecret: cluster.Name + "-client-tls",
	}
	if certs := cluster.Spec.Certificates; certs != nil {
		if certs.ServerCASecret != "" {
			conn.CASecretName = certs.ServerCASecret
		}
		if certs.ReplicationTLSSecret != "" {
			conn.ClientTLSSecret = certs.ReplicationTLSSecret
		}
	}

	transport, err := c.transport(ctx, cluster.Namespace, conn)
	if err != nil {
		return nil, err
	}
	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 5 * time.Second}
	}
	clientCopy := *httpClient
	clientCopy.Transport = transport

	var requestBody *bytes.Reader
	if body == nil {
		requestBody = bytes.NewReader(nil)
	} else {
		payload, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		requestBody = bytes.NewReader(payload)
	}
	url := fmt.Sprintf("https://%s.%s.svc:8080%s", conn.ServiceName, cluster.Namespace, path)
	req, err := http.NewRequestWithContext(ctx, method, url, requestBody)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return clientCopy.Do(req)
}

// statusTLS holds the names needed to build the mTLS connection to an instance
// manager's control API.
type statusTLS struct {
	ServiceName     string
	CASecretName    string
	ClientTLSSecret string
}

func (c *HTTPControlClient) transport(ctx context.Context, namespace string, conn statusTLS) (*http.Transport, error) {
	caSecret := &corev1.Secret{}
	if err := c.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: conn.CASecretName}, caSecret); err != nil {
		return nil, err
	}
	clientSecret := &corev1.Secret{}
	if err := c.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: conn.ClientTLSSecret}, clientSecret); err != nil {
		return nil, err
	}

	cert, err := tls.X509KeyPair(clientSecret.Data[corev1.TLSCertKey], clientSecret.Data[corev1.TLSPrivateKeyKey])
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caSecret.Data["ca.crt"]) {
		return nil, fmt.Errorf("secret %s does not contain a valid ca.crt", conn.CASecretName)
	}

	return &http.Transport{
		TLSClientConfig: &tls.Config{
			MinVersion:   tls.VersionTLS12,
			ServerName:   conn.ServiceName + "." + namespace + ".svc",
			Certificates: []tls.Certificate{cert},
			RootCAs:      roots,
		},
	}, nil
}
