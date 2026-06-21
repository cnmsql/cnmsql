/*
Copyright 2026 The CloudNative MySQL Authors.

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
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
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

// SetAsPrimary performs a planned Group Replication primary change on the named
// instance, designating the member with the given server_uuid.
func (c *HTTPControlClient) SetAsPrimary(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName, memberUUID string) error {
	return c.action(ctx, cluster, instanceName, "/group/set-as-primary", webserver.GroupSetAsPrimaryRequest{MemberUUID: memberUUID})
}

// ListDatabases reads the user-managed MySQL schemas from the named instance.
func (c *HTTPControlClient) ListDatabases(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string) (*user.ListDatabasesResponse, error) {
	var result user.ListDatabasesResponse
	if err := c.fetch(ctx, cluster, instanceName, "/database/list", &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// managerUpgradeTimeout bounds an in-place manager upgrade request: streaming
// the (tens-of-MB) operator binary and getting the pre-re-exec acknowledgement
// back takes longer than a status poll, so it overrides the 5s default.
const managerUpgradeTimeout = 60 * time.Second

// UpgradeInstanceManager streams binary to the named instance's
// /instance/manager/upgrade endpoint, tagged with expectedHash, so the manager
// validates and re-execs it in place. A non-200 response (including the 400 the
// manager returns for a hash mismatch) is treated as an error.
func (c *HTTPControlClient) UpgradeInstanceManager(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string, binary io.Reader, expectedHash string) error {
	resp, err := c.doRaw(ctx, cluster, instanceName, http.MethodPost, "/instance/manager/upgrade", binary, map[string]string{
		webserver.ManagerHashHeader: expectedHash,
	}, managerUpgradeTimeout)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("instance /instance/manager/upgrade returned %s: %s", resp.Status, bytes.TrimSpace(body))
	}
	return nil
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
	var requestBody io.Reader
	headers := map[string]string{}
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		requestBody = bytes.NewReader(payload)
		headers["Content-Type"] = "application/json"
	}
	return c.doRaw(ctx, cluster, instanceName, method, path, requestBody, headers, 0)
}

// doRaw issues a request with an arbitrary body reader and headers over the
// instance's mTLS control API. It backs both the JSON helpers (do) and the
// binary-streaming upgrade path. A nil body sends an empty request; a zero
// timeout falls back to the 5s status-poll default.
func (c *HTTPControlClient) doRaw(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName, method, path string, body io.Reader, headers map[string]string, timeout time.Duration) (*http.Response, error) {
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
	if timeout > 0 {
		clientCopy.Timeout = timeout
	}

	url := fmt.Sprintf("https://%s.%s.svc:8080%s", conn.ServiceName, cluster.Namespace, path)
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
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
