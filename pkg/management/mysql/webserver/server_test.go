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

package webserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/replication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/user"
)

// fakeController is a configurable InstanceController for handler tests.
type fakeController struct {
	healthErr         error
	startupErr        error
	readyErr          error
	status            *Status
	statusErr         error
	promoteErr        error
	demoteErr         error
	configureErr      error
	restartErr        error
	restartInPlaceErr error
	upgradeErr        error
	semiSyncWaitErr   error

	promoteCalled        bool
	demoteCalled         bool
	configureSource      *replication.SourceOptions
	semiSyncWaitCount    *int
	restartCalled        bool
	restartInPlaceCalled bool
	upgradeHash          string
	upgradeBody          []byte
	upgradeCalled        bool

	reloadReq  *ReloadRequest
	reloadResp *ReloadResponse
	reloadErr  error

	userMgmtErr   error
	createUserReq *user.CreateUserRequest
	alterUserReq  *user.AlterUserRequest
	dropUserReq   *user.DropUserRequest
	listUsers     *user.ListUsersResponse
	createDBReq   *user.CreateDatabaseRequest
	dropDBReq     *user.DropDatabaseRequest
	listDatabases *user.ListDatabasesResponse

	setAsPrimaryErr  error
	setAsPrimaryUUID string
}

func (f *fakeController) CreateUser(_ context.Context, req user.CreateUserRequest) error {
	f.createUserReq = &req
	return f.userMgmtErr
}
func (f *fakeController) AlterUser(_ context.Context, req user.AlterUserRequest) error {
	f.alterUserReq = &req
	return f.userMgmtErr
}
func (f *fakeController) DropUser(_ context.Context, req user.DropUserRequest) error {
	f.dropUserReq = &req
	return f.userMgmtErr
}
func (f *fakeController) ListUsers(context.Context) (*user.ListUsersResponse, error) {
	return f.listUsers, f.userMgmtErr
}
func (f *fakeController) CreateDatabase(_ context.Context, req user.CreateDatabaseRequest) error {
	f.createDBReq = &req
	return f.userMgmtErr
}
func (f *fakeController) DropDatabase(_ context.Context, req user.DropDatabaseRequest) error {
	f.dropDBReq = &req
	return f.userMgmtErr
}
func (f *fakeController) ListDatabases(context.Context) (*user.ListDatabasesResponse, error) {
	return f.listDatabases, f.userMgmtErr
}

func (f *fakeController) Healthz(context.Context) error  { return f.healthErr }
func (f *fakeController) Startupz(context.Context) error { return f.startupErr }
func (f *fakeController) Readyz(context.Context) error   { return f.readyErr }
func (f *fakeController) Status(context.Context) (*Status, error) {
	return f.status, f.statusErr
}
func (f *fakeController) Promote(context.Context) error { f.promoteCalled = true; return f.promoteErr }
func (f *fakeController) Demote(context.Context) error  { f.demoteCalled = true; return f.demoteErr }
func (f *fakeController) EnsureReplicaConfigured(_ context.Context, opts replication.SourceOptions) error {
	f.configureSource = &opts
	return f.configureErr
}
func (f *fakeController) SetSemiSyncWaitForReplicaCount(_ context.Context, count int) error {
	f.semiSyncWaitCount = &count
	return f.semiSyncWaitErr
}
func (f *fakeController) Restart(context.Context) error { f.restartCalled = true; return f.restartErr }
func (f *fakeController) RestartInPlace(context.Context) error {
	f.restartInPlaceCalled = true
	return f.restartInPlaceErr
}
func (f *fakeController) UpgradeInstanceManager(_ context.Context, r io.Reader, expectedHash string) error {
	f.upgradeCalled = true
	f.upgradeHash = expectedHash
	if f.upgradeErr != nil {
		return f.upgradeErr
	}
	body, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.upgradeBody = body
	return nil
}
func (f *fakeController) Reload(_ context.Context, req ReloadRequest) (*ReloadResponse, error) {
	f.reloadReq = &req
	return f.reloadResp, f.reloadErr
}
func (f *fakeController) SetAsPrimary(_ context.Context, memberUUID string) error {
	f.setAsPrimaryUUID = memberUUID
	return f.setAsPrimaryErr
}

// backupController is an InstanceController that also streams a backup.
type backupController struct {
	fakeController
	payload string
	err     error
}

func (b *backupController) BackupStream(_ context.Context, w io.Writer) error {
	if b.err != nil {
		return b.err
	}
	_, _ = io.WriteString(w, b.payload)
	return nil
}

func TestBackupRouteOnlyWhenStreamerImplemented(t *testing.T) {
	// A plain controller does not advertise the backup route.
	if rec := do(t, Handler(&fakeController{}), http.MethodGet, "/cluster/backup"); rec.Code != http.StatusNotFound {
		t.Errorf("backup route on plain controller = %d, want 404", rec.Code)
	}

	// A streamer controller serves the archive.
	h := Handler(&backupController{payload: "xbstream-bytes"})
	rec := do(t, h, http.MethodGet, "/cluster/backup")
	if rec.Code != http.StatusOK {
		t.Fatalf("backup = %d, want 200", rec.Code)
	}
	if rec.Body.String() != "xbstream-bytes" {
		t.Errorf("backup body = %q", rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/x-xbstream" {
		t.Errorf("content type = %q", ct)
	}

	// An error before any bytes surfaces as a 500.
	herr := Handler(&backupController{err: errors.New("xtrabackup failed")})
	if rec := do(t, herr, http.MethodGet, "/cluster/backup"); rec.Code != http.StatusInternalServerError {
		t.Errorf("failed backup = %d, want 500", rec.Code)
	}
}

func do(t *testing.T, h http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, nil)
	h.ServeHTTP(rec, req)
	return rec
}

func doWithBody(t *testing.T, h http.Handler, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	h.ServeHTTP(rec, req)
	return rec
}

func TestHealthzReadyz(t *testing.T) {
	h := Handler(&fakeController{})
	if rec := do(t, h, http.MethodGet, "/healthz"); rec.Code != http.StatusOK {
		t.Errorf("healthz = %d, want 200", rec.Code)
	}
	if rec := do(t, h, http.MethodGet, "/livez"); rec.Code != http.StatusOK {
		t.Errorf("livez = %d, want 200", rec.Code)
	}
	if rec := do(t, h, http.MethodGet, "/readyz"); rec.Code != http.StatusOK {
		t.Errorf("readyz = %d, want 200", rec.Code)
	}

	unhealthy := Handler(&fakeController{
		healthErr: errors.New("down"),
		readyErr:  errors.New("not ready"),
	})
	if rec := do(t, unhealthy, http.MethodGet, "/healthz"); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("unhealthy healthz = %d, want 503", rec.Code)
	}
	if rec := do(t, unhealthy, http.MethodGet, "/livez"); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("unhealthy livez = %d, want 503", rec.Code)
	}
	if rec := do(t, unhealthy, http.MethodGet, "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("not-ready readyz = %d, want 503", rec.Code)
	}
}

func TestHealthHandlerOnlyServesProbes(t *testing.T) {
	h := HealthHandler(&fakeController{})
	if rec := do(t, h, http.MethodGet, "/livez"); rec.Code != http.StatusOK {
		t.Errorf("livez = %d, want 200", rec.Code)
	}
	if rec := do(t, h, http.MethodGet, "/readyz"); rec.Code != http.StatusOK {
		t.Errorf("readyz = %d, want 200", rec.Code)
	}
	if rec := do(t, h, http.MethodGet, "/status"); rec.Code != http.StatusNotFound {
		t.Errorf("status on health handler = %d, want 404", rec.Code)
	}
	if rec := do(t, h, http.MethodPost, "/promote"); rec.Code != http.StatusNotFound {
		t.Errorf("promote on health handler = %d, want 404", rec.Code)
	}
}

func TestStatusJSON(t *testing.T) {
	lag := int64(2)
	h := Handler(&fakeController{status: &Status{
		InstanceName: "cluster-1",
		Role:         RoleReplica,
		Version:      "8.0.36",
		IsReady:      true,
		ReadOnly:     true,
		GTIDExecuted: "uuid:1-100",
		Replication: &ReplicationStatus{
			SourceHost:          "cluster-rw",
			IORunning:           true,
			SQLRunning:          true,
			SecondsBehindSource: &lag,
		},
	}})

	rec := do(t, h, http.MethodGet, "/status")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q", ct)
	}

	var got Status
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Role != RoleReplica || got.InstanceName != "cluster-1" {
		t.Errorf("unexpected status: %+v", got)
	}
	if got.Replication == nil || got.Replication.SecondsBehindSource == nil || *got.Replication.SecondsBehindSource != 2 {
		t.Errorf("replication lag not round-tripped: %+v", got.Replication)
	}
}

func TestStatusError(t *testing.T) {
	h := Handler(&fakeController{statusErr: errors.New("boom")})
	rec := do(t, h, http.MethodGet, "/status")
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status err = %d, want 500", rec.Code)
	}
}

func TestLifecycleActions(t *testing.T) {
	fc := &fakeController{}
	h := Handler(fc)

	if rec := do(t, h, http.MethodPost, "/promote"); rec.Code != http.StatusOK || !fc.promoteCalled {
		t.Errorf("promote = %d called=%v", rec.Code, fc.promoteCalled)
	}
	if rec := do(t, h, http.MethodPost, "/demote"); rec.Code != http.StatusOK || !fc.demoteCalled {
		t.Errorf("demote = %d called=%v", rec.Code, fc.demoteCalled)
	}
	body := `{"source":{"host":"demo-2.default.svc","port":3306,"user":"cloudnative-mysql_repl","autoPosition":true,"ssl":true}}` //nolint:lll
	rec := doWithBody(t, h, "/replica/source", body)
	if rec.Code != http.StatusOK || fc.configureSource == nil {
		t.Errorf("configure replica = %d source=%#v", rec.Code, fc.configureSource)
	}
	if fc.configureSource.Host != "demo-2.default.svc" || !fc.configureSource.AutoPosition {
		t.Errorf("configure source = %#v", fc.configureSource)
	}
	if rec := do(t, h, http.MethodPost, "/restart"); rec.Code != http.StatusOK || !fc.restartCalled {
		t.Errorf("restart = %d called=%v", rec.Code, fc.restartCalled)
	}
	rec = do(t, h, http.MethodPost, "/instance/manager/restart-inplace")
	if rec.Code != http.StatusOK || !fc.restartInPlaceCalled {
		t.Errorf("restart-inplace = %d called=%v", rec.Code, fc.restartInPlaceCalled)
	}
	rec = doWithBody(t, h, "/semisync/wait", `{"count":2}`)
	if rec.Code != http.StatusOK || fc.semiSyncWaitCount == nil || *fc.semiSyncWaitCount != 2 {
		t.Errorf("semisync wait = %d count=%v", rec.Code, fc.semiSyncWaitCount)
	}
}

func TestUpgradeManagerRoute(t *testing.T) {
	t.Run("streams body and hash to the controller", func(t *testing.T) {
		fc := &fakeController{}
		req := httptest.NewRequest(http.MethodPost, "/instance/manager/upgrade", strings.NewReader("new-binary-bytes"))
		req.Header.Set(ManagerHashHeader, "expected-hash")
		rec := httptest.NewRecorder()
		Handler(fc).ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("upgrade = %d, want 200", rec.Code)
		}
		if !fc.upgradeCalled || fc.upgradeHash != "expected-hash" || string(fc.upgradeBody) != "new-binary-bytes" {
			t.Errorf("upgrade called=%v hash=%q body=%q", fc.upgradeCalled, fc.upgradeHash, fc.upgradeBody)
		}
	})

	t.Run("hash mismatch maps to 400", func(t *testing.T) {
		fc := &fakeController{upgradeErr: ErrInvalidInstanceManagerBinary}
		req := httptest.NewRequest(http.MethodPost, "/instance/manager/upgrade", strings.NewReader("x"))
		rec := httptest.NewRecorder()
		Handler(fc).ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("invalid binary = %d, want 400", rec.Code)
		}
	})

	t.Run("other errors map to 500", func(t *testing.T) {
		fc := &fakeController{upgradeErr: errors.New("disk full")}
		req := httptest.NewRequest(http.MethodPost, "/instance/manager/upgrade", strings.NewReader("x"))
		rec := httptest.NewRecorder()
		Handler(fc).ServeHTTP(rec, req)
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("write failure = %d, want 500", rec.Code)
		}
	})
}

func TestReloadHandler(t *testing.T) {
	fc := &fakeController{reloadResp: &ReloadResponse{
		Applied: []string{"max_connections"},
		Skipped: map[string]string{"innodb_buffer_pool_size": "not dynamic"},
	}}
	h := Handler(fc)
	rec := doWithBody(t, h, "/reload", `{"parameters":{"max_connections":"200"}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("reload = %d, want 200", rec.Code)
	}
	if fc.reloadReq == nil || fc.reloadReq.Parameters["max_connections"] != "200" {
		t.Fatalf("reload req = %#v", fc.reloadReq)
	}
	var got ReloadResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(got.Applied) != 1 || got.Applied[0] != "max_connections" {
		t.Errorf("applied = %v", got.Applied)
	}
	if got.Skipped["innodb_buffer_pool_size"] == "" {
		t.Errorf("skipped = %v", got.Skipped)
	}
}

func TestLifecycleActionError(t *testing.T) {
	h := Handler(&fakeController{promoteErr: errors.New("cannot promote")})
	rec := do(t, h, http.MethodPost, "/promote")
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("promote err = %d, want 500", rec.Code)
	}
}

func TestUserManagementRoutes(t *testing.T) {
	fc := &fakeController{}
	h := Handler(fc)

	rec := doWithBody(t, h, "/user/create",
		`{"name":"app","host":"%","password":"pw","maxUserConnections":5}`)
	if rec.Code != http.StatusOK || fc.createUserReq == nil {
		t.Fatalf("create user = %d req=%#v", rec.Code, fc.createUserReq)
	}
	if fc.createUserReq.Name != "app" || fc.createUserReq.MaxUserConnections != 5 {
		t.Errorf("create user req = %#v", fc.createUserReq)
	}

	rec = doWithBody(t, h, "/user/alter", `{"name":"app","password":"newpw"}`)
	if rec.Code != http.StatusOK || fc.alterUserReq == nil || fc.alterUserReq.Password == nil {
		t.Errorf("alter user = %d req=%#v", rec.Code, fc.alterUserReq)
	}

	rec = doWithBody(t, h, "/user/drop", `{"name":"app","host":"%"}`)
	if rec.Code != http.StatusOK || fc.dropUserReq == nil {
		t.Errorf("drop user = %d req=%#v", rec.Code, fc.dropUserReq)
	}

	rec = doWithBody(t, h, "/database/create", `{"name":"appdb","characterSet":"utf8mb4"}`)
	if rec.Code != http.StatusOK || fc.createDBReq == nil || fc.createDBReq.Name != "appdb" {
		t.Errorf("create db = %d req=%#v", rec.Code, fc.createDBReq)
	}

	rec = doWithBody(t, h, "/database/drop", `{"name":"appdb"}`)
	if rec.Code != http.StatusOK || fc.dropDBReq == nil {
		t.Errorf("drop db = %d req=%#v", rec.Code, fc.dropDBReq)
	}
}

func TestListUsersRoute(t *testing.T) {
	fc := &fakeController{listUsers: &user.ListUsersResponse{
		Users: []user.UserInfo{{Name: "app", Host: "%", RequireTLS: "x509"}},
	}}
	rec := do(t, Handler(fc), http.MethodGet, "/user/list")
	if rec.Code != http.StatusOK {
		t.Fatalf("list users = %d, want 200", rec.Code)
	}
	var got user.ListUsersResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got.Users) != 1 || got.Users[0].Name != "app" || got.Users[0].RequireTLS != "x509" {
		t.Errorf("list users body = %+v", got)
	}
}

func TestListDatabasesRoute(t *testing.T) {
	fc := &fakeController{listDatabases: &user.ListDatabasesResponse{Databases: []string{"a", "b"}}}
	rec := do(t, Handler(fc), http.MethodGet, "/database/list")
	if rec.Code != http.StatusOK {
		t.Fatalf("list databases = %d, want 200", rec.Code)
	}
	var got user.ListDatabasesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got.Databases) != 2 {
		t.Errorf("list databases body = %+v", got)
	}
}

func TestUserManagementBadBody(t *testing.T) {
	rec := doWithBody(t, Handler(&fakeController{}), "/user/create", `{not json`)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("bad body = %d, want 400", rec.Code)
	}
}

func TestUserManagementError(t *testing.T) {
	h := Handler(&fakeController{userMgmtErr: errors.New("mysql down")})
	rec := doWithBody(t, h, "/user/create", `{"name":"app"}`)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("user mgmt err = %d, want 500", rec.Code)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	h := Handler(&fakeController{})
	// GET on a POST-only route should not be served as 200.
	rec := do(t, h, http.MethodGet, "/promote")
	if rec.Code == http.StatusOK {
		t.Errorf("GET /promote should not return 200")
	}
}
