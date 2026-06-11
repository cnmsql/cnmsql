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

package webserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// fakeController is a configurable InstanceController for handler tests.
type fakeController struct {
	healthErr  error
	readyErr   error
	status     *Status
	statusErr  error
	promoteErr error
	demoteErr  error
	restartErr error

	promoteCalled bool
	demoteCalled  bool
	restartCalled bool
}

func (f *fakeController) Healthz(context.Context) error { return f.healthErr }
func (f *fakeController) Readyz(context.Context) error  { return f.readyErr }
func (f *fakeController) Status(context.Context) (*Status, error) {
	return f.status, f.statusErr
}
func (f *fakeController) Promote(context.Context) error { f.promoteCalled = true; return f.promoteErr }
func (f *fakeController) Demote(context.Context) error  { f.demoteCalled = true; return f.demoteErr }
func (f *fakeController) Restart(context.Context) error { f.restartCalled = true; return f.restartErr }

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

func TestHealthzReadyz(t *testing.T) {
	h := Handler(&fakeController{})
	if rec := do(t, h, http.MethodGet, "/healthz"); rec.Code != http.StatusOK {
		t.Errorf("healthz = %d, want 200", rec.Code)
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
	if rec := do(t, unhealthy, http.MethodGet, "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("not-ready readyz = %d, want 503", rec.Code)
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
	if rec := do(t, h, http.MethodPost, "/restart"); rec.Code != http.StatusOK || !fc.restartCalled {
		t.Errorf("restart = %d called=%v", rec.Code, fc.restartCalled)
	}
}

func TestLifecycleActionError(t *testing.T) {
	h := Handler(&fakeController{promoteErr: errors.New("cannot promote")})
	rec := do(t, h, http.MethodPost, "/promote")
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("promote err = %d, want 500", rec.Code)
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
