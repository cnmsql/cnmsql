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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
)

// snapshotController adds the optional SnapshotQuiescer capability to
// fakeController.
type snapshotController struct {
	fakeController
	anchor   *SnapshotAnchor
	beginErr error
	endErr   error
	state    SnapshotState
	begun    *SnapshotBeginRequest
	ended    *SnapshotEndRequest
}

func (c *snapshotController) BeginSnapshot(_ context.Context, req SnapshotBeginRequest) (*SnapshotAnchor, error) {
	c.begun = &req
	return c.anchor, c.beginErr
}

func (c *snapshotController) EndSnapshot(_ context.Context, req SnapshotEndRequest) error {
	c.ended = &req
	return c.endErr
}

func (c *snapshotController) SnapshotState() SnapshotState { return c.state }

func TestSnapshotRoutesOnlyWhenQuiescerImplemented(t *testing.T) {
	t.Parallel()
	if rec := do(t, Handler(&fakeController{}), http.MethodGet, "/cluster/snapshot"); rec.Code != http.StatusNotFound {
		t.Fatalf("GET /cluster/snapshot without a quiescer = %d, want 404", rec.Code)
	}
	if rec := do(t, Handler(&snapshotController{}), http.MethodGet, "/cluster/snapshot"); rec.Code != http.StatusOK {
		t.Fatalf("GET /cluster/snapshot with a quiescer = %d, want 200", rec.Code)
	}
}

func TestSnapshotBeginReturnsAnchor(t *testing.T) {
	t.Parallel()
	c := &snapshotController{anchor: &SnapshotAnchor{
		AnchorGTID: "uuid:1-10", BinlogFile: "binlog.000003", BinlogPos: 157, Flavor: "mysql",
	}}
	rec := doWithBody(t, Handler(c), "/cluster/snapshot/begin", `{"backupName":"nightly","timeout":"30s"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("begin = %d, want 200: %s", rec.Code, rec.Body)
	}
	if c.begun == nil || c.begun.BackupName != "nightly" || c.begun.Timeout != "30s" {
		t.Fatalf("begin request = %+v", c.begun)
	}
	var got SnapshotAnchor
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got != *c.anchor {
		t.Fatalf("anchor = %+v, want %+v", got, *c.anchor)
	}
}

func TestSnapshotErrorsMapToStatus(t *testing.T) {
	t.Parallel()
	cases := map[error]int{
		ErrSnapshotHoldConflict:                                  http.StatusConflict,
		ErrSnapshotNotHeld:                                       http.StatusNotFound,
		fmt.Errorf("wrapped: %w", ErrSnapshotHoldExpired):        http.StatusGone,
		fmt.Errorf("bad timeout: %w", ErrInvalidSnapshotRequest): http.StatusBadRequest,
		fmt.Errorf("mysqld is gone"):                             http.StatusInternalServerError,
	}
	for err, want := range cases {
		c := &snapshotController{beginErr: err, endErr: err}
		if rec := doWithBody(t, Handler(c), "/cluster/snapshot/begin", `{"backupName":"b"}`); rec.Code != want {
			t.Errorf("begin with %v = %d, want %d", err, rec.Code, want)
		}
		if rec := doWithBody(t, Handler(c), "/cluster/snapshot/end", `{"backupName":"b"}`); rec.Code != want {
			t.Errorf("end with %v = %d, want %d", err, rec.Code, want)
		}
	}
}

func TestSnapshotStateReportsFence(t *testing.T) {
	t.Parallel()
	c := &snapshotController{state: SnapshotState{Fenced: true, MysqldStopped: true}}
	rec := do(t, Handler(c), http.MethodGet, "/cluster/snapshot")
	var got SnapshotState
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if !got.Fenced || !got.MysqldStopped || got.Hold != nil {
		t.Fatalf("state = %+v", got)
	}
}

func TestSnapshotRoutesAreOperatorOnly(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"GET /cluster/snapshot", "POST /cluster/snapshot/begin", "POST /cluster/snapshot/end"} {
		if _, ok := peerRoutes[route]; ok {
			t.Errorf("%s must not be a peer route", route)
		}
	}
}
