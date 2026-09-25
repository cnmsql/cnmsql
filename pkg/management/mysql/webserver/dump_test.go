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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

type fakeDumpSession struct {
	info   DumpInfo
	body   string
	result DumpResult
	err    error
	closed bool
}

func (s *fakeDumpSession) Info() DumpInfo { return s.info }

func (s *fakeDumpSession) Stream(_ context.Context, w io.Writer) (DumpResult, error) {
	_, _ = io.WriteString(w, s.body)
	return s.result, s.err
}

func (s *fakeDumpSession) Close() { s.closed = true }

// dumpController adds the optional DumpStreamer capability to fakeController.
type dumpController struct {
	fakeController
	session  *fakeDumpSession
	startErr error
	req      *DumpRequest
}

func (c *dumpController) StartDump(_ context.Context, req DumpRequest) (DumpSession, error) {
	c.req = &req
	if c.startErr != nil {
		return nil, c.startErr
	}
	return c.session, nil
}

func postDump(t *testing.T, h http.Handler, body string) *http.Response {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/cluster/dump", strings.NewReader(body)))
	return rec.Result()
}

func TestDumpHandlerStreamsWithHeadersAndTrailers(t *testing.T) {
	session := &fakeDumpSession{
		info: DumpInfo{Tool: "mysqldump", Flavor: "mysql", ServerVersion: "8.4.11-11", Databases: []string{"shop", "a,b"}},
		body: "-- MySQL dump\n-- Dump completed on 2026-09-25\n",
		result: DumpResult{
			SnapshotBinlog: "mysql-bin.000001:3200",
			SnapshotGTID:   "0-1-13",
		},
	}
	ctrl := &dumpController{session: session}
	resp := postDump(t, Handler(ctrl), `{"password":"pw","databases":["shop"],"extraArgs":["--x"]}`)
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if string(body) != session.body {
		t.Fatalf("body = %q", body)
	}
	if ctrl.req == nil || ctrl.req.Password != "pw" || !slices.Equal(ctrl.req.Databases, []string{"shop"}) ||
		!slices.Equal(ctrl.req.ExtraArgs, []string{"--x"}) {
		t.Fatalf("request = %+v", ctrl.req)
	}
	if resp.Header.Get(DumpToolHeader) != "mysqldump" || resp.Header.Get(DumpFlavorHeader) != "mysql" ||
		resp.Header.Get(DumpServerVersionHeader) != "8.4.11-11" {
		t.Fatalf("headers = %v", resp.Header)
	}
	dbs, err := DecodeDumpDatabases(resp.Header.Get(DumpDatabasesHeader))
	if err != nil || !slices.Equal(dbs, []string{"shop", "a,b"}) {
		t.Fatalf("databases = %v, %v", dbs, err)
	}
	if resp.Trailer.Get(DumpSnapshotBinlogTrailer) != "mysql-bin.000001:3200" ||
		resp.Trailer.Get(DumpSnapshotGTIDTrailer) != "0-1-13" || resp.Trailer.Get(DumpErrorTrailer) != "" {
		t.Fatalf("trailers = %v", resp.Trailer)
	}
	if !session.closed {
		t.Fatal("session was not closed")
	}
}

func TestDumpHandlerReportsMidStreamFailureInTrailer(t *testing.T) {
	session := &fakeDumpSession{body: "-- partial", err: errors.New("mysqldump exited with status 2")}
	resp := postDump(t, Handler(&dumpController{session: session}), `{"password":"pw"}`)
	_, _ = io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, the 200 is committed before the stream", resp.StatusCode)
	}
	if got := resp.Trailer.Get(DumpErrorTrailer); !strings.Contains(got, "status 2") {
		t.Fatalf("error trailer = %q", got)
	}
	if !session.closed {
		t.Fatal("session was not closed")
	}
}

func TestDumpHandlerRefusals(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
		reason string
	}{
		{fmt.Errorf("%w: mysqldump not found in PATH", ErrDumpToolUnavailable),
			http.StatusNotImplemented, DumpReasonToolUnavailable},
		{ErrDumpAccountMissing, http.StatusServiceUnavailable, DumpReasonAccountMissing},
		{ErrDumpInProgress, http.StatusConflict, DumpReasonInProgress},
		{fmt.Errorf("%w: database \"x\" does not exist", ErrInvalidDumpRequest),
			http.StatusUnprocessableEntity, DumpReasonInvalidRequest},
		{errors.New("boom"), http.StatusInternalServerError, ""},
	} {
		t.Run(tc.err.Error(), func(t *testing.T) {
			resp := postDump(t, Handler(&dumpController{startErr: tc.err}), `{"password":"pw"}`)
			if resp.StatusCode != tc.status {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.status)
			}
			var body DumpErrorBody
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.Reason != tc.reason || body.Error != tc.err.Error() {
				t.Fatalf("body = %+v", body)
			}
		})
	}
}

func TestDumpHandlerRejectsMalformedBody(t *testing.T) {
	resp := postDump(t, Handler(&dumpController{session: &fakeDumpSession{}}), `{`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestDumpHandlerRejectsOversizedBody(t *testing.T) {
	ctrl := &dumpController{session: &fakeDumpSession{}}
	body := `{"password":"` + strings.Repeat("x", maxDumpRequestBodyBytes) + `"}`
	resp := postDump(t, Handler(ctrl), body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if ctrl.req != nil {
		t.Fatal("an oversized body must not reach StartDump")
	}
}

func TestDumpRouteAbsentWithoutStreamer(t *testing.T) {
	// A manager that predates logical backups has no route: the worker reads the
	// 404 as InstanceManagerOutdated.
	resp := postDump(t, Handler(&fakeController{}), `{"password":"pw"}`)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestDumpRouteIsPostOnly(t *testing.T) {
	rec := httptest.NewRecorder()
	Handler(&dumpController{session: &fakeDumpSession{}}).
		ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/cluster/dump?password=pw", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d, want 405", rec.Code)
	}
}

func TestDumpDatabasesEncoding(t *testing.T) {
	in := []string{"shop", "a,b", "ünï cødé", "x/y%z"}
	got, err := DecodeDumpDatabases(EncodeDumpDatabases(in))
	if err != nil || !slices.Equal(got, in) {
		t.Fatalf("round trip = %v, %v", got, err)
	}
	if strings.ContainsAny(EncodeDumpDatabases(in), " ü") {
		t.Fatal("encoded header must be ASCII without spaces")
	}
	if got, _ := DecodeDumpDatabases(""); got != nil {
		t.Fatalf("empty = %v", got)
	}
}
