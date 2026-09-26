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
	"net/url"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fakeLoadSession struct {
	got    string
	result LoadResult
	err    error
	closed bool
}

func (s *fakeLoadSession) Load(_ context.Context, r io.Reader) (LoadResult, error) {
	b, err := io.ReadAll(r)
	s.got = string(b)
	if err != nil {
		return LoadResult{}, err
	}
	return s.result, s.err
}

func (s *fakeLoadSession) Close() { s.closed = true }

// loadController adds the optional LoadStreamer capability to fakeController.
type loadController struct {
	fakeController
	session  *fakeLoadSession
	startErr error
	req      *LoadRequest
}

func (c *loadController) StartLoad(_ context.Context, req LoadRequest) (LoadSession, error) {
	c.req = &req
	if c.startErr != nil {
		return nil, c.startErr
	}
	return c.session, nil
}

// untouchedBody fails the test if the handler reads the request body.
type untouchedBody struct{ read atomic.Bool }

func (b *untouchedBody) Read([]byte) (int, error) {
	b.read.Store(true)
	return 0, io.EOF
}

func loadURL(databases []string, policy string) string {
	q := url.Values{LoadDatabaseParam: databases, LoadPolicyParam: {policy}}
	return "/cluster/load?" + q.Encode()
}

func TestLoadHandlerLoadsAndReportsTheResult(t *testing.T) {
	session := &fakeLoadSession{result: LoadResult{Databases: []string{"a,b"}, Bytes: 7}}
	c := &loadController{session: session}
	rec := httptest.NewRecorder()
	Handler(c).ServeHTTP(rec, httptest.NewRequest(http.MethodPost,
		loadURL([]string{"a,b", "shop"}, LoadPolicyDropAndRecreate), strings.NewReader("SELECT 1;")))
	resp := rec.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var result LoadResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(result.Databases, []string{"a,b"}) || result.Bytes != 7 {
		t.Errorf("result = %+v", result)
	}
	if !slices.Equal(c.req.Databases, []string{"a,b", "shop"}) || c.req.Policy != LoadPolicyDropAndRecreate {
		t.Errorf("request = %+v", c.req)
	}
	if session.got != "SELECT 1;" || !session.closed {
		t.Errorf("session got %q, closed %v", session.got, session.closed)
	}
}

func TestLoadHandlerRefusesBeforeReadingTheBody(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
		reason string
	}{
		{ErrLoadToolUnavailable, http.StatusNotImplemented, LoadReasonToolUnavailable},
		{ErrLoadInProgress, http.StatusConflict, LoadReasonInProgress},
		{ErrNotPrimary, http.StatusConflict, LoadReasonNotPrimary},
		{ErrInvalidLoadRequest, http.StatusUnprocessableEntity, LoadReasonInvalidRequest},
		{ErrDatabaseNotEmpty, http.StatusConflict, LoadReasonDatabaseNotEmpty},
		{errors.New("dropping database: boom"), http.StatusInternalServerError, LoadReasonFailed},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			body := &untouchedBody{}
			c := &loadController{startErr: fmt.Errorf("wrapped: %w", tc.err)}
			rec := httptest.NewRecorder()
			Handler(c).ServeHTTP(rec, httptest.NewRequest(http.MethodPost,
				loadURL([]string{"shop"}, LoadPolicyFailIfExists), body))
			resp := rec.Result()
			if resp.StatusCode != tc.status {
				t.Errorf("status = %d, want %d", resp.StatusCode, tc.status)
			}
			var refusal DumpErrorBody
			if err := json.NewDecoder(resp.Body).Decode(&refusal); err != nil {
				t.Fatal(err)
			}
			if refusal.Reason != tc.reason || !strings.Contains(refusal.Error, "wrapped") {
				t.Errorf("refusal = %+v", refusal)
			}
			if body.read.Load() {
				t.Error("the body was read before the load was accepted")
			}
		})
	}
}

func TestLoadHandlerReportsAFailedLoad(t *testing.T) {
	session := &fakeLoadSession{err: errors.New("load: mysql failed: ERROR 1146")}
	rec := httptest.NewRecorder()
	Handler(&loadController{session: session}).ServeHTTP(rec, httptest.NewRequest(http.MethodPost,
		loadURL([]string{"shop"}, LoadPolicyFailIfExists), strings.NewReader("x")))
	resp := rec.Result()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var refusal DumpErrorBody
	if err := json.NewDecoder(resp.Body).Decode(&refusal); err != nil {
		t.Fatal(err)
	}
	if refusal.Reason != LoadReasonFailed || !strings.Contains(refusal.Error, "ERROR 1146") {
		t.Errorf("refusal = %+v", refusal)
	}
	if !session.closed {
		t.Error("session not closed")
	}
}

// With Expect: 100-continue a refused load never sends the dump: the client
// waits for "100 Continue", which the server only sends once the handler
// reads the body.
func TestLoadHandlerRefusalSendsNoBodyWithExpectContinue(t *testing.T) {
	srv := httptest.NewServer(Handler(&loadController{startErr: ErrNotPrimary}))
	defer srv.Close()
	// A bounded body, so a regression that reads it fails the assertion below
	// instead of streaming until the test times out.
	var sent atomic.Int64
	remaining := int64(1 << 20)
	body := io.NopCloser(readerFunc(func(p []byte) (int, error) {
		if remaining <= 0 {
			return 0, io.EOF
		}
		n := min(int64(len(p)), remaining)
		remaining -= n
		sent.Add(n)
		return int(n), nil
	}))
	req, err := http.NewRequest(http.MethodPost, srv.URL+loadURL([]string{"shop"}, LoadPolicyFailIfExists), body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Expect", "100-continue")
	client := &http.Client{
		Transport: &http.Transport{ExpectContinueTimeout: 10 * time.Second},
		Timeout:   30 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if n := sent.Load(); n != 0 {
		t.Errorf("client sent %d body bytes for a refused load", n)
	}
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }
