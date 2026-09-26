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
	"io"
	"net/http"
)

// Load policies: what a load does with a selected database that already holds
// objects. They match LogicalRestore.spec.policy.
const (
	LoadPolicyFailIfExists    = "FailIfExists"
	LoadPolicyDropAndRecreate = "DropAndRecreate"
)

// Query parameters of POST /cluster/load. Nothing in a load request is
// secret: the load runs as the instance manager's own control account.
const (
	// LoadDatabaseParam is repeated once per selected database.
	LoadDatabaseParam = "database"
	// LoadPolicyParam is one of the LoadPolicy* values.
	LoadPolicyParam = "policy"
)

// LoadRequest selects what a load restores and how.
type LoadRequest struct {
	// Databases are the schemas to load. The stream's other sections are
	// dropped.
	Databases []string
	// Policy is LoadPolicyFailIfExists or LoadPolicyDropAndRecreate.
	Policy string
}

// LoadResult is the JSON body of a successful load.
type LoadResult struct {
	// Databases are the schemas the stream held and the client loaded.
	Databases []string `json:"databases"`
	// Bytes is the size of the SQL stream received, before filtering.
	Bytes int64 `json:"bytes"`
}

// LoadSession is a load that passed its checks, applied its policy, and holds
// the instance's load slot with the SQL client running. Close releases the
// slot, stops a client that is still running, and removes the session's
// credentials file; it is safe to call after Load.
type LoadSession interface {
	// Load feeds the stream to the SQL client and waits for it. An error
	// reading r kills the client instead of ending its input, so a statement
	// cut in the middle never runs.
	Load(ctx context.Context, r io.Reader) (LoadResult, error)
	Close()
}

// LoadStreamer loads logical dumps into a primary. The POST /cluster/load
// route is only served when the controller implements it.
type LoadStreamer interface {
	// StartLoad runs every check that can refuse a load (tool present, no
	// load already running, writable primary, valid request, policy) and
	// applies the policy, before the request body is read. The body is only
	// read, and "100 Continue" only sent, once it returns a session.
	StartLoad(ctx context.Context, req LoadRequest) (LoadSession, error)
}

// Errors StartLoad returns to refuse a load. Nothing was changed on the
// instance when one of them is returned.
var (
	// ErrLoadToolUnavailable: the image does not ship the SQL client.
	ErrLoadToolUnavailable = errors.New("load tool unavailable")
	// ErrLoadInProgress: another load is running on this instance.
	ErrLoadInProgress = errors.New("load already in progress")
	// ErrNotPrimary: the instance is read-only (a replica, a Group
	// Replication secondary, or a demoted primary).
	ErrNotPrimary = errors.New("instance is not a writable primary")
	// ErrInvalidLoadRequest: no database, an unknown policy, or a system or
	// operator schema.
	ErrInvalidLoadRequest = errors.New("invalid load request")
	// ErrDatabaseNotEmpty: FailIfExists found a selected database holding a
	// table, view, routine or event.
	ErrDatabaseNotEmpty = errors.New("database not empty")
)

// Reasons returned in the JSON error body of a failed load. They double as the
// LogicalRestore failure reasons the worker reports.
const (
	LoadReasonToolUnavailable  = "LoadToolUnavailable"
	LoadReasonInProgress       = "LoadInProgress"
	LoadReasonNotPrimary       = "NotPrimary"
	LoadReasonInvalidRequest   = "InvalidLoadRequest"
	LoadReasonDatabaseNotEmpty = "DatabaseNotEmpty"
	// LoadReasonFailed: the load started and failed; the selected databases may
	// be partly loaded.
	LoadReasonFailed = "LoadFailed"
)

// loadRefusal maps a StartLoad error to its status and reason. An error that
// is none of the refusals is a failure after the policy may have run.
func loadRefusal(err error) (int, string) {
	switch {
	case errors.Is(err, ErrLoadToolUnavailable):
		return http.StatusNotImplemented, LoadReasonToolUnavailable
	case errors.Is(err, ErrLoadInProgress):
		return http.StatusConflict, LoadReasonInProgress
	case errors.Is(err, ErrNotPrimary):
		return http.StatusConflict, LoadReasonNotPrimary
	case errors.Is(err, ErrInvalidLoadRequest):
		return http.StatusUnprocessableEntity, LoadReasonInvalidRequest
	case errors.Is(err, ErrDatabaseNotEmpty):
		return http.StatusConflict, LoadReasonDatabaseNotEmpty
	default:
		return http.StatusInternalServerError, LoadReasonFailed
	}
}

// loadHandler serves POST /cluster/load. The request's query selects the
// databases and the policy, and its body is the SQL stream. Unlike the dump,
// the response comes after the whole stream went through, so every outcome is
// a real status with a JSON body.
func loadHandler(streamer LoadStreamer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		req := LoadRequest{
			Databases: query[LoadDatabaseParam],
			Policy:    query.Get(LoadPolicyParam),
		}
		session, err := streamer.StartLoad(r.Context(), req)
		if err != nil {
			status, reason := loadRefusal(err)
			writeReasonError(w, status, reason, err)
			return
		}
		defer session.Close()

		result, err := session.Load(r.Context(), r.Body)
		if err != nil {
			writeReasonError(w, http.StatusInternalServerError, LoadReasonFailed, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	}
}

// writeReasonError writes a JSON error body carrying a machine-readable
// reason, the shape the backup and restore workers read.
func writeReasonError(w http.ResponseWriter, status int, reason string, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(ReasonErrorBody{Reason: reason, Error: err.Error()})
}
