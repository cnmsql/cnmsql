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
	"net/url"
	"strings"
)

// DumpRequest is the JSON body of POST /cluster/dump. It is a POST because the
// body carries the database list and the dump client's extra arguments, which
// must not go in a URL that proxies and access logs record. The dump account's
// password is not in the request: the instance manager reads the dump Secret
// itself (design 030).
type DumpRequest struct {
	// Databases limits the dump. Empty dumps every application schema.
	Databases []string `json:"databases,omitempty"`
	// ExtraArgs are appended to the dump client's arguments.
	ExtraArgs []string `json:"extraArgs,omitempty"`
}

// DumpInfo describes a dump before it streams. It travels as response headers.
type DumpInfo struct {
	// Tool is the dump client (mysqldump, mariadb-dump).
	Tool string
	// Flavor is the engine flavor (mysql, mariadb).
	Flavor string
	// ServerVersion is the server's version string.
	ServerVersion string
	// Databases are the resolved schemas being dumped.
	Databases []string
}

// DumpResult carries what a dump can only report once it finished. It travels
// as response trailers.
type DumpResult struct {
	// SnapshotGTID is the snapshot's GTID position, when the engine reports one.
	SnapshotGTID string
	// SnapshotBinlog is the snapshot's binlog coordinates as file:position.
	SnapshotBinlog string
}

// DumpSession is a dump that passed its checks and holds the instance's dump
// slot. Close releases the slot and removes the session's credentials file, and
// is safe to call after Stream.
type DumpSession interface {
	Info() DumpInfo
	Stream(ctx context.Context, w io.Writer) (DumpResult, error)
	Close()
}

// DumpStreamer runs logical dumps. The POST /cluster/dump route is only served
// when the controller implements it.
type DumpStreamer interface {
	// StartDump runs every check that can refuse a dump (tool present, no dump
	// already running, account present, databases valid) before any byte of the
	// response is sent, so a refusal is a real HTTP status.
	StartDump(ctx context.Context, req DumpRequest) (DumpSession, error)
}

// Errors StartDump returns to refuse a dump. Each maps to its own HTTP status
// and reason, so the backup worker can fail the Backup with a precise reason.
var (
	// ErrDumpToolUnavailable: the image does not ship the dump client (images
	// published before logical backup support strip it).
	ErrDumpToolUnavailable = errors.New("dump tool unavailable")
	// ErrDumpAccountMissing: cnmsql_dump@localhost does not exist here yet, for
	// example on a replica that has not applied it. Retryable.
	ErrDumpAccountMissing = errors.New("dump account missing")
	// ErrDumpInProgress: another dump is running on this instance. Retryable.
	ErrDumpInProgress = errors.New("dump already in progress")
	// ErrInvalidDumpRequest: the request names a database that does not exist
	// or cannot be dumped, or resolves to no database at all, or the manager
	// has not read the dump account's password from its Secret yet.
	ErrInvalidDumpRequest = errors.New("invalid dump request")
)

// Reasons returned in the JSON error body of a refused dump. They double as the
// Backup failure reasons the worker reports.
const (
	DumpReasonToolUnavailable = "LogicalToolUnavailable"
	DumpReasonAccountMissing  = "DumpAccountMissing"
	DumpReasonInProgress      = "DumpInProgress"
	DumpReasonInvalidRequest  = "InvalidDumpRequest"
)

// Response headers and trailers of POST /cluster/dump. They are exported so the
// backup worker reads them off the response.
const (
	DumpToolHeader          = "X-Cnmsql-Dump-Tool"
	DumpFlavorHeader        = "X-Cnmsql-Dump-Flavor"
	DumpServerVersionHeader = "X-Cnmsql-Dump-Server-Version"
	// DumpDatabasesHeader lists the dumped schemas, each path-escaped and
	// comma-separated (schema names may contain commas and non-ASCII
	// characters). See EncodeDumpDatabases.
	DumpDatabasesHeader = "X-Cnmsql-Dump-Databases"

	DumpSnapshotGTIDTrailer   = "X-Cnmsql-Dump-Snapshot-Gtid"
	DumpSnapshotBinlogTrailer = "X-Cnmsql-Dump-Snapshot-Binlog"
	DumpErrorTrailer          = "X-Cnmsql-Dump-Error"
)

// ReasonErrorBody is the JSON body of an error with a reason a worker can act
// on: a refused dump, or a refused or failed load.
type ReasonErrorBody struct {
	Reason string `json:"reason"`
	Error  string `json:"error"`
}

// EncodeDumpDatabases renders the schema list for DumpDatabasesHeader.
func EncodeDumpDatabases(databases []string) string {
	escaped := make([]string, len(databases))
	for i, db := range databases {
		escaped[i] = url.PathEscape(db)
	}
	return strings.Join(escaped, ",")
}

// DecodeDumpDatabases parses DumpDatabasesHeader.
func DecodeDumpDatabases(header string) ([]string, error) {
	if header == "" {
		return nil, nil
	}
	parts := strings.Split(header, ",")
	out := make([]string, len(parts))
	for i, p := range parts {
		db, err := url.PathUnescape(p)
		if err != nil {
			return nil, err
		}
		out[i] = db
	}
	return out, nil
}

// dumpRefusal maps a StartDump error to its status and reason.
func dumpRefusal(err error) (int, string) {
	switch {
	case errors.Is(err, ErrDumpToolUnavailable):
		return http.StatusNotImplemented, DumpReasonToolUnavailable
	case errors.Is(err, ErrDumpAccountMissing):
		return http.StatusServiceUnavailable, DumpReasonAccountMissing
	case errors.Is(err, ErrDumpInProgress):
		return http.StatusConflict, DumpReasonInProgress
	case errors.Is(err, ErrInvalidDumpRequest):
		return http.StatusUnprocessableEntity, DumpReasonInvalidRequest
	default:
		return http.StatusInternalServerError, ""
	}
}

// maxDumpRequestBodyBytes bounds the JSON body of POST /cluster/dump: a dump
// request carries two schema lists, not a data channel.
const maxDumpRequestBodyBytes = 1 << 20

// dumpHandler serves POST /cluster/dump. The checks in StartDump decide the
// status; after that the SQL stream is the body, and a failure mid-stream can
// only be reported in the error trailer, like the physical backup stream.
func dumpHandler(streamer DumpStreamer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req DumpRequest
		r.Body = http.MaxBytesReader(w, r.Body, maxDumpRequestBodyBytes)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		session, err := streamer.StartDump(r.Context(), req)
		if err != nil {
			status, reason := dumpRefusal(err)
			writeReasonError(w, status, reason, err)
			return
		}
		defer session.Close()

		info := session.Info()
		h := w.Header()
		h.Set("Content-Type", "application/sql")
		h.Set(DumpToolHeader, info.Tool)
		h.Set(DumpFlavorHeader, info.Flavor)
		h.Set(DumpServerVersionHeader, info.ServerVersion)
		h.Set(DumpDatabasesHeader, EncodeDumpDatabases(info.Databases))
		h.Set("Trailer", DumpSnapshotGTIDTrailer+", "+DumpSnapshotBinlogTrailer+", "+DumpErrorTrailer)
		w.WriteHeader(http.StatusOK)

		result, err := session.Stream(r.Context(), w)
		if err != nil {
			h.Set(DumpErrorTrailer, err.Error())
			return
		}
		if result.SnapshotGTID != "" {
			h.Set(DumpSnapshotGTIDTrailer, result.SnapshotGTID)
		}
		if result.SnapshotBinlog != "" {
			h.Set(DumpSnapshotBinlogTrailer, result.SnapshotBinlog)
		}
	}
}
