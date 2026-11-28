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
	"net/http"
)

// SnapshotBeginRequest is the JSON body of POST /cluster/snapshot/begin.
type SnapshotBeginRequest struct {
	// BackupName names the Backup the hold is for. A second begin with the same
	// name returns the existing anchor, so an operator retry is harmless.
	BackupName string `json:"backupName"`
	// Timeout bounds the hold (a Go duration). When it elapses the instance
	// releases the lock on its own. Empty takes DefaultSnapshotHoldTimeout.
	Timeout string `json:"timeout,omitempty"`
}

// SnapshotEndRequest is the JSON body of POST /cluster/snapshot/end.
type SnapshotEndRequest struct {
	BackupName string `json:"backupName"`
}

// SnapshotAnchor is the position of a snapshot's consistent point, read under
// the global read lock.
type SnapshotAnchor struct {
	// AnchorGTID is gtid_executed (MySQL) or gtid_binlog_pos (MariaDB).
	AnchorGTID string `json:"anchorGTID"`
	// BinlogFile and BinlogPos are the binary-log coordinates of the cut.
	BinlogFile string `json:"binlogFile,omitempty"`
	BinlogPos  int64  `json:"binlogPos,omitempty"`
	// Flavor is the engine flavor (mysql, mariadb).
	Flavor string `json:"flavor,omitempty"`
	// ServerVersion is the server's version string.
	ServerVersion string `json:"serverVersion,omitempty"`
}

// SnapshotHoldStatus describes an active snapshot hold.
type SnapshotHoldStatus struct {
	BackupName string `json:"backupName"`
	// Since and ExpiresAt are RFC 3339 timestamps.
	Since     string `json:"since"`
	ExpiresAt string `json:"expiresAt"`
}

// SnapshotState is the response of GET /cluster/snapshot. It never queries
// mysqld, so it answers while the instance is fenced.
type SnapshotState struct {
	// Hold is the active hold, nil when none.
	Hold *SnapshotHoldStatus `json:"hold,omitempty"`
	// Fenced reports the instance is fenced, MysqldStopped that mysqld has
	// exited. A cold snapshot is cut only once both are true.
	Fenced        bool `json:"fenced"`
	MysqldStopped bool `json:"mysqldStopped"`
}

// SnapshotQuiescer holds an instance consistent while a volume snapshot is cut.
// The /cluster/snapshot routes are only served when the controller implements
// it.
type SnapshotQuiescer interface {
	BeginSnapshot(ctx context.Context, req SnapshotBeginRequest) (*SnapshotAnchor, error)
	EndSnapshot(ctx context.Context, req SnapshotEndRequest) error
	SnapshotState() SnapshotState
}

// Errors the snapshot routes map to their own HTTP status, so the operator can
// tell a refusal from a failure.
var (
	// ErrSnapshotHoldConflict (409): a hold for another Backup is active.
	ErrSnapshotHoldConflict = errors.New("a snapshot hold for another backup is active")
	// ErrSnapshotNotHeld (404): nothing is held for this Backup.
	ErrSnapshotNotHeld = errors.New("no snapshot hold is active for this backup")
	// ErrSnapshotHoldExpired (410): the hold for this Backup timed out and was
	// released before end, so the snapshot may postdate the anchor.
	ErrSnapshotHoldExpired = errors.New("the snapshot hold expired before it was ended")
	// ErrInvalidSnapshotRequest (400): a malformed request.
	ErrInvalidSnapshotRequest = errors.New("invalid snapshot request")
)

func snapshotBeginHandler(q SnapshotQuiescer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req SnapshotBeginRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		anchor, err := q.BeginSnapshot(r.Context(), req)
		if err != nil {
			writeSnapshotError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(anchor)
	}
}

func snapshotEndHandler(q SnapshotQuiescer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req SnapshotEndRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := q.EndSnapshot(r.Context(), req); err != nil {
			writeSnapshotError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func snapshotStateHandler(q SnapshotQuiescer) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(q.SnapshotState())
	}
}

// writeSnapshotError maps the snapshot errors to their status, anything else
// to a 500.
func writeSnapshotError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, ErrSnapshotHoldConflict):
		status = http.StatusConflict
	case errors.Is(err, ErrSnapshotNotHeld):
		status = http.StatusNotFound
	case errors.Is(err, ErrSnapshotHoldExpired):
		status = http.StatusGone
	case errors.Is(err, ErrInvalidSnapshotRequest):
		status = http.StatusBadRequest
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}
