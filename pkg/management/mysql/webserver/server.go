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

// Package webserver exposes the instance control API the operator calls over
// mutually-authenticated TLS, plus a small unauthenticated health handler for
// Kubernetes probes.
package webserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/replication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/user"
)

// InstanceController is the behaviour the HTTP layer drives. It is implemented
// by the real instance manager (backed by the pool and the replication
// package) and faked in tests, keeping the HTTP handlers free of MySQL
// specifics.
type InstanceController interface {
	// Healthz reports liveness: the managed process is up.
	Healthz(ctx context.Context) error
	// Readyz reports readiness: the instance can serve its role.
	Readyz(ctx context.Context) error
	// Status returns the full instance status.
	Status(ctx context.Context) (*Status, error)
	// Promote transitions a replica to primary.
	Promote(ctx context.Context) error
	// Demote transitions a primary to replica (read-only).
	Demote(ctx context.Context) error
	// EnsureReplicaConfigured points the instance at a source and starts
	// replication.
	EnsureReplicaConfigured(ctx context.Context, opts replication.SourceOptions) error
	// SetSemiSyncWaitForReplicaCount adjusts the semi-sync source's required
	// acknowledgement count at runtime (semi-sync self-healing).
	SetSemiSyncWaitForReplicaCount(ctx context.Context, count int) error
	// Restart restarts the managed mysqld process.
	Restart(ctx context.Context) error
	// Reload re-applies dynamic configuration parameters to the running mysqld
	// via SET GLOBAL, without restarting the process.
	Reload(ctx context.Context, req ReloadRequest) (*ReloadResponse, error)
	// CreateUser creates a MySQL user and applies its grants.
	CreateUser(ctx context.Context, req user.CreateUserRequest) error
	// AlterUser mutates an existing MySQL user.
	AlterUser(ctx context.Context, req user.AlterUserRequest) error
	// DropUser removes a MySQL user.
	DropUser(ctx context.Context, req user.DropUserRequest) error
	// ListUsers reports the managed MySQL users and their attributes.
	ListUsers(ctx context.Context) (*user.ListUsersResponse, error)
	// CreateDatabase creates a MySQL schema.
	CreateDatabase(ctx context.Context, req user.CreateDatabaseRequest) error
	// DropDatabase drops a MySQL schema.
	DropDatabase(ctx context.Context, req user.DropDatabaseRequest) error
	// ListDatabases reports the user-managed MySQL schemas.
	ListDatabases(ctx context.Context) (*user.ListDatabasesResponse, error)
}

// BackupStreamer streams a consistent physical backup (xbstream archive) to the
// writer, used by a joining replica to clone this instance. It is optional: the
// GET /cluster/backup route is only served when the controller implements it.
type BackupStreamer interface {
	BackupStream(ctx context.Context, w io.Writer) error
}

// Handler builds the http.Handler serving the instance control API. Exposing
// the handler (rather than only a server) lets it be tested with httptest and
// wrapped by the caller for TLS.
func Handler(controller InstanceController) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthHandler(controller.Healthz))
	mux.HandleFunc("GET /livez", healthHandler(controller.Healthz))
	mux.HandleFunc("GET /readyz", healthHandler(controller.Readyz))
	mux.HandleFunc("GET /status", statusHandler(controller))
	mux.HandleFunc("POST /promote", actionHandler(controller.Promote))
	mux.HandleFunc("POST /demote", actionHandler(controller.Demote))
	mux.HandleFunc("POST /replica/source", configureReplicaHandler(controller))
	mux.HandleFunc("POST /semisync/wait", semiSyncWaitHandler(controller))
	mux.HandleFunc("POST /restart", actionHandler(controller.Restart))
	mux.HandleFunc("POST /reload", reloadHandler(controller))
	mux.HandleFunc("POST /user/create", bodyActionHandler(controller.CreateUser))
	mux.HandleFunc("POST /user/alter", bodyActionHandler(controller.AlterUser))
	mux.HandleFunc("POST /user/drop", bodyActionHandler(controller.DropUser))
	mux.HandleFunc("GET /user/list", resultHandler(controller.ListUsers))
	mux.HandleFunc("POST /database/create", bodyActionHandler(controller.CreateDatabase))
	mux.HandleFunc("POST /database/drop", bodyActionHandler(controller.DropDatabase))
	mux.HandleFunc("GET /database/list", resultHandler(controller.ListDatabases))
	if streamer, ok := controller.(BackupStreamer); ok {
		mux.HandleFunc("GET /cluster/backup", backupHandler(streamer))
	}
	return mux
}

// HealthHandler builds the unauthenticated health API used by Kubernetes
// probes. It deliberately exposes only liveness/readiness endpoints; lifecycle
// actions and status remain on the mTLS control API.
func HealthHandler(controller InstanceController) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", healthHandler(controller.Healthz))
	mux.HandleFunc("GET /readyz", healthHandler(controller.Readyz))
	return mux
}

// ConfigureReplicaRequest is the JSON body accepted by POST /replica/source.
type ConfigureReplicaRequest struct {
	Source replication.SourceOptions `json:"source"`
}

// SemiSyncWaitRequest is the JSON body accepted by POST /semisync/wait. Count is
// the number of replica acknowledgements the semi-sync source should wait for.
type SemiSyncWaitRequest struct {
	Count int `json:"count"`
}

// ReloadRequest is the JSON body accepted by POST /reload. Parameters are the
// user-supplied my.cnf [mysqld] settings the operator wants applied at runtime.
type ReloadRequest struct {
	Parameters map[string]string `json:"parameters"`
}

// ReloadResponse reports the outcome of a reload. Applied lists the parameters
// successfully set via SET GLOBAL; Skipped maps each parameter that could not be
// applied at runtime (e.g. a non-dynamic variable) to the reason.
type ReloadResponse struct {
	Applied []string          `json:"applied,omitempty"`
	Skipped map[string]string `json:"skipped,omitempty"`
}

func reloadHandler(controller InstanceController) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req ReloadRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp, err := controller.Reload(r.Context(), req)
		if err != nil {
			writeError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			writeError(w, err)
		}
	}
}

func semiSyncWaitHandler(controller InstanceController) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req SemiSyncWaitRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := controller.SetSemiSyncWaitForReplicaCount(r.Context(), req.Count); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func configureReplicaHandler(controller InstanceController) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req ConfigureReplicaRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := controller.EnsureReplicaConfigured(r.Context(), req.Source); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

// backupHandler streams an xbstream physical backup. Because the body is sent
// incrementally, an error mid-stream cannot change the already-sent 200 status;
// the truncated archive will simply fail to extract on the replica.
func backupHandler(streamer BackupStreamer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-xbstream")
		if err := streamer.BackupStream(r.Context(), w); err != nil {
			// If nothing was written yet, this still surfaces as a 500.
			writeError(w, err)
		}
	}
}

// healthHandler maps a probe func to 200 OK / 503 Service Unavailable.
func healthHandler(probe func(context.Context) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := probe(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
}

// statusHandler serves the instance status as JSON.
func statusHandler(controller InstanceController) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		status, err := controller.Status(r.Context())
		if err != nil {
			writeError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(status); err != nil {
			writeError(w, err)
		}
	}
}

// actionHandler maps a lifecycle command to 200 OK / 500 on error.
func actionHandler(action func(context.Context) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := action(r.Context()); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

// writeError reports a server-side failure as a 500 with a JSON body. Every
// handler failure here is an internal error; client errors are handled by the
// router (404/405).
func writeError(w http.ResponseWriter, err error) {
	if err == nil {
		err = errors.New("unknown error")
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}
