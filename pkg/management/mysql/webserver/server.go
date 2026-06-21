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
	// Healthz reports liveness: the manager is up and the instance is not a
	// partitioned primary. It does not depend on mysqld being up.
	Healthz(ctx context.Context) error
	// Startupz reports startup completion: mysqld is up and answering.
	Startupz(ctx context.Context) error
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
	// RestartInPlace re-execs the instance manager in place, adopting the running
	// mysqld so the manager binary is swapped without restarting the server (the
	// zero-restart operator-upgrade path).
	RestartInPlace(ctx context.Context) error
	// UpgradeInstanceManager streams a new instance-manager binary from r, verifies
	// it against expectedHash, writes it over the on-disk binary, then re-execs in
	// place adopting the running mysqld. It is the streamed variant of
	// RestartInPlace used by the operator to roll out a new manager version with no
	// mysqld restart.
	UpgradeInstanceManager(ctx context.Context, r io.Reader, expectedHash string) error
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
	// SetAsPrimary performs a planned Group Replication primary change to the
	// member with the given server_uuid via group_replication_set_as_primary.
	SetAsPrimary(ctx context.Context, memberUUID string) error
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
	mux.HandleFunc("GET /startupz", healthHandler(controller.Startupz))
	mux.HandleFunc("GET /readyz", healthHandler(controller.Readyz))
	mux.HandleFunc("GET /status", statusHandler(controller))
	mux.HandleFunc("POST /promote", actionHandler(controller.Promote))
	mux.HandleFunc("POST /demote", actionHandler(controller.Demote))
	mux.HandleFunc("POST /replica/source", configureReplicaHandler(controller))
	mux.HandleFunc("POST /semisync/wait", semiSyncWaitHandler(controller))
	mux.HandleFunc("POST /restart", actionHandler(controller.Restart))
	mux.HandleFunc("POST /instance/manager/restart-inplace", actionHandler(controller.RestartInPlace))
	mux.HandleFunc("POST /instance/manager/upgrade", upgradeManagerHandler(controller))
	mux.HandleFunc("POST /reload", reloadHandler(controller))
	mux.HandleFunc("POST /user/create", bodyActionHandler(controller.CreateUser))
	mux.HandleFunc("POST /user/alter", bodyActionHandler(controller.AlterUser))
	mux.HandleFunc("POST /user/drop", bodyActionHandler(controller.DropUser))
	mux.HandleFunc("GET /user/list", resultHandler(controller.ListUsers))
	mux.HandleFunc("POST /database/create", bodyActionHandler(controller.CreateDatabase))
	mux.HandleFunc("POST /database/drop", bodyActionHandler(controller.DropDatabase))
	mux.HandleFunc("GET /database/list", resultHandler(controller.ListDatabases))
	mux.HandleFunc("POST /group/set-as-primary", groupSetAsPrimaryHandler(controller))
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
	mux.HandleFunc("GET /startupz", healthHandler(controller.Startupz))
	mux.HandleFunc("GET /readyz", healthHandler(controller.Readyz))
	return mux
}

// ManagerHashHeader carries the SHA-256 the streamed instance-manager binary
// must hash to, so the receiving manager can reject a corrupted or mismatched
// upload before replacing its own binary.
const ManagerHashHeader = "X-CNMySQL-Manager-Hash"

// ErrInvalidInstanceManagerBinary is returned when a streamed instance-manager
// binary does not hash to the expected value. It maps to a 400 so the operator
// can tell a bad upload apart from a server-side failure.
var ErrInvalidInstanceManagerBinary = errors.New("invalid instance manager binary")

// upgradeManagerHandler streams the request body (a new instance-manager binary)
// to the controller, which validates it against the X-CNMySQL-Manager-Hash header
// then re-execs in place. A hash mismatch is a 400; anything else is a 500.
func upgradeManagerHandler(controller InstanceController) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		expectedHash := r.Header.Get(ManagerHashHeader)
		if err := controller.UpgradeInstanceManager(r.Context(), r.Body, expectedHash); err != nil {
			if errors.Is(err, ErrInvalidInstanceManagerBinary) {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
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

// GroupSetAsPrimaryRequest is the JSON body accepted by POST /group/set-as-primary.
type GroupSetAsPrimaryRequest struct {
	MemberUUID string `json:"memberUUID"`
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

func groupSetAsPrimaryHandler(controller InstanceController) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req GroupSetAsPrimaryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.MemberUUID == "" {
			http.Error(w, "memberUUID is required", http.StatusBadRequest)
			return
		}
		if err := controller.SetAsPrimary(r.Context(), req.MemberUUID); err != nil {
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
