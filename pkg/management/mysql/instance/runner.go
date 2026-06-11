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

package instance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yyewolf/cnmysql/pkg/management/mysql/instance/rolereconciler"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/pool"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/replication"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/version"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/webserver"
)

// RunOptions configures the PID1 run loop.
type RunOptions struct {
	// MysqldPath is the mysqld binary (default "mysqld").
	MysqldPath string
	// ConfigFile is the defaults file passed to mysqld.
	ConfigFile string
	// DataDir and Socket locate the server.
	DataDir string
	Socket  string
	// Version is the MySQL server version (e.g. "8.0.36").
	Version string
	// InstanceName is reported in status.
	InstanceName string
	// Role is the expected role for readiness/status.
	Role webserver.Role
	// Source configures replication when Role is replica. It lets the main
	// process repair missing source metadata after a physical clone.
	Source *replication.SourceOptions
	// ClusterName and Namespace enable the in-Pod role reconciler: when both are
	// set, the run loop watches the owning Cluster and drives the local mysqld to
	// match status.targetPrimary / currentPrimary (CNPG pull-model). The role is
	// then dynamic and SourceTemplate provides the replication connection
	// parameters (the source host is derived from currentPrimary).
	ClusterName    string
	Namespace      string
	SourceTemplate replication.SourceOptions
	// Control describes the privileged control connection used for monitoring.
	Control pool.ControlParams
	// WebserverAddr is the listen address for the control API.
	WebserverAddr string
	// Backup, when set, enables the streaming physical-backup endpoint so this
	// instance can clone replicas.
	Backup *BackupConfig
	// TLS configures the control API mTLS. When ServerCertFile is empty the
	// control API is served over plain HTTP (development only).
	TLS webserver.TLSOptions
	// ShutdownTimeout bounds the graceful mysqld shutdown.
	ShutdownTimeout time.Duration
	// ReadyTimeout bounds waiting for the control connection after start.
	ReadyTimeout time.Duration
}

func (o *RunOptions) applyDefaults() {
	if o.MysqldPath == "" {
		o.MysqldPath = defaultMysqldBinary
	}
	if o.ShutdownTimeout == 0 {
		o.ShutdownTimeout = DefaultShutdownTimeout
	}
	if o.ReadyTimeout == 0 {
		o.ReadyTimeout = 120 * time.Second
	}
}

// Run is the PID1 entrypoint: it starts mysqld, waits for it to become
// reachable, serves the control API, and shuts everything down cleanly on
// SIGTERM/SIGINT or when mysqld exits.
func Run(ctx context.Context, opts RunOptions) error {
	opts.applyDefaults()

	ver, err := version.Parse(opts.Version)
	if err != nil {
		return err
	}

	args := []string{}
	if opts.ConfigFile != "" {
		args = append(args, "--defaults-file="+opts.ConfigFile)
	}
	if opts.DataDir != "" {
		args = append(args, "--datadir="+opts.DataDir)
	}
	if opts.Socket != "" {
		args = append(args, "--socket="+opts.Socket)
	}
	// When the in-Pod reconciler manages role, boot read-only so a (re)starting
	// instance cannot accept writes before its role is reconciled; the reconciler
	// clears it on the confirmed primary. This is not applied to the bootstrap
	// temporary servers (initdb/join), which must be writable.
	if opts.ClusterName != "" && opts.Namespace != "" {
		if ver.HasSuperReadOnly() {
			args = append(args, "--super-read-only=ON")
		} else {
			args = append(args, "--read-only=ON")
		}
	}

	sup := NewProcessSupervisor(opts.MysqldPath, args, WithShutdownTimeout(opts.ShutdownTimeout))
	if err := sup.Start(ctx); err != nil {
		return err
	}

	// Catch termination signals to shut down mysqld gracefully.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)

	// Establish the privileged control connection.
	controlCfg := pool.ControlConfig(ver, opts.Control)
	db, err := openControl(ctx, controlCfg, opts.ReadyTimeout)
	if err != nil {
		_ = sup.Shutdown(ctx)
		return err
	}
	defer func() { _ = db.Close() }()

	controller, err := NewController(opts.InstanceName, db, opts.Version, opts.Role, sup)
	if err != nil {
		_ = sup.Shutdown(ctx)
		return err
	}
	// When the owning Cluster is known, the in-Pod role reconciler drives role
	// transitions dynamically; the run loop only resumes persisted replication.
	// Otherwise fall back to the static --role/--source bootstrap.
	roleManaged := opts.ClusterName != "" && opts.Namespace != ""
	if roleManaged {
		if err := controller.EnsureReplicaStarted(ctx); err != nil {
			_ = sup.Shutdown(ctx)
			return err
		}
	} else if opts.Role == webserver.RoleReplica {
		if opts.Source == nil {
			_ = sup.Shutdown(ctx)
			return errors.New("replica source is required when role is replica")
		}
		if err := controller.EnsureReplicaConfigured(ctx, *opts.Source); err != nil {
			_ = sup.Shutdown(ctx)
			return err
		}
	} else {
		if err := controller.EnsureReplicaStarted(ctx); err != nil {
			_ = sup.Shutdown(ctx)
			return err
		}
	}
	if opts.Backup != nil {
		controller.SetBackupConfig(*opts.Backup)
	}

	srv, err := buildServer(opts, controller)
	if err != nil {
		_ = sup.Shutdown(ctx)
		return err
	}

	serverErr := make(chan error, 1)
	go func() { serverErr <- serve(srv, opts.TLS) }()

	// mysqld exit signals the supervisor's wait channel.
	mysqldExit := make(chan error, 1)
	go func() { mysqldExit <- sup.Wait() }()

	// In-Pod role reconciler (CNPG pull-model). Runs until its context is
	// cancelled during shutdown.
	roleErr := make(chan error, 1)
	mgrCtx, cancelMgr := context.WithCancel(ctx)
	defer cancelMgr()
	if roleManaged {
		go func() {
			roleErr <- rolereconciler.Start(mgrCtx, rolereconciler.StartOptions{
				Namespace:      opts.Namespace,
				ClusterName:    opts.ClusterName,
				InstanceName:   opts.InstanceName,
				SourceTemplate: opts.SourceTemplate,
				Local:          controller,
			})
		}()
	}

	var runErr error
	select {
	case sig := <-signals:
		runErr = fmt.Errorf("received signal %s", sig)
	case err := <-mysqldExit:
		runErr = fmt.Errorf("mysqld exited: %w", err)
	case err := <-serverErr:
		runErr = fmt.Errorf("control API server failed: %w", err)
	case err := <-roleErr:
		runErr = fmt.Errorf("role reconciler failed: %w", err)
	}
	cancelMgr()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), opts.ShutdownTimeout)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	_ = sup.Shutdown(shutdownCtx)

	return runErr
}

// buildServer constructs the control API server, with or without mTLS.
func buildServer(opts RunOptions, controller webserver.InstanceController) (*http.Server, error) {
	if opts.TLS.ServerCertFile != "" {
		return webserver.NewServer(opts.WebserverAddr, controller, opts.TLS)
	}
	return &http.Server{
		Addr:              opts.WebserverAddr,
		Handler:           webserver.Handler(controller),
		ReadHeaderTimeout: 10 * time.Second,
	}, nil
}

// serve starts the server, returning nil on a clean shutdown.
func serve(srv *http.Server, tls webserver.TLSOptions) error {
	var err error
	if tls.ServerCertFile != "" {
		err = srv.ListenAndServeTLS("", "")
	} else {
		err = srv.ListenAndServe()
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// openControl opens the control connection, retrying until ready.
func openControl(ctx context.Context, cfg pool.Config, timeout time.Duration) (*sql.DB, error) {
	deadline := time.Now().Add(timeout)
	for {
		db, err := pool.Open(ctx, cfg)
		if err == nil {
			return db, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("control connection not ready within %s: %w", timeout, err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}
