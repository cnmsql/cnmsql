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

	logf "sigs.k8s.io/controller-runtime/pkg/log"

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
	// HealthAddr is the plain HTTP listen address for Kubernetes probes.
	HealthAddr string
	// Backup, when set, enables the streaming physical-backup endpoint so this
	// instance can clone replicas.
	Backup *BackupConfig
	// Archiving, when set and Enabled, runs the continuous binlog archiver in
	// this Pod (active only while the instance is the writable primary).
	Archiving *ArchivingConfig
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
	if o.HealthAddr == "" {
		o.HealthAddr = ":8081"
	}
}

// Run is the PID1 entrypoint: it starts mysqld, waits for it to become
// reachable, serves the control API, and shuts everything down cleanly on
// SIGTERM/SIGINT or when mysqld exits.
func Run(ctx context.Context, opts RunOptions) error {
	opts.applyDefaults()
	log := logf.FromContext(ctx).WithName("instance-runner").WithValues(
		"instance", opts.InstanceName,
		"version", opts.Version,
	)
	log.Info("Starting instance manager",
		"dataDir", opts.DataDir,
		"socket", opts.Socket,
		"controlAddr", opts.WebserverAddr,
		"healthAddr", opts.HealthAddr,
		"cluster", opts.ClusterName,
		"namespace", opts.Namespace)

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

	mysqldOut, mysqldErr := newProcessLogWriters(log.WithName("mysqld"))
	sup := NewProcessSupervisor(opts.MysqldPath, args,
		WithShutdownTimeout(opts.ShutdownTimeout),
		WithOutput(mysqldOut, mysqldErr))
	log.Info("Starting mysqld", "binary", opts.MysqldPath)
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
	log.Info("Connected to mysqld control interface")
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
		log.Info("Resuming configured replication if needed")
		if err := controller.EnsureReplicaStarted(ctx); err != nil {
			_ = sup.Shutdown(ctx)
			return err
		}
	} else if opts.Role == webserver.RoleReplica {
		if opts.Source == nil {
			_ = sup.Shutdown(ctx)
			return errors.New("replica source is required when role is replica")
		}
		log.Info("Configuring static replica source", "sourceHost", opts.Source.Host, "sourcePort", opts.Source.Port)
		if err := controller.EnsureReplicaConfigured(ctx, *opts.Source); err != nil {
			_ = sup.Shutdown(ctx)
			return err
		}
	} else {
		log.Info("Resuming configured replication if needed")
		if err := controller.EnsureReplicaStarted(ctx); err != nil {
			_ = sup.Shutdown(ctx)
			return err
		}
	}
	if opts.Backup != nil {
		controller.SetBackupConfig(*opts.Backup)
	}

	// Continuous binlog archiver: runs in every Pod but only ships from the
	// writable primary. Its terminal error is fatal to the run loop like the
	// other long-lived servers.
	var archiveErr <-chan error = make(chan error) // never fires unless enabled
	archiveCtx, cancelArchive := context.WithCancel(logf.IntoContext(ctx, log))
	defer cancelArchive()
	if opts.Archiving != nil && opts.Archiving.Enabled {
		log.Info("Enabling continuous binlog archiving")
		loop, errCh, err := startArchiver(archiveCtx, *opts.Archiving, db)
		if err != nil {
			_ = sup.Shutdown(ctx)
			return err
		}
		controller.SetArchivingProvider(archivingStatusProvider(loop))
		archiveErr = errCh
	}

	srv, err := buildServer(opts, controller)
	if err != nil {
		_ = sup.Shutdown(ctx)
		return err
	}
	healthSrv := buildHealthServer(opts, controller)

	serverErr := make(chan error, 1)
	log.Info("Starting control API server", "addr", opts.WebserverAddr, "tls", opts.TLS.ServerCertFile != "")
	go func() { serverErr <- serve(srv, opts.TLS) }()
	healthErr := make(chan error, 1)
	log.Info("Starting health API server", "addr", opts.HealthAddr)
	go func() { healthErr <- servePlain(healthSrv) }()

	// mysqld exit signals the supervisor's wait channel.
	mysqldExit := make(chan error, 1)
	go func() { mysqldExit <- sup.Wait() }()

	// In-Pod role reconciler (CNPG pull-model). Runs until its context is
	// cancelled during shutdown.
	roleErr := make(chan error, 1)
	mgrCtx, cancelMgr := context.WithCancel(ctx)
	defer cancelMgr()
	if roleManaged {
		log.Info("Starting role reconciler")
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
		log.Info("Received signal", "signal", sig.String())
		runErr = fmt.Errorf("received signal %s", sig)
	case err := <-mysqldExit:
		log.Error(err, "Mysqld exited")
		runErr = fmt.Errorf("mysqld exited: %w", err)
	case err := <-serverErr:
		log.Error(err, "Control API server failed")
		runErr = fmt.Errorf("control API server failed: %w", err)
	case err := <-healthErr:
		log.Error(err, "Health API server failed")
		runErr = fmt.Errorf("health API server failed: %w", err)
	case err := <-roleErr:
		log.Error(err, "Role reconciler failed")
		runErr = fmt.Errorf("role reconciler failed: %w", err)
	case err := <-archiveErr:
		log.Error(err, "Continuous archiver failed")
		runErr = fmt.Errorf("continuous archiver failed: %w", err)
	}
	cancelMgr()
	cancelArchive()

	log.Info("Shutting down instance manager")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), opts.ShutdownTimeout)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	_ = healthSrv.Shutdown(shutdownCtx)
	_ = sup.Shutdown(shutdownCtx)

	return runErr
}

func buildHealthServer(opts RunOptions, controller webserver.InstanceController) *http.Server {
	return &http.Server{
		Addr:              opts.HealthAddr,
		Handler:           webserver.HealthHandler(controller),
		ReadHeaderTimeout: 10 * time.Second,
	}
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

func servePlain(srv *http.Server) error {
	err := srv.ListenAndServe()
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
