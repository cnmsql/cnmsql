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

package instance

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/go-logr/logr"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/instance/rolereconciler"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/metrics"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/pool"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/version"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver/metricserver"
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
	// GroupReplication enables the MySQL Group Replication code paths in this Pod:
	// the controller reports the GR status block and the in-Pod role reconciler
	// uses the group role strategy instead of async promote/follow. Off for async
	// clusters.
	GroupReplication bool
	// Control describes the privileged control connection used for monitoring.
	Control pool.ControlParams
	// WebserverAddr is the listen address for the control API.
	WebserverAddr string
	// HealthAddr is the plain HTTP listen address for Kubernetes probes.
	HealthAddr string
	// MetricsAddr is the plain HTTP listen address for Prometheus metrics.
	MetricsAddr string
	// Backup, when set, enables the streaming physical-backup endpoint so this
	// instance can clone replicas.
	Backup *BackupConfig
	// Archiving, when set and Enabled, runs the continuous binlog archiver in
	// this Pod (active only while the instance is the writable primary).
	Archiving *ArchivingConfig
	// SemiSyncEnabled installs and enables the semi-synchronous replication
	// plugins after mysqld starts. The initial wait/timeout values mirror the
	// rendered loose- my.cnf values, but are applied explicitly after plugin load.
	SemiSyncEnabled       bool
	SemiSyncWaitCount     int
	SemiSyncTimeoutMillis int
	// TLS configures the control API mTLS. When ServerCertFile is empty the
	// control API is served over plain HTTP (development only).
	TLS webserver.TLSOptions
	// MetricsTLS, when true, serves the Prometheus metrics endpoint over the
	// same mutual TLS as the control API (reusing TLS): scrapers must present a
	// client certificate signed by the cluster CA. When false metrics are served
	// over plain HTTP.
	MetricsTLS bool
	// PIDFile is where the supervisor records the running mysqld PID so a
	// re-exec'd manager image can find and adopt it. Defaults to mysqld.pid
	// alongside the socket.
	PIDFile string
	// ShutdownTimeout bounds the graceful mysqld shutdown.
	ShutdownTimeout time.Duration
	// StopDelay is the maximum time in seconds allowed for complete Pod stop.
	// Maps to Kubernetes TerminationGracePeriodSeconds.
	StopDelay time.Duration
	// SmartShutdownTimeout is the time reserved for a graceful (innodb_fast_shutdown=1)
	// shutdown attempt before the fallback to SIGKILL.
	SmartShutdownTimeout time.Duration
	// ReadyTimeout bounds waiting for the control connection after start.
	ReadyTimeout time.Duration
}

func (o *RunOptions) applyDefaults() {
	if o.MysqldPath == "" {
		o.MysqldPath = defaultMysqldBinary
	}
	if o.PIDFile == "" {
		if o.Socket != "" {
			o.PIDFile = filepath.Join(filepath.Dir(o.Socket), "mysqld.pid")
		} else {
			o.PIDFile = "/var/run/mysqld/mysqld.pid"
		}
	}
	if o.ShutdownTimeout == 0 {
		o.ShutdownTimeout = DefaultShutdownTimeout
	}
	if o.StopDelay == 0 {
		o.StopDelay = DefaultShutdownTimeout
	}
	// The smart (clean) shutdown budget must sit below the hard stop delay so
	// there is headroom for the forced SIGKILL fallback within the Pod's grace.
	if o.SmartShutdownTimeout == 0 || o.SmartShutdownTimeout >= o.StopDelay {
		o.SmartShutdownTimeout = o.StopDelay / 2
	}
	if o.ReadyTimeout == 0 {
		o.ReadyTimeout = 120 * time.Second
	}
	if o.HealthAddr == "" {
		o.HealthAddr = ":8081"
	}
	if o.MetricsAddr == "" {
		o.MetricsAddr = ":9187"
	}
}

func configureSemiSync(ctx context.Context, repl *replication.Manager, opts RunOptions) error {
	log := logf.FromContext(ctx).WithName("semi-sync")

	roState, err := repl.ReadOnly(ctx)
	if err != nil {
		return err
	}
	restoreReadOnly := func() error {
		if roState.ReadOnly {
			if err := repl.SetReadOnly(ctx, true); err != nil {
				return err
			}
		}
		if roState.SuperReadOnly {
			return repl.SetSuperReadOnly(ctx, true)
		}
		return nil
	}

	log.Info("Clearing read_only for semi-sync plugin installation")
	if roState.SuperReadOnly {
		if err := repl.SetSuperReadOnly(ctx, false); err != nil {
			return err
		}
	}
	if roState.ReadOnly {
		if err := repl.SetReadOnly(ctx, false); err != nil {
			return err
		}
	}

	err = func() error {
		log.Info("Installing semi-sync replication plugins")
		if err := repl.InstallSemiSyncSource(ctx); err != nil {
			return err
		}
		if err := repl.InstallSemiSyncReplica(ctx); err != nil {
			return err
		}
		log.Info("Enabling semi-sync replication")
		if err := repl.EnableSemiSync(ctx); err != nil {
			return err
		}
		if opts.SemiSyncWaitCount > 0 {
			log.Info("Setting semi-sync wait for replica count", "count", opts.SemiSyncWaitCount)
			if err := repl.SetSemiSyncWaitForReplicaCount(ctx, opts.SemiSyncWaitCount); err != nil {
				return err
			}
		}
		if opts.SemiSyncTimeoutMillis > 0 {
			log.Info("Setting semi-sync timeout", "timeoutMillis", opts.SemiSyncTimeoutMillis)
			if err := repl.SetSemiSyncTimeoutMillis(ctx, opts.SemiSyncTimeoutMillis); err != nil {
				return err
			}
		}
		return nil
	}()
	if restoreErr := restoreReadOnly(); restoreErr != nil {
		if err == nil {
			err = restoreErr
		} else {
			log.Error(restoreErr, "Failed to restore read_only state after semi-sync configuration")
		}
	}
	return err
}

// Run is the PID1 entrypoint: it starts mysqld, waits for it to become
// reachable, serves the control API, and shuts everything down cleanly on
// SIGTERM/SIGINT or when mysqld exits.
//
//nolint:gocyclo // PID1 coordinates sibling servers and lifecycle exit paths in one select.
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
		"metricsAddr", opts.MetricsAddr,
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

	// The long-lived mysqld is supervised out-of-band (by PID, with inherited
	// output descriptors and a pidfile) so the manager can later re-exec itself
	// in place without disturbing mysqld. A named FIFO (re-openable, CLOEXEC
	// cleared on the read end) pipes mysqld output through the structured
	// processLogWriter, preserving the log format even across a re-exec.
	//
	// When this image is itself the product of an in-place re-exec, it adopts the
	// already-running mysqld (and re-attaches the inherited FIFO read end) instead
	// of starting a fresh server, so the upgrade never restarts mysqld.
	adopting, adoptPID := adoptRequest()
	fifoPath := opts.PIDFile + ".fifo"

	var fifoLog *FifoLog
	if adopting {
		readFD, ferr := readPIDFileFIFOFD(opts.PIDFile)
		if ferr != nil {
			return ferr
		}
		fifoLog, err = FifoLogFromFD(fifoPath, readFD, log)
	} else {
		fifoLog, err = NewFifoLog(fifoPath, log)
	}
	if err != nil {
		return err
	}
	fifoLog.Start(ctx)
	defer fifoLog.Close()

	sup := NewDetachedSupervisor(opts.MysqldPath, args,
		WithDetachedShutdownTimeout(opts.ShutdownTimeout),
		WithFIFO(fifoLog),
		WithPIDFile(opts.PIDFile))
	if adopting {
		log.Info("Adopting running mysqld after in-place manager upgrade",
			"pid", adoptPID, "pidFile", opts.PIDFile, "fifo", fifoPath)
		if err := sup.AdoptProcess(adoptPID); err != nil {
			return err
		}
		MarkRecentlyReExecd()
	} else {
		// Defense in depth against an unsupported major-version transition that
		// bypassed the admission guard: refuse to start before mysqld touches the
		// (irreversibly upgraded) data dictionary. Skipped when adopting, where the
		// version is unchanged.
		if err := guardDataDirUpgrade(opts.DataDir, opts.Version); err != nil {
			log.Error(err, "Refusing to start mysqld: unsupported MySQL version transition")
			return err
		}
		log.Info("Starting mysqld", "binary", opts.MysqldPath, "pidFile", opts.PIDFile, "fifo", fifoPath)
		if err := sup.Start(ctx); err != nil {
			return err
		}
	}

	// Catch termination signals to shut down mysqld gracefully.
	// SIGHUP triggers certificate reload.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
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

	// Record the version now serving this data directory so the next start can
	// validate its transition (guardDataDirUpgrade). Non-fatal: a missing marker
	// only means the next start cannot apply the guard and defers to admission.
	if err := writeVersionMarker(opts.DataDir, opts.Version); err != nil {
		log.Error(err, "Could not record MySQL version marker")
	}

	controller, err := NewController(opts.InstanceName, db, opts.Version, opts.Role, sup)
	if err != nil {
		_ = sup.Shutdown(ctx)
		return err
	}
	if opts.GroupReplication {
		controller.EnableGroupReplication()
	}
	// Skip destructive (re)configuration when adopting: mysqld is already running,
	// configured, and serving its role. Semi-sync plugins are already installed and
	// enabled, and re-applying them would clear read_only on a serving instance.
	if opts.SemiSyncEnabled && !adopting {
		if err := configureSemiSync(ctx, controller.repl, opts); err != nil {
			_ = sup.Shutdown(ctx)
			return err
		}
	}
	// When the owning Cluster is known, the in-Pod role reconciler drives role
	// transitions dynamically; the run loop only resumes persisted replication.
	// Otherwise fall back to the static --role/--source bootstrap.
	roleManaged := opts.ClusterName != "" && opts.Namespace != ""

	// Under cluster management the instance can detect API-server isolation and
	// fail its liveness probe so the kubelet restarts a partitioned container.
	var isolationDetector *IsolationDetector
	if roleManaged {
		isolationDetector = NewIsolationDetector(DefaultIsolationTimeout)
		controller.SetIsolationDetector(isolationDetector)
	}

	// The fence gate lets the in-Pod role reconciler stop mysqld for a fenced
	// instance while the manager stays alive. It is shared with the mysqld
	// supervisor watcher below so an intentional fence-stop is not mistaken for a
	// crash.
	fence := NewFenceGate()
	controller.SetFenceGate(fence)
	switch {
	case adopting:
		// Adopting an already-serving mysqld after an in-place upgrade: replication
		// is already configured and running. Re-attaching would be disruptive; the
		// role reconciler resumes steady-state management below.
		log.Info("Skipping replication bootstrap; adopting already-configured mysqld")
	case roleManaged:
		log.Info("Resuming configured replication if needed")
		if err := controller.EnsureReplicaStarted(ctx); err != nil {
			_ = sup.Shutdown(ctx)
			return err
		}
	case opts.Role == webserver.RoleReplica:
		if opts.Source == nil {
			_ = sup.Shutdown(ctx)
			return errors.New("replica source is required when role is replica")
		}
		log.Info("Configuring static replica source", "sourceHost", opts.Source.Host, "sourcePort", opts.Source.Port)
		if err := controller.EnsureReplicaConfigured(ctx, *opts.Source); err != nil {
			_ = sup.Shutdown(ctx)
			return err
		}
	default:
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

	var cm *webserver.TLSCertManager
	srv, err := buildServer(opts, controller)
	if err != nil {
		_ = sup.Shutdown(ctx)
		return err
	}
	if opts.TLS.ServerCertFile != "" {
		cm, err = webserver.NewTLSCertManager(opts.TLS)
		if err != nil {
			_ = sup.Shutdown(ctx)
			return fmt.Errorf("loading TLS certificates: %w", err)
		}
		cm.OnReload = func(ctx context.Context) error {
			log.Info("Reloading mysqld TLS certificates")
			if _, err := db.ExecContext(ctx, "ALTER INSTANCE RELOAD TLS"); err != nil {
				return fmt.Errorf("ALTER INSTANCE RELOAD TLS: %w", err)
			}
			return nil
		}
		srv = webserver.NewServerDynamic(opts.WebserverAddr, controller, cm)
		startCertWatcher(ctx, cm)
	}
	healthSrv := buildHealthServer(opts, controller)

	// The metrics endpoint reuses the control API's server certificate and
	// client CA, so a TLS-enabled metrics server requires the same key material
	// to be present. Scrapers authenticate with a client cert signed by that CA.
	var metricsTLSConfig *tls.Config
	if opts.MetricsTLS {
		if cm != nil {
			metricsTLSConfig = cm.TLSConfig()
		} else {
			metricsTLSConfig, err = opts.TLS.MTLSConfig()
			if err != nil {
				_ = sup.Shutdown(ctx)
				return fmt.Errorf("configuring metrics TLS: %w", err)
			}
		}
	}
	metricsSrv := metricserver.New(opts.MetricsAddr, metrics.NewExporter(db), metricsTLSConfig)

	serverErr := make(chan error, 1)
	log.Info("Starting control API server", "addr", opts.WebserverAddr, "tls", opts.TLS.ServerCertFile != "")
	go func() { serverErr <- serve(srv, opts.TLS) }()
	healthErr := make(chan error, 1)
	log.Info("Starting health API server", "addr", opts.HealthAddr)
	go func() { healthErr <- servePlain(healthSrv) }()
	metricsErr := make(chan error, 1)
	log.Info("Starting metrics API server", "addr", opts.MetricsAddr, "tls", opts.MetricsTLS)
	go func() {
		if opts.MetricsTLS {
			metricsErr <- serve(metricsSrv, opts.TLS)
		} else {
			metricsErr <- servePlain(metricsSrv)
		}
	}()

	// mysqld exit signals the supervisor's wait channel. A crash (non-zero exit
	// while unfenced) is fatal to the run loop (PID 1 exits, the kubelet restarts
	// the Pod). An intentional stop while the instance is fenced is not: the
	// manager stays alive with mysqld down and restarts it once the instance is
	// unfenced. A clean exit (zero) while unfenced is the clone restart case:
	// MySQL 8.0's CLONE INSTANCE shuts down the server after a successful clone,
	// expecting to be restarted by the supervisor; we restart mysqld in-place
	// instead of propagating the exit and restarting the Pod.
	mysqldExit := make(chan error, 1)
	go func() {
		for {
			err := sup.Wait()
			if !fence.IsFenced() {
				if err == nil {
					log.Info("mysqld exited cleanly after clone; restarting")
					if serr := sup.Start(ctx); serr != nil {
						mysqldExit <- fmt.Errorf("restarting mysqld after clone: %w", serr)
						return
					}
					continue
				}
				mysqldExit <- err
				return
			}
			log.Info("mysqld stopped while fenced; manager staying alive")
			if werr := fence.WaitUntilUnfenced(ctx); werr != nil {
				// Context cancelled during shutdown; stop supervising.
				return
			}
			log.Info("Restarting mysqld after unfence")
			if serr := sup.Start(ctx); serr != nil {
				mysqldExit <- fmt.Errorf("restarting mysqld after unfence: %w", serr)
				return
			}
		}
	}()

	// In-Pod role reconciler (CNPG pull-model). Runs until its context is
	// cancelled during shutdown.
	roleErr := make(chan error, 1)
	mgrCtx, cancelMgr := context.WithCancel(ctx)
	defer cancelMgr()
	if roleManaged {
		log.Info("Starting role reconciler")
		go func() {
			roleErr <- rolereconciler.Start(mgrCtx, rolereconciler.StartOptions{
				Namespace:          opts.Namespace,
				ClusterName:        opts.ClusterName,
				InstanceName:       opts.InstanceName,
				SourceTemplate:     opts.SourceTemplate,
				Local:              controller,
				GroupReplication:   opts.GroupReplication,
				OnAPIServerContact: isolationDetector.RecordContact,
			})
		}()
	}

	var runErr error
	for runErr == nil {
		select {
		case sig := <-signals:
			if sig == syscall.SIGHUP {
				log.Info("Received SIGHUP, reloading TLS certificates")
				if cm != nil {
					if err := cm.Reload(ctx); err != nil {
						log.Error(err, "Failed to reload TLS certificates")
					} else {
						log.Info("TLS certificates reloaded")
					}
				}
				continue
			}
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
		case err := <-metricsErr:
			log.Error(err, "Metrics API server failed")
			runErr = fmt.Errorf("metrics API server failed: %w", err)
		case err := <-roleErr:
			log.Error(err, "Role reconciler failed")
			runErr = fmt.Errorf("role reconciler failed: %w", err)
		case err := <-archiveErr:
			log.Error(err, "Continuous archiver failed")
			runErr = fmt.Errorf("continuous archiver failed: %w", err)
		}
	}
	cancelMgr()
	cancelArchive()

	log.Info("Shutting down instance manager")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), opts.ShutdownTimeout)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	_ = healthSrv.Shutdown(shutdownCtx)
	_ = metricsSrv.Shutdown(shutdownCtx)

	// Close the control connection before shutting down mysqld so the
	// graceful COM_QUIT handshake completes while the server is still alive.
	// The deferred close in the early-return paths is a no-op afterwards.
	_ = db.Close()
	shutdownMysqld(log, sup, opts)

	return runErr
}

func buildHealthServer(opts RunOptions, controller webserver.InstanceController) *http.Server {
	return &http.Server{
		Addr:              opts.HealthAddr,
		Handler:           webserver.HealthHandler(controller),
		ReadHeaderTimeout: 10 * time.Second,
	}
}

// startCertWatcher watches the certificate files managed by cm and calls
// cm.Reload() whenever any of them are modified. Debouncing handles short-lived
// write bursts (e.g. atomic rename, multiple Secrets updating).
//
// Kubernetes mounts Secrets as symlinks into atomic per-update directories, so a
// plain inotify watch on the resolved file misses Secret updates (the symlink
// target changes but the old inode never fires). Watching the parent directory
// catches the atomic swap the kubelet performs. A periodic poll guards against
// edge cases where the directory watch also misses events.
func startCertWatcher(ctx context.Context, cm *webserver.TLSCertManager) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logf.FromContext(ctx).Error(err, "Failed to start certificate file watcher")
		return
	}

	log := logf.FromContext(ctx).WithName("cert-watcher")

	dirs := map[string]struct{}{}
	for _, f := range cm.WatchedFiles() {
		dirs[filepath.Dir(f)] = struct{}{}
	}
	for d := range dirs {
		if err := watcher.Add(d); err != nil {
			log.Error(err, "Failed to watch certificate directory, reload only via SIGHUP",
				"dir", d)
			_ = watcher.Close()
			return
		}
	}

	files := cm.WatchedFiles()

	go func() {
		defer func() { _ = watcher.Close() }()
		debounce := 500 * time.Millisecond
		pollInterval := 60 * time.Second
		var timer *time.Timer
		var reloadC <-chan time.Time
		pollTicker := time.NewTicker(pollInterval)
		defer pollTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				if timer != nil {
					timer.Stop()
				}
				return
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Remove|fsnotify.Rename|fsnotify.Chmod) != 0 {
					if !affectsWatchedFiles(event.Name, files) {
						continue
					}
					if timer != nil {
						timer.Stop()
					}
					timer = time.NewTimer(debounce)
					reloadC = timer.C
					log.V(1).Info("Certificate directory event", "name", event.Name, "op", event.Op)
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Error(err, "Certificate file watcher error")
			case <-reloadC:
				reloadC = nil
				log.Info("Certificate files changed, reloading")
				if err := cm.Reload(ctx); err != nil {
					log.Error(err, "Failed to reload TLS certificates")
				} else {
					log.Info("TLS certificates reloaded")
				}
			case <-pollTicker.C:
				log.V(1).Info("Polling for certificate changes")
				if err := cm.Reload(ctx); err != nil {
					log.V(1).Info("Polling reload skipped", "error", err.Error())
				}
			}
		}
	}()
}

// affectsWatchedFiles reports whether name is or is inside any of the watched
// file paths (or their parent directories), so directory-level fsnotify events
// are filtered to only the certificate directories.
func affectsWatchedFiles(name string, files []string) bool {
	for _, f := range files {
		if name == f {
			return true
		}
		dir := filepath.Dir(f)
		if name == dir {
			return true
		}
		if strings.HasPrefix(name, dir+string(filepath.Separator)) {
			return true
		}
	}
	return false
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
func serve(srv *http.Server, tlsOpts webserver.TLSOptions) error {
	var err error
	if tlsOpts.ServerCertFile != "" {
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

// shutdownMysqld performs a two-phase shutdown of the mysqld process: SIGTERM
// triggers a clean (innodb_fast_shutdown=1) shutdown, which is given up to
// SmartShutdownTimeout to finish; if it overruns, mysqld keeps shutting down
// until StopDelay before being forced down with SIGKILL (crash recovery on the
// next start). StopDelay matches the Pod's TerminationGracePeriodSeconds, so the
// kubelet does not SIGKILL the Pod out from under us first.
func shutdownMysqld(
	log logr.Logger,
	sup *DetachedSupervisor,
	opts RunOptions,
) {
	log.Info("Requesting mysqld shutdown",
		"smartShutdownTimeout", opts.SmartShutdownTimeout,
		"stopDelay", opts.StopDelay)

	killed, err := sup.ShutdownGraceful(opts.SmartShutdownTimeout, opts.StopDelay)
	switch {
	case err != nil:
		log.Error(err, "Mysqld shutdown returned an error")
	case killed:
		log.Info("Mysqld did not stop within the stop delay; forced an immediate shutdown",
			"stopDelay", opts.StopDelay)
	default:
		log.Info("Mysqld stopped gracefully")
	}
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
