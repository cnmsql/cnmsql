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

package instance

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
)

// AdoptMysqldPIDEnv names the environment variable a re-exec'ing manager sets to
// hand the running mysqld PID to its replacement image. When Run sees it, it
// adopts the already-running mysqld (DetachedSupervisor.AdoptProcess) instead of
// starting a fresh one, so an in-place manager upgrade leaves mysqld untouched.
const AdoptMysqldPIDEnv = "CNMYSQL_ADOPT_MYSQLD_PID"

// IsolationLastContactEnv names the environment variable a re-exec'ing manager
// sets to hand the time of its last successful Kubernetes API-server contact
// (Unix nanoseconds) to its replacement image. The new image seeds its isolation
// detector from it instead of resetting the clock to "now", so an in-place
// upgrade does not make a healthy primary briefly look isolated — and a
// genuinely partitioned primary still trips the liveness probe at the real
// timeout measured from its last true contact. Empty/absent when the previous
// image had no detector (non-cluster-managed instance).
const IsolationLastContactEnv = "CNMYSQL_ISOLATION_LAST_CONTACT"

// selfExe is the path re-exec'd for a byte-identical in-place restart
// (restart-inplace). /proc/self/exe always resolves to the running manager
// binary, so execve re-loads exactly what is already on disk.
const selfExe = "/proc/self/exe"

// uploadDir is the directory the new instance-manager binary is streamed into,
// and managerPath is the on-disk binary the instance containers exec
// (/controller/manager, copied there by the bootstrap-controller init
// container). For a streamed upgrade the new binary is written into uploadDir,
// renamed over managerPath, and that path is re-exec'd — not selfExe, which
// still points at the now-unlinked old binary after the rename. They are
// package vars only so tests can redirect them to a temp dir.
var (
	uploadDir   = "/controller"
	managerPath = "/controller/manager"
)

// inPlaceUpgrading marks that an in-place manager re-exec is in flight, so any
// concurrent shutdown path (signal handler, mysqld-exit watcher) does not tear
// mysqld down while the manager swaps itself. syscall.Exec runs no deferreds, so
// the old image's shutdown never fires on success; this guard covers the window
// before the exec and a failed exec.
var inPlaceUpgrading atomic.Bool

// SetInPlaceUpgrading records that an in-place upgrade is starting.
func SetInPlaceUpgrading() { inPlaceUpgrading.Store(true) }

// IsInPlaceUpgrading reports whether an in-place upgrade is in flight.
func IsInPlaceUpgrading() bool { return inPlaceUpgrading.Load() }

// ReExecForUpgrade replaces the running manager image with a fresh exec of itself,
// passing mysqldPID via AdoptMysqldPIDEnv so the new image adopts the running
// mysqld rather than starting a new one, and lastAPIContact via
// IsolationLastContactEnv so the new image resumes the isolation clock instead of
// resetting it. On success it does not return (the process image is replaced); it
// only returns when the execve itself fails, in which case the caller keeps
// supervising the existing mysqld unharmed.
func ReExecForUpgrade(mysqldPID int, lastAPIContact time.Time) error {
	return reExecPath(selfExe, mysqldPID, lastAPIContact)
}

// ReExecOnDiskForUpgrade re-execs the on-disk instance-manager binary
// (managerPath) rather than selfExe. It is the second half of a streamed
// upgrade: WriteInstanceManager has already replaced managerPath with the new
// binary, so the running image (whose /proc/self/exe still points at the old,
// now-unlinked binary) must exec the path to pick the new one up.
func ReExecOnDiskForUpgrade(mysqldPID int, lastAPIContact time.Time) error {
	return reExecPath(managerPath, mysqldPID, lastAPIContact)
}

// reExecPath replaces the running manager image with a fresh exec of the binary
// at path, passing mysqldPID via AdoptMysqldPIDEnv so the new image adopts the
// running mysqld rather than starting a new one, and lastAPIContact via
// IsolationLastContactEnv so the new image resumes the isolation clock. On
// success it does not return (the process image is replaced); it only returns
// when the execve itself fails, in which case the caller keeps supervising the
// existing mysqld unharmed.
func reExecPath(path string, mysqldPID int, lastAPIContact time.Time) error {
	if mysqldPID <= 0 {
		return fmt.Errorf("re-exec for in-place upgrade: invalid mysqld pid %d", mysqldPID)
	}
	if err := syscall.Exec(path, os.Args, reexecEnv(mysqldPID, lastAPIContact)); err != nil {
		return fmt.Errorf("re-exec %s for in-place upgrade: %w", path, err)
	}
	return nil
}

// WriteInstanceManager streams a new instance-manager binary from r into
// uploadDir, verifies its SHA-256 matches expectedHash (when non-empty), makes
// it executable, and atomically renames it over managerPath. It does not
// re-exec; the caller schedules ReExecOnDiskForUpgrade once the HTTP response
// has flushed. A hash mismatch returns webserver.ErrInvalidInstanceManagerBinary
// and leaves managerPath untouched, so a corrupted upload never replaces the
// running binary.
func WriteInstanceManager(r io.Reader, expectedHash string) (err error) {
	tmp, err := os.CreateTemp(uploadDir, "manager_*.new")
	if err != nil {
		return fmt.Errorf("creating temp file for instance-manager upgrade: %w", err)
	}
	tmpName := tmp.Name()
	// Remove the temp file unless it was successfully renamed into place.
	renamed := false
	defer func() {
		if !renamed {
			if rmErr := os.Remove(tmpName); rmErr != nil && !os.IsNotExist(rmErr) {
				err = errors.Join(err, fmt.Errorf("removing temp upgrade file %s: %w", tmpName, rmErr))
			}
		}
	}()

	hash := sha256.New()
	if _, err := io.Copy(tmp, io.TeeReader(r, hash)); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("writing new instance-manager binary: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing new instance-manager binary: %w", err)
	}

	gotHash := fmt.Sprintf("%x", hash.Sum(nil))
	if expectedHash != "" && gotHash != expectedHash {
		return fmt.Errorf("%w: got %s, want %s", webserver.ErrInvalidInstanceManagerBinary, gotHash, expectedHash)
	}

	if err := os.Chmod(tmpName, 0o755); err != nil { //nolint:gosec // executable, our own temp file
		return fmt.Errorf("making new instance-manager binary executable: %w", err)
	}
	if err := os.Rename(tmpName, managerPath); err != nil {
		return fmt.Errorf("replacing instance-manager binary %s: %w", managerPath, err)
	}
	renamed = true
	return nil
}

// adoptRequest reports whether the manager was re-exec'd for an in-place upgrade
// and, if so, the running mysqld PID it must adopt. It reads AdoptMysqldPIDEnv set
// by the previous image's ReExecForUpgrade.
func adoptRequest() (adopting bool, mysqldPID int) {
	v := strings.TrimSpace(os.Getenv(AdoptMysqldPIDEnv))
	if v == "" {
		return false, 0
	}
	pid, err := strconv.Atoi(v)
	if err != nil || pid <= 0 {
		return false, 0
	}
	return true, pid
}

// readPIDFileFIFOFD reads the inherited FIFO read fd the previous manager image
// recorded in the pidfile (the "fd=" line written by DetachedSupervisor), so the
// re-exec'd image can re-attach the mysqld log pipeline via FifoLogFromFD.
func readPIDFileFIFOFD(pidFilePath string) (int, error) {
	data, err := os.ReadFile(pidFilePath)
	if err != nil {
		return 0, fmt.Errorf("reading pidfile %s for FIFO re-adoption: %w", pidFilePath, err)
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		if rest, ok := strings.CutPrefix(line, "fd="); ok {
			fd, err := strconv.Atoi(strings.TrimSpace(rest))
			if err != nil {
				return 0, fmt.Errorf("parsing fd= in pidfile %s: %w", pidFilePath, err)
			}
			return fd, nil
		}
	}
	return 0, fmt.Errorf("pidfile %s has no fd= entry for FIFO re-adoption", pidFilePath)
}

// reexecEnv returns the current environment with AdoptMysqldPIDEnv set (replacing
// any existing entry) to the mysqld PID the re-exec'd image must adopt, and
// IsolationLastContactEnv set to lastAPIContact (Unix nanoseconds) so the new
// image resumes the isolation clock. A zero lastAPIContact is omitted (and any
// stale entry cleared), so a re-exec from an image without an isolation detector
// hands nothing forward. Split out from ReExecForUpgrade so the env wiring is
// unit-testable without an execve.
func reexecEnv(mysqldPID int, lastAPIContact time.Time) []string {
	env := upsertEnv(os.Environ(), AdoptMysqldPIDEnv, strconv.Itoa(mysqldPID))
	if lastAPIContact.IsZero() {
		return removeEnv(env, IsolationLastContactEnv)
	}
	return upsertEnv(env, IsolationLastContactEnv, strconv.FormatInt(lastAPIContact.UnixNano(), 10))
}

// upsertEnv sets key=value in env, replacing an existing entry for key or
// appending a new one. The returned slice may share backing storage with env.
func upsertEnv(env []string, key, value string) []string {
	prefix := key + "="
	entry := prefix + value
	for i, kv := range env {
		if strings.HasPrefix(kv, prefix) {
			env[i] = entry
			return env
		}
	}
	return append(env, entry)
}

// removeEnv drops any entry for key from env, returning the filtered slice.
func removeEnv(env []string, key string) []string {
	prefix := key + "="
	out := env[:0]
	for _, kv := range env {
		if strings.HasPrefix(kv, prefix) {
			continue
		}
		out = append(out, kv)
	}
	return out
}

// isolationLastContactFromEnv reads the last successful API-server contact time a
// re-exec'ing manager recorded in IsolationLastContactEnv, so the adopting image
// can resume the isolation clock. It reports ok=false when the variable is
// absent or malformed, leaving the caller to use the fresh "just contacted" seed.
func isolationLastContactFromEnv() (t time.Time, ok bool) {
	v := strings.TrimSpace(os.Getenv(IsolationLastContactEnv))
	if v == "" {
		return time.Time{}, false
	}
	nanos, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(0, nanos), true
}
