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

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
)

// AdoptMysqldPIDEnv names the environment variable a re-exec'ing manager sets to
// hand the running mysqld PID to its replacement image. When Run sees it, it
// adopts the already-running mysqld (DetachedSupervisor.AdoptProcess) instead of
// starting a fresh one, so an in-place manager upgrade leaves mysqld untouched.
const AdoptMysqldPIDEnv = "CNMYSQL_ADOPT_MYSQLD_PID"

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
// mysqld rather than starting a new one. On success it does not return (the
// process image is replaced); it only returns when the execve itself fails, in
// which case the caller keeps supervising the existing mysqld unharmed.
func ReExecForUpgrade(mysqldPID int) error {
	return reExecPath(selfExe, mysqldPID)
}

// ReExecOnDiskForUpgrade re-execs the on-disk instance-manager binary
// (managerPath) rather than selfExe. It is the second half of a streamed
// upgrade: WriteInstanceManager has already replaced managerPath with the new
// binary, so the running image (whose /proc/self/exe still points at the old,
// now-unlinked binary) must exec the path to pick the new one up.
func ReExecOnDiskForUpgrade(mysqldPID int) error {
	return reExecPath(managerPath, mysqldPID)
}

// reExecPath replaces the running manager image with a fresh exec of the binary
// at path, passing mysqldPID via AdoptMysqldPIDEnv so the new image adopts the
// running mysqld rather than starting a new one. On success it does not return
// (the process image is replaced); it only returns when the execve itself fails,
// in which case the caller keeps supervising the existing mysqld unharmed.
func reExecPath(path string, mysqldPID int) error {
	if mysqldPID <= 0 {
		return fmt.Errorf("re-exec for in-place upgrade: invalid mysqld pid %d", mysqldPID)
	}
	if err := syscall.Exec(path, os.Args, reexecEnv(mysqldPID)); err != nil {
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
// any existing entry) to the mysqld PID the re-exec'd image must adopt. Split out
// from ReExecForUpgrade so the env wiring is unit-testable without an execve.
func reexecEnv(mysqldPID int) []string {
	prefix := AdoptMysqldPIDEnv + "="
	value := prefix + strconv.Itoa(mysqldPID)
	env := os.Environ()
	for i, kv := range env {
		if len(kv) >= len(prefix) && kv[:len(prefix)] == prefix {
			env[i] = value
			return env
		}
	}
	return append(env, value)
}
