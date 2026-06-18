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
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
)

// AdoptMysqldPIDEnv names the environment variable a re-exec'ing manager sets to
// hand the running mysqld PID to its replacement image. When Run sees it, it
// adopts the already-running mysqld (DetachedSupervisor.AdoptProcess) instead of
// starting a fresh one, so an in-place manager upgrade leaves mysqld untouched.
const AdoptMysqldPIDEnv = "CNMYSQL_ADOPT_MYSQLD_PID"

// selfExe is the path re-exec'd for an in-place upgrade. /proc/self/exe always
// resolves to the running manager binary, so execve re-loads whatever is on disk
// there (in this slice, the same binary; once binary upload lands, the new one).
const selfExe = "/proc/self/exe"

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
	if mysqldPID <= 0 {
		return fmt.Errorf("re-exec for in-place upgrade: invalid mysqld pid %d", mysqldPID)
	}
	if err := syscall.Exec(selfExe, os.Args, reexecEnv(mysqldPID)); err != nil {
		return fmt.Errorf("re-exec %s for in-place upgrade: %w", selfExe, err)
	}
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
