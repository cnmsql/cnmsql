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

package binlog

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os/exec"

	"github.com/go-logr/logr"
)

// DefaultMysqlbinlog is the mysqlbinlog binary name resolved from PATH.
const DefaultMysqlbinlog = "mysqlbinlog"

// MysqlbinlogScanner returns a Scanner that shells out to mysqlbinlog to read a
// file's GTID range and timestamps. binPath defaults to DefaultMysqlbinlog when
// empty. mysqlbinlog stdout is a data path that is parsed by Scan; only its
// stderr is turned into structured log lines, per the project logging decision.
func MysqlbinlogScanner(binPath string, logger logr.Logger) Scanner {
	if binPath == "" {
		binPath = DefaultMysqlbinlog
	}
	return func(ctx context.Context, path string) (ScanResult, error) {
		args, err := ReadArgs(path)
		if err != nil {
			return ScanResult{}, err
		}
		cmd := exec.CommandContext(ctx, binPath, args...)

		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return ScanResult{}, fmt.Errorf("binlog: mysqlbinlog stdout pipe: %w", err)
		}
		stderr, err := cmd.StderrPipe()
		if err != nil {
			return ScanResult{}, fmt.Errorf("binlog: mysqlbinlog stderr pipe: %w", err)
		}
		if err := cmd.Start(); err != nil {
			return ScanResult{}, fmt.Errorf("binlog: starting mysqlbinlog: %w", err)
		}

		// Drain stderr into structured logs concurrently so the pipe never blocks.
		stderrDone := make(chan struct{})
		go func() {
			defer close(stderrDone)
			sc := bufio.NewScanner(stderr)
			for sc.Scan() {
				logger.Info("Process output", "process", "mysqlbinlog", "stream", "stderr", "line", sc.Text())
			}
		}()

		res, scanErr := Scan(stdout)
		// Ensure stdout is fully drained even on a parse error, so Wait succeeds.
		_, _ = io.Copy(io.Discard, stdout)
		<-stderrDone

		waitErr := cmd.Wait()
		if waitErr != nil {
			return ScanResult{}, fmt.Errorf("binlog: mysqlbinlog %s: %w", path, waitErr)
		}
		if scanErr != nil {
			return ScanResult{}, scanErr
		}
		return res, nil
	}
}
