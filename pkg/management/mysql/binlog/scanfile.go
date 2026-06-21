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
