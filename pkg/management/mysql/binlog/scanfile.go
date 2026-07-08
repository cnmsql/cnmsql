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

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// DefaultMysqlbinlog is the mysqlbinlog binary name resolved from PATH.
const DefaultMysqlbinlog = "mysqlbinlog"

// MysqlbinlogScanner returns a Scanner that shells out to mysqlbinlog to read a
// file's GTID range and timestamps. binPath defaults to DefaultMysqlbinlog when
// empty. mysqlbinlog stdout is a data path that is parsed by Scan; only its
// stderr is turned into structured log lines, per the project logging decision.
// When mariaDB is true, mariadb-binlog output is parsed (domain-server-seq
// triples) and the process name in log lines reflects that.
func MysqlbinlogScanner(binPath string, mariaDB bool) Scanner {
	if binPath == "" {
		binPath = DefaultMysqlbinlog
	}
	procName := "mysqlbinlog"
	if mariaDB {
		procName = "mariadb-binlog"
	}
	return func(ctx context.Context, path string) (ScanResult, error) {
		return runBinlogClient(ctx, binPath, procName, path,
			func(r io.Reader) (ScanResult, error) {
				return Scan(r, ScanOpts{MariaDB: mariaDB})
			})
	}
}

// MariadbTxnBoundaries runs mariadb-binlog over one file and returns the start
// byte offset of every GTID transaction, in file order. It is the positional
// counterpart of the GTID-set scan, used to bound MariaDB point-in-time recovery
// (which mariadb-binlog cannot filter by GTID) at exact transaction boundaries.
func MariadbTxnBoundaries(ctx context.Context, binPath, path string) ([]TxnBoundary, error) {
	if binPath == "" {
		binPath = DefaultMysqlbinlog
	}
	return runBinlogClient(ctx, binPath, "mariadb-binlog", path, scanMariaDBBoundaries)
}

// runBinlogClient shells out to the binlog client over one file and feeds its
// stdout (a data path) to parse. stderr is drained into structured logs so the
// pipe never blocks, and stdout is fully consumed even on a parse error so Wait
// succeeds.
func runBinlogClient[T any](
	ctx context.Context, binPath, procName, path string,
	parse func(io.Reader) (T, error),
) (T, error) {
	var zero T
	logger := logf.FromContext(ctx)
	args, err := ReadArgs(path)
	if err != nil {
		return zero, err
	}
	cmd := exec.CommandContext(ctx, binPath, args...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return zero, fmt.Errorf("binlog: %s stdout pipe: %w", procName, err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return zero, fmt.Errorf("binlog: %s stderr pipe: %w", procName, err)
	}
	if err := cmd.Start(); err != nil {
		return zero, fmt.Errorf("binlog: starting %s: %w", procName, err)
	}

	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			logger.Info("Process output", "process", procName, "stream", "stderr", "line", sc.Text())
		}
	}()

	res, parseErr := parse(stdout)
	_, _ = io.Copy(io.Discard, stdout)
	<-stderrDone

	waitErr := cmd.Wait()
	if waitErr != nil {
		return zero, fmt.Errorf("binlog: %s %s: %w", procName, path, waitErr)
	}
	if parseErr != nil {
		return zero, parseErr
	}
	return res, nil
}
