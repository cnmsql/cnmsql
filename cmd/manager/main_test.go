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

package main

import (
	"bytes"
	"strings"
	"testing"

	"go.uber.org/zap/zapcore"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/slowlog"
)

// TestManagerLoggerSharesTheRecordOutput: the manager's logs and the slow log
// records go to the same stderr. They must share one lock, or a record larger
// than a pipe write (PIPE_BUF) can be spliced with a manager line and corrupt
// both JSON lines.
func TestManagerLoggerSharesTheRecordOutput(t *testing.T) {
	var buf bytes.Buffer
	saved := slowlog.Output
	slowlog.Output = zapcore.Lock(zapcore.AddSync(&buf))
	t.Cleanup(func() { slowlog.Output = saved })

	newLogger().Info("Manager line")
	if !strings.Contains(buf.String(), `"msg":"Manager line"`) {
		t.Fatalf("manager logger did not write through slowlog.Output: %q", buf.String())
	}
}
