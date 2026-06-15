/*
Copyright 2026 The cloudnative-mysql Authors.

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
	"bytes"
	"sync"

	"github.com/go-logr/logr"
)

// processLogWriter turns child-process stdout/stderr into structured log lines.
type processLogWriter struct {
	logger logr.Logger
	stream string

	mu  sync.Mutex
	buf bytes.Buffer
}

func newProcessLogWriter(logger logr.Logger, stream string) *processLogWriter {
	return &processLogWriter{logger: logger, stream: stream}
}

func newProcessLogWriters(logger logr.Logger) (*processLogWriter, *processLogWriter) {
	return newProcessLogWriter(logger, "stdout"), newProcessLogWriter(logger, "stderr")
}

func (w *processLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	written := len(p)
	for len(p) > 0 {
		if i := bytes.IndexByte(p, '\n'); i >= 0 {
			w.buf.Write(p[:i])
			w.flushLocked()
			p = p[i+1:]
			continue
		}
		w.buf.Write(p)
		break
	}
	return written, nil
}

func (w *processLogWriter) flushLocked() {
	line := bytes.TrimRight(w.buf.Bytes(), "\r")
	w.buf.Reset()
	if len(line) == 0 {
		return
	}
	w.logger.Info("Process output", "stream", w.stream, "line", string(line))
}
