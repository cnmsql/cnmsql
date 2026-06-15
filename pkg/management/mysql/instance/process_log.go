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
