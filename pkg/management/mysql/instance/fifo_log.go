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
	"bufio"
	"context"
	"fmt"
	"os"
	"sync"
	"syscall"

	"github.com/go-logr/logr"
)

// FifoLog creates a named FIFO (mkfifo) for the long-lived mysqld process output,
// clears CLOEXEC on its read end, and runs a goroutine that wraps every line into
// a structured log via processLogWriter. Both the FIFO path and the read FD
// survive a manager re-exec so a re-exec'd image can re-adopt the log pipeline
// without losing any output lines.
type FifoLog struct {
	fifoPath string
	logger   logr.Logger

	readFile *os.File
	writeEnd *os.File

	mu   sync.Mutex
	done chan struct{}
}

// NewFifoLog creates the named FIFO, opens its read end with CLOEXEC cleared,
// and opens the write end (which stays blocking until Start is called to begin
// consuming). writeEnd is the *os.File to assign as the child process's output
// target.
func NewFifoLog(fifoPath string, logger logr.Logger) (*FifoLog, error) {
	if err := syscall.Mkfifo(fifoPath, 0600); err != nil {
		return nil, fmt.Errorf("fifo_log: mkfifo %s: %w", fifoPath, err)
	}

	rfd, err := syscall.Open(fifoPath, syscall.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		_ = os.Remove(fifoPath)
		return nil, fmt.Errorf("fifo_log: opening read end of %s: %w", fifoPath, err)
	}
	if _, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(rfd), syscall.F_SETFD, 0); errno != 0 {
		_ = syscall.Close(rfd)
		_ = os.Remove(fifoPath)
		return nil, fmt.Errorf("fifo_log: clearing CLOEXEC on read fd of %s: %w", fifoPath, errno)
	}
	readFile := os.NewFile(uintptr(rfd), fifoPath)

	writeEnd, err := os.OpenFile(fifoPath, os.O_WRONLY, 0)
	if err != nil {
		_ = readFile.Close()
		_ = os.Remove(fifoPath)
		return nil, fmt.Errorf("fifo_log: opening write end of %s: %w", fifoPath, err)
	}

	return &FifoLog{
		fifoPath: fifoPath,
		logger:   logger,
		readFile: readFile,
		writeEnd: writeEnd,
	}, nil
}

// WriteEnd returns the *os.File the child process inherits as its
// stdout/stderr target. Must not be called after Close.
func (f *FifoLog) WriteEnd() *os.File {
	return f.writeEnd
}

// ReadFD returns the numeric file descriptor of the read end, for inclusion in
// the pidfile so a re-exec'd manager can recover it.
func (f *FifoLog) ReadFD() int {
	return int(f.readFile.Fd())
}

// Path returns the FIFO path registered on the filesystem.
func (f *FifoLog) Path() string {
	return f.fifoPath
}

// Start begins consuming lines from the FIFO in a background goroutine. Each
// complete line is emitted as a structured log via logr (same shape as
// processLogWriter: stream=mysqld line=<text>). The goroutine exits when the
// FIFO read end returns EOF (all writers closed) or the context is cancelled.
func (f *FifoLog) Start(ctx context.Context) {
	f.mu.Lock()
	if f.done != nil {
		f.mu.Unlock()
		return
	}
	f.done = make(chan struct{})
	done := f.done
	f.mu.Unlock()

	log := f.logger.WithName("mysqld")
	writer := newProcessLogWriter(log, "mysqld")

	go func() {
		defer close(done)
		scanner := bufio.NewScanner(f.readFile)
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			_, _ = writer.Write(append(scanner.Bytes(), '\n'))
		}
	}()
}

// Close releases both ends of the FIFO and removes the filesystem node. Safe to
// call multiple times (idempotent). The reader goroutine exits as the read end
// closes.
func (f *FifoLog) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.readFile != nil {
		_ = f.readFile.Close()
		f.readFile = nil
	}
	if f.writeEnd != nil {
		_ = f.writeEnd.Close()
		f.writeEnd = nil
	}
	_ = os.Remove(f.fifoPath)
}
