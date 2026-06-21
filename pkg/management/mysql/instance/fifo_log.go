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
	"bufio"
	"context"
	"errors"
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
		// A crashed prior process can leave its FIFO behind on a persistent
		// volume. A stale named pipe carries no data — its reader and writer are
		// both gone — so it is safe to remove and recreate. Anything else at the
		// path is unexpected and left untouched so the caller still errors out.
		if !errors.Is(err, syscall.EEXIST) {
			return nil, fmt.Errorf("fifo_log: mkfifo %s: %w", fifoPath, err)
		}
		info, statErr := os.Stat(fifoPath)
		if statErr != nil || info.Mode()&os.ModeNamedPipe == 0 {
			return nil, fmt.Errorf("fifo_log: mkfifo %s: %w", fifoPath, err)
		}
		logger.Info("Removing a stale FIFO left by a previous run", "fifo", fifoPath)
		if rmErr := os.Remove(fifoPath); rmErr != nil {
			return nil, fmt.Errorf("fifo_log: removing stale fifo %s: %w", fifoPath, rmErr)
		}
		if err := syscall.Mkfifo(fifoPath, 0600); err != nil {
			return nil, fmt.Errorf("fifo_log: mkfifo %s: %w", fifoPath, err)
		}
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

// FifoLogFromFD reconstructs a FifoLog around a read end inherited across a
// manager re-exec. The FIFO node already exists and its read fd (CLOEXEC cleared)
// survived execve at readFD, recorded in the pidfile by the previous image. It
// wraps that fd and re-opens a write-end keepalive so the reader does not see EOF
// if mysqld momentarily closes its end, mirroring NewFifoLog. Call Start to resume
// consuming structured log lines.
func FifoLogFromFD(fifoPath string, readFD int, logger logr.Logger) (*FifoLog, error) {
	if readFD < 0 {
		return nil, fmt.Errorf("fifo_log: invalid inherited read fd %d for %s", readFD, fifoPath)
	}
	readFile := os.NewFile(uintptr(readFD), fifoPath)
	if readFile == nil {
		return nil, fmt.Errorf("fifo_log: inherited read fd %d for %s is not valid", readFD, fifoPath)
	}

	writeEnd, err := os.OpenFile(fifoPath, os.O_WRONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("fifo_log: re-opening write end of %s: %w", fifoPath, err)
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
