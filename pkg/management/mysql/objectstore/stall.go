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

package objectstore

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// DefaultStallTimeout is how long an upload may see no bytes move before the
// stall watchdog fails it. It must stay well above a healthy transfer's worst
// inter-read gap, and well below the backup worker Job's active deadline.
const DefaultStallTimeout = 5 * time.Minute

// ErrStalled is the error a stalled upload reader reports. Uploads surface it
// as an ObjectStoreStalled failure so the operator can tell a hung object store
// apart from an exhausted worker.
var ErrStalled = errors.New("object store upload stalled: no bytes moved")

// stallChunkSize is how far the watchdog reads ahead of the consumer. One chunk
// in flight keeps the wrapped reader interruptible without buffering the
// upload in memory.
const stallChunkSize = 256 << 10

// stallChunk is one read from the wrapped source, handed from the pump
// goroutine to Read.
type stallChunk struct {
	data []byte
	err  error
}

// StallWatchReader wraps an upload reader and fails it when bytes stop moving:
// a Read that sees no data for the timeout returns ErrStalled, and a consumer
// that stops reading (the object store stopped responding) is torn down by the
// onStall callback so the blocked upload fails too.
//
// The zero-progress deadline covers the stream, not the store's own request
// handling: once the reader has delivered EOF, completing the upload is the
// store's business. Close stops the watchdog without closing the wrapped
// reader, which stays owned by the caller.
type StallWatchReader struct {
	src     io.Reader
	timeout time.Duration
	onStall func()

	chunks    chan stallChunk
	done      chan struct{}
	closeOnce sync.Once
	fireOnce  sync.Once

	mu          sync.Mutex
	pending     []byte
	pendingErr  error
	lastRead    time.Time
	finished    bool
	stalledFlag bool
}

// NewStallWatchReader wraps src so uploads fail when no bytes move for timeout.
// onStall, typically the upload context's cancel, runs once when the watchdog
// fires, so an upload blocked inside the store's HTTP calls is aborted too.
func NewStallWatchReader(src io.Reader, timeout time.Duration, onStall func()) *StallWatchReader {
	w := &StallWatchReader{
		src:      src,
		timeout:  timeout,
		onStall:  onStall,
		chunks:   make(chan stallChunk, 1),
		done:     make(chan struct{}),
		lastRead: time.Now(),
	}
	go w.pump()
	go w.watchdog()
	return w
}

// pump pulls from the source ahead of the consumer so a blocking read can be
// raced against the stall deadline. It never reuses a chunk buffer: a delivered
// chunk belongs to the consumer.
func (w *StallWatchReader) pump() {
	for {
		buf := make([]byte, stallChunkSize)
		n, err := w.src.Read(buf)
		select {
		case w.chunks <- stallChunk{data: buf[:n], err: err}:
		case <-w.done:
			return
		}
		if err != nil {
			return
		}
	}
}

// watchdog fails the stream when progress stops while nobody is reading —
// the consumer is blocked inside the store's HTTP calls, so nothing else
// would ever notice.
func (w *StallWatchReader) watchdog() {
	if w.timeout/4 <= 0 {
		// Too short to poll: the per-Read deadline still applies.
		return
	}
	ticker := time.NewTicker(w.timeout / 4)
	defer ticker.Stop()
	for {
		select {
		case <-w.done:
			return
		case <-ticker.C:
			w.mu.Lock()
			finished, stalled, last := w.finished, w.stalledFlag, w.lastRead
			w.mu.Unlock()
			if finished || stalled {
				return
			}
			if time.Since(last) > w.timeout {
				w.stall()
				return
			}
		}
	}
}

// stall records the stall and runs the caller's callback once.
func (w *StallWatchReader) stall() {
	w.mu.Lock()
	w.stalledFlag = true
	w.mu.Unlock()
	if w.onStall != nil {
		w.fireOnce.Do(w.onStall)
	}
}

// Stalled reports whether the watchdog fired.
func (w *StallWatchReader) Stalled() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stalledFlag
}

// Close stops the pump and the watchdog. The wrapped reader stays open.
func (w *StallWatchReader) Close() error {
	w.closeOnce.Do(func() { close(w.done) })
	return nil
}

// Read delivers the next bytes from the source, failing with ErrStalled when
// the source stops producing for the timeout.
func (w *StallWatchReader) Read(p []byte) (int, error) {
	for {
		w.mu.Lock()
		if len(w.pending) > 0 {
			n := copy(p, w.pending)
			w.pending = w.pending[n:]
			w.lastRead = time.Now()
			w.mu.Unlock()
			return n, nil
		}
		if w.pendingErr != nil {
			err := w.pendingErr
			w.pendingErr = nil
			w.mu.Unlock()
			if errors.Is(err, io.EOF) {
				return 0, io.EOF
			}
			return 0, w.stallErr(err)
		}
		w.mu.Unlock()

		if len(p) == 0 {
			return 0, nil
		}

		timer := time.NewTimer(w.timeout)
		select {
		case chunk := <-w.chunks:
			timer.Stop()
			if len(chunk.data) > 0 {
				n := copy(p, chunk.data)
				w.mu.Lock()
				if n < len(chunk.data) {
					w.pending = chunk.data[n:]
				}
				w.lastRead = time.Now()
				if chunk.err != nil {
					w.pendingErr = chunk.err
					w.finished = true
				}
				w.mu.Unlock()
				return n, nil
			}
			if chunk.err != nil {
				w.mu.Lock()
				w.pendingErr = chunk.err
				w.finished = true
				w.mu.Unlock()
				continue
			}
			// A zero-byte read without an error: keep waiting.
		case <-timer.C:
			w.stall()
			return 0, w.stallErr(ErrStalled)
		case <-w.done:
			return 0, w.stallErr(errors.New("stall watchdog closed"))
		}
	}
}

// stallErr reports err as the upload's failure, naming the stall deadline when
// the watchdog fired.
func (w *StallWatchReader) stallErr(err error) error {
	if !w.Stalled() {
		return err
	}
	return fmt.Errorf("%w for %s: %w", ErrStalled, w.timeout, err)
}
