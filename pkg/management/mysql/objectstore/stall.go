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

// ErrStalled is the error a stalled stream reader reports. Uploads surface it
// as an ObjectStoreStalled or SourceStalled failure, depending on which side
// stopped, so the operator can tell a hung object store or source apart from an
// exhausted worker.
var ErrStalled = errors.New("stream stalled: no bytes moved")

// StallSide says which end of a watched stream stopped moving.
type StallSide string

const (
	// StallNone: the watchdog has not fired.
	StallNone StallSide = ""
	// StallSource: the consumer was waiting for bytes the source never sent.
	StallSource StallSide = "source"
	// StallConsumer: the consumer stopped reading and sending — for an upload,
	// the object store stopped accepting the part in flight.
	StallConsumer StallSide = "consumer"
)

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

// StallWatchReader wraps a stream reader and fails it when bytes stop moving:
// a Read that sees no data from the source for the timeout returns ErrStalled
// (a source stall), and, when a consumer watchdog is enabled, a consumer that
// neither reads nor reports upload progress for the timeout (the object store
// stopped responding) is torn down by the onStall callback so the blocked
// upload fails too (a consumer stall).
//
// The zero-progress deadline covers the stream, not the store's own request
// handling: once the reader has delivered EOF, completing the upload is the
// store's business. EOF is sticky, so a caller can probe a drained reader.
// Close stops the watchdog without closing the wrapped reader, which stays
// owned by the caller.
type StallWatchReader struct {
	src     io.Reader
	timeout time.Duration
	onStall func()

	chunks    chan stallChunk
	done      chan struct{}
	closeOnce sync.Once
	fireOnce  sync.Once

	mu         sync.Mutex
	pending    []byte
	pendingErr error
	eof        bool
	// lastActivity is the last time bytes moved: a Read returned data, or the
	// store reported sending part bytes through UploadProgress.
	lastActivity time.Time
	// inRead is set while the consumer waits inside Read; the per-Read timer,
	// not the consumer watchdog, owns that wait.
	inRead   bool
	finished bool
	side     StallSide
}

// NewStallWatchReader wraps src for an upload: Reads fail when the source sends
// nothing for timeout, and a watchdog fires when the consumer neither reads nor
// sends for timeout. onStall, typically the upload context's cancel, runs once
// when either fires, so an upload blocked inside the store's HTTP calls is
// aborted too.
func NewStallWatchReader(src io.Reader, timeout time.Duration, onStall func()) *StallWatchReader {
	w := newStallWatchReader(src, timeout, onStall)
	go w.watchdog()
	return w
}

// NewSourceStallReader wraps src for a download: Reads fail when the source
// sends nothing for timeout, and onStall (typically the request context's
// cancel, so the blocked source read is torn down too) runs once. There is no
// consumer watchdog — a consumer that is slow to take the next bytes (an
// extractor, a SQL client building an index) is not a stall.
func NewSourceStallReader(src io.Reader, timeout time.Duration, onStall func()) *StallWatchReader {
	return newStallWatchReader(src, timeout, onStall)
}

func newStallWatchReader(src io.Reader, timeout time.Duration, onStall func()) *StallWatchReader {
	w := &StallWatchReader{
		src:          src,
		timeout:      timeout,
		onStall:      onStall,
		chunks:       make(chan stallChunk, 1),
		done:         make(chan struct{}),
		lastActivity: time.Now(),
	}
	go w.pump()
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
			finished, side, inRead, last := w.finished, w.side, w.inRead, w.lastActivity
			w.mu.Unlock()
			if finished || side != StallNone {
				return
			}
			if !inRead && time.Since(last) > w.timeout {
				w.stall(StallConsumer)
				return
			}
		}
	}
}

// stall records the stall and runs the caller's callback once.
func (w *StallWatchReader) stall(side StallSide) {
	w.mu.Lock()
	if w.side == StallNone {
		w.side = side
	}
	w.mu.Unlock()
	if w.onStall != nil {
		w.fireOnce.Do(w.onStall)
	}
}

// Stalled reports whether the watchdog fired.
func (w *StallWatchReader) Stalled() bool {
	return w.StalledSide() != StallNone
}

// StalledSide reports which end of the stream stopped, or StallNone.
func (w *StallWatchReader) StalledSide() StallSide {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.side
}

// UploadProgress implements ProgressReporter: the store reports each chunk of
// a part it sends, which counts as progress while the consumer is not reading.
// A slow link that takes minutes to send one part is then not a stall.
func (w *StallWatchReader) UploadProgress() io.Reader {
	return progressTouch{w}
}

// progressTouch records upload progress on its watch reader.
type progressTouch struct{ w *StallWatchReader }

func (p progressTouch) Read(b []byte) (int, error) {
	if len(b) > 0 {
		p.w.mu.Lock()
		p.w.lastActivity = time.Now()
		p.w.mu.Unlock()
	}
	return len(b), nil
}

// Close stops the pump and the watchdog. The wrapped reader stays open.
func (w *StallWatchReader) Close() error {
	w.closeOnce.Do(func() { close(w.done) })
	return nil
}

// Read delivers the next bytes from the source, failing with ErrStalled when
// the source stops producing for the timeout. Once it has returned io.EOF it
// keeps returning io.EOF.
func (w *StallWatchReader) Read(p []byte) (int, error) {
	w.mu.Lock()
	w.inRead = true
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		w.inRead = false
		w.mu.Unlock()
	}()
	for {
		w.mu.Lock()
		if len(w.pending) > 0 {
			n := copy(p, w.pending)
			w.pending = w.pending[n:]
			w.lastActivity = time.Now()
			w.mu.Unlock()
			return n, nil
		}
		if w.eof {
			w.mu.Unlock()
			return 0, io.EOF
		}
		if w.pendingErr != nil {
			err := w.pendingErr
			w.pendingErr = nil
			if errors.Is(err, io.EOF) {
				w.eof = true
				w.mu.Unlock()
				return 0, io.EOF
			}
			w.mu.Unlock()
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
				w.lastActivity = time.Now()
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
			w.stall(StallSource)
			return 0, w.stallErr(ErrStalled)
		case <-w.done:
			return 0, w.stallErr(errors.New("stall watchdog closed"))
		}
	}
}

// stallErr reports err as the stream's failure, naming the stall deadline and
// the side that stopped when the watchdog fired.
func (w *StallWatchReader) stallErr(err error) error {
	what := ""
	switch w.StalledSide() {
	case StallNone:
		return err
	case StallSource:
		what = "the source sent nothing"
	case StallConsumer:
		what = "the consumer took nothing"
	}
	if errors.Is(err, ErrStalled) {
		return fmt.Errorf("%w: %s for %s", ErrStalled, what, w.timeout)
	}
	return fmt.Errorf("%w: %s for %s: %w", ErrStalled, what, w.timeout, err)
}
