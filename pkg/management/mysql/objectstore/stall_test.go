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
	"bytes"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// blockingReader blocks every Read until release is closed, then reports EOF.
// It stands in for a source (or a store's request loop) that stopped responding.
type blockingReader struct {
	release chan struct{}
}

func newBlockingReader() *blockingReader {
	return &blockingReader{release: make(chan struct{})}
}

func (b *blockingReader) Read(_ []byte) (int, error) {
	<-b.release
	return 0, io.EOF
}

func (b *blockingReader) Close() { close(b.release) }

// slowReader delays every Read, to stand in for a source that is slow but alive.
type slowReader struct {
	reader io.Reader
	delay  time.Duration
}

func (s *slowReader) Read(p []byte) (int, error) {
	time.Sleep(s.delay)
	return s.reader.Read(p)
}

// stallCounter records how often a watchdog fired.
type stallCounter struct {
	mu     sync.Mutex
	called int
}

func (c *stallCounter) fire() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.called++
}

func (c *stallCounter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.called
}

func TestStallWatchReaderStreamsToEOF(t *testing.T) {
	t.Parallel()

	src := strings.NewReader("hello stalled world")
	watch := NewStallWatchReader(src, time.Minute, func() { t.Error("watchdog fired on a healthy stream") })
	defer func() { _ = watch.Close() }()

	got, err := io.ReadAll(watch)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if string(got) != "hello stalled world" {
		t.Fatalf("read %q", got)
	}
	if watch.Stalled() {
		t.Fatal("a completing stream must not be reported as stalled")
	}
}

func TestStallWatchReaderFailsWhenSourceStalls(t *testing.T) {
	t.Parallel()

	src := newBlockingReader()
	defer src.Close()
	counter := &stallCounter{}
	watch := NewStallWatchReader(src, 50*time.Millisecond, counter.fire)
	defer func() { _ = watch.Close() }()

	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, watch)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, ErrStalled) {
			t.Fatalf("err = %v, want ErrStalled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a stalled source must fail the read, not block it forever")
	}
	if !watch.Stalled() {
		t.Fatal("watch must report the stall")
	}
	if counter.count() != 1 {
		t.Fatalf("onStall called %d times, want once", counter.count())
	}
}

func TestStallWatchReaderFailsWhenConsumerStopsReading(t *testing.T) {
	t.Parallel()

	// The source sends one chunk and then stops responding, while the consumer
	// (the upload) stops reading: nothing unblocks a plain reader wrapper, so
	// the watchdog must fire on its own and take the stream down with it.
	src := io.MultiReader(bytes.NewReader(bytes.Repeat([]byte("x"), 1024)), &blockingReader{release: make(chan struct{})})
	counter := &stallCounter{}
	watch := NewStallWatchReader(src, 50*time.Millisecond, counter.fire)
	defer func() { _ = watch.Close() }()

	buf := make([]byte, 16)
	if _, err := io.ReadFull(watch, buf); err != nil {
		t.Fatalf("first read: %v", err)
	}

	// No more reads happen from here on; the watchdog must still fire.
	deadline := time.Now().Add(10 * time.Second)
	for counter.count() <= 0 {

		if time.Now().After(deadline) {
			t.Fatal("watchdog never fired after the consumer stopped reading")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !watch.Stalled() {
		t.Fatal("watch must report the stall")
	}
}

func TestStallWatchReaderToleratesSlowButProgressingSource(t *testing.T) {
	t.Parallel()

	payload := strings.Repeat("y", 4096)
	src := &slowReader{reader: strings.NewReader(payload), delay: 10 * time.Millisecond}
	watch := NewStallWatchReader(src, 200*time.Millisecond, func() { t.Error("watchdog fired on a slow but live stream") })
	defer func() { _ = watch.Close() }()

	got, err := io.ReadAll(watch)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if string(got) != payload {
		t.Fatalf("read %d bytes, want %d", len(got), len(payload))
	}
	if watch.Stalled() {
		t.Fatal("a progressing stream must not be reported as stalled")
	}
}

func TestStallWatchReaderCloseStopsWatchdog(t *testing.T) {
	t.Parallel()

	src := newBlockingReader()
	defer src.Close()
	counter := &stallCounter{}
	watch := NewStallWatchReader(src, 50*time.Millisecond, counter.fire)

	if err := watch.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := watch.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}

	// A closed watchdog must stay quiet well past its deadline.
	time.Sleep(250 * time.Millisecond)
	if counter.count() != 0 {
		t.Fatalf("onStall called %d times after close, want none", counter.count())
	}
	if watch.Stalled() {
		t.Fatal("a closed watchdog must not report a stall")
	}
}
