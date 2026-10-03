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

package slowlog

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/slowlog/internal/slowparse"
)

// parseFrom parses f from offset to its end and calls fn with every entry, in
// order. fn returning false stops the parse. The parser reads to EOF and
// stops; it emits the entry it was reading at EOF even if mysqld has not
// finished writing it, which the tailer accounts for.
func parseFrom(f *os.File, offset int64, fn func(*slowparse.Event) bool) error {
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	p := slowparse.NewParser(f, slowparse.Options{
		StartOffset: uint64(offset),
		// MariaDB writes "# Time: YYMMDD HH:MM:SS" in server time; instance
		// containers run in UTC.
		DefaultLocation: time.UTC,
		MaxQueryBytes:   MaxQueryBytes,
	})
	errc := make(chan error, 1)
	go func() { errc <- runParser(p) }()
	stopped := false
	for e := range p.Events() {
		if stopped {
			continue
		}
		if !fn(e) {
			stopped = true
			p.Stop()
		}
	}
	return <-errc
}

// runParser runs p to completion and turns a parser panic into an error. The
// parser's deferred close of the event channel still runs, so the caller's
// range loop ends.
func runParser(p *slowparse.Parser) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("slow log parser: %v", r)
		}
	}()
	return p.Start()
}
