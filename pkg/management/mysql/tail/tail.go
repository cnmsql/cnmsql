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

// Package tail keeps the end of a stream: the last bytes a process wrote, where
// its error or its closing line is.
package tail

// Writer is an io.Writer that keeps only the last max bytes written to it.
type Writer struct {
	max int
	buf []byte
}

// NewWriter returns a Writer that keeps the last max bytes.
func NewWriter(max int) *Writer { return &Writer{max: max} }

func (t *Writer) Write(p []byte) (int, error) {
	t.buf = append(t.buf, p...)
	if len(t.buf) > t.max {
		t.buf = t.buf[len(t.buf)-t.max:]
	}
	return len(p), nil
}

// Bytes returns the kept bytes. They are only valid until the next Write.
func (t *Writer) Bytes() []byte { return t.buf }

func (t *Writer) String() string { return string(t.buf) }
