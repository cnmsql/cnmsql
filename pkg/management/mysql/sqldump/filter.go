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

// Package sqldump reads the SQL dumps that mysqldump and mariadb-dump write
// with --databases.
package sqldump

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
)

// markerPrefix starts the comment line both dump clients write before each
// database's section. A database can have two sections: views are written in
// a second pass at the end, under a repeated marker.
const markerPrefix = "-- Current Database: `"

// maxMarkerLine bounds a marker line. A database name is at most 64
// characters of 3 bytes each, and doubled backticks at most double that.
const maxMarkerLine = 1024

// readBufferBytes is the filter's read buffer. Lines longer than this (an
// extended INSERT holding a large row) are streamed through in pieces.
const readBufferBytes = 1 << 20

// FilterDatabases copies a dump from r to w, keeping only the sections of the
// selected databases. Everything before the first marker (the header that
// sets the session up) is always kept. A section runs from its marker to the
// next one, so the trailer that restores session variables after the last
// section is dropped when that section is not selected; it only resets the
// loading session and changes no data. An empty selection copies the whole
// dump.
//
// The markers cannot be forged by data: both clients escape newlines inside
// values, so a line can only start with a marker when the client wrote it.
//
// It returns the databases whose sections it kept, in the order they first
// appeared.
func FilterDatabases(w io.Writer, r io.Reader, selected []string) ([]string, error) {
	if len(selected) == 0 {
		_, err := io.Copy(w, r)
		return nil, err
	}
	keepDB := make(map[string]bool, len(selected))
	for _, db := range selected {
		keepDB[db] = true
	}

	br := bufio.NewReaderSize(r, readBufferBytes)
	var kept []string
	keep := true
	atLineStart := true
	for {
		if atLineStart {
			peek, _ := br.Peek(len(markerPrefix))
			if string(peek) == markerPrefix {
				line, err := readMarkerLine(br)
				if err != nil {
					return kept, err
				}
				db, err := parseMarker(line)
				if err != nil {
					return kept, err
				}
				keep = keepDB[db]
				if keep {
					if !slices.Contains(kept, db) {
						kept = append(kept, db)
					}
					if _, err := w.Write(line); err != nil {
						return kept, err
					}
				}
				continue
			}
		}
		chunk, err := br.ReadSlice('\n')
		if len(chunk) > 0 {
			if keep {
				if _, werr := w.Write(chunk); werr != nil {
					return kept, werr
				}
			}
			atLineStart = chunk[len(chunk)-1] == '\n'
		}
		switch {
		case err == nil, errors.Is(err, bufio.ErrBufferFull):
		case errors.Is(err, io.EOF):
			return kept, nil
		default:
			return kept, err
		}
	}
}

// readMarkerLine reads a whole marker line, newline included when present.
func readMarkerLine(br *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		chunk, err := br.ReadSlice('\n')
		line = append(line, chunk...)
		if len(line) > maxMarkerLine {
			return nil, fmt.Errorf("dump: database marker line longer than %d bytes", maxMarkerLine)
		}
		switch {
		case err == nil, errors.Is(err, io.EOF):
			return line, nil
		case errors.Is(err, bufio.ErrBufferFull):
		default:
			return nil, err
		}
	}
}

// parseMarker returns the database a marker line names. The name is quoted
// with backticks, and a backtick inside it is doubled.
func parseMarker(line []byte) (string, error) {
	quoted := strings.TrimRight(string(line), "\r\n")
	quoted = strings.TrimPrefix(quoted, markerPrefix[:len(markerPrefix)-1])
	if len(quoted) < 2 || quoted[0] != '`' || quoted[len(quoted)-1] != '`' {
		return "", fmt.Errorf("dump: malformed database marker %q", bytes.TrimRight(line, "\r\n"))
	}
	inner := quoted[1 : len(quoted)-1]
	var name strings.Builder
	for i := 0; i < len(inner); i++ {
		if inner[i] == '`' {
			if i+1 >= len(inner) || inner[i+1] != '`' {
				return "", fmt.Errorf("dump: malformed database marker %q", bytes.TrimRight(line, "\r\n"))
			}
			i++
		}
		name.WriteByte(inner[i])
	}
	if name.Len() == 0 {
		return "", fmt.Errorf("dump: database marker %q names no database", bytes.TrimRight(line, "\r\n"))
	}
	return name.String(), nil
}
