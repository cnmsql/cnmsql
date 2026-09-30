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

// Package rootclient is the handshake between `kubectl cnmsql` and
// `manager instance client`, which starts the database client as root inside
// an instance container. The plugin sends the root password as the first line
// of the exec's stdin, only after the in-pod side has turned terminal echo off
// and printed PasswordReady, so the password never appears in any process's
// arguments nor on a terminal. Running this in the manager rather than in a
// shell keeps the instance image free to have no shell at all.
package rootclient

import (
	"errors"
	"fmt"
	"io"
	"strings"
)

// PasswordReady is printed once terminal echo is off and the password line
// can be sent. It is an OSC sequence with a private number, so a terminal that
// ever received it would ignore it rather than display it.
const PasswordReady = "\x1b]7717;cnmsql-password\x07"

// ReadPassword turns echo off when in is a terminal, prints PasswordReady to
// out, and reads the password line from in. It reads one byte at a time so
// that everything after the line is left in in for the client. Echo is
// restored before it returns.
func ReadPassword(in io.Reader, out io.Writer) (string, error) {
	restore := disableEcho(in)
	defer restore()

	if _, err := io.WriteString(out, PasswordReady); err != nil {
		return "", fmt.Errorf("writing the password prompt: %w", err)
	}
	var line strings.Builder
	b := make([]byte, 1)
	for {
		n, err := in.Read(b)
		if n == 1 {
			if b[0] == '\n' {
				return strings.TrimSuffix(line.String(), "\r"), nil
			}
			line.WriteByte(b[0])
		}
		if errors.Is(err, io.EOF) {
			return "", errors.New("stdin closed before the password line")
		}
		if err != nil {
			return "", fmt.Errorf("reading the password: %w", err)
		}
	}
}
