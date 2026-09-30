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

package rootclient

import (
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// disableEcho turns echo off when in is a terminal and returns a function that
// restores it. Anything else (a pipe, without a TTY) is left alone.
func disableEcho(in io.Reader) func() {
	f, ok := in.(*os.File)
	if !ok {
		return func() {}
	}
	fd := int(f.Fd())
	termios, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	if err != nil {
		return func() {}
	}
	saved := *termios
	termios.Lflag &^= unix.ECHO
	if err := unix.IoctlSetTermios(fd, unix.TCSETS, termios); err != nil {
		return func() {}
	}
	return func() { _ = unix.IoctlSetTermios(fd, unix.TCSETS, &saved) }
}
