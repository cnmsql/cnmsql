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
	"fmt"
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

// openPTY opens a pseudo-terminal pair and returns its terminal side.
func openPTY(t *testing.T) *os.File {
	t.Helper()
	ptmx, err := os.OpenFile("/dev/ptmx", os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		t.Skipf("no pseudo-terminals here: %v", err)
	}
	t.Cleanup(func() { _ = ptmx.Close() })
	if err := unix.IoctlSetPointerInt(int(ptmx.Fd()), unix.TIOCSPTLCK, 0); err != nil {
		t.Fatalf("unlocking the pty: %v", err)
	}
	n, err := unix.IoctlGetInt(int(ptmx.Fd()), unix.TIOCGPTN)
	if err != nil {
		t.Fatalf("getting the pty number: %v", err)
	}
	tty, err := os.OpenFile(fmt.Sprintf("/dev/pts/%d", n), os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		t.Fatalf("opening the pty: %v", err)
	}
	t.Cleanup(func() { _ = tty.Close() })
	return tty
}

func echoOn(t *testing.T, f *os.File) bool {
	t.Helper()
	termios, err := unix.IoctlGetTermios(int(f.Fd()), unix.TCGETS)
	if err != nil {
		t.Fatalf("reading the terminal settings: %v", err)
	}
	return termios.Lflag&unix.ECHO != 0
}

func TestDisableEchoOnATerminal(t *testing.T) {
	tty := openPTY(t)
	if !echoOn(t, tty) {
		t.Fatal("a new pty should echo")
	}
	restore := disableEcho(tty)
	if echoOn(t, tty) {
		t.Error("echo is still on")
	}
	restore()
	if !echoOn(t, tty) {
		t.Error("echo was not restored")
	}
}
