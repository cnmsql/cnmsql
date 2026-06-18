/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

// Package executablehash computes the SHA-256 hash of the running binary so the
// operator can detect when an instance manager is stale and needs upgrade.
package executablehash

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"sync"
)

var (
	processBinaryHash string
	mu                sync.Mutex
)

// Get returns the SHA-256 hex hash of the running process binary. The result is
// cached after the first call, so repeated calls are cheap.
func Get() (string, error) {
	mu.Lock()
	defer mu.Unlock()

	if processBinaryHash != "" {
		return processBinaryHash, nil
	}

	exe, err := os.Executable()
	if err != nil {
		return "", err
	}

	f, err := os.Open(exe)
	if err != nil {
		return "", err
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	processBinaryHash = fmt.Sprintf("%x", h.Sum(nil))
	return processBinaryHash, nil
}
