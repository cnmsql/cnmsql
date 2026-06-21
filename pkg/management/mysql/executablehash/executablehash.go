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
