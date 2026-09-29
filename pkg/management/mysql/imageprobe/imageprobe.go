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

// Package imageprobe is the contract between `manager instance probe`, which
// runs inside an instance image, and the operator, which reads its result from
// the probe Pod's termination message (design 033).
package imageprobe

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/version"
)

// Flavors, matching the Cluster API's spec.flavor values.
const (
	FlavorMySQL   = "mysql"
	FlavorMariaDB = "mariadb"
)

// Result is what a probe learns about an image by running its server binary.
type Result struct {
	// Flavor is "mysql" or "mariadb".
	Flavor string `json:"flavor"`
	// ServerVersion is the major.minor.patch release, e.g. "8.4.11".
	ServerVersion string `json:"serverVersion"`
	// Banner is the first line of `mysqld --version`, for diagnostics.
	Banner string `json:"banner,omitempty"`
}

// Run probes the server binary at mysqld.
func Run(ctx context.Context, mysqld string) (Result, error) {
	server, err := version.Detect(ctx, mysqld)
	if err != nil {
		return Result{}, err
	}
	flavor := FlavorMySQL
	if server.MariaDB {
		flavor = FlavorMariaDB
	}
	return Result{Flavor: flavor, ServerVersion: server.Version, Banner: server.Banner}, nil
}

// Encode serializes a result. It stays well under the 4 KiB a termination
// message holds.
func Encode(r Result) ([]byte, error) {
	return json.Marshal(r)
}

// Decode parses a probe's termination message.
func Decode(message string) (Result, error) {
	var r Result
	if err := json.Unmarshal([]byte(message), &r); err != nil {
		return Result{}, fmt.Errorf("decoding the image probe result %q: %w", message, err)
	}
	if r.Flavor != FlavorMySQL && r.Flavor != FlavorMariaDB {
		return Result{}, fmt.Errorf("image probe reported an unknown flavor %q", r.Flavor)
	}
	if _, err := version.Parse(r.ServerVersion); err != nil {
		return Result{}, fmt.Errorf("image probe reported an invalid server version: %w", err)
	}
	return r, nil
}
