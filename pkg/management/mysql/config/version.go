/*
Copyright 2026 The CNMySQL Authors.

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

package config

import (
	"fmt"
	"strconv"
	"strings"
)

// version is a parsed MySQL major.minor version. The patch level is ignored for
// the keyword decisions we make.
type version struct {
	major int
	minor int
}

// parseVersion extracts the major and minor version from a MySQL version
// string such as "8.0.36", "5.7.44-48" or "8.4". A leading "v" is tolerated.
func parseVersion(v string) (version, error) {
	v = strings.TrimSpace(v)
	v = strings.TrimPrefix(v, "v")
	if v == "" {
		return version{}, fmt.Errorf("empty MySQL version")
	}

	// Drop any vendor suffix after a dash (e.g. "5.7.44-48").
	if idx := strings.IndexByte(v, '-'); idx != -1 {
		v = v[:idx]
	}

	parts := strings.Split(v, ".")
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return version{}, fmt.Errorf("invalid MySQL version %q: %w", v, err)
	}

	minor := 0
	if len(parts) > 1 {
		minor, err = strconv.Atoi(parts[1])
		if err != nil {
			return version{}, fmt.Errorf("invalid MySQL version %q: %w", v, err)
		}
	}

	return version{major: major, minor: minor}, nil
}

// atLeast reports whether the version is greater than or equal to major.minor.
func (v version) atLeast(major, minor int) bool {
	if v.major != major {
		return v.major > major
	}
	return v.minor >= minor
}
