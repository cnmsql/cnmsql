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

// Package scrapers hosts MySQL metric scrapers vendored from
// github.com/prometheus/mysqld_exporter (Apache-2.0). The Scrape* types and
// their support files are generated from the pinned git submodule under
// pkg/vendor; only runner.go and doc.go are hand-written. Run `go generate` (or
// `make generate-scrapers`) after bumping the submodule.
package scrapers

//go:generate go run ./gen
