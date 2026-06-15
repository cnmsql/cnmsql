/*
Copyright 2026 The cloudnative-mysql Authors.

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

// Package scrapers hosts MySQL metric scrapers vendored from
// github.com/prometheus/mysqld_exporter (Apache-2.0). The Scrape* types and
// their support files are generated from the pinned git submodule under
// pkg/vendor; only runner.go and doc.go are hand-written. Run `go generate` (or
// `make generate-scrapers`) after bumping the submodule.
package scrapers

//go:generate go run ./gen
