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

package cmd

import (
	"os"
	"testing"

	"github.com/logrusorgru/aurora/v4"
)

// TestMain turns colors off so tests can compare rendered text exactly. The
// root command's PersistentPreRunE would do the same for a non-terminal
// stdout, but most tests call commands below the root.
func TestMain(m *testing.M) {
	aurora.DefaultColorizer = aurora.New(aurora.WithColors(false))
	os.Exit(m.Run())
}
