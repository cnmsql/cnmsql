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

// Command manager is the in-pod instance manager for CNMySQL. It runs as PID1
// inside every MySQL pod, supervises mysqld, bootstraps and joins instances,
// drives GTID replication, and exposes a control API to the operator.
package main

import (
	"fmt"
	"os"

	"github.com/yyewolf/cnmysql/internal/cmd/manager"
)

func main() {
	if err := manager.NewRootCommand().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "manager: error:", err)
		os.Exit(1)
	}
}
