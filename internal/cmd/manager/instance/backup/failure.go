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

package backup

import "github.com/cnmsql/cnmsql/pkg/management/mysql/backupworker"

// terminationLogPath is where the termination message goes; tests override it.
var terminationLogPath = backupworker.TerminationLogPath

// reportFailure publishes err's reason, if it has one, as the container's
// termination message.
func reportFailure(err error) { backupworker.ReportFailure(terminationLogPath, err) }
