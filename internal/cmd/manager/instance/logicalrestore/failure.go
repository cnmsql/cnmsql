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

package logicalrestore

import (
	"errors"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/backupworker"
)

// failure is a worker error with a reason the operator can put on the
// LogicalRestore, and whether the target was left unchanged.
type failure struct {
	reason    string
	unchanged bool
	err       error
}

func (f *failure) Error() string { return f.err.Error() }
func (f *failure) Unwrap() error { return f.err }

// unchanged is a failure that happened before the target changed anything.
func unchanged(reason string, err error) error {
	return &failure{reason: reason, unchanged: true, err: err}
}

// changed is a failure after the target may have dropped or loaded data.
func changed(reason string, err error) error {
	return &failure{reason: reason, err: err}
}

// terminationLogPath is where the termination message goes; tests override it.
var terminationLogPath = backupworker.TerminationLogPath

// reportFailure publishes err's reason, if it has one, as the container's
// termination message.
func reportFailure(err error) {
	var f *failure
	if !errors.As(err, &f) || f.reason == "" {
		return
	}
	backupworker.WriteTerminationMessage(terminationLogPath, backupworker.TerminationMessage{
		Reason:    f.reason,
		Message:   f.Error(),
		Unchanged: f.unchanged,
	})
}
