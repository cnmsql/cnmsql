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

import (
	"encoding/json"
	"errors"
	"os"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/backupworker"
)

// failure is a worker error with a reason the operator can put on the Backup.
type failure struct {
	reason string
	err    error
}

func (f *failure) Error() string { return f.err.Error() }
func (f *failure) Unwrap() error { return f.err }

// terminationLogPath is the kubelet's default terminationMessagePath.
var terminationLogPath = "/dev/termination-log"

// reportFailure publishes err's reason, if it has one, as the container's
// termination message. It is best effort: the worker is exiting anyway.
func reportFailure(err error) {
	var f *failure
	if !errors.As(err, &f) || f.reason == "" {
		return
	}
	msg := backupworker.TerminationMessage{Reason: f.reason, Message: f.Error()}
	payload, jerr := json.Marshal(msg)
	for jerr == nil && len(payload) > backupworker.MaxTerminationMessageBytes && len(msg.Message) > 0 {
		msg.Message = msg.Message[:len(msg.Message)/2]
		payload, jerr = json.Marshal(msg)
	}
	if jerr != nil {
		return
	}
	_ = os.WriteFile(terminationLogPath, payload, 0o644)
}
