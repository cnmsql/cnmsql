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

package backupworker

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// Failure is a worker error with a reason the operator puts on the object's
// status. Unchanged says the target was left as it was (restores only).
type Failure struct {
	Reason    string
	Unchanged bool
	Err       error
}

func (f *Failure) Error() string { return f.Err.Error() }
func (f *Failure) Unwrap() error { return f.Err }

// ReportFailure writes err's reason, if it has one, as the container's
// termination message at path. It is best effort: the worker is exiting anyway.
func ReportFailure(path string, err error) {
	var f *Failure
	if !errors.As(err, &f) || f.Reason == "" {
		return
	}
	WriteTerminationMessage(path, TerminationMessage{
		Reason:    f.Reason,
		Message:   f.Error(),
		Unchanged: f.Unchanged,
	})
}

// maxRefusalBytes bounds the error body read from an instance manager.
const maxRefusalBytes = 64 << 10

// ReadRefusal reads an instance manager's error response. A body that is not
// a reason error (a proxy's page, an older manager's plain text) becomes the
// message. It does not close the body.
func ReadRefusal(resp *http.Response) webserver.ReasonErrorBody {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxRefusalBytes))
	var body webserver.ReasonErrorBody
	if json.Unmarshal(raw, &body) != nil || body.Error == "" {
		body.Error = string(bytes.TrimSpace(raw))
	}
	if body.Error == "" {
		body.Error = http.StatusText(resp.StatusCode)
	}
	return body
}

// ManagerPredatesEndpoint reports whether an instance manager answered as one
// that does not serve the endpoint yet: it is older than the operator.
func ManagerPredatesEndpoint(status int) bool {
	return status == http.StatusNotFound || status == http.StatusMethodNotAllowed
}
