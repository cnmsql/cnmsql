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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadRefusal(t *testing.T) {
	for _, tc := range []struct {
		name, body, reason, message string
		status                      int
	}{
		{"a reason error", `{"reason":"NotPrimary","error":"prod-2 is read-only"}`,
			"NotPrimary", "prod-2 is read-only", http.StatusConflict},
		{"plain text", "upstream timed out\n", "", "upstream timed out", http.StatusBadGateway},
		{"no body", "", "", "Not Implemented", http.StatusNotImplemented},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			rec.WriteHeader(tc.status)
			_, _ = rec.WriteString(tc.body)
			got := ReadRefusal(rec.Result())
			if got.Reason != tc.reason || got.Error != tc.message {
				t.Errorf("refusal = %+v, want reason %q, error %q", got, tc.reason, tc.message)
			}
		})
	}
}

func TestReportFailureWritesTheReason(t *testing.T) {
	path := filepath.Join(t.TempDir(), "termination-log")
	ReportFailure(path, errors.New("no reason"))
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("an error without a reason wrote a termination message")
	}

	failure := &Failure{Reason: "DatabaseNotEmpty", Unchanged: true, Err: errors.New("shop holds tables")}
	ReportFailure(path, fmt.Errorf("wrapped: %w", failure))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var msg TerminationMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		t.Fatal(err)
	}
	if msg.Reason != "DatabaseNotEmpty" || !msg.Unchanged || !strings.Contains(msg.Message, "shop holds tables") {
		t.Errorf("message = %+v", msg)
	}
}
