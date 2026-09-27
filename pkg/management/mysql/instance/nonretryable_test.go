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

package instance

import (
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// A statement the server rejects fails every retry the same way, so it is
// non-retryable; a connection-level failure may be transient.
func TestPostInitStatementError(t *testing.T) {
	syntax := &mysql.MySQLError{Number: 1064, Message: "You have an error in your SQL syntax"}
	err := postInitStatementError(2, syntax)
	if !errors.Is(err, ErrNonRetryable) {
		t.Fatalf("server-rejected statement: %v, want ErrNonRetryable", err)
	}
	if !errors.Is(err, syntax) {
		t.Fatalf("error %v does not wrap the server error", err)
	}

	err = postInitStatementError(1, driver.ErrBadConn)
	if errors.Is(err, ErrNonRetryable) {
		t.Fatalf("connection failure: %v, want retryable", err)
	}
	if !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("error %v does not wrap the connection error", err)
	}
}
