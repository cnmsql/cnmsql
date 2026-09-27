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
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"
)

// ExitCodeNonRetryable is the manager binary's exit code for a failure that a
// retry cannot fix. The operator's bootstrap Jobs fail on it at once through a
// podFailurePolicy rule instead of spending their whole backoff (about eleven
// minutes at the default limit) re-running the same doomed attempt.
const ExitCodeNonRetryable = 3

// ErrNonRetryable marks a failure that fails the same way on every attempt,
// such as user-supplied SQL the server rejects.
var ErrNonRetryable = errors.New("non-retryable failure")

// postInitStatementError wraps the failure of the n-th (1-based) postInitSQL
// statement. The server rejecting the statement is non-retryable: the same SQL
// is re-run on every attempt. Any other failure (a lost connection, a
// cancelled context) may be transient and is left retryable.
func postInitStatementError(n int, err error) error {
	if _, ok := errors.AsType[*mysql.MySQLError](err); ok {
		return fmt.Errorf("%w: postInitSQL statement %d failed: %w", ErrNonRetryable, n, err)
	}
	return fmt.Errorf("postInitSQL statement %d failed: %w", n, err)
}
