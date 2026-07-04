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

package user

import (
	"errors"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// Server error numbers we special-case. MariaDB reuses MySQL's numbering.
const (
	// errNonexistingGrant is ER_NONEXISTING_GRANT ("There is no such grant
	// defined for user ..."), returned for a REVOKE of a privilege the account
	// does not hold.
	errNonexistingGrant = 1141
	// errNonexistingTableGrant is ER_NONEXISTING_TABLE_GRANT, the table/schema
	// analogue.
	errNonexistingTableGrant = 1147
)

// mysqlErrorNumber returns the server error number for an error, or 0 if it is
// not a *mysql.MySQLError.
func mysqlErrorNumber(err error) uint16 {
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) {
		return myErr.Number
	}
	return 0
}

// isNonexistingGrant reports whether err means the REVOKE targeted a grant the
// account does not hold. Without REVOKE IF EXISTS (MariaDB), re-applying a
// revoke that already took effect returns this, so it is treated as a no-op to
// keep reconciliation idempotent.
func isNonexistingGrant(err error) bool {
	switch mysqlErrorNumber(err) {
	case errNonexistingGrant, errNonexistingTableGrant:
		return true
	default:
		return false
	}
}

// isRevoke reports whether stmt is a REVOKE statement.
func isRevoke(stmt string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(stmt)), "REVOKE ")
}
