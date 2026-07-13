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

package heartbeat

import (
	"errors"

	"github.com/go-sql-driver/mysql"
)

// MySQL server error numbers we special-case.
const (
	// errUnknownDatabase is ER_BAD_DB_ERROR.
	errUnknownDatabase = 1049
	// errNoSuchTable is ER_NO_SUCH_TABLE.
	errNoSuchTable = 1146
	// errUnknownSystemVariable is ER_UNKNOWN_SYSTEM_VARIABLE. MariaDB answers it
	// for super_read_only, which is a MySQL-only variable.
	errUnknownSystemVariable = 1193
)

// isMissingTable reports whether err says the heartbeat table is not there yet.
// A replica reaches that state legitimately: it can be polled before any primary
// has stamped the table into existence and replicated the DDL to it.
func isMissingTable(err error) bool {
	var myErr *mysql.MySQLError
	if !errors.As(err, &myErr) {
		return false
	}
	return myErr.Number == errNoSuchTable || myErr.Number == errUnknownDatabase
}
