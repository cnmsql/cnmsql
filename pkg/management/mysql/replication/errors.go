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

package replication

import (
	"errors"

	"github.com/go-sql-driver/mysql"
)

// MySQL server error numbers we special-case.
const (
	// errPluginInstalled is ER_PLUGIN_INSTALLED.
	errPluginInstalled = 1968
	// errUnknownSystemVariable is ER_UNKNOWN_SYSTEM_VARIABLE.
	errUnknownSystemVariable = 1193
)

// mysqlErrorNumber returns the MySQL server error number for an error, or 0 if
// it is not a *mysql.MySQLError.
func mysqlErrorNumber(err error) uint16 {
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) {
		return myErr.Number
	}
	return 0
}
