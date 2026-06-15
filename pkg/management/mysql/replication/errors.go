/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
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
	// errFunctionAlreadyExists is ER_UDF_EXISTS. Percona can return it when
	// INSTALL PLUGIN is replayed for an already-loaded semi-sync plugin.
	errFunctionAlreadyExists = 1125
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
