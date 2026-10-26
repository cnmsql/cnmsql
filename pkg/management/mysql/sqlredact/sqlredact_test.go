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

package sqlredact

import "testing"

// SQL copies statements into error messages and logs. The table pins the
// MySQL-style redaction of password literals: the syntax prefix stays visible
// for debugging, the literal becomes <secret>.
func TestSQLRedactsPasswordLiterals(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{
			"CREATE USER IF NOT EXISTS 'app'@'%' IDENTIFIED BY 's3cr3t'",
			"CREATE USER IF NOT EXISTS 'app'@'%' IDENTIFIED BY <secret>",
		},
		{
			"ALTER USER 'app'@'10.0.0.1' IDENTIFIED BY 's3cr3t' REQUIRE X509",
			"ALTER USER 'app'@'10.0.0.1' IDENTIFIED BY <secret> REQUIRE X509",
		},
		{
			`ALTER USER 'a'@'%' IDENTIFIED BY "p\"w"`,
			"ALTER USER 'a'@'%' IDENTIFIED BY <secret>",
		},
		{
			"CREATE USER 'a'@'%' IDENTIFIED BY 's3cr''et'",
			"CREATE USER 'a'@'%' IDENTIFIED BY <secret>",
		},
		{
			"CREATE USER a IDENTIFIED BY PASSWORD '*94BDCEBE19083CE2A1F959FD02F964C7AF4CFC29'",
			"CREATE USER a IDENTIFIED BY PASSWORD <secret>",
		},
		{
			"CREATE USER a IDENTIFIED WITH caching_sha2_password BY 's3cr3t'",
			"CREATE USER a IDENTIFIED WITH caching_sha2_password BY <secret>",
		},
		{
			// MariaDB chain authentication.
			"CREATE USER a IDENTIFIED VIA mysql_native_password USING 'hash' OR pam USING 's3cr3t'",
			"CREATE USER a IDENTIFIED VIA mysql_native_password USING <secret> OR pam USING <secret>",
		},
		{
			"SET PASSWORD FOR 'a'@'%' = 's3cr3t'",
			"SET PASSWORD FOR 'a'@'%' = <secret>",
		},
		{
			"CHANGE REPLICATION SOURCE TO SOURCE_HOST='h', SOURCE_PASSWORD='s3cr3t', SOURCE_PORT=3306",
			"CHANGE REPLICATION SOURCE TO SOURCE_HOST='h', SOURCE_PASSWORD=<secret>, SOURCE_PORT=3306",
		},
		{
			"CHANGE REPLICATION SOURCE TO SOURCE_PASSWORD = 's3cr3t'",
			"CHANGE REPLICATION SOURCE TO SOURCE_PASSWORD = <secret>",
		},
		{
			"CHANGE MASTER TO MASTER_HOST='h', MASTER_PASSWORD='s3cr3t', MASTER_PORT=3306",
			"CHANGE MASTER TO MASTER_HOST='h', MASTER_PASSWORD=<secret>, MASTER_PORT=3306",
		},
		{
			"START REPLICA USER='r' PASSWORD='s3cr3t'",
			"START REPLICA USER='r' PASSWORD=<secret>",
		},
		{
			// Unrelated statements pass through untouched.
			"GRANT SELECT ON db.* TO 'app'@'%'",
			"GRANT SELECT ON db.* TO 'app'@'%'",
		},
		{
			"SELECT * FROM t WHERE name = 'password'",
			"SELECT * FROM t WHERE name = 'password'",
		},
	} {
		if got := SQL(tc.in); got != tc.want {
			t.Errorf("SQL(%q)\n got %q\nwant %q", tc.in, got, tc.want)
		}
	}
}
