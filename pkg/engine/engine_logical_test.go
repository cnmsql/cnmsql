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

package engine

import (
	"slices"
	"strings"
	"testing"
)

// Comment lines captured from real dumps taken in the published instance
// images (cnmsql-instance 8.0-5 / 8.4-5, cnmsql-mariadb-instance 11.4-4).
const (
	percona80Comments = `-- MySQL dump 10.13  Distrib 8.0.46-37, for Linux (x86_64)
--
-- Host: localhost    Database: shop
-- ------------------------------------------------------
-- Server version	8.0.46-37
--
-- Position to start replication or point-in-time recovery from
--
-- CHANGE MASTER TO MASTER_LOG_FILE='mysql-bin.000001', MASTER_LOG_POS=3204;
--
-- Current Database: ` + "`shop`" + `
--
-- Dump completed on 2026-09-25 16:29:14
`
	percona84Comments = `-- MySQL dump 10.13  Distrib 8.4.11-11, for Linux (x86_64)
-- Server version	8.4.11-11
-- Position to start replication or point-in-time recovery from
-- CHANGE REPLICATION SOURCE TO SOURCE_LOG_FILE='mysql-bin.000001', SOURCE_LOG_POS=3200;
-- Current Database: ` + "`shop`" + `
-- Dump completed on 2026-09-25 16:29:19
`
	mariadb114Comments = `-- MariaDB dump 10.19-11.4.13-MariaDB, for debian-linux-gnu (x86_64)
-- Server version	11.4.13-MariaDB-deb12-log
-- Position to start replication or point-in-time recovery from
-- CHANGE MASTER TO MASTER_LOG_FILE='mysql-bin.000001', MASTER_LOG_POS=2730;
-- Current Database: ` + "`shop`" + `
-- The deferred gtid setting for slave corresponding to the master-data CHANGE-MASTER follows
-- Preferably use GTID to start replication from GTID position:
-- SET GLOBAL gtid_slave_pos='0-1-13';
-- Dump completed on 2026-09-25 16:31:03
`
)

func TestLogicalToolBinaries(t *testing.T) {
	for _, tc := range []struct {
		flavor     Flavor
		dump, load string
	}{
		{FlavorMySQL, "mysqldump", "mysql"},
		{FlavorMariaDB, "mariadb-dump", "mariadb"},
	} {
		lt := MustForFlavor(tc.flavor).Logical()
		if lt.DumpBinary() != tc.dump || lt.LoadBinary() != tc.load {
			t.Errorf("%s: binaries = %q/%q, want %q/%q", tc.flavor, lt.DumpBinary(), lt.LoadBinary(), tc.dump, tc.load)
		}
		if lt.LoadBinary() != MustForFlavor(tc.flavor).Backup().SQLClientBinary() {
			t.Errorf("%s: LoadBinary must be the PITR SQL client", tc.flavor)
		}
	}
}

func TestLogicalDumpArgs(t *testing.T) {
	opts := func(v string) DumpOpts {
		return DumpOpts{
			DefaultsFile:  "/tmp/dump/client.cnf",
			Databases:     []string{"shop", "billing"},
			ExtraArgs:     []string{"--max-allowed-packet=1G"},
			ServerVersion: mustVersion(t, v),
		}
	}
	for _, tc := range []struct {
		name     string
		flavor   Flavor
		version  string
		want     []string
		unwanted []string
	}{
		{"mysql 8.0.25", FlavorMySQL, "8.0.25",
			[]string{"--master-data=2", "--set-gtid-purged=OFF"}, []string{"--source-data=2"}},
		{"mysql 8.0.26", FlavorMySQL, "8.0.26",
			[]string{"--source-data=2", "--set-gtid-purged=OFF"}, []string{"--master-data=2"}},
		{"mysql 8.4", FlavorMySQL, "8.4.11", []string{"--source-data=2"}, []string{"--master-data=2"}},
		{"mysql 9.x", FlavorMySQL, "9.6.0", []string{"--source-data=2"}, []string{"--master-data=2"}},
		{"mariadb 10.11", FlavorMariaDB, "10.11.19-MariaDB",
			[]string{"--master-data=2"}, []string{"--set-gtid-purged=OFF", "--gtid", "--source-data=2"}},
		{"mariadb 12.3", FlavorMariaDB, "12.3.3-MariaDB",
			[]string{"--master-data=2"}, []string{"--set-gtid-purged=OFF", "--gtid"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args, err := MustForFlavor(tc.flavor).Logical().DumpArgs(opts(tc.version))
			if err != nil {
				t.Fatal(err)
			}
			if args[0] != "--defaults-extra-file=/tmp/dump/client.cnf" {
				t.Errorf("first arg = %q, want the defaults file", args[0])
			}
			for _, w := range append(tc.want, "--single-transaction", "--routines", "--events", "--triggers",
				"--hex-blob", "--no-tablespaces", "--default-character-set=utf8mb4") {
				if !slices.Contains(args, w) {
					t.Errorf("missing %q in %v", w, args)
				}
			}
			for _, u := range tc.unwanted {
				if slices.Contains(args, u) {
					t.Errorf("unexpected %q in %v", u, args)
				}
			}
			// Extra args come after the operator's, then the explicit database list.
			tail := args[len(args)-4:]
			if !slices.Equal(tail, []string{"--max-allowed-packet=1G", "--databases", "shop", "billing"}) {
				t.Errorf("tail = %v", tail)
			}
			for _, a := range args {
				if strings.Contains(a, "password") {
					t.Errorf("argv must not carry a password: %q", a)
				}
			}
		})
	}
}

func TestLogicalDumpArgsRequireDefaultsFileAndDatabases(t *testing.T) {
	lt := MustForFlavor(FlavorMySQL).Logical()
	if _, err := lt.DumpArgs(DumpOpts{Databases: []string{"a"}}); err == nil {
		t.Error("expected an error without a defaults file")
	}
	if _, err := lt.DumpArgs(DumpOpts{DefaultsFile: "/x"}); err == nil {
		t.Error("expected an error without databases")
	}
}

func TestParseSnapshotPosition(t *testing.T) {
	for _, tc := range []struct {
		name     string
		flavor   Flavor
		comments string
		want     BinlogInfo
	}{
		{"percona 8.0 master spelling", FlavorMySQL, percona80Comments,
			BinlogInfo{File: "mysql-bin.000001", Position: 3204}},
		{"percona 8.4 source spelling", FlavorMySQL, percona84Comments,
			BinlogInfo{File: "mysql-bin.000001", Position: 3200}},
		{"mariadb deferred gtid", FlavorMariaDB, mariadb114Comments,
			BinlogInfo{File: "mysql-bin.000001", Position: 2730, GTIDSet: "0-1-13"}},
		{"mariadb without gtid comment", FlavorMariaDB, percona80Comments,
			BinlogInfo{File: "mysql-bin.000001", Position: 3204}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := MustForFlavor(tc.flavor).Logical().ParseSnapshotPosition(tc.comments)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestParseSnapshotPositionIgnoresUncommentedStatements(t *testing.T) {
	// Only commented coordinates count: an uncommented CHANGE MASTER would be a
	// dump taken with --master-data=1, which the facet never requests.
	_, err := MustForFlavor(FlavorMySQL).Logical().ParseSnapshotPosition(
		"CHANGE MASTER TO MASTER_LOG_FILE='mysql-bin.000001', MASTER_LOG_POS=4;\n")
	if err == nil {
		t.Fatal("expected an error without a commented position")
	}
}

func TestDumpAccountGrants(t *testing.T) {
	has := func(grants []AccountGrant, priv string) bool {
		for _, g := range grants {
			if g.On == "*.*" && slices.Contains(g.Privileges, priv) {
				return true
			}
		}
		return false
	}
	mysql := MustForFlavor(FlavorMySQL).Logical()
	for _, v := range []string{"8.0.46", "8.4.11", "9.6.0"} {
		g := mysql.DumpAccountGrants(mustVersion(t, v))
		for _, p := range []string{"SELECT", "SHOW VIEW", "TRIGGER", "EVENT", "RELOAD", "REPLICATION CLIENT"} {
			if !has(g, p) {
				t.Errorf("mysql %s: missing %s", v, p)
			}
		}
		for _, p := range []string{"PROCESS", "LOCK TABLES", "INSERT", "SUPER"} {
			if has(g, p) {
				t.Errorf("mysql %s: unexpected %s", v, p)
			}
		}
		r := mysql.DumpAccountRevokes(mustVersion(t, v))
		if len(r) != 1 || r[0].On != "mysql.*" || !slices.Equal(r[0].Privileges, []string{"SELECT"}) {
			t.Errorf("mysql %s: revokes = %+v", v, r)
		}
	}

	mariadb := MustForFlavor(FlavorMariaDB).Logical()
	for _, tc := range []struct {
		version     string
		showRoutine bool
	}{
		{"10.11.19-MariaDB", false},
		{"11.4.13-MariaDB", true},
		{"11.8.9-MariaDB", true},
		{"12.3.3-MariaDB", true},
	} {
		v := mustVersion(t, tc.version)
		g := mariadb.DumpAccountGrants(v)
		if !has(g, "BINLOG MONITOR") || has(g, "REPLICATION CLIENT") {
			t.Errorf("mariadb %s: want BINLOG MONITOR instead of REPLICATION CLIENT: %+v", tc.version, g)
		}
		if has(g, "SHOW CREATE ROUTINE") != tc.showRoutine {
			t.Errorf("mariadb %s: SHOW CREATE ROUTINE = %v, want %v", tc.version, !tc.showRoutine, tc.showRoutine)
		}
		if r := mariadb.DumpAccountRevokes(v); len(r) != 0 {
			t.Errorf("mariadb %s: revokes = %+v, want none", tc.version, r)
		}
	}
}
