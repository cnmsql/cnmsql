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

package restore

import (
	"testing"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// TestToolFlagsDefaultEmpty guards the regression that broke MariaDB backups:
// the backup-tool flags must default to empty so the instance manager falls
// back to the engine's flavor-appropriate binary. A non-empty default (e.g.
// "xtrabackup") would pin every flavor to the MySQL tool.
func TestToolFlagsDefaultEmpty(t *testing.T) {
	cmd := NewCommand()
	for _, name := range []string{"xtrabackup", "xbstream", "mysqlbinlog", "mysql"} {
		f := cmd.Flags().Lookup(name)
		if f == nil {
			t.Fatalf("flag --%s not found", name)
		}
		if f.DefValue != "" {
			t.Errorf("--%s default = %q, want empty (engine selects the tool)", name, f.DefValue)
		}
	}
}

func TestBinlogStoreFromEnvFallsBackToBase(t *testing.T) {
	t.Setenv(objectstore.EnvBucket, "backups")
	t.Setenv(objectstore.EnvPath, "base")
	client, store, err := binlogStoreFromEnv(true)
	if err != nil {
		t.Fatal(err)
	}
	if client != nil {
		t.Fatal("no binlog env: the base-backup client is reused, so none is built")
	}
	if store.Bucket != "backups" || store.Path != "base" {
		t.Fatalf("store = %+v, want the base bucket/path", store)
	}
}

func TestBinlogStoreFromEnvSeparate(t *testing.T) {
	t.Setenv(objectstore.EnvBucket, "backups")
	t.Setenv(objectstore.BinlogEnvName(objectstore.EnvEndpoint), "http://127.0.0.1:9")
	t.Setenv(objectstore.BinlogEnvName(objectstore.EnvBucket), "binlogs")
	t.Setenv(objectstore.BinlogEnvName(objectstore.EnvPath), "archive")
	client, store, err := binlogStoreFromEnv(true)
	if err != nil {
		t.Fatal(err)
	}
	if client == nil {
		t.Fatal("a separate binlog store needs its own client")
	}
	if store.Bucket != "binlogs" || store.Path != "archive" {
		t.Fatalf("store = %+v, want the binlog bucket/path", store)
	}
}

func TestBinlogStoreFromEnvSkippedWithoutReplay(t *testing.T) {
	t.Setenv(objectstore.EnvBucket, "backups")
	t.Setenv(objectstore.BinlogEnvName(objectstore.EnvEndpoint), "http://127.0.0.1:9")
	t.Setenv(objectstore.BinlogEnvName(objectstore.EnvBucket), "binlogs")
	client, _, err := binlogStoreFromEnv(false)
	if err != nil {
		t.Fatal(err)
	}
	if client != nil {
		t.Fatal("a restore without replay must not build a client for the archive store")
	}
}
