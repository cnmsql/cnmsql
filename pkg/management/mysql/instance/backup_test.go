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

package instance

import (
	"context"
	"io"
	"testing"
)

func TestBackupStreamUnconfiguredErrors(t *testing.T) {
	c := &Controller{}
	if err := c.BackupStream(context.Background(), io.Discard); err == nil {
		t.Fatal("expected an error when backup streaming is not configured")
	}
}

func TestSetBackupConfigDefaults(t *testing.T) {
	c := &Controller{}
	c.SetBackupConfig(BackupConfig{DataDir: "/var/lib/mysql", Socket: "/run/mysqld.sock", User: "cnmysql_backup"})
	if c.backup == nil {
		t.Fatal("backup config not set")
	}
	if c.backup.XtrabackupPath != "xtrabackup" {
		t.Errorf("xtrabackup path default = %q", c.backup.XtrabackupPath)
	}
	if c.backup.WorkDir == "" {
		t.Error("work dir default not applied")
	}
}
