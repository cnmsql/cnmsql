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
	c.SetBackupConfig(BackupConfig{
		DataDir: "/var/lib/mysql",
		Socket:  "/run/mysqld.sock",
		User:    "cloudnative-mysql_backup",
	})
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
