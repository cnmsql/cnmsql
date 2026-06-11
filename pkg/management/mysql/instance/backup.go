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
	"errors"
	"io"
	"os"
	"os/exec"

	"github.com/yyewolf/cnmysql/pkg/management/mysql/xtrabackup"
)

// BackupConfig configures the streaming physical backup an instance serves to
// joining replicas. XtraBackup must run where the data files live, so the
// backup is always taken locally and streamed to the caller.
type BackupConfig struct {
	// XtrabackupPath is the xtrabackup binary (default "xtrabackup").
	XtrabackupPath string
	// DataDir is the data directory to back up.
	DataDir string
	// Socket connects xtrabackup to the local mysqld for locking and binlog
	// coordinates.
	Socket string
	// User and Password authenticate the local backup account.
	User     string
	Password string
	// WorkDir is a writable scratch directory xtrabackup uses for transient
	// metadata while streaming (default os.TempDir()).
	WorkDir string
	// Compress streams a compressed archive (requires qpress/zstd on both ends).
	Compress bool
	// Parallel sets the number of copy threads (0 = tool default).
	Parallel int
}

func (c *BackupConfig) applyDefaults() {
	if c.XtrabackupPath == "" {
		c.XtrabackupPath = defaultXtrabackupBinary
	}
	if c.WorkDir == "" {
		c.WorkDir = os.TempDir()
	}
}

// SetBackupConfig enables the streaming backup endpoint on the controller. With
// it unset, BackupStream returns an error and the control API still advertises
// the route (the handler reports the misconfiguration).
func (c *Controller) SetBackupConfig(cfg BackupConfig) {
	cfg.applyDefaults()
	c.backup = &cfg
}

// BackupStream runs `xtrabackup --backup --stream=xbstream` against the local
// data directory and copies the archive to w. It satisfies
// webserver.BackupStreamer.
func (c *Controller) BackupStream(ctx context.Context, w io.Writer) error {
	if c.backup == nil {
		return errors.New("backup streaming is not configured on this instance")
	}
	args, err := xtrabackup.BackupArgs(xtrabackup.BackupOptions{
		TargetDir: c.backup.WorkDir,
		Socket:    c.backup.Socket,
		User:      c.backup.User,
		Password:  c.backup.Password,
		Parallel:  c.backup.Parallel,
		Stream:    true,
		Compress:  c.backup.Compress,
	})
	if err != nil {
		return err
	}

	cmd := exec.CommandContext(ctx, c.backup.XtrabackupPath, args...)
	cmd.Stdout = w
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}

// Ensure Controller advertises the optional streaming backup capability.
var _ interface {
	BackupStream(context.Context, io.Writer) error
} = (*Controller)(nil)
