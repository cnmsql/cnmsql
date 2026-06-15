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
	"errors"
	"io"
	"os"
	"os/exec"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/xtrabackup"
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
	cmd.Stderr = newProcessLogWriter(logf.FromContext(ctx).WithName("xtrabackup").WithValues(
		"instance", c.name,
		"dataDir", c.backup.DataDir,
	), "stderr")
	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}

// Ensure Controller advertises the optional streaming backup capability.
var _ interface {
	BackupStream(context.Context, io.Writer) error
} = (*Controller)(nil)
