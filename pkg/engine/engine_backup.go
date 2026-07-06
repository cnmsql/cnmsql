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
	"github.com/cnmsql/cnmsql/pkg/management/mysql/xtrabackup"
)

const (
	mysqlBackupBinary     = "xtrabackup"
	mysqlStreamBinary     = "xbstream"
	mysqlBinlogClient     = "mysqlbinlog"
	mysqlSQLClient        = "mysql"
	mysqlBinlogInfoFile   = "xtrabackup_binlog_info"
	mariadbBackupBinary   = "mariabackup"
	mariadbStreamBinary   = "mbstream"
	mariadbBinlogClient   = "mariadb-binlog"
	mariadbSQLClient      = "mariadb"
	mariadbBinlogInfoFile = "mariadb_backup_binlog_info"
)

// BackupOpts configures a physical backup stream.
type BackupOpts struct {
	TargetDir string
	Host      string
	Port      int
	Socket    string
	User      string
	Password  string
	Parallel  int
	Stream    bool
	Compress  bool
	ExtraArgs []string
}

func (o BackupOpts) toXtrabackup() xtrabackup.BackupOptions {
	return xtrabackup.BackupOptions{
		TargetDir: o.TargetDir,
		Host:      o.Host,
		Port:      o.Port,
		Socket:    o.Socket,
		User:      o.User,
		Password:  o.Password,
		Parallel:  o.Parallel,
		Stream:    o.Stream,
		Compress:  o.Compress,
		ExtraArgs: o.ExtraArgs,
	}
}

// BinlogInfo is the parsed content of the backup's binlog-info file.
type BinlogInfo struct {
	File     string
	Position int64
	GTIDSet  string
}

func fromXtrabackupBinlogInfo(info xtrabackup.BinlogInfo) BinlogInfo {
	return BinlogInfo{
		File:     info.File,
		Position: info.Position,
		GTIDSet:  info.GTIDSet,
	}
}

// BackupTool exposes the binary names and argument builders for physical backup,
// restore, stream extraction and point-in-time replay. The arg builders produce
// identical output for MySQL and MariaDB — the difference is the binary that
// executes them (xtrabackup vs mariabackup, xbstream vs mbstream, etc.).
type BackupTool interface {
	BackupBinary() string
	StreamBinary() string
	BinlogClientBinary() string
	SQLClientBinary() string
	// BinlogInfoFileName is the canonical binlog-info filename to write.
	BinlogInfoFileName() string
	// BinlogInfoFileNames lists the binlog-info filenames to try when reading, in
	// preference order. MariaBackup renamed this file from xtrabackup_binlog_info
	// to mariadb_backup_binlog_info in MariaDB 11.1, so a MariaDB backup taken on an
	// older server (e.g. 10.11) carries the legacy name; readers must accept both or
	// they silently see an empty anchor and replay from genesis.
	BinlogInfoFileNames() []string

	BackupArgs(opts BackupOpts) ([]string, error)
	ExtractArgs(targetDir string) ([]string, error)
	DecompressArgs(targetDir string) ([]string, error)
	PrepareArgs(targetDir string, extraArgs ...string) ([]string, error)
	CopyBackArgs(targetDir, dataDir string, extraArgs ...string) ([]string, error)
	ParseBinlogInfo(content string) (BinlogInfo, error)
}

type baseBackupTool struct{}

func (baseBackupTool) BackupArgs(opts BackupOpts) ([]string, error) {
	return xtrabackup.BackupArgs(opts.toXtrabackup())
}

func (baseBackupTool) ExtractArgs(targetDir string) ([]string, error) {
	return xtrabackup.ExtractArgs(targetDir)
}

func (baseBackupTool) DecompressArgs(targetDir string) ([]string, error) {
	return xtrabackup.DecompressArgs(targetDir)
}

func (baseBackupTool) PrepareArgs(targetDir string, extraArgs ...string) ([]string, error) {
	return xtrabackup.PrepareArgs(targetDir, extraArgs...)
}

func (baseBackupTool) CopyBackArgs(targetDir, dataDir string, extraArgs ...string) ([]string, error) {
	return xtrabackup.CopyBackArgs(targetDir, dataDir, extraArgs...)
}

func (baseBackupTool) ParseBinlogInfo(content string) (BinlogInfo, error) {
	info, err := xtrabackup.ParseBinlogInfo(content)
	if err != nil {
		return BinlogInfo{}, err
	}
	return fromXtrabackupBinlogInfo(info), nil
}

type mysqlBackupTool struct{ baseBackupTool }

func (mysqlBackupTool) BackupBinary() string          { return mysqlBackupBinary }
func (mysqlBackupTool) StreamBinary() string          { return mysqlStreamBinary }
func (mysqlBackupTool) BinlogClientBinary() string    { return mysqlBinlogClient }
func (mysqlBackupTool) SQLClientBinary() string       { return mysqlSQLClient }
func (mysqlBackupTool) BinlogInfoFileName() string    { return mysqlBinlogInfoFile }
func (mysqlBackupTool) BinlogInfoFileNames() []string { return []string{mysqlBinlogInfoFile} }

type mariadbBackupTool struct{ baseBackupTool }

func (mariadbBackupTool) BackupBinary() string       { return mariadbBackupBinary }
func (mariadbBackupTool) StreamBinary() string       { return mariadbStreamBinary }
func (mariadbBackupTool) BinlogClientBinary() string { return mariadbBinlogClient }
func (mariadbBackupTool) SQLClientBinary() string    { return mariadbSQLClient }
func (mariadbBackupTool) BinlogInfoFileName() string { return mariadbBinlogInfoFile }

// MariaBackup < 11.1 writes xtrabackup_binlog_info; 11.1+ writes
// mariadb_backup_binlog_info. Prefer the modern name, fall back to the legacy one.
func (mariadbBackupTool) BinlogInfoFileNames() []string {
	return []string{mariadbBinlogInfoFile, mysqlBinlogInfoFile}
}
