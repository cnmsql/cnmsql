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

// Package xtrabackup builds the command lines for Percona XtraBackup operations
// (backup, prepare, copy-back) and parses its metadata. Provisioning a replica
// is a physical clone: back up the primary's data files, prepare them into a
// consistent state, copy them into the replica's data directory, then resume
// GTID replication from the backup's position.
package xtrabackup

import (
	"fmt"
	"strconv"
	"strings"
)

// BackupOptions configures `xtrabackup --backup`.
type BackupOptions struct {
	// TargetDir is where the backup is written.
	TargetDir string
	// Host, Port and Socket locate the source server (Socket takes precedence).
	Host   string
	Port   int
	Socket string
	// User and Password authenticate to the source.
	User     string
	Password string
	// Parallel sets the number of copy threads (0 = tool default).
	Parallel int
	// Stream, when true, writes the backup to stdout as an xbstream archive
	// instead of populating TargetDir. TargetDir is still required: xtrabackup
	// uses it as a working directory for transient metadata.
	Stream bool
	// Compress enables on-the-fly compression of the stream. It requires the
	// matching decompression tooling (qpress/zstd) on both ends.
	Compress bool
	// ExtraArgs are appended verbatim.
	ExtraArgs []string
}

// BackupArgs builds the argument list for `xtrabackup --backup`.
func BackupArgs(o BackupOptions) ([]string, error) {
	if o.TargetDir == "" {
		return nil, fmt.Errorf("xtrabackup: target dir is required")
	}
	if o.User == "" {
		return nil, fmt.Errorf("xtrabackup: user is required")
	}

	args := []string{"--backup", "--target-dir=" + o.TargetDir, "--user=" + o.User}
	if o.Password != "" {
		args = append(args, "--password="+o.Password)
	}
	if o.Socket != "" {
		args = append(args, "--socket="+o.Socket)
	} else if o.Host != "" {
		args = append(args, "--host="+o.Host)
		if o.Port != 0 {
			args = append(args, "--port="+strconv.Itoa(o.Port))
		}
	}
	if o.Parallel > 0 {
		args = append(args, "--parallel="+strconv.Itoa(o.Parallel))
	}
	if o.Stream {
		args = append(args, "--stream=xbstream")
	}
	if o.Compress {
		args = append(args, "--compress")
	}
	return append(args, o.ExtraArgs...), nil
}

// ExtractArgs builds the `xbstream -x` argument list that extracts a streamed
// archive (read from stdin) into targetDir.
func ExtractArgs(targetDir string) ([]string, error) {
	if targetDir == "" {
		return nil, fmt.Errorf("xtrabackup: target dir is required")
	}
	return []string{"-x", "--directory=" + targetDir}, nil
}

// DecompressArgs builds the `xtrabackup --decompress` argument list, used after
// extracting a compressed stream.
func DecompressArgs(targetDir string) ([]string, error) {
	if targetDir == "" {
		return nil, fmt.Errorf("xtrabackup: target dir is required")
	}
	return []string{"--decompress", "--target-dir=" + targetDir}, nil
}

// PrepareArgs builds the argument list for `xtrabackup --prepare`.
func PrepareArgs(targetDir string, extraArgs ...string) ([]string, error) {
	if targetDir == "" {
		return nil, fmt.Errorf("xtrabackup: target dir is required")
	}
	return append([]string{"--prepare", "--target-dir=" + targetDir}, extraArgs...), nil
}

// CopyBackArgs builds the argument list for `xtrabackup --copy-back`, which
// restores a prepared backup into an empty data directory.
func CopyBackArgs(targetDir, dataDir string, extraArgs ...string) ([]string, error) {
	if targetDir == "" || dataDir == "" {
		return nil, fmt.Errorf("xtrabackup: target dir and data dir are required")
	}
	return append([]string{
		"--copy-back",
		"--target-dir=" + targetDir,
		"--datadir=" + dataDir,
	}, extraArgs...), nil
}

// BinlogInfo is the parsed content of the xtrabackup_binlog_info file produced
// by a backup.
type BinlogInfo struct {
	// File and Position are the binary log coordinates of the backup.
	File     string
	Position int64
	// GTIDSet is the set of executed GTIDs at the backup point; empty when the
	// source did not have GTIDs enabled.
	GTIDSet string
}

// ParseBinlogInfo parses the tab-separated xtrabackup_binlog_info content:
//
//	<file>\t<position>\t<gtid-set>
//
// The GTID set may span multiple lines (one per source UUID); everything after
// the second tab is treated as the set, with newlines normalised to spaces.
func ParseBinlogInfo(content string) (BinlogInfo, error) {
	trimmed := strings.TrimRight(content, "\n")
	if trimmed == "" {
		return BinlogInfo{}, fmt.Errorf("xtrabackup: empty binlog info")
	}

	// Split into at most three parts on the first two tabs.
	parts := strings.SplitN(trimmed, "\t", 3)
	info := BinlogInfo{File: parts[0]}

	if len(parts) >= 2 {
		pos, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil {
			return BinlogInfo{}, fmt.Errorf("xtrabackup: invalid binlog position %q: %w", parts[1], err)
		}
		info.Position = pos
	}
	if len(parts) == 3 {
		// Multiple UUID ranges are newline-separated in the file; GTID sets use
		// commas as separators, so join the lines with commas.
		fields := strings.Fields(parts[2])
		info.GTIDSet = strings.Join(fields, "")
	}
	return info, nil
}
