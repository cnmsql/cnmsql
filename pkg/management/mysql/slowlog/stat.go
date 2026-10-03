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

package slowlog

import (
	"io/fs"
	"os"
	"syscall"
)

// fileStat is the part of a stat the tailer and watchdog use.
type fileStat struct {
	size int64
	ino  uint64
}

func toStat(info fs.FileInfo) fileStat {
	st := fileStat{size: info.Size()}
	if sys, ok := info.Sys().(*syscall.Stat_t); ok {
		st.ino = sys.Ino
	}
	return st
}

// statPath stats path, following symlinks. ok is false when it does not exist
// or cannot be read.
func statPath(path string) (fileStat, bool) {
	info, err := os.Stat(path)
	if err != nil {
		return fileStat{}, false
	}
	return toStat(info), true
}

func statFile(f *os.File) (fileStat, error) {
	info, err := f.Stat()
	if err != nil {
		return fileStat{}, err
	}
	return toStat(info), nil
}
