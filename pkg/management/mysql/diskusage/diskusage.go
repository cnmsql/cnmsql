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

// Package diskusage reports filesystem-level space usage for a path, the way
// `df` does. It backs both the instance's storage metrics and the operator's
// StoragePressure condition, which read the same statfs so the two never
// disagree about how full a data volume is.
package diskusage

import "syscall"

// Usage is a point-in-time snapshot of a filesystem's space, in bytes.
type Usage struct {
	// UsedBytes is capacity minus all free blocks (including the root-reserved
	// blocks), so UsedBytes + free == CapacityBytes. This is the "Used" column df
	// prints.
	UsedBytes int64
	// CapacityBytes is the total size of the filesystem.
	CapacityBytes int64
	// AvailableBytes is the space free to an unprivileged writer (excludes the
	// root-reserved blocks). mysqld writes as an unprivileged user, so this is the
	// space it can actually consume — always <= CapacityBytes - UsedBytes.
	AvailableBytes int64
}

// Of statfs's path and returns its space usage. It returns the syscall error
// unchanged when the path cannot be stat'd (e.g. it does not exist).
func Of(path string) (Usage, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return Usage{}, err
	}
	bsize := int64(st.Bsize)
	capacity := int64(st.Blocks) * bsize
	return Usage{
		UsedBytes:      capacity - int64(st.Bfree)*bsize,
		CapacityBytes:  capacity,
		AvailableBytes: int64(st.Bavail) * bsize,
	}, nil
}

// Ratio is UsedBytes/CapacityBytes in [0,1]. It returns 0 for a zero-capacity
// snapshot (an unreadable or not-yet-observed volume) so callers treat "no data"
// as "no pressure" rather than dividing by zero.
func (u Usage) Ratio() float64 {
	if u.CapacityBytes <= 0 {
		return 0
	}
	return float64(u.UsedBytes) / float64(u.CapacityBytes)
}
