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

package replication

import (
	"errors"
	"testing"
)

func TestIsReplicationMetadataError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "relay log corruption",
			err:  errors.New("Could not parse relay log event entry"),
			want: true,
		},
		{
			name: "relay log open failure",
			err:  errors.New("Failed to open the relay log '/var/lib/mysql/relay-bin.000123'"),
			want: true,
		},
		{
			name: "relay log initialization",
			err:  errors.New("Error initializing relay log position: Could not find target log file"),
			want: true,
		},
		{
			name: "master info file",
			err:  errors.New("Master information file not found"),
			want: true,
		},
		{
			name: "generic connection error",
			err:  errors.New("connection refused"),
			want: false,
		},
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := IsReplicationMetadataError(tt.err); got != tt.want {
				t.Errorf("IsReplicationMetadataError() = %v, want %v", got, tt.want)
			}
		})
	}
}
