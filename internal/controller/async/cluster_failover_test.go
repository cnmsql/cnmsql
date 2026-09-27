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

package async

import (
	"testing"

	"github.com/cnmsql/cnmsql/internal/controller/topology"
)

// TestPrimaryUnhealthyReason pins the trigger reasons the failover log line
// reports. The action ("Failing over primary from=X to=Y") is useless without
// the cause: a Pod that is not ready, an instance manager that stopped
// answering, or an instance that no longer acts as primary are different
// incidents with different runbooks.
func TestPrimaryUnhealthyReason(t *testing.T) {
	t.Parallel()

	primary := drainPrimary
	for _, tc := range []struct {
		name     string
		observed topology.FailoverState
		want     string
	}{
		{
			name: "status endpoint unreachable",
			observed: topology.FailoverState{
				PrimaryName:   primary,
				InstanceNames: []string{primary, drainReplica},
				Instances: map[string]topology.FailoverInstance{
					drainReplica: {Ready: true, Replica: true},
				},
			},
			want: "instance manager status is unreachable",
		},
		{
			name: "pod not ready",
			observed: topology.FailoverState{
				PrimaryName:   primary,
				InstanceNames: []string{primary, drainReplica},
				Instances: map[string]topology.FailoverInstance{
					primary:      {Ready: false, Primary: true, Role: "primary"},
					drainReplica: {Ready: true, Replica: true},
				},
			},
			want: "pod is not ready",
		},
		{
			name: "pod terminating",
			observed: topology.FailoverState{
				PrimaryName:   primary,
				InstanceNames: []string{primary, drainReplica},
				Terminating:   []string{primary},
				Instances: map[string]topology.FailoverInstance{
					primary:      {Ready: true, Primary: true, Role: "primary"},
					drainReplica: {Ready: true, Replica: true},
				},
			},
			want: "pod is terminating",
		},
		{
			name: "role no longer primary",
			observed: topology.FailoverState{
				PrimaryName:   primary,
				InstanceNames: []string{primary, drainReplica},
				Instances: map[string]topology.FailoverInstance{
					primary:      {Ready: true, Primary: false, Role: "replica"},
					drainReplica: {Ready: true, Replica: true},
				},
			},
			want: "instance reports role replica, not primary",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := primaryUnhealthyReason(tc.observed)
			if got != tc.want {
				t.Fatalf("primaryUnhealthyReason = %q, want %q", got, tc.want)
			}
		})
	}
}
