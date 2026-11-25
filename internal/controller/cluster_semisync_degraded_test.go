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

package controller

import (
	"context"
	"strings"
	"testing"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

func semiSyncDegradedCluster() *mysqlv1alpha1.Cluster {
	cluster := baseCluster()
	cluster.Spec.MySQL.SemiSync = &mysqlv1alpha1.SemiSyncConfiguration{Enabled: true}
	return cluster
}

func liveSemiSyncStatus(active bool, inactiveSeconds int64, fenced bool) *webserver.Status {
	st := &webserver.Status{InstanceName: testPrimary, SemiSyncReadOnlyEngaged: fenced}
	st.SemiSync.SourceActive = &active
	if !active {
		st.SemiSync.InactiveSeconds = &inactiveSeconds
	}
	return st
}

func TestEvaluateSemiSync(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		status       *webserver.Status
		degradedAt   *int32
		wantObserved bool
		wantDegraded bool
		wantReadOnly bool
	}{
		{name: "primary unreachable", status: nil},
		{name: "instance manager without the live flag", status: &webserver.Status{}},
		{name: "active", status: liveSemiSyncStatus(true, 0, false), wantObserved: true},
		{name: "inactive within the default delay",
			status: liveSemiSyncStatus(false, 9, false), wantObserved: true},
		{name: "inactive past the default delay",
			status: liveSemiSyncStatus(false, 10, false), wantObserved: true, wantDegraded: true},
		{name: "a zero delay reports at once",
			status: liveSemiSyncStatus(false, 0, false), degradedAt: ptr.To[int32](0),
			wantObserved: true, wantDegraded: true},
		{name: "a longer delay",
			status: liveSemiSyncStatus(false, 59, false), degradedAt: ptr.To[int32](60), wantObserved: true},
		{name: "fenced read-only counts as degraded",
			status: liveSemiSyncStatus(false, 1, true), wantObserved: true, wantDegraded: true, wantReadOnly: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cluster := semiSyncDegradedCluster()
			cluster.Spec.MySQL.SemiSync.DegradedAfterSeconds = tc.degradedAt
			got := evaluateSemiSync(cluster, observedCluster{
				PrimaryName:      testPrimary,
				StatusByInstance: map[string]*webserver.Status{testPrimary: tc.status},
			})
			if got.Observed != tc.wantObserved || got.Degraded != tc.wantDegraded || got.ReadOnly != tc.wantReadOnly {
				t.Fatalf("got %+v, want observed=%v degraded=%v readOnly=%v",
					got, tc.wantObserved, tc.wantDegraded, tc.wantReadOnly)
			}
		})
	}

	disabled := baseCluster()
	got := evaluateSemiSync(disabled, observedCluster{
		PrimaryName:      testPrimary,
		StatusByInstance: map[string]*webserver.Status{testPrimary: liveSemiSyncStatus(false, 60, false)},
	})
	if got.Observed {
		t.Fatalf("semi-sync disabled, but observed %+v", got)
	}
}

func TestSemiSyncDegradedConditionAndEvents(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	recorder := record.NewFakeRecorder(16)
	r := &ClusterReconciler{Recorder: recorder}
	cluster := semiSyncDegradedCluster()

	step := func(obs semiSyncObservation) {
		t.Helper()
		before := apimeta.FindStatusCondition(cluster.Status.Conditions, mysqlv1alpha1.ConditionSemiSyncDegraded)
		var beforeCopy *metav1.Condition
		if before != nil {
			c := *before
			beforeCopy = &c
		}
		r.applySemiSyncDegradedCondition(cluster, observedCluster{SemiSync: obs})
		r.recordSemiSyncEvents(ctx, cluster, beforeCopy)
	}
	condition := func() *metav1.Condition {
		return apimeta.FindStatusCondition(cluster.Status.Conditions, mysqlv1alpha1.ConditionSemiSyncDegraded)
	}

	// Nothing observed: no condition, never a false False.
	step(semiSyncObservation{})
	if condition() != nil {
		t.Fatalf("condition set without an observation: %+v", condition())
	}

	step(semiSyncObservation{Observed: true, Primary: testPrimary})
	if c := condition(); c == nil || c.Status != metav1.ConditionFalse || c.Reason != semiSyncReasonActive {
		t.Fatalf("active: condition = %+v", c)
	}
	assertNoEvent(t, recorder, "")

	step(semiSyncObservation{Observed: true, Primary: testPrimary, Degraded: true})
	if c := condition(); c == nil || c.Status != metav1.ConditionTrue || c.Reason != semiSyncReasonAsynchronous {
		t.Fatalf("degraded: condition = %+v", c)
	}
	assertEvent(t, recorder, eventSemiSyncDegraded)

	// The primary is briefly unreachable: the condition stays as it was.
	step(semiSyncObservation{})
	if c := condition(); c == nil || c.Status != metav1.ConditionTrue {
		t.Fatalf("unobserved pass changed the condition: %+v", c)
	}
	assertNoEvent(t, recorder, "")

	step(semiSyncObservation{Observed: true, Primary: testPrimary, Degraded: true, ReadOnly: true})
	if c := condition(); c.Reason != semiSyncReasonReadOnly || !strings.Contains(c.Message, "pauses writes") {
		t.Fatalf("fenced: condition = %+v", c)
	}
	assertEvent(t, recorder, eventSemiSyncReadOnly)

	step(semiSyncObservation{Observed: true, Primary: testPrimary})
	if c := condition(); c.Status != metav1.ConditionFalse {
		t.Fatalf("restored: condition = %+v", c)
	}
	assertEvent(t, recorder, eventSemiSyncRestored)
	assertEvent(t, recorder, eventSemiSyncReadOnlyReleased)

	cluster.Spec.MySQL.SemiSync.Enabled = false
	step(semiSyncObservation{})
	if condition() != nil {
		t.Fatalf("condition kept after semi-sync was disabled")
	}
}
