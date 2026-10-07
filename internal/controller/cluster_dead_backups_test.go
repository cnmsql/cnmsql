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
	"reflect"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

const deadSuccessorUUID = "7f2b1c90-0000-11e1-9e33-c80aa9429562"

func TestJudgeAnchor(t *testing.T) {
	dead, err := judgeAnchor(gapUUID+":1-219", gapUUID+":1-218,"+deadSuccessorUUID+":1-50", nil, false)
	if err != nil || dead.gtidSet != gapUUID+":219" {
		t.Fatalf("dead = %+v, err = %v", dead, err)
	}
	dead, err = judgeAnchor(gapUUID+":1-200", gapUUID+":1-218,"+deadSuccessorUUID+":1-50", nil, false)
	if err != nil || !dead.empty() {
		t.Fatalf("a canonical anchor was judged dead: %+v, %v", dead, err)
	}

	timeline := engine.MariaDBTimeline{{ServerID: 1}, {ServerID: 2, Handoff: "0-1-218"}}
	dead, err = judgeAnchor("0-1-220", "", timeline, true)
	want := []objectstore.ArchiveDisownedRange{{Domain: 0, Server: 1, After: 218, Through: 220}}
	if err != nil || !reflect.DeepEqual(dead.ranges, want) {
		t.Fatalf("dead = %+v, err = %v, want %+v", dead, err, want)
	}
	if dead, err = judgeAnchor("0-2-250", "", timeline, true); err != nil || !dead.empty() {
		t.Fatalf("a canonical MariaDB anchor was judged dead: %+v, %v", dead, err)
	}
}

func TestJudgeBackupsMarksADeadBranchBackup(t *testing.T) {
	ctx := context.Background()
	cluster := archivingCluster()
	readAt := time.Now()
	dead := completedBackup("before-failover", gapUUID+":1-219", readAt.Add(-time.Hour))
	alive := completedBackup("after-failover", gapUUID+":1-218,"+deadSuccessorUUID+":1-40", readAt.Add(-time.Minute))
	tooNew := completedBackup("too-new", gapUUID+":1-218,"+deadSuccessorUUID+":1-90", readAt.Add(time.Minute))
	scheme := testScheme(t)
	index := &objectstore.ArchiveIndex{}
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Backup{}).
			WithObjects(cluster, dead, alive, tooNew).Build(),
		Scheme: scheme,
		updateArchiveIndex: func(_ context.Context, _ *mysqlv1alpha1.Cluster,
			mutate func(*objectstore.ArchiveIndex, bool) (bool, error)) error {
			_, err := mutate(index, true)
			return err
		},
	}
	observed := observedCluster{
		PrimaryName:       "demo-2",
		PrimaryGTIDReadAt: readAt,
		GTIDByInstance:    map[string]string{"demo-2": gapUUID + ":1-218," + deadSuccessorUUID + ":1-50"},
		StatusByInstance:  map[string]*webserver.Status{"demo-2": {Role: webserver.RolePrimary}},
	}
	r.judgeBackups(ctx, cluster, observed)

	condition := func(name string) string {
		b := &mysqlv1alpha1.Backup{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: "default", Name: name}, b); err != nil {
			t.Fatal(err)
		}
		if c := apimeta.FindStatusCondition(b.Status.Conditions, mysqlv1alpha1.ConditionDeadBranch); c != nil {
			return string(c.Status)
		}
		return ""
	}
	if got := condition("before-failover"); got != "True" {
		t.Fatalf("dead-branch backup condition = %q, want True", got)
	}
	if got := condition("after-failover"); got != "" {
		t.Fatalf("canonical backup condition = %q, want none", got)
	}
	if got := condition("too-new"); got != "" {
		t.Fatalf("a backup newer than the primary's position was judged: %q", got)
	}
	if index.Disowned == nil || index.Disowned.GTIDSet != gapUUID+":219" {
		t.Fatalf("index disowned = %+v", index.Disowned)
	}

	// A replica's status is no authority.
	observed.StatusByInstance["demo-2"] = &webserver.Status{Role: webserver.RoleReplica, ReadOnly: true}
	index.Disowned = nil
	r.judgeBackups(ctx, cluster, observed)
	if index.Disowned != nil {
		t.Fatalf("judged against a non-writable instance: %+v", index.Disowned)
	}
}
