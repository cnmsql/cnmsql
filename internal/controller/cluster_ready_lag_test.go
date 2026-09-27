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
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

func readyLagCluster(lag *metav1.Duration) *mysqlv1alpha1.Cluster {
	cluster := baseCluster()
	cluster.Spec.Replication = &mysqlv1alpha1.ReplicationConfiguration{
		MaxReadyLag: lag,
	}
	cluster.SetDefaults()
	return cluster
}

func TestReadyLagDisabledByDefault(t *testing.T) {
	cluster := baseCluster()
	if cluster.MaxReadyLag() != nil {
		t.Fatal("the readiness lag gate should be disabled by default")
	}
	args := (&ClusterReconciler{}).runArgs(cluster, testPlan(), instancePlan{})
	for _, a := range args {
		if strings.Contains(a, "max-ready-lag") {
			t.Fatalf("unexpected readiness lag flag: %v", args)
		}
	}
}

func TestReadyLagRunArgsCarryTheBound(t *testing.T) {
	cluster := readyLagCluster(&metav1.Duration{Duration: 30 * time.Second})
	lag := cluster.MaxReadyLag()
	if lag == nil || *lag != 30*time.Second {
		t.Fatalf("MaxReadyLag() = %v, want 30s", lag)
	}
	args := (&ClusterReconciler{}).runArgs(cluster, testPlan(), instancePlan{})
	if !containsArg(args, "--max-ready-lag-millis=30000") {
		t.Fatalf("missing --max-ready-lag-millis=30000: %v", args)
	}
}
