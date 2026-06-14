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

package plugin

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
)

// Annotation and label keys understood by the operator (kept in sync with the
// controller).
const (
	FencingAnnotation = "cnmysql.cloudnative-mysql.io/fencing"
	RestartAnnotation = "cnmysql.cloudnative-mysql.io/restart"
	ReloadAnnotation  = "cnmysql.cloudnative-mysql.io/reload"

	ClusterLabel = "mysql.cloudnative-mysql.io/cluster"
	RoleLabel    = "mysql.cloudnative-mysql.io/role"
)

// metaListByCluster builds the List options selecting a cluster's instances.
func metaListByCluster(cluster string) metav1.ListOptions {
	return metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{ClusterLabel: cluster}).String(),
	}
}

// GetCluster fetches a Cluster CR by name in the environment's namespace.
func (e *Env) GetCluster(ctx context.Context, name string) (*mysqlv1alpha1.Cluster, error) {
	cluster := &mysqlv1alpha1.Cluster{}
	key := types.NamespacedName{Namespace: e.Namespace, Name: name}
	if err := e.Client.Get(ctx, key, cluster); err != nil {
		return nil, fmt.Errorf("getting cluster %q: %w", name, err)
	}
	return cluster, nil
}

// ListPods returns the Pods belonging to a cluster, selected by the cluster
// label the operator stamps on every instance.
func (e *Env) ListPods(ctx context.Context, cluster *mysqlv1alpha1.Cluster) ([]corev1.Pod, error) {
	pods, err := e.Clientset.CoreV1().Pods(cluster.Namespace).List(ctx, metaListByCluster(cluster.Name))
	if err != nil {
		return nil, fmt.Errorf("listing pods for cluster %q: %w", cluster.Name, err)
	}
	return pods.Items, nil
}

// PrimaryInstance returns the current primary's name, falling back to the
// target primary while a switchover is in progress.
func PrimaryInstance(cluster *mysqlv1alpha1.Cluster) string {
	if cluster.Status.CurrentPrimary != "" {
		return cluster.Status.CurrentPrimary
	}
	return cluster.Status.TargetPrimary
}

// PodReady reports whether a Pod's Ready condition is true.
func PodReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// Contains reports whether s is in the slice.
func Contains(list []string, s string) bool {
	return slices.Contains(list, s)
}

// Confirm prompts the user for a yes/no answer on stdin. It returns true only
// when the user explicitly types y/yes. When skip is true (e.g. a --yes flag),
// it returns true without prompting.
func Confirm(prompt string, skip bool) bool {
	if skip {
		return true
	}
	fmt.Printf("%s [y/N]: ", prompt)
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil {
		return false
	}
	answer := strings.ToLower(strings.TrimSpace(line))
	return answer == "y" || answer == "yes"
}
