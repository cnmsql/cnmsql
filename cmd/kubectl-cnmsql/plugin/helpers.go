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

package plugin

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"golang.org/x/term"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// Annotation and label keys understood by the operator (kept in sync with the
// controller).
const (
	FencingAnnotation = "cnmsql.cnmsql.co/fencing"
	FencingValue      = "true"
	RestartAnnotation = "cnmsql.cnmsql.co/restart"
	ReloadAnnotation  = "cnmsql.cnmsql.co/reload"
	ReinitAnnotation  = "cnmsql.cnmsql.co/reinit"

	// ForceQuorumRecoveryAnnotation, set to "yes" on a Group Replication Cluster,
	// asks the operator to attempt a guarded quorum recovery. The operator still
	// gate-checks that quorum is provably lost and a safe survivor exists, and
	// refuses otherwise — the annotation is a request, not a command.
	ForceQuorumRecoveryAnnotation = "cnmsql.cnmsql.co/force-quorum-recovery"

	ClusterLabel = "mysql.cnmsql.co/cluster"
	RoleLabel    = "mysql.cnmsql.co/role"
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

// ResolveCluster fetches the named Cluster, or — when name is empty — defaults
// to the sole Cluster in the namespace. With several clusters present it picks
// the first by name and warns on stderr; with none it returns an error asking
// for an explicit name.
func (e *Env) ResolveCluster(ctx context.Context, name string) (*mysqlv1alpha1.Cluster, error) {
	if name != "" {
		return e.GetCluster(ctx, name)
	}
	list := &mysqlv1alpha1.ClusterList{}
	if err := e.Client.List(ctx, list, client.InNamespace(e.Namespace)); err != nil {
		return nil, fmt.Errorf("listing clusters in namespace %q: %w", e.Namespace, err)
	}
	switch len(list.Items) {
	case 0:
		return nil, fmt.Errorf("no clusters found in namespace %q; specify a CLUSTER name", e.Namespace)
	case 1:
		return &list.Items[0], nil
	default:
		sort.Slice(list.Items, func(i, j int) bool { return list.Items[i].Name < list.Items[j].Name })
		chosen := &list.Items[0]
		names := make([]string, 0, len(list.Items))
		for i := range list.Items {
			names = append(names, list.Items[i].Name)
		}
		fmt.Fprintf(os.Stderr, "warning: %d clusters in namespace %q (%s); defaulting to %q\n",
			len(list.Items), e.Namespace, strings.Join(names, ", "), chosen.Name)
		return chosen, nil
	}
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

// ReadPassword obtains a password without ever exposing it on the command line.
// When fromStdin is true it reads a single line from stdin (suitable for
// `--password-stdin`, e.g. piping from a secret); otherwise it prompts on the
// controlling terminal with echo disabled. The password is never accepted as a
// flag argument, keeping it out of shell history and the process table.
func ReadPassword(fromStdin bool) (string, error) {
	if fromStdin {
		reader := bufio.NewReader(os.Stdin)
		line, err := reader.ReadString('\n')
		if err != nil && line == "" {
			return "", fmt.Errorf("reading password from stdin: %w", err)
		}
		return strings.TrimRight(line, "\r\n"), nil
	}
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return "", fmt.Errorf("no terminal available for password prompt; use --password-stdin")
	}
	fmt.Print("Password: ")
	pw, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Println()
	if err != nil {
		return "", fmt.Errorf("reading password: %w", err)
	}
	return string(pw), nil
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
