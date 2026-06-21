/*
Copyright 2026 The CloudNative MySQL Authors.

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

package cmd

import (
	"context"
	"fmt"
	"sort"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/cmd/kubectl-cnmysql/plugin"
)

const (
	readyYes = "yes"
	readyNo  = "no"
)

func newStatusCommand() *cobra.Command {
	return newWatchingCommand("status [CLUSTER]", "Show the status of a cluster and its instances",
		"status ", runStatus)
}

func runStatus(ctx context.Context, clusterName, output string) error {
	env, err := newEnv()
	if err != nil {
		return err
	}
	cluster, err := env.ResolveCluster(ctx, clusterName)
	if err != nil {
		return err
	}

	if output != "" {
		return plugin.PrintObject(cluster, output)
	}

	pods, err := env.ListPods(ctx, cluster)
	if err != nil {
		return err
	}

	printSummary(cluster)
	printConditions(cluster)
	printInstances(cluster, pods)
	return nil
}

func printSummary(c *mysqlv1alpha1.Cluster) {
	plugin.Section("Cluster Summary")
	plugin.KeyVal("Name", c.Name)
	plugin.KeyVal("Namespace", c.Namespace)
	plugin.KeyVal("Phase", orNone(c.Status.Phase))
	if c.Status.PhaseReason != "" {
		plugin.KeyVal("Phase Reason", c.Status.PhaseReason)
	}
	plugin.KeyVal("Instances", fmt.Sprintf("%d/%d ready", c.Status.ReadyInstances, c.Status.Instances))
	plugin.KeyVal("Primary", orNone(c.Status.CurrentPrimary))
	if c.Status.TargetPrimary != "" && c.Status.TargetPrimary != c.Status.CurrentPrimary {
		plugin.KeyVal("Target Primary", c.Status.TargetPrimary)
	}
	plugin.KeyVal("Image", orNone(c.Status.Image))
	if len(c.Status.FencedInstances) > 0 {
		plugin.KeyVal("Fenced", fmt.Sprintf("%v", c.Status.FencedInstances))
	}
	if len(c.Status.DivergedInstances) > 0 {
		plugin.KeyVal("Diverged", fmt.Sprintf("%v", c.Status.DivergedInstances))
	}
}

func printConditions(c *mysqlv1alpha1.Cluster) {
	if len(c.Status.Conditions) == 0 {
		return
	}
	plugin.Section("Conditions")
	rows := make([][]string, 0, len(c.Status.Conditions))
	for _, cond := range c.Status.Conditions {
		rows = append(rows, []string{cond.Type, string(cond.Status), cond.Reason, cond.Message})
	}
	plugin.Table([]string{"TYPE", "STATUS", "REASON", "MESSAGE"}, rows)
}

func printInstances(c *mysqlv1alpha1.Cluster, pods []corev1.Pod) {
	plugin.Section("Instances")
	sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })
	primary := plugin.PrimaryInstance(c)
	rows := make([][]string, 0, len(pods))
	for i := range pods {
		pod := &pods[i]
		role := "replica"
		if pod.Name == primary {
			role = "primary"
		}
		ready := readyNo
		if plugin.PodReady(pod) {
			ready = readyYes
		}
		flags := ""
		if plugin.Contains(c.Status.FencedInstances, pod.Name) {
			flags += "fenced "
		}
		if plugin.Contains(c.Status.DivergedInstances, pod.Name) {
			flags += "diverged "
		}
		rows = append(rows, []string{
			pod.Name,
			role,
			ready,
			string(pod.Status.Phase),
			pod.Spec.NodeName,
			flags,
		})
	}
	plugin.Table([]string{"NAME", "ROLE", "READY", "PHASE", "NODE", "FLAGS"}, rows)
}

func orNone(s string) string {
	if s == "" {
		return "<none>"
	}
	return s
}
