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

package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/cnmsql/cnmsql/cmd/kubectl-cnmsql/plugin"
)

func newDestroyCommand() *cobra.Command {
	var (
		keepPVC bool
		yes     bool
	)
	cmd := &cobra.Command{
		Use:   "destroy CLUSTER INSTANCE",
		Short: "Destroy a single instance (Pod and its PVC)",
		Long: "Delete an instance's Pod and, unless --keep-pvc is given, its data " +
			"PVC. With --keep-pvc the PVC's owner references are removed so it " +
			"survives, letting you re-import the data later.",
		Args:              cobra.ExactArgs(2),
		ValidArgsFunction: completeClusterInstanceArgs,
		Example: `  # Destroy an instance and its PVC (with confirmation prompt)
  kubectl cnmsql destroy cluster-sample cluster-sample-3

  # Destroy an instance but keep the PVC (detach it from the cluster)
  kubectl cnmsql destroy cluster-sample cluster-sample-3 --keep-pvc

  # Skip confirmation prompt
  kubectl cnmsql destroy cluster-sample cluster-sample-3 --yes`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDestroy(cmd.Context(), args[0], args[1], keepPVC, yes)
		},
	}
	cmd.Flags().BoolVar(&keepPVC, "keep-pvc", false, "retain the data PVC (detach it from the cluster)")
	cmd.Flags().BoolVarP(&yes, "yes", "y", false, "skip confirmation prompt")
	return cmd
}

func runDestroy(ctx context.Context, clusterName, instance string, keepPVC, yes bool) error {
	env, err := newEnv()
	if err != nil {
		return err
	}
	cluster, err := env.GetCluster(ctx, clusterName)
	if err != nil {
		return err
	}

	// Establish that INSTANCE belongs to CLUSTER before touching anything: the
	// PVC is looked up by name, and a name alone could match an unrelated
	// claim in the namespace.
	pod, err := env.Clientset.CoreV1().Pods(cluster.Namespace).Get(ctx, instance, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		pod = nil
	case err != nil:
		return fmt.Errorf("reading pod %q: %w", instance, err)
	}
	pvc := &corev1.PersistentVolumeClaim{}
	switch err := env.Client.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: instance}, pvc); {
	case apierrors.IsNotFound(err):
		pvc = nil
	case err != nil:
		return fmt.Errorf("reading PVC %q: %w", instance, err)
	}
	if pod != nil && pod.Labels[plugin.ClusterLabel] != cluster.Name {
		return fmt.Errorf("pod %q is not an instance of cluster %q", instance, cluster.Name)
	}
	if pvc != nil && pvc.Labels[plugin.ClusterLabel] != cluster.Name {
		return fmt.Errorf("PVC %q does not belong to cluster %q; refusing to touch it", instance, cluster.Name)
	}
	if pod == nil && pvc == nil {
		return fmt.Errorf("instance %q not found in cluster %q", instance, cluster.Name)
	}

	var parts []string
	if pod != nil {
		parts = append(parts, "Pod "+instance)
	}
	if pvc != nil && !keepPVC {
		parts = append(parts, "PVC "+instance+" (its data is lost)")
	}
	prompt := fmt.Sprintf("Destroy instance %q of %q, deleting %s?", instance, cluster.Name, strings.Join(parts, " and "))
	if pvc != nil && keepPVC {
		prompt += " The PVC is kept and detached from the cluster."
	}
	if instance == plugin.PrimaryInstance(cluster) {
		prompt = fmt.Sprintf("%q is the PRIMARY. %s", instance, prompt)
	}
	if !plugin.Confirm(prompt, yes) {
		fmt.Println("aborted")
		return nil
	}

	// Handle the PVC before deleting the Pod, so the operator cannot recreate
	// the Pod on top of it in between.
	if pvc != nil {
		if keepPVC {
			if len(pvc.OwnerReferences) > 0 {
				before := pvc.DeepCopy()
				pvc.OwnerReferences = nil
				if err := env.Client.Patch(ctx, pvc, client.MergeFrom(before)); err != nil {
					return fmt.Errorf("detaching PVC %q: %w", instance, err)
				}
			}
			fmt.Printf("retained PVC %q\n", instance)
		} else {
			if err := env.Client.Delete(ctx, pvc); err != nil {
				return fmt.Errorf("deleting PVC %q: %w", instance, err)
			}
			fmt.Printf("deleted PVC %q\n", instance)
		}
	}

	// A graceful delete lets the preStop hook hand a primary's role over
	// before mysqld stops.
	if pod != nil {
		err = env.Clientset.CoreV1().Pods(cluster.Namespace).Delete(ctx, instance, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("deleting pod %q: %w", instance, err)
		}
	}
	fmt.Printf("destroyed instance %q\n", instance)
	return nil
}
