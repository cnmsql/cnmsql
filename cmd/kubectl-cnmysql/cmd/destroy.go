/*
Copyright 2026 The cloudnative-mysql Authors.

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

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/CloudNative-MySQL/cloudnative-mysql/cmd/kubectl-cnmysql/plugin"
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

	action := "destroy instance and delete its PVC"
	if keepPVC {
		action = "destroy instance (keeping its PVC)"
	}
	if !plugin.Confirm(fmt.Sprintf("%s %q?", action, instance), yes) {
		fmt.Println("aborted")
		return nil
	}

	// The PVC shares the instance's name; handle it before deleting the Pod.
	pvc := &corev1.PersistentVolumeClaim{}
	pvcKey := client.ObjectKey{Namespace: cluster.Namespace, Name: instance}
	switch err := env.Client.Get(ctx, pvcKey, pvc); {
	case err == nil:
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
	case apierrors.IsNotFound(err):
		// No PVC; nothing to do.
	default:
		return fmt.Errorf("reading PVC %q: %w", instance, err)
	}

	err = env.Clientset.CoreV1().Pods(cluster.Namespace).Delete(ctx, instance, deleteNow())
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("deleting pod %q: %w", instance, err)
	}
	fmt.Printf("destroyed instance %q\n", instance)
	return nil
}
