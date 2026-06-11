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

package rolereconciler

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/replication"
)

// StartOptions configures the in-Pod role manager.
type StartOptions struct {
	// RestConfig is the Kubernetes client config; when nil, in-cluster config is
	// used (the Pod's ServiceAccount).
	RestConfig *rest.Config
	// Namespace and ClusterName identify the owning Cluster to watch.
	Namespace   string
	ClusterName string
	// InstanceName is this instance's Pod name.
	InstanceName string
	// SourceTemplate holds the static replication connection parameters; the
	// source host is derived from currentPrimary.
	SourceTemplate replication.SourceOptions
	// Local drives the local mysqld.
	Local LocalInstance
}

// Start builds a controller-runtime manager scoped to the owning Cluster's
// namespace and runs the role reconciler until ctx is cancelled.
func Start(ctx context.Context, opts StartOptions) error {
	cfg := opts.RestConfig
	if cfg == nil {
		var err error
		cfg, err = rest.InClusterConfig()
		if err != nil {
			return fmt.Errorf("loading in-cluster config: %w", err)
		}
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return err
	}
	if err := mysqlv1alpha1.AddToScheme(scheme); err != nil {
		return err
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:         scheme,
		Metrics:        metricsserver.Options{BindAddress: "0"},
		LeaderElection: false,
		Cache:          clusterCacheOptions(opts),
	})
	if err != nil {
		return fmt.Errorf("creating role manager: %w", err)
	}

	reconciler := &Reconciler{
		Client:         mgr.GetClient(),
		ClusterKey:     types.NamespacedName{Namespace: opts.Namespace, Name: opts.ClusterName},
		InstanceName:   opts.InstanceName,
		ServiceDomain:  opts.Namespace + ".svc",
		SourceTemplate: opts.SourceTemplate,
		Local:          opts.Local,
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		return err
	}
	return mgr.Start(ctx)
}

func clusterCacheOptions(opts StartOptions) cache.Options {
	return cache.Options{
		ByObject: map[client.Object]cache.ByObject{
			&mysqlv1alpha1.Cluster{}: {
				Namespaces: map[string]cache.Config{opts.Namespace: {}},
				Field:      fields.OneTermEqualSelector("metadata.name", opts.ClusterName),
			},
		},
	}
}
