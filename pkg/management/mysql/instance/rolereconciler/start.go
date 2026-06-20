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

package rolereconciler

import (
	"context"
	"fmt"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/replication"
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
	// OnAPIServerContact, when set, is called every time the in-Pod manager
	// confirms it can reach the Kubernetes API server. It feeds the isolation
	// detector that backs the liveness probe.
	OnAPIServerContact func()
	// APIServerProbeInterval paces the reachability prober (default 5s). Ignored
	// when OnAPIServerContact is nil.
	APIServerProbeInterval time.Duration
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
	doorbellClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("creating GR doorbell client: %w", err)
	}

	reconciler := &Reconciler{
		Client:         mgr.GetClient(),
		DoorbellClient: doorbellClient,
		ClusterKey:     types.NamespacedName{Namespace: opts.Namespace, Name: opts.ClusterName},
		InstanceName:   opts.InstanceName,
		ServiceDomain:  opts.Namespace + ".svc",
		SourceTemplate: opts.SourceTemplate,
		Local:          opts.Local,
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		return err
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := reconciler.releaseLease(releaseCtx); err != nil {
			logf.FromContext(ctx).Error(err, "Could not release primary lease during shutdown")
		}
	}()

	// Run an independent API-server reachability prober. It must not rely on the
	// controller-runtime cache (which keeps serving the last-known Cluster while
	// partitioned); a direct round-trip to the API server is the only honest
	// isolation signal.
	if opts.OnAPIServerContact != nil {
		if err := startAPIServerProber(ctx, cfg, opts); err != nil {
			return err
		}
	}

	return mgr.Start(ctx)
}

// startAPIServerProber periodically round-trips to the API server and reports
// each success through opts.OnAPIServerContact. It runs until ctx is cancelled.
//
// It calls /version (via ServerVersion), which is reachable by any authenticated
// client through the system:discovery role. /readyz and /healthz are not bound
// to a Pod ServiceAccount by default, so probing them would fail with 403 and
// make a perfectly healthy instance declare itself isolated.
func startAPIServerProber(ctx context.Context, cfg *rest.Config, opts StartOptions) error {
	interval := opts.APIServerProbeInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	// Bound each probe so a hung connection cannot stall detection past the
	// isolation timeout.
	probeCfg := rest.CopyConfig(cfg)
	probeCfg.Timeout = interval
	clientset, err := kubernetes.NewForConfig(probeCfg)
	if err != nil {
		return fmt.Errorf("building API-server probe client: %w", err)
	}

	go func() {
		log := logf.FromContext(ctx).WithName("apiserver-prober")
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := clientset.Discovery().ServerVersion(); err != nil {
					log.V(1).Info("API server unreachable", "error", err.Error())
					continue
				}
				opts.OnAPIServerContact()
			}
		}
	}()
	return nil
}

func clusterCacheOptions(opts StartOptions) cache.Options {
	return cache.Options{
		ByObject: map[client.Object]cache.ByObject{
			&mysqlv1alpha1.Cluster{}: {
				Namespaces: map[string]cache.Config{opts.Namespace: {}},
				Field:      fields.OneTermEqualSelector("metadata.name", opts.ClusterName),
			},
			&coordinationv1.Lease{}: {
				Namespaces: map[string]cache.Config{opts.Namespace: {}},
				Field:      fields.OneTermEqualSelector("metadata.name", opts.ClusterName+"-primary"),
			},
		},
	}
}
