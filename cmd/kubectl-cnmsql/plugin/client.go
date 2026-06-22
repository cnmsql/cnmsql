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

// Package plugin holds the shared infrastructure for the kubectl-cnmsql
// plugin: Kubernetes client setup, output formatting and small helpers reused
// across commands.
package plugin

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// Scheme carries the API types the plugin needs to (de)serialize.
var Scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(Scheme))
	utilruntime.Must(mysqlv1alpha1.AddToScheme(Scheme))
}

// Env bundles everything a command needs to talk to a cluster: the resolved
// namespace, a controller-runtime client for CRDs, a typed clientset for core
// resources (pods/secrets/logs/port-forward), and the underlying REST config.
type Env struct {
	Namespace   string
	Client      client.Client
	Clientset   kubernetes.Interface
	RESTConfig  *rest.Config
	configFlags *genericclioptions.ConfigFlags
}

// NewEnv resolves the kubeconfig/context/namespace from the standard kubectl
// flags and builds the clients.
func NewEnv(configFlags *genericclioptions.ConfigFlags) (*Env, error) {
	restConfig, err := configFlags.ToRESTConfig()
	if err != nil {
		return nil, fmt.Errorf("loading kubeconfig: %w", err)
	}

	namespace, _, err := configFlags.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return nil, fmt.Errorf("resolving namespace: %w", err)
	}

	c, err := client.New(restConfig, client.Options{Scheme: Scheme})
	if err != nil {
		return nil, fmt.Errorf("building controller-runtime client: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("building kubernetes clientset: %w", err)
	}

	return &Env{
		Namespace:   namespace,
		Client:      c,
		Clientset:   clientset,
		RESTConfig:  restConfig,
		configFlags: configFlags,
	}, nil
}
