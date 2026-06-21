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

package groupreplication

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
)

var _ topology.Reconciler = (*Reconciler)(nil)

// Reconciler owns Group Replication topology-specific reconciliation.
type Reconciler struct {
	client client.Client
	scheme *runtime.Scheme

	switchoverControl topology.GroupSwitchoverControl
	recorder          record.EventRecorder
}

// NewReconciler creates a Group Replication topology reconciler.
func NewReconciler(
	kubeClient client.Client,
	scheme *runtime.Scheme,
	switchoverControl topology.GroupSwitchoverControl,
	recorder record.EventRecorder,
) *Reconciler {
	return &Reconciler{
		client:            kubeClient,
		scheme:            scheme,
		switchoverControl: switchoverControl,
		recorder:          recorder,
	}
}
