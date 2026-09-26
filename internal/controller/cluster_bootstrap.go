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
	corev1 "k8s.io/api/core/v1"
)

const (
	// pvcStatusAnnotation records whether an instance's data volume holds a
	// bootstrapped data directory (design 031). A new volume starts
	// initializing; the operator marks it ready when the instance's bootstrap
	// Job succeeds, and only then creates the instance Pod.
	pvcStatusAnnotation   = "mysql.cnmsql.co/pvc-status"
	pvcStatusInitializing = "initializing"
	pvcStatusReady        = "ready"
)

// pvcBootstrapped reports whether the volume holds a bootstrapped data
// directory. A volume without the annotation predates bootstrap Jobs: the
// instance Pod that created it bootstrapped it in an init container, so it
// counts as bootstrapped.
func pvcBootstrapped(pvc *corev1.PersistentVolumeClaim) bool {
	status, ok := pvc.Annotations[pvcStatusAnnotation]
	return !ok || status == pvcStatusReady
}
