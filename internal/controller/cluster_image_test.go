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
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/engine"
)

func imageInfo(image, serverVersion string) *mysqlv1alpha1.ImageInfo {
	return &mysqlv1alpha1.ImageInfo{Image: image, Flavor: mysqlv1alpha1.FlavorMySQL, ServerVersion: serverVersion}
}

func catalogRef(series string) *mysqlv1alpha1.ImageCatalogRef {
	return &mysqlv1alpha1.ImageCatalogRef{
		TypedLocalObjectReference: corev1.TypedLocalObjectReference{Name: "images", Kind: catalogKindCluster},
		Series:                    series,
	}
}

func TestValidateTargetImage(t *testing.T) {
	t.Parallel()
	mysql := engine.MustForFlavor(engine.FlavorMySQL)

	cases := []struct {
		name     string
		mutate   func(*mysqlv1alpha1.Cluster)
		previous *mysqlv1alpha1.ImageInfo
		info     *mysqlv1alpha1.ImageInfo
		reason   string // empty: accepted
	}{
		{
			name: "first image, tag names its series",
			info: imageInfo("ghcr.io/cnmsql/cnmsql-instance:8.4.11-202610011200-bookworm", "8.4.11"),
		},
		{
			name: "digest-only reference names no series",
			info: imageInfo("ghcr.io/cnmsql/cnmsql-instance@sha256:0123", "8.4.11"),
		},
		{
			name: "moving tag without a series",
			info: imageInfo("registry.example/mysql:latest", "8.0.46"),
		},
		{
			name:   "tag names another series than the image runs",
			info:   imageInfo("registry.example/mysql:8.4", "9.6.0"),
			reason: reasonImageSeriesMismatch,
		},
		{
			name:   "catalog entry maps a series to another series' image",
			mutate: func(c *mysqlv1alpha1.Cluster) { c.Spec.ImageCatalogRef = catalogRef("8.4") },
			info:   imageInfo("registry.example/mysql@sha256:0123", "8.0.46"),
			reason: reasonImageSeriesMismatch,
		},
		{
			name:   "catalog series matches, the tag is ignored",
			mutate: func(c *mysqlv1alpha1.Cluster) { c.Spec.ImageCatalogRef = catalogRef("8.4") },
			info:   imageInfo("registry.example/mysql:whatever", "8.4.11"),
		},
		{
			name:   "flavor mismatch",
			info:   &mysqlv1alpha1.ImageInfo{Image: "registry.example/mariadb:11.4", Flavor: mysqlv1alpha1.FlavorMariaDB, ServerVersion: "11.4.13"},
			reason: reasonImageFlavorMismatch,
		},
		{
			name:     "patch bump",
			previous: imageInfo("registry.example/mysql:8.4.10", "8.4.10"),
			info:     imageInfo("registry.example/mysql:8.4.11", "8.4.11"),
		},
		{
			name:     "patch downgrade within a series",
			previous: imageInfo("registry.example/mysql:8.4.11", "8.4.11"),
			info:     imageInfo("registry.example/mysql:8.4.10", "8.4.10"),
		},
		{
			name:     "single series hop",
			previous: imageInfo("registry.example/mysql:8.0", "8.0.46"),
			info:     imageInfo("registry.example/mysql@sha256:0123", "8.4.11"),
		},
		{
			name:     "skipped series behind a digest",
			previous: imageInfo("registry.example/mysql:8.0", "8.0.46"),
			info:     imageInfo("registry.example/mysql@sha256:0123", "9.6.0"),
			reason:   reasonImageUnsupportedMove,
		},
		{
			name:     "series downgrade",
			previous: imageInfo("registry.example/mysql:8.4", "8.4.11"),
			info:     imageInfo("registry.example/mysql@sha256:0123", "8.0.46"),
			reason:   reasonImageUnsupportedMove,
		},
		{
			name:   "MySQL 5.6",
			info:   imageInfo("registry.example/mysql@sha256:0123", "5.6.51"),
			reason: reasonImageUnsupportedMove,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cluster := baseCluster()
			if tc.mutate != nil {
				tc.mutate(cluster)
			}
			err := validateTargetImage(cluster, mysql, tc.previous, tc.info)
			if tc.reason == "" {
				if err != nil {
					t.Fatalf("unexpected rejection: %v", err)
				}
				return
			}
			rejected, ok := errors.AsType[*imageRejectedError](err)
			if !ok || rejected.Reason != tc.reason {
				t.Fatalf("error = %v, want an %s rejection", err, tc.reason)
			}
		})
	}
}

func TestResolveTargetImage(t *testing.T) {
	t.Parallel()
	mysql := engine.MustForFlavor(engine.FlavorMySQL)
	const current, next = "registry.example/mysql:8.4.10", "registry.example/mysql:8.4.11"

	t.Run("an unchanged image is not probed again", func(t *testing.T) {
		t.Parallel()
		cluster := baseCluster()
		cluster.Status.TargetImage = imageInfo(current, "8.4.10")
		r := &ClusterReconciler{ImageProber: fixedImageProber{err: errors.New("must not probe")}}
		got, decision, err := r.resolveTargetImage(context.Background(), cluster, mysql, current)
		if err != nil || got.ServerVersion != "8.4.10" || decision.accepted != nil || decision.status != metav1.ConditionTrue {
			t.Fatalf("got %+v, %+v, %v", got, decision, err)
		}
	})

	t.Run("a new image is used once probed", func(t *testing.T) {
		t.Parallel()
		cluster := baseCluster()
		cluster.Status.TargetImage = imageInfo(current, "8.4.10")
		r := &ClusterReconciler{ImageProber: fixedImageProber{info: imageInfo(next, "8.4.11")}}
		got, decision, err := r.resolveTargetImage(context.Background(), cluster, mysql, next)
		if err != nil || got.Image != next || decision.accepted == nil || decision.accepted.ServerVersion != "8.4.11" {
			t.Fatalf("got %+v, %+v, %v", got, decision, err)
		}
	})

	t.Run("the cluster stays on its image while the new one is probed", func(t *testing.T) {
		t.Parallel()
		cluster := baseCluster()
		cluster.Status.TargetImage = imageInfo(current, "8.4.10")
		r := &ClusterReconciler{ImageProber: fixedImageProber{}}
		got, decision, err := r.resolveTargetImage(context.Background(), cluster, mysql, next)
		if err != nil || got.Image != current || decision.reason != reasonImageProbing || decision.status != metav1.ConditionFalse {
			t.Fatalf("got %+v, %+v, %v", got, decision, err)
		}
	})

	t.Run("the cluster stays on its image when the new one is rejected", func(t *testing.T) {
		t.Parallel()
		cluster := baseCluster()
		cluster.Status.TargetImage = imageInfo(current, "8.4.10")
		r := &ClusterReconciler{ImageProber: fixedImageProber{err: &imageRejectedError{Reason: reasonImagePullFailed, Message: "Image cannot be pulled"}}}
		got, decision, err := r.resolveTargetImage(context.Background(), cluster, mysql, next)
		if err != nil || got.Image != current || decision.reason != reasonImagePullFailed || !strings.Contains(decision.message, "staying on "+current) {
			t.Fatalf("got %+v, %+v, %v", got, decision, err)
		}
	})

	t.Run("a new cluster waits for its first probe", func(t *testing.T) {
		t.Parallel()
		r := &ClusterReconciler{ImageProber: fixedImageProber{}}
		_, _, err := r.resolveTargetImage(context.Background(), baseCluster(), mysql, next)
		if _, ok := errors.AsType[*imageProbingError](err); !ok {
			t.Fatalf("error = %v, want an imageProbingError", err)
		}
	})

	t.Run("a new cluster is blocked by a rejected image", func(t *testing.T) {
		t.Parallel()
		r := &ClusterReconciler{ImageProber: fixedImageProber{info: imageInfo(next, "9.6.0")}}
		_, _, err := r.resolveTargetImage(context.Background(), baseCluster(), mysql, next)
		if rejected, ok := errors.AsType[*imageRejectedError](err); !ok || rejected.Reason != reasonImageSeriesMismatch {
			t.Fatalf("error = %v, want a series mismatch", err)
		}
	})
}

func TestBuildPlanKeepsThePreviousImageWhileProbing(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	cluster.Spec.ImageName = "registry.example/mysql:8.4.11"
	cluster.Status.TargetImage = imageInfo("registry.example/mysql:8.4.10", "8.4.10")
	r := &ClusterReconciler{
		Client:      fake.NewClientBuilder().WithScheme(testScheme(t)).Build(),
		Scheme:      testScheme(t),
		ImageProber: fixedImageProber{},
	}
	plan, err := r.buildPlan(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Image != "registry.example/mysql:8.4.10" || plan.ServerVersion != "8.4.10" {
		t.Fatalf("plan image %q version %q, want the previous target image", plan.Image, plan.ServerVersion)
	}
}

func TestRecordImageDecision(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	accepted := imageInfo("registry.example/mysql:8.4.11", "8.4.11")
	if err := r.recordImageDecision(context.Background(), cluster, imageDecision{
		accepted: accepted, status: metav1.ConditionTrue, reason: reasonImageProbed, message: imageSummary(accepted),
	}); err != nil {
		t.Fatal(err)
	}
	stored := &mysqlv1alpha1.Cluster{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(cluster), stored); err != nil {
		t.Fatal(err)
	}
	cond := apimeta.FindStatusCondition(stored.Status.Conditions, mysqlv1alpha1.ConditionImageReady)
	if stored.Status.TargetImage == nil || stored.Status.TargetImage.ServerVersion != "8.4.11" || cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("status = %+v", stored.Status)
	}
}

func probeReconcilerFixture(t *testing.T, objs ...client.Object) (*podImageProber, client.Client, *mysqlv1alpha1.Cluster) {
	t.Helper()
	cluster := baseCluster()
	cluster.UID = "cluster-uid"
	cluster.Spec.ImagePullSecrets = []mysqlv1alpha1.LocalObjectReference{{Name: "registry-creds"}}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(append(objs, cluster)...).Build()
	return &podImageProber{Client: c, Scheme: scheme}, c, cluster
}

func TestPodImageProberCreatesAProbePod(t *testing.T) {
	t.Parallel()
	const image = "registry.example/mysql@sha256:0123"
	prober, c, cluster := probeReconcilerFixture(t)

	info, err := prober.Probe(context.Background(), cluster, image, "ghcr.io/cnmsql/cnmsql:v1")
	if info != nil || err != nil {
		t.Fatalf("first probe = %+v, %v; want a pending probe", info, err)
	}
	pod := &corev1.Pod{}
	name := imageProbePodName(cluster, image)
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: name}, pod); err != nil {
		t.Fatal(err)
	}
	if _, isInstance := pod.Labels[clusterLabel]; isInstance || pod.Labels[imageProbeLabel] != cluster.Name {
		t.Errorf("labels = %v: a probe must not look like an instance", pod.Labels)
	}
	if owner := metav1.GetControllerOf(pod); owner == nil || owner.Name != cluster.Name {
		t.Errorf("owner = %v, want the cluster", owner)
	}
	container := pod.Spec.Containers[0]
	if container.Image != image || strings.Join(container.Args, " ") != "instance probe --mysqld="+mysqldBinary {
		t.Errorf("container = %s %v", container.Image, container.Args)
	}
	if pod.Spec.InitContainers[0].Image != "ghcr.io/cnmsql/cnmsql:v1" {
		t.Errorf("init image = %s, want the operator image", pod.Spec.InitContainers[0].Image)
	}
	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever || len(pod.Spec.ImagePullSecrets) != 1 ||
		pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Errorf("spec = %+v", pod.Spec)
	}
}

func probePod(cluster *mysqlv1alpha1.Cluster, image string, phase corev1.PodPhase, status corev1.ContainerStatus) *corev1.Pod {
	status.Name = imageProbeContainerName
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      imageProbePodName(cluster, image),
			Namespace: cluster.Namespace,
			Labels:    map[string]string{imageProbeLabel: cluster.Name},
		},
		Status: corev1.PodStatus{Phase: phase, ContainerStatuses: []corev1.ContainerStatus{status}},
	}
}

func TestPodImageProberReadsTheResult(t *testing.T) {
	t.Parallel()
	const image = "registry.example/mysql:8.4"
	cluster := baseCluster()
	pod := probePod(cluster, image, corev1.PodSucceeded, corev1.ContainerStatus{
		ImageID: "registry.example/mysql@sha256:abcd",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			Message:    `{"flavor":"mysql","serverVersion":"8.4.11","banner":"mysqld  Ver 8.4.11-11"}`,
			FinishedAt: metav1.Now(),
		}},
	})
	stale := probePod(cluster, "registry.example/mysql:8.0", corev1.PodPending, corev1.ContainerStatus{})
	prober, c, cluster := probeReconcilerFixture(t, pod, stale)
	ctx := context.Background()
	exists := func(name string) bool {
		err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, &corev1.Pod{})
		if err != nil && !apierrors.IsNotFound(err) {
			t.Fatal(err)
		}
		return err == nil
	}

	want := mysqlv1alpha1.ImageInfo{Image: image, ImageID: "registry.example/mysql@sha256:abcd", Flavor: mysqlv1alpha1.FlavorMySQL, ServerVersion: "8.4.11"}
	for range 2 {
		info, err := prober.Probe(ctx, cluster, image, "")
		if err != nil {
			t.Fatal(err)
		}
		if info == nil || *info != want {
			t.Fatalf("info = %+v, want %+v", info, want)
		}
	}
	if exists(stale.Name) {
		t.Error("the probe of an image the cluster no longer resolves to was kept")
	}
	if !exists(pod.Name) {
		t.Fatal("the probe Pod was deleted before its result was accepted")
	}
	if err := prober.Release(ctx, cluster, image); err != nil {
		t.Fatal(err)
	}
	if exists(pod.Name) {
		t.Error("the probe Pod was kept after Release")
	}
}

func TestPodImageProberProbesAnOldResultAgain(t *testing.T) {
	t.Parallel()
	const image = "registry.example/mysql:8.4"
	cluster := baseCluster()
	pod := probePod(cluster, image, corev1.PodSucceeded, corev1.ContainerStatus{State: corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{
			Message:    `{"flavor":"mysql","serverVersion":"8.4.11","banner":"mysqld  Ver 8.4.11-11"}`,
			FinishedAt: metav1.NewTime(time.Now().Add(-2 * imageProbeRetry)),
		},
	}})
	prober, c, cluster := probeReconcilerFixture(t, pod)
	info, err := prober.Probe(context.Background(), cluster, image, "")
	if info != nil || err != nil {
		t.Fatalf("probe = %+v, %v; want a new probe pending", info, err)
	}
	err = c.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: pod.Name}, &corev1.Pod{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("old probe Pod: %v, want it deleted so the image is probed again", err)
	}
}

// A rejected image must not be probed again on every reconcile: the probe Pod
// holding the result stays, so the next reconcile reads it instead of creating
// a new one, which would trigger yet another reconcile.
func TestResolveTargetImageKeepsTheProbeOfARejectedImage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, tc := range []struct {
		image    string
		accepted bool
	}{
		{image: "registry.example/mysql:9.6", accepted: true},
		{image: "registry.example/mysql:9.0", accepted: false},
	} {
		t.Run(tc.image, func(t *testing.T) {
			t.Parallel()
			cluster := baseCluster()
			cluster.Spec.ImageName = tc.image
			pod := probePod(cluster, tc.image, corev1.PodSucceeded, corev1.ContainerStatus{State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					Message:    `{"flavor":"mysql","serverVersion":"9.6.0","banner":"mysqld  Ver 9.6.0-1"}`,
					FinishedAt: metav1.Now(),
				},
			}})
			prober, c, _ := probeReconcilerFixture(t, pod)
			r := &ClusterReconciler{Client: c, Scheme: c.Scheme(), ImageProber: prober}

			for range 2 {
				info, _, err := r.resolveTargetImage(ctx, cluster, engine.MustForFlavor(engine.FlavorMySQL), tc.image)
				if tc.accepted {
					if err != nil || info == nil || info.ServerVersion != "9.6.0" {
						t.Fatalf("resolveTargetImage = %+v, %v; want 9.6.0 accepted", info, err)
					}
					break
				}
				if _, ok := errors.AsType[*imageRejectedError](err); !ok {
					t.Fatalf("resolveTargetImage error = %v, want a rejection", err)
				}
			}
			err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: pod.Name}, &corev1.Pod{})
			if tc.accepted != apierrors.IsNotFound(err) {
				t.Fatalf("probe Pod after resolve: %v; accepted=%v", err, tc.accepted)
			}
		})
	}
}

func TestPodImageProberReportsFailures(t *testing.T) {
	t.Parallel()
	const image = "registry.example/mysql:8.4"
	cluster := baseCluster()
	cases := []struct {
		name   string
		pod    *corev1.Pod
		reason string
	}{
		{
			name: "image cannot be pulled",
			pod: probePod(cluster, image, corev1.PodPending, corev1.ContainerStatus{State: corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "unauthorized"},
			}}),
			reason: reasonImagePullFailed,
		},
		{
			name: "server binary failed",
			pod: probePod(cluster, image, corev1.PodFailed, corev1.ContainerStatus{State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Message: "running mysqld --version: no such file", FinishedAt: metav1.Now()},
			}}),
			reason: reasonImageProbeFailed,
		},
		{
			name: "result is not a probe result",
			pod: probePod(cluster, image, corev1.PodSucceeded, corev1.ContainerStatus{State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{Message: "hello", FinishedAt: metav1.Now()},
			}}),
			reason: reasonImageProbeFailed,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			prober, _, cluster := probeReconcilerFixture(t, tc.pod)
			_, err := prober.Probe(context.Background(), cluster, image, "")
			if rejected, ok := errors.AsType[*imageRejectedError](err); !ok || rejected.Reason != tc.reason {
				t.Fatalf("error = %v, want a %s rejection", err, tc.reason)
			}
		})
	}
}

func TestPodImageProberRetriesAnOldFailure(t *testing.T) {
	t.Parallel()
	const image = "registry.example/mysql:8.4"
	cluster := baseCluster()
	pod := probePod(cluster, image, corev1.PodFailed, corev1.ContainerStatus{State: corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, FinishedAt: metav1.NewTime(time.Now().Add(-2 * imageProbeRetry))},
	}})
	prober, c, cluster := probeReconcilerFixture(t, pod)
	if _, err := prober.Probe(context.Background(), cluster, image, ""); err == nil {
		t.Fatal("expected the failure to be reported")
	}
	err := c.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: pod.Name}, &corev1.Pod{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("old failed probe Pod: %v, want it deleted so the image is probed again", err)
	}
}

func TestClustersUsingCatalog(t *testing.T) {
	t.Parallel()
	cluster := func(name, ns, kind, catalog string) *mysqlv1alpha1.Cluster {
		c := baseCluster()
		c.Name, c.Namespace = name, ns
		if catalog != "" {
			c.Spec.ImageCatalogRef = &mysqlv1alpha1.ImageCatalogRef{
				TypedLocalObjectReference: corev1.TypedLocalObjectReference{Name: catalog, Kind: kind},
				Series:                    "8.4",
			}
		}
		return c
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		cluster("a", "default", "", "images"),
		cluster("b", "default", catalogKindNamespaced, "images"),
		cluster("c", "other", catalogKindNamespaced, "images"),
		cluster("d", "default", catalogKindCluster, "images"),
		cluster("e", "other", catalogKindCluster, "images"),
		cluster("f", "default", catalogKindNamespaced, "others"),
		cluster("g", "default", "", ""),
	).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	names := func(kind string, catalog client.Object) string {
		requests := r.clustersUsingCatalog(kind)(context.Background(), catalog)
		out := make([]string, 0, len(requests))
		for _, req := range requests {
			out = append(out, req.Namespace+"/"+req.Name)
		}
		return strings.Join(out, ",")
	}
	namespaced := &mysqlv1alpha1.ImageCatalog{ObjectMeta: metav1.ObjectMeta{Name: "images", Namespace: "default"}}
	if got := names(catalogKindNamespaced, namespaced); got != "default/a,default/b" {
		t.Errorf("ImageCatalog default/images -> %s", got)
	}
	clusterScoped := &mysqlv1alpha1.ClusterImageCatalog{ObjectMeta: metav1.ObjectMeta{Name: "images"}}
	if got := names(catalogKindCluster, clusterScoped); got != "default/d,other/e" {
		t.Errorf("ClusterImageCatalog images -> %s", got)
	}
}

func TestResolveImageRejectsClusterCatalogWhenNamespaced(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	catalog := &mysqlv1alpha1.ClusterImageCatalog{
		ObjectMeta: metav1.ObjectMeta{Name: "images"},
		Spec: mysqlv1alpha1.ImageCatalogSpec{Images: []mysqlv1alpha1.CatalogImage{
			{Series: "8.4", Image: "example.com/mysql:8.4"},
		}},
	}
	cluster := baseCluster()
	cluster.Spec.ImageName = ""
	cluster.Spec.ImageCatalogRef = &mysqlv1alpha1.ImageCatalogRef{
		TypedLocalObjectReference: corev1.TypedLocalObjectReference{Name: "images", Kind: catalogKindCluster},
		Series:                    "8.4",
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(catalog).Build()

	r := &ClusterReconciler{Client: c, Scheme: scheme}
	if got, err := r.resolveImage(context.Background(), cluster, nil); err != nil || got != "example.com/mysql:8.4" {
		t.Fatalf("cluster-wide resolveImage = %q, %v", got, err)
	}

	r.Namespaced = true
	if _, err := r.resolveImage(context.Background(), cluster, nil); err == nil || !strings.Contains(err.Error(), "namespaced operator") {
		t.Fatalf("namespaced resolveImage error = %v, want a namespaced-operator error", err)
	}
}
