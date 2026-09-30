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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/imageprobe"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/version"
)

// Image version discovery (design 033). The operator never guesses what an
// image contains from its tag: it runs the image in a probe Pod, which reports
// the flavor and server version its server binary knows, validates the result
// against the cluster, and only then rolls the instances onto it.

const (
	// imageProbeLabel marks a probe Pod with its cluster's name. Probe Pods do
	// not carry the cluster label, which selects instance Pods.
	imageProbeLabel = "mysql.cnmsql.co/image-probe"
	// imageProbeContainerName is the probe Pod's container running the image.
	imageProbeContainerName = "probe"
	// imageProbeDeadline bounds a probe Pod's run, image pull included.
	imageProbeDeadline = int64(300)
	// imageProbeRetry is how long a finished probe Pod whose result was not
	// accepted (it failed, or the image it reported was rejected) is kept,
	// reporting why, before it is deleted so the next reconcile probes the
	// image again.
	imageProbeRetry = 5 * time.Minute

	reasonImageProbed          = "Probed"
	reasonImageProbing         = "Probing"
	reasonImagePullFailed      = "ImagePullFailed"
	reasonImageProbeFailed     = "ImageProbeFailed"
	reasonImageFlavorMismatch  = "ImageFlavorMismatch"
	reasonImageSeriesMismatch  = "ImageSeriesMismatch"
	reasonImageUnsupportedMove = "UnsupportedUpgrade"
)

// ImageProber learns what an instance image contains by running it.
type ImageProber interface {
	// Probe returns what image contains. While the probe is still running it
	// returns a nil result and no error. An *imageRejectedError means the image
	// cannot be used as is (it cannot be pulled, or its server binary failed);
	// any other error is transient.
	Probe(ctx context.Context, cluster *mysqlv1alpha1.Cluster, image, operatorImage string) (*mysqlv1alpha1.ImageInfo, error)

	// Release discards the probe of an image the cluster accepted. Until
	// then Probe keeps returning the same result, so a rejected image is not
	// probed again on every reconcile.
	Release(ctx context.Context, cluster *mysqlv1alpha1.Cluster, image string) error
}

// imageRejectedError is an image the cluster cannot move to. The cluster stays
// on its previous target image, or is blocked when it has none.
type imageRejectedError struct {
	Reason  string
	Message string
}

func (e *imageRejectedError) Error() string { return e.Message }

// imageProbingError is a cluster that has no target image yet and waits for
// its first probe. It keeps provisioning and is looked at again.
type imageProbingError struct {
	Image string
}

func (e *imageProbingError) Error() string {
	return fmt.Sprintf("Probing image %s", e.Image)
}

// newImageProber builds the default prober. Tests replace it.
var newImageProber = func(r *ClusterReconciler) ImageProber {
	return &podImageProber{Client: r.Client, Scheme: r.Scheme}
}

func (r *ClusterReconciler) imageProber() ImageProber {
	if r.ImageProber == nil {
		r.ImageProber = newImageProber(r)
	}
	return r.ImageProber
}

// imageDecision is what resolveTargetImage concluded about the resolved image,
// for Reconcile to record: the ImageReady condition and, when a new image was
// accepted, status.targetImage. buildPlan itself writes nothing.
type imageDecision struct {
	accepted *mysqlv1alpha1.ImageInfo
	status   metav1.ConditionStatus
	reason   string
	message  string
}

// resolveTargetImage returns what the cluster's instances should run: the
// resolved image once it is probed and valid, else the previous target image.
// A cluster with no previous target image gets an *imageProbingError while
// its first image is probed, and the *imageRejectedError when it is rejected.
func (r *ClusterReconciler) resolveTargetImage(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	eng engine.Engine,
	image string,
) (*mysqlv1alpha1.ImageInfo, imageDecision, error) {
	previous := cluster.Status.TargetImage
	if previous != nil && previous.Image == image {
		return previous, imageDecision{status: metav1.ConditionTrue, reason: reasonImageProbed, message: imageSummary(previous)}, nil
	}

	info, err := r.imageProber().Probe(ctx, cluster, image, r.OperatorImageName)
	if err == nil && info != nil {
		err = validateTargetImage(cluster, eng, previous, info)
	}
	var rejected *imageRejectedError
	switch {
	case errors.As(err, &rejected):
		if previous == nil {
			return nil, imageDecision{}, err
		}
		return previous, imageDecision{
			status:  metav1.ConditionFalse,
			reason:  rejected.Reason,
			message: fmt.Sprintf("%s; staying on %s", rejected.Message, previous.Image),
		}, nil
	case err != nil:
		return nil, imageDecision{}, err
	case info == nil:
		if previous == nil {
			return nil, imageDecision{}, &imageProbingError{Image: image}
		}
		return previous, imageDecision{
			status:  metav1.ConditionFalse,
			reason:  reasonImageProbing,
			message: fmt.Sprintf("Probing image %s; staying on %s until it is validated", image, previous.Image),
		}, nil
	}
	if err := r.imageProber().Release(ctx, cluster, image); err != nil {
		return nil, imageDecision{}, err
	}
	return info, imageDecision{accepted: info, status: metav1.ConditionTrue, reason: reasonImageProbed, message: imageSummary(info)}, nil
}

// recordImageDecision writes an image decision to the status, and emits an
// event when the image is accepted or rejected. It skips the write when
// nothing would change.
func (r *ClusterReconciler) recordImageDecision(ctx context.Context, cluster *mysqlv1alpha1.Cluster, d imageDecision) error {
	current := apimeta.FindStatusCondition(cluster.Status.Conditions, mysqlv1alpha1.ConditionImageReady)
	if d.accepted == nil && current != nil && current.Status == d.status && current.Reason == d.reason && current.Message == d.message {
		return nil
	}
	if r.Recorder != nil {
		switch {
		case d.accepted != nil:
			r.Recorder.Event(cluster, corev1.EventTypeNormal, "ImageProbed", d.message)
		case d.status == metav1.ConditionFalse && d.reason != reasonImageProbing:
			r.Recorder.Event(cluster, corev1.EventTypeWarning, d.reason, d.message)
		}
	}
	if d.accepted != nil {
		logf.FromContext(ctx).Info("Accepted probed image", "image", d.accepted.Image, "imageID", d.accepted.ImageID,
			"flavor", d.accepted.Flavor, "serverVersion", d.accepted.ServerVersion)
	}
	return r.updateStatus(ctx, cluster, func(s *mysqlv1alpha1.ClusterStatus) {
		if d.accepted != nil {
			s.TargetImage = d.accepted.DeepCopy()
		}
		apimeta.SetStatusCondition(&s.Conditions, metav1.Condition{
			Type:               mysqlv1alpha1.ConditionImageReady,
			Status:             d.status,
			Reason:             d.reason,
			Message:            d.message,
			ObservedGeneration: cluster.Generation,
		})
	})
}

// imageDecisionForError is the condition to record for a plan that failed on
// its image.
func imageDecisionForError(err error) (imageDecision, bool) {
	if rejected, ok := errors.AsType[*imageRejectedError](err); ok {
		return imageDecision{status: metav1.ConditionFalse, reason: rejected.Reason, message: rejected.Message}, true
	}
	if probing, ok := errors.AsType[*imageProbingError](err); ok {
		return imageDecision{status: metav1.ConditionFalse, reason: reasonImageProbing, message: probing.Error()}, true
	}
	return imageDecision{}, false
}

func imageSummary(info *mysqlv1alpha1.ImageInfo) string {
	return fmt.Sprintf("Image %s runs %s %s", info.Image, info.Flavor, info.ServerVersion)
}

// validateTargetImage checks a probed image against the cluster before any
// instance moves to it: the flavor must be the cluster's, the series must be
// the one the catalog entry (or the image tag) names, and the move from the
// previous target image must be a supported upgrade. These are the checks
// admission cannot make, since it never sees what an image contains.
func validateTargetImage(cluster *mysqlv1alpha1.Cluster, eng engine.Engine, previous, info *mysqlv1alpha1.ImageInfo) error {
	if want := cluster.ResolvedFlavor(); info.Flavor != want {
		return &imageRejectedError{
			Reason:  reasonImageFlavorMismatch,
			Message: fmt.Sprintf("Image %s runs %s, but the cluster's flavor is %s", info.Image, info.Flavor, want),
		}
	}
	probed, err := eng.ParseServerVersion(info.ServerVersion)
	if err != nil {
		return &imageRejectedError{Reason: reasonImageProbeFailed, Message: err.Error()}
	}
	if info.Flavor == mysqlv1alpha1.FlavorMySQL && !probed.AtLeast(5, 7, 0) {
		return &imageRejectedError{
			Reason:  reasonImageUnsupportedMove,
			Message: fmt.Sprintf("Image %s runs MySQL %s; MySQL 5.6 and older are not supported", info.Image, info.ServerVersion),
		}
	}
	if named, source := namedSeries(cluster, info.Image); source != "" && named != probed.Series() {
		return &imageRejectedError{
			Reason: reasonImageSeriesMismatch,
			Message: fmt.Sprintf("Image %s runs %s, which is not series %d.%d named by %s",
				info.Image, info.ServerVersion, named.Major, named.Minor, source),
		}
	}
	if previous == nil {
		return nil
	}
	from, err := eng.ParseServerVersion(previous.ServerVersion)
	if err != nil {
		return nil
	}
	if err := eng.CheckUpgrade(from, probed); err != nil {
		return &imageRejectedError{
			Reason:  reasonImageUnsupportedMove,
			Message: fmt.Sprintf("Cannot move from %s (%s) to %s (%s): %v", previous.Image, previous.ServerVersion, info.Image, info.ServerVersion, err),
		}
	}
	return nil
}

// namedSeries is the series the spec names for its image: the catalog entry's
// series, else the image tag's when the tag starts with one. source describes
// where it came from and is empty when nothing names a series (a digest-only
// reference, a tag such as "latest").
func namedSeries(cluster *mysqlv1alpha1.Cluster, image string) (version.Version, string) {
	if ref := cluster.Spec.ImageCatalogRef; ref != nil {
		if v, err := version.Parse(ref.Series); err == nil {
			return v.Series(), fmt.Sprintf("the %s %q entry", catalogKind(ref), ref.Series)
		}
		return version.Version{}, ""
	}
	if tag := imageTag(image); tag != "" {
		if v, err := version.Parse(tag); err == nil {
			return v.Series(), fmt.Sprintf("the image tag %q", tag)
		}
	}
	return version.Version{}, ""
}

func catalogKind(ref *mysqlv1alpha1.ImageCatalogRef) string {
	if ref.Kind == "" {
		return catalogKindNamespaced
	}
	return ref.Kind
}

const (
	catalogKindNamespaced = "ImageCatalog"
	catalogKindCluster    = "ClusterImageCatalog"
)

// clustersUsingCatalog maps an ImageCatalog (kind catalogKindNamespaced) or
// ClusterImageCatalog event to the clusters that resolve their image from it,
// so a catalog revision reaches them without waiting for a resync.
func (r *ClusterReconciler) clustersUsingCatalog(kind string) handler.MapFunc {
	return func(ctx context.Context, catalog client.Object) []reconcile.Request {
		var opts []client.ListOption
		if kind == catalogKindNamespaced {
			opts = append(opts, client.InNamespace(catalog.GetNamespace()))
		}
		clusters := &mysqlv1alpha1.ClusterList{}
		if err := r.List(ctx, clusters, opts...); err != nil {
			logf.FromContext(ctx).Error(err, "Failed to list Clusters for an image catalog change", "kind", kind, "name", catalog.GetName())
			return nil
		}
		var requests []reconcile.Request
		for i := range clusters.Items {
			ref := clusters.Items[i].Spec.ImageCatalogRef
			if ref == nil || ref.Name != catalog.GetName() || catalogKind(ref) != kind {
				continue
			}
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&clusters.Items[i])})
		}
		return requests
	}
}

// podImageProber probes an image by running `manager instance probe` in it, in
// a short-lived Pod owned by the cluster, and reading the result from the
// container's termination message.
type podImageProber struct {
	client.Client
	Scheme *runtime.Scheme
}

func (p *podImageProber) Probe(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	image, operatorImage string,
) (*mysqlv1alpha1.ImageInfo, error) {
	name := imageProbePodName(cluster, image)
	if err := p.deleteStaleProbes(ctx, cluster, name); err != nil {
		return nil, err
	}

	pod := &corev1.Pod{}
	err := p.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, pod)
	if apierrors.IsNotFound(err) {
		pod = imageProbePod(cluster, name, image, operatorImage)
		if err := controllerutil.SetControllerReference(cluster, pod, p.Scheme); err != nil {
			return nil, err
		}
		logf.FromContext(ctx).Info("Creating image probe Pod", "pod", name, "image", image)
		if err := p.Create(ctx, pod); err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, err
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	status := probeContainerStatus(pod)
	switch pod.Status.Phase {
	case corev1.PodSucceeded:
		// The Pod stays until Release, when the result is accepted: deleting
		// it here would have the next reconcile, which the deletion
		// triggers, probe a rejected image again straight away.
		if status == nil || status.State.Terminated == nil {
			return nil, &imageRejectedError{Reason: reasonImageProbeFailed, Message: fmt.Sprintf("Image probe Pod %s reported no result", name)}
		}
		if time.Since(status.State.Terminated.FinishedAt.Time) > imageProbeRetry {
			// An old result that was never accepted: probe again, in case
			// the image behind the reference has changed.
			if err := p.Delete(ctx, pod); client.IgnoreNotFound(err) != nil {
				return nil, err
			}
			return nil, nil
		}
		result, err := imageprobe.Decode(status.State.Terminated.Message)
		if err != nil {
			return nil, &imageRejectedError{Reason: reasonImageProbeFailed, Message: fmt.Sprintf("Image %s: %v", image, err)}
		}
		return &mysqlv1alpha1.ImageInfo{
			Image:         image,
			ImageID:       status.ImageID,
			Flavor:        mysqlv1alpha1.Flavor(result.Flavor),
			ServerVersion: result.ServerVersion,
		}, nil
	case corev1.PodFailed:
		finished := pod.CreationTimestamp.Time
		detail := pod.Status.Message
		if status != nil && status.State.Terminated != nil {
			finished = status.State.Terminated.FinishedAt.Time
			if msg := status.State.Terminated.Message; msg != "" {
				detail = msg
			}
		}
		if time.Since(finished) > imageProbeRetry {
			if err := p.Delete(ctx, pod); client.IgnoreNotFound(err) != nil {
				return nil, err
			}
		}
		return nil, &imageRejectedError{
			Reason:  reasonImageProbeFailed,
			Message: fmt.Sprintf("Image %s could not be probed: %s", image, firstLine(detail)),
		}
	}
	if status != nil && status.State.Waiting != nil {
		switch status.State.Waiting.Reason {
		case "ErrImagePull", "ImagePullBackOff", "InvalidImageName", "ErrImageNeverPull":
			return nil, &imageRejectedError{
				Reason:  reasonImagePullFailed,
				Message: fmt.Sprintf("Image %s cannot be pulled: %s", image, status.State.Waiting.Message),
			}
		}
	}
	return nil, nil
}

// Release deletes the probe Pod of an accepted image.
func (p *podImageProber) Release(ctx context.Context, cluster *mysqlv1alpha1.Cluster, image string) error {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: cluster.Namespace, Name: imageProbePodName(cluster, image)}}
	return client.IgnoreNotFound(p.Delete(ctx, pod))
}

// deleteStaleProbes removes the cluster's probe Pods for images it no longer
// resolves to.
func (p *podImageProber) deleteStaleProbes(ctx context.Context, cluster *mysqlv1alpha1.Cluster, keep string) error {
	pods := &corev1.PodList{}
	if err := p.List(ctx, pods, client.InNamespace(cluster.Namespace), client.MatchingLabels{imageProbeLabel: cluster.Name}); err != nil {
		return err
	}
	for i := range pods.Items {
		if pods.Items[i].Name == keep {
			continue
		}
		if err := p.Delete(ctx, &pods.Items[i]); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
}

func probeContainerStatus(pod *corev1.Pod) *corev1.ContainerStatus {
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].Name == imageProbeContainerName {
			return &pod.Status.ContainerStatuses[i]
		}
	}
	return nil
}

func firstLine(s string) string {
	for i, c := range s {
		if c == '\n' {
			return s[:i]
		}
	}
	return s
}

// imageProbePodName is stable per cluster and image, so a probe survives
// operator restarts and is not duplicated.
func imageProbePodName(cluster *mysqlv1alpha1.Cluster, image string) string {
	sum := sha256.Sum256([]byte(image))
	return fmt.Sprintf("%s-image-%s", cluster.Name, hex.EncodeToString(sum[:])[:10])
}

// imageProbePod runs `manager instance probe` in image. It pulls with the
// cluster's pull policy and secrets and schedules like an instance (node
// selector, node affinity, tolerations), so it runs on the architecture and
// with the registry access the instances will have. It needs no API access,
// no volume but the manager's scratch space, and very little CPU and memory.
func imageProbePod(cluster *mysqlv1alpha1.Cluster, name, image, operatorImage string) *corev1.Pod {
	// Like instance Pods, fall back to the probed image when the operator does
	// not know its own image (it always does when deployed).
	if operatorImage == "" {
		operatorImage = image
	}
	deadline := imageProbeDeadline
	automount := false
	resources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("10m"),
			corev1.ResourceMemory: resource.MustParse("32Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("200m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		},
	}
	scratch := []corev1.VolumeMount{{Name: scratchVolumeName, MountPath: scratchMountPath}}
	var nodeAffinity *corev1.Affinity
	if cluster.Spec.Affinity.NodeAffinity != nil {
		nodeAffinity = &corev1.Affinity{NodeAffinity: cluster.Spec.Affinity.NodeAffinity}
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				imageProbeLabel:                cluster.Name,
				appLabelKey:                    appLabelValue,
				"app.kubernetes.io/managed-by": appLabelValue,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyNever,
			ActiveDeadlineSeconds:        &deadline,
			AutomountServiceAccountToken: &automount,
			Volumes: []corev1.Volume{
				{Name: scratchVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			},
			InitContainers: []corev1.Container{{
				Name:            bootstrapControllerName,
				Image:           operatorImage,
				ImagePullPolicy: cluster.Spec.ImagePullPolicy,
				Command:         []string{operatorManagerBinary},
				Args:            []string{managerBootstrapCmd, managerBinary},
				VolumeMounts:    scratch,
				Resources:       resources,
				SecurityContext: cluster.Spec.SecurityContext,
			}},
			Containers: []corev1.Container{{
				Name:                     imageProbeContainerName,
				Image:                    image,
				ImagePullPolicy:          cluster.Spec.ImagePullPolicy,
				Command:                  []string{managerBinary},
				Args:                     []string{managerInstanceCmd, "probe", "--mysqld=" + mysqldBinary},
				VolumeMounts:             scratch,
				Resources:                resources,
				SecurityContext:          cluster.Spec.SecurityContext,
				TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
			}},
			NodeSelector:      cluster.Spec.Affinity.NodeSelector,
			Affinity:          nodeAffinity,
			Tolerations:       cluster.Spec.Affinity.Tolerations,
			PriorityClassName: cluster.Spec.PriorityClassName,
			SchedulerName:     cluster.Spec.SchedulerName,
			SecurityContext:   podSecurityContext(cluster),
		},
	}
	for _, pullSecret := range cluster.Spec.ImagePullSecrets {
		pod.Spec.ImagePullSecrets = append(pod.Spec.ImagePullSecrets, corev1.LocalObjectReference{Name: pullSecret.Name})
	}
	return pod
}
