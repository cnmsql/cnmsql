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
	"fmt"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/version"
)

// Probe Pods never run in unit tests (fake client, envtest), so every
// reconciler built in this package probes with tagImageProber unless a test
// sets ImageProber itself.
func init() {
	newImageProber = func(*ClusterReconciler) ImageProber { return tagImageProber{} }
}

// testImageVersions is what the test images "contain", keyed by tag.
var testImageVersions = map[string]string{
	"8.0":   mysqlDefaultServerVersion,
	"8.4":   "8.4.0",
	"9.x":   "9.6.0",
	"10.11": "10.11.8",
	"11.4":  "11.4.3",
	"11.8":  "11.8.2",
	"12.3":  "12.3.0",
}

// tagImageProber stands in for the probe Pods: it reads the version from the
// image tag, through testImageVersions or the tag itself, and reports the
// cluster's own flavor.
type tagImageProber struct{}

func (tagImageProber) Probe(_ context.Context, cluster *mysqlv1alpha1.Cluster, image, _ string) (*mysqlv1alpha1.ImageInfo, error) {
	tag := imageTag(image)
	serverVersion, ok := testImageVersions[tag]
	if !ok {
		v, err := version.Parse(tag)
		if err != nil {
			return nil, &imageRejectedError{Reason: reasonImageProbeFailed, Message: fmt.Sprintf("test image %s has no version", image)}
		}
		serverVersion = fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
	}
	return &mysqlv1alpha1.ImageInfo{
		Image:         image,
		ImageID:       image,
		Flavor:        cluster.ResolvedFlavor(),
		ServerVersion: serverVersion,
	}, nil
}

// fixedImageProber returns the same result for every image.
type fixedImageProber struct {
	info *mysqlv1alpha1.ImageInfo
	err  error
}

func (p fixedImageProber) Probe(context.Context, *mysqlv1alpha1.Cluster, string, string) (*mysqlv1alpha1.ImageInfo, error) {
	if p.info == nil {
		return nil, p.err
	}
	info := *p.info
	return &info, p.err
}
