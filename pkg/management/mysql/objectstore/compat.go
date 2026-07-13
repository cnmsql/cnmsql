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

package objectstore

import (
	"errors"
	"net/http"

	"github.com/minio/minio-go/v7"
)

// asErrorResponse extracts the S3 error response from err, unwrapping the
// context this package adds to it. minio.ToErrorResponse only type-asserts, so
// it would miss a wrapped one.
func asErrorResponse(err error) (minio.ErrorResponse, bool) {
	var response minio.ErrorResponse
	if errors.As(err, &response) {
		return response, true
	}
	return minio.ErrorResponse{}, false
}

// isNotFound reports whether err is an object-store "this key is not there"
// answer.
//
// The S3 API has no single spelling for it: GET of a missing key returns
// NoSuchKey, HEAD returns a bodyless 404 the SDK surfaces as NotFound, and
// several compatible stores (Ceph RGW, B2) return 404 with an empty or
// provider-specific code. Keying off the status code and treating the error
// codes as a secondary signal keeps a benign miss from being reported as a hard
// failure, which is what turns a routine retention pass into a stuck backup.
func isNotFound(err error) bool {
	response, ok := asErrorResponse(err)
	if !ok {
		return false
	}
	if response.StatusCode == http.StatusNotFound {
		return true
	}
	switch response.Code {
	case minio.NoSuchKey, minio.NoSuchBucket, "NotFound", "NoSuchVersion":
		return true
	default:
		return false
	}
}

// isUnsupportedListV2 reports whether err is an endpoint telling us it does not
// implement ListObjectsV2 (list-type=2), as the GCS XML interop API does. Such a
// store answers the unknown query parameter with a 400/501 rather than listing,
// so the caller retries with the V1 listing every provider implements.
//
// A 403 is deliberately not treated as unsupported: that is a credentials or
// bucket-policy problem, and retrying it under V1 would only hide it.
func isUnsupportedListV2(err error) bool {
	response, ok := asErrorResponse(err)
	if !ok {
		return false
	}
	switch response.StatusCode {
	case http.StatusBadRequest, http.StatusNotImplemented:
		return true
	}
	switch response.Code {
	case "NotImplemented", "InvalidArgument", "InvalidRequest", "UnsupportedArgument":
		return true
	default:
		return false
	}
}
