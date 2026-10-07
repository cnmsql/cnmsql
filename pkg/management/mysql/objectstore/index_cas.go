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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/minio/minio-go/v7"
)

var (
	// ErrPreconditionFailed is returned by a conditional write that lost to a
	// concurrent writer: the object changed (or appeared) since it was read.
	ErrPreconditionFailed = errors.New("objectstore: object changed since it was read")
	// ErrIndexContention is returned when an index update kept losing to other
	// writers and gave up.
	ErrIndexContention = errors.New("objectstore: archive index kept changing under the update")
)

// indexUpdateAttempts bounds how often UpdateArchiveIndex re-reads the index
// after losing a race. Three writers exist (the primary's archiver, a former
// primary's drain, the operator's retention), each writing at most every few
// seconds, so a handful of attempts always suffices in practice.
const indexUpdateAttempts = 8

// VersionedStore reads an object with a version token and writes it back only
// if that version is still current.
type VersionedStore interface {
	// GetJSONVersion decodes bucket/key into v and returns its version token;
	// found is false (and the token empty) when the object does not exist.
	GetJSONVersion(ctx context.Context, bucket, key string, v any) (etag string, found bool, err error)
	// PutJSONIf writes v only if the object is still at version etag, or, with
	// an empty etag, only if it does not exist. A lost race is
	// ErrPreconditionFailed.
	PutJSONIf(ctx context.Context, bucket, key string, v any, etag string) error
}

// UpdateArchiveIndex applies mutate to the current archive index and writes the
// result only if no other writer changed the index in between. A lost race
// re-reads the index and re-applies mutate to what the winner wrote, so no
// writer's change is lost. mutate reports whether there is anything to write;
// it sees exists=false (and a zero index) when no index exists yet.
func UpdateArchiveIndex(
	ctx context.Context, store VersionedStore, bucket, key string,
	mutate func(idx *ArchiveIndex, exists bool) (bool, error),
) error {
	for range indexUpdateAttempts {
		var idx ArchiveIndex
		etag, exists, err := store.GetJSONVersion(ctx, bucket, key, &idx)
		if err != nil {
			return fmt.Errorf("reading archive index: %w", err)
		}
		write, err := mutate(&idx, exists)
		if err != nil || !write {
			return err
		}
		err = store.PutJSONIf(ctx, bucket, key, &idx, etag)
		if errors.Is(err, ErrPreconditionFailed) {
			continue
		}
		if err != nil {
			return fmt.Errorf("writing archive index: %w", err)
		}
		return nil
	}
	return fmt.Errorf("%w: s3://%s/%s", ErrIndexContention, bucket, key)
}

// GetJSONVersion implements VersionedStore over the object's ETag.
func (c *Client) GetJSONVersion(ctx context.Context, bucket, key string, v any) (string, bool, error) {
	obj, err := c.mc.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return "", false, fmt.Errorf("opening s3://%s/%s: %w", bucket, key, err)
	}
	defer func() { _ = obj.Close() }()
	info, err := obj.Stat()
	if err != nil {
		if IsNotFound(err) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("stat s3://%s/%s: %w", bucket, key, err)
	}
	payload, err := io.ReadAll(obj)
	if err != nil {
		return "", false, fmt.Errorf("reading s3://%s/%s: %w", bucket, key, err)
	}
	if err := json.Unmarshal(payload, v); err != nil {
		return "", false, fmt.Errorf("decoding s3://%s/%s: %w", bucket, key, err)
	}
	return strings.Trim(info.ETag, `"`), true, nil
}

// PutJSONIf implements VersionedStore with S3 conditional writes: If-Match on
// the read ETag, or If-None-Match: * to create. An endpoint that does not
// implement conditional PUTs (501 NotImplemented) gets an unconditional write,
// the behavior before conditional writes, and is not asked again; one that
// silently ignores the headers behaves the same way.
func (c *Client) PutJSONIf(ctx context.Context, bucket, key string, v any, etag string) error {
	payload, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling object %s: %w", key, err)
	}
	put := func(conditional bool) error {
		opts := c.putOptions("application/json")
		if conditional {
			if etag == "" {
				opts.SetMatchETagExcept("*")
			} else {
				opts.SetMatchETag(etag)
			}
		}
		_, err := c.mc.PutObject(ctx, bucket, key, strings.NewReader(string(payload)), int64(len(payload)), opts)
		return err
	}
	conditional := !c.unconditional.Load()
	err = put(conditional)
	if err != nil && conditional && conditionalWritesUnsupported(err) {
		c.unconditional.Store(true)
		err = put(false)
	}
	if err == nil {
		return nil
	}
	if resp := minio.ToErrorResponse(err); resp.Code == minio.PreconditionFailed ||
		resp.StatusCode == http.StatusPreconditionFailed || resp.Code == "ConditionalRequestConflict" {
		return fmt.Errorf("%w: s3://%s/%s", ErrPreconditionFailed, bucket, key)
	}
	return fmt.Errorf("uploading s3://%s/%s: %w", bucket, key, err)
}

// conditionalWritesUnsupported reports whether a PUT failed because the
// endpoint does not implement its If-Match / If-None-Match headers.
func conditionalWritesUnsupported(err error) bool {
	resp := minio.ToErrorResponse(err)
	return resp.StatusCode == http.StatusNotImplemented || resp.Code == "NotImplemented"
}
