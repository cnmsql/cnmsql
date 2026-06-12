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

package objectstore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
)

// Environment variables consumed by NewClientFromEnv. The backup/recovery
// workers receive these from the operator, sourcing the secret-backed ones from
// the configured S3 credentials.
const (
	EnvEndpoint         = "CNMYSQL_S3_ENDPOINT"
	EnvRegion           = "CNMYSQL_S3_REGION"
	EnvSignatureVersion = "CNMYSQL_S3_SIGNATURE_VERSION"
	EnvForcePathStyle   = "CNMYSQL_S3_FORCE_PATH_STYLE"
	EnvAccessKeyID      = "CNMYSQL_S3_ACCESS_KEY_ID"
	EnvSecretAccessKey  = "CNMYSQL_S3_SECRET_ACCESS_KEY"
	EnvSessionToken     = "CNMYSQL_S3_SESSION_TOKEN"
	// EnvBucket and EnvPath carry the (non-secret) destination bucket and key
	// prefix. The continuous archiver reads these to know where to ship binlogs;
	// the one-shot backup worker still takes them as flags.
	EnvBucket = "CNMYSQL_S3_BUCKET"
	EnvPath   = "CNMYSQL_S3_PATH"
)

// StoreFromEnv builds an S3ObjectStore destination (bucket + path) from the
// environment. Endpoint/credentials come separately via ConfigFromEnv.
func StoreFromEnv() mysqlv1alpha1.S3ObjectStore {
	return mysqlv1alpha1.S3ObjectStore{
		Bucket: os.Getenv(EnvBucket),
		Path:   os.Getenv(EnvPath),
	}
}

// Config describes how to reach an S3-compatible object store.
type Config struct {
	// Endpoint is the object-store endpoint. It may include a scheme
	// (https://... or http://...); https is assumed when none is given. An empty
	// endpoint targets AWS S3 (s3.amazonaws.com).
	Endpoint string
	// Region is the bucket region.
	Region string
	// AccessKeyID and SecretAccessKey are the static credentials. When both are
	// empty the AWS default credential chain (IAM role, env, ...) is used.
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	// SignatureV2 selects legacy AWS Signature V2 instead of the default V4.
	SignatureV2 bool
	// ForcePathStyle uses path-style bucket addressing (host/bucket/key) instead
	// of virtual-hosted style. Required by most S3-compatible stores (MinIO, ...).
	ForcePathStyle bool
}

// ConfigFromEnv builds a Config from the CNMYSQL_S3_* environment variables.
func ConfigFromEnv() Config {
	cfg := Config{
		Endpoint:        os.Getenv(EnvEndpoint),
		Region:          os.Getenv(EnvRegion),
		AccessKeyID:     os.Getenv(EnvAccessKeyID),
		SecretAccessKey: os.Getenv(EnvSecretAccessKey),
		SessionToken:    os.Getenv(EnvSessionToken),
		SignatureV2:     strings.EqualFold(os.Getenv(EnvSignatureVersion), "s3v2"),
	}
	if force, err := strconv.ParseBool(os.Getenv(EnvForcePathStyle)); err == nil {
		cfg.ForcePathStyle = force
	}
	return cfg
}

// ConfigFromStore maps an API object store plus already-resolved credential
// values into a client Config. It mirrors the env the pods receive, so the
// operator's own object-store access matches the workers'.
func ConfigFromStore(store mysqlv1alpha1.S3ObjectStore, accessKeyID, secretAccessKey, sessionToken string) Config {
	cfg := Config{
		Endpoint:        store.Endpoint,
		Region:          store.Region,
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		SessionToken:    sessionToken,
		SignatureV2:     store.SignatureVersion == mysqlv1alpha1.SignatureVersionV2,
	}
	if store.ForcePathStyle != nil {
		cfg.ForcePathStyle = *store.ForcePathStyle
	}
	return cfg
}

// Client is a thin wrapper over the S3 SDK exposing the operations the
// backup/recovery workers need.
type Client struct {
	mc *minio.Client
}

// NewClient builds an object-store client from cfg.
func NewClient(cfg Config) (*Client, error) {
	endpoint, secure, err := parseEndpoint(cfg.Endpoint)
	if err != nil {
		return nil, err
	}

	var creds *credentials.Credentials
	switch {
	case cfg.AccessKeyID == "" && cfg.SecretAccessKey == "":
		creds = credentials.NewIAM("")
	case cfg.SignatureV2:
		creds = credentials.NewStaticV2(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken)
	default:
		creds = credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken)
	}

	lookup := minio.BucketLookupAuto
	if cfg.ForcePathStyle {
		lookup = minio.BucketLookupPath
	}

	// Default the signing region so SigV4 works against S3-compatible stores
	// (e.g. MinIO) where users routinely leave the region unset.
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	mc, err := minio.New(endpoint, &minio.Options{
		Creds:        creds,
		Secure:       secure,
		Region:       region,
		BucketLookup: lookup,
	})
	if err != nil {
		return nil, fmt.Errorf("creating object-store client: %w", err)
	}
	return &Client{mc: mc}, nil
}

// NewClientFromEnv builds a client from the CNMYSQL_S3_* environment variables.
func NewClientFromEnv() (*Client, error) {
	return NewClient(ConfigFromEnv())
}

// Upload streams reader into bucket/key. A negative size streams with multipart
// uploads of an unknown total length, which is what backup archives need.
func (c *Client) Upload(
	ctx context.Context,
	bucket, key string,
	reader io.Reader,
	size int64,
	contentType string,
) error {
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	_, err := c.mc.PutObject(ctx, bucket, key, reader, size, minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		return fmt.Errorf("uploading s3://%s/%s: %w", bucket, key, err)
	}
	return nil
}

// PutJSON marshals v and uploads it as bucket/key.
func (c *Client) PutJSON(ctx context.Context, bucket, key string, v any) error {
	payload, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling object %s: %w", key, err)
	}
	_, err = c.mc.PutObject(ctx, bucket, key, strings.NewReader(string(payload)), int64(len(payload)),
		minio.PutObjectOptions{ContentType: "application/json"})
	if err != nil {
		return fmt.Errorf("uploading s3://%s/%s: %w", bucket, key, err)
	}
	return nil
}

// Download streams bucket/key into writer and returns the number of bytes copied.
func (c *Client) Download(ctx context.Context, bucket, key string, writer io.Writer) (int64, error) {
	obj, err := c.mc.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return 0, fmt.Errorf("opening s3://%s/%s: %w", bucket, key, err)
	}
	defer func() { _ = obj.Close() }()
	n, err := io.Copy(writer, obj)
	if err != nil {
		return n, fmt.Errorf("downloading s3://%s/%s: %w", bucket, key, err)
	}
	return n, nil
}

// IsEmptyPrefix reports whether no objects exist under bucket/prefix. It is used
// by the operator's empty-archive safety check, so a fresh cluster never adopts
// (and overwrites) a destination that already holds another cluster's backups.
func (c *Client) IsEmptyPrefix(ctx context.Context, bucket, prefix string) (bool, error) {
	objects := c.mc.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
		MaxKeys:   1,
	})
	object, ok := <-objects
	if !ok {
		// Channel closed without yielding anything: nothing under the prefix.
		return true, nil
	}
	if object.Err != nil {
		return false, fmt.Errorf("listing s3://%s/%s: %w", bucket, prefix, object.Err)
	}
	return false, nil
}

// Exists reports whether bucket/key exists. A not-found response is reported as
// (false, nil); any other error is returned.
func (c *Client) Exists(ctx context.Context, bucket, key string) (bool, error) {
	_, err := c.mc.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	if minio.ToErrorResponse(err).Code == "NoSuchKey" {
		return false, nil
	}
	return false, fmt.Errorf("stat s3://%s/%s: %w", bucket, key, err)
}

// GetJSON downloads bucket/key and unmarshals it into v.
func (c *Client) GetJSON(ctx context.Context, bucket, key string, v any) error {
	obj, err := c.mc.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("opening s3://%s/%s: %w", bucket, key, err)
	}
	defer func() { _ = obj.Close() }()
	payload, err := io.ReadAll(obj)
	if err != nil {
		return fmt.Errorf("reading s3://%s/%s: %w", bucket, key, err)
	}
	if err := json.Unmarshal(payload, v); err != nil {
		return fmt.Errorf("decoding s3://%s/%s: %w", bucket, key, err)
	}
	return nil
}

// parseEndpoint splits an endpoint into a host[:port] and whether TLS is used.
// An empty endpoint defaults to AWS S3 over TLS.
func parseEndpoint(endpoint string) (host string, secure bool, err error) {
	if endpoint == "" {
		return "s3.amazonaws.com", true, nil
	}
	if !strings.Contains(endpoint, "://") {
		// Bare host[:port]; default to TLS.
		return endpoint, true, nil
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", false, fmt.Errorf("parsing endpoint %q: %w", endpoint, err)
	}
	if u.Host == "" {
		return "", false, fmt.Errorf("endpoint %q has no host", endpoint)
	}
	return u.Host, u.Scheme != "http", nil
}
