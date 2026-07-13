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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/encrypt"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// Environment variables consumed by NewClientFromEnv. The backup/recovery
// workers receive these from the operator, sourcing the secret-backed ones from
// the configured S3 credentials.
const (
	EnvEndpoint         = "cnmsql_S3_ENDPOINT"
	EnvRegion           = "cnmsql_S3_REGION"
	EnvSignatureVersion = "cnmsql_S3_SIGNATURE_VERSION"
	EnvForcePathStyle   = "cnmsql_S3_FORCE_PATH_STYLE"
	EnvAccessKeyID      = "cnmsql_S3_ACCESS_KEY_ID"
	EnvSecretAccessKey  = "cnmsql_S3_SECRET_ACCESS_KEY"
	EnvSessionToken     = "cnmsql_S3_SESSION_TOKEN"
	// EnvBucket and EnvPath carry the (non-secret) destination bucket and key
	// prefix. The continuous archiver reads these to know where to ship binlogs;
	// the one-shot backup worker still takes them as flags.
	EnvBucket = "cnmsql_S3_BUCKET"
	EnvPath   = "cnmsql_S3_PATH"
	// EnvServerSideEncryption and EnvStorageClass carry the per-object write
	// options applied to every upload.
	EnvServerSideEncryption = "cnmsql_S3_SERVER_SIDE_ENCRYPTION"
	EnvStorageClass         = "cnmsql_S3_STORAGE_CLASS"
	// EnvTLSInsecure and EnvCABundle configure endpoint TLS verification. The CA
	// bundle carries the PEM itself (from the referenced secret), not a path.
	EnvTLSInsecure = "cnmsql_S3_TLS_INSECURE_SKIP_VERIFY"
	EnvCABundle    = "cnmsql_S3_CA_BUNDLE"
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
	// ServerSideEncryption selects the SSE algorithm applied to every upload:
	// "AES256" (SSE-S3) or "aws:kms" / "aws:kms:<key-id>" (SSE-KMS). Empty
	// uploads without an SSE header, which is what non-AWS providers expect.
	ServerSideEncryption string
	// StorageClass sets the x-amz-storage-class of every upload (e.g.
	// "STANDARD_IA", or a provider-specific class). Empty leaves it unset.
	StorageClass string
	// InsecureSkipVerify disables endpoint certificate verification.
	InsecureSkipVerify bool
	// CABundle is a PEM bundle used to verify the endpoint certificate, for
	// stores fronted by a private CA. It carries the PEM itself, not a path.
	CABundle string
}

// ConfigFromEnv builds a Config from the cnmsql_S3_* environment variables.
func ConfigFromEnv() Config {
	cfg := Config{
		Endpoint:             os.Getenv(EnvEndpoint),
		Region:               os.Getenv(EnvRegion),
		AccessKeyID:          os.Getenv(EnvAccessKeyID),
		SecretAccessKey:      os.Getenv(EnvSecretAccessKey),
		SessionToken:         os.Getenv(EnvSessionToken),
		SignatureV2:          strings.EqualFold(os.Getenv(EnvSignatureVersion), "s3v2"),
		ServerSideEncryption: os.Getenv(EnvServerSideEncryption),
		StorageClass:         os.Getenv(EnvStorageClass),
		CABundle:             os.Getenv(EnvCABundle),
	}
	if force, err := strconv.ParseBool(os.Getenv(EnvForcePathStyle)); err == nil {
		cfg.ForcePathStyle = force
	}
	if insecure, err := strconv.ParseBool(os.Getenv(EnvTLSInsecure)); err == nil {
		cfg.InsecureSkipVerify = insecure
	}
	return cfg
}

// StoreSecrets holds the object-store values the operator has resolved out of
// the referenced Secrets. They are passed to ConfigFromStore separately from the
// (non-secret) S3ObjectStore spec.
type StoreSecrets struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	// CABundle is the PEM read from spec.tls.caBundleSecret, when set.
	CABundle string
}

// ConfigFromStore maps an API object store plus already-resolved secret values
// into a client Config. It mirrors the env the pods receive, so the operator's
// own object-store access matches the workers'.
func ConfigFromStore(store mysqlv1alpha1.S3ObjectStore, secrets StoreSecrets) Config {
	cfg := Config{
		Endpoint:        store.Endpoint,
		Region:          store.Region,
		AccessKeyID:     secrets.AccessKeyID,
		SecretAccessKey: secrets.SecretAccessKey,
		SessionToken:    secrets.SessionToken,
		SignatureV2:     store.SignatureVersion == mysqlv1alpha1.SignatureVersionV2,
		CABundle:        secrets.CABundle,
	}
	if store.ForcePathStyle != nil {
		cfg.ForcePathStyle = *store.ForcePathStyle
	}
	if store.ServerSideEncryption != nil {
		cfg.ServerSideEncryption = *store.ServerSideEncryption
	}
	if store.StorageClass != nil {
		cfg.StorageClass = *store.StorageClass
	}
	if store.TLS != nil {
		cfg.InsecureSkipVerify = store.TLS.InsecureSkipVerify
	}
	return cfg
}

// Client is a thin wrapper over the S3 SDK exposing the operations the
// backup/recovery workers need.
type Client struct {
	mc           *minio.Client
	sse          encrypt.ServerSide
	storageClass string
	// listV1 is set once a ListObjectsV2 call has been rejected by the endpoint,
	// after which every listing uses the V1 API. See listObjects.
	listV1 atomic.Bool
}

// NewClient builds an object-store client from cfg.
func NewClient(cfg Config) (*Client, error) {
	endpoint, secure, err := parseEndpoint(cfg.Endpoint)
	if err != nil {
		return nil, err
	}

	creds := resolveCredentials(cfg)

	sse, err := parseServerSideEncryption(cfg.ServerSideEncryption)
	if err != nil {
		return nil, err
	}

	transport, err := newTransport(cfg, secure)
	if err != nil {
		return nil, err
	}

	lookup := minio.BucketLookupAuto
	if cfg.ForcePathStyle {
		lookup = minio.BucketLookupPath
	}

	mc, err := minio.New(endpoint, &minio.Options{
		Creds:        creds,
		Secure:       secure,
		Region:       signingRegion(cfg.Region, endpoint),
		BucketLookup: lookup,
		Transport:    transport,
	})
	if err != nil {
		return nil, fmt.Errorf("creating object-store client: %w", err)
	}
	return &Client{mc: mc, sse: sse, storageClass: cfg.StorageClass}, nil
}

// resolveCredentials builds the credential provider for cfg. Static keys are
// used verbatim; with none configured we fall back to the ambient AWS chain
// (environment, shared credentials file, then the instance/IRSA endpoint), which
// is what spec.credentials.inheritFromIAMRole means in practice.
func resolveCredentials(cfg Config) *credentials.Credentials {
	switch {
	case cfg.AccessKeyID != "" || cfg.SecretAccessKey != "":
		if cfg.SignatureV2 {
			return credentials.NewStaticV2(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken)
		}
		return credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken)
	default:
		return credentials.NewChainCredentials([]credentials.Provider{
			&credentials.EnvAWS{},
			&credentials.FileAWSCredentials{},
			&credentials.IAM{Client: &http.Client{Timeout: 30 * time.Second}},
		})
	}
}

// newTransport returns the HTTP transport for the client, or nil to let the SDK
// build its default. A custom one is only needed when the endpoint's TLS has to
// be trusted through a private CA or not verified at all.
func newTransport(cfg Config, secure bool) (http.RoundTripper, error) {
	if !secure || (cfg.CABundle == "" && !cfg.InsecureSkipVerify) {
		return nil, nil
	}
	transport, err := minio.DefaultTransport(secure)
	if err != nil {
		return nil, fmt.Errorf("building object-store transport: %w", err)
	}
	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	transport.TLSClientConfig.InsecureSkipVerify = cfg.InsecureSkipVerify
	if cfg.CABundle != "" {
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM([]byte(cfg.CABundle)) {
			return nil, fmt.Errorf("object-store CA bundle contains no valid PEM certificate")
		}
		transport.TLSClientConfig.RootCAs = pool
	}
	return transport, nil
}

// parseServerSideEncryption maps the spec's SSE algorithm onto an SDK
// encryption. An unknown value is rejected up front rather than being sent as an
// opaque header that most providers answer with a confusing 400.
func parseServerSideEncryption(algorithm string) (encrypt.ServerSide, error) {
	switch {
	case algorithm == "":
		return nil, nil
	case strings.EqualFold(algorithm, "AES256"), strings.EqualFold(algorithm, "aws:s3"):
		return encrypt.NewSSE(), nil
	case strings.EqualFold(algorithm, "aws:kms"):
		return encrypt.NewSSEKMS("", nil)
	case strings.HasPrefix(strings.ToLower(algorithm), "aws:kms:"):
		return encrypt.NewSSEKMS(algorithm[len("aws:kms:"):], nil)
	default:
		return nil, fmt.Errorf(
			"unsupported serverSideEncryption %q: use \"AES256\", \"aws:kms\" or \"aws:kms:<key-id>\"", algorithm)
	}
}

// signingRegion picks the region used to sign requests. Most S3-compatible
// stores ignore it but still require it to match what they expect: MinIO, Ceph
// and friends accept us-east-1, while Cloudflare R2 only signs with "auto".
func signingRegion(region, endpoint string) string {
	if region != "" {
		return region
	}
	if strings.HasSuffix(hostOnly(endpoint), ".r2.cloudflarestorage.com") {
		return "auto"
	}
	return "us-east-1"
}

// hostOnly strips a trailing port from a host[:port].
func hostOnly(endpoint string) string {
	if host, _, err := net.SplitHostPort(endpoint); err == nil {
		return host
	}
	return endpoint
}

// putOptions returns the write options every upload shares: the configured SSE
// and storage class, plus the content type.
func (c *Client) putOptions(contentType string) minio.PutObjectOptions {
	return minio.PutObjectOptions{
		ContentType:          contentType,
		ServerSideEncryption: c.sse,
		StorageClass:         c.storageClass,
	}
}

// NewClientFromEnv builds a client from the cnmsql_S3_* environment variables.
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
	_, err := c.mc.PutObject(ctx, bucket, key, reader, size, c.putOptions(contentType))
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
		c.putOptions("application/json"))
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
	objects, err := c.listObjects(ctx, bucket, prefix, true, 1)
	if err != nil {
		return false, err
	}
	return len(objects) == 0, nil
}

// ObjectInfo describes a single object returned by ListObjects.
type ObjectInfo struct {
	// Key is the full object key.
	Key string
	// Size is the object size in bytes.
	Size int64
	// LastModified is when the object was last written.
	LastModified time.Time
}

// ListObjects returns the objects under bucket/prefix. When recursive is false
// only the immediate level is walked (delimiter-based), so listing a cluster
// prefix yields its backup directories rather than every leaf object.
func (c *Client) ListObjects(ctx context.Context, bucket, prefix string, recursive bool) ([]ObjectInfo, error) {
	return c.listObjects(ctx, bucket, prefix, recursive, 0)
}

// listObjects walks bucket/prefix, stopping early once limit objects have been
// collected (limit <= 0 walks everything).
//
// Listing is the one call whose API version is not universally implemented:
// ListObjectsV2 is what every modern store speaks, but the GCS XML interop
// endpoint only implements the original (V1) listing and answers V2 with a 400.
// The first such rejection latches the client onto V1 for the rest of its life,
// so the fallback costs one wasted request per process rather than one per call.
func (c *Client) listObjects(ctx context.Context, bucket, prefix string, recursive bool, limit int) (
	[]ObjectInfo, error,
) {
	infos, err := c.listObjectsWith(ctx, bucket, prefix, recursive, limit, c.listV1.Load())
	if err == nil || c.listV1.Load() || !isUnsupportedListV2(err) {
		return infos, err
	}
	c.listV1.Store(true)
	return c.listObjectsWith(ctx, bucket, prefix, recursive, limit, true)
}

func (c *Client) listObjectsWith(ctx context.Context, bucket, prefix string, recursive bool, limit int, useV1 bool) (
	[]ObjectInfo, error,
) {
	// The SDK only stops producing when the context is cancelled, so an early
	// return without this would leak its listing goroutine.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var infos []ObjectInfo
	for object := range c.mc.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: recursive,
		MaxKeys:   limit,
		UseV1:     useV1,
	}) {
		if object.Err != nil {
			return nil, fmt.Errorf("listing s3://%s/%s: %w", bucket, prefix, object.Err)
		}
		infos = append(infos, ObjectInfo{
			Key:          object.Key,
			Size:         object.Size,
			LastModified: object.LastModified,
		})
		if limit > 0 && len(infos) >= limit {
			break
		}
	}
	return infos, nil
}

// Remove deletes a single object. A not-found object is treated as success.
//
// Deletion is deliberately one object per request: the bulk DeleteObjects (POST
// ?delete) API is the operation S3-compatible stores most often omit, and no
// provider lets us delete a prefix in a single call, so RemovePrefix expands to
// per-object deletes that every store supports.
func (c *Client) Remove(ctx context.Context, bucket, key string) error {
	err := c.mc.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{})
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("removing s3://%s/%s: %w", bucket, key, err)
	}
	return nil
}

// RemovePrefix deletes every object under bucket/prefix. It is used to drop an
// expired base backup directory (archive + metadata) wholesale.
func (c *Client) RemovePrefix(ctx context.Context, bucket, prefix string) error {
	objects, err := c.ListObjects(ctx, bucket, prefix, true)
	if err != nil {
		return err
	}
	for _, object := range objects {
		if err := c.Remove(ctx, bucket, object.Key); err != nil {
			return err
		}
	}
	return nil
}

// Exists reports whether bucket/key exists. A not-found response is reported as
// (false, nil); any other error is returned.
func (c *Client) Exists(ctx context.Context, bucket, key string) (bool, error) {
	_, err := c.mc.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	if isNotFound(err) {
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
