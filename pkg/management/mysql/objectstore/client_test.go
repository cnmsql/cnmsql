/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package objectstore

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseEndpoint(t *testing.T) {
	t.Parallel()

	cases := []struct {
		endpoint   string
		wantHost   string
		wantSecure bool
		wantErr    bool
	}{
		{endpoint: "", wantHost: "s3.amazonaws.com", wantSecure: true},
		{endpoint: "minio.svc:9000", wantHost: "minio.svc:9000", wantSecure: true},
		{endpoint: "https://s3.example.com", wantHost: "s3.example.com", wantSecure: true},
		{endpoint: "http://minio.svc:9000", wantHost: "minio.svc:9000", wantSecure: false},
		{endpoint: "://broken", wantErr: true},
	}
	for _, tc := range cases {
		host, secure, err := parseEndpoint(tc.endpoint)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("parseEndpoint(%q) expected error", tc.endpoint)
			}
			continue
		}
		if err != nil {
			t.Fatalf("parseEndpoint(%q) error: %v", tc.endpoint, err)
		}
		if host != tc.wantHost || secure != tc.wantSecure {
			t.Fatalf("parseEndpoint(%q) = (%q, %t), want (%q, %t)", tc.endpoint, host, secure, tc.wantHost, tc.wantSecure)
		}
	}
}

func TestConfigFromEnv(t *testing.T) {
	t.Setenv(EnvEndpoint, "http://minio.svc:9000")
	t.Setenv(EnvRegion, "us-east-1")
	t.Setenv(EnvSignatureVersion, "s3v2")
	t.Setenv(EnvForcePathStyle, "true")
	t.Setenv(EnvAccessKeyID, "key")
	t.Setenv(EnvSecretAccessKey, "secret")

	cfg := ConfigFromEnv()
	if cfg.Endpoint != "http://minio.svc:9000" || cfg.Region != "us-east-1" {
		t.Fatalf("endpoint/region = %q/%q", cfg.Endpoint, cfg.Region)
	}
	if !cfg.SignatureV2 {
		t.Fatal("expected signature v2")
	}
	if !cfg.ForcePathStyle {
		t.Fatal("expected force path style")
	}
	if cfg.AccessKeyID != "key" || cfg.SecretAccessKey != "secret" {
		t.Fatalf("credentials = %q/%q", cfg.AccessKeyID, cfg.SecretAccessKey)
	}
}

func TestNewClientFromConfig(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{
		Endpoint:        "http://minio.svc:9000",
		Region:          "us-east-1",
		AccessKeyID:     "key",
		SecretAccessKey: "secret",
		ForcePathStyle:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if client == nil || client.mc == nil {
		t.Fatal("expected initialised client")
	}
}

func TestSHA256Reader(t *testing.T) {
	t.Parallel()

	reader := NewSHA256Reader(strings.NewReader("hello world"))
	buf := make([]byte, 4)
	total := 0
	for {
		n, err := reader.Read(buf)
		total += n
		if err != nil {
			break
		}
	}
	if total != 11 {
		t.Fatalf("read %d bytes, want 11", total)
	}
	if reader.Count() != 11 {
		t.Fatalf("count = %d, want 11", reader.Count())
	}
	if got := reader.SumHex(); got != "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9" {
		t.Fatalf("sha256 = %q", got)
	}
}

func TestIsEmptyPrefix(t *testing.T) {
	t.Parallel()

	const nonEmptyBody = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>backups</Name><Prefix>demo/</Prefix><KeyCount>1</KeyCount>
  <MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated>
  <Contents><Key>demo/backup-1/id/backup.xbstream</Key><Size>42</Size></Contents>
</ListBucketResult>`
	const emptyBody = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>backups</Name><Prefix>demo/</Prefix><KeyCount>0</KeyCount>
  <MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated>
</ListBucketResult>`

	cases := map[string]struct {
		body      string
		wantEmpty bool
	}{
		"non-empty": {body: nonEmptyBody, wantEmpty: false},
		"empty":     {body: emptyBody, wantEmpty: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()

			client, err := NewClient(Config{
				Endpoint:        server.URL,
				Region:          "us-east-1",
				AccessKeyID:     "key",
				SecretAccessKey: "secret",
				ForcePathStyle:  true,
			})
			if err != nil {
				t.Fatal(err)
			}
			empty, err := client.IsEmptyPrefix(context.Background(), "backups", "demo/")
			if err != nil {
				t.Fatal(err)
			}
			if empty != tc.wantEmpty {
				t.Fatalf("IsEmptyPrefix = %t, want %t", empty, tc.wantEmpty)
			}
		})
	}
}
