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

package plugin

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/rootclient"
)

// SocketPath is the mysqld Unix socket inside an instance container.
const SocketPath = "/var/run/mysqld/mysqld.sock"

// InstanceManager is the instance manager binary in every instance container.
// The operator copies it in at Pod start, so it is there whatever the image
// ships, even no shell.
const InstanceManager = "/controller/manager"

// RootSecretName returns the Secret holding the cluster's root password,
// honoring spec.rootPasswordSecret and otherwise defaulting to the
// operator-generated <cluster>-root Secret.
func RootSecretName(cluster *mysqlv1alpha1.Cluster) string {
	if cluster.Spec.RootPasswordSecret != nil && cluster.Spec.RootPasswordSecret.Name != "" {
		return cluster.Spec.RootPasswordSecret.Name
	}
	return cluster.Name + "-root"
}

// RootPassword loads the cluster's root password from its Secret.
func (e *Env) RootPassword(ctx context.Context, cluster *mysqlv1alpha1.Cluster) (string, error) {
	name := RootSecretName(cluster)
	secret, err := e.Clientset.CoreV1().Secrets(cluster.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("getting root password secret %q: %w", name, err)
	}
	password := string(secret.Data["password"])
	if password == "" {
		return "", fmt.Errorf("root password secret %q has an empty password", name)
	}
	return password, nil
}

// ClientBinary returns the database client matching the cluster's flavor.
func ClientBinary(cluster *mysqlv1alpha1.Cluster) string {
	if cluster.ResolvedFlavor() == mysqlv1alpha1.FlavorMariaDB {
		return string(mysqlv1alpha1.FlavorMariaDB)
	}
	return string(mysqlv1alpha1.FlavorMySQL)
}

// RootClientOptions describes a database client session run as root on an
// instance.
type RootClientOptions struct {
	Cluster  *mysqlv1alpha1.Cluster
	Instance string
	// Args are extra client arguments, e.g. a database name or --execute.
	Args []string

	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
	TTY    bool
}

// rootClientCommand builds the argv that runs the cluster's database client
// as root over the local socket, through `manager instance client`, which
// reads the password from the first line of stdin with echo off (see package
// rootclient). The client and its arguments are passed as argv, so no
// caller-supplied value is ever parsed by a shell. The default character set
// is pinned before any caller args so a bare instance image cannot negotiate
// latin1 and double-encode UTF-8; the client honours the last occurrence of a
// flag, so callers may still override it.
func rootClientCommand(opts RootClientOptions) []string {
	return append([]string{InstanceManager, "instance", "client", "--",
		ClientBinary(opts.Cluster), "--socket=" + SocketPath, "--user=root",
		"--default-character-set=utf8mb4"}, opts.Args...)
}

// RootClient runs the cluster's database client as root on an instance,
// handing it the root password through the exec stream once the remote side
// is ready for it.
func (e *Env) RootClient(ctx context.Context, opts RootClientOptions) error {
	password, err := e.RootPassword(ctx, opts.Cluster)
	if err != nil {
		return err
	}
	command := rootClientCommand(opts)

	stdout := newReadyWriter(opts.Stdout)
	defer stdout.Flush()
	return e.Exec(ctx, ExecOptions{
		Namespace: opts.Cluster.Namespace,
		Pod:       opts.Instance,
		Container: InstanceContainer,
		Command:   command,
		Stdin:     newGatedReader(ctx, stdout.ready, password, opts.Stdin),
		Stdout:    stdout,
		Stderr:    opts.Stderr,
		TTY:       opts.TTY,
	})
}

// readyWriter passes output through while watching for, and removing, the
// rootclient.PasswordReady marker. It closes ready when the marker is seen.
type readyWriter struct {
	w       io.Writer
	pending []byte
	seen    bool
	ready   chan struct{}
	once    sync.Once
}

func newReadyWriter(w io.Writer) *readyWriter {
	if w == nil {
		w = io.Discard
	}
	return &readyWriter{w: w, ready: make(chan struct{})}
}

func (r *readyWriter) Write(p []byte) (int, error) {
	if r.seen {
		return r.w.Write(p)
	}
	r.pending = append(r.pending, p...)
	if i := bytes.Index(r.pending, []byte(rootclient.PasswordReady)); i >= 0 {
		out := append(r.pending[:i:i], r.pending[i+len(rootclient.PasswordReady):]...)
		r.pending = nil
		r.seen = true
		r.once.Do(func() { close(r.ready) })
		if _, err := r.w.Write(out); err != nil {
			return 0, err
		}
		return len(p), nil
	}
	// Flush everything that cannot be the beginning of the marker.
	keep := markerPrefixLen(r.pending)
	if flush := r.pending[:len(r.pending)-keep]; len(flush) > 0 {
		if _, err := r.w.Write(flush); err != nil {
			return 0, err
		}
		r.pending = append([]byte(nil), r.pending[len(r.pending)-keep:]...)
	}
	return len(p), nil
}

// Flush writes out anything held back while looking for the marker.
func (r *readyWriter) Flush() {
	if len(r.pending) > 0 {
		_, _ = r.w.Write(r.pending)
		r.pending = nil
	}
}

// markerPrefixLen returns the length of the longest suffix of b that is a
// prefix of rootclient.PasswordReady.
func markerPrefixLen(b []byte) int {
	for n := min(len(b), len(rootclient.PasswordReady)-1); n > 0; n-- {
		if bytes.HasPrefix([]byte(rootclient.PasswordReady), b[len(b)-n:]) {
			return n
		}
	}
	return 0
}

// gatedReader withholds all input until ready is closed, then yields the
// password line followed by the caller's input.
type gatedReader struct {
	ctx   context.Context
	ready <-chan struct{}
	open  bool
	r     io.Reader
}

func newGatedReader(ctx context.Context, ready <-chan struct{}, password string, rest io.Reader) *gatedReader {
	readers := []io.Reader{strings.NewReader(password + "\n")}
	if rest != nil {
		readers = append(readers, rest)
	}
	return &gatedReader{ctx: ctx, ready: ready, r: io.MultiReader(readers...)}
}

func (g *gatedReader) Read(p []byte) (int, error) {
	if !g.open {
		select {
		case <-g.ready:
			g.open = true
		case <-g.ctx.Done():
			return 0, g.ctx.Err()
		}
	}
	return g.r.Read(p)
}
