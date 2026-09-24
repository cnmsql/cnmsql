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
	"context"
	"fmt"
	"io"
	"net/url"
	"os"

	"golang.org/x/term"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

// InstanceContainer is the name of the container running mysqld in an
// instance Pod.
const InstanceContainer = "mysql"

// ExecOptions describes a command run inside a Pod container.
type ExecOptions struct {
	Namespace string
	Pod       string
	Container string
	Command   []string

	// Stdin, when non-nil, is streamed to the command's standard input.
	Stdin  io.Reader
	Stdout io.Writer
	// Stderr is ignored with TTY, where the remote terminal merges it into
	// Stdout.
	Stderr io.Writer

	// TTY allocates a remote terminal. When the local stdin is a terminal it is
	// switched to raw mode for the session and window resizes are forwarded.
	TTY bool
}

// Exec runs a command in a Pod container through the API server. It uses the
// plugin's own REST config, so --kubeconfig, --context and every other
// connection flag apply, and nothing (no password, no SQL) is placed on a
// local command line. Like kubectl it prefers the WebSocket transport and
// falls back to SPDY for API servers that do not offer it.
func (e *Env) Exec(ctx context.Context, opts ExecOptions) error {
	req := e.Clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(opts.Namespace).
		Name(opts.Pod).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: opts.Container,
			Command:   opts.Command,
			Stdin:     opts.Stdin != nil,
			Stdout:    opts.Stdout != nil,
			Stderr:    opts.Stderr != nil && !opts.TTY,
			TTY:       opts.TTY,
		}, scheme.ParameterCodec)

	executor, err := newExecutor(e.RESTConfig, req.URL())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream := remotecommand.StreamOptions{
		Stdin:  opts.Stdin,
		Stdout: opts.Stdout,
		Tty:    opts.TTY,
	}
	if !opts.TTY {
		stream.Stderr = opts.Stderr
	}
	if opts.TTY {
		stdinFd := int(os.Stdin.Fd()) //nolint:gosec // file descriptors always fit in int
		if term.IsTerminal(stdinFd) {
			state, err := term.MakeRaw(stdinFd)
			if err != nil {
				return fmt.Errorf("switching the terminal to raw mode: %w", err)
			}
			defer func() { _ = term.Restore(stdinFd, state) }()
		}
		stream.TerminalSizeQueue = newSizeQueue(ctx, int(os.Stdout.Fd())) //nolint:gosec // see above
	}

	if err := executor.StreamWithContext(ctx, stream); err != nil {
		return fmt.Errorf("exec in %s/%s: %w", opts.Namespace, opts.Pod, err)
	}
	return nil
}

func newExecutor(config *rest.Config, u *url.URL) (remotecommand.Executor, error) {
	spdy, err := remotecommand.NewSPDYExecutor(config, "POST", u)
	if err != nil {
		return nil, fmt.Errorf("building SPDY executor: %w", err)
	}
	ws, err := remotecommand.NewWebSocketExecutor(config, "GET", u.String())
	if err != nil {
		return nil, fmt.Errorf("building WebSocket executor: %w", err)
	}
	return remotecommand.NewFallbackExecutor(ws, spdy, func(err error) bool {
		return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
	})
}

// sizeQueue feeds the local terminal size to the remote terminal: once at
// start, then on every resize until the session ends.
type sizeQueue struct {
	ch chan remotecommand.TerminalSize
}

func newSizeQueue(ctx context.Context, fd int) *sizeQueue {
	q := &sizeQueue{ch: make(chan remotecommand.TerminalSize, 1)}
	send := func() {
		w, h, err := term.GetSize(fd)
		if err != nil {
			return
		}
		select {
		case q.ch <- remotecommand.TerminalSize{Width: uint16(w), Height: uint16(h)}: //nolint:gosec // terminal sizes fit
		default:
		}
	}
	send()
	go func() {
		watchResize(ctx, send)
		close(q.ch)
	}()
	return q
}

// Next implements remotecommand.TerminalSizeQueue.
func (q *sizeQueue) Next() *remotecommand.TerminalSize {
	size, ok := <-q.ch
	if !ok {
		return nil
	}
	return &size
}
