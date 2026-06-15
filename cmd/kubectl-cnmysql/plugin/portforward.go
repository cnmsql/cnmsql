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

package plugin

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

// PortForward is an active port-forward to a single Pod port. Local is the
// loopback address (host:port) the forward listens on; call Close to tear it
// down.
type PortForward struct {
	Local  string
	stopCh chan struct{}
	doneCh chan struct{}
	errCh  chan error
}

// Close stops the port-forward and waits for its goroutine to exit.
func (p *PortForward) Close() {
	if p == nil || p.stopCh == nil {
		return
	}
	close(p.stopCh)
	<-p.doneCh
}

// ForwardPod opens a port-forward to the named Pod's given remote port, picking
// an ephemeral local port. It blocks until the tunnel is ready (or fails).
func (e *Env) ForwardPod(ctx context.Context, namespace, podName string, remotePort int) (*PortForward, error) {
	roundTripper, upgrader, err := spdy.RoundTripperFor(e.RESTConfig)
	if err != nil {
		return nil, fmt.Errorf("building spdy transport: %w", err)
	}

	reqURL := e.Clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(namespace).
		Name(podName).
		SubResource("portforward").
		URL()

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: roundTripper}, http.MethodPost,
		&url.URL{Scheme: reqURL.Scheme, Host: reqURL.Host, Path: reqURL.Path})

	stopCh := make(chan struct{})
	readyCh := make(chan struct{})
	errCh := make(chan error, 1)
	doneCh := make(chan struct{})

	// "0:<remotePort>" lets the forwarder pick a free local port.
	fw, err := portforward.New(dialer, []string{fmt.Sprintf("0:%d", remotePort)}, stopCh, readyCh, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("creating port-forward: %w", err)
	}

	go func() {
		defer close(doneCh)
		if err := fw.ForwardPorts(); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-readyCh:
	case err := <-errCh:
		return nil, fmt.Errorf("starting port-forward to %s: %w", podName, err)
	case <-ctx.Done():
		close(stopCh)
		<-doneCh
		return nil, ctx.Err()
	}

	ports, err := fw.GetPorts()
	if err != nil || len(ports) == 0 {
		close(stopCh)
		<-doneCh
		return nil, fmt.Errorf("resolving forwarded port: %w", err)
	}

	return &PortForward{
		Local:  fmt.Sprintf("127.0.0.1:%d", ports[0].Local),
		stopCh: stopCh,
		doneCh: doneCh,
		errCh:  errCh,
	}, nil
}
