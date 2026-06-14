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
