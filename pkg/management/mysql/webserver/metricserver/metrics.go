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

// Package metricserver serves Prometheus metrics for an instance.
package metricserver

import (
	"crypto/tls"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// New creates a standalone metrics HTTP server exposing the given collectors.
// When tlsConfig is non-nil the server is configured to serve over (mutual) TLS;
// the caller then starts it with ListenAndServeTLS. A nil tlsConfig serves plain
// HTTP.
func New(addr string, tlsConfig *tls.Config, collectors_ ...prometheus.Collector) *http.Server {
	registry := prometheus.NewRegistry()
	for _, collector := range collectors_ {
		registry.MustRegister(collector)
	}
	registry.MustRegister(collectors.NewGoCollector())

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		TLSConfig:         tlsConfig,
		ReadHeaderTimeout: 10 * time.Second,
	}
}
