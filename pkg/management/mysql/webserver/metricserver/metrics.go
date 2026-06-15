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

// New creates a standalone metrics HTTP server. When tlsConfig is non-nil the
// server is configured to serve over (mutual) TLS; the caller then starts it
// with ListenAndServeTLS. A nil tlsConfig serves plain HTTP.
func New(addr string, collector prometheus.Collector, tlsConfig *tls.Config) *http.Server {
	registry := prometheus.NewRegistry()
	registry.MustRegister(collector)
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
