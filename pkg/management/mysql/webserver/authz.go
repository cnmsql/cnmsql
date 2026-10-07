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

package webserver

import (
	"net/http"
)

// OperatorCommonName is the subject common name of the operator's client
// certificate (the cluster's <cluster>-client-tls Secret). Only a client
// presenting it may use the full control API.
const OperatorCommonName = "cnmsql-operator"

// peerRoutes are the only routes a verified client other than the operator may
// call. Instance certificates carry the client-auth usage under the same CA so
// a joining replica can stream a base backup from the primary; nothing else on
// the control API is meant for a peer.
var peerRoutes = map[string]struct{}{
	http.MethodGet + " /cluster/backup": {},
}

// AuthorizeClients wraps the control API so a request is served only when its
// verified client certificate is allowed to call the route: the operator's
// identity may call every route, any other certificate the client CA verified
// only the peer routes. Without this, every certificate under the cluster CA
// (each instance's own) could promote, demote, load SQL into or replace the
// instance-manager binary of any other instance.
func AuthorizeClients(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0]) == 0 {
			http.Error(w, "client certificate required", http.StatusUnauthorized)
			return
		}
		if r.TLS.VerifiedChains[0][0].Subject.CommonName == OperatorCommonName {
			next.ServeHTTP(w, r)
			return
		}
		if _, ok := peerRoutes[r.Method+" "+r.URL.Path]; ok {
			next.ServeHTTP(w, r)
			return
		}
		http.Error(w, "client certificate is not allowed to call this route", http.StatusForbidden)
	})
}
