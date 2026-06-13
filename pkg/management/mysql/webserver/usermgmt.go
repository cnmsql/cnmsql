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

package webserver

import (
	"context"
	"encoding/json"
	"net/http"
)

// bodyActionHandler maps a JSON-bodied command to 200 OK, 400 on a malformed
// body, or 500 on execution error. It backs the user/database mutation routes.
func bodyActionHandler[T any](action func(context.Context, T) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req T
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := action(r.Context(), req); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

// resultHandler serves the JSON result of a producing func, 500 on error. It
// backs the user/database list routes.
func resultHandler[R any](produce func(context.Context) (R, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		result, err := produce(r.Context())
		if err != nil {
			writeError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(result); err != nil {
			writeError(w, err)
		}
	}
}
