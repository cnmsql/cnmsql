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
