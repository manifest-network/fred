package backendidentity

import (
	"fmt"
	"net/http"
	"net/url"
)

// ResponseMiddleware exposes id on every response and rejects a present
// expected-ID query unless it is exactly one canonical matching UUIDv4. An
// absent parameter remains valid on compatibility/read routes; the supported
// v0.13 cutover is stopped and never relies on mixed-version mutation traffic.
func ResponseMiddleware(id ID, next http.Handler) (http.Handler, error) {
	if !id.Valid() {
		return nil, ErrInvalidID
	}
	if next == nil {
		return nil, fmt.Errorf("backend identity response handler is required")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(ResponseHeader, id.String())
		query, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			http.Error(w, "malformed query string", http.StatusBadRequest)
			return
		}
		if values, present := query[QueryParameter]; present {
			if len(values) != 1 {
				http.Error(w, "backend storage identity must appear exactly once", http.StatusBadRequest)
				return
			}
			expected, parseErr := Parse(values[0])
			if parseErr != nil {
				http.Error(w, "invalid backend storage identity", http.StatusBadRequest)
				return
			}
			if expected != id {
				http.Error(w, "backend storage identity mismatch", http.StatusConflict)
				return
			}
		}
		next.ServeHTTP(w, r)
	}), nil
}

// RequireBoundPath validates the canonical identity captured by a ServeMux
// `{storage_id}` wildcard before body decoding or backend invocation.
func RequireBoundPath(id ID, next http.Handler) (http.Handler, error) {
	if !id.Valid() {
		return nil, ErrInvalidID
	}
	if next == nil {
		return nil, fmt.Errorf("backend identity path handler is required")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed, err := Parse(r.PathValue("storage_id"))
		if err != nil || observed != id {
			http.Error(w, "backend storage identity path mismatch", http.StatusNotFound)
			return
		}
		next.ServeHTTP(w, r)
	}), nil
}
