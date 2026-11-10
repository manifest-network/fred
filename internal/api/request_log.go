package api

import (
	"net/http"
	"unicode/utf8"
)

// The request method and path are chosen by the client, before any
// authentication, so they reach a log line only through loggedMethod and
// loggedPath. maxHeaderBytes already bounds the whole request head, but
// without truncation every request could still write up to 64 KiB of log,
// and the rate limiter logs every request it refuses.
const (
	// maxLoggedMethodBytes is longer than any registered HTTP method.
	maxLoggedMethodBytes = 32
	// maxLoggedPathBytes is longer than any path the API serves.
	maxLoggedPathBytes = 256
)

// loggedMethod is r's method, truncated for a log line.
func loggedMethod(r *http.Request) string {
	return truncateLogValue(r.Method, maxLoggedMethodBytes)
}

// loggedPath is r's path, truncated for a log line.
func loggedPath(r *http.Request) string {
	return truncateLogValue(r.URL.Path, maxLoggedPathBytes)
}

// truncateLogValue bounds s to limit bytes plus a marker. The cut backs off to
// a UTF-8 rune boundary, so it never splits a multi-byte character; a rune is
// at most 4 bytes, so it drops at most 3. slog quotes control characters, so
// only the size needs bounding here.
func truncateLogValue(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	end := limit
	for end > 0 && !utf8.RuneStart(s[end]) {
		end--
	}
	return s[:end] + "…(truncated)"
}
