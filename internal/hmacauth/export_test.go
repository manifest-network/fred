package hmacauth

// export_test.go holds scaffolding that exists ONLY for this package's
// tests. It compiles into the test binary and never into a fred binary,
// which is the point: hmacauth's production files must not carry code
// whose only caller is a test (ENG-354).

import (
	"net/http"
	"time"
)

// SignRequestWithTime is SignRequest with an explicit timestamp. It
// exists so the equivalence test can prove the *http.Request wrapper
// produces byte-identical output to the SignWithTime primitive it
// delegates to; production signs with the current time via SignRequest.
func SignRequestWithTime(secret string, req *http.Request, body []byte, t time.Time) string {
	return SignWithTime(secret, req.Method, req.URL.RequestURI(), body, t)
}
