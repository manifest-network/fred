package docker

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
)

var errImageInspectResponseTooLarge = errors.New("docker image inspection response exceeds metadata byte budget")

// boundImageInspectResponse limits bytes before the SDK buffers and decodes the
// DTO. Admission subsequently bounds each metadata collection before sorting.
func boundImageInspectResponse(req *http.Request, response *http.Response, err error) (*http.Response, error) {
	if err != nil || response == nil || response.Body == nil || req.Method != http.MethodGet ||
		!strings.Contains(req.URL.Path, "/images/") || !strings.HasSuffix(req.URL.Path, "/json") {
		return response, err
	}
	response.Body = &imageInspectResponseBody{
		body:    response.Body,
		bounded: io.LimitedReader{R: response.Body, N: imageexec.MaxInspectResponseBytes + 1},
	}
	return response, nil
}

type imageInspectResponseBody struct {
	body    io.ReadCloser
	bounded io.LimitedReader
}

func (r *imageInspectResponseBody) Read(buffer []byte) (int, error) {
	n, err := r.bounded.Read(buffer)
	if r.bounded.N == 0 {
		return n, errImageInspectResponseTooLarge
	}
	return n, err
}

func (r *imageInspectResponseBody) Close() error { return r.body.Close() }
