package imagefetch

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const registryAttempts = 3

// registryTransport owns retries before verified content becomes import
// authority. Metadata is published only after its entire bounded response has
// arrived. Only a descriptor-bound immutable blob can retain an interrupted
// prefix; its final size and digest still have to pass fetch's verification.
type registryTransport struct {
	base boundedTransport
	blob *ocispec.Descriptor
}

func (t registryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet {
		return t.base.RoundTrip(req)
	}
	transfer := &registryTransfer{transport: t.base, request: req}
	if t.blob != nil && requestsBlob(req, t.blob.Digest.String()) {
		transfer.size = t.blob.Size
		response, err := transfer.openBlob(0)
		if err == nil && response.StatusCode == http.StatusOK {
			response.Body = &resumingBody{transfer: transfer, body: response.Body}
		}
		return response, err
	}
	for {
		response, err := transfer.open(0)
		if err != nil {
			return nil, err
		}
		// Bound authentication/error metadata too, but preserve its status for
		// the registry client's challenge and redirect handling.
		body, err := readRegistryMetadata(response.Body)
		if err == nil {
			response.Body = io.NopCloser(bytes.NewReader(body))
			return response, nil
		}
		if !transfer.canRetry(err) {
			return nil, err
		}
	}
}

func readRegistryMetadata(body io.ReadCloser) ([]byte, error) {
	defer func() { _ = body.Close() }()
	return io.ReadAll(&budgetReader{reader: body, remaining: maxMetadataBytes})
}

func requestsBlob(req *http.Request, id string) bool {
	for req != nil {
		if strings.HasSuffix(req.URL.Path, "/blobs/"+id) {
			return true
		}
		if req.Response == nil {
			return false
		}
		req = req.Response.Request
	}
	return false
}

type registryTransfer struct {
	transport boundedTransport
	request   *http.Request
	attempts  int
	size      int64
}

func (t *registryTransfer) canRetry(err error) bool {
	return t.attempts < registryAttempts && t.request.Context().Err() == nil && transientRegistryError(err)
}

func transientRegistryError(err error) bool {
	var network net.Error
	return errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, errRegistryIdle) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, net.ErrClosed) ||
		(errors.As(err, &network) && network.Timeout())
}

func transientRegistryStatus(status int) bool {
	switch status {
	case http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError,
		http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func (t *registryTransfer) open(offset int64) (*http.Response, error) {
	for {
		if err := t.request.Context().Err(); err != nil {
			return nil, err
		}
		if t.attempts > 0 {
			timer := time.NewTimer(time.Duration(t.attempts) * 100 * time.Millisecond)
			select {
			case <-timer.C:
			case <-t.request.Context().Done():
				timer.Stop()
				return nil, t.request.Context().Err()
			}
		}
		t.attempts++
		request := t.request.Clone(t.request.Context())
		if offset > 0 {
			request.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
		}
		response, err := t.transport.RoundTrip(request)
		if err != nil {
			if t.canRetry(err) {
				continue
			}
			return nil, err
		}
		if transientRegistryStatus(response.StatusCode) && t.attempts < registryAttempts {
			_ = response.Body.Close()
			continue
		}
		return response, nil
	}
}

func (t *registryTransfer) openBlob(offset int64) (*http.Response, error) {
	for {
		response, err := t.open(offset)
		if err != nil {
			return nil, err
		}
		if err := t.acceptBlob(response, offset); err != nil {
			if t.canRetry(err) {
				continue
			}
			return nil, err
		}
		return response, nil
	}
}

func (t *registryTransfer) acceptBlob(response *http.Response, offset int64) error {
	accepted := false
	defer func() {
		if !accepted {
			_ = response.Body.Close()
		}
	}()
	remaining := t.size - offset
	switch response.StatusCode {
	case http.StatusPartialContent:
		if offset == 0 || response.Header.Get("Content-Range") != fmt.Sprintf("bytes %d-%d/%d", offset, t.size-1, t.size) {
			return errors.New("registry resumed blob has an invalid content range")
		}
	case http.StatusOK:
		remaining = t.size
	default:
		accepted = true
		return nil
	}
	if response.ContentLength >= 0 && response.ContentLength != remaining {
		return errors.New("registry blob length differs from its verified descriptor")
	}
	if offset > 0 && response.StatusCode == http.StatusOK {
		// Registries may ignore Range. Replay only this blob's prefix,
		// retaining the original hash and the single staging allocation.
		if _, err := io.CopyN(io.Discard, response.Body, offset); err != nil {
			return err
		}
	}
	accepted = true
	return nil
}

type resumingBody struct {
	transfer *registryTransfer
	body     io.ReadCloser
	offset   int64
	resume   bool
}

func (b *resumingBody) Read(p []byte) (int, error) {
	for {
		if b.resume {
			response, err := b.transfer.openBlob(b.offset)
			if err != nil {
				return 0, err
			}
			if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusPartialContent {
				_ = response.Body.Close()
				return 0, fmt.Errorf("registry blob resume returned HTTP %d", response.StatusCode)
			}
			b.body, b.resume = response.Body, false
		}
		n, err := b.body.Read(p)
		b.offset += int64(n)
		if err != nil && b.offset < b.transfer.size && b.transfer.canRetry(err) {
			_ = b.body.Close()
			b.resume = true
			if n == 0 {
				continue
			}
			err = nil
		}
		return n, err
	}
}

func (b *resumingBody) Close() error { return b.body.Close() }
