package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const (
	// CallbackSignatureHeader is the header name for HMAC signatures on callbacks.
	// Format: "sha256=<hex-encoded-hmac>"
	CallbackSignatureHeader = "X-Fred-Signature"

	// signaturePrefix is the prefix for the signature value.
	signaturePrefix = "sha256="
)

// CallbackAuthenticator verifies HMAC signatures on backend callbacks.
type CallbackAuthenticator struct {
	secret []byte
}

// NewCallbackAuthenticator creates a new callback authenticator with the given secret.
func NewCallbackAuthenticator(secret string) *CallbackAuthenticator {
	return &CallbackAuthenticator{
		secret: []byte(secret),
	}
}

// ComputeSignature computes the HMAC-SHA256 signature for a payload.
// Returns the signature in the format "sha256=<hex>".
func (a *CallbackAuthenticator) ComputeSignature(payload []byte) string {
	mac := hmac.New(sha256.New, a.secret)
	mac.Write(payload)
	return signaturePrefix + hex.EncodeToString(mac.Sum(nil))
}

// VerifySignature verifies that the provided signature matches the payload.
// The signature should be in the format "sha256=<hex>".
func (a *CallbackAuthenticator) VerifySignature(payload []byte, signature string) bool {
	if !strings.HasPrefix(signature, signaturePrefix) {
		return false
	}

	providedSig, err := hex.DecodeString(strings.TrimPrefix(signature, signaturePrefix))
	if err != nil {
		return false
	}

	mac := hmac.New(sha256.New, a.secret)
	mac.Write(payload)
	expectedSig := mac.Sum(nil)

	return hmac.Equal(providedSig, expectedSig)
}

// VerifyRequest reads the request body, verifies the signature, and returns the body bytes.
// Returns an error if verification fails.
func (a *CallbackAuthenticator) VerifyRequest(r *http.Request) ([]byte, error) {
	signature := r.Header.Get(CallbackSignatureHeader)
	if signature == "" {
		return nil, fmt.Errorf("missing %s header", CallbackSignatureHeader)
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read request body: %w", err)
	}

	if !a.VerifySignature(body, signature) {
		return nil, fmt.Errorf("invalid signature")
	}

	return body, nil
}
