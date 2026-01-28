package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/gorilla/mux"
	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner"
)

// PayloadPublisher publishes payload events to the provisioner.
type PayloadPublisher interface {
	PublishPayload(event provisioner.PayloadEvent) error
	StorePayload(leaseUUID string, payload []byte) bool
	DeletePayload(leaseUUID string) // Used for rollback on publish failure
}

// PayloadHandler handles payload upload requests.
type PayloadHandler struct {
	client       ChainClient
	publisher    PayloadPublisher
	providerUUID string
	bech32Prefix string
}

// NewPayloadHandler creates a new payload handler.
func NewPayloadHandler(client ChainClient, publisher PayloadPublisher, providerUUID, bech32Prefix string) *PayloadHandler {
	return &PayloadHandler{
		client:       client,
		publisher:    publisher,
		providerUUID: providerUUID,
		bech32Prefix: bech32Prefix,
	}
}

// HandlePayloadUpload handles POST /v1/leases/{lease_uuid}/data
func (h *PayloadHandler) HandlePayloadUpload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	leaseUUID := vars["lease_uuid"]

	// Validate lease UUID format
	if !config.IsValidUUID(leaseUUID) {
		slog.Warn("invalid lease UUID format", "lease_uuid", leaseUUID)
		writeError(w, errMsgInvalidLeaseUUID, http.StatusBadRequest)
		return
	}

	// Extract and validate bearer token (PayloadAuthToken)
	token, err := h.extractPayloadToken(r)
	if err != nil {
		slog.Warn("invalid authorization", "error", err)
		metrics.PayloadUploadsTotal.WithLabelValues("invalid_auth").Inc()
		writeError(w, errMsgUnauthorized, http.StatusUnauthorized)
		return
	}

	// Validate the token
	if err := token.Validate(h.bech32Prefix); err != nil {
		slog.Warn("token validation failed", "error", err)
		metrics.PayloadUploadsTotal.WithLabelValues("invalid_auth").Inc()
		writeError(w, errMsgUnauthorized, http.StatusUnauthorized)
		return
	}

	// Verify the token's lease UUID matches the request
	if token.LeaseUUID != leaseUUID {
		slog.Warn("lease UUID mismatch",
			"token_lease_uuid", token.LeaseUUID,
			"request_lease_uuid", leaseUUID,
		)
		writeError(w, errMsgUnauthorized, http.StatusUnauthorized)
		return
	}

	// Query the lease from chain and verify ownership
	lease, status, err := verifyLeaseAccess(r.Context(), h.client, h.providerUUID, leaseUUID, token.Tenant, false)
	if err != nil {
		writeError(w, err.Error(), status)
		return
	}

	// Verify lease is PENDING
	if lease.State != billingtypes.LEASE_STATE_PENDING {
		slog.Warn("lease not pending",
			"lease_uuid", leaseUUID,
			"state", lease.State.String(),
		)
		writeError(w, "lease not pending", http.StatusNotFound)
		return
	}

	// Verify lease has meta_hash
	if len(lease.MetaHash) == 0 {
		slog.Warn("lease has no meta_hash", "lease_uuid", leaseUUID)
		writeError(w, "lease does not expect payload", http.StatusBadRequest)
		return
	}

	// Verify token's meta_hash matches lease's meta_hash using constant-time comparison
	// to prevent timing attacks on hash values
	tokenMetaHashBytes, err := hex.DecodeString(token.MetaHash)
	if err != nil {
		slog.Warn("invalid token meta_hash hex", "error", err)
		writeError(w, errMsgUnauthorized, http.StatusUnauthorized)
		return
	}

	if subtle.ConstantTimeCompare(tokenMetaHashBytes, lease.MetaHash) != 1 {
		slog.Warn("meta_hash mismatch",
			"token_meta_hash", token.MetaHash,
			"lease_meta_hash", hex.EncodeToString(lease.MetaHash),
		)
		writeError(w, errMsgUnauthorized, http.StatusUnauthorized)
		return
	}

	// Read the payload body with context awareness
	// This allows the read to be aborted if the request times out
	payload, err := readBodyWithContext(r.Context(), r.Body)
	if err != nil {
		if r.Context().Err() != nil {
			if errors.Is(r.Context().Err(), context.DeadlineExceeded) {
				// Server-side timeout - write a proper error response
				slog.Warn("payload read timeout", "error", err, "lease_uuid", leaseUUID)
				writeError(w, "request timeout", http.StatusGatewayTimeout)
				return
			}
			// context.Canceled - likely client disconnect, don't write response
			slog.Warn("payload read cancelled", "error", err, "lease_uuid", leaseUUID)
			return
		}
		slog.Error("failed to read payload body", "error", err)
		writeError(w, "failed to read request body", http.StatusBadRequest)
		return
	}

	if len(payload) == 0 {
		writeError(w, "payload is empty", http.StatusBadRequest)
		return
	}

	// Compute SHA-256 hash of the payload and verify using constant-time comparison
	payloadHash := sha256.Sum256(payload)
	if subtle.ConstantTimeCompare(payloadHash[:], lease.MetaHash) != 1 {
		slog.Warn("payload hash mismatch",
			"payload_hash", hex.EncodeToString(payloadHash[:]),
			"expected_hash", hex.EncodeToString(lease.MetaHash),
			"lease_uuid", leaseUUID,
		)
		metrics.PayloadUploadsTotal.WithLabelValues("hash_mismatch").Inc()
		writeError(w, "payload hash does not match meta_hash", http.StatusBadRequest)
		return
	}

	// Store the payload (also serves as idempotency check)
	if !h.publisher.StorePayload(leaseUUID, payload) {
		slog.Warn("payload already received", "lease_uuid", leaseUUID)
		metrics.PayloadUploadsTotal.WithLabelValues("conflict").Inc()
		writeError(w, "payload already received", http.StatusConflict)
		return
	}

	// Publish payload event to Watermill
	event := provisioner.PayloadEvent{
		LeaseUUID:   leaseUUID,
		Tenant:      lease.Tenant,
		MetaHashHex: hex.EncodeToString(lease.MetaHash),
	}

	if err := h.publisher.PublishPayload(event); err != nil {
		// Rollback: remove stored payload on publish failure to allow retry
		h.publisher.DeletePayload(leaseUUID)
		slog.Error("failed to publish payload event", "error", err, "lease_uuid", leaseUUID)
		metrics.PayloadUploadsTotal.WithLabelValues("error").Inc()
		writeError(w, errMsgInternalServerError, http.StatusInternalServerError)
		return
	}

	// Record success metrics
	metrics.PayloadUploadsTotal.WithLabelValues("success").Inc()
	metrics.PayloadSizeBytes.Observe(float64(len(payload)))

	slog.Info("payload received and validated",
		"lease_uuid", leaseUUID,
		"tenant", token.Tenant,
		"payload_size", len(payload),
	)

	w.WriteHeader(http.StatusAccepted)
}

// extractPayloadToken extracts and parses the bearer token for payload upload.
func (h *PayloadHandler) extractPayloadToken(r *http.Request) (*PayloadAuthToken, error) {
	tokenStr, err := extractBearerToken(r)
	if err != nil {
		return nil, err
	}
	return ParsePayloadAuthToken(tokenStr)
}

// readBodyWithContext reads the request body while respecting context cancellation.
// Unlike io.ReadAll, this checks the context between chunk reads, allowing the read
// to be aborted if the request times out.
func readBodyWithContext(ctx context.Context, body io.Reader) ([]byte, error) {
	const chunkSize = 32 * 1024 // 32KB chunks

	var buf bytes.Buffer
	chunk := make([]byte, chunkSize)

	for {
		// Check context before each read
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		n, err := body.Read(chunk)
		if n > 0 {
			buf.Write(chunk[:n])
		}

		if err == io.EOF {
			return buf.Bytes(), nil
		}
		if err != nil {
			return nil, err
		}
	}
}
