package shared

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/hmacauth"
)

const (
	// CallbackMaxAttempts is the number of times to attempt callback delivery.
	CallbackMaxAttempts = 3

	// CallbackTimeout is the per-attempt timeout for callback HTTP requests.
	CallbackTimeout = 10 * time.Second
)

// defaultCallbackBackoff defines the default delay before each retry attempt.
var defaultCallbackBackoff = [CallbackMaxAttempts]time.Duration{0, 1 * time.Second, 5 * time.Second}

// CallbackSender handles HMAC-signed callback delivery with retry and persistence.
type CallbackSender struct {
	store        *CallbackStore
	httpClient   *http.Client
	secret       string
	logger       *slog.Logger
	stopCtx      context.Context
	backoff      [CallbackMaxAttempts]time.Duration
	onDelivery   func(outcome string) // nil-safe; injected by the caller for metrics
	onStoreError func()               // nil-safe; called when bbolt persistence fails
}

// CallbackSenderConfig configures a CallbackSender.
type CallbackSenderConfig struct {
	Store        *CallbackStore
	HTTPClient   *http.Client
	Secret       string
	Logger       *slog.Logger
	StopCtx      context.Context
	OnDelivery   func(outcome string)                // optional metrics callback
	OnStoreError func()                              // optional; called when bbolt persistence fails
	Backoff      *[CallbackMaxAttempts]time.Duration // retry delays; nil uses default {0, 1s, 5s}
}

// NewCallbackSender creates a new CallbackSender.
// Panics if HTTPClient, Logger, or StopCtx is nil (programming error).
// Store may be nil to disable callback persistence (callbacks will not survive restarts).
func NewCallbackSender(cfg CallbackSenderConfig) *CallbackSender {
	if cfg.HTTPClient == nil {
		panic("shared.NewCallbackSender: HTTPClient must not be nil")
	}
	if cfg.Logger == nil {
		panic("shared.NewCallbackSender: Logger must not be nil")
	}
	if cfg.StopCtx == nil {
		panic("shared.NewCallbackSender: StopCtx must not be nil")
	}

	backoff := defaultCallbackBackoff
	if cfg.Backoff != nil {
		backoff = *cfg.Backoff
	}

	return &CallbackSender{
		store:        cfg.Store,
		httpClient:   cfg.HTTPClient,
		secret:       cfg.Secret,
		logger:       cfg.Logger,
		stopCtx:      cfg.StopCtx,
		backoff:      backoff,
		onDelivery:   cfg.OnDelivery,
		onStoreError: cfg.OnStoreError,
	}
}

// SendCallback sends a provision result callback with HMAC signature.
// It persists the callback before delivery and removes it on success.
// The caller must provide the callbackURL (resolved from its own state) and
// the backendName (so Fred can label metrics per-backend without a placement
// lookup, which is often already deleted for intentional deprovisions).
func (s *CallbackSender) SendCallback(leaseUUID, callbackURL, backendName string, status backend.CallbackStatus, errMsg string) {
	if callbackURL == "" {
		s.logger.Warn("no callback URL for lease", "lease_uuid", leaseUUID)
		return
	}

	payload := backend.CallbackPayload{
		LeaseUUID: leaseUUID,
		Status:    status,
		Error:     errMsg,
		Backend:   backendName,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		s.logger.Error("failed to marshal callback payload", "error", err)
		return
	}

	// Persist callback before attempting delivery so it survives restarts.
	// Success encodes "not failed" rather than "is success" so a rollback to a
	// pre-Status binary replays a deprovisioned entry as 'success' (benign on
	// dashboards) instead of 'failed' (which would re-introduce the spurious
	// failure events this PR eliminates).
	if s.store != nil {
		if storeErr := s.store.Store(CallbackEntry{
			LeaseUUID:   leaseUUID,
			CallbackURL: callbackURL,
			Success:     status != backend.CallbackStatusFailed,
			Status:      status,
			Backend:     backendName,
			Error:       errMsg,
			CreatedAt:   time.Now(),
		}); storeErr != nil {
			s.logger.Error("failed to persist callback", "error", storeErr, "lease_uuid", leaseUUID)
			if s.onStoreError != nil {
				s.onStoreError()
			}
		}
	}

	delivered := s.DeliverCallback(leaseUUID, callbackURL, body)

	// Remove from persistent store on successful delivery
	if delivered && s.store != nil {
		if rmErr := s.store.Remove(leaseUUID); rmErr != nil {
			s.logger.Error("failed to remove delivered callback from store", "error", rmErr, "lease_uuid", leaseUUID)
		}
	}
}

// DeliverCallback attempts to deliver a callback with retries.
// Returns true if delivery succeeded.
func (s *CallbackSender) DeliverCallback(leaseUUID, callbackURL string, body []byte) bool {
	for attempt := range CallbackMaxAttempts {
		if attempt > 0 {
			// Wait with backoff, but abort if shutting down
			select {
			case <-s.stopCtx.Done():
				s.logger.Warn("callback retry aborted by shutdown",
					"lease_uuid", leaseUUID,
					"attempt", attempt+1,
				)
				s.reportDelivery("failure")
				return false
			case <-time.After(s.backoff[attempt]):
			}
		}

		if s.trySendCallback(leaseUUID, callbackURL, body) {
			s.reportDelivery("success")
			return true
		}
	}

	s.reportDelivery("failure")
	s.logger.Error("callback delivery failed after retries",
		"lease_uuid", leaseUUID,
		"attempts", CallbackMaxAttempts,
	)
	return false
}

// trySendCallback makes a single callback attempt. Returns true on success.
func (s *CallbackSender) trySendCallback(leaseUUID, callbackURL string, body []byte) bool {
	ctx, cancel := context.WithTimeout(s.stopCtx, CallbackTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, callbackURL, bytes.NewReader(body))
	if err != nil {
		s.logger.Error("failed to create callback request", "error", err, "lease_uuid", leaseUUID)
		return false
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign(s.secret, body))

	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Warn("callback attempt failed",
			"error", err,
			"lease_uuid", leaseUUID,
		)
		return false
	}

	// Always read and close the response body to allow connection reuse.
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		s.logger.Debug("callback sent", "lease_uuid", leaseUUID)
		return true
	}

	s.logger.Warn("callback returned error status",
		"status", resp.StatusCode,
		"lease_uuid", leaseUUID,
		"body", string(respBody),
	)
	return false
}

// ReplayPendingCallbacks replays any callbacks that were persisted but not
// successfully delivered before the previous shutdown.
func (s *CallbackSender) ReplayPendingCallbacks() {
	if s.store == nil {
		return
	}

	entries, err := s.store.ListPending()
	if err != nil {
		s.logger.Error("failed to list pending callbacks", "error", err)
		return
	}
	if len(entries) == 0 {
		return
	}

	s.logger.Info("replaying pending callbacks", "count", len(entries))
	for _, entry := range entries {
		// Legacy entries have empty Status; fall back to the Success bool.
		status := entry.Status
		if status == "" {
			status = backend.CallbackStatusSuccess
			if !entry.Success {
				status = backend.CallbackStatusFailed
			}
		}
		payload := backend.CallbackPayload{
			LeaseUUID: entry.LeaseUUID,
			Status:    status,
			Error:     entry.Error,
			Backend:   entry.Backend,
		}
		body, marshalErr := json.Marshal(payload)
		if marshalErr != nil {
			s.logger.Error("removing malformed callback entry", "error", marshalErr, "lease_uuid", entry.LeaseUUID)
			if rmErr := s.store.Remove(entry.LeaseUUID); rmErr != nil {
				s.logger.Error("failed to remove malformed callback entry",
					"error", rmErr,
					"lease_uuid", entry.LeaseUUID,
				)
			}
			continue
		}

		if s.DeliverCallback(entry.LeaseUUID, entry.CallbackURL, body) {
			if rmErr := s.store.Remove(entry.LeaseUUID); rmErr != nil {
				s.logger.Error("failed to remove replayed callback", "error", rmErr, "lease_uuid", entry.LeaseUUID)
			}
		}
	}
}

// reportDelivery calls the onDelivery hook if configured.
func (s *CallbackSender) reportDelivery(outcome string) {
	if s.onDelivery != nil {
		s.onDelivery(outcome)
	}
}
