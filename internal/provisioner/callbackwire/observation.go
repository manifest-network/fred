// Package callbackwire decodes the exact bytes and route covered by callback
// authentication. Its observations are descriptive only; placement mutation
// requires the opaque hmacauth.VerifiedRequest from which they were derived.
package callbackwire

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

var (
	ErrInvalidProof   = errors.New("callback request was not authenticated")
	ErrInvalidRoute   = errors.New("invalid callback route")
	ErrInvalidPayload = errors.New("invalid callback payload")
)

type Selector uint8

const (
	SelectorInvalid Selector = iota
	SelectorLegacy
	SelectorOperation
	SelectorLifecycle
)

// Observation is a typed, non-authoritative view derived from an authenticated
// request. It is useful for ingress validation and logging; only the original
// VerifiedRequest may be submitted for settlement.
type Observation struct {
	leaseUUID   string
	status      backend.CallbackStatus
	failure     string
	backendName string
	retained    bool
	operationID operation.OperationID
	lifecycleID lifecycle.ID
	storageID   backendidentity.ID
	selector    Selector
}

var (
	_ fmt.Formatter  = Observation{}
	_ slog.LogValuer = Observation{}
)

const observationDiagnostic = "callbackwire.Observation{redacted}"

// Format prevents reflective formatting from disclosing callback identifiers,
// backend lineage, or a backend-provided failure string. Callers that need a
// specific non-sensitive field must select it explicitly through an accessor.
func (o Observation) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte(observationDiagnostic))
}

// LogValue makes passing an observation directly to slog safe by default.
func (o Observation) LogValue() slog.Value {
	return slog.StringValue(observationDiagnostic)
}

func (o Observation) LeaseUUID() string                  { return o.leaseUUID }
func (o Observation) Status() backend.CallbackStatus     { return o.status }
func (o Observation) Failure() string                    { return o.failure }
func (o Observation) BackendName() string                { return o.backendName }
func (o Observation) Retained() bool                     { return o.retained }
func (o Observation) OperationID() operation.OperationID { return o.operationID }
func (o Observation) LifecycleID() lifecycle.ID          { return o.lifecycleID }
func (o Observation) StorageID() backendidentity.ID      { return o.storageID }
func (o Observation) Selector() Selector                 { return o.selector }

// Payload returns a detached compatibility DTO. It carries no settlement
// authority; callers must retain and submit the VerifiedRequest to Apply.
func (o Observation) Payload() backend.CallbackPayload {
	payload := backend.CallbackPayload{
		LeaseUUID: o.leaseUUID, Status: o.status, Error: o.failure,
		Backend: o.backendName, Retained: o.retained,
	}
	if o.storageID.Valid() {
		payload.BackendStorageID = o.storageID.String()
	}
	if o.selector == SelectorOperation {
		payload.OperationID = o.operationID.String()
	}
	if o.selector == SelectorLifecycle {
		payload.LifecycleID = o.lifecycleID.String()
	}
	return payload
}

// DecodeVerified derives one callback observation exclusively from the exact
// HMAC-covered method, URI, body, and selected key route.
func DecodeVerified(request hmacauth.VerifiedRequest) (Observation, error) {
	if !request.ValidCallback() {
		return Observation{}, ErrInvalidProof
	}
	if request.Method() != http.MethodPost {
		return Observation{}, fmt.Errorf("%w: method must be POST", ErrInvalidRoute)
	}
	parsed, err := url.ParseRequestURI(request.URI())
	if err != nil {
		return Observation{}, fmt.Errorf("%w: %w", ErrInvalidRoute, err)
	}
	if !strings.HasSuffix(parsed.EscapedPath(), "/callbacks/provision") {
		return Observation{}, fmt.Errorf("%w: request path is not the provision callback endpoint", ErrInvalidRoute)
	}

	payload, err := decodePayload(request.Body())
	if err != nil {
		return Observation{}, fmt.Errorf("%w: %w", ErrInvalidPayload, err)
	}
	if !backend.IsCanonicalLeaseUUID(payload.LeaseUUID) {
		return Observation{}, fmt.Errorf("%w: lease UUID must be a canonical UUID", ErrInvalidPayload)
	}
	switch payload.Status {
	case backend.CallbackStatusSuccess,
		backend.CallbackStatusFailed,
		backend.CallbackStatusDeprovisioned:
	default:
		return Observation{}, fmt.Errorf("%w: invalid status %q", ErrInvalidPayload, payload.Status)
	}
	if payload.Retained && payload.Status != backend.CallbackStatusDeprovisioned {
		return Observation{}, fmt.Errorf("%w: retained requires deprovisioned status", ErrInvalidPayload)
	}

	observation := Observation{
		leaseUUID: payload.LeaseUUID, status: payload.Status,
		failure: payload.Error, backendName: payload.Backend,
		retained: payload.Retained, selector: SelectorLegacy,
	}
	if request.KeyRoute() != "" && payload.BackendStorageID != request.KeyRoute() {
		return Observation{}, fmt.Errorf(
			"%w: authenticated key route does not match payload storage identity",
			ErrInvalidPayload,
		)
	}
	if payload.BackendStorageID != "" {
		storageID, parseErr := backendidentity.Parse(payload.BackendStorageID)
		if parseErr != nil {
			return Observation{}, fmt.Errorf("%w: backend storage identity: %w", ErrInvalidPayload, parseErr)
		}
		observation.storageID = storageID
	}

	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil {
		return Observation{}, fmt.Errorf("%w: malformed query: %w", ErrInvalidRoute, err)
	}
	operationID, operationPresent, err := operation.ParseQuery(query)
	if err != nil {
		return Observation{}, fmt.Errorf("%w: operation ID: %w", ErrInvalidRoute, err)
	}
	lifecycleID, lifecyclePresent, err := lifecycle.ParseQuery(query)
	if err != nil {
		return Observation{}, fmt.Errorf("%w: lifecycle ID: %w", ErrInvalidRoute, err)
	}
	if operationPresent && lifecyclePresent {
		return Observation{}, fmt.Errorf("%w: callback carries both operation and lifecycle authority", ErrInvalidRoute)
	}
	if operationPresent {
		if payload.Status == backend.CallbackStatusDeprovisioned {
			return Observation{}, fmt.Errorf("%w: deprovisioned status requires lifecycle or legacy authority", ErrInvalidPayload)
		}
		observation.operationID = operationID
		observation.selector = SelectorOperation
	} else if lifecyclePresent {
		observation.lifecycleID = lifecycleID
		observation.selector = SelectorLifecycle
	}
	return observation, nil
}

// SelectUntrustedStorageRoute parses only enough of an unauthenticated body to
// select the candidate HMAC key. The result carries no callback authority; a
// caller must still authenticate the exact body and bind this route into the
// resulting VerifiedRequest. Reusing decodePayload keeps pre-auth key selection
// and post-auth callback interpretation on one strict JSON grammar.
func SelectUntrustedStorageRoute(body []byte) (backendidentity.ID, error) {
	payload, err := decodePayload(body)
	if err != nil {
		return backendidentity.ID{}, err
	}
	storageID, err := backendidentity.Parse(payload.BackendStorageID)
	if err != nil {
		return backendidentity.ID{}, fmt.Errorf("backend storage identity: %w", err)
	}
	return storageID, nil
}

var payloadFields = [...]string{
	"lease_uuid", "status", "error", "backend_storage_id", "backend",
	"operation_id", "lifecycle_id", "retained",
}

// decodePayload keeps unknown fields forward compatible while rejecting
// duplicate or case-aliased protocol names and trailing JSON data.
func decodePayload(body []byte) (backend.CallbackPayload, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	opening, err := decoder.Token()
	if err != nil {
		return backend.CallbackPayload{}, fmt.Errorf("decode payload: %w", err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return backend.CallbackPayload{}, errors.New("payload must be a JSON object")
	}

	var payload backend.CallbackPayload
	seen := make(map[string]struct{}, len(payloadFields))
	for decoder.More() {
		fieldToken, tokenErr := decoder.Token()
		if tokenErr != nil {
			return backend.CallbackPayload{}, fmt.Errorf("decode field name: %w", tokenErr)
		}
		field, ok := fieldToken.(string)
		if !ok {
			return backend.CallbackPayload{}, errors.New("field name must be a string")
		}
		if _, duplicate := seen[field]; duplicate {
			return backend.CallbackPayload{}, fmt.Errorf("payload contains duplicate field %q", field)
		}
		seen[field] = struct{}{}
		for _, canonical := range payloadFields {
			if strings.EqualFold(field, canonical) && field != canonical {
				return backend.CallbackPayload{}, fmt.Errorf(
					"payload contains ambiguous field %q; use %q", field, canonical,
				)
			}
		}

		var target any
		switch field {
		case "lease_uuid":
			target = &payload.LeaseUUID
		case "status":
			target = &payload.Status
		case "error":
			target = &payload.Error
		case "backend_storage_id":
			target = &payload.BackendStorageID
		case "backend":
			target = &payload.Backend
		case "operation_id":
			target = &payload.OperationID
		case "lifecycle_id":
			target = &payload.LifecycleID
		case "retained":
			target = &payload.Retained
		default:
			target = new(json.RawMessage)
		}
		if err := decoder.Decode(target); err != nil {
			return backend.CallbackPayload{}, fmt.Errorf("decode field %q: %w", field, err)
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return backend.CallbackPayload{}, fmt.Errorf("close payload: %w", err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return backend.CallbackPayload{}, errors.New("payload must end with a JSON object")
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return backend.CallbackPayload{}, errors.New("payload contains trailing JSON data")
		}
		return backend.CallbackPayload{}, fmt.Errorf("decode payload trailer: %w", err)
	}
	return payload, nil
}
