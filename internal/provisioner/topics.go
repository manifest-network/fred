package provisioner

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/callbackurl"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

// Sentinel errors for provisioner operations.
var (
	// ErrMalformedMessage indicates the message payload could not be parsed.
	// This is a terminal error - the message should not be retried.
	ErrMalformedMessage = errors.New("malformed message payload")

	// ErrNoBackendAvailable indicates no backend is configured to handle the request.
	ErrNoBackendAvailable = errors.New("no backend available")

	// ErrPlacementUnresolvable indicates a lease has a placement record naming a
	// backend the router does not know. fred refuses the operation rather than
	// choosing a different backend: the lease's data lives on the recorded
	// machine, so provisioning it elsewhere creates an empty volume while the
	// real data sits untouched (ENG-635).
	//
	// It is deliberately NOT one of the terminal errors in handleProvisionError:
	// a backend can be absent because it was paused, renamed or is mid-redeploy,
	// and rejecting or closing a lease for that would turn a recoverable outage
	// into permanent tenant data loss. It falls through to the transient default
	// and is retried on the next cycle.
	ErrPlacementUnresolvable = errors.New("placement backend not found in router")

	// ErrProvisioningFailed indicates the backend failed to provision the resource.
	ErrProvisioningFailed = errors.New("provisioning failed")

	// ErrDeprovisionFailed indicates the backend failed to deprovision the resource.
	ErrDeprovisionFailed = errors.New("deprovision failed")

	// ErrAcknowledgeFailed indicates the lease acknowledgment on chain failed.
	ErrAcknowledgeFailed = errors.New("lease acknowledgment failed")

	// ErrPayloadStoreUnavailable indicates an operation needed the payload store
	// but none is configured (payload_store_db_path unset).
	//
	// It exists so the /update path can tell "persisted" from "there was nowhere
	// to persist to" and answer the tenant honestly. Silently succeeding is what
	// ENG-619 was: the update reaches the backend, nothing durable records it,
	// and the next reprovision reverts the tenant with no signal anywhere.
	ErrPayloadStoreUnavailable = errors.New("payload store not configured")
)

// Internal event-routing topics and stable message metric labels.
const (
	TopicLeaseCreated    = "events.lease.created"
	TopicLeaseClosed     = "events.lease.closed"
	TopicLeaseExpired    = "events.lease.expired"
	TopicBackendCallback = "events.backend.callback" // legacy message-adapter label
	TopicPayloadReceived = "events.payload.received"
	TopicLeaseEvent      = "events.lease.event"
)

// CallbackPath is the path suffix for backend provision callbacks.
const CallbackPath = callbackurl.ProvisionPath

// CallbackOperationIDParam carries the initiating operation's identity in the
// callback URL. Callback HMACs cover RequestURI, so the value
// cannot be changed independently of the signed backend request.
const CallbackOperationIDParam = operation.QueryParameter

// BuildCallbackURL constructs a validated tokenless callback URL for an
// explicitly migrated legacy lifecycle. It shares the typed builder's URL
// handling so an unrelated base query cannot swallow the callback path.
func BuildCallbackURL(baseURL string) (string, error) {
	return buildCallbackURL(baseURL, "", "")
}

// BuildCallbackURLForOperation binds a callback to a validated operation ID.
// Invalid IDs and malformed base URLs fail instead of silently emitting an
// unscoped callback address.
func BuildCallbackURLForOperation(baseURL string, operationID operation.OperationID) (string, error) {
	text, err := operationID.MarshalText()
	if err != nil {
		return "", fmt.Errorf("set callback operation ID: %w", err)
	}
	return buildCallbackURLWithCapability(baseURL, operation.QueryParameter, string(text))
}

// BuildCallbackURLForLifecycle binds an observation callback to the current
// typed lease-lifecycle capability. Invalid IDs and malformed base URLs fail
// instead of silently falling back to a legacy tokenless address.
func BuildCallbackURLForLifecycle(baseURL string, lifecycleID lifecycle.ID) (string, error) {
	text, err := lifecycleID.MarshalText()
	if err != nil {
		return "", fmt.Errorf("set callback lifecycle ID: %w", err)
	}
	return buildCallbackURLWithCapability(baseURL, lifecycle.QueryParameter, string(text))
}

// buildCallbackURLWithCapability validates the base query with the same parser
// used by callback ingress, then appends one typed capability without
// re-encoding unrelated fields. The exact raw query is covered by callback
// HMACs, so normalization here would create an avoidable second wire form.
func buildCallbackURLWithCapability(baseURL, parameter, value string) (string, error) {
	return buildCallbackURL(baseURL, parameter, value)
}

func buildCallbackURL(baseURL, parameter, value string) (string, error) {
	base, err := parseCallbackBaseURL(baseURL)
	if err != nil {
		return "", err
	}
	callbackURL, err := base.ProvisionURL()
	if err != nil {
		return "", fmt.Errorf("append callback provision path: %w", err)
	}
	if parameter == "" {
		return callbackURL.String(), nil
	}
	capability := url.QueryEscape(parameter) + "=" + url.QueryEscape(value)
	if callbackURL.RawQuery == "" {
		callbackURL.RawQuery = capability
	} else {
		callbackURL.RawQuery += "&" + capability
	}
	return callbackURL.String(), nil
}

// parseCallbackBaseURL proves the static portion from which typed callback
// destinations are derived. Reject components that HTTP never transmits or
// that could carry ambient/conflicting authority before an operation is
// admitted; callers then only need to append one validated typed selector.
func parseCallbackBaseURL(baseURL string) (callbackurl.Base, error) {
	base, err := callbackurl.ParseBase(baseURL)
	if err != nil {
		return callbackurl.Base{}, fmt.Errorf("callback base URL %w", err)
	}
	return base, nil
}

// ChainClient defines the chain operations needed by the provisioner.
type ChainClient interface {
	GetLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error)
	GetPendingLeases(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error)
	AcknowledgeLeases(ctx context.Context, leaseUUIDs []string) (uint64, []string, error)
	RejectLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error)
	CloseLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error)
}
