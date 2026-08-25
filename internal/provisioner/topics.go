package provisioner

import (
	"context"
	"errors"
	"strconv"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
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

// Watermill topic names for internal event routing.
const (
	TopicLeaseCreated    = "events.lease.created"
	TopicLeaseClosed     = "events.lease.closed"
	TopicLeaseExpired    = "events.lease.expired"
	TopicBackendCallback = "events.backend.callback"
	TopicPayloadReceived = "events.payload.received"
	TopicLeaseEvent      = "events.lease.event"
)

// CallbackPath is the path suffix for backend provision callbacks.
const CallbackPath = "/callbacks/provision"

// CallbackOperationGenerationParam carries the initiating tracker's operation
// identity in the callback URL. Callback HMACs cover RequestURI, so the value
// cannot be changed independently of the signed backend request.
const CallbackOperationGenerationParam = "operation_generation"

// BuildCallbackURL constructs the full callback URL from a base URL.
func BuildCallbackURL(baseURL string) string {
	return baseURL + CallbackPath
}

// BuildCallbackURLForGeneration binds a callback to one in-flight operation.
func BuildCallbackURLForGeneration(baseURL string, generation uint64) string {
	if generation == 0 {
		return BuildCallbackURL(baseURL)
	}
	return BuildCallbackURL(baseURL) + "?" + CallbackOperationGenerationParam + "=" + strconv.FormatUint(generation, 10)
}

// ChainClient defines the chain operations needed by the provisioner.
type ChainClient interface {
	GetLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error)
	GetPendingLeases(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error)
	AcknowledgeLeases(ctx context.Context, leaseUUIDs []string) (uint64, []string, error)
	RejectLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error)
	CloseLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error)
}
