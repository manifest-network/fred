package provisioner

import (
	"context"
	"errors"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
)

// Sentinel errors for provisioner operations.
var (
	// ErrMalformedMessage indicates the message payload could not be parsed.
	// This is a terminal error - the message should not be retried.
	ErrMalformedMessage = errors.New("malformed message payload")

	// ErrNoBackendAvailable indicates no backend is configured to handle the request.
	ErrNoBackendAvailable = errors.New("no backend available")

	// ErrProvisioningFailed indicates the backend failed to provision the resource.
	ErrProvisioningFailed = errors.New("provisioning failed")

	// ErrDeprovisionFailed indicates the backend failed to deprovision the resource.
	ErrDeprovisionFailed = errors.New("deprovision failed")

	// ErrAcknowledgeFailed indicates the lease acknowledgment on chain failed.
	ErrAcknowledgeFailed = errors.New("lease acknowledgment failed")
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

// BuildCallbackURL constructs the full callback URL from a base URL.
func BuildCallbackURL(baseURL string) string {
	return baseURL + CallbackPath
}

// ChainClient defines the chain operations needed by the provisioner.
type ChainClient interface {
	GetLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error)
	GetPendingLeases(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error)
	AcknowledgeLeases(ctx context.Context, leaseUUIDs []string) (uint64, []string, error)
	RejectLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error)
	CloseLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error)
}
