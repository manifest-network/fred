package provisioner

import (
	"context"
	"errors"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/manifest-network/fred/internal/metrics"
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
	// It is deliberately NOT one of the terminal results in handleProvisionResult:
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
	TopicBackendCallback = "events.backend.callback" // metrics label for synchronous callback application
	TopicPayloadReceived = "events.payload.received"
	TopicLeaseEvent      = "events.lease.event"
)

// messageTopic is a consumed topic with its message counters, bound at package
// initialization so each topic's series exist before its first message.
type messageTopic struct {
	name      string
	messages  metrics.MessageOutcomeCounters
	malformed prometheus.Counter
}

var (
	leaseCreatedTopic = messageTopic{
		TopicLeaseCreated, metrics.WatermillMessages.LeaseCreated, metrics.MalformedMessages.LeaseCreated,
	}
	leaseClosedTopic = messageTopic{
		TopicLeaseClosed, metrics.WatermillMessages.LeaseClosed, metrics.MalformedMessages.LeaseClosed,
	}
	leaseExpiredTopic = messageTopic{
		TopicLeaseExpired, metrics.WatermillMessages.LeaseExpired, metrics.MalformedMessages.LeaseExpired,
	}
	backendCallbackTopic = messageTopic{
		TopicBackendCallback, metrics.WatermillMessages.BackendCallback, metrics.MalformedMessages.BackendCallback,
	}
	payloadReceivedTopic = messageTopic{
		TopicPayloadReceived, metrics.WatermillMessages.PayloadReceived, metrics.MalformedMessages.PayloadReceived,
	}
)

// ChainClient defines the chain operations needed by the provisioner.
type ChainClient interface {
	GetLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error)
	GetPendingLeases(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error)
	AcknowledgeLeases(ctx context.Context, leaseUUIDs []string) (uint64, []string, error)
	RejectLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error)
	CloseLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error)
}
