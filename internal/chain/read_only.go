package chain

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"

	grpctypes "github.com/cosmos/cosmos-sdk/types/grpc"
	"github.com/cosmos/cosmos-sdk/types/query"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
)

// ReadOnlyClientConfig is the subset of chain configuration needed by an
// offline query. It deliberately contains no signer, keyring, gas, or
// transaction settings.
type ReadOnlyClientConfig struct {
	Endpoint       string
	TLSEnabled     bool
	TLSCAFile      string
	TLSSkipVerify  bool
	QueryPageLimit int
}

// ReadOnlyClient exposes only chain queries needed by offline safety tools. It
// cannot derive keys or broadcast transactions by construction.
type ReadOnlyClient struct {
	conn           *grpc.ClientConn
	pages          providerLeasePageQuerier
	queryPageLimit uint64
}

// ProviderLeaseSnapshot is one complete, height-pinned view of a provider's
// lease history. Its fields are private so callers cannot manufacture or
// mutate the evidence returned by ReadOnlyClient.
type ProviderLeaseSnapshot struct {
	providerUUID string
	blockHeight  int64
	total        int
	blocking     []BlockingProviderLease
	leaseUUIDs   []string
	leaseItems   map[string][]backend.LeaseItem
}

// BlockingProviderLease is a lease whose state is not terminal. Unknown and
// future states are blocking as well as PENDING and ACTIVE; fresh placement
// initialization must fail closed when this slice is non-empty.
type BlockingProviderLease struct {
	LeaseUUID string
	State     billingtypes.LeaseState
}

// ProviderUUID returns the provider queried for this snapshot.
func (snapshot ProviderLeaseSnapshot) ProviderUUID() string { return snapshot.providerUUID }

// BlockHeight returns the positive Cosmos block height shared by every page.
func (snapshot ProviderLeaseSnapshot) BlockHeight() int64 { return snapshot.blockHeight }

// TotalLeases returns the number of terminal and non-terminal leases examined.
func (snapshot ProviderLeaseSnapshot) TotalLeases() int { return snapshot.total }

// BlockingLeases returns a defensive copy of every non-terminal or unknown
// lease observed in the snapshot.
func (snapshot ProviderLeaseSnapshot) BlockingLeases() []BlockingProviderLease {
	return append([]BlockingProviderLease(nil), snapshot.blocking...)
}

// BlockingLeaseUUIDs returns the identities behind BlockingLeaseCount without
// exposing chain protobuf states to offline authority packages. Unknown and
// future states are already included in this set by SnapshotProviderLeases.
func (snapshot ProviderLeaseSnapshot) BlockingLeaseUUIDs() []string {
	leaseUUIDs := make([]string, len(snapshot.blocking))
	for index := range snapshot.blocking {
		leaseUUIDs[index] = snapshot.blocking[index].LeaseUUID
	}
	return leaseUUIDs
}

// BlockingLeaseCount returns the number of non-terminal or unknown leases in
// the complete snapshot. It lets the placement cutover boundary consume only
// aggregate eligibility evidence without depending on chain protobuf types.
func (snapshot ProviderLeaseSnapshot) BlockingLeaseCount() int { return len(snapshot.blocking) }

// LeaseUUIDs returns a defensive copy of the exact all-state membership seen
// at BlockHeight. Offline v0.13 preparation uses this to prove that every
// placement/backend survivor belongs to the provider being installed as
// durable authority, including retention-only terminal leases.
func (snapshot ProviderLeaseSnapshot) LeaseUUIDs() []string {
	leaseUUIDs := make([]string, len(snapshot.leaseUUIDs))
	copy(leaseUUIDs, snapshot.leaseUUIDs)
	return leaseUUIDs
}

// LeaseItems returns a detached copy of the immutable paid topology for every
// lease in LeaseUUIDs. SnapshotProviderLeases has already checked the uint64 to
// int boundary and converted the protobuf values to Fred's backend wire shape.
func (snapshot ProviderLeaseSnapshot) LeaseItems() map[string][]backend.LeaseItem {
	if snapshot.leaseItems == nil {
		return nil
	}
	items := make(map[string][]backend.LeaseItem, len(snapshot.leaseItems))
	for leaseUUID, leaseItems := range snapshot.leaseItems {
		items[leaseUUID] = append([]backend.LeaseItem(nil), leaseItems...)
	}
	return items
}

// Valid reports whether the snapshot came from a complete positive-height
// query for one provider. A valid snapshot may still contain blocking leases.
func (snapshot ProviderLeaseSnapshot) Valid() bool {
	return snapshot.providerUUID != "" && snapshot.blockHeight > 0 &&
		snapshot.total >= len(snapshot.blocking) && snapshot.leaseItems != nil &&
		len(snapshot.leaseItems) == snapshot.total
}

// NewReadOnlyClient constructs a signer-free chain query client.
func NewReadOnlyClient(cfg ReadOnlyClientConfig) (*ReadOnlyClient, error) {
	if cfg.Endpoint == "" {
		return nil, errors.New("chain gRPC endpoint is required")
	}
	conn, err := dialGRPC(cfg.Endpoint, cfg.TLSEnabled, cfg.TLSCAFile, cfg.TLSSkipVerify)
	if err != nil {
		return nil, err
	}
	pageLimit := max(cfg.QueryPageLimit, 0)
	if pageLimit == 0 {
		pageLimit = 100
	}
	return &ReadOnlyClient{
		conn:           conn,
		pages:          grpcProviderLeasePageQuerier{client: billingtypes.NewQueryClient(conn)},
		queryPageLimit: uint64(pageLimit),
	}, nil
}

// Close closes the query connection. It is safe to call on a nil receiver.
func (client *ReadOnlyClient) Close() error {
	if client == nil || client.conn == nil {
		return nil
	}
	return client.conn.Close()
}

// SnapshotProviderLeases fetches every lease for providerUUID using the
// unfiltered provider index. The first response supplies the block height; all
// later pages are explicitly queried at that same height. This prevents a
// PENDING-to-ACTIVE transition between separately filtered or paginated reads
// from disappearing from a fresh-initialization proof.
func (client *ReadOnlyClient) SnapshotProviderLeases(
	ctx context.Context,
	providerUUID string,
) (ProviderLeaseSnapshot, error) {
	if client == nil || client.pages == nil {
		return ProviderLeaseSnapshot{}, errors.New("read-only chain client is not initialized")
	}
	if ctx == nil {
		return ProviderLeaseSnapshot{}, errors.New("chain query context is required")
	}
	if providerUUID == "" {
		return ProviderLeaseSnapshot{}, errors.New("provider UUID is required")
	}
	if err := ctx.Err(); err != nil {
		return ProviderLeaseSnapshot{}, err
	}

	snapshot := ProviderLeaseSnapshot{
		providerUUID: providerUUID,
		leaseUUIDs:   make([]string, 0),
		leaseItems:   make(map[string][]backend.LeaseItem),
	}
	seenLeases := make(map[string]struct{})
	seenContinuationKeys := make(map[string]struct{})
	var nextKey []byte
	var pinnedHeight int64
	for {
		response, observedHeight, err := client.pages.LeasesByProviderPage(
			ctx,
			&billingtypes.QueryLeasesByProviderRequest{
				ProviderUuid: providerUUID,
				StateFilter:  billingtypes.LEASE_STATE_UNSPECIFIED,
				Pagination: &query.PageRequest{
					Key:   append([]byte(nil), nextKey...),
					Limit: client.queryPageLimit,
				},
			},
			pinnedHeight,
		)
		if err != nil {
			return ProviderLeaseSnapshot{}, fmt.Errorf("query provider leases: %w", err)
		}
		if response == nil {
			return ProviderLeaseSnapshot{}, errors.New("query provider leases returned a nil response")
		}
		if observedHeight <= 0 {
			return ProviderLeaseSnapshot{}, fmt.Errorf("query provider leases returned invalid block height %d", observedHeight)
		}
		if pinnedHeight == 0 {
			pinnedHeight = observedHeight
		} else if observedHeight != pinnedHeight {
			return ProviderLeaseSnapshot{}, fmt.Errorf(
				"query provider leases changed block height from %d to %d",
				pinnedHeight,
				observedHeight,
			)
		}

		for index := range response.Leases {
			lease := response.Leases[index]
			leaseID, parseErr := uuid.Parse(lease.Uuid)
			if parseErr != nil || leaseID == uuid.Nil || leaseID.String() != lease.Uuid {
				return ProviderLeaseSnapshot{}, fmt.Errorf(
					"query provider leases returned non-canonical lease UUID %q",
					lease.Uuid,
				)
			}
			if lease.ProviderUuid != providerUUID {
				return ProviderLeaseSnapshot{}, fmt.Errorf(
					"query provider leases returned lease %s for provider %q, expected %q",
					lease.Uuid,
					lease.ProviderUuid,
					providerUUID,
				)
			}
			if _, duplicate := seenLeases[lease.Uuid]; duplicate {
				return ProviderLeaseSnapshot{}, fmt.Errorf(
					"query provider leases returned duplicate lease %s",
					lease.Uuid,
				)
			}
			seenLeases[lease.Uuid] = struct{}{}
			snapshot.leaseUUIDs = append(snapshot.leaseUUIDs, lease.Uuid)
			leaseItems := make([]backend.LeaseItem, len(lease.Items))
			for itemIndex, item := range lease.Items {
				if item.Quantity > uint64(math.MaxInt) {
					return ProviderLeaseSnapshot{}, fmt.Errorf(
						"query provider leases returned lease %s item %d quantity %d larger than int",
						lease.Uuid,
						itemIndex,
						item.Quantity,
					)
				}
				leaseItems[itemIndex] = backend.LeaseItem{
					SKU:          item.SkuUuid,
					Quantity:     int(item.Quantity),
					ServiceName:  item.ServiceName,
					CustomDomain: item.CustomDomain,
				}
			}
			snapshot.leaseItems[lease.Uuid] = leaseItems
			snapshot.total++
			if !terminalLeaseState(lease.State) {
				snapshot.blocking = append(snapshot.blocking, BlockingProviderLease{
					LeaseUUID: lease.Uuid,
					State:     lease.State,
				})
			}
		}

		if response.Pagination == nil || len(response.Pagination.NextKey) == 0 {
			break
		}
		continuationKey := string(response.Pagination.NextKey)
		if _, duplicate := seenContinuationKeys[continuationKey]; duplicate ||
			bytes.Equal(response.Pagination.NextKey, nextKey) {
			return ProviderLeaseSnapshot{}, errors.New("query provider leases returned a repeated continuation key")
		}
		seenContinuationKeys[continuationKey] = struct{}{}
		nextKey = append(nextKey[:0], response.Pagination.NextKey...)
	}
	snapshot.blockHeight = pinnedHeight
	return snapshot, nil
}

func terminalLeaseState(state billingtypes.LeaseState) bool {
	switch state {
	case billingtypes.LEASE_STATE_CLOSED,
		billingtypes.LEASE_STATE_REJECTED,
		billingtypes.LEASE_STATE_EXPIRED:
		return true
	default:
		return false
	}
}

type providerLeasePageQuerier interface {
	LeasesByProviderPage(
		context.Context,
		*billingtypes.QueryLeasesByProviderRequest,
		int64,
	) (*billingtypes.QueryLeasesByProviderResponse, int64, error)
}

type grpcProviderLeasePageQuerier struct {
	client billingtypes.QueryClient
}

func (querier grpcProviderLeasePageQuerier) LeasesByProviderPage(
	ctx context.Context,
	request *billingtypes.QueryLeasesByProviderRequest,
	pinnedHeight int64,
) (*billingtypes.QueryLeasesByProviderResponse, int64, error) {
	if pinnedHeight > 0 {
		ctx = metadata.AppendToOutgoingContext(
			ctx,
			grpctypes.GRPCBlockHeightHeader,
			strconv.FormatInt(pinnedHeight, 10),
		)
	}
	var headers metadata.MD
	response, err := querier.client.LeasesByProvider(ctx, request, grpc.Header(&headers))
	if err != nil {
		return nil, 0, err
	}
	heights := headers.Get(grpctypes.GRPCBlockHeightHeader)
	if len(heights) != 1 {
		return nil, 0, fmt.Errorf(
			"query provider leases returned %d %q headers, expected exactly one",
			len(heights),
			grpctypes.GRPCBlockHeightHeader,
		)
	}
	height, err := strconv.ParseInt(heights[0], 10, 64)
	if err != nil || height <= 0 {
		return nil, 0, fmt.Errorf("query provider leases returned invalid block height %q", heights[0])
	}
	return response, height, nil
}
