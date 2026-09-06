package shared

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/backendname"
)

// closedLeaseTombstone is the immutable lifecycle authority left after a
// successful close. It supersedes the lease's per-operation replay history:
// this stronger lease-wide fence recognizes the active release's final
// operation identity and makes every unknown successor impossible to admit for
// the permanently retired UUID.
type closedLeaseTombstone struct {
	CloseIntentID    string    `json:"close_intent_id"`
	LeaseUUID        string    `json:"lease_uuid"`
	CallbackURL      string    `json:"callback_url,omitempty"`
	Backend          string    `json:"backend"`
	BackendStorageID string    `json:"backend_storage_id"`
	Tenant           string    `json:"tenant,omitempty"`
	ProviderUUID     string    `json:"provider_uuid,omitempty"`
	CleanupOnly      bool      `json:"cleanup_only"`
	ClosedAt         time.Time `json:"closed_at"`
}

// ClosedLeaseReceipt is the opaque, permanent cleanup authority for one UUID
// whose successful close crossed the irreversible lifecycle boundary. A
// backend may use it only to exclude that UUID from live projection and remove
// managed substrate that independently proves the same lease identity.
// Its zero value is invalid; private fields make every non-zero value
// store-issued and eliminate nil-interface authority states.
type ClosedLeaseReceipt struct {
	entry     closedLeaseTombstone
	storageID backendidentity.ID
}

// ClosedLeaseAuthorityKind makes the two valid cleanup authorities explicit:
// principal-bound cleanup requires both tenant and provider, while an orphan
// receipt authorizes only lease-UUID cleanup under the already-attested backend
// storage identity. The decoder rejects every mixed representation.
type ClosedLeaseAuthorityKind string

const (
	ClosedLeaseAuthorityPrincipal ClosedLeaseAuthorityKind = "principal_bound"
	ClosedLeaseAuthorityOrphan    ClosedLeaseAuthorityKind = "orphan"
)

func (r ClosedLeaseReceipt) Valid() bool {
	return validateClosedLeaseTombstone(r.entry, r.entry.LeaseUUID) == nil &&
		r.storageID.Valid() && r.storageID.String() == r.entry.BackendStorageID
}

func (r ClosedLeaseReceipt) LeaseUUID() string                    { return r.entry.LeaseUUID }
func (r ClosedLeaseReceipt) Backend() string                      { return r.entry.Backend }
func (r ClosedLeaseReceipt) BackendStorageID() backendidentity.ID { return r.storageID }
func (r ClosedLeaseReceipt) Tenant() string                       { return r.entry.Tenant }
func (r ClosedLeaseReceipt) ProviderUUID() string                 { return r.entry.ProviderUUID }
func (r ClosedLeaseReceipt) CleanupOnly() bool                    { return r.entry.CleanupOnly }
func (r ClosedLeaseReceipt) AuthorityKind() ClosedLeaseAuthorityKind {
	if r.entry.Tenant == "" {
		return ClosedLeaseAuthorityOrphan
	}
	return ClosedLeaseAuthorityPrincipal
}
func (r ClosedLeaseReceipt) ClosedAt() time.Time { return r.entry.ClosedAt }

func newClosedLeaseMutationHead(
	claim CloseIntentClaim,
	closedAt time.Time,
) (closedLeaseMutationHead, error) {
	entry := closedLeaseTombstone{
		CloseIntentID:    claim.IntentID(),
		LeaseUUID:        claim.LeaseUUID(),
		CallbackURL:      claim.CallbackURL(),
		Backend:          claim.Backend(),
		BackendStorageID: claim.BackendStorageID().String(),
		Tenant:           claim.Tenant(),
		ProviderUUID:     claim.ProviderUUID(),
		CleanupOnly:      claim.CleanupOnly(),
		ClosedAt:         closedAt,
	}
	data, err := marshalClosedLeaseTombstone(entry)
	if err != nil {
		return closedLeaseMutationHead{}, err
	}
	return closedLeaseMutationHead{entry: entry, digest: sha256.Sum256(data)}, nil
}

// LookupClosedLeaseReceipts resolves only the requested lease heads in one
// read transaction. Recovery uses this instead of scanning the permanent
// closed history: closed UUIDs never expire, while cleanup work is needed only
// for UUIDs that currently have a live substrate or projection witness.
func (s *CallbackStore) LookupClosedLeaseReceipts(
	leaseUUIDs []string,
) ([]ClosedLeaseReceipt, error) {
	unique := make(map[string]struct{}, len(leaseUUIDs))
	for _, leaseUUID := range leaseUUIDs {
		if err := validateCanonicalLeaseUUID(leaseUUID); err != nil {
			return nil, err
		}
		unique[leaseUUID] = struct{}{}
	}
	keys := slices.Sorted(maps.Keys(unique))
	if len(keys) == 0 {
		return nil, nil
	}
	receipts := make([]ClosedLeaseReceipt, 0, len(keys))
	err := s.view(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
		if bucket == nil {
			return fmt.Errorf("callback lease mutation head bucket missing")
		}
		for _, leaseUUID := range keys {
			value := bucket.Get([]byte(leaseUUID))
			if value == nil {
				continue
			}
			head, err := decodeLeaseMutationHead([]byte(leaseUUID), value)
			if err != nil {
				return err
			}
			closed, ok := head.(closedLeaseMutationHead)
			if !ok {
				continue
			}
			storageID, err := backendidentity.Parse(closed.entry.BackendStorageID)
			if err != nil {
				return err
			}
			receipts = append(receipts, ClosedLeaseReceipt{
				entry: closed.entry, storageID: storageID,
			})
		}
		return nil
	})
	return receipts, err
}

func closedLeaseTombstoneMatchesProbe(
	entry closedLeaseTombstone,
	probe OperationIntentProbe,
) bool {
	return entry.LeaseUUID == probe.leaseUUID &&
		entry.CallbackURL != "" &&
		entry.CallbackURL == probe.callbackURL &&
		entry.Backend == probe.backend &&
		entry.BackendStorageID == probe.storageID.String()
}

func decodeClosedLeaseTombstone(key, value []byte) (closedLeaseTombstone, error) {
	var entry closedLeaseTombstone
	if err := decodeStrictAuthoritativeObject(value, maxCloseIntentEntryBytes, &entry); err != nil {
		return closedLeaseTombstone{}, fmt.Errorf("decode closed callback lease %q: %w", key, err)
	}
	if err := validateClosedLeaseTombstone(entry, string(key)); err != nil {
		return closedLeaseTombstone{}, fmt.Errorf("invalid closed callback lease %q: %w", key, err)
	}
	return entry, nil
}

func marshalClosedLeaseTombstone(entry closedLeaseTombstone) ([]byte, error) {
	if err := validateClosedLeaseTombstone(entry, entry.LeaseUUID); err != nil {
		return nil, err
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return nil, fmt.Errorf("marshal closed callback lease: %w", err)
	}
	if len(data) > maxCloseIntentEntryBytes {
		return nil, fmt.Errorf("closed callback lease exceeds %d bytes", maxCloseIntentEntryBytes)
	}
	return data, nil
}

func validateClosedLeaseTombstone(entry closedLeaseTombstone, leaseUUID string) error {
	if _, err := parseCloseIntentID(entry.CloseIntentID); err != nil {
		return err
	}
	if err := validateCanonicalLeaseUUID(leaseUUID); err != nil {
		return err
	}
	if entry.LeaseUUID != leaseUUID {
		return fmt.Errorf("closed callback lease mismatch: key %q contains %q", leaseUUID, entry.LeaseUUID)
	}
	if err := backendname.Validate(entry.Backend); err != nil {
		return fmt.Errorf("closed callback lease backend: %w", err)
	}
	if _, err := backendidentity.Parse(entry.BackendStorageID); err != nil {
		return fmt.Errorf("closed callback lease storage identity: %w", err)
	}
	if entry.CleanupOnly {
		if (entry.Tenant == "") != (entry.ProviderUUID == "") {
			return fmt.Errorf("cleanup-only closed callback lease principal must be wholly absent or wholly present")
		}
		if entry.Tenant != "" {
			if err := validateCloseIntentIdentity("tenant", entry.Tenant); err != nil {
				return err
			}
		}
		if entry.ProviderUUID != "" {
			if err := validateCloseIntentIdentity("provider", entry.ProviderUUID); err != nil {
				return err
			}
		}
	} else {
		if err := validateCloseIntentIdentity("tenant", entry.Tenant); err != nil {
			return err
		}
		if err := validateCloseIntentIdentity("provider", entry.ProviderUUID); err != nil {
			return err
		}
	}
	if entry.CallbackURL != "" {
		if err := validateCallbackDestination(entry.CallbackURL); err != nil {
			return err
		}
		if err := backend.ValidateOperationCallbackURL(entry.CallbackURL); err != nil {
			return fmt.Errorf("closed callback lease has invalid operation callback: %w", err)
		}
	}
	return validateStoredCallbackCreatedAt(entry.ClosedAt)
}
