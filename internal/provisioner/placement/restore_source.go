package placement

import (
	"fmt"

	"github.com/manifest-network/fred/internal/backendidentity"
)

// restoreSourceReservationStage closes the source-reservation lifetime over
// two stages. A source authorization cannot settle a target, and only durable
// target admission may replace it with a full RestoreClaim.
type restoreSourceReservationStage interface {
	isRestoreSourceReservationStage()
}

// restoreSourceReservation is issued while holding the Store mutex and kept
// live in the same reservation table consumed by all source mutation fences.
// Pointer identity distinguishes successive authorizations of the same exact
// record, including when the first request exits before creating a target.
type restoreSourceReservation struct {
	issuer      *Store
	revision    RecordRevision
	backendName string
	storageID   backendidentity.ID
	principal   runtimePrincipal
}

func (*restoreSourceReservation) isRestoreSourceReservationStage() {}
func (RestoreClaim) isRestoreSourceReservationStage()              {}

func (source *restoreSourceReservation) validFor(store *Store) bool {
	return source != nil && store != nil && source.issuer == store && source.revision.Valid() &&
		source.revision.issuer == store.recordIssuer && source.backendName != "" &&
		source.principal.validOptional()
}

// reserveRestoreSource atomically selects and fences current source ownership.
// This grants no dispatch authority: baseline, inventory barriers, exact
// reservation, namespace and target admission are rechecked at consumption.
func (s *Store) reserveRestoreSource(leaseUUID string) (*restoreSourceReservation, error) {
	if leaseUUID == "" {
		return nil, fmt.Errorf("%w: source lease UUID is required", ErrInvalidPlacement)
	}
	if err := s.reattestRuntimeAuthority(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.restoreSourceClaimedLocked(leaseUUID) {
		return nil, fmt.Errorf("%w: lease %q", ErrRestoreSourceClaimed, leaseUUID)
	}
	if s.attemptClaimedLocked(leaseUUID) {
		return nil, fmt.Errorf("%w: lease %q", ErrRestoreSourceUnavailable, leaseUUID)
	}
	record, exists := s.cache[leaseUUID]
	if !exists || record.State() == StateAbsent {
		return nil, fmt.Errorf("%w: lease %q", ErrRestoreSourceNotFound, leaseUUID)
	}
	revision := s.newRecordRevision(leaseUUID, record.revision)
	if record.State() != StateConfirmed || record.Attempt != "" || !revision.Valid() {
		return nil, fmt.Errorf("%w: lease %q", ErrRestoreSourceUnavailable, leaseUUID)
	}
	source := &restoreSourceReservation{
		issuer: s, revision: revision, backendName: record.Backend,
		storageID: s.backendStorageIDs[record.Backend],
		principal: s.lifecycleCache[leaseUUID].principal,
	}
	if !source.validFor(s) {
		return nil, ErrInvalidRestoreClaim
	}
	s.restoreClaims[leaseUUID] = source
	return source, nil
}

// releaseRestoreSource consumes only the still-pending authorization stage.
// Once target admission transfers ownership to RestoreClaim, synchronous
// settlement alone releases it; an old deferred release cannot revoke that
// dispatch or a later authorization of the same source.
func (s *Store) releaseRestoreSource(source *restoreSourceReservation) {
	if !source.validFor(s) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.restoreClaims[source.revision.leaseUUID] == source {
		delete(s.restoreClaims, source.revision.leaseUUID)
	}
}
