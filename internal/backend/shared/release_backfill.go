package shared

import (
	"context"
	"errors"
	"fmt"

	"github.com/manifest-network/fred/internal/backend"
)

// ReleaseBackfiller is the construction-bound authority for the three
// compare-and-swap upgrades which enrich an already-active Release. It is a
// separate capability from operation settlement: backfills do not settle an
// operation, but they must share the exact callback/release journal pair and
// its per-lease transition gate so they cannot cross lifecycle publication.
// The zero value is invalid.
type ReleaseBackfiller struct {
	journalPair
}

// NewReleaseBackfiller binds release upgrades to one exact, open
// callback/release journal pair. Matching paths or storage lineage are not
// enough: reopening either store requires constructing a new capability.
func NewReleaseBackfiller(
	callbacks *CallbackStore,
	releases *ReleaseStore,
) (*ReleaseBackfiller, error) {
	pair, err := newJournalPair(callbacks, releases)
	if err != nil {
		return nil, err
	}
	return &ReleaseBackfiller{journalPair: pair}, nil
}

func (backfiller *ReleaseBackfiller) valid() bool {
	return backfiller != nil && backfiller.journalPair.valid()
}

// BackfillActiveResourceProfilesContext freezes resource authority through the exact
// callback/release journal pair. The shared lease gate makes this compare-and-
// swap linearizable with runtime-observation permits and every operation,
// maintenance, and close transition.
func (backfiller *ReleaseBackfiller) BackfillActiveResourceProfilesContext(
	ctx context.Context,
	leaseUUID string,
	version int,
	items []backend.LeaseItem,
	resourceProfiles []SKUResourceSnapshot,
) error {
	if ctx == nil {
		return errors.New("resource profile backfill ownership context is required")
	}
	if !backfiller.valid() {
		return errors.New("resource profile backfill requires an open journal pair")
	}
	unlock, err := backfiller.lockLeaseContext(ctx, leaseUUID)
	if err != nil {
		return fmt.Errorf("acquire resource profile backfill authority: %w", err)
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return backfiller.releases.backfillActiveResourceProfiles(
		leaseUUID, version, items, resourceProfiles,
	)
}

// BackfillLegacyActiveAuthorityContext freezes a stopped v0.13 workload's topology
// through the exact callback/release journal pair. ReleaseStore deliberately
// exposes no independent writer which could bypass this lease transition gate.
func (backfiller *ReleaseBackfiller) BackfillLegacyActiveAuthorityContext(
	ctx context.Context,
	leaseUUID string,
	expected Release,
	items []backend.LeaseItem,
	resourceProfiles []SKUResourceSnapshot,
) error {
	if ctx == nil {
		return errors.New("legacy active authority backfill ownership context is required")
	}
	if !backfiller.valid() {
		return errors.New("legacy active authority backfill requires an open journal pair")
	}
	unlock, err := backfiller.lockLeaseContext(ctx, leaseUUID)
	if err != nil {
		return fmt.Errorf("acquire legacy active authority backfill: %w", err)
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return backfiller.releases.backfillLegacyActiveAuthority(
		leaseUUID, expected, items, resourceProfiles,
	)
}

// BackfillLegacyRuntimeAuthorityContext freezes the tokenless v0.13 callback
// principal through the exact callback/release journal pair. It cannot race a
// phase-qualified lifecycle publication between release re-attestation and
// callback enqueue because both transitions require this same lease gate.
func (backfiller *ReleaseBackfiller) BackfillLegacyRuntimeAuthorityContext(
	ctx context.Context,
	leaseUUID string,
	expected Release,
	authority LegacyRuntimeAuthority,
) error {
	if ctx == nil {
		return errors.New("legacy runtime authority backfill ownership context is required")
	}
	if !backfiller.valid() {
		return errors.New("legacy runtime authority backfill requires an open journal pair")
	}
	unlock, err := backfiller.lockLeaseContext(ctx, leaseUUID)
	if err != nil {
		return fmt.Errorf("acquire legacy runtime authority backfill: %w", err)
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return backfiller.releases.backfillLegacyRuntimeAuthority(leaseUUID, expected, authority)
}
