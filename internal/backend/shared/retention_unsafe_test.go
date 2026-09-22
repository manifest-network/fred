package shared

import (
	"time"

	"github.com/manifest-network/fred/internal/backend"
)

// The retention store's raw writers are deliberately unavailable to
// production callers. Focused storage tests retain a package-local shim so
// they can construct malformed and historical rows without reopening a public
// mutation surface.
func (s *RetentionStore) putForTest(entry RetentionEntry) error { return s.putUnsafe(entry) }

func (s *RetentionStore) putActiveMergedForTest(entry RetentionEntry) (bool, error) {
	return s.putActiveMerged(entry, nil)
}

func (s *RetentionStore) putReapingForTest(entry RetentionEntry) (bool, error) {
	_, ok, err := s.putReaping(entry, nil)
	return ok, err
}

func (s *RetentionStore) deleteForTest(orig string) error { return s.deleteUnsafe(orig) }

func (s *RetentionStore) deleteIfRestoringForTest(
	orig, destination string,
	generation int,
) (bool, error) {
	return s.deleteIfRestoringUnsafe(orig, destination, generation)
}

func (s *RetentionStore) deleteIfActiveForTest(orig string) ([]string, bool, error) {
	return s.deleteIfActiveUnsafe(orig)
}

func (s *RetentionStore) updateRestoringDestinationCallbacksForTest(
	orig, destination string,
	generation int,
	callbackURL, lifecycleCallbackURL string,
) (bool, error) {
	return s.updateRestoringDestinationCallbacksUnsafe(
		orig, destination, generation, callbackURL, lifecycleCallbackURL,
	)
}

func (s *RetentionStore) markReapingIfActiveForTest(orig string) ([]string, bool, error) {
	return s.markReapingIfActiveUnsafe(orig)
}

func (s *RetentionStore) markReapingIfExpiredForTest(
	orig string,
	maxAge time.Duration,
) ([]string, bool, error) {
	return s.markReapingIfExpiredUnsafe(orig, maxAge)
}

func (s *RetentionStore) revertToActiveWithResourceProfilesForTest(
	orig, destination string,
	generation int,
	profiles []SKUResourceSnapshot,
) (bool, error) {
	return s.revertToActiveWithResourceProfilesUnsafe(
		orig, destination, generation, profiles,
	)
}

func (s *RetentionStore) claimForRestoreWithAuthorityForTest(
	orig, destination string,
	maxAge time.Duration,
	destinationItems []backend.LeaseItem,
	destinationProfiles []SKUResourceSnapshot,
	operationID OperationID,
	callbackURL, lifecycleCallbackURL string,
) (*RetentionEntry, error) {
	return s.claimForRestoreWithAuthorityUnsafe(
		orig, destination, maxAge, destinationItems, destinationProfiles,
		operationID, callbackURL, lifecycleCallbackURL,
	)
}

func (s *RetentionStore) claimForRestoreWithAuthorityAtForTest(
	orig, destination string,
	maxAge time.Duration,
	destinationItems []backend.LeaseItem,
	destinationProfiles []SKUResourceSnapshot,
	operationID OperationID,
	callbackURL, lifecycleCallbackURL string,
	createdAt time.Time,
) (*RetentionEntry, error) {
	return s.claimForRestoreWithAuthorityAtUnsafe(
		orig, destination, maxAge, destinationItems, destinationProfiles,
		operationID, callbackURL, lifecycleCallbackURL, createdAt,
	)
}
