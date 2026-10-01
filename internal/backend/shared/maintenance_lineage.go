package shared

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
)

// maintenanceLineageBucketName holds one ordering row per live lease. It is
// created lazily on the first write, so a journal that predates the rolling
// receipt window stays readable, and it is deleted with the lease's history
// when the lease closes.
var maintenanceLineageBucketName = []byte("completed_callback_maintenance_lineage")

const (
	maintenanceLineageV1         = 1
	maxMaintenanceLineageBytes   = 1 << 10
	failedReceiptEvictionGrace   = time.Hour
	maxProviderMaintenanceWindow = 1_024
	// maxCustomDomainMaintenanceWindow bounds the backend-minted receipts the
	// custom-domain reconciler leaves behind. They live in their own window so
	// reconciler churn can never evict a tenant's restart or update receipt.
	maxCustomDomainMaintenanceWindow = 64
)

// ErrMaintenanceExpired is a pre-side-effect refusal: a provider restart or
// update is older than this lease's admission high-water mark, so its receipt
// may have left the rolling window. Executing it could move the lease
// backward; it is refused instead of being treated as new work.
var ErrMaintenanceExpired = errors.New("maintenance command is older than the lease's retained history")

// MaintenanceExpiredError carries the admission high-water mark the refused
// command was older than.
type MaintenanceExpiredError struct {
	LeaseUUID string
	HighWater time.Time
}

func (e *MaintenanceExpiredError) Error() string {
	if e == nil || e.HighWater.IsZero() {
		return ErrMaintenanceExpired.Error()
	}
	return fmt.Sprintf("%s for lease %q (high-water %s)",
		ErrMaintenanceExpired, e.LeaseUUID, e.HighWater.UTC().Format(time.RFC3339Nano))
}

func (e *MaintenanceExpiredError) Unwrap() []error {
	return []error{ErrMaintenanceExpired, backend.ErrMaintenanceExpired}
}

// maintenanceLineage is the durable ordering fact that lets a lease keep only
// a rolling window of maintenance receipts. Each field only moves forward, and
// none depends on which receipts are still retained.
type maintenanceLineage struct {
	Version uint8 `json:"version"`
	// HighWaterAdmittedAt and HighWaterID name the newest provider-stamped
	// restart or update ever published for the lease. A stamped command older
	// than it cannot be new work.
	HighWaterAdmittedAt time.Time     `json:"high_water_admitted_at,omitzero"`
	HighWaterID         MaintenanceID `json:"high_water_id,omitzero"`
	// LatestUpdateSequence is the completion sequence of the newest settled
	// update, so supersession never depends on the retained set.
	LatestUpdateSequence uint64 `json:"latest_update_sequence,omitempty"`
	// CleanupConfirmedSequence: every failed, effect-started receipt at or
	// below it either had its late-arrival cleanup attested after the grace
	// window or can no longer authorize cleanup, so it no longer needs to be
	// retained for that authority.
	CleanupConfirmedSequence uint64 `json:"cleanup_confirmed_sequence,omitempty"`
	// EvictedThroughReleaseVersion is the highest target release version of
	// any evicted receipt. Release versions only increase and one maintenance
	// head runs at a time, so every attempt at or below it has completed.
	EvictedThroughReleaseVersion int `json:"evicted_through_release_version,omitempty"`
	// ProviderEvicted records that a provider receipt has left the window, so
	// an unstamped provider request can no longer prove it is new work.
	ProviderEvicted bool `json:"provider_evicted,omitempty"`
}

func validateMaintenanceLineage(lineage maintenanceLineage) error {
	if lineage.Version != maintenanceLineageV1 {
		return fmt.Errorf("maintenance lineage has unsupported version %d", lineage.Version)
	}
	if lineage.HighWaterAdmittedAt.IsZero() != lineage.HighWaterID.IsZero() {
		return errors.New("maintenance lineage high-water mark is incomplete")
	}
	if !lineage.HighWaterAdmittedAt.IsZero() {
		if err := validateMaintenanceAdmissionStamp(lineage.HighWaterAdmittedAt); err != nil {
			return fmt.Errorf("maintenance lineage high-water mark: %w", err)
		}
	}
	if lineage.EvictedThroughReleaseVersion < 0 {
		return errors.New("maintenance lineage evicted release version is invalid")
	}
	return nil
}

// validateMaintenanceAdmissionStamp accepts a UTC instant on or after the Unix
// epoch with no monotonic reading, so stored stamps compare exactly.
func validateMaintenanceAdmissionStamp(stamp time.Time) error {
	if stamp.IsZero() || stamp.Before(time.Unix(0, 0)) {
		return errors.New("maintenance admission stamp precedes the Unix epoch")
	}
	if stamp.Location() != time.UTC || stamp != stamp.Round(0) {
		return errors.New("maintenance admission stamp must be a plain UTC instant")
	}
	return nil
}

func loadStoredMaintenanceLineageTx(tx *bolt.Tx, leaseUUID string) (maintenanceLineage, bool, error) {
	bucket := tx.Bucket(maintenanceLineageBucketName)
	if bucket == nil {
		return maintenanceLineage{}, false, nil
	}
	value := bucket.Get([]byte(leaseUUID))
	if value == nil {
		if bucket.Bucket([]byte(leaseUUID)) != nil {
			return maintenanceLineage{}, false, fmt.Errorf("maintenance lineage %q is a nested bucket", leaseUUID)
		}
		return maintenanceLineage{}, false, nil
	}
	var lineage maintenanceLineage
	if err := decodeStrictAuthoritativeObject(value, maxMaintenanceLineageBytes, &lineage); err != nil {
		return maintenanceLineage{}, false, fmt.Errorf("decode maintenance lineage for lease %q: %w", leaseUUID, err)
	}
	if err := validateMaintenanceLineage(lineage); err != nil {
		return maintenanceLineage{}, false, fmt.Errorf("invalid maintenance lineage for lease %q: %w", leaseUUID, err)
	}
	return lineage, true, nil
}

// loadMaintenanceLineageTx returns the lease's lineage. A lease with no row
// has never evicted anything, so its retained receipts are complete and the
// lineage is derived from them exactly.
func loadMaintenanceLineageTx(tx *bolt.Tx, leaseUUID string) (maintenanceLineage, error) {
	lineage, stored, err := loadStoredMaintenanceLineageTx(tx, leaseUUID)
	if err != nil || stored {
		return lineage, err
	}
	records, err := listMaintenanceReceiptsTx(tx, leaseUUID)
	if err != nil {
		return maintenanceLineage{}, err
	}
	lineage = maintenanceLineage{Version: maintenanceLineageV1}
	for _, record := range records {
		if record.Kind == MaintenanceIntentUpdate && record.CompletionSequence > lineage.LatestUpdateSequence {
			lineage.LatestUpdateSequence = record.CompletionSequence
		}
	}
	return lineage, nil
}

func putMaintenanceLineageTx(tx *bolt.Tx, leaseUUID string, lineage maintenanceLineage) error {
	if err := validateCanonicalLeaseUUID(leaseUUID); err != nil {
		return err
	}
	if err := validateMaintenanceLineage(lineage); err != nil {
		return err
	}
	data, err := json.Marshal(lineage)
	if err != nil {
		return fmt.Errorf("marshal maintenance lineage: %w", err)
	}
	if len(data) > maxMaintenanceLineageBytes {
		return fmt.Errorf("maintenance lineage exceeds %d bytes", maxMaintenanceLineageBytes)
	}
	bucket, err := tx.CreateBucketIfNotExists(maintenanceLineageBucketName)
	if err != nil {
		return err
	}
	return bucket.Put([]byte(leaseUUID), data)
}

func deleteMaintenanceLineageTx(tx *bolt.Tx, leaseUUID string) error {
	bucket := tx.Bucket(maintenanceLineageBucketName)
	if bucket == nil {
		return nil
	}
	return bucket.Delete([]byte(leaseUUID))
}

// validateMaintenanceWindowCounts bounds one lease's retained receipts per
// window; head is the kind of a live maintenance head, if any. Each window is
// checked on its own, as admission fills it. The custom-domain bound is the
// old lifetime limit of 1,024 so that a journal written before the windows
// existed, when one limit covered every kind, still validates; a newer
// custom-domain publish trims that window to 64.
func validateMaintenanceWindowCounts(
	leaseUUID string,
	records []maintenanceCompletionRecord,
	head *MaintenanceIntentKind,
) error {
	provider, custom := 0, 0
	for _, record := range records {
		switch maintenanceWindowFor(record.Kind) {
		case maintenanceWindowProvider:
			provider++
		case maintenanceWindowCustomDomain:
			custom++
		}
	}
	if provider > maxProviderMaintenanceWindow || custom > maxProviderMaintenanceWindow {
		return fmt.Errorf("maintenance receipt capacity exceeded for lease %q", leaseUUID)
	}
	if head == nil {
		return nil
	}
	inWindow := provider
	if maintenanceWindowFor(*head) == maintenanceWindowCustomDomain {
		inWindow = custom
	}
	if inWindow >= maxProviderMaintenanceWindow {
		return fmt.Errorf("maintenance head for lease %q has no reserved receipt capacity", leaseUUID)
	}
	return nil
}

// maintenanceWindowKind names the window a receipt kind counts against.
type maintenanceWindowKind uint8

const (
	maintenanceWindowInvalid maintenanceWindowKind = iota
	maintenanceWindowProvider
	maintenanceWindowCustomDomain
)

func maintenanceWindowFor(kind MaintenanceIntentKind) maintenanceWindowKind {
	switch kind {
	case MaintenanceIntentRestart, MaintenanceIntentUpdate:
		return maintenanceWindowProvider
	case MaintenanceIntentCustomDomain:
		return maintenanceWindowCustomDomain
	default:
		return maintenanceWindowInvalid
	}
}

func (window maintenanceWindowKind) limit() int {
	switch window {
	case maintenanceWindowProvider:
		return maxProviderMaintenanceWindow
	case maintenanceWindowCustomDomain:
		return maxCustomDomainMaintenanceWindow
	default:
		return 0
	}
}

// receiptEvictable reports whether a retained receipt may leave the window. A
// failed, effect-started receipt still carries late-arrival cleanup authority
// until a cleanup past the grace window was attested.
func (lineage maintenanceLineage) receiptEvictable(record maintenanceCompletionRecord) bool {
	if record.Status == backend.CallbackStatusFailed && record.EffectStarted {
		return record.CompletionSequence <= lineage.CleanupConfirmedSequence
	}
	return true
}

// planMaintenanceEvictions chooses, oldest completion first, the receipts
// of window that must leave so one more receipt of it fits. It refuses when
// the window is full of receipts that must stay.
func planMaintenanceEvictions(
	records []maintenanceCompletionRecord,
	lineage maintenanceLineage,
	window maintenanceWindowKind,
) ([]maintenanceCompletionRecord, bool) {
	inWindow := make([]maintenanceCompletionRecord, 0, len(records))
	for _, record := range records {
		if maintenanceWindowFor(record.Kind) == window {
			inWindow = append(inWindow, record)
		}
	}
	excess := len(inWindow) - (window.limit() - 1)
	if excess <= 0 {
		return nil, true
	}
	slices.SortFunc(inWindow, func(a, b maintenanceCompletionRecord) int {
		switch {
		case a.CompletionSequence < b.CompletionSequence:
			return -1
		case a.CompletionSequence > b.CompletionSequence:
			return 1
		default:
			return 0
		}
	})
	evicted := make([]maintenanceCompletionRecord, 0, excess)
	for _, record := range inWindow {
		if len(evicted) == excess {
			break
		}
		if lineage.receiptEvictable(record) {
			evicted = append(evicted, record)
		}
	}
	return evicted, len(evicted) == excess
}

// evictMaintenanceReceiptsTx deletes planned receipts, releases exactly one
// global reservation per receipt, and advances the lineage facts eviction
// would otherwise erase.
func evictMaintenanceReceiptsTx(
	tx *bolt.Tx,
	leaseUUID string,
	lineage *maintenanceLineage,
	evicted []maintenanceCompletionRecord,
) error {
	if len(evicted) == 0 {
		return nil
	}
	root := tx.Bucket(callbackMaintenanceHistoryBucketName)
	if root == nil {
		return errors.New("completed maintenance history bucket missing")
	}
	leaseBucket := root.Bucket([]byte(leaseUUID))
	if leaseBucket == nil {
		return fmt.Errorf("completed maintenance history %q is missing", leaseUUID)
	}
	for _, record := range evicted {
		if err := leaseBucket.Delete([]byte(record.MaintenanceID.String())); err != nil {
			return err
		}
		if maintenanceWindowFor(record.Kind) == maintenanceWindowProvider {
			lineage.ProviderEvicted = true
		}
		if record.Kind == MaintenanceIntentUpdate && record.CompletionSequence > lineage.LatestUpdateSequence {
			lineage.LatestUpdateSequence = record.CompletionSequence
		}
		if record.TargetReleaseVersion > lineage.EvictedThroughReleaseVersion {
			lineage.EvictedThroughReleaseVersion = record.TargetReleaseVersion
		}
	}
	return releaseCallbackReceiptReservationsTx(tx, uint64(len(evicted)))
}
