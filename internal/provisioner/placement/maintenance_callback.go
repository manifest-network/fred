package placement

import (
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/callbackwire"
)

// recordMaintenanceCompletion is called only after this coordinator has
// authenticated the original request and verified its lifecycle and storage
// lineage. It records a receipt without acquiring the Registry lease lane:
// that lane stays held by maintenance until payload finalization or failure.
// Thus a callback arriving before the HTTP acceptance cannot deadlock against
// the request it completes. This transaction never writes committed payloads.
func (coordinator *AuthenticatedCallbackCoordinator) recordMaintenanceCompletion(
	callback callbackwire.Observation,
	backendName string,
) error {
	if !callback.MaintenanceID().Valid() {
		return nil
	}
	s := coordinator.coordinator.store
	s.mu.Lock()
	defer s.mu.Unlock()
	changed := false
	err := s.updateRuntimeAuthority(func(tx *bolt.Tx) error {
		pending, records, err := maintenanceCommandBuckets(tx)
		if err != nil {
			return err
		}
		key := maintenanceReceiptKey(callback.LeaseUUID(), callback.MaintenanceID())
		encoded := records.Get(key)
		if encoded == nil {
			return nil // Unknown or already reclaimed commands cannot grant authority.
		}
		command, outcome, createdAt, _, _, err := decodeMaintenanceCommand(encoded)
		if err != nil {
			return fmt.Errorf("%w: decode maintenance completion command: %w", ErrMaintenanceJournalCorrupt, err)
		}
		if command.BackendName() != backendName || command.BackendStorageID() != callback.StorageID() ||
			command.LifecycleID() != callback.LifecycleID() ||
			command.lifecycleLegacy != (callback.Selector() == callbackwire.SelectorLegacy) {
			return errors.New("maintenance completion lineage differs from exact command")
		}
		if command.Kind() != MaintenanceCommandUpdate || outcome != MaintenanceOutcomePending ||
			string(pending.Get([]byte(callback.LeaseUUID()))) != callback.MaintenanceID().String() {
			return nil
		}
		switch command.phase {
		case maintenanceDeliveryOutstanding, maintenanceCompletionOutstanding:
		case maintenancePayloadConfirmed:
			if callback.Status() != backend.CallbackStatusSuccess {
				return errors.New("maintenance completion contradicts durable success receipt")
			}
			return nil
		default:
			return ErrInvalidMaintenanceCommand
		}
		if callback.Status() == backend.CallbackStatusSuccess {
			command.phase = maintenancePayloadConfirmed
			value, err := encodeMaintenanceCommand(command, MaintenanceOutcomePending, createdAt, time.Time{})
			if err != nil {
				return err
			}
			if err := records.Put(key, value); err != nil {
				return err
			}
			changed = true
			return nil
		}
		if callback.Status() != backend.CallbackStatusFailed {
			return errors.New("maintenance completion has invalid terminal status")
		}
		settledAt := s.now().UTC()
		if settledAt.Before(createdAt) {
			settledAt = createdAt
		}
		value, _, err := encodeMaintenanceSettlement(command,
			maintenanceSettlement{outcome: MaintenanceOutcomeExecutionFailed}, createdAt, settledAt)
		if err != nil {
			return err
		}
		if err := records.Put(key, value); err != nil {
			return err
		}
		if err := pending.Delete([]byte(callback.LeaseUUID())); err != nil {
			return err
		}
		changed = true
		return nil
	})
	if err == nil && changed {
		s.maintenanceCompletionVersion.Add(1)
		s.wakeMaintenance()
	}
	return err

}
