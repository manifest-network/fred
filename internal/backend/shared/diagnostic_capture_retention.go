package shared

import (
	"errors"

	bolt "go.etcd.io/bbolt"
)

// retainedDiagnostic pins a durable attempt only while its exact physical
// cleanup is active. Pins protect deletion, without holding a bbolt transaction
// or its mmap lock across Docker calls. They need not survive restart: the
// durable record survives, while the in-process cleanup cannot.
type retainedDiagnostic struct {
	readers int
	retire  bool
}

// update serializes brief record mutations with pin acquisition/release.
// All attempt deletion flows through deleteAttemptTx under this lock.
func (store *DiagnosticsStore) update(action func(*bolt.Tx) error) error {
	store.retentionMu.Lock()
	defer store.retentionMu.Unlock()
	if store.closing {
		return bolt.ErrDatabaseNotOpen
	}
	return store.boltStore.update(action)
}

// Close waits for physical cleanup to relinquish its saved diagnostic, without
// stopping an unrelated writer behind a read transaction's mmap lock.
func (store *DiagnosticsStore) Close() error {
	store.retentionMu.Lock()
	store.closing = true
	store.retentionMu.Unlock()
	store.actions.Wait()
	return store.boltStore.Close()
}

func (store *DiagnosticsStore) deleteAttemptTx(tx *bolt.Tx, key []byte) error {
	if retained := store.retained[string(key)]; retained != nil {
		// A rolled-back cleanup must not become a deferred deletion.
		tx.OnCommit(func() { retained.retire = true })
		return nil
	}
	return tx.Bucket(attemptDiagnosticsBucketName).Delete(key)
}

func (store *DiagnosticsStore) keepAttemptTx(tx *bolt.Tx, key []byte) {
	if retained := store.retained[string(key)]; retained != nil {
		// A later capture/publication supersedes an earlier deletion request.
		tx.OnCommit(func() { retained.retire = false })
	}
}

func (store *DiagnosticsStore) retainAttempt(identity diagnosticAttemptIdentity) error {
	store.retentionMu.Lock()
	defer store.retentionMu.Unlock()
	if store.closing {
		return bolt.ErrDatabaseNotOpen
	}
	// End the read transaction before admitting any physical action.
	if _, err := store.readAttempt(identity); err != nil {
		return err
	}
	key := identity.key()
	retained := store.retained[key]
	if retained == nil {
		retained = &retainedDiagnostic{}
		store.retained[key] = retained
	}
	retained.readers++
	store.actions.Add(1)
	return nil
}

func (store *DiagnosticsStore) releaseAttempt(identity diagnosticAttemptIdentity) error {
	defer store.actions.Done()
	store.retentionMu.Lock()
	defer store.retentionMu.Unlock()
	key := identity.key()
	retained := store.retained[key]
	if retained == nil || retained.readers <= 0 {
		return errors.New("diagnostic capture retention is unavailable")
	}
	retained.readers--
	if retained.readers != 0 {
		return nil
	}
	delete(store.retained, key)
	if !retained.retire {
		return nil
	}
	// Close has not reached bbolt yet: it waits for this action's Done above.
	return store.boltStore.update(func(tx *bolt.Tx) error {
		return store.deleteAttemptTx(tx, []byte(key))
	})
}
