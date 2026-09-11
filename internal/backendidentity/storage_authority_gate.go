package backendidentity

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var errInvalidStorageAuthorityGate = errors.New("storage authority gate is not initialized")

// StorageAuthorityGate linearizes authoritative journal commits with terminal
// backend-storage withdrawal. Every identity-bound Commit and its immediate
// pathname postcheck execute through Run; Latch waits for an already-admitted
// boundary before it publishes the terminal cause. Consequently a write either
// commits before withdrawal or is rejected and rolled back—none can commit
// after the latch is visible.
//
// The gate intentionally owns a mutex distinct from each backend's substrate
// identity-verification mutex. A store can discover terminal drift while that
// verifier mutex is held without recursively acquiring it.
type StorageAuthorityGate struct {
	mu sync.Mutex
	// failure is published through an immutable box so Error stays lock-free.
	// Authoritative bbolt reads intentionally re-check the gate while holding a
	// read transaction; taking mu there inverts bbolt's mmap lock against Run's
	// commit boundary (gate -> mmap). Atomic publication preserves those mid- and
	// post-read checks without introducing another lock edge.
	failure   atomic.Pointer[storageAuthorityFailure]
	onFailure func(error)
}

type storageAuthorityFailure struct {
	err error
}

// NewStorageAuthorityGate constructs a backend-lifetime authority gate with
// its immutable failure hook. Identity-bound stores accept only a gate created
// here, so terminal authority cannot be cleared by swapping the gate or its
// cancellation handler after the stores become reachable.
func NewStorageAuthorityGate(onFailure func(error)) (*StorageAuthorityGate, error) {
	if onFailure == nil {
		return nil, errors.New("storage authority failure handler is required")
	}
	return &StorageAuthorityGate{onFailure: onFailure}, nil
}

// Valid reports whether gate was constructed with its immutable failure hook.
func (gate *StorageAuthorityGate) Valid() bool {
	return gate != nil && gate.onFailure != nil
}

// Error returns the first terminal storage-authority failure, if any.
func (gate *StorageAuthorityGate) Error() error {
	if !gate.Valid() {
		return errInvalidStorageAuthorityGate
	}
	return gate.loadFailure()
}

func (gate *StorageAuthorityGate) loadFailure() error {
	failure := gate.failure.Load()
	if failure == nil {
		return nil
	}
	return failure.err
}

// publishFailureLocked replaces the immutable published failure. The caller
// must hold mu, which serializes first-failure selection and promotion; readers
// need only one atomic load and can safely call Error while holding other locks.
func (gate *StorageAuthorityGate) publishFailureLocked(err error) {
	gate.failure.Store(&storageAuthorityFailure{err: err})
}

// Run executes one authoritative durability boundary under the backend-wide
// authority gate. A terminal error returned by write is published before the
// gate admits another commit. The failure handler runs after unlocking so
// cancellation code can safely query the latch.
func (gate *StorageAuthorityGate) Run(write func() error) error {
	if !gate.Valid() {
		return errInvalidStorageAuthorityGate
	}
	if write == nil {
		return errors.New("authoritative storage write is required")
	}

	latched, handler, notify, panicValue, panicked, err := gate.runCriticalSection(write)
	if notify && handler != nil {
		handler(latched)
	}
	if panicked {
		panic(panicValue)
	}
	return err
}

// runCriticalSection publishes an ambiguous terminal outcome and releases the
// gate if write panics. Run invokes the cancellation hook after unlocking and
// then propagates the original value to the backend's goroutine boundary.
func (gate *StorageAuthorityGate) runCriticalSection(
	write func() error,
) (
	latched error,
	handler func(error),
	notify bool,
	panicValue any,
	panicked bool,
	err error,
) {
	gate.mu.Lock()
	defer func() {
		panicValue = recover()
		if panicValue != nil {
			panicked = true
			err = fmt.Errorf(
				"%w: panic inside authoritative storage durability boundary: %v",
				ErrMutationOutcomeAmbiguous,
				panicValue,
			)
			latched, notify = gate.latchTerminalLocked(err)
			handler = gate.onFailure
		}
		gate.mu.Unlock()
	}()
	if failure := gate.loadFailure(); failure != nil {
		return nil, nil, false, nil, false, failure
	}
	err = write()
	latched, notify = gate.latchTerminalLocked(err)
	return latched, gate.onFailure, notify, nil, false, err
}

// Latch publishes the first terminal storage-authority cause. If a write is
// currently inside Run, Lock waits until that write's complete durability
// boundary returns; publication and backend cancellation therefore happen only
// after the earlier transaction can no longer commit.
func (gate *StorageAuthorityGate) Latch(cause error) error {
	if cause == nil {
		return nil
	}
	if !gate.Valid() {
		return errors.Join(errInvalidStorageAuthorityGate, cause)
	}
	gate.mu.Lock()
	latched, notify := gate.latchTerminalLocked(cause)
	handler := gate.onFailure
	gate.mu.Unlock()
	if notify && handler != nil {
		handler(latched)
	}
	return latched
}

// PromoteAmbiguous replaces a previously latched lower-level identity-drift
// cause with the ambiguity classification that wraps it. This is used only
// after a substrate mutation has already run: VerifyStorageIdentity first
// publishes the raw postcheck drift, then the mutation boundary establishes
// that the side effect's outcome is unknown. The original cause remains in the
// promoted error chain.
func (gate *StorageAuthorityGate) PromoteAmbiguous(cause error) error {
	if cause == nil {
		return nil
	}
	if !gate.Valid() {
		return errors.Join(errInvalidStorageAuthorityGate, cause)
	}
	if !errors.Is(cause, ErrMutationOutcomeAmbiguous) {
		return gate.Latch(cause)
	}
	gate.mu.Lock()
	failure := gate.loadFailure()
	if failure != nil && !errors.Is(cause, failure) {
		// Never replace an independent first failure. Promotion is valid only
		// when the ambiguity classification preserves the already-published
		// lower-level cause in its error chain.
		latched := failure
		gate.mu.Unlock()
		return latched
	}
	notify := failure == nil
	if failure == nil || !errors.Is(failure, ErrMutationOutcomeAmbiguous) {
		gate.publishFailureLocked(cause)
	}
	latched := gate.loadFailure()
	handler := gate.onFailure
	gate.mu.Unlock()
	if notify && handler != nil {
		handler(latched)
	}
	return latched
}

func (gate *StorageAuthorityGate) latchTerminalLocked(cause error) (error, bool) {
	failure := gate.loadFailure()
	if cause == nil || (!errors.Is(cause, ErrIdentityDrift) &&
		!errors.Is(cause, ErrMutationOutcomeAmbiguous)) {
		return failure, false
	}
	if failure == nil {
		gate.publishFailureLocked(cause)
		return cause, true
	}
	return failure, false
}
