package storeauthority

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrInvalidGate reports use of a zero-value gate or a constructor without
	// the terminal-error classifier needed to distinguish withdrawal from an
	// ordinary rejected transaction.
	ErrInvalidGate = errors.New("store authority gate is not initialized")

	// ErrNonTerminalWithdrawal reports an attempt to publish an error which the
	// gate's immutable classifier does not recognize as terminal.
	ErrNonTerminalWithdrawal = errors.New("store authority withdrawal cause is not terminal")

	// ErrWriteBoundaryPanicked reports a panic after a durable-store write
	// boundary began. Even if a deferred rollback releases bbolt's writer lock,
	// the process cannot prove which side effects completed, so authority is
	// permanently withdrawn before the original panic is propagated.
	ErrWriteBoundaryPanicked = errors.New("store authority write boundary panicked")
)

// TerminalClassifier identifies errors which permanently withdraw one store's
// process-lifetime authority. It is fixed when the gate is constructed.
type TerminalClassifier func(error) bool

// FailureHandler observes the first published terminal failure after the gate
// has released its mutex. It is fixed when the gate is constructed and may be
// nil when the owner has no lifecycle to cancel.
type FailureHandler func(error)

// Gate linearizes a durable write boundary with terminal authority withdrawal.
// Run holds the gate across the caller's precheck, Commit, and postcheck. A
// write already admitted to Run may finish before a concurrent Withdraw is
// published; after the failure is published, no later Run callback can execute.
//
// The zero value is invalid. Construct a Gate with New and retain the same
// pointer for the complete lifetime of the store it protects.
type Gate struct {
	mu        sync.Mutex
	failure   error
	terminal  TerminalClassifier
	onFailure FailureHandler
}

// New constructs a process-lifetime authority gate. terminal is mandatory;
// onFailure is optional but, when supplied, is immutable.
func New(terminal TerminalClassifier, onFailure FailureHandler) (*Gate, error) {
	if terminal == nil {
		return nil, errors.New("store authority terminal-error classifier is required")
	}
	return &Gate{terminal: terminal, onFailure: onFailure}, nil
}

// Valid reports whether gate was built by New with its immutable classifier.
func (gate *Gate) Valid() bool {
	return gate != nil && gate.terminal != nil
}

// Error returns the first terminal authority failure, if one has been
// published.
func (gate *Gate) Error() error {
	if !gate.Valid() {
		return ErrInvalidGate
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	return gate.failure
}

// Run executes one complete durable-write boundary. A terminal error returned
// by boundary is published before another writer can be admitted. Ordinary
// mutation errors are returned without withdrawing authority.
func (gate *Gate) Run(boundary func() error) error {
	if !gate.Valid() {
		return ErrInvalidGate
	}
	if boundary == nil {
		return errors.New("store authority write boundary is required")
	}

	notify, panicValue, panicked, err := gate.runCriticalSection(boundary)
	if notify && gate.onFailure != nil {
		gate.onFailure(err)
	}
	if panicked {
		panic(panicValue)
	}
	return err
}

func (gate *Gate) runCriticalSection(
	boundary func() error,
) (notify bool, panicValue any, panicked bool, err error) {
	gate.mu.Lock()
	defer func() {
		panicValue = recover()
		if panicValue != nil {
			panicked = true
			err, notify = gate.latchPanicLocked(panicValue)
		}
		gate.mu.Unlock()
	}()
	if gate.failure != nil {
		return false, nil, false, gate.failure
	}
	err = boundary()
	if err != nil && gate.terminal(err) {
		gate.failure = err
		return true, nil, false, gate.failure
	}
	return false, nil, false, err
}

// latchPanicLocked records a boundary panic independently of terminal. A
// caller-selected classifier is appropriate for ordinary returned errors, but
// no classifier can make an interrupted Go control flow a definite commit
// outcome. Caller holds gate.mu.
func (gate *Gate) latchPanicLocked(panicValue any) (error, bool) {
	if gate.failure != nil {
		return gate.failure, false
	}
	var failure error
	if cause, ok := panicValue.(error); ok {
		failure = fmt.Errorf("%w: %w", ErrWriteBoundaryPanicked, cause)
	} else {
		failure = fmt.Errorf("%w: %v", ErrWriteBoundaryPanicked, panicValue)
	}
	gate.failure = failure
	return gate.failure, true
}

// Withdraw publishes cause as the first terminal authority failure. If a Run
// callback is active, Withdraw waits for its complete durability boundary; the
// admitted write therefore finishes before withdrawal becomes visible.
func (gate *Gate) Withdraw(cause error) error {
	if cause == nil {
		return nil
	}
	if !gate.Valid() {
		return errors.Join(ErrInvalidGate, cause)
	}
	if !gate.terminal(cause) {
		return fmt.Errorf("%w: %w", ErrNonTerminalWithdrawal, cause)
	}

	gate.mu.Lock()
	notify := false
	if gate.failure == nil {
		gate.failure = cause
		notify = true
	}
	failure := gate.failure
	gate.mu.Unlock()
	if notify && gate.onFailure != nil {
		gate.onFailure(failure)
	}
	return failure
}
