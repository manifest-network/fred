package storeauthority

import (
	"errors"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errTerminalTestFailure = errors.New("terminal test failure")

func newTestGate(t *testing.T, handler FailureHandler) *Gate {
	t.Helper()
	gate, err := New(func(err error) bool { return errors.Is(err, errTerminalTestFailure) }, handler)
	require.NoError(t, err)
	return gate
}

func TestGateWithdrawalWaitsForAdmittedWrite(t *testing.T) {
	gate := newTestGate(t, nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- gate.Run(func() error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	withdrawDone := make(chan error, 1)
	go func() { withdrawDone <- gate.Withdraw(errTerminalTestFailure) }()
	runtime.Gosched()
	select {
	case <-withdrawDone:
		t.Fatal("withdrawal became visible before the admitted write completed")
	default:
	}

	close(release)
	require.NoError(t, <-writeDone)
	assert.ErrorIs(t, <-withdrawDone, errTerminalTestFailure)

	var lateWriteRan atomic.Bool
	err := gate.Run(func() error {
		lateWriteRan.Store(true)
		return nil
	})
	assert.ErrorIs(t, err, errTerminalTestFailure)
	assert.False(t, lateWriteRan.Load())
}

func TestGatePublishesBoundaryFailureBeforeNotification(t *testing.T) {
	notified := make(chan error, 1)
	var gate *Gate
	gate = newTestGate(t, func(err error) {
		assert.ErrorIs(t, gate.Error(), errTerminalTestFailure)
		notified <- err
	})

	assert.ErrorIs(t, gate.Run(func() error { return errTerminalTestFailure }), errTerminalTestFailure)
	assert.ErrorIs(t, <-notified, errTerminalTestFailure)
}

func TestGateBoundaryPanicWithdrawsBeforePropagatingOriginalPanic(t *testing.T) {
	notified := make(chan error, 1)
	gate := newTestGate(t, func(err error) { notified <- err })
	panicValue := errors.New("synthetic durable write panic")

	func() {
		defer func() { require.Same(t, panicValue, recover()) }()
		_ = gate.Run(func() error { panic(panicValue) })
	}()

	latched := gate.Error()
	require.ErrorIs(t, latched, ErrWriteBoundaryPanicked)
	assert.ErrorIs(t, latched, panicValue)
	assert.EqualError(t, <-notified, latched.Error())

	var lateWriteRan atomic.Bool
	err := gate.Run(func() error {
		lateWriteRan.Store(true)
		return nil
	})
	assert.EqualError(t, err, latched.Error())
	assert.False(t, lateWriteRan.Load())
}

func TestGateRejectsNonTerminalWithdrawal(t *testing.T) {
	gate := newTestGate(t, nil)
	ordinary := errors.New("ordinary transaction rejection")
	err := gate.Withdraw(ordinary)
	assert.ErrorIs(t, err, ErrNonTerminalWithdrawal)
	assert.NoError(t, gate.Error())
	assert.NoError(t, gate.Run(func() error { return nil }))
}

func TestGateZeroValueAndMissingClassifierAreInvalid(t *testing.T) {
	gate, err := New(nil, nil)
	require.Error(t, err)
	assert.Nil(t, gate)

	var zero Gate
	assert.False(t, zero.Valid())
	assert.ErrorIs(t, zero.Error(), ErrInvalidGate)
	assert.ErrorIs(t, zero.Run(func() error { return nil }), ErrInvalidGate)
}
