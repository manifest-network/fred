package storeauthority

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHoldExcludesWritersWithoutGrantingWithdrawal(t *testing.T) {
	gate := newTestGate(t, nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	holdDone := make(chan error, 1)
	go func() {
		holdDone <- gate.Hold(func() error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	writeDone := make(chan error, 1)
	go func() { writeDone <- gate.Run(func() error { return nil }) }()
	select {
	case <-writeDone:
		t.Fatal("a writer ran while a read section held the gate")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-holdDone)
	require.NoError(t, <-writeDone)
}

func TestHoldErrorsAndPanicsNeverWithdrawAuthority(t *testing.T) {
	gate := newTestGate(t, nil)
	require.ErrorIs(t, gate.Hold(func() error { return errTerminalTestFailure }), errTerminalTestFailure)
	assert.NoError(t, gate.Error(), "a terminal-classified error from a read section is not a withdrawal")

	assert.PanicsWithValue(t, "read section panicked", func() {
		_ = gate.Hold(func() error { panic("read section panicked") })
	})
	assert.NoError(t, gate.Error(), "a read section panic leaves no partial write to latch")
	require.NoError(t, gate.Run(func() error { return nil }), "the gate is released after the panic")
}

func TestHoldRefusesAWithdrawnOrInvalidGate(t *testing.T) {
	gate := newTestGate(t, nil)
	require.ErrorIs(t, gate.Withdraw(errTerminalTestFailure), errTerminalTestFailure)
	ran := false
	err := gate.Hold(func() error { ran = true; return nil })
	assert.ErrorIs(t, err, errTerminalTestFailure)
	assert.False(t, ran)

	var zero Gate
	assert.ErrorIs(t, zero.Hold(func() error { return nil }), ErrInvalidGate)
	assert.Error(t, gate.Hold(nil))
	assert.False(t, errors.Is(newTestGate(t, nil).Hold(nil), ErrInvalidGate))
}
