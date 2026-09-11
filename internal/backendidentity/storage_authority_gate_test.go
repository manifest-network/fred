package backendidentity

import (
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageAuthorityGateWithdrawalLinearizesAfterAdmittedCommit(t *testing.T) {
	gate, err := NewStorageAuthorityGate(func(error) {})
	require.NoError(t, err)
	commitEntered := make(chan struct{})
	boundaryLocked := make(chan bool, 1)
	allowCommit := make(chan struct{})
	writeDone := make(chan error, 1)
	var committed atomic.Bool
	go func() {
		writeDone <- gate.Run(func() error {
			gateLocked := !gate.mu.TryLock()
			if !gateLocked {
				gate.mu.Unlock()
			}
			boundaryLocked <- gateLocked
			close(commitEntered)
			<-allowCommit
			committed.Store(true)
			return nil
		})
	}()
	<-commitEntered
	require.True(t, <-boundaryLocked, "Run must hold authority through the commit boundary")

	cause := errors.Join(ErrIdentityDrift, assert.AnError)
	withdrawDone := make(chan error, 1)
	go func() { withdrawDone <- gate.Latch(cause) }()
	runtime.Gosched()
	select {
	case <-withdrawDone:
		t.Fatal("withdrawal became visible before the admitted commit completed")
	default:
	}

	close(allowCommit)
	require.NoError(t, <-writeDone)
	assert.True(t, committed.Load())
	assert.EqualError(t, <-withdrawDone, cause.Error())
	assert.EqualError(t, gate.Error(), cause.Error())

	var lateWriteRan atomic.Bool
	err = gate.Run(func() error {
		lateWriteRan.Store(true)
		return nil
	})
	assert.EqualError(t, err, cause.Error())
	assert.False(t, lateWriteRan.Load(), "a post-withdrawal transaction must not enter its commit boundary")
}

func TestStorageAuthorityGateWriteFailurePublishesBeforeNextAdmission(t *testing.T) {
	cause := errors.Join(ErrMutationOutcomeAmbiguous, assert.AnError)
	notified := make(chan error, 1)
	gate, err := NewStorageAuthorityGate(func(err error) { notified <- err })
	require.NoError(t, err)

	assert.EqualError(t, gate.Run(func() error { return cause }), cause.Error())
	assert.EqualError(t, <-notified, cause.Error())
	assert.EqualError(t, gate.Error(), cause.Error())

	lateWriteRan := false
	err = gate.Run(func() error {
		lateWriteRan = true
		return nil
	})
	assert.EqualError(t, err, cause.Error())
	assert.False(t, lateWriteRan)
}

func TestStorageAuthorityGateErrorDoesNotWaitBehindAdmittedCommit(t *testing.T) {
	gate, err := NewStorageAuthorityGate(func(error) {})
	require.NoError(t, err)
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

	readDone := make(chan error, 1)
	go func() { readDone <- gate.Error() }()
	select {
	case readErr := <-readDone:
		require.NoError(t, readErr)
	case <-time.After(time.Second):
		close(release)
		<-writeDone
		t.Fatal("authority publication read waited behind the commit mutex")
	}

	close(release)
	require.NoError(t, <-writeDone)
}

func TestStorageAuthorityGatePromotionPublishesReplacement(t *testing.T) {
	gate, err := NewStorageAuthorityGate(func(error) {})
	require.NoError(t, err)
	drift := errors.Join(ErrIdentityDrift, errors.New("store path replaced"))
	assert.EqualError(t, gate.Latch(drift), drift.Error())

	ambiguous := fmt.Errorf("%w: post-mutation check: %w", ErrMutationOutcomeAmbiguous, drift)
	assert.EqualError(t, gate.PromoteAmbiguous(ambiguous), ambiguous.Error())
	assert.EqualError(t, gate.Error(), ambiguous.Error(),
		"lock-free readers must observe the promoted immutable publication")
}

func TestStorageAuthorityGatePanicLatchesAmbiguousOutcomeAndUnlocks(t *testing.T) {
	notified := make(chan error, 1)
	gate, err := NewStorageAuthorityGate(func(err error) { notified <- err })
	require.NoError(t, err)
	panicValue := errors.New("synthetic write panic")
	func() {
		defer func() {
			require.Same(t, panicValue, recover())
		}()
		_ = gate.Run(func() error { panic(panicValue) })
	}()

	latched := gate.Error()
	require.Error(t, latched)
	assert.ErrorIs(t, latched, ErrMutationOutcomeAmbiguous)
	assert.ErrorContains(t, latched, panicValue.Error())
	assert.EqualError(t, <-notified, latched.Error())

	var lateWriteRan atomic.Bool
	err = gate.Run(func() error {
		lateWriteRan.Store(true)
		return nil
	})
	assert.EqualError(t, err, latched.Error())
	assert.False(t, lateWriteRan.Load())
	assert.EqualError(t, gate.Latch(errors.Join(ErrIdentityDrift, assert.AnError)), latched.Error())
}

func TestNewStorageAuthorityGateRejectsMissingFailureHandler(t *testing.T) {
	gate, err := NewStorageAuthorityGate(nil)
	require.Error(t, err)
	assert.Nil(t, gate)

	var zero StorageAuthorityGate
	assert.False(t, zero.Valid())
	assert.ErrorIs(t, zero.Error(), errInvalidStorageAuthorityGate)
}
