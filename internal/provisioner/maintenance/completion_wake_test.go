package maintenance

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func TestSignedCompletionReleasesLaneBeforeRecoveryTick(t *testing.T) {
	for _, status := range []backend.CallbackStatus{backend.CallbackStatusSuccess, backend.CallbackStatusFailed} {
		t.Run(string(status), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store, _ := newPlacementAuthority(t, testLeaseA)
				payloads := &fakePayloads{}
				service, runtime := newTestServiceWithRuntime(t, store, &fakeBackend{}, payloads, testLeaseA)
				command := Command{ID: requestID(t, testRequestA), LeaseUUID: testLeaseA, Tenant: testTenant, Kind: KindUpdate, Payload: []byte("exact update")}
				require.Equal(t, OutcomeAccepted, service.Execute(t.Context(), command).Outcome())
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- service.Start(ctx, time.Hour) }()
				synctest.Wait()
				assertMaintenanceLaneHeld(t, runtime, testLeaseA)
				started := time.Now()
				completeUpdateForTest(t, store, command.ID, status)
				synctest.Wait()
				assertMaintenanceLaneReleased(t, runtime, testLeaseA)
				require.Equal(t, started, time.Now(), "completion must not wait for a periodic tick")
				if status == backend.CallbackStatusSuccess {
					require.Equal(t, []byte("exact update"), payloads.lastWrite())
				} else {
					require.Zero(t, payloads.writeCount())
				}
				cancel()
				require.ErrorIs(t, <-done, context.Canceled)
			})
		})
	}
}

func TestFailedConfirmedPayloadDoesNotSpinCompletionWake(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, _ := newPlacementAuthority(t, testLeaseA)
		payloads := &fakePayloads{failures: 100}
		service, runtime := newTestServiceWithRuntime(t, store, &fakeBackend{}, payloads, testLeaseA)
		command := Command{ID: requestID(t, testRequestA), LeaseUUID: testLeaseA, Tenant: testTenant, Kind: KindUpdate, Payload: []byte("exact update")}
		require.Equal(t, OutcomeAccepted, service.Execute(t.Context(), command).Outcome())
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- service.Start(ctx, time.Hour) }()
		synctest.Wait()
		completeUpdateForTest(t, store, command.ID, backend.CallbackStatusSuccess)
		synctest.Wait()
		payloads.mu.Lock()
		attempts := 100 - payloads.failures
		payloads.mu.Unlock()
		require.Equal(t, 1, attempts, "durable completion schedules one attempt; persistence failures retain periodic retry")
		assertMaintenanceLaneHeld(t, runtime, testLeaseA)
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
	})
}
