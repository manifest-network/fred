package leasesm

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// A cohort divergence that reaches the actor while a close is admitted is
// routed, because the projection still reads Ready, and queues behind the
// close. It is superseded whatever the close's outcome: after a failed close
// the actor refuses it because the close owns the lease; after a successful
// close the retiring actor rejects it unhandled. Neither is a failed
// divergence, and neither fails the lease or sends a lifecycle callback
// (ENG-2155).
func TestCohortDivergenceQueuedBehindACloseIsSuperseded(t *testing.T) {
	for _, closeSucceeds := range []bool{true, false} {
		t.Run(fmt.Sprintf("close_succeeds=%t", closeSucceeds), func(t *testing.T) {
			leaseUUID := testActorLeaseUUID
			runtime := newTestRuntimeGenerationProof(t, leaseUUID)
			store := newMockProvisionStore()
			store.put(leaseUUID, &ProvisionState{
				LeaseUUID:            leaseUUID,
				Status:               backend.ProvisionStatusReady,
				FailCount:            1,
				ActiveReleaseVersion: runtime.Version(),
				ActiveOperationID:    runtime.OperationID(),
			})
			inClose, finishClose := make(chan struct{}), make(chan struct{})
			var lifecycleFailures atomic.Int64
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			actor := newTestActor(t, leaseUUID, testActorOpts{
				StopCtx:        ctx,
				ProvisionStore: store,
				DoDeprovisionFn: func(context.Context, ActorCloseScope) error {
					close(inClose)
					<-finishClose
					if closeSucceeds {
						store.remove(leaseUUID)
						return nil
					}
					return errors.New("injected close failure")
				},
				SendLifecycleFailureFn: func(shared.RuntimeGenerationProof, string) { lifecycleFailures.Add(1) },
			})

			closeReply := make(chan CloseAnswer, 1)
			requireCloseAccepted(t, actor, closeReply)
			select {
			case <-inClose:
			case <-time.After(5 * time.Second):
				t.Fatal("the close did not start")
			}
			observation, reply, err := NewCohortDivergedObservation(context.Background(), runtime)
			require.NoError(t, err)
			require.True(t, observation.Current(store), "the projection still reads Ready while the close runs")
			requireAccepted(t, func() bool { return actor.TryEnqueueObservation(observation) })
			close(finishClose)

			closeErr := handledCloseOutcome(t, <-closeReply)
			if closeSucceeds {
				require.NoError(t, closeErr)
			} else {
				require.Error(t, closeErr)
			}
			select {
			case answer := <-reply.Answer():
				requireDivergenceSuperseded(t, answer, "a close took the lease before the report")
			case <-time.After(5 * time.Second):
				t.Fatal("the cohort divergence was not answered")
			}
			require.Zero(t, lifecycleFailures.Load(), "a superseded report sends no lifecycle callback")
			if got, exists := store.Get(leaseUUID); exists {
				require.NotEqual(t, backend.ProvisionStatusFailed, got.Status, "a superseded report does not fail the lease")
				require.Equal(t, 1, got.FailCount)
			}
		})
	}
}
