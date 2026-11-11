package docker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// A Ready lease whose committed release has no surviving containers is a cohort
// divergence. recoverState publishes it, releases its snapshot, and only later
// routes the divergence to the lease actor. A close admitted in between owns
// the actor while the projection still reads Ready, so routing delivers the
// report and it queues behind the close. The actor answers it superseded,
// whether the close then succeeds or fails, and the pass skips it and finishes
// instead of returning an error (ENG-2155).
//
// Recovery is held at its first inventory read made after the snapshot lock is
// released (the closed-substrate sweep). The close is held at
// acquireCloseIntent's recoverySnapshotMu.RLock: after its actor took it, and
// before doClosePhysical moves the projection out of Ready.
func TestRecoverState_CloseAdmittedBeforeCohortDivergenceDoesNotAbortThePass(t *testing.T) {
	for _, closeSucceeds := range []bool{true, false} {
		t.Run(fmt.Sprintf("close_succeeds=%t", closeSucceeds), func(t *testing.T) {
			type recoveryCtxKey struct{}
			var b *Backend
			recoveryPaused, resumeRecovery := make(chan struct{}), make(chan struct{})
			var pauseOnce sync.Once
			mock := &mockDockerClient{
				ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
					if ctx.Value(recoveryCtxKey{}) != nil && b.recoverySnapshotMu.TryRLock() {
						// No writer holds the snapshot: a recovery read after publication.
						b.recoverySnapshotMu.RUnlock()
						pauseOnce.Do(func() {
							close(recoveryPaused)
							<-resumeRecovery
						})
					}
					return nil, nil
				},
			}
			volumes := newVolumeSet(canonicalVolumeName(closeDeprovisionLeaseUUID, "app", 0))
			if !closeSucceeds {
				volumes.destroyFn = func(string) error { return errors.New("injected volume failure") }
			}
			var stores closeRecoveryStores
			b, stores = openCloseRecoveryBackend(t, t.TempDir(), mock, volumes.manager())
			seedCloseDeprovisionLease(t, b, stores)
			b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error { return nil }}
			logs := &lockedLog{}
			b.logger = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

			recoverDone := make(chan error, 1)
			go func() {
				recoverDone <- b.recoverState(context.WithValue(context.Background(), recoveryCtxKey{}, true))
			}()
			select {
			case <-recoveryPaused:
			case err := <-recoverDone:
				t.Fatalf("recovery finished before its closed-substrate sweep: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("recovery did not reach its closed-substrate sweep")
			}
			b.provisionsMu.RLock()
			require.Equal(t, backend.ProvisionStatusReady, b.provisions[closeDeprovisionLeaseUUID].Status,
				"recovery publishes the diverged lease as Ready and transitions it later through its actor")
			b.provisionsMu.RUnlock()

			b.recoverySnapshotMu.Lock() // hold the close in admission
			closeDone := make(chan error, 1)
			go func() { closeDone <- b.Deprovision(context.Background(), closeDeprovisionLeaseUUID) }()
			var actor *leasesm.LeaseActor
			require.Eventually(t, func() bool {
				b.actorsMu.Lock()
				actor = b.actors[closeDeprovisionLeaseUUID]
				b.actorsMu.Unlock()
				return actor != nil && actor.CurrentMessageStart() != 0
			}, 5*time.Second, time.Millisecond, "the close must be inside its actor's handler")

			close(resumeRecovery)
			require.Eventually(t, func() bool { return actor.InboxDepth() == 1 }, 5*time.Second, time.Millisecond,
				"the cohort divergence queues behind the close")
			b.recoverySnapshotMu.Unlock()

			select {
			case err := <-recoverDone:
				require.NoError(t, err, "a close admitted after publication must not abort the recovery pass")
			case <-time.After(10 * time.Second):
				t.Fatal("recovery did not finish")
			}
			closeErr := <-closeDone
			if closeSucceeds {
				require.NoError(t, closeErr)
			} else {
				require.Error(t, closeErr)
			}
			require.Contains(t, logs.String(), "cohort divergence superseded before its actor applied it")
			require.Contains(t, logs.String(), "state recovered", "the pass ran to its end")
			closeCloseRecoveryBackend(t, b, stores)
		})
	}
}
