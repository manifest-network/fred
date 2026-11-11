package docker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The periodic recovery pass snapshots every close intent, including a close a
// live request is still tearing down. The live close's committed head has
// given the command fence back, so recovery takes it, finds the intent
// pending, and queues its own close behind the live one. When the live close
// completes, its actor retires without handling recovery's close. Recovery
// must then admit the close again, find it settled, and report nothing: the
// close did not fail, and no second teardown runs (ENG-2154).
func TestRecoverState_CloseQueuedBehindALiveCloseIsNotAFailure(t *testing.T) {
	dir := t.TempDir()
	volumeState := newVolumeSet(canonicalVolumeName(closeRecoveryLeaseUUID, "app", 0))
	var destroys atomic.Int32
	destroyStarted, continueDestroy := make(chan struct{}), make(chan struct{})
	var destroyOnce sync.Once
	volumeState.destroyFn = func(string) error {
		destroys.Add(1)
		destroyOnce.Do(func() { close(destroyStarted) })
		<-continueDestroy
		return nil
	}
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil },
	}
	b, stores := openCloseRecoveryBackend(t, dir, mock, volumeState.manager())
	seedCloseRecoveryProjection(t, b, stores, "")
	require.NoError(t, b.pool.TryAllocate(closeRecoveryLeaseUUID+"-app-0", "docker-small", "tenant-a"))
	logs := &lockedLog{}
	b.logger = slog.New(slog.NewTextHandler(logs, nil))

	closeDone := make(chan error, 1)
	go func() { closeDone <- b.Deprovision(context.Background(), closeRecoveryLeaseUUID) }()
	select {
	case <-destroyStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("the live close did not reach teardown")
	}
	closing := b.actorFor(closeRecoveryLeaseUUID)

	recoverDone := make(chan error, 1)
	go func() { recoverDone <- b.recoverState(context.Background()) }()
	require.Eventually(t, func() bool { return closing.InboxDepth() == 1 }, 5*time.Second, time.Millisecond,
		"recovery's close must queue behind the live one")
	close(continueDestroy)
	require.NoError(t, <-closeDone)
	select {
	case err := <-recoverDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the recovery pass did not finish")
	}

	select {
	case <-closing.Done():
	default:
		t.Fatal("the completed live close must have retired its actor")
	}
	require.NotContains(t, logs.String(), "durable close recovery remains pending",
		"a close the retiring actor did not handle is not a failed close")
	require.Contains(t, logs.String(), "durable close recovery completed")
	require.EqualValues(t, 1, destroys.Load(), "recovery found the close settled and ran no second teardown")
	_, found, err := stores.callbacks.GetCloseIntent(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.False(t, found, "the close settled")
	closeCloseRecoveryBackend(t, b, stores)
}

// An actor can retire without settling the close behind which recovery's close
// waits: a cleanup-only close has no projection, so its actor retires even when
// the attempt fails. Recovery's close was never handled; admitted again, it
// finds the intent still pending and runs on the actor that replaces the
// retiring one, through either recovery entry point (ENG-2154).
func TestResumeRecoveredCloseReroutesACloseTheRetiringActorDidNotHandle(t *testing.T) {
	for _, entry := range []struct {
		name   string
		resume func(*Backend) error
	}{
		{"blocking", func(b *Backend) error {
			claim, found, err := b.closeSettlement.GetCloseIntent(closeRecoveryLeaseUUID)
			if err != nil || !found {
				return errors.Join(err, errors.New("no close intent to resume"))
			}
			return b.resumeRecoveredClose(context.Background(), claim)
		}},
		{"try", func(b *Backend) error {
			resumed, err := b.tryResumeRecoveredClose(context.Background(), closeRecoveryLeaseUUID)
			if err == nil && !resumed {
				return errors.New("the fence was free, so the close must have been resumed")
			}
			return err
		}},
	} {
		t.Run(entry.name, func(t *testing.T) {
			dir := t.TempDir()
			volumeState := newVolumeSet(canonicalVolumeName(closeRecoveryLeaseUUID, "app", 0))
			var destroys atomic.Int32
			destroyStarted, continueDestroy := make(chan struct{}), make(chan struct{})
			volumeState.destroyFn = func(string) error {
				if destroys.Add(1) > 1 {
					return nil
				}
				close(destroyStarted)
				<-continueDestroy
				return errors.New("injected volume failure")
			}
			mock := &mockDockerClient{
				ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil },
			}
			b, stores := openCloseRecoveryBackend(t, dir, mock, volumeState.manager())
			seedCloseRecoveryRelease(t, stores)

			closeDone := make(chan error, 1)
			go func() { closeDone <- b.Deprovision(context.Background(), closeRecoveryLeaseUUID) }()
			select {
			case <-destroyStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("the live close did not reach teardown")
			}
			claim, found, err := stores.callbacks.GetCloseIntent(closeRecoveryLeaseUUID)
			require.NoError(t, err)
			require.True(t, found)
			require.True(t, claim.CleanupOnly())
			closing := b.actorFor(closeRecoveryLeaseUUID)

			resumeDone := make(chan error, 1)
			go func() { resumeDone <- entry.resume(b) }()
			require.Eventually(t, func() bool { return closing.InboxDepth() == 1 }, 5*time.Second, time.Millisecond,
				"recovery's close must queue behind the live one")
			close(continueDestroy)
			require.Error(t, <-closeDone, "the live attempt failed")
			select {
			case err := <-resumeDone:
				require.NoError(t, err, "the close the retiring actor did not handle is routed again, not reported")
			case <-time.After(5 * time.Second):
				t.Fatal("the recovered close was not answered")
			}

			select {
			case <-closing.Done():
			default:
				t.Fatal("the failed cleanup-only close must have retired its actor")
			}
			require.EqualValues(t, 2, destroys.Load(), "the recovered close ran once, on the replacement actor")
			_, found, err = stores.callbacks.GetCloseIntent(closeRecoveryLeaseUUID)
			require.NoError(t, err)
			require.False(t, found, "the recovered close settled")
			closeCloseRecoveryBackend(t, b, stores)
		})
	}
}
