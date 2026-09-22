package shared

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestCallbackHealthContextCanceledBeforeStoreAccess(t *testing.T) {
	store := newBoundCallbackHealthStore(t, 1)
	require.NoError(t, store.Close())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, store.HealthyContext(ctx), context.Canceled,
		"an expired probe must not enter a synchronous store check")
}

// A real cancellation is triggered after the owner reaches a known number of
// cancellation checkpoints. This avoids timing-dependent sleeps or a hook in
// the production traversal while checking that it returns before all rows.
type cancelDuringHealthContext struct {
	context.Context
	cancel context.CancelFunc
	checks atomic.Int64
	after  int64
}

func (ctx *cancelDuringHealthContext) Err() error {
	if ctx.checks.Add(1) == ctx.after {
		ctx.cancel()
	}
	return ctx.Context.Err()
}

func TestCallbackHealthContextStopsCurrentTraversal(t *testing.T) {
	store := newBoundCallbackHealthStore(t, 128)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	observed := &cancelDuringHealthContext{Context: ctx, cancel: cancel, after: 32}
	require.ErrorIs(t, store.HealthyContext(observed), context.Canceled)
	require.EqualValues(t, 32, observed.checks.Load(), "no row work continues after cancellation")
	// A canceled read neither withdraws storage identity nor stores a cached
	// failure/success. The next admitted read sees current committed contents.
	require.NoError(t, store.HealthyContext(t.Context()))
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackLeaseMutationHeadBucketName).Put(
			[]byte("00000000-0000-4000-8000-000000000001"), []byte("corrupt"))
	}))
	require.Error(t, store.HealthyContext(t.Context()), "prior successful health cannot conceal new corruption")
}

func TestCallbackHealthContextChecksEveryRowFamily(t *testing.T) {
	for name, validate := range map[string]func(context.Context, *CallbackStore, *bolt.Tx) error{
		"uuid slots": func(ctx context.Context, _ *CallbackStore, tx *bolt.Tx) error {
			return validateLeaseMutationUUIDSlotsContextTx(ctx, tx)
		},
		"heads and histories": func(ctx context.Context, _ *CallbackStore, tx *bolt.Tx) error {
			return validateCallbackReceiptStateContextTx(ctx, tx)
		},
		"queue": func(ctx context.Context, _ *CallbackStore, tx *bolt.Tx) error {
			return validateCallbackQueueContextTx(ctx, tx)
		},
		"operation history": func(ctx context.Context, _ *CallbackStore, tx *bolt.Tx) error {
			_, err := listOperationHistoryContextTx(ctx, tx, "00000000-0000-4000-8000-000000000001")
			return err
		},
		"maintenance history": func(ctx context.Context, _ *CallbackStore, tx *bolt.Tx) error {
			_, err := listMaintenanceReceiptsContextTx(ctx, tx, "00000000-0000-4000-8000-000000000001")
			return err
		},
		"compensation": func(ctx context.Context, _ *CallbackStore, tx *bolt.Tx) error {
			return validateMaintenanceCompensationsContextTx(ctx, tx)
		},
		"image inspection": func(ctx context.Context, _ *CallbackStore, tx *bolt.Tx) error {
			return visitImageInspectionsContextTx(ctx, tx, nil)
		},
		"volume launch": func(ctx context.Context, s *CallbackStore, tx *bolt.Tx) error {
			return (&VolumeLaunchJournal{store: s}).validateContextTx(ctx, tx)
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := newBoundCallbackHealthStore(t, 1)
			require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
				seedCallbackHealthOperationHistory(t, store, tx, "00000000-0000-4000-8000-000000000001", "docker-a")
				seedCallbackHealthMaintenanceHistory(t, store, tx, "00000000-0000-4000-8000-000000000001", 1)
				for _, name := range [][]byte{maintenanceCompensationBucket, imageInspectionsBucketName, volumeLaunchDebtBucketName} {
					_, err := tx.CreateBucketIfNotExists(name)
					require.NoError(t, err)
				}
				return nil
			}))
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			require.ErrorIs(t, store.view(func(tx *bolt.Tx) error {
				return validate(ctx, store, tx)
			}), context.Canceled)
		})
	}
}
