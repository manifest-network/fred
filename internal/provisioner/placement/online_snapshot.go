package placement

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/provisioner/payload"
)

// liveStoreInitialMmapSize is the live placement database's initial bbolt
// mapping. An online snapshot holds a read transaction while it copies, and a
// writer that must remap waits for every open read; a large initial mapping
// (address space, not memory) makes that rare.
const liveStoreInitialMmapSize = 1 << 30

// ErrSnapshotDeadline means an online snapshot did not finish copying within
// the deadline fixed when its cut was captured. Its read transactions were
// ended so live writers could proceed.
var ErrSnapshotDeadline = errors.New("online snapshot copy exceeded its deadline")

// ConsistentCut is a read-only view of placements.db and payloads.db at one
// instant. Both read transactions begin while the placement write gate
// excludes writers, so no placement write lands between them: the pair is a
// state a crash at that instant could leave, which recovery already handles.
//
// Only CaptureConsistentCut mints one. It is used once, by Stream or Discard,
// and its transactions end no later than the deadline fixed at capture: a cut
// that is neither streamed nor discarded by then expires on its own. The zero
// value is invalid.
type ConsistentCut struct {
	state *cutState
}

type cutState struct {
	placement      *bolt.Tx
	payload        payload.SnapshotRead
	providerUUID   string
	placementsSize int64
	payloadsSize   int64
	expires        time.Time
	consumed       atomic.Bool
	expiry         *time.Timer
	store          *Store
	payloads       *payload.Store
}

// CaptureConsistentCut begins read transactions on both stores while holding
// the placement write gate, and fixes the deadline by which both transactions
// end. The gated section is exactly the two Begin calls; authority is checked
// before them and again after the copy, never while a transaction is open.
func (s *Store) CaptureConsistentCut(
	payloads *payload.Store,
	deadline time.Duration,
) (ConsistentCut, error) {
	if s == nil || s.db == nil {
		return ConsistentCut{}, ErrRuntimeAuthorityUnavailable
	}
	if payloads == nil {
		return ConsistentCut{}, errors.New("payload store is required for a consistent snapshot")
	}
	if deadline <= 0 {
		return ConsistentCut{}, errors.New("online snapshot deadline must be positive")
	}
	if err := s.reattestRuntimeAuthority(); err != nil {
		return ConsistentCut{}, err
	}
	if err := payloads.CheckSnapshotAuthority(); err != nil {
		return ConsistentCut{}, err
	}
	s.runtimeAuthorityMu.RLock()
	gate := s.runtimeAuthorityGate
	s.runtimeAuthorityMu.RUnlock()
	if gate == nil || !gate.Valid() {
		return ConsistentCut{}, ErrRuntimeAuthorityUnavailable
	}
	var (
		placementTx *bolt.Tx
		payloadRead payload.SnapshotRead
	)
	if err := gate.Hold(func() error {
		tx, err := s.db.Begin(false)
		if err != nil {
			return fmt.Errorf("begin placement snapshot read: %w", err)
		}
		handedOff := false
		defer func() {
			if !handedOff {
				_ = tx.Rollback()
			}
		}()
		read, err := payloads.BeginSnapshotRead()
		if err != nil {
			return err
		}
		placementTx, payloadRead = tx, read
		handedOff = true
		return nil
	}); err != nil {
		return ConsistentCut{}, err
	}
	expires := time.Now().Add(deadline)
	handedOff := false
	defer func() {
		if !handedOff {
			_ = placementTx.Rollback()
			_ = payloadRead.Rollback()
		}
	}()

	// Read the binding before arming the expiry: until then this goroutine is
	// the transactions' only user.
	metadata, err := loadTopologyMetadata(placementTx)
	if err != nil {
		return ConsistentCut{}, fmt.Errorf("read snapshot provider binding: %w", err)
	}
	if metadata.ProviderUUID != s.providerUUID {
		return ConsistentCut{}, fmt.Errorf(
			"%w: snapshot carries provider %q, store is bound to %q",
			ErrProviderAuthorityMismatch, metadata.ProviderUUID, s.providerUUID)
	}
	state := &cutState{
		placement:      placementTx,
		payload:        payloadRead,
		providerUUID:   metadata.ProviderUUID,
		placementsSize: placementTx.Size(),
		payloadsSize:   payloadRead.Size(),
		expires:        expires,
		store:          s,
		payloads:       payloads,
	}
	state.expiry = time.AfterFunc(time.Until(expires), func() { _ = state.discard() })
	handedOff = true
	return ConsistentCut{state: state}, nil
}

// Size is the number of bytes Stream will copy from both databases.
func (cut ConsistentCut) Size() int64 {
	if cut.state == nil {
		return 0
	}
	return cut.state.placementsSize + cut.state.payloadsSize
}

// CutCopy is one database's copy: its byte size and SHA-256.
type CutCopy struct {
	Size   int64
	SHA256 [sha256.Size]byte
}

// CutReceipt describes one streamed cut. Only Stream mints it, and only after
// every byte of both databases reached its destination, so a snapshot manifest
// can describe only bytes that were actually copied. The zero value is
// invalid.
type CutReceipt struct {
	providerUUID string
	placements   CutCopy
	payloads     CutCopy
}

// Valid reports whether receipt was minted by Stream.
func (receipt CutReceipt) Valid() bool { return receipt.providerUUID != "" }

// ProviderUUID is the provider recorded inside the copied placement database.
func (receipt CutReceipt) ProviderUUID() string { return receipt.providerUUID }

// Placements describes the placements.db copy.
func (receipt CutReceipt) Placements() CutCopy { return receipt.placements }

// Payloads describes the payloads.db copy.
func (receipt CutReceipt) Payloads() CutCopy { return receipt.payloads }

// Stream copies both databases concurrently into placementsDst and payloadsDst,
// ending each read transaction as soon as its copy ends. At the deadline fixed
// at capture, both transactions end even if a destination write is blocked,
// so live writers never wait on a snapshot past that deadline. Authority is
// checked again after both transactions end.
func (cut ConsistentCut) Stream(
	ctx context.Context,
	placementsDst, payloadsDst io.Writer,
) (CutReceipt, error) {
	state := cut.state
	if state == nil {
		return CutReceipt{}, errors.New("consistent snapshot cut is invalid")
	}
	if placementsDst == nil || payloadsDst == nil {
		return CutReceipt{}, errors.Join(
			errors.New("snapshot destinations are required"), cut.Discard())
	}
	if !state.consumed.CompareAndSwap(false, true) {
		return CutReceipt{}, errors.New("consistent snapshot cut was already used or expired")
	}
	state.expiry.Stop()
	ctx, cancel := context.WithDeadlineCause(ctx, state.expires, ErrSnapshotDeadline)
	defer cancel()
	placementCopy := copyOwnedRead(ctx, state.placement.WriteTo, state.placement.Rollback, placementsDst)
	payloadCopy := copyOwnedRead(ctx, state.payload.WriteTo, state.payload.Rollback, payloadsDst)
	placements, payloads := placementCopy.wait(ctx), payloadCopy.wait(ctx)
	if err := errors.Join(placements.err, payloads.err); err != nil {
		return CutReceipt{}, fmt.Errorf("copy online snapshot: %w", err)
	}
	// A copy that ends at shutdown is never published, and the stores may
	// already be closing.
	if err := context.Cause(ctx); err != nil {
		return CutReceipt{}, err
	}
	if placements.size != state.placementsSize || payloads.size != state.payloadsSize {
		return CutReceipt{}, fmt.Errorf(
			"copy online snapshot: copied %d and %d bytes, expected %d and %d",
			placements.size, payloads.size, state.placementsSize, state.payloadsSize)
	}
	if err := state.store.reattestRuntimeAuthority(); err != nil {
		return CutReceipt{}, err
	}
	if err := state.payloads.CheckSnapshotAuthority(); err != nil {
		return CutReceipt{}, err
	}
	return CutReceipt{
		providerUUID: state.providerUUID,
		placements:   CutCopy{Size: placements.size, SHA256: placements.sum},
		payloads:     CutCopy{Size: payloads.size, SHA256: payloads.sum},
	}, nil
}

// Discard ends an unused cut's read transactions. It is a no-op once the cut
// was streamed, discarded, or expired.
func (cut ConsistentCut) Discard() error {
	if cut.state == nil {
		return nil
	}
	cut.state.expiry.Stop()
	return cut.state.discard()
}

// discard ends both transactions if nothing else used the cut first. Neither
// transaction is in use when the swap succeeds, so ending them from this
// goroutine is safe.
func (state *cutState) discard() error {
	if !state.consumed.CompareAndSwap(false, true) {
		return nil
	}
	return errors.Join(state.placement.Rollback(), state.payload.Rollback())
}

type copyResult struct {
	size int64
	sum  [sha256.Size]byte
	err  error
}

// ownedCopy tracks one read transaction's copy: owner reports once the
// transaction has ended, reader once the destination has the bytes.
type ownedCopy struct {
	owner  chan error
	reader chan copyResult
}

// copyOwnedRead copies one read transaction through a pipe. The transaction's
// own goroutine runs WriteTo and then Rollback, so the transaction is never
// rolled back from another goroutine mid-copy. Closing the pipe's read side
// when ctx ends makes WriteTo's next write fail, which bounds the
// transaction's life even when a destination write is blocked.
func copyOwnedRead(
	ctx context.Context,
	writeTo func(io.Writer) (int64, error),
	rollback func() error,
	dst io.Writer,
) ownedCopy {
	pipeReader, pipeWriter := io.Pipe()
	state := ownedCopy{owner: make(chan error, 1), reader: make(chan copyResult, 1)}
	go func() {
		var writeErr error
		defer func() {
			if recovered := recover(); recovered != nil {
				writeErr = fmt.Errorf("online snapshot copy panicked: %v", recovered)
			}
			_ = pipeWriter.CloseWithError(writeErr)
			state.owner <- errors.Join(writeErr, rollback())
		}()
		_, writeErr = writeTo(pipeWriter)
	}()
	stop := context.AfterFunc(ctx, func() { _ = pipeReader.CloseWithError(context.Cause(ctx)) })
	go func() {
		hash := sha256.New()
		size, err := io.Copy(io.MultiWriter(dst, hash), pipeReader)
		stop()
		_ = pipeReader.CloseWithError(err)
		var sum [sha256.Size]byte
		copy(sum[:], hash.Sum(nil))
		state.reader <- copyResult{size: size, sum: sum, err: err}
	}()
	return state
}

// wait returns once the transaction has ended and the copy has ended, or ctx
// is done. A destination write still blocked after ctx ends is abandoned: the
// transaction it read from has already ended. A copy that failed after ctx
// ended carries ctx's cause, whichever side reported first: bbolt flattens the
// pipe's error to text, so the cause would otherwise be lost.
func (state ownedCopy) wait(ctx context.Context) copyResult {
	ownerErr := <-state.owner
	select {
	case result := <-state.reader:
		result.err = errors.Join(result.err, ownerErr)
		if result.err != nil && ctx.Err() != nil {
			result.err = errors.Join(context.Cause(ctx), result.err)
		}
		return result
	case <-ctx.Done():
		return copyResult{err: errors.Join(context.Cause(ctx), ownerErr)}
	}
}
