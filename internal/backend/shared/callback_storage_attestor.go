package shared

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/util"
)

const callbackStorageAttestationTimeout = 10 * time.Second

// CallbackStorageVerifier re-attests one backend storage substrate. Its
// identity and authority gate are part of the proof: reporting only an ID is
// insufficient because two backend lifetimes may legitimately carry copied
// metadata while owning different fail-stop gates.
type CallbackStorageVerifier interface {
	StorageIdentity() backendidentity.ID
	StorageAuthorityGate() *backendidentity.StorageAuthorityGate
	Verify(context.Context) error
}

// CallbackStorageAttestor is an opaque capability binding one runtime storage
// verifier to one exact, open callback journal instance. The zero value is
// invalid. The fixed timeout belongs to this capability so publication and
// transport cannot be configured with contradictory or unbounded probes.
type CallbackStorageAttestor struct {
	store     *CallbackStore
	base      *boltStore
	binding   *openedStoreIdentityBinding
	gate      *backendidentity.StorageAuthorityGate
	verifier  CallbackStorageVerifier
	storageID backendidentity.ID
	stopCtx   context.Context
	timeout   time.Duration
}

// NewCallbackStorageAttestor constructs an exact-store callback authority.
// The verifier must report the same immutable storage identity and the same
// backend-lifetime commit gate captured by the journal when it was opened.
func NewCallbackStorageAttestor(
	store *CallbackStore,
	verifier CallbackStorageVerifier,
	stopCtx context.Context,
) (*CallbackStorageAttestor, error) {
	if store == nil || store.boltStore == nil || store.binding == nil ||
		store.backendAuthorityGate == nil {
		return nil, errors.New("callback storage attestor: exact identity-bound durable store is required")
	}
	if store.ctx == nil || store.ctx.Err() != nil {
		return nil, errors.New("callback storage attestor: callback store is closed")
	}
	if util.IsNilInterface(verifier) {
		return nil, errors.New("callback storage attestor: storage verifier is required")
	}
	if stopCtx == nil {
		return nil, errors.New("callback storage attestor: stop context is required")
	}
	if stopCtx.Err() != nil {
		return nil, errors.New("callback storage attestor: stop context is canceled")
	}
	storageID := verifier.StorageIdentity()
	if !storageID.Valid() || storageID != store.binding.storageID {
		return nil, errors.New("callback storage attestor: verifier belongs to another storage identity")
	}
	gate := verifier.StorageAuthorityGate()
	if gate == nil || !gate.Valid() || gate != store.backendAuthorityGate {
		return nil, errors.New("callback storage attestor: verifier belongs to another backend authority gate")
	}
	return &CallbackStorageAttestor{
		store: store, base: store.boltStore, binding: store.binding, gate: gate,
		verifier: verifier, storageID: storageID, stopCtx: stopCtx,
		timeout: callbackStorageAttestationTimeout,
	}, nil
}

// MustNewCallbackStorageAttestor is the panic-on-programmer-error form for
// static backend composition and tests.
func MustNewCallbackStorageAttestor(
	store *CallbackStore,
	verifier CallbackStorageVerifier,
	stopCtx context.Context,
) *CallbackStorageAttestor {
	attestor, err := NewCallbackStorageAttestor(store, verifier, stopCtx)
	if err != nil {
		panic(err)
	}
	return attestor
}

func (a *CallbackStorageAttestor) validFor(store *CallbackStore) bool {
	return a != nil && store != nil && a.store == store && a.base != nil &&
		store.boltStore == a.base && store.binding == a.binding &&
		store.backendAuthorityGate == a.gate && store.ctx != nil && store.ctx.Err() == nil &&
		a.binding != nil && a.binding.storageID == a.storageID && a.storageID.Valid() &&
		a.gate != nil && a.gate.Valid() && a.verifier != nil &&
		a.verifier.StorageIdentity() == a.storageID &&
		a.verifier.StorageAuthorityGate() == a.gate && a.stopCtx != nil &&
		a.stopCtx.Err() == nil && a.timeout > 0
}

func (a *CallbackStorageAttestor) verify(ownerCtx context.Context) error {
	if !a.validFor(a.store) {
		return fmt.Errorf("%w: callback storage attestor is invalid or its store is closed",
			backendidentity.ErrIdentityDrift)
	}
	if ownerCtx != nil && ownerCtx.Err() != nil {
		return ownerCtx.Err()
	}
	if err := a.gate.Error(); err != nil {
		return err
	}
	verificationCtx, cancelVerification := context.WithTimeout(a.stopCtx, a.timeout)
	stopOwnerCancellation := func() bool { return false }
	if ownerCtx != nil {
		stopOwnerCancellation = context.AfterFunc(ownerCtx, cancelVerification)
	}
	err := a.verifier.Verify(verificationCtx)
	stopOwnerCancellation()
	cancelVerification()
	if err != nil {
		return err
	}
	if !a.validFor(a.store) {
		return fmt.Errorf("%w: callback storage attestor became invalid during verification",
			backendidentity.ErrIdentityDrift)
	}
	return a.gate.Error()
}
