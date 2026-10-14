package placement

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/manifest-network/fred/internal/backend"
)

func recoveryRequestFields(
	metadata AttemptMetadata,
) (BackendRequestSnapshot, CallbackPair, error) {
	request := metadata.RequestSnapshot()
	callbacks := metadata.CallbackPair()
	if !request.Valid() || !callbacks.ValidFor(metadata.OperationID()) {
		return BackendRequestSnapshot{}, CallbackPair{}, errors.New("durable recovery request is invalid")
	}
	return request, callbacks, nil
}

func (recovery *AttemptRecoveryCoordinator) redeliverProvision(
	ctx context.Context,
	leaseUUID, backendName string,
	metadata AttemptMetadata,
	payloadExpected bool,
) backend.ProvisionCallOutcome {
	client, err := exactBackend(recovery.backends, backendName)
	if err != nil {
		return backend.ConservativeProvisionCallOutcome(err)
	}
	requestSnapshot, callbacks, err := recoveryRequestFields(metadata)
	if err != nil {
		return backend.ConservativeProvisionCallOutcome(err)
	}
	request := backend.ProvisionRequest{
		LeaseUUID: leaseUUID,
		Tenant:    requestSnapshot.Tenant(), ProviderUUID: requestSnapshot.ProviderUUID(),
		Items:                requestSnapshot.Items(),
		CallbackURL:          callbacks.OperationURL(),
		LifecycleCallbackURL: callbacks.LifecycleURL(),
	}
	fingerprint := metadata.PayloadFingerprint()
	if !fingerprint.Valid() {
		if payloadExpected {
			return backend.ConservativeProvisionCallOutcome(
				errors.New("payload-bearing provision recovery has no durable fingerprint"),
			)
		}
		return recovery.redeliverProvisionRequest(ctx, client, request)
	}
	if recovery.payloads == nil {
		return backend.ConservativeProvisionCallOutcome(
			errors.New("provision recovery payload reader is unavailable"),
		)
	}
	payloadBytes, recordedHash, err := recovery.payloads.GetWithHash(leaseUUID)
	if err != nil {
		return backend.ConservativeProvisionCallOutcome(
			fmt.Errorf("read exact provision recovery payload: %w", err),
		)
	}
	if payloadBytes == nil {
		return backend.ConservativeProvisionCallOutcome(
			errors.New("exact provision recovery payload is unavailable"),
		)
	}
	wantHash := fingerprint.Bytes()
	if len(recordedHash) != 0 && !bytes.Equal(recordedHash, wantHash) {
		return backend.ConservativeProvisionCallOutcome(
			errors.New("payload-store hash differs from durable attempt"),
		)
	}
	digest := sha256.Sum256(payloadBytes)
	if !bytes.Equal(digest[:], wantHash) {
		return backend.ConservativeProvisionCallOutcome(
			errors.New("payload bytes differ from durable attempt"),
		)
	}
	request.Payload = append([]byte(nil), payloadBytes...)
	request.PayloadHash = fingerprint.String()
	return recovery.redeliverProvisionRequest(ctx, client, request)
}

// redeliverProvisionRequest sends a provision rebuilt from a durable attempt
// only if the current billing policy still permits it. A refused request is
// never sent, and the conservative outcome leaves the attempt unresolved: the
// earlier delivery may have reached the backend, so its evidence is kept for
// inventory to settle.
func (recovery *AttemptRecoveryCoordinator) redeliverProvisionRequest(
	ctx context.Context, client backend.Backend, request backend.ProvisionRequest,
) backend.ProvisionCallOutcome {
	delivery, err := recovery.coordinator.attestProvision(request)
	if err != nil {
		return backend.ConservativeProvisionCallOutcome(err)
	}
	return invokeProvision(ctx, client, delivery)
}

func (recovery *AttemptRecoveryCoordinator) redeliverRestore(
	ctx context.Context,
	leaseUUID, backendName string,
	metadata AttemptMetadata,
) backend.RestoreCallOutcome {
	client, err := exactBackend(recovery.backends, backendName)
	if err != nil {
		return backend.ConservativeRestoreCallOutcome(err)
	}
	requestSnapshot, callbacks, err := recoveryRequestFields(metadata)
	if err != nil {
		return backend.ConservativeRestoreCallOutcome(err)
	}
	// A refused restore is never sent; the conservative outcome keeps the
	// attempt unresolved, as for a refused provision redelivery.
	delivery, err := recovery.coordinator.attestRestore(backend.RestoreRequest{
		LeaseUUID: leaseUUID, FromLeaseUUID: metadata.RestoreSourceLeaseUUID(),
		Tenant: requestSnapshot.Tenant(), ProviderUUID: requestSnapshot.ProviderUUID(),
		Items:                requestSnapshot.Items(),
		CallbackURL:          callbacks.OperationURL(),
		LifecycleCallbackURL: callbacks.LifecycleURL(),
	})
	if err != nil {
		return backend.ConservativeRestoreCallOutcome(err)
	}
	return invokeRestore(ctx, client, delivery)
}

func (recovery *AttemptRecoveryCoordinator) teardownTerminal(
	ctx context.Context,
	leaseUUID string,
	backendNames []string,
) error {
	// Resolve the complete exact candidate set before the first side effect. A
	// removed name can therefore never turn terminal convergence into a partial
	// teardown that is accidentally reported as success.
	clients := make([]backend.Backend, 0, len(backendNames))
	for _, backendName := range backendNames {
		client, err := exactBackend(recovery.backends, backendName)
		if err != nil {
			return err
		}
		clients = append(clients, client)
	}
	var errs []error
	for index, client := range clients {
		if err := invokeDeprovision(ctx, client, leaseUUID); err != nil {
			errs = append(errs, fmt.Errorf("backend %s: %w", backendNames[index], err))
		}
	}
	return errors.Join(errs...)
}
