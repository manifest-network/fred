package placement

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

const (
	repairLease          = "018f47a2-8b1c-7def-8123-456789abcdef"
	repairOtherLease     = "018f47a2-8b1c-7def-8123-456789abcdee"
	repairOperation      = "550e8400-e29b-41d4-a716-446655440000"
	repairOwnerOperation = "6ba7b810-9dad-41d1-80b4-00c04fd430c8"
	repairNewOperation   = "d9428888-122b-41e1-b85c-61c67afba0c6"
)

func newProviderBoundRepairStore(t *testing.T, dbPath string) *Store {
	t.Helper()
	require.NoError(t, InitializeFreshStoreContext(
		t.Context(), freshTestPlan(t, dbPath, []string{"backend-a", "backend-b"}),
	))
	store, err := OpenStore(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	return store
}

func repairBackendRequestSnapshot(t *testing.T) BackendRequestSnapshot {
	t.Helper()
	snapshot, err := NewBackendRequestSnapshot(
		"tenant-test", freshTestProviderUUID,
		[]backend.LeaseItem{{SKU: "sku-test", Quantity: 1, ServiceName: "app"}},
	)
	require.NoError(t, err)
	return snapshot
}

func requireRepairTypedAttempt(
	t *testing.T,
	store *Store,
	leaseUUID, backendName string,
	operationID operation.OperationID,
) AttemptToken {
	t.Helper()
	if !store.CurrentAdmissionBaseline().Valid() {
		requireTestAdmission(t, store)
	}
	baseline := store.CurrentAdmissionBaseline()
	current := store.Lookup(leaseUUID)
	var (
		token   AttemptToken
		applied bool
		err     error
	)
	switch current.State() {
	case StateAbsent:
		scope, scopeErr := store.ScopeAdmission(baseline, []string{backendName})
		require.NoError(t, scopeErr)
		token, applied, err = store.BeginNewAttempt(
			scope, leaseUUID, backendName, operationID, PayloadFingerprint{},
			repairBackendRequestSnapshot(t), testCallbackPair(operationID),
		)
	case StateConfirmed:
		token, applied, err = store.BeginOwnedAttempt(
			baseline, current.RecordRevision(), backendName, operationID,
			PayloadFingerprint{}, repairBackendRequestSnapshot(t), testCallbackPair(operationID),
		)
	default:
		require.FailNow(t, "placement cannot begin a repair test attempt", "state=%s", current.State())
	}
	require.NoError(t, err)
	require.True(t, applied)
	require.True(t, token.Valid())
	return token
}

func testRepairInventorySnapshot(
	t *testing.T,
	repair *AttemptRepair,
	overrides map[string]RepairBackendInventory,
) RepairInventorySnapshot {
	t.Helper()
	inventories := make(map[string]RepairBackendInventory)
	for _, backendName := range repair.BackendTopology() {
		id, bound := repair.ExpectedBackendStorageIdentity(backendName)
		require.True(t, bound)
		require.True(t, id.Valid())
		inventory := RepairBackendInventory{
			StorageIdentity: id,
			Provisions:      []backend.ProvisionInfo{},
			Retentions:      []backend.RetainedLease{},
		}
		if override, exists := overrides[backendName]; exists {
			override.StorageIdentity = id
			if override.Provisions == nil {
				override.Provisions = []backend.ProvisionInfo{}
			}
			if override.Retentions == nil {
				override.Retentions = []backend.RetainedLease{}
			}
			inventory = override
		}
		inventories[backendName] = inventory
	}
	snapshot, err := NewRepairInventorySnapshot(repair.BackendTopology(), inventories)
	require.NoError(t, err)
	return snapshot
}

func requireAttemptRepairAuthorities(
	t *testing.T,
	repair *AttemptRepair,
	candidate AttemptRepairCandidate,
) (context.Context, AttemptRepairEvidence, DrainAttestation, AttemptRepairProbe) {
	t.Helper()
	ctx, evidence, drain, probe, _ := requireAttemptRepairAuthoritiesWithBackup(
		t, repair, candidate,
	)
	return ctx, evidence, drain, probe
}

func requireAttemptRepairAuthoritiesWithBackup(
	t *testing.T,
	repair *AttemptRepair,
	candidate AttemptRepairCandidate,
) (
	context.Context,
	AttemptRepairEvidence,
	DrainAttestation,
	AttemptRepairProbe,
	*ExactBackupTarget,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	snapshot := testRepairInventorySnapshot(t, repair, nil)
	evidence, err := repair.VerifyAttemptRepairEvidenceContext(
		ctx, candidate, snapshot,
	)
	require.NoError(t, err)
	drain, err := repair.AttestDrain(candidate.ConfirmationValue(), DrainAttestationText)
	require.NoError(t, err)
	backup := requireExactRepairBackup(t, repair)
	probe := func(context.Context) (RepairInventorySnapshot, error) { return snapshot, nil }
	return ctx, evidence, drain, probe, backup
}

func requireConflictRepairAuthorities(
	t *testing.T,
	repair *AttemptRepair,
	candidate ConflictRepairCandidate,
	retained bool,
	lifecycleObservation *backend.LifecycleGenerationObservation,
) (context.Context, ConflictRepairPlan, DrainAttestation, ConflictRepairProbe) {
	t.Helper()
	ctx, plan, drain, probe, _ := requireConflictRepairAuthoritiesWithBackup(
		t, repair, candidate, retained, lifecycleObservation,
	)
	return ctx, plan, drain, probe
}

func requireConflictRepairAuthoritiesWithBackup(
	t *testing.T,
	repair *AttemptRepair,
	candidate ConflictRepairCandidate,
	retained bool,
	lifecycleObservation *backend.LifecycleGenerationObservation,
) (
	context.Context,
	ConflictRepairPlan,
	DrainAttestation,
	ConflictRepairProbe,
	*ExactBackupTarget,
) {
	t.Helper()
	override := RepairBackendInventory{}
	if retained {
		override.Retentions = []backend.RetainedLease{{LeaseUUID: candidate.LeaseUUID()}}
	} else {
		override.Provisions = []backend.ProvisionInfo{{
			LeaseUUID:           candidate.LeaseUUID(),
			ProviderUUID:        freshTestProviderUUID,
			Tenant:              "tenant-a",
			LifecycleGeneration: lifecycleObservation,
		}}
	}
	snapshot := testRepairInventorySnapshot(t, repair, map[string]RepairBackendInventory{
		candidate.SelectedBackend(): override,
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	plan, err := repair.PlanConflictRepairContext(ctx, candidate, snapshot)
	require.NoError(t, err)
	drain, err := repair.AttestDrain(plan.ConfirmationValue(), DrainAttestationText)
	require.NoError(t, err)
	backup := requireExactRepairBackup(t, repair)
	probe := func(context.Context) (RepairInventorySnapshot, error) { return snapshot, nil }
	return ctx, plan, drain, probe, backup
}

func requireExactRepairBackup(t *testing.T, repair *AttemptRepair) *ExactBackupTarget {
	t.Helper()
	target := boundExactBackupTarget(
		t,
		filepath.Join(t.TempDir(), "placements.pre-repair.bak"),
	)
	require.NoError(t, repair.CreateExactBackup(target))
	return target
}

func createRepairFixture(
	t *testing.T,
	confirmed bool,
) (dbPath string, operationID operation.OperationID) {
	t.Helper()
	dbPath = filepath.Join(t.TempDir(), "placements.db")
	store := newProviderBoundRepairStore(t, dbPath)
	requireTestAdmission(t, store)
	operationID, err := operation.ParseID(repairOperation)
	require.NoError(t, err)
	if confirmed {
		ownerOperation, parseErr := operation.ParseID(repairOwnerOperation)
		require.NoError(t, parseErr)
		ownerAttempt := requireRepairTypedAttempt(t, store, repairLease, "backend-a", ownerOperation)
		applied, confirmErr := store.ConfirmAttempt(ownerAttempt)
		require.NoError(t, confirmErr)
		require.True(t, applied)
	}
	requireRepairTypedAttempt(t, store, repairLease, "backend-a", operationID)
	require.NoError(t, store.Close())
	return dbPath, operationID
}

func TestOpenAttemptRepairIsNonMutatingAndHoldsExclusiveLock(t *testing.T) {
	dbPath, operationID := createRepairFixture(t, false)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	_, err = repair.MatchAttempt(repairLease, "backend-a", operationID)
	require.NoError(t, err)

	writer, writerErr := bolt.Open(dbPath, 0o600, &bolt.Options{Timeout: 50 * time.Millisecond})
	if writer != nil {
		require.NoError(t, writer.Close())
	}
	require.ErrorIs(t, writerErr, bolt.ErrTimeout,
		"the repair session must exclude providerd and every other writer")
	require.NoError(t, repair.Close())

	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, after, "open, inspect, and close must not change database bytes")
}

func TestRepairOpenersRequireExactCanonicalProviderAuthority(t *testing.T) {
	dbPath, _ := createRepairFixture(t, false)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	otherProvider := "1e1698c3-a922-460a-8296-70efdbc03032"

	type closer interface{ Close() error }
	openers := []struct {
		name string
		open func(string, string) (closer, error)
	}{
		{
			name: "read-only inspector",
			open: func(path, providerUUID string) (closer, error) {
				return OpenRepairInspector(path, providerUUID)
			},
		},
		{
			name: "exclusive mutation session",
			open: func(path, providerUUID string) (closer, error) {
				return OpenAttemptRepair(path, providerUUID)
			},
		},
	}
	for _, opener := range openers {
		t.Run(opener.name, func(t *testing.T) {
			for _, providerUUID := range []string{
				"not-a-uuid",
				"550E8400-E29B-41D4-A716-446655440000",
				"00000000-0000-0000-0000-000000000000",
				otherProvider,
			} {
				handle, openErr := opener.open(dbPath, providerUUID)
				require.ErrorIs(t, openErr, ErrProviderAuthorityMismatch)
				assert.Nil(t, handle)

				writer, writerErr := bolt.Open(
					dbPath, 0o600, &bolt.Options{Timeout: 50 * time.Millisecond},
				)
				require.NoError(t, writerErr,
					"a rejected provider must not retain a placement database lock")
				require.NoError(t, writer.Close())
			}
		})
	}
	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, after,
		"provider validation and mismatch rejection must not change database bytes")
}

func TestRepairOpenersRejectEmptyDatabaseWithoutInitializingIt(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "empty.db")
	require.NoError(t, os.WriteFile(dbPath, nil, 0o600))

	inspector, err := OpenRepairInspector(dbPath, freshTestProviderUUID)
	require.ErrorIs(t, err, ErrAttemptRepairSchema)
	assert.Nil(t, inspector)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.ErrorIs(t, err, ErrAttemptRepairSchema)
	assert.Nil(t, repair)

	info, err := os.Stat(dbPath)
	require.NoError(t, err)
	assert.Zero(t, info.Size(), "repair open must not let bbolt initialize an empty file")
}

func TestRepairOpenersRejectInvalidProviderBeforeInspectingPath(t *testing.T) {
	missingPath := filepath.Join(t.TempDir(), "must-not-be-created.db")
	_, err := OpenRepairInspector(missingPath, "invalid")
	require.ErrorIs(t, err, ErrProviderAuthorityMismatch)
	_, err = OpenAttemptRepair(missingPath, "invalid")
	require.ErrorIs(t, err, ErrProviderAuthorityMismatch)
	_, statErr := os.Stat(missingPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestAttemptRepairMatchRequiresExactCurrentTuple(t *testing.T) {
	dbPath, operationID := createRepairFixture(t, false)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	otherOperation, err := operation.ParseID(repairNewOperation)
	require.NoError(t, err)

	for name, target := range map[string]struct {
		lease     string
		backend   string
		operation operation.OperationID
	}{
		"wrong lease":     {repairOtherLease, "backend-a", operationID},
		"wrong backend":   {repairLease, "backend-b", operationID},
		"stale operation": {repairLease, "backend-a", otherOperation},
	} {
		t.Run(name, func(t *testing.T) {
			candidate, err := repair.MatchAttempt(target.lease, target.backend, target.operation)
			require.ErrorIs(t, err, ErrAttemptRepairTarget)
			assert.Empty(t, candidate.ConfirmationValue())
			assert.Equal(t, operationID, repair.store.Lookup(repairLease).AttemptOperationID())
		})
	}
}

func TestAttemptRepairStaleCandidateCannotClearNewerSameBackendOperation(t *testing.T) {
	dbPath, oldOperation := createRepairFixture(t, false)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	stale, err := repair.MatchAttempt(repairLease, "backend-a", oldOperation)
	require.NoError(t, err)
	ctx, evidence, drain, probe := requireAttemptRepairAuthorities(t, repair, stale)

	settled, err := repair.store.RefuseOperation(repairLease, "backend-a", oldOperation)
	require.NoError(t, err)
	require.True(t, settled)
	newOperation, err := operation.ParseID(repairNewOperation)
	require.NoError(t, err)
	requireRepairTypedAttempt(t, repair.store, repairLease, "backend-a", newOperation)

	_, err = repair.RefuseContext(ctx, stale, evidence, drain, probe)
	require.ErrorIs(t, err, ErrAttemptRepairTarget)
	assert.Equal(t, newOperation, repair.store.Lookup(repairLease).AttemptOperationID(),
		"a stale candidate must not clear a newer same-backend attempt")
}

func TestAttemptRepairRefusePreservesConfirmedOwner(t *testing.T) {
	dbPath, operationID := createRepairFixture(t, true)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	candidate, err := repair.MatchAttempt(repairLease, "backend-a", operationID)
	require.NoError(t, err)
	assert.Equal(t, "backend-a", candidate.ConfirmedOwner())
	ctx, evidence, drain, probe := requireAttemptRepairAuthorities(t, repair, candidate)

	result, err := repair.RefuseContext(ctx, candidate, evidence, drain, probe)
	require.NoError(t, err)
	assert.Equal(t, "backend-a", result.ConfirmedOwner)
	require.NoError(t, repair.Sync())
	require.NoError(t, repair.Close())

	reopened, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	after := reopened.Lookup(repairLease)
	assert.Equal(t, StateConfirmed, after.State())
	assert.Equal(t, "backend-a", after.Backend)
	assert.Empty(t, after.Attempt)
	assert.False(t, after.AttemptOperationID().Valid())
}

func TestAttemptRepairRejectsInPlaceBackupMutationBeforeCommit(t *testing.T) {
	dbPath, operationID := createRepairFixture(t, false)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	candidate, err := repair.MatchAttempt(repairLease, "backend-a", operationID)
	require.NoError(t, err)
	ctx, evidence, drain, probe, backup := requireAttemptRepairAuthoritiesWithBackup(
		t, repair, candidate,
	)
	corruptExactBackupInPlace(t, backup)

	_, err = repair.RefuseContext(ctx, candidate, evidence, drain, probe)
	require.ErrorContains(t, err, "backup bytes changed")
	assert.Equal(t, StateAttempting, repair.store.Lookup(repairLease).State())
	after, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, after,
		"an in-place-corrupted rollback image must fail before attempt repair commits")
}

func TestAttemptRepairRefuseRequiresIndependentInventoryAndDrainCapabilities(t *testing.T) {
	dbPath, operationID := createRepairFixture(t, false)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	candidate, err := repair.MatchAttempt(repairLease, "backend-a", operationID)
	require.NoError(t, err)
	ctx, evidence, drain, probe := requireAttemptRepairAuthorities(t, repair, candidate)

	_, err = repair.RefuseContext(ctx, candidate, AttemptRepairEvidence{}, drain, probe)
	require.ErrorIs(t, err, ErrAttemptRepairTarget)
	_, err = repair.RefuseContext(ctx, candidate, evidence, DrainAttestation{}, probe)
	require.ErrorIs(t, err, ErrAttemptRepairTarget)
	assert.Equal(t, operationID, repair.store.Lookup(repairLease).AttemptOperationID())
}

func TestAttemptRepairEvidenceIsDeadlineAndCancellationScopeBound(t *testing.T) {
	dbPath, operationID := createRepairFixture(t, false)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	candidate, err := repair.MatchAttempt(repairLease, "backend-a", operationID)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	snapshot := testRepairInventorySnapshot(t, repair, nil)
	evidence, err := repair.VerifyAttemptRepairEvidenceContext(
		ctx, candidate, snapshot,
	)
	require.NoError(t, err)
	drain, err := repair.AttestDrain(candidate.ConfirmationValue(), DrainAttestationText)
	require.NoError(t, err)
	probe := func(context.Context) (RepairInventorySnapshot, error) { return snapshot, nil }

	foreignCtx, foreignCancel := context.WithTimeout(context.Background(), time.Minute)
	defer foreignCancel()
	_, err = repair.RefuseContext(foreignCtx, candidate, evidence, drain, probe)
	require.ErrorIs(t, err, ErrAttemptRepairTarget)
	require.ErrorContains(t, err, "another cancellation scope")
	assert.Equal(t, operationID, repair.store.Lookup(repairLease).AttemptOperationID())

	cancel()
	_, err = repair.RefuseContext(ctx, candidate, evidence, drain, probe)
	require.ErrorIs(t, err, ErrAttemptRepairTarget)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, operationID, repair.store.Lookup(repairLease).AttemptOperationID())

	liveCtx, liveCancel := context.WithTimeout(context.Background(), time.Minute)
	defer liveCancel()
	expiredEvidence, err := repair.VerifyAttemptRepairEvidenceContext(
		liveCtx, candidate, testRepairInventorySnapshot(t, repair, nil),
	)
	require.NoError(t, err)
	expiredEvidence.notAfter = time.Now().Add(-time.Nanosecond)
	_, err = repair.RefuseContext(liveCtx, candidate, expiredEvidence, drain, probe)
	require.ErrorIs(t, err, ErrAttemptRepairTarget)
	require.ErrorContains(t, err, "expired")
	assert.Equal(t, operationID, repair.store.Lookup(repairLease).AttemptOperationID())
}

func TestAttemptRepairFinalProbeClosesEvidenceToWriteWindow(t *testing.T) {
	dbPath, operationID := createRepairFixture(t, false)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	candidate, err := repair.MatchAttempt(repairLease, "backend-a", operationID)
	require.NoError(t, err)
	initial := testRepairInventorySnapshot(t, repair, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	evidence, err := repair.VerifyAttemptRepairEvidenceContext(ctx, candidate, initial)
	require.NoError(t, err)
	drain, err := repair.AttestDrain(candidate.ConfirmationValue(), DrainAttestationText)
	require.NoError(t, err)
	appeared := testRepairInventorySnapshot(t, repair, map[string]RepairBackendInventory{
		"backend-a": {Retentions: []backend.RetainedLease{{LeaseUUID: repairLease}}},
	})
	probeCalled := false
	_, err = repair.RefuseContext(ctx, candidate, evidence, drain, func(context.Context) (RepairInventorySnapshot, error) {
		probeCalled = true
		return appeared, nil
	})
	require.ErrorIs(t, err, ErrRepairLeasePresent)
	assert.True(t, probeCalled)
	assert.Equal(t, operationID, repair.store.Lookup(repairLease).AttemptOperationID())

	probeCtx, cancelDuringProbe := context.WithTimeout(context.Background(), time.Minute)
	evidence, err = repair.VerifyAttemptRepairEvidenceContext(probeCtx, candidate, initial)
	require.NoError(t, err)
	_, err = repair.RefuseContext(probeCtx, candidate, evidence, drain, func(context.Context) (RepairInventorySnapshot, error) {
		cancelDuringProbe()
		return initial, nil
	})
	require.ErrorIs(t, err, ErrAttemptRepairTarget)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, operationID, repair.store.Lookup(repairLease).AttemptOperationID())
}

func TestAttemptRepairRejectsMismatchedPreservedProvisionIdentity(t *testing.T) {
	dbPath, operationID := createRepairFixture(t, true)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	candidate, err := repair.MatchAttempt(repairLease, "backend-a", operationID)
	require.NoError(t, err)
	ownerOperation, err := operation.ParseID(repairOwnerOperation)
	require.NoError(t, err)
	ownerLifecycle, err := lifecycle.FromOperationID(ownerOperation)
	require.NoError(t, err)

	for name, provision := range map[string]backend.ProvisionInfo{
		"provider": {
			LeaseUUID: repairLease, ProviderUUID: repairOtherLease, Tenant: "tenant-test",
		},
		"tenant": {
			LeaseUUID: repairLease, ProviderUUID: freshTestProviderUUID, Tenant: "another-tenant",
		},
	} {
		t.Run(name, func(t *testing.T) {
			provision.LifecycleGeneration = &backend.LifecycleGenerationObservation{
				Kind: backend.LifecycleGenerationTyped,
				ID:   ownerLifecycle.String(),
			}
			snapshot := testRepairInventorySnapshot(t, repair, map[string]RepairBackendInventory{
				"backend-a": {Provisions: []backend.ProvisionInfo{provision}},
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			_, evidenceErr := repair.VerifyAttemptRepairEvidenceContext(ctx, candidate, snapshot)
			require.ErrorIs(t, evidenceErr, ErrRepairLeasePresent)
			require.ErrorContains(t, evidenceErr, name)
		})
	}
}

func TestRepairInventorySnapshotDeepCopiesNestedWireEvidence(t *testing.T) {
	dbPath, operationID := createRepairFixture(t, false)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	candidate, err := repair.MatchAttempt(repairLease, "backend-a", operationID)
	require.NoError(t, err)

	generation := &backend.LifecycleGenerationObservation{
		Kind: backend.LifecycleGenerationTyped,
		ID:   "550e8400-e29b-41d4-a716-446655440099",
	}
	provisions := []backend.ProvisionInfo{{
		LeaseUUID:           repairOtherLease,
		Items:               []backend.LeaseItem{{SKU: "sku-a", Quantity: 1}},
		ServiceImages:       map[string]string{"web": "image:v1"},
		LifecycleGeneration: generation,
	}}
	inventories := make(map[string]RepairBackendInventory)
	for _, backendName := range repair.BackendTopology() {
		id, ok := repair.ExpectedBackendStorageIdentity(backendName)
		require.True(t, ok)
		inventories[backendName] = RepairBackendInventory{
			StorageIdentity: id,
			Provisions:      []backend.ProvisionInfo{},
			Retentions:      []backend.RetainedLease{},
		}
	}
	inventory := inventories["backend-a"]
	inventory.Provisions = provisions
	inventories["backend-a"] = inventory
	snapshot, err := NewRepairInventorySnapshot(repair.BackendTopology(), inventories)
	require.NoError(t, err)

	// Mutating every caller-owned reference after construction must neither
	// alter the proof nor race with its consumer.
	provisions[0].LeaseUUID = repairLease
	provisions[0].Items[0].SKU = "mutated"
	provisions[0].ServiceImages["web"] = "image:mutated"
	generation.ID = "not-a-uuid"
	inventories["backend-a"] = RepairBackendInventory{}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	evidence, err := repair.VerifyAttemptRepairEvidenceContext(ctx, candidate, snapshot)
	require.NoError(t, err)
	assert.NotEqual(t, AttemptRepairEvidence{}, evidence)
}

func TestRepairInventorySnapshotRejectsZeroAndTamperedFacts(t *testing.T) {
	dbPath, operationID := createRepairFixture(t, false)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	candidate, err := repair.MatchAttempt(repairLease, "backend-a", operationID)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	_, err = repair.VerifyAttemptRepairEvidenceContext(ctx, candidate, RepairInventorySnapshot{})
	require.Error(t, err)

	tampered := testRepairInventorySnapshot(t, repair, nil)
	observed := tampered.inventories["backend-a"]
	observed.Retentions = append(observed.Retentions, backend.RetainedLease{
		LeaseUUID: repairLease,
	})
	tampered.inventories["backend-a"] = observed
	_, err = repair.VerifyAttemptRepairEvidenceContext(ctx, candidate, tampered)
	require.ErrorContains(t, err, "digest does not match")
}

func TestAttemptRepairCandidateMatchesOnlyPreservedTypedProvision(t *testing.T) {
	dbPath, operationID := createRepairFixture(t, true)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	candidate, err := repair.MatchAttempt(repairLease, "backend-a", operationID)
	require.NoError(t, err)

	ownerOperation, err := operation.ParseID(repairOwnerOperation)
	require.NoError(t, err)
	ownerLifecycle, err := lifecycle.FromOperationID(ownerOperation)
	require.NoError(t, err)
	attemptedLifecycle, err := lifecycle.FromOperationID(operationID)
	require.NoError(t, err)

	tests := []struct {
		name        string
		backendName string
		observation LifecycleObservation
		want        bool
	}{
		{
			name:        "exact prior typed generation",
			backendName: "backend-a",
			observation: LifecycleObservation{Kind: LifecycleObservationTyped, ID: ownerLifecycle},
			want:        true,
		},
		{
			name:        "attempted generation",
			backendName: "backend-a",
			observation: LifecycleObservation{Kind: LifecycleObservationTyped, ID: attemptedLifecycle},
		},
		{
			name:        "prior generation on other backend",
			backendName: "backend-b",
			observation: LifecycleObservation{Kind: LifecycleObservationTyped, ID: ownerLifecycle},
		},
		{
			name:        "unknown generation",
			backendName: "backend-a",
			observation: LifecycleObservation{Kind: LifecycleObservationUnknown},
		},
		{
			name:        "legacy does not match typed prior",
			backendName: "backend-a",
			observation: LifecycleObservation{Kind: LifecycleObservationLegacy},
		},
		{
			name:        "unusable generation",
			backendName: "backend-a",
			observation: LifecycleObservation{Kind: LifecycleObservationUnusable},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, candidate.MatchesPreservedProvision(
				test.backendName, test.observation,
			))
		})
	}
}

func TestAttemptRepairCandidateMatchesExplicitLegacyForPreservedLegacyOwner(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, dbPath, map[string][]byte{
		repairLease: []byte(`{"backend":"backend-a","set_at":"2026-08-27T12:00:00Z"}`),
	})
	preparer, err := OpenLegacyUpgradePreparer(dbPath)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	inventories := map[string]BackendInventory{
		"backend-a": {
			StorageIdentity:        testBackendStorageID("backend-a"),
			Provisions:             []string{repairLease},
			ProvisionProviderUUIDs: map[string]string{repairLease: freshTestProviderUUID},
			ProvisionItems: map[string][]backend.LeaseItem{
				repairLease: {{SKU: "sku-test", Quantity: 1, ServiceName: "app"}},
			},
			Retentions: []string{},
		},
	}
	chainProof := chainProofForProvider(t, freshTestProviderUUID, repairLease)
	backupPath := filepath.Join(filepath.Dir(dbPath), "placements.v013.bak")
	backupTarget := boundExactBackupTarget(t, backupPath)
	capability, err := preparer.AuthorizePreparation(
		ctx, freshTestProviderUUID, []string{"backend-a"}, inventories,
		chainProof, backupTarget, LegacyPreparationDrainAttestation,
	)
	require.NoError(t, err)
	_, err = preparer.PrepareContext(
		ctx,
		freshTestProviderUUID,
		[]string{"backend-a"},
		inventories,
		chainProof,
		capability,
	)
	require.NoError(t, err)
	require.NoError(t, preparer.Close())
	store, err := OpenStore(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	requireTestAdmission(t, store)
	operationID, err := operation.ParseID(repairOperation)
	require.NoError(t, err)
	requireRepairTypedAttempt(t, store, repairLease, "backend-a", operationID)
	require.NoError(t, store.Close())

	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	candidate, err := repair.MatchAttempt(repairLease, "backend-a", operationID)
	require.NoError(t, err)

	assert.True(t, candidate.MatchesPreservedProvision(
		"backend-a", LifecycleObservation{Kind: LifecycleObservationLegacy},
	))
	attemptedLifecycle, err := lifecycle.FromOperationID(operationID)
	require.NoError(t, err)
	assert.False(t, candidate.MatchesPreservedProvision(
		"backend-a", LifecycleObservation{Kind: LifecycleObservationTyped, ID: attemptedLifecycle},
	))

	ctx, evidence, drain, probe := requireAttemptRepairAuthorities(t, repair, candidate)
	result, err := repair.RefuseContext(ctx, candidate, evidence, drain, probe)
	require.NoError(t, err)
	assert.Equal(t, "backend-a", result.ConfirmedOwner)
	require.NoError(t, repair.Sync())
	require.NoError(t, repair.Close())

	reopened, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	after := reopened.Lookup(repairLease)
	assert.Equal(t, StateConfirmed, after.State())
	assert.Equal(t, "backend-a", after.Backend)
	assert.Equal(t, LifecycleVerdictLegacy, reopened.CurrentLifecycle(repairLease).Verdict())
}

func TestAttemptRepairRejectsAttemptIndistinguishableFromPriorGeneration(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	store := newProviderBoundRepairStore(t, dbPath)
	requireTestAdmission(t, store)
	operationID, err := operation.ParseID(repairOperation)
	require.NoError(t, err)
	ownerAttempt := requireRepairTypedAttempt(t, store, repairLease, "backend-a", operationID)
	applied, err := store.ConfirmAttempt(ownerAttempt)
	require.NoError(t, err)
	require.True(t, applied)
	requireRepairTypedAttempt(t, store, repairLease, "backend-a", operationID)
	require.NoError(t, store.Close())

	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	_, err = repair.MatchAttempt(repairLease, "backend-a", operationID)
	require.ErrorIs(t, err, ErrAttemptRepairTarget)
	assert.ErrorContains(t, err, "indistinguishable")
}

func TestAttemptRepairRefuseDeletesOnlyAttemptOnlyRow(t *testing.T) {
	dbPath, operationID := createRepairFixture(t, false)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	candidate, err := repair.MatchAttempt(repairLease, "backend-a", operationID)
	require.NoError(t, err)
	ctx, evidence, drain, probe := requireAttemptRepairAuthorities(t, repair, candidate)

	result, err := repair.RefuseContext(ctx, candidate, evidence, drain, probe)
	require.NoError(t, err)
	assert.Empty(t, result.ConfirmedOwner)
	require.NoError(t, repair.Sync())
	require.NoError(t, repair.Close())

	reopened, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	assert.Equal(t, StateAbsent, reopened.Lookup(repairLease).State())
}

func TestAttemptRepairRefusePreservesPriorDetachedLifecycleAuthority(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	store := newProviderBoundRepairStore(t, dbPath)
	requireTestAdmission(t, store)
	ownerOperation, err := operation.ParseID(repairOwnerOperation)
	require.NoError(t, err)
	ownerAttempt := requireRepairTypedAttempt(t, store, repairLease, "backend-a", ownerOperation)
	confirmed, err := store.ConfirmAttempt(ownerAttempt)
	require.NoError(t, err)
	require.True(t, confirmed)
	requireDeleteRecord(t, store, repairLease)
	pendingOperation, err := operation.ParseID(repairOperation)
	require.NoError(t, err)
	requireRepairTypedAttempt(t, store, repairLease, "backend-a", pendingOperation)
	require.NoError(t, store.Close())

	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	candidate, err := repair.MatchAttempt(repairLease, "backend-a", pendingOperation)
	require.NoError(t, err)
	ctx, evidence, drain, probe := requireAttemptRepairAuthorities(t, repair, candidate)
	result, err := repair.RefuseContext(ctx, candidate, evidence, drain, probe)
	require.NoError(t, err)
	assert.Empty(t, result.ConfirmedOwner)
	require.NoError(t, repair.Sync())
	require.NoError(t, repair.Close())

	reopened, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	assert.Equal(t, StateAbsent, reopened.Lookup(repairLease).State())
	ownerLifecycle, err := lifecycle.FromOperationID(ownerOperation)
	require.NoError(t, err)
	authorization := reopened.AuthorizeLifecycle(repairLease, ownerLifecycle)
	assert.Equal(t, LifecycleVerdictTeardownOnly, authorization.Verdict(),
		"repairing a newer recordless attempt must not erase older teardown authority")
}

func TestAttemptRepairWriteFailureLeavesExactAttemptIntact(t *testing.T) {
	dbPath, operationID := createRepairFixture(t, false)
	db, err := bolt.Open(dbPath, 0o600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	store, err := loadStore(db)
	require.NoError(t, err)
	sourceInfo, err := os.Stat(dbPath)
	require.NoError(t, err)
	repair := &AttemptRepair{store: store, sourceInfo: sourceInfo}
	t.Cleanup(func() { _ = repair.Close() })
	candidate, err := repair.MatchAttempt(repairLease, "backend-a", operationID)
	require.NoError(t, err)
	ctx, evidence, drain, probe := requireAttemptRepairAuthorities(t, repair, candidate)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	_, err = repair.RefuseContext(ctx, candidate, evidence, drain, probe)
	require.ErrorIs(t, err, bolt.ErrDatabaseReadOnly)
	assert.Equal(t, operationID, repair.store.Lookup(repairLease).AttemptOperationID(),
		"Store updates its cache only after the bbolt transaction commits")
	after, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, after)
}

func TestOpenAttemptRepairMissingPathDoesNotCreateDatabase(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "missing.db")
	_, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.Error(t, err)
	assert.True(t, errors.Is(err, os.ErrNotExist))
	_, statErr := os.Stat(dbPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestRepairOpenRejectsAmbiguousMetadataWithoutMutation(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "placements.db")
	replaceTopologyMetadataForTest(t, dbPath, []byte(`{"schema":2,"schema":1}`))
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	inspector, err := OpenRepairInspector(dbPath, freshTestProviderUUID)
	require.ErrorIs(t, err, ErrAttemptRepairSchema)
	require.ErrorContains(t, err, `duplicate placement metadata field "schema"`)
	assert.Nil(t, inspector)
	mutator, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.ErrorIs(t, err, ErrAttemptRepairSchema)
	require.ErrorContains(t, err, `duplicate placement metadata field "schema"`)
	assert.Nil(t, mutator)

	after, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, after,
		"failed read-only or exclusive repair open must not normalize ambiguous metadata")
}

func TestRepairInspectorListsAndInspectsExactRowsWithoutMutation(t *testing.T) {
	dbPath, operationID := createRepairFixture(t, false)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	inspector, err := OpenRepairInspector(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	records := inspector.List()
	require.Len(t, records, 1)
	assert.Equal(t, RepairRecord{
		LeaseUUID:     repairLease,
		State:         StateAttempting.String(),
		Attempt:       "backend-a",
		OperationID:   operationID.String(),
		OperationKind: operation.KindProvision.String(),
		Tenant:        "tenant-test",
		ProviderUUID:  freshTestProviderUUID,
		RequestItems: []backend.LeaseItem{{
			SKU: "sku-test", Quantity: 1, ServiceName: "app",
		}},
		Revision: records[0].Revision,
	}, records[0])
	assert.NotZero(t, records[0].Revision)

	record, exists, err := inspector.Inspect(repairLease)
	require.NoError(t, err)
	require.True(t, exists)
	assert.Equal(t, records[0], record)
	_, exists, err = inspector.Inspect(repairOtherLease)
	require.NoError(t, err)
	assert.False(t, exists)

	writer, writerErr := bolt.Open(dbPath, 0o600, &bolt.Options{Timeout: 50 * time.Millisecond})
	if writer != nil {
		require.NoError(t, writer.Close())
	}
	require.ErrorIs(t, writerErr, bolt.ErrTimeout,
		"offline read-only inspection must not race a providerd writer")
	require.NoError(t, inspector.Close())
	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

func TestRepairInspectorExposesPersistedUntrustedPositiveQuarantine(t *testing.T) {
	dbPath := createUntrustedPositiveRepairFixture(t)

	inspector, err := OpenRepairInspector(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, inspector.Close()) })

	records := inspector.List()
	require.Len(t, records, 1)
	assert.Equal(t, repairLease, records[0].LeaseUUID)
	assert.Equal(t, StateUnusable.String(), records[0].State)
	assert.True(t, records[0].Conflict)
	assert.True(t, records[0].UntrustedPositive)
	assert.Equal(t, []string{"backend-a"}, records[0].ConflictBackends)
	assert.False(t, records[0].ConflictOwnersUnknown)

	record, exists, err := inspector.Inspect(repairLease)
	require.NoError(t, err)
	require.True(t, exists)
	assert.Equal(t, records[0], record)

	ordinary := newRepairRecord(repairOtherLease, Placement{
		Conflict:         true,
		ConflictBackends: []string{"backend-a", "backend-b"},
		revision:         1,
	})
	assert.False(t, ordinary.UntrustedPositive,
		"an ordinary conflict must not be mislabeled as a rejected-positive quarantine")
}

func TestRepairRecordExposesNonSecretExactAttemptFacts(t *testing.T) {
	id, err := operation.ParseID(repairOperation)
	require.NoError(t, err)
	payloadHash := sha256.Sum256([]byte("repair-visible-payload"))
	fingerprint, err := NewPayloadFingerprint(payloadHash[:])
	require.NoError(t, err)

	provisionRecord := newRepairRecord(repairLease, Placement{
		Attempt:                   "backend-a",
		SetAt:                     time.Now(),
		revision:                  1,
		attemptOperationID:        id,
		attemptOperationKind:      operation.KindProvision,
		attemptPayloadFingerprint: fingerprint,
	})
	assert.Equal(t, operation.KindProvision.String(), provisionRecord.OperationKind)
	assert.Equal(t, fingerprint.String(), provisionRecord.PayloadHash)
	assert.Empty(t, provisionRecord.RestoreSourceLeaseUUID)

	restoreRecord := newRepairRecord(repairOtherLease, Placement{
		Attempt:                       "backend-a",
		SetAt:                         time.Now(),
		revision:                      1,
		attemptOperationID:            id,
		attemptOperationKind:          operation.KindRestore,
		attemptRestoreSourceLeaseUUID: repairLease,
	})
	assert.Equal(t, operation.KindRestore.String(), restoreRecord.OperationKind)
	assert.Equal(t, repairLease, restoreRecord.RestoreSourceLeaseUUID)
	assert.Empty(t, restoreRecord.PayloadHash)
}

func createConflictRepairFixture(t *testing.T) string {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	store := newProviderBoundRepairStore(t, dbPath)
	requireTestAdmission(t, store)
	fence := store.BeginInventorySession()
	_, err := store.ProjectInventory(fence, InventoryProjection{
		Conflicts: map[string][]string{
			repairLease: {"backend-a", "backend-b"},
		},
	})
	store.EndInventorySession(fence)
	require.NoError(t, err)
	require.NoError(t, store.Close())
	return dbPath
}

func createUntrustedPositiveRepairFixture(t *testing.T) string {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	store := newProviderBoundRepairStore(t, dbPath)
	requireTestAdmission(t, store)
	fence := store.BeginInventorySession()
	_, err := store.ProjectInventory(fence, InventoryProjection{
		UntrustedPositives: map[string][]string{
			repairLease: {"backend-a"},
		},
	})
	store.EndInventorySession(fence)
	require.NoError(t, err)
	require.NoError(t, store.Close())
	return dbPath
}

func TestConflictRepairCandidateBindsExactRevisionTopologyAndCandidateSet(t *testing.T) {
	dbPath := createConflictRepairFixture(t)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })

	candidate, err := repair.MatchConflict(repairLease, "backend-a")
	require.NoError(t, err)
	assert.Equal(t, repairLease, candidate.LeaseUUID())
	assert.Equal(t, "backend-a", candidate.SelectedBackend())
	assert.Equal(t, []string{"backend-a", "backend-b"}, candidate.CandidateBackends())
	assert.NotZero(t, candidate.Revision())
	assert.Contains(t, candidate.ConfirmationValue(),
		"resolve-conflict:"+repairLease+":backend-a:")

	_, err = repair.MatchConflict(repairLease, "backend-c")
	require.ErrorIs(t, err, ErrConflictRepairTarget)
	_, err = repair.MatchConflict(repairOtherLease, "backend-a")
	require.ErrorIs(t, err, ErrConflictRepairTarget)
}

func TestConflictRepairResolvesProvisionAndRetentionWithoutInventingAuthority(t *testing.T) {
	tests := []struct {
		name            string
		retained        bool
		lifecycle       func(t *testing.T) *backend.LifecycleGenerationObservation
		wantRetained    bool
		wantLifecycle   LifecycleVerdict
		wantLifecycleID bool
	}{
		{
			name: "typed active provision",
			lifecycle: func(t *testing.T) *backend.LifecycleGenerationObservation {
				t.Helper()
				id, err := lifecycle.ParseID(repairOwnerOperation)
				require.NoError(t, err)
				return &backend.LifecycleGenerationObservation{
					Kind: backend.LifecycleGenerationTyped,
					ID:   id.String(),
				}
			},
			wantLifecycle:   LifecycleVerdictAuthorized,
			wantLifecycleID: true,
		},
		{
			name:          "retained lease",
			retained:      true,
			wantRetained:  true,
			wantLifecycle: LifecycleVerdictUnusable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dbPath := createConflictRepairFixture(t)
			repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
			require.NoError(t, err)
			candidate, err := repair.MatchConflict(repairLease, "backend-a")
			require.NoError(t, err)
			oldRevision := candidate.Revision()
			var observation *backend.LifecycleGenerationObservation
			if test.lifecycle != nil {
				observation = test.lifecycle(t)
			}
			ctx, plan, drain, probe := requireConflictRepairAuthorities(
				t, repair, candidate, test.retained, observation,
			)

			result, err := repair.ResolveConflictContext(ctx, plan, drain, probe)
			require.NoError(t, err)
			assert.Equal(t, "backend-a", result.ConfirmedOwner)
			assert.Equal(t, test.wantRetained, result.Retained)
			require.NoError(t, repair.Sync())
			require.NoError(t, repair.Close())

			reopened, err := newStoreForTest(dbPath)
			require.NoError(t, err)
			t.Cleanup(func() { _ = reopened.Close() })
			resolved := reopened.Lookup(repairLease)
			assert.Equal(t, StateConfirmed, resolved.State())
			assert.Equal(t, "backend-a", resolved.Backend)
			assert.Empty(t, resolved.Attempt)
			assert.False(t, resolved.Conflict)
			assert.Empty(t, resolved.ConflictBackends)
			assert.Greater(t, resolved.Revision(), oldRevision)
			authorization := reopened.CurrentLifecycle(repairLease)
			assert.Equal(t, test.wantLifecycle, authorization.Verdict())
			assert.Equal(t, test.wantLifecycleID, authorization.ID().Valid())
		})
	}
}

func TestConflictRepairRejectsInPlaceBackupMutationBeforeCommit(t *testing.T) {
	dbPath := createConflictRepairFixture(t)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	candidate, err := repair.MatchConflict(repairLease, "backend-a")
	require.NoError(t, err)
	ctx, plan, drain, probe, backup := requireConflictRepairAuthoritiesWithBackup(
		t, repair, candidate, true, nil,
	)
	corruptExactBackupInPlace(t, backup)

	_, err = repair.ResolveConflictContext(ctx, plan, drain, probe)
	require.ErrorContains(t, err, "backup bytes changed")
	assert.True(t, repair.store.Lookup(repairLease).Conflict)
	after, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, after,
		"an in-place-corrupted rollback image must fail before conflict repair commits")
}

func TestConflictRepairRejectsStaleOpaqueCandidate(t *testing.T) {
	dbPath := createConflictRepairFixture(t)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	stale, err := repair.MatchConflict(repairLease, "backend-a")
	require.NoError(t, err)
	ctx, plan, drain, probe := requireConflictRepairAuthorities(
		t, repair, stale, true, nil,
	)
	_, err = repair.ResolveConflictContext(ctx, plan, drain, probe)
	require.NoError(t, err)

	_, err = repair.ResolveConflictContext(ctx, plan, drain, probe)
	require.ErrorIs(t, err, ErrConflictRepairTarget)
	assert.Equal(t, StateConfirmed, repair.store.Lookup(repairLease).State())
}

func TestConflictRepairRequiresIndependentInventoryAndDrainCapabilities(t *testing.T) {
	dbPath := createConflictRepairFixture(t)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	candidate, err := repair.MatchConflict(repairLease, "backend-a")
	require.NoError(t, err)
	ctx, plan, drain, probe := requireConflictRepairAuthorities(
		t, repair, candidate, true, nil,
	)

	_, err = repair.ResolveConflictContext(ctx, ConflictRepairPlan{}, drain, probe)
	require.ErrorIs(t, err, ErrConflictRepairTarget)
	_, err = repair.ResolveConflictContext(ctx, plan, DrainAttestation{}, probe)
	require.ErrorIs(t, err, ErrConflictRepairTarget)
	assert.Equal(t, StateUnusable, repair.store.Lookup(repairLease).State())
}

func TestConflictRepairPlanRejectsContextTransplantAndFinalInventoryDrift(t *testing.T) {
	dbPath := createConflictRepairFixture(t)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	candidate, err := repair.MatchConflict(repairLease, "backend-a")
	require.NoError(t, err)
	lifecycleID, err := lifecycle.ParseID(repairOwnerOperation)
	require.NoError(t, err)
	ctx, plan, drain, originalProbe := requireConflictRepairAuthorities(
		t, repair, candidate, false, &backend.LifecycleGenerationObservation{
			Kind: backend.LifecycleGenerationTyped,
			ID:   lifecycleID.String(),
		},
	)

	foreignCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, err = repair.ResolveConflictContext(foreignCtx, plan, drain, originalProbe)
	require.ErrorIs(t, err, ErrConflictRepairTarget)
	require.ErrorContains(t, err, "another cancellation scope")

	drifted := testRepairInventorySnapshot(t, repair, map[string]RepairBackendInventory{
		"backend-a": {
			Provisions: []backend.ProvisionInfo{{
				LeaseUUID: repairLease, ProviderUUID: freshTestProviderUUID, Tenant: "tenant-a",
				LifecycleGeneration: &backend.LifecycleGenerationObservation{
					Kind: backend.LifecycleGenerationTyped,
					ID:   lifecycleID.String(),
				},
			}},
		},
		"backend-b": {
			Provisions: []backend.ProvisionInfo{{
				LeaseUUID: repairOtherLease, ProviderUUID: freshTestProviderUUID, Tenant: "tenant-b",
			}},
		},
	})
	probeCalled := false
	_, err = repair.ResolveConflictContext(ctx, plan, drain, func(context.Context) (RepairInventorySnapshot, error) {
		probeCalled = true
		return drifted, nil
	})
	require.ErrorIs(t, err, ErrConflictRepairTarget)
	require.ErrorContains(t, err, "final live inventory no longer matches")
	assert.True(t, probeCalled)
	assert.Equal(t, StateUnusable, repair.store.Lookup(repairLease).State())
}

func TestConflictRepairIdentityRulesFailClosedBeforePlanningAuthority(t *testing.T) {
	dbPath := createConflictRepairFixture(t)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	candidate, err := repair.MatchConflict(repairLease, "backend-a")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	for name, inventory := range map[string]RepairBackendInventory{
		"active provider": {
			Provisions: []backend.ProvisionInfo{{
				LeaseUUID: repairLease, ProviderUUID: repairOtherLease, Tenant: "tenant-a",
			}},
		},
		"retained provider": {
			Retentions: []backend.RetainedLease{{
				LeaseUUID: repairLease, ProviderUUID: repairOtherLease, Tenant: "tenant-a",
			}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			snapshot := testRepairInventorySnapshot(t, repair, map[string]RepairBackendInventory{
				"backend-a": inventory,
			})
			_, planErr := repair.PlanConflictRepairContext(ctx, candidate, snapshot)
			require.ErrorIs(t, planErr, ErrRepairConflictEvidence)
			require.ErrorContains(t, planErr, "provider")
		})
	}

	retainedSnapshot := testRepairInventorySnapshot(t, repair, map[string]RepairBackendInventory{
		"backend-a": {Retentions: []backend.RetainedLease{{LeaseUUID: repairLease}}},
	})
	plan, err := repair.PlanConflictRepairContext(ctx, candidate, retainedSnapshot)
	require.NoError(t, err)
	assert.False(t, plan.identityAuthoritative)
	assert.True(t, plan.retained)
	assert.Equal(t, LifecycleObservationUnusable, plan.lifecycle.Kind)
}
