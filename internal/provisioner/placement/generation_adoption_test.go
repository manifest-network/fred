package placement

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

// createGenerationQuarantineFixture confirms repairLease on backend-a with a
// typed generation and a runtime principal, then projects an inventory in
// which backend-a reports a different typed generation: the quarantine a
// restore from an older copy leaves on a lease re-provisioned since.
func createGenerationQuarantineFixture(t *testing.T) (dbPath string, stored, observed lifecycle.ID) {
	t.Helper()
	return createGenerationQuarantineFixtureWith(t, func(*Store) {})
}

// createGenerationQuarantineFixtureWith runs then on the open store after the
// inventory that quarantines the lease.
func createGenerationQuarantineFixtureWith(
	t *testing.T,
	then func(*Store),
) (dbPath string, stored, observed lifecycle.ID) {
	t.Helper()
	dbPath = filepath.Join(t.TempDir(), "placements.db")
	store := newProviderBoundRepairStore(t, dbPath)
	requireTestAdmission(t, store)
	ownerOperation, err := operation.ParseID(repairOwnerOperation)
	require.NoError(t, err)
	attempt := requireRepairTypedAttempt(t, store, repairLease, "backend-a", ownerOperation)
	applied, err := confirmAttemptForTest(store, attempt)
	require.NoError(t, err)
	require.True(t, applied)
	authorization := store.CurrentLifecycle(repairLease)
	require.True(t, authorization.Authorized())
	stored = authorization.ID()

	observed = requireLifecycleID(t, "8601")
	projectInventoryForTest(t, store, InventoryProjection{
		Placements: map[string]string{repairLease: "backend-a"},
		lifecycles: map[string]LifecycleObservation{
			repairLease: {Kind: LifecycleObservationTyped, ID: observed},
		},
	})
	require.True(t, store.lifecycleCache[repairLease].unusable)
	then(store)
	require.NoError(t, store.Close())
	return dbPath, stored, observed
}

func generationOwner(id lifecycle.ID, status backend.ProvisionStatus) backend.ProvisionInfo {
	return backend.ProvisionInfo{
		LeaseUUID:    repairLease,
		ProviderUUID: freshTestProviderUUID,
		Tenant:       "tenant-test",
		Status:       status,
		LifecycleGeneration: &backend.LifecycleGenerationObservation{
			Kind: backend.LifecycleGenerationTyped,
			ID:   id.String(),
		},
	}
}

func generationSnapshot(
	t *testing.T,
	repair *AttemptRepair,
	owners map[string][]backend.ProvisionInfo,
) RepairInventorySnapshot {
	t.Helper()
	overrides := make(map[string]RepairBackendInventory, len(owners))
	for backendName, provisions := range owners {
		overrides[backendName] = RepairBackendInventory{Provisions: provisions}
	}
	return testRepairInventorySnapshot(t, repair, overrides)
}

func openGenerationRepair(t *testing.T, dbPath string) *AttemptRepair {
	t.Helper()
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	return repair
}

func TestGenerationAdoptionReplacesTheQuarantinedGeneration(t *testing.T) {
	dbPath, stored, observed := createGenerationQuarantineFixture(t)
	repair := openGenerationRepair(t, dbPath)
	candidate, err := repair.MatchGenerationQuarantine(repairLease, "backend-a")
	require.NoError(t, err)
	assert.Equal(t, stored.Fingerprint(), candidate.StoredGenerationFingerprint())

	snapshot := generationSnapshot(t, repair, map[string][]backend.ProvisionInfo{
		"backend-a": {generationOwner(observed, backend.ProvisionStatusReady)},
	})
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	plan, err := repair.PlanGenerationAdoptionContext(ctx, candidate, snapshot)
	require.NoError(t, err)
	assert.Equal(t, observed.Fingerprint(), plan.ObservedGenerationFingerprint())
	assert.NotContains(t, plan.ConfirmationValue(), observed.String(), "the confirmation reveals no generation")
	assert.NotContains(t, plan.ConfirmationValue(), stored.String())

	attestation, err := repair.AttestGeneration(plan.ConfirmationValue(), GenerationAttestationText)
	require.NoError(t, err)
	requireExactRepairBackup(t, repair)
	probe := func(context.Context) (RepairInventorySnapshot, error) { return snapshot, nil }
	result, err := repair.AdoptObservedGenerationContext(ctx, plan, attestation, probe)
	require.NoError(t, err)
	require.NoError(t, repair.Close())

	inspector, err := OpenRepairInspector(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	require.NoError(t, inspector.VerifyGenerationAdoptionPostcondition(candidate, result))
	require.NoError(t, inspector.Close())

	reopened, err := OpenStore(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	defer func() { _ = reopened.Close() }()
	authorization := reopened.CurrentLifecycle(repairLease)
	require.True(t, authorization.Authorized(), "the adopted generation authorizes callbacks after reopen")
	assert.Equal(t, observed, authorization.ID())
	record := reopened.Lookup(repairLease)
	assert.Equal(t, "backend-a", record.Backend)
	assert.False(t, record.AttemptOperationID().Valid(), "the old generation's operation metadata is gone")
}

func TestGenerationAdoptionRefusesWhatInventoryDoesNotProve(t *testing.T) {
	dbPath, stored, observed := createGenerationQuarantineFixture(t)
	repair := openGenerationRepair(t, dbPath)
	candidate, err := repair.MatchGenerationQuarantine(repairLease, "backend-a")
	require.NoError(t, err)
	retained := RepairBackendInventory{Retentions: []backend.RetainedLease{{LeaseUUID: repairLease}}}
	foreignTenant := generationOwner(observed, backend.ProvisionStatusReady)
	foreignTenant.Tenant = "tenant-other"
	foreignProvider := generationOwner(observed, backend.ProvisionStatusReady)
	foreignProvider.ProviderUUID = "1e1698c3-a922-460a-8296-70efdbc03032"
	unknownGeneration := generationOwner(observed, backend.ProvisionStatusReady)
	unknownGeneration.LifecycleGeneration = nil

	for name, snapshot := range map[string]RepairInventorySnapshot{
		"no owner": generationSnapshot(t, repair, nil),
		"another backend also reports it": generationSnapshot(t, repair, map[string][]backend.ProvisionInfo{
			"backend-a": {generationOwner(observed, backend.ProvisionStatusReady)},
			"backend-b": {generationOwner(observed, backend.ProvisionStatusReady)},
		}),
		"only another backend reports it": generationSnapshot(t, repair, map[string][]backend.ProvisionInfo{
			"backend-b": {generationOwner(observed, backend.ProvisionStatusReady)},
		}),
		"another backend retains it": testRepairInventorySnapshot(t, repair, map[string]RepairBackendInventory{
			"backend-a": {Provisions: []backend.ProvisionInfo{generationOwner(observed, backend.ProvisionStatusReady)}},
			"backend-b": retained,
		}),
		"the stored generation": generationSnapshot(t, repair, map[string][]backend.ProvisionInfo{
			"backend-a": {generationOwner(stored, backend.ProvisionStatusReady)},
		}),
		"an operation in flight": generationSnapshot(t, repair, map[string][]backend.ProvisionInfo{
			"backend-a": {generationOwner(observed, backend.ProvisionStatusUpdating)},
		}),
		"another tenant": generationSnapshot(t, repair, map[string][]backend.ProvisionInfo{
			"backend-a": {foreignTenant},
		}),
		"another provider": generationSnapshot(t, repair, map[string][]backend.ProvisionInfo{
			"backend-a": {foreignProvider},
		}),
		"no typed generation": generationSnapshot(t, repair, map[string][]backend.ProvisionInfo{
			"backend-a": {unknownGeneration},
		}),
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			_, err := repair.PlanGenerationAdoptionContext(ctx, candidate, snapshot)
			assert.ErrorIs(t, err, ErrGenerationAdoptionEvidence)
		})
	}
}

func TestGenerationAdoptionRequiresTheExactConfirmedPlan(t *testing.T) {
	dbPath, _, observed := createGenerationQuarantineFixture(t)
	repair := openGenerationRepair(t, dbPath)
	candidate, err := repair.MatchGenerationQuarantine(repairLease, "backend-a")
	require.NoError(t, err)
	snapshot := generationSnapshot(t, repair, map[string][]backend.ProvisionInfo{
		"backend-a": {generationOwner(observed, backend.ProvisionStatusReady)},
	})
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	plan, err := repair.PlanGenerationAdoptionContext(ctx, candidate, snapshot)
	require.NoError(t, err)

	_, err = repair.AttestGeneration(plan.ConfirmationValue(), "I attest")
	assert.Error(t, err)
	attestation, err := repair.AttestGeneration(plan.ConfirmationValue(), GenerationAttestationText)
	require.NoError(t, err)
	foreign, err := repair.AttestGeneration("adopt-generation:other", GenerationAttestationText)
	require.NoError(t, err)
	requireExactRepairBackup(t, repair)

	changed := generationSnapshot(t, repair, map[string][]backend.ProvisionInfo{
		"backend-a": {generationOwner(requireLifecycleID(t, "8602"), backend.ProvisionStatusReady)},
	})
	elsewhere := generationSnapshot(t, repair, map[string][]backend.ProvisionInfo{
		"backend-a": {generationOwner(observed, backend.ProvisionStatusReady)},
		"backend-b": {{LeaseUUID: restoreTargetLease, ProviderUUID: freshTestProviderUUID, Tenant: "tenant-test"}},
	})
	for name, attempt := range map[string]func() error{
		"an attestation for another plan": func() error {
			_, err := repair.AdoptObservedGenerationContext(ctx, plan, foreign,
				func(context.Context) (RepairInventorySnapshot, error) { return snapshot, nil })
			return err
		},
		"no final probe": func() error {
			_, err := repair.AdoptObservedGenerationContext(ctx, plan, attestation, nil)
			return err
		},
		"a final inventory with another generation": func() error {
			_, err := repair.AdoptObservedGenerationContext(ctx, plan, attestation,
				func(context.Context) (RepairInventorySnapshot, error) { return changed, nil })
			return err
		},
		"a final inventory that changed elsewhere": func() error {
			_, err := repair.AdoptObservedGenerationContext(ctx, plan, attestation,
				func(context.Context) (RepairInventorySnapshot, error) { return elsewhere, nil })
			return err
		},
		"a context that is not the plan's": func() error {
			other, cancelOther := context.WithTimeout(t.Context(), time.Minute)
			defer cancelOther()
			_, err := repair.AdoptObservedGenerationContext(other, plan, attestation,
				func(context.Context) (RepairInventorySnapshot, error) { return snapshot, nil })
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Error(t, attempt())
		})
	}
	_, err = repair.MatchGenerationQuarantine(repairLease, "backend-a")
	assert.NoError(t, err, "every refused adoption left the quarantine in place")
}

func TestMatchGenerationQuarantineRefusesAQuarantineItsRowsExplain(t *testing.T) {
	t.Run("a usable lease", func(t *testing.T) {
		dbPath, _, _ := createGenerationQuarantineFixture(t)
		rewriteLifecycleRowForTest(t, dbPath, func(capability *lifecycleCapability) {
			capability.unusable = false
		})
		repair := openGenerationRepair(t, dbPath)
		_, err := repair.MatchGenerationQuarantine(repairLease, "backend-a")
		assert.ErrorContains(t, err, "not quarantined", "a healthy generation is never replaced")
	})
	t.Run("another backend", func(t *testing.T) {
		dbPath, _, _ := createGenerationQuarantineFixture(t)
		repair := openGenerationRepair(t, dbPath)
		_, err := repair.MatchGenerationQuarantine(repairLease, "backend-b")
		assert.ErrorIs(t, err, ErrGenerationAdoptionTarget)
	})
	t.Run("a principal that contradicts the operation metadata", func(t *testing.T) {
		dbPath, _, _ := createGenerationQuarantineFixture(t)
		rewriteLifecycleRowForTest(t, dbPath, func(capability *lifecycleCapability) {
			capability.principal = runtimePrincipal{tenant: "tenant-other", providerUUID: freshTestProviderUUID}
		})
		repair := openGenerationRepair(t, dbPath)
		_, err := repair.MatchGenerationQuarantine(repairLease, "backend-a")
		assert.ErrorIs(t, err, ErrGenerationAdoptionTarget)
	})
	t.Run("no runtime principal", func(t *testing.T) {
		dbPath, _, _ := createGenerationQuarantineFixture(t)
		rewriteLifecycleRowForTest(t, dbPath, func(capability *lifecycleCapability) {
			capability.principal = runtimePrincipal{}
		})
		repair := openGenerationRepair(t, dbPath)
		_, err := repair.MatchGenerationQuarantine(repairLease, "backend-a")
		assert.ErrorContains(t, err, "could not authorize maintenance")
	})
	t.Run("a legacy generation", func(t *testing.T) {
		dbPath, _, _ := createGenerationQuarantineFixture(t)
		rewriteLifecycleRowForTest(t, dbPath, func(capability *lifecycleCapability) {
			capability.id = lifecycle.ID{}
		})
		rewritePlacementRowForTest(t, dbPath, func(record *Placement) { clearOperationMetadata(record) })
		repair := openGenerationRepair(t, dbPath)
		_, err := repair.MatchGenerationQuarantine(repairLease, "backend-a")
		assert.ErrorContains(t, err, "not typed")
	})
	t.Run("a durable placement row that is not the canonical encoding", func(t *testing.T) {
		dbPath, _, _ := createGenerationQuarantineFixture(t)
		db, err := bolt.Open(dbPath, 0o600, &bolt.Options{Timeout: time.Second})
		require.NoError(t, err)
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			placements := tx.Bucket(bucketName)
			var indented bytes.Buffer
			require.NoError(t, json.Indent(&indented, placements.Get([]byte(repairLease)), "", "  "))
			return placements.Put([]byte(repairLease), indented.Bytes())
		}))
		require.NoError(t, db.Close())
		repair := openGenerationRepair(t, dbPath)
		_, err = repair.MatchGenerationQuarantine(repairLease, "backend-a")
		assert.ErrorIs(t, err, ErrGenerationAdoptionTarget, "the loader already treats it as unusable")
	})
	t.Run("a pending maintenance command", func(t *testing.T) {
		dbPath, _, _ := createGenerationQuarantineFixture(t)
		db, err := bolt.Open(dbPath, 0o600, &bolt.Options{Timeout: time.Second})
		require.NoError(t, err)
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			pending, _, bucketErr := maintenanceCommandBuckets(tx)
			if bucketErr != nil {
				return bucketErr
			}
			return pending.Put([]byte(repairLease), []byte(maintenanceIDA))
		}))
		require.NoError(t, db.Close())
		repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
		if err == nil {
			defer func() { _ = repair.Close() }()
			_, err = repair.MatchGenerationQuarantine(repairLease, "backend-a")
		}
		assert.Error(t, err, "a pending command blocks adoption, when the database opens or when the lease is matched")
	})
	t.Run("a durable row that is not the canonical encoding", func(t *testing.T) {
		dbPath, _, _ := createGenerationQuarantineFixture(t)
		db, err := bolt.Open(dbPath, 0o600, &bolt.Options{Timeout: time.Second})
		require.NoError(t, err)
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			capabilities := tx.Bucket(lifecycleCapabilityBucketName)
			var indented bytes.Buffer
			require.NoError(t, json.Indent(&indented, capabilities.Get([]byte(repairLease)), "", "  "))
			return capabilities.Put([]byte(repairLease), indented.Bytes())
		}))
		require.NoError(t, db.Close())
		repair := openGenerationRepair(t, dbPath)
		_, err = repair.MatchGenerationQuarantine(repairLease, "backend-a")
		assert.ErrorContains(t, err, "canonical encoding")
	})
	t.Run("a restore that uses it as its source", func(t *testing.T) {
		dbPath, _, _ := createGenerationQuarantineFixtureWith(t, func(store *Store) {
			source, err := store.reserveRestoreSource(repairLease)
			require.NoError(t, err)
			defer store.releaseRestoreSource(source)
			restoreOperation := requireOperationID(t, "8603")
			_, err = store.beginReservedRestore(store.CurrentAdmissionBaseline(), source, restoreTargetLease,
				restoreOperation, repairBackendRequestSnapshot(t), testCallbackPair(restoreOperation))
			require.NoError(t, err)
		})
		repair := openGenerationRepair(t, dbPath)
		_, err := repair.MatchGenerationQuarantine(repairLease, "backend-a")
		assert.ErrorContains(t, err, "is restoring from it")
	})
}

const restoreTargetLease = "00000000-0000-4000-8000-0000000000c1"

// rewritePlacementRowForTest edits the stored placement row of repairLease.
func rewritePlacementRowForTest(t *testing.T, dbPath string, edit func(*Placement)) {
	t.Helper()
	db, err := bolt.Open(dbPath, 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		placements := tx.Bucket(bucketName)
		record, decoded := decodeAuthorityPlacement(repairLease, placements.Get([]byte(repairLease)))
		require.True(t, decoded)
		edit(&record)
		encoded, encodeErr := encodePlacement(record)
		require.NoError(t, encodeErr)
		return placements.Put([]byte(repairLease), encoded)
	}))
}

// rewriteLifecycleRowForTest edits the stored lifecycle row of repairLease.
func rewriteLifecycleRowForTest(t *testing.T, dbPath string, edit func(*lifecycleCapability)) {
	t.Helper()
	db, err := bolt.Open(dbPath, 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		capabilities := tx.Bucket(lifecycleCapabilityBucketName)
		capability, decodeErr := decodeLifecycleCapability(capabilities.Get([]byte(repairLease)))
		require.NoError(t, decodeErr)
		edit(&capability)
		encoded, encodeErr := encodeLifecycleCapability(capability)
		require.NoError(t, encodeErr)
		return capabilities.Put([]byte(repairLease), encoded)
	}))
}

func TestClassifyCountsAnAdoptableQuarantine(t *testing.T) {
	dbPath, _, _ := createGenerationQuarantineFixture(t)
	expectation, err := NewAuthorityExpectation(freshTestProviderUUID, []string{"backend-a", "backend-b"})
	require.NoError(t, err)
	report, err := InspectAuthorityFile(dbPath, expectation)
	require.NoError(t, err)
	assert.Equal(t, 1, report.Counts.UnusableAdoptionCandidates)
	for _, row := range report.Rows {
		if row.LeaseUUID == repairLease {
			assert.Equal(t, "unusable_adoption_candidate", row.LifecycleVerdict)
		}
	}
	assert.False(t, strings.Contains(strings.Join(rowVerdicts(report), ","), "life_"), "no generation is rendered")
}

// TestClassifyAndOpenAgreeOnAPrincipalContradiction pins the shared binding
// check: a row open-time loading would quarantine is not typed_active offline.
func TestClassifyAndOpenAgreeOnAPrincipalContradiction(t *testing.T) {
	dbPath, _, _ := createGenerationQuarantineFixture(t)
	rewriteLifecycleRowForTest(t, dbPath, func(capability *lifecycleCapability) {
		capability.unusable = false
		capability.principal = runtimePrincipal{tenant: "tenant-other", providerUUID: freshTestProviderUUID}
	})
	expectation, err := NewAuthorityExpectation(freshTestProviderUUID, []string{"backend-a", "backend-b"})
	require.NoError(t, err)
	report, err := InspectAuthorityFile(dbPath, expectation)
	require.NoError(t, err)
	assert.Contains(t, rowVerdicts(report), "mismatched")
}

func rowVerdicts(report AuthorityReport) []string {
	verdicts := make([]string, 0, len(report.Rows))
	for _, row := range report.Rows {
		verdicts = append(verdicts, row.LifecycleVerdict)
	}
	return verdicts
}

// TestGenerationAdoptionCapabilitiesAreMintedOnlyByTheirConstructors keeps
// the minting claims on the adoption types true.
func TestGenerationAdoptionCapabilitiesAreMintedOnlyByTheirConstructors(t *testing.T) {
	for _, violation := range literalsMintedOutside(t, map[string]string{
		"GenerationAdoptionCandidate": "matchGenerationQuarantineLocked",
		"GenerationAdoptionPlan":      "newGenerationAdoptionPlan",
		"GenerationAttestation":       "AttestGeneration",
		"GenerationAdoptionResult":    "AdoptObservedGenerationContext",
	}) {
		t.Error(violation)
	}
}

func TestRepairListMarksOnlyAdoptionCandidates(t *testing.T) {
	dbPath, _, _ := createGenerationQuarantineFixture(t)
	inspector, err := OpenRepairInspector(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	defer func() { require.NoError(t, inspector.Close()) }()
	records := inspector.List()
	require.Len(t, records, 1)
	assert.True(t, records[0].AdoptionCandidate)
	record, found, err := inspector.Inspect(repairLease)
	require.NoError(t, err)
	require.True(t, found)
	assert.True(t, record.AdoptionCandidate)

	healthyPath, _, _ := createGenerationQuarantineFixture(t)
	rewriteLifecycleRowForTest(t, healthyPath, func(capability *lifecycleCapability) {
		capability.unusable = false
	})
	healthy, err := OpenRepairInspector(healthyPath, freshTestProviderUUID)
	require.NoError(t, err)
	defer func() { require.NoError(t, healthy.Close()) }()
	for _, record := range healthy.List() {
		assert.False(t, record.AdoptionCandidate, "a healthy lease is not a candidate")
	}
}
