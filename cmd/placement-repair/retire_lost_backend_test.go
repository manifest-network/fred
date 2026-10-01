package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/placementprobe"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

const retirementCommandTarget = "backend-b"

type retirementPlanOutput struct {
	placement.BackendRetirementFacts
	TargetProbe string `json:"target_probe"`
	AttestLost  string `json:"attest_lost"`
	Confirm     string `json:"confirm"`
}

// newRetirementCommandFixture returns a stopped two-backend database with one
// lease confirmed on backend-b, and a config whose backend-b answers with
// targetHandler (nil: nothing listens) while backend-a must never be contacted.
func newRetirementCommandFixture(t *testing.T, targetHandler http.Handler) (dbPath, configPath string) {
	t.Helper()
	return newRetirementCommandFixtureAfter(t, targetHandler, nil)
}

// newRetirementCommandFixtureAfter is newRetirementCommandFixture with history
// applied to the open store before the lease is projected.
func newRetirementCommandFixtureAfter(
	t *testing.T,
	targetHandler http.Handler,
	history func(*placement.Store),
) (dbPath, configPath string) {
	t.Helper()
	names := []string{repairCommandBackend, retirementCommandTarget}
	dbPath = filepath.Join(t.TempDir(), "placements.db")
	store := initializeRepairPlacementStore(t, dbPath, names)
	if history != nil {
		history(store)
	}
	projectRepairInventoryWithRows(t, newRepairReconciliation(t, store, names), names,
		placement.ReconciliationProjection{
			Placements: map[string]string{repairCommandLease: retirementCommandTarget},
		},
		map[string][]backend.ProvisionInfo{
			repairCommandBackend: {},
			retirementCommandTarget: {{
				LeaseUUID: repairCommandLease, BackendName: retirementCommandTarget,
				ProviderUUID: repairCommandProviderUUID, Tenant: "tenant-a",
			}},
		},
	)
	require.Equal(t, placement.StateConfirmed, store.Lookup(repairCommandLease).State())
	require.NoError(t, store.Close())

	survivor := newVerifiedInventoryServer(t, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		t.Errorf("retirement contacted the surviving backend %s", r.URL.Path)
	}))
	target := newVerifiedInventoryServer(t, targetHandler)
	configPath = writeRepairConfigURLs(t, dbPath, map[string]string{
		repairCommandBackend: survivor.URL, retirementCommandTarget: target.URL,
	})
	if targetHandler == nil {
		target.Close()
	}
	return dbPath, configPath
}

func retirementArgs(configPath string, extra ...string) []string {
	return append([]string{
		"-config", configPath, "-retire-lost-backend", "-backend", retirementCommandTarget,
		"-storage-id", "6ba7b811-9dad-41d1-80b4-00c04fd430c8",
	}, extra...)
}

func planRetirementCommand(t *testing.T, configPath string) retirementPlanOutput {
	t.Helper()
	var stdout bytes.Buffer
	require.NoError(t, run(t.Context(), retirementArgs(configPath), &stdout, &bytes.Buffer{}))
	var output retirementPlanOutput
	decoder := json.NewDecoder(&stdout)
	decoder.DisallowUnknownFields()
	require.NoError(t, decoder.Decode(&output))
	require.False(t, decoder.More(), "the plan is exactly one JSON object")
	return output
}

func TestRun_RetireLostBackendDryRunPrintsTheBoundPlanWithoutMutating(t *testing.T) {
	dbPath, configPath := newRetirementCommandFixture(t, nil)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	plan := planRetirementCommand(t, configPath)
	assert.Equal(t, retirementCommandTarget, plan.Backend)
	assert.Equal(t, repairBackendStorageID(t, retirementCommandTarget).String(), plan.StorageID)
	assert.Equal(t, []string{repairCommandLease}, plan.LostLeases)
	assert.Equal(t, []string{repairCommandBackend, retirementCommandTarget}, plan.TopologyBefore)
	assert.Equal(t, []string{repairCommandBackend}, plan.TopologyAfter)
	assert.False(t, plan.RecordlessUnproven, "a current admission baseline proves every live lease has a row")
	assert.Equal(t, "no_identity", plan.TargetProbe)
	assert.Equal(t, placement.LostBackendAttestationText, plan.AttestLost)
	assert.NotEmpty(t, plan.Confirm)

	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, after, "the dry run must not change the database")
}

func TestRun_RetireLostBackendRefusesATargetServingItsPinnedStorage(t *testing.T) {
	dbPath, configPath := newRetirementCommandFixture(t, repairInventoryHandlerForBackend(
		t, retirementCommandTarget, []backend.ProvisionInfo{}, []backend.RetainedLease{}))
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	err = run(t.Context(), retirementArgs(configPath), &bytes.Buffer{}, &bytes.Buffer{})
	require.ErrorIs(t, err, placementprobe.ErrRetirementTargetAlive)

	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

func TestRun_RetireLostBackendRefusesATargetAddressServingAnotherBackend(t *testing.T) {
	dbPath, configPath := newRetirementCommandFixture(t, repairInventoryHandlerForBackend(
		t, repairCommandBackend, []backend.ProvisionInfo{}, []backend.RetainedLease{}))
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	err = run(t.Context(), retirementArgs(configPath), &bytes.Buffer{}, &bytes.Buffer{})
	require.ErrorIs(t, err, placementprobe.ErrRetirementTargetMisrouted,
		"an address that reaches a survivor never probed the backend being retired")

	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

// joinAndRemoveRemovedBackend adds backend-z with two concrete empty
// inventories and removes it again through the production topology API, so
// its storage pin remains only as history.
func joinAndRemoveRemovedBackend(t *testing.T) func(*placement.Store) {
	t.Helper()
	observe := func(backendName string) placement.CompleteBackendObservation {
		observation, err := placement.NewCompleteBackendObservation(
			repairBackendStorageID(t, backendName), []backend.ProvisionInfo{}, []backend.RetainedLease{},
		)
		require.NoError(t, err)
		return observation
	}
	return func(store *placement.Store) {
		require.NoError(t, store.ConfigureBackendTopologyWithCompleteObservations(
			[]string{repairCommandBackend, retirementCommandTarget, removedRepairBackend},
			map[string]placement.CompleteBackendObservation{
				repairCommandBackend:    observe(repairCommandBackend),
				retirementCommandTarget: observe(retirementCommandTarget),
				removedRepairBackend:    observe(removedRepairBackend),
			},
		))
		require.NoError(t, store.ConfigureBackendTopologyWithCompleteObservations(
			[]string{repairCommandBackend, retirementCommandTarget},
			map[string]placement.CompleteBackendObservation{
				repairCommandBackend:    observe(repairCommandBackend),
				retirementCommandTarget: observe(retirementCommandTarget),
			},
		))
	}
}

// TestRun_RetireLostBackendRefusesATargetAddressServingARemovedBackend covers
// an address that reaches a backend which has left the topology. Its storage
// pin is history only, and the probe must still recognize it instead of
// reading it as a rebuilt host's new storage and retiring backend-b.
func TestRun_RetireLostBackendRefusesATargetAddressServingARemovedBackend(t *testing.T) {
	dbPath, configPath := newRetirementCommandFixtureAfter(t, repairInventoryHandlerForBackend(
		t, removedRepairBackend, []backend.ProvisionInfo{}, []backend.RetainedLease{}),
		joinAndRemoveRemovedBackend(t),
	)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	err = run(t.Context(), retirementArgs(configPath), &bytes.Buffer{}, &bytes.Buffer{})
	require.ErrorIs(t, err, placementprobe.ErrRetirementTargetMisrouted,
		"an address that reaches the removed backend-z never probed backend-b")
	backupPath := filepath.Join(t.TempDir(), "pre-retirement.db")
	err = run(t.Context(), retirementArgs(configPath,
		"-apply", "-backup", backupPath, "-confirm", "unused",
		"-attest-lost", placement.LostBackendAttestationText,
	), &bytes.Buffer{}, &bytes.Buffer{})
	require.ErrorIs(t, err, placementprobe.ErrRetirementTargetMisrouted)
	assert.NoFileExists(t, backupPath, "the refusal comes before any backup")

	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

func TestRun_RetireLostBackendApplyRequiresExactConfirmationAndAttestation(t *testing.T) {
	dbPath, configPath := newRetirementCommandFixture(t, nil)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	plan := planRetirementCommand(t, configPath)
	backupPath := filepath.Join(t.TempDir(), "pre-retirement.db")

	for name, test := range map[string]struct {
		extra []string
		want  string
	}{
		"missing confirm":     {[]string{"-attest-lost", plan.AttestLost}, "-confirm must exactly equal"},
		"mismatched confirm":  {[]string{"-confirm", plan.Confirm + "0", "-attest-lost", plan.AttestLost}, "-confirm must exactly equal"},
		"missing attestation": {[]string{"-confirm", plan.Confirm}, "-attest-lost must exactly equal"},
		"inexact attestation": {[]string{"-confirm", plan.Confirm, "-attest-lost", "yes"}, "-attest-lost must exactly equal"},
	} {
		args := retirementArgs(configPath, append([]string{"-apply", "-backup", backupPath}, test.extra...)...)
		err := run(t.Context(), args, &bytes.Buffer{}, &bytes.Buffer{})
		require.ErrorContains(t, err, test.want, name)
		assert.NoFileExists(t, backupPath, "%s: no backup before an exact confirmation and attestation", name)
	}
	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

func TestRun_RetireLostBackendApplyRecordsTheLeaseLostOnce(t *testing.T) {
	dbPath, configPath := newRetirementCommandFixture(t, nil)
	original, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	plan := planRetirementCommand(t, configPath)
	backupPath := filepath.Join(t.TempDir(), "pre-retirement.db")

	var stdout bytes.Buffer
	require.NoError(t, run(t.Context(), retirementArgs(configPath,
		"-apply", "-backup", backupPath, "-confirm", plan.Confirm, "-attest-lost", plan.AttestLost,
	), &stdout, &bytes.Buffer{}))
	assert.Contains(t, stdout.String(), `PASS: retired backend "backend-b"`)
	assert.Contains(t, stdout.String(), `remove "backend-b" from the providerd config`)
	backup, err := os.ReadFile(backupPath)
	require.NoError(t, err)
	assert.Equal(t, original, backup, "the exact rollback image is the pre-retirement database")

	inspector, err := placement.OpenRepairInspector(dbPath, repairCommandProviderUUID)
	require.NoError(t, err)
	record, found, err := inspector.Inspect(repairCommandLease)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "lost", record.State)
	assert.Equal(t, retirementCommandTarget, record.LostBackend)
	assert.Empty(t, record.Backend)
	require.NoError(t, inspector.Close())

	// The old config still names the retired backend, so every mode refuses it.
	err = run(t.Context(), retirementArgs(configPath), &bytes.Buffer{}, &bytes.Buffer{})
	require.ErrorContains(t, err, "does not exactly match durable topology")
}

func TestRun_RetireLostBackendClassifiesTheLostRowAsCurrent(t *testing.T) {
	dbPath, configPath := newRetirementCommandFixture(t, nil)
	plan := planRetirementCommand(t, configPath)
	require.NoError(t, run(t.Context(), retirementArgs(configPath,
		"-apply", "-backup", filepath.Join(t.TempDir(), "pre-retirement.db"),
		"-confirm", plan.Confirm, "-attest-lost", plan.AttestLost,
	), &bytes.Buffer{}, &bytes.Buffer{}))

	survivorConfig := writeRepairConfig(t, dbPath, "http://127.0.0.1:1", repairCommandBackend)
	var stdout bytes.Buffer
	require.NoError(t, run(t.Context(), []string{"-config", survivorConfig, "-classify"}, &stdout, &bytes.Buffer{}))
	var report placement.AuthorityReport
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &report))
	assert.Equal(t, placement.AuthorityPreparedCurrent, report.Classification)
	assert.Empty(t, report.Diagnostics, "a retirement leaves no finding behind")
	assert.Equal(t, 1, report.Counts.LostPlacementRows)
	assert.Zero(t, report.Counts.UnusablePlacementRows)
}

func TestRun_RetireLostBackendValidatesItsFlags(t *testing.T) {
	_, configPath := newRetirementCommandFixture(t, nil)
	for name, test := range map[string]struct {
		args []string
		want string
	}{
		"storage-id outside the mode": {
			[]string{"-config", configPath, "-storage-id", "6ba7b811-9dad-41d1-80b4-00c04fd430c8"},
			"require -retire-lost-backend",
		},
		"attest-lost without apply": {
			retirementArgs(configPath, "-attest-lost", placement.LostBackendAttestationText),
			"-attest-lost requires -apply",
		},
		"missing storage-id": {
			[]string{"-config", configPath, "-retire-lost-backend", "-backend", retirementCommandTarget},
			"requires -backend and -storage-id",
		},
		"lease selector": {
			retirementArgs(configPath, "-lease", repairCommandLease),
			"cannot be combined with -lease",
		},
		"other mode": {
			retirementArgs(configPath, "-classify"),
			"mutually exclusive",
		},
		"timeout": {
			retirementArgs(configPath, "-timeout", "1ns"),
			"does not accept -timeout",
		},
		"wrong pin": {
			[]string{
				"-config", configPath, "-retire-lost-backend", "-backend", retirementCommandTarget,
				"-storage-id", repairBackendStorageID(t, repairCommandBackend).String(),
			},
			"is not the pin for backend",
		},
	} {
		err := run(t.Context(), test.args, &bytes.Buffer{}, &bytes.Buffer{})
		require.ErrorContains(t, err, test.want, name)
	}
}

func TestRun_RetireLostBackendReopenedSemanticFailureIsCommitted(t *testing.T) {
	_, configPath := newRetirementCommandFixture(t, nil)
	plan := planRetirementCommand(t, configPath)
	dependencies := defaultCommandDependencies()
	openInspector := dependencies.openPostconditionInspector
	cause := errors.New("synthetic reopened semantic failure")
	dependencies.openPostconditionInspector = func(
		path, providerUUID string,
	) (repairPostconditionInspector, error) {
		inspector, err := openInspector(path, providerUUID)
		if err != nil {
			return nil, err
		}
		return &semanticFailingRepairInspector{repairPostconditionInspector: inspector, cause: cause}, nil
	}
	err := runWithDependencies(t.Context(), retirementArgs(configPath,
		"-apply", "-backup", filepath.Join(t.TempDir(), "pre-retirement.db"),
		"-confirm", plan.Confirm, "-attest-lost", plan.AttestLost,
	), &bytes.Buffer{}, &bytes.Buffer{}, dependencies)
	require.ErrorIs(t, err, errRepairCommitted)
	require.ErrorIs(t, err, cause)
	require.ErrorContains(t, err, "reopened database semantic verification")
}
