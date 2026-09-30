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

	"github.com/manifest-network/fred/internal/provisioner/placement"
)

type restoredBackupPlanOutput struct {
	placement.RestoredBackupFacts
	Confirm string `json:"confirm"`
}

// newRestoredBackupCommandFixture returns a stopped database that carries an
// admission baseline and a config naming a backend that must never be
// contacted: attestation reads no inventory.
func newRestoredBackupCommandFixture(t *testing.T) (dbPath, configPath string) {
	t.Helper()
	dbPath = filepath.Join(t.TempDir(), "placements.db")
	store := initializeRepairPlacementStore(t, dbPath, []string{repairCommandBackend})
	require.True(t, store.CurrentAdmissionBaseline().Valid())
	require.NoError(t, store.Close())
	server := newVerifiedInventoryServer(t, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		t.Errorf("restored-backup attestation contacted backend inventory %s", r.URL.Path)
	}))
	return dbPath, writeRepairConfig(t, dbPath, server.URL, repairCommandBackend)
}

func planRestoredBackupCommand(t *testing.T, configPath string) restoredBackupPlanOutput {
	t.Helper()
	var stdout bytes.Buffer
	require.NoError(t, run(t.Context(), []string{
		"-config", configPath, "-attest-restored-backup",
	}, &stdout, &bytes.Buffer{}))
	var output restoredBackupPlanOutput
	decoder := json.NewDecoder(&stdout)
	decoder.DisallowUnknownFields()
	require.NoError(t, decoder.Decode(&output))
	require.False(t, decoder.More(), "the plan is exactly one JSON object")
	return output
}

func TestRun_AttestRestoredBackupDryRunPrintsTheBoundPlanWithoutMutating(t *testing.T) {
	dbPath, configPath := newRestoredBackupCommandFixture(t)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	plan := planRestoredBackupCommand(t, configPath)
	assert.True(t, plan.Required)
	assert.Equal(t, repairCommandProviderUUID, plan.ProviderUUID)
	assert.Equal(t, []string{repairCommandBackend}, plan.Topology)
	assert.Equal(t, repairBackendStorageID(t, repairCommandBackend).String(),
		plan.StorageIDs[repairCommandBackend])
	assert.NotZero(t, plan.BaselineTopologyID)
	assert.NotEmpty(t, plan.Confirm)

	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, after, "the dry run must not change the database")
}

func TestRun_AttestRestoredBackupApplyRequiresExactConfirmation(t *testing.T) {
	dbPath, configPath := newRestoredBackupCommandFixture(t)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	plan := planRestoredBackupCommand(t, configPath)
	backupPath := filepath.Join(t.TempDir(), "pre-attestation.db")

	for name, args := range map[string][]string{
		"missing":  {"-apply", "-backup", backupPath},
		"mismatch": {"-apply", "-backup", backupPath, "-confirm", plan.Confirm + "0"},
	} {
		err := run(t.Context(), append([]string{
			"-config", configPath, "-attest-restored-backup",
		}, args...), &bytes.Buffer{}, &bytes.Buffer{})
		require.ErrorContains(t, err, "-confirm must exactly equal", name)
		assert.NoFileExists(t, backupPath, "%s: no backup before an exact confirmation", name)
	}
	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

func TestRun_AttestRestoredBackupApplyClearsEvidenceOnce(t *testing.T) {
	dbPath, configPath := newRestoredBackupCommandFixture(t)
	restored, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	plan := planRestoredBackupCommand(t, configPath)
	backupPath := filepath.Join(t.TempDir(), "pre-attestation.db")

	var stdout bytes.Buffer
	require.NoError(t, run(t.Context(), []string{
		"-config", configPath, "-attest-restored-backup",
		"-apply", "-backup", backupPath, "-confirm", plan.Confirm,
	}, &stdout, &bytes.Buffer{}))
	assert.Contains(t, stdout.String(), "PASS: attested restored placement database")
	backup, err := os.ReadFile(backupPath)
	require.NoError(t, err)
	assert.Equal(t, restored, backup, "the exact rollback image is the restored copy")

	store, err := placement.OpenStore(dbPath, repairCommandProviderUUID)
	require.NoError(t, err)
	assert.False(t, store.CurrentAdmissionBaseline().Valid())
	require.NoError(t, store.Close())

	again := planRestoredBackupCommand(t, configPath)
	assert.False(t, again.Required)
	assert.Empty(t, again.Confirm, "nothing is left to confirm")
	secondBackup := filepath.Join(t.TempDir(), "second.db")
	err = run(t.Context(), []string{
		"-config", configPath, "-attest-restored-backup",
		"-apply", "-backup", secondBackup, "-confirm", plan.Confirm,
	}, &bytes.Buffer{}, &bytes.Buffer{})
	require.ErrorContains(t, err, "nothing to attest")
	assert.NoFileExists(t, secondBackup)
}

func TestRun_AttestRestoredBackupReopenedSemanticFailureIsCommitted(t *testing.T) {
	_, configPath := newRestoredBackupCommandFixture(t)
	plan := planRestoredBackupCommand(t, configPath)
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
	err := runWithDependencies(t.Context(), []string{
		"-config", configPath, "-attest-restored-backup",
		"-apply", "-backup", filepath.Join(t.TempDir(), "pre-attestation.db"), "-confirm", plan.Confirm,
	}, &bytes.Buffer{}, &bytes.Buffer{}, dependencies)
	require.ErrorIs(t, err, errRepairCommitted)
	require.ErrorIs(t, err, cause)
	require.ErrorContains(t, err, "reopened database semantic verification")
}

func TestRun_AttestRestoredBackupRejectsRecordSelectorsAndOtherModes(t *testing.T) {
	_, configPath := newRestoredBackupCommandFixture(t)
	const selectors = "cannot be combined with -lease, -backend, -operation-id, or -attest-drained"
	for name, test := range map[string]struct {
		extra []string
		want  string
	}{
		"lease":        {[]string{"-lease", repairCommandLease}, selectors},
		"backend":      {[]string{"-backend", repairCommandBackend}, selectors},
		"operation-id": {[]string{"-operation-id", repairCommandOwnerOperation}, selectors},
		"timeout":      {[]string{"-timeout", "5s"}, "does not accept -timeout"},
		"classify":     {[]string{"-classify"}, "mutually exclusive"},
		"resolve":      {[]string{"-resolve-conflict"}, "mutually exclusive"},
	} {
		err := run(t.Context(), append([]string{
			"-config", configPath, "-attest-restored-backup",
		}, test.extra...), &bytes.Buffer{}, &bytes.Buffer{})
		require.ErrorContains(t, err, test.want, name)
	}
}
