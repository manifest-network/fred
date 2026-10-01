package main

import (
	"bytes"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

const repairCommandNewGeneration = "6ba7b812-9dad-41d1-80b4-00c04fd430c8"

func generationCommandOwner(generation string) backend.ProvisionInfo {
	return backend.ProvisionInfo{
		LeaseUUID: repairCommandLease, BackendName: "backend-a",
		ProviderUUID: repairCommandProviderUUID, Tenant: "tenant-a",
		Status: backend.ProvisionStatusReady,
		LifecycleGeneration: &backend.LifecycleGenerationObservation{
			Kind: backend.LifecycleGenerationTyped,
			ID:   generation,
		},
	}
}

// createGenerationQuarantineCommandDatabase confirms the lease on backend-a at
// one generation, then reopens the store and projects an inventory in which
// backend-a reports another: the quarantine a restore from an older copy
// leaves on a lease re-provisioned since.
func createGenerationQuarantineCommandDatabase(t *testing.T) string {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	backends := []string{"backend-a", "backend-b"}
	projectGeneration := func(store *placement.Store, generation string) {
		projectRepairInventoryWithRows(t, newRepairReconciliation(t, store, backends), backends,
			placement.ReconciliationProjection{
				Placements: map[string]string{repairCommandLease: "backend-a"},
			},
			map[string][]backend.ProvisionInfo{"backend-a": {generationCommandOwner(generation)}},
		)
	}
	store := initializeRepairPlacementStore(t, dbPath, backends)
	projectGeneration(store, repairCommandOwnerOperation)
	require.True(t, store.CurrentLifecycle(repairCommandLease).Authorized())
	require.NoError(t, store.Close())

	store, err := placement.OpenStore(
		dbPath, repairCommandProviderUUID,
		placement.WithCallbackRouteFactory(repairCallbackRouteFactory(t)),
	)
	require.NoError(t, err)
	projectGeneration(store, repairCommandNewGeneration)
	require.Equal(t, placement.LifecycleVerdictUnusable, store.CurrentLifecycle(repairCommandLease).Verdict())
	require.NoError(t, store.Close())
	return dbPath
}

type generationCommandFixture struct {
	dbPath     string
	configPath string
	args       []string
}

func newGenerationCommandFixture(t *testing.T) generationCommandFixture {
	t.Helper()
	dbPath := createGenerationQuarantineCommandDatabase(t)
	owner := newRepairInventoryServer(t,
		[]backend.ProvisionInfo{generationCommandOwner(repairCommandNewGeneration)}, nil)
	t.Cleanup(owner.Close)
	other := newRepairInventoryServer(t, nil, nil, "backend-b")
	t.Cleanup(other.Close)
	configPath := writeRepairConfigURLs(t, dbPath, map[string]string{
		"backend-a": owner.URL,
		"backend-b": other.URL,
	})
	return generationCommandFixture{
		dbPath:     dbPath,
		configPath: configPath,
		args: []string{
			"-config", configPath,
			"-adopt-observed-generation",
			"-lease", repairCommandLease,
			"-backend", "backend-a",
			"-timeout", "5s",
		},
	}
}

var dryRunConfirmation = regexp.MustCompile(`-confirm "([^"]+)"`)

func (fixture generationCommandFixture) dryRun(t *testing.T) (string, string) {
	t.Helper()
	var stdout bytes.Buffer
	require.NoError(t, run(t.Context(), fixture.args, &stdout, &bytes.Buffer{}))
	match := dryRunConfirmation.FindStringSubmatch(stdout.String())
	require.Len(t, match, 2, stdout.String())
	return stdout.String(), match[1]
}

func generationFingerprint(t *testing.T, text string) string {
	t.Helper()
	id, err := lifecycle.ParseID(text)
	require.NoError(t, err)
	return id.Fingerprint()
}

func TestRun_AdoptObservedGenerationDryRunRevealsNoGeneration(t *testing.T) {
	fixture := newGenerationCommandFixture(t)
	before, err := os.ReadFile(fixture.dbPath)
	require.NoError(t, err)
	output, confirmation := fixture.dryRun(t)
	assert.Contains(t, output, "DRY RUN ONLY")
	assert.Contains(t, output, generationFingerprint(t, repairCommandOwnerOperation))
	assert.Contains(t, output, generationFingerprint(t, repairCommandNewGeneration))
	assert.Contains(t, output, placement.GenerationAttestationText)
	assert.NotContains(t, output, repairCommandOwnerOperation, "lifecycle IDs are callback capabilities")
	assert.NotContains(t, output, repairCommandNewGeneration)
	assert.NotContains(t, confirmation, repairCommandNewGeneration)
	after, err := os.ReadFile(fixture.dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, after, "a dry run changes nothing")
}

func TestRun_AdoptObservedGenerationRequiresBoundConfirmationAndAttestation(t *testing.T) {
	fixture := newGenerationCommandFixture(t)
	_, confirmation := fixture.dryRun(t)
	for name, test := range map[string]struct {
		extra []string
		want  string
	}{
		"wrong confirmation": {
			extra: []string{
				"-apply", "-backup", filepath.Join(t.TempDir(), "wrong-confirmation.bak"),
				"-confirm", "adopt-generation:wrong", "-attest-generation", placement.GenerationAttestationText,
			},
			want: "-confirm must exactly equal",
		},
		"wrong attestation": {
			extra: []string{
				"-apply", "-backup", filepath.Join(t.TempDir(), "wrong-attestation.bak"),
				"-confirm", confirmation, "-attest-generation", "it is fine",
			},
			want: "-attest-generation must exactly equal",
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := run(t.Context(), append(append([]string{}, fixture.args...), test.extra...),
				&bytes.Buffer{}, &bytes.Buffer{})
			require.ErrorContains(t, err, test.want)
		})
	}
	repair, err := placement.OpenAttemptRepair(fixture.dbPath, repairCommandProviderUUID)
	require.NoError(t, err)
	_, err = repair.MatchGenerationQuarantine(repairCommandLease, "backend-a")
	require.NoError(t, err, "refused applies leave the quarantine intact")
	require.NoError(t, repair.Close())
}

func TestRun_AdoptObservedGenerationApplies(t *testing.T) {
	fixture := newGenerationCommandFixture(t)
	_, confirmation := fixture.dryRun(t)
	before, err := os.ReadFile(fixture.dbPath)
	require.NoError(t, err)
	backupPath := filepath.Join(t.TempDir(), "placements.pre-adoption.bak")
	var stdout bytes.Buffer
	err = run(t.Context(), append(append([]string{}, fixture.args...),
		"-apply", "-backup", backupPath, "-confirm", confirmation,
		"-attest-generation", placement.GenerationAttestationText,
	), &stdout, &bytes.Buffer{})
	require.NoError(t, err)
	assert.Contains(t, stdout.String(), "PASS:")
	assert.NotContains(t, stdout.String(), repairCommandNewGeneration)
	backup, err := os.ReadFile(backupPath)
	require.NoError(t, err)
	assert.Equal(t, before, backup, "the backup is the exact pre-adoption database")

	reopened, err := placement.OpenStore(
		fixture.dbPath, repairCommandProviderUUID,
		placement.WithCallbackRouteFactory(repairCallbackRouteFactory(t)),
	)
	require.NoError(t, err)
	defer func() { _ = reopened.Close() }()
	authorization := reopened.CurrentLifecycle(repairCommandLease)
	require.True(t, authorization.Authorized())
	assert.Equal(t, generationFingerprint(t, repairCommandNewGeneration), authorization.ID().Fingerprint())
}

func TestRun_AdoptObservedGenerationRefusesAGenerationTheDryRunDidNotShow(t *testing.T) {
	dbPath := createGenerationQuarantineCommandDatabase(t)
	var reported atomic.Value
	reported.Store(repairCommandNewGeneration)
	owner := newVerifiedInventoryServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		owners := []backend.ProvisionInfo{generationCommandOwner(reported.Load().(string))}
		repairInventoryHandlerForBackend(t, "backend-a", owners, []backend.RetainedLease{}).ServeHTTP(w, r)
	}))
	t.Cleanup(owner.Close)
	other := newRepairInventoryServer(t, nil, nil, "backend-b")
	t.Cleanup(other.Close)
	fixture := generationCommandFixture{
		dbPath: dbPath,
		configPath: writeRepairConfigURLs(t, dbPath, map[string]string{
			"backend-a": owner.URL, "backend-b": other.URL,
		}),
	}
	fixture.args = []string{
		"-config", fixture.configPath, "-adopt-observed-generation",
		"-lease", repairCommandLease, "-backend", "backend-a", "-timeout", "5s",
	}
	_, confirmation := fixture.dryRun(t)
	reported.Store("6ba7b813-9dad-41d1-80b4-00c04fd430c8")
	err := run(t.Context(), append(append([]string{}, fixture.args...),
		"-apply", "-backup", filepath.Join(t.TempDir(), "changed.bak"), "-confirm", confirmation,
		"-attest-generation", placement.GenerationAttestationText,
	), &bytes.Buffer{}, &bytes.Buffer{})
	assert.ErrorContains(t, err, "-confirm must exactly equal",
		"the confirmation binds the generation the operator saw")
}

func TestRun_AdoptObservedGenerationFlagsAreExact(t *testing.T) {
	fixture := newGenerationCommandFixture(t)
	with := func(extra ...string) []string { return append(append([]string{}, fixture.args...), extra...) }
	for name, test := range map[string]struct {
		args []string
		want string
	}{
		"an operation ID": {
			args: with("-operation-id", repairCommandOperation),
			want: "-adopt-observed-generation does not accept -operation-id",
		},
		"a drain attestation": {
			args: with("-apply", "-backup", filepath.Join(t.TempDir(), "drained.bak"),
				"-attest-drained", drainedAttestation),
			want: "takes -attest-generation, not -attest-drained",
		},
		"an attestation without apply": {
			args: with("-attest-generation", placement.GenerationAttestationText),
			want: "-attest-generation requires -apply",
		},
		"another mode": {
			args: with("-resolve-conflict"),
			want: "are mutually exclusive",
		},
		"a generation attestation without the mode": {
			args: []string{
				"-config", fixture.configPath, "-resolve-conflict", "-lease", repairCommandLease,
				"-backend", "backend-a", "-attest-generation", placement.GenerationAttestationText,
			},
			want: "-attest-generation requires -adopt-observed-generation",
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.ErrorContains(t, run(t.Context(), test.args, &bytes.Buffer{}, &bytes.Buffer{}), test.want)
		})
	}
}
