package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

const (
	repairCommandLease          = "018f47a2-8b1c-7def-8123-456789abcdef"
	repairCommandProviderUUID   = "550e8400-e29b-41d4-a716-446655440000"
	repairCommandOperation      = "550e8400-e29b-41d4-a716-446655440000"
	repairCommandOwnerOperation = "6ba7b810-9dad-41d1-80b4-00c04fd430c8"
	repairCommandBackend        = "backend-a"
)

func repairBackendStorageID(t *testing.T, backendName string) backendidentity.ID {
	t.Helper()
	encoded := map[string]string{
		"backend-a": "550e8400-e29b-41d4-a716-446655440001",
		"backend-b": "6ba7b811-9dad-41d1-80b4-00c04fd430c8",
	}[backendName]
	id, err := backendidentity.Parse(encoded)
	require.NoError(t, err)
	return id
}

func repairBackendRequestSnapshot(t *testing.T) placement.BackendRequestSnapshot {
	t.Helper()
	snapshot, err := placement.NewBackendRequestSnapshot(
		"tenant-a",
		repairCommandProviderUUID,
		[]backend.LeaseItem{{SKU: "sku-a", Quantity: 1, ServiceName: "app"}},
	)
	require.NoError(t, err)
	return snapshot
}

func repairCallbackPair(
	t *testing.T,
	id operation.OperationID,
) placement.CallbackPair {
	t.Helper()
	pair, err := placement.NewCallbackPair(
		id,
		"https://provider.test/callbacks/provision?operation_id="+id.String(),
		"https://provider.test/callbacks/provision?lifecycle_id="+id.String(),
	)
	require.NoError(t, err)
	return pair
}

type repairFreshChainSnapshot struct{}

func (repairFreshChainSnapshot) Valid() bool             { return true }
func (repairFreshChainSnapshot) ProviderUUID() string    { return repairCommandProviderUUID }
func (repairFreshChainSnapshot) BlockHeight() int64      { return 1 }
func (repairFreshChainSnapshot) TotalLeases() int        { return 0 }
func (repairFreshChainSnapshot) BlockingLeaseCount() int { return 0 }

type repairLegacyChainSnapshot struct{ leaseUUIDs []string }

func (snapshot repairLegacyChainSnapshot) Valid() bool          { return true }
func (snapshot repairLegacyChainSnapshot) ProviderUUID() string { return repairCommandProviderUUID }
func (snapshot repairLegacyChainSnapshot) BlockHeight() int64   { return 2 }
func (snapshot repairLegacyChainSnapshot) TotalLeases() int     { return len(snapshot.leaseUUIDs) }
func (snapshot repairLegacyChainSnapshot) BlockingLeaseCount() int {
	return 0
}
func (snapshot repairLegacyChainSnapshot) LeaseUUIDs() []string {
	return slices.Clone(snapshot.leaseUUIDs)
}
func (snapshot repairLegacyChainSnapshot) LeaseItems() map[string][]backend.LeaseItem {
	items := make(map[string][]backend.LeaseItem, len(snapshot.leaseUUIDs))
	for _, leaseUUID := range snapshot.leaseUUIDs {
		items[leaseUUID] = []backend.LeaseItem{{
			SKU: "sku-test", Quantity: 1, ServiceName: "app",
		}}
	}
	return items
}

var errRepairVerdictWrite = errors.New("synthetic repair verdict write failure")

type semanticFailingRepairInspector struct {
	repairPostconditionInspector
	cause error
}

func (inspector *semanticFailingRepairInspector) VerifyRefusalPostcondition(
	placement.AttemptRepairCandidate,
	placement.AttemptRepairResult,
) error {
	return inspector.cause
}

func (inspector *semanticFailingRepairInspector) VerifyConflictResolutionPostcondition(
	placement.ConflictRepairCandidate,
	placement.ConflictRepairResult,
) error {
	return inspector.cause
}

type repairVerdictErrorWriter struct{}

func (repairVerdictErrorWriter) Write([]byte) (int, error) {
	return 0, errRepairVerdictWrite
}

type repairShortWriter struct{}

func (repairShortWriter) Write(value []byte) (int, error) {
	return max(0, len(value)-1), nil
}

func TestWriteCommandErrorCannotForgeVerdictOrTerminalOutput(t *testing.T) {
	var output bytes.Buffer
	writeCommandError(
		&output,
		errors.New("remote failure\nPASS: forged\r\x1b]8;;https://evil.invalid\x07link"),
	)

	rendered := output.String()
	assert.True(t, strings.HasPrefix(rendered, "placement-repair: ERROR \""))
	assert.Equal(t, 1, strings.Count(rendered, "\n"),
		"an error must occupy exactly one physical terminal line")
	assert.NotContains(t, rendered, "\nPASS: forged")
	assert.NotContains(t, rendered, "\r")
	assert.NotContains(t, rendered, "\x1b")
	assert.NotContains(t, rendered, "\x07")
	assert.Contains(t, rendered, `\nPASS: forged\r\u001b`)
}

func TestRun_FlagParseErrorDoesNotWriteUnstructuredInput(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run(
		t.Context(),
		[]string{"-timeout", "bad\nPASS: forged\x1b[31m"},
		&stdout,
		&stderr,
	)
	require.Error(t, err)
	assert.Empty(t, stdout.String())
	assert.Empty(t, stderr.String(),
		"main alone must render the returned parse error through writeCommandError")
}

func TestRun_HelpWritesUsageAndSucceeds(t *testing.T) {
	for _, helpFlag := range []string{"-h", "--help"} {
		t.Run(helpFlag, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := run(t.Context(), []string{helpFlag}, &stdout, &stderr)

			require.NoError(t, err)
			assert.Empty(t, stdout.String())
			assert.Contains(t, stderr.String(), "Usage of placement-repair:")
			assert.Contains(t, stderr.String(), "-config")
			assert.NotContains(t, stderr.String(), "placement-repair: ERROR")
		})
	}
}

func TestRun_ClassifyEmitsMachineReadableSchemaNeutralVerdict(t *testing.T) {
	dbPath := writeRepairLegacyAuthorityDB(t)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	configPath := writeRepairConfig(
		t, dbPath, "http://127.0.0.1:1", repairCommandBackend,
	)

	var stdout bytes.Buffer
	err = run(t.Context(), []string{"-config", configPath, "-classify"}, &stdout, &bytes.Buffer{})
	require.NoError(t, err)
	assert.NotContains(t, stdout.String(), "PASS")
	var report placement.AuthorityReport
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &report))
	assert.Equal(t, placement.AuthorityPristineV013, report.Classification)
	assert.Equal(t, []string{repairCommandBackend}, report.ExpectedBackendTopology)
	require.Len(t, report.Rows, 1)
	assert.False(t, report.Rows[0].UntrustedPositive)
	assert.Contains(t, stdout.String(), `"untrusted_positive":false`,
		"legacy authority output must explicitly distinguish an ordinary row from quarantine")

	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, after, "classification must not rewrite the stopped authority")
}

func TestRun_ClassifyExposesPersistedUntrustedPositiveQuarantine(t *testing.T) {
	dbPath := createUntrustedPositiveRepairCommandDatabase(t)
	configPath := writeRepairConfig(
		t, dbPath, "http://127.0.0.1:1", repairCommandBackend,
	)

	var stdout bytes.Buffer
	err := run(
		t.Context(), []string{"-config", configPath, "-classify"},
		&stdout, &bytes.Buffer{},
	)
	require.NoError(t, err)
	var report placement.AuthorityReport
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &report))
	require.Len(t, report.Rows, 1)
	assert.True(t, report.Rows[0].UntrustedPositive)
	assert.Contains(t, stdout.String(), `"untrusted_positive":true`)
}

func TestRun_ClassifyUnsafeAuthorityStillEmitsJSONAndFails(t *testing.T) {
	dbPath := writeRepairLegacyAuthorityDB(t)
	db, err := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		_, createErr := tx.CreateBucket([]byte("placement_lifecycle_capabilities"))
		return createErr
	}))
	require.NoError(t, db.Close())
	configPath := writeRepairConfig(
		t, dbPath, "http://127.0.0.1:1", repairCommandBackend,
	)

	var stdout bytes.Buffer
	err = run(t.Context(), []string{"-config", configPath, "-classify"}, &stdout, &bytes.Buffer{})
	require.ErrorContains(t, err, string(placement.AuthorityMixedOrIncomplete))
	assert.NotContains(t, stdout.String(), "PASS")
	var report placement.AuthorityReport
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &report))
	assert.Equal(t, placement.AuthorityMixedOrIncomplete, report.Classification)
}

func TestRun_ClassifyPreparedCurrentAuthoritiesIsDeterministicAndByteExact(t *testing.T) {
	tests := []struct {
		name   string
		create func(*testing.T) string
	}{
		{
			name: "prepared v0.13 authority",
			create: func(t *testing.T) string {
				return prepareRepairLegacyAuthority(t)
			},
		},
		{
			name: "freshly initialized authority",
			create: func(t *testing.T) string {
				dbPath := filepath.Join(t.TempDir(), "placements.db")
				store := initializeRepairPlacementStore(
					t, dbPath, []string{repairCommandBackend},
				)
				require.NoError(t, store.Close())
				return dbPath
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dbPath := test.create(t)
			before, err := os.ReadFile(dbPath)
			require.NoError(t, err)
			configPath := writeRepairConfig(
				t, dbPath, "http://127.0.0.1:1", repairCommandBackend,
			)

			var first, second bytes.Buffer
			for _, output := range []*bytes.Buffer{&first, &second} {
				err = run(
					t.Context(),
					[]string{"-config", configPath, "-classify"},
					output,
					&bytes.Buffer{},
				)
				require.NoError(t, err)
			}
			assert.Equal(t, first.Bytes(), second.Bytes())
			assert.LessOrEqual(t, first.Len(), placement.MaxAuthorityReportBytes)
			var report placement.AuthorityReport
			require.NoError(t, json.Unmarshal(first.Bytes(), &report))
			assert.Equal(t, placement.AuthorityPreparedCurrent, report.Classification)
			assert.True(t, report.SafeForCutover())

			after, readErr := os.ReadFile(dbPath)
			require.NoError(t, readErr)
			assert.Equal(t, before, after,
				"read-only classification must preserve the authority byte-for-byte")
		})
	}
}

func TestRun_ClassifyCancellationCannotEmitSafeSuccess(t *testing.T) {
	dbPath := writeRepairLegacyAuthorityDB(t)
	configPath := writeRepairConfig(
		t, dbPath, "http://127.0.0.1:1", repairCommandBackend,
	)
	ctx, cancel := context.WithCancel(t.Context())
	dependencies := defaultCommandDependencies()
	dependencies.inspectAuthorityFile = func(
		string,
		placement.AuthorityExpectation,
	) (placement.AuthorityReport, error) {
		cancel()
		return placement.AuthorityReport{
			Classification: placement.AuthorityPristineV013,
		}, nil
	}

	var stdout bytes.Buffer
	err := runWithDependencies(
		ctx,
		[]string{"-config", configPath, "-classify"},
		&stdout,
		&bytes.Buffer{},
		dependencies,
	)
	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, stdout.String(), "cancellation must suppress a stale safe report")
}

func TestRunAuthorityClassificationRejectsShortReportWrite(t *testing.T) {
	expectation, err := placement.NewAuthorityExpectation(
		repairCommandProviderUUID, []string{repairCommandBackend},
	)
	require.NoError(t, err)
	err = runAuthorityClassification(
		t.Context(),
		"/unused/placement.db",
		expectation,
		repairShortWriter{},
		func(string, placement.AuthorityExpectation) (placement.AuthorityReport, error) {
			return placement.AuthorityReport{
				Classification: placement.AuthorityPristineV013,
			}, nil
		},
	)
	require.ErrorIs(t, err, io.ErrShortWrite)
}

func TestRun_ClassifyIsMutuallyExclusiveAndRejectsRecordSelectors(t *testing.T) {
	dbPath := writeRepairLegacyAuthorityDB(t)
	configPath := writeRepairConfig(
		t, dbPath, "http://127.0.0.1:1", repairCommandBackend,
	)

	t.Run("other mode", func(t *testing.T) {
		var stdout bytes.Buffer
		err := run(t.Context(), []string{
			"-config", configPath, "-classify", "-list",
		}, &stdout, &bytes.Buffer{})
		require.ErrorContains(t, err, "mutually exclusive")
		assert.Empty(t, stdout.String())
	})

	t.Run("record selector", func(t *testing.T) {
		var stdout bytes.Buffer
		err := run(t.Context(), []string{
			"-config", configPath, "-classify", "-lease", repairCommandLease,
		}, &stdout, &bytes.Buffer{})
		require.ErrorContains(t, err, "cannot be combined with record or mutation flags")
		assert.Empty(t, stdout.String())
	})

	for _, test := range []struct {
		name string
		args []string
		want string
	}{
		{
			name: "classify explicit timeout",
			args: []string{"-config", configPath, "-classify", "-timeout", "5s"},
			want: "read-only -classify does not accept -timeout",
		},
		{
			name: "list explicit timeout",
			args: []string{"-config", configPath, "-list", "-timeout", "5s"},
			want: "read-only -list/-inspect does not accept -timeout",
		},
		{
			name: "inspect explicit timeout",
			args: []string{
				"-config", configPath, "-inspect", "-lease", repairCommandLease,
				"-timeout", "5s",
			},
			want: "read-only -list/-inspect does not accept -timeout",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var stdout bytes.Buffer
			err := run(t.Context(), test.args, &stdout, &bytes.Buffer{})
			require.ErrorContains(t, err, test.want)
			assert.Empty(t, stdout.String())
		})
	}
}

func writeRepairLegacyAuthorityDB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "placements.v013.db")
	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		placements, createErr := tx.CreateBucket([]byte("placements"))
		if createErr != nil {
			return createErr
		}
		return placements.Put(
			[]byte(repairCommandLease),
			[]byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
		)
	}))
	require.NoError(t, db.Close())
	return path
}

func prepareRepairLegacyAuthority(t *testing.T) string {
	t.Helper()
	dbPath := writeRepairLegacyAuthorityDB(t)
	backupPath := filepath.Join(t.TempDir(), "placements.v013.bak")
	inventories := map[string]placement.BackendInventory{
		repairCommandBackend: {
			StorageIdentity: repairBackendStorageID(t, repairCommandBackend),
			Provisions:      []string{repairCommandLease},
			ProvisionProviderUUIDs: map[string]string{
				repairCommandLease: "",
			},
			ProvisionItems: map[string][]backend.LeaseItem{
				repairCommandLease: {{SKU: "sku-test", Quantity: 1, ServiceName: "app"}},
			},
			Retentions: []string{},
		},
	}
	chainProof, err := placement.NewLegacyUpgradeChainProof(repairLegacyChainSnapshot{
		leaseUUIDs: []string{repairCommandLease},
	})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	preparer, err := placement.OpenLegacyUpgradePreparer(dbPath)
	require.NoError(t, err)
	backupTarget, err := placement.BindExactBackupTarget(backupPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backupTarget.Close()) })
	capability, err := preparer.AuthorizePreparation(
		ctx,
		repairCommandProviderUUID,
		[]string{repairCommandBackend},
		inventories,
		chainProof,
		backupTarget,
		placement.LegacyPreparationDrainAttestation,
	)
	require.NoError(t, err)
	_, err = preparer.PrepareContext(
		ctx,
		repairCommandProviderUUID,
		[]string{repairCommandBackend},
		inventories,
		chainProof,
		capability,
	)
	require.NoError(t, err)
	require.NoError(t, preparer.Close())
	return dbPath
}

func TestRun_DryRunClosesWithoutChangingDatabaseBytes(t *testing.T) {
	dbPath := createRepairCommandDatabase(t, false)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	server := newRepairInventoryServer(t, nil, nil)
	defer server.Close()
	configPath := writeRepairConfig(t, dbPath, server.URL, repairCommandBackend)

	var stdout bytes.Buffer
	err = run(t.Context(), repairArgs(configPath), &stdout, &bytes.Buffer{})
	require.NoError(t, err)
	assert.Contains(t, stdout.String(), "DRY RUN ONLY")
	assert.NotContains(t, stdout.String(), "PASS:")
	assert.Contains(t, stdout.String(), `on backend "backend-a"`)
	assert.Contains(t, stdout.String(), repairConfirmation())
	assert.Contains(t, stdout.String(), "Inventory evidence does not prove")

	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, after, "default dry-run must not initialize, migrate, or rewrite the database")
}

func TestRun_ApplyRequiresExactConfirmationAndDrainAttestation(t *testing.T) {
	dbPath := createRepairCommandDatabase(t, false)
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("invalid operator confirmation must fail before any backend probe")
	}))
	defer server.Close()
	configPath := writeRepairConfig(t, dbPath, server.URL, repairCommandBackend)

	tests := []struct {
		name        string
		confirm     string
		attestation string
		want        string
	}{
		{
			name:        "wrong tuple confirmation",
			confirm:     "refuse-attempt:wrong",
			attestation: drainedAttestation,
			want:        "-confirm must exactly equal",
		},
		{
			name:        "wrong drain attestation",
			confirm:     repairConfirmation(),
			attestation: "I think it is drained",
			want:        "-attest-drained must exactly equal",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backupPath := filepath.Join(t.TempDir(), "placements.pre-repair.bak")
			args := append(repairArgs(configPath),
				"-apply", "-backup", backupPath,
				"-confirm", test.confirm, "-attest-drained", test.attestation,
			)
			var stdout bytes.Buffer
			err := run(t.Context(), args, &stdout, &bytes.Buffer{})
			require.ErrorContains(t, err, test.want)
			assert.Empty(t, stdout.String())
		})
	}

	repair, err := placement.OpenAttemptRepair(dbPath, repairCommandProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	operationID, err := operation.ParseID(repairCommandOperation)
	require.NoError(t, err)
	_, err = repair.MatchAttempt(repairCommandLease, repairCommandBackend, operationID)
	require.NoError(t, err, "failed confirmation must leave the exact attempt intact")
}

func TestRun_ApplyRequiresNewExactBackupBeforeMutation(t *testing.T) {
	t.Run("missing backup", func(t *testing.T) {
		dbPath := createRepairCommandDatabase(t, false)
		before, err := os.ReadFile(dbPath)
		require.NoError(t, err)
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Fatal("missing mandatory backup must fail before any backend probe")
		}))
		defer server.Close()
		configPath := writeRepairConfig(t, dbPath, server.URL, repairCommandBackend)

		args := append(repairArgs(configPath),
			"-apply", "-confirm", repairConfirmation(),
			"-attest-drained", drainedAttestation,
		)
		err = run(t.Context(), args, &bytes.Buffer{}, &bytes.Buffer{})
		require.ErrorContains(t, err, "-backup is required with -apply")
		after, readErr := os.ReadFile(dbPath)
		require.NoError(t, readErr)
		assert.Equal(t, before, after)
	})

	t.Run("preexisting backup", func(t *testing.T) {
		dbPath := createRepairCommandDatabase(t, false)
		before, err := os.ReadFile(dbPath)
		require.NoError(t, err)
		server := newRepairInventoryServer(t, nil, nil)
		defer server.Close()
		configPath := writeRepairConfig(t, dbPath, server.URL, repairCommandBackend)
		backupPath := filepath.Join(t.TempDir(), "placements.pre-repair.bak")
		const sentinel = "existing operator artifact"
		require.NoError(t, os.WriteFile(backupPath, []byte(sentinel), 0o600))

		args := append(repairArgs(configPath),
			"-apply", "-backup", backupPath,
			"-confirm", repairConfirmation(),
			"-attest-drained", drainedAttestation,
		)
		var stdout bytes.Buffer
		err = run(t.Context(), args, &stdout, &bytes.Buffer{})
		require.ErrorContains(t, err, "backup destination already exists")
		assert.False(t, errors.Is(err, errRepairCommitted))
		assert.Empty(t, stdout.String())
		after, readErr := os.ReadFile(dbPath)
		require.NoError(t, readErr)
		assert.Equal(t, before, after,
			"a no-overwrite refusal must leave placement authority byte-identical")
		backup, readErr := os.ReadFile(backupPath)
		require.NoError(t, readErr)
		assert.Equal(t, sentinel, string(backup))
	})
}

func TestRun_ExpiredEvidenceAfterBackupNeverMutates(t *testing.T) {
	t.Run("attempt refusal", func(t *testing.T) {
		dbPath := createRepairCommandDatabase(t, false)
		before, err := os.ReadFile(dbPath)
		require.NoError(t, err)
		server := newRepairInventoryServer(t, nil, nil)
		defer server.Close()
		configPath := writeRepairConfig(t, dbPath, server.URL, repairCommandBackend)
		backupPath := filepath.Join(t.TempDir(), "placements.expired-proof.bak")
		args := append(repairArgs(configPath),
			"-apply", "-backup", backupPath,
			"-confirm", repairConfirmation(),
			"-attest-drained", drainedAttestation,
		)
		ctx, cancel := context.WithCancel(t.Context())
		dependencies := defaultCommandDependencies()
		dependencies.createExactBackup = func(
			repair *placement.AttemptRepair,
			target *placement.ExactBackupTarget,
		) error {
			backupErr := repair.CreateExactBackup(target)
			cancel()
			return backupErr
		}

		var stdout bytes.Buffer
		err = runWithDependencies(ctx, args, &stdout, &bytes.Buffer{}, dependencies)
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorContains(t, err, "BACKUP PUBLISHED:")
		require.ErrorContains(t, err, "no repair mutation committed")
		require.ErrorContains(t, err, "rerun dry-run, and choose a new -backup path")
		assert.False(t, errors.Is(err, errRepairCommitted))
		assert.Empty(t, stdout.String())
		assertExpiredProofBackupAndSourceBytes(t, dbPath, backupPath, before)

		repair, openErr := placement.OpenAttemptRepair(dbPath, repairCommandProviderUUID)
		require.NoError(t, openErr)
		operationID, parseErr := operation.ParseID(repairCommandOperation)
		require.NoError(t, parseErr)
		_, matchErr := repair.MatchAttempt(
			repairCommandLease, repairCommandBackend, operationID,
		)
		require.NoError(t, matchErr,
			"expired evidence after backup must leave the exact attempt intact")
		require.NoError(t, repair.Close())
	})

	t.Run("conflict resolution", func(t *testing.T) {
		dbPath := createConflictRepairCommandDatabase(t)
		before, err := os.ReadFile(dbPath)
		require.NoError(t, err)
		owner := backend.ProvisionInfo{
			LeaseUUID:    repairCommandLease,
			ProviderUUID: repairCommandProviderUUID,
			Tenant:       "tenant-a",
			LifecycleGeneration: &backend.LifecycleGenerationObservation{
				Kind: backend.LifecycleGenerationTyped,
				ID:   repairCommandOwnerOperation,
			},
		}
		selected := newRepairInventoryServer(t, []backend.ProvisionInfo{owner}, nil)
		defer selected.Close()
		other := newRepairInventoryServer(t, nil, nil, "backend-b")
		defer other.Close()
		configPath := writeRepairConfigURLs(t, dbPath, map[string]string{
			"backend-a": selected.URL,
			"backend-b": other.URL,
		})
		backupPath := filepath.Join(t.TempDir(), "placements.expired-conflict-proof.bak")
		args := []string{
			"-config", configPath,
			"-resolve-conflict",
			"-lease", repairCommandLease,
			"-backend", "backend-a",
			"-timeout", "5s",
			"-apply", "-backup", backupPath,
			"-confirm", conflictRepairConfirmation(t, dbPath, owner),
			"-attest-drained", drainedAttestation,
		}
		ctx, cancel := context.WithCancel(t.Context())
		dependencies := defaultCommandDependencies()
		dependencies.createExactBackup = func(
			repair *placement.AttemptRepair,
			target *placement.ExactBackupTarget,
		) error {
			backupErr := repair.CreateExactBackup(target)
			cancel()
			return backupErr
		}

		var stdout bytes.Buffer
		err = runWithDependencies(ctx, args, &stdout, &bytes.Buffer{}, dependencies)
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorContains(t, err, "BACKUP PUBLISHED:")
		require.ErrorContains(t, err, "no repair mutation committed")
		require.ErrorContains(t, err, "rerun dry-run, and choose a new -backup path")
		assert.False(t, errors.Is(err, errRepairCommitted))
		assert.Empty(t, stdout.String())
		assertExpiredProofBackupAndSourceBytes(t, dbPath, backupPath, before)

		repair, openErr := placement.OpenAttemptRepair(dbPath, repairCommandProviderUUID)
		require.NoError(t, openErr)
		_, matchErr := repair.MatchConflict(repairCommandLease, "backend-a")
		require.NoError(t, matchErr,
			"expired evidence after backup must leave the exact conflict intact")
		require.NoError(t, repair.Close())
	})
}

func assertExpiredProofBackupAndSourceBytes(
	t *testing.T,
	dbPath string,
	backupPath string,
	want []byte,
) {
	t.Helper()
	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, want, after,
		"expired evidence must leave placement authority byte-identical")
	backup, err := os.ReadFile(backupPath)
	require.NoError(t, err)
	assert.Equal(t, want, backup,
		"the published backup remains an exact artifact even though it cannot authorize mutation")
}

func TestRun_RequiresExactConfiguredAndDurableBackendTopology(t *testing.T) {
	dbPath := createRepairCommandDatabase(t, false)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("topology mismatch must fail before any backend probe")
	}))
	defer server.Close()
	configPath := writeRepairConfig(t, dbPath, server.URL,
		repairCommandBackend, "backend-omitted-from-durable-topology")

	var stdout bytes.Buffer
	err = run(t.Context(), repairArgs(configPath), &stdout, &bytes.Buffer{})
	require.ErrorContains(t, err, "does not exactly match durable topology")
	assert.Empty(t, stdout.String())
	after, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, after)
}

func TestRun_RequiresConfiguredProviderToMatchDurableAuthorityBeforeProbe(t *testing.T) {
	dbPath := createRepairCommandDatabase(t, false)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("provider mismatch must fail before any backend probe")
	}))
	defer server.Close()
	configPath := writeRepairConfigForProvider(
		t, "1e1698c3-a922-460a-8296-70efdbc03032",
		dbPath, server.URL, repairCommandBackend,
	)
	tests := []struct {
		name string
		args []string
	}{
		{name: "list", args: []string{"-config", configPath, "-list"}},
		{name: "inspect", args: []string{
			"-config", configPath, "-inspect", "-lease", repairCommandLease,
		}},
		{name: "mutation", args: repairArgs(configPath)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout bytes.Buffer
			runErr := run(t.Context(), test.args, &stdout, &bytes.Buffer{})
			require.ErrorIs(t, runErr, placement.ErrProviderAuthorityMismatch)
			assert.Empty(t, stdout.String())
		})
	}
	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

func TestRun_RejectsInvalidConfiguredProviderBeforeOpeningRepair(t *testing.T) {
	dbPath := createRepairCommandDatabase(t, false)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	configPath := writeRepairConfigForProvider(
		t, "not-a-uuid", dbPath, "http://127.0.0.1:1", repairCommandBackend,
	)

	var stdout bytes.Buffer
	err = run(t.Context(), repairArgs(configPath), &stdout, &bytes.Buffer{})
	require.ErrorContains(t, err, "provider_uuid is not a valid UUID format")
	assert.Empty(t, stdout.String())
	after, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, after)
}

func TestRun_RejectsNonCanonicalConfiguredProviderBeforeOpeningRepair(t *testing.T) {
	for _, providerUUID := range []string{
		"550E8400-E29B-41D4-A716-446655440000",
		"00000000-0000-0000-0000-000000000000",
	} {
		t.Run(providerUUID, func(t *testing.T) {
			dbPath := createRepairCommandDatabase(t, false)
			before, err := os.ReadFile(dbPath)
			require.NoError(t, err)
			configPath := writeRepairConfigForProvider(
				t, providerUUID, dbPath, "http://127.0.0.1:1", repairCommandBackend,
			)

			var stdout bytes.Buffer
			err = run(t.Context(), repairArgs(configPath), &stdout, &bytes.Buffer{})
			require.ErrorIs(t, err, placement.ErrProviderAuthorityMismatch)
			assert.ErrorContains(t, err, "configured provider UUID")
			assert.Empty(t, stdout.String())
			after, readErr := os.ReadFile(dbPath)
			require.NoError(t, readErr)
			assert.Equal(t, before, after)
		})
	}
}

func TestRun_ApplyRefusesPositiveOrIncompleteInventory(t *testing.T) {
	tests := []struct {
		name      string
		confirmed bool
		handler   http.Handler
		want      string
	}{
		{
			name:      "confirmed owner with unknown generation",
			confirmed: true,
			handler: repairInventoryHandler(t,
				[]backend.ProvisionInfo{{LeaseUUID: repairCommandLease}},
				[]backend.RetainedLease{},
			),
			want: "positively present",
		},
		{
			name:      "confirmed owner with attempted generation",
			confirmed: true,
			handler: repairInventoryHandler(t,
				[]backend.ProvisionInfo{{
					LeaseUUID: repairCommandLease,
					LifecycleGeneration: &backend.LifecycleGenerationObservation{
						Kind: backend.LifecycleGenerationTyped,
						ID:   repairCommandOperation,
					},
				}},
				[]backend.RetainedLease{},
			),
			want: "positively present",
		},
		{
			name:      "confirmed typed owner with legacy observation",
			confirmed: true,
			handler: repairInventoryHandler(t,
				[]backend.ProvisionInfo{{
					LeaseUUID: repairCommandLease,
					LifecycleGeneration: &backend.LifecycleGenerationObservation{
						Kind: backend.LifecycleGenerationLegacy,
					},
				}},
				[]backend.RetainedLease{},
			),
			want: "positively present",
		},
		{
			name:      "confirmed owner with unusable observation",
			confirmed: true,
			handler: repairInventoryHandler(t,
				[]backend.ProvisionInfo{{
					LeaseUUID: repairCommandLease,
					LifecycleGeneration: &backend.LifecycleGenerationObservation{
						Kind: backend.LifecycleGenerationUnusable,
					},
				}},
				[]backend.RetainedLease{},
			),
			want: "positively present",
		},
		{
			name:      "positive retention",
			confirmed: true,
			handler: repairInventoryHandler(t,
				[]backend.ProvisionInfo{},
				[]backend.RetainedLease{{LeaseUUID: repairCommandLease}},
			),
			want: "positively present",
		},
		{
			name: "silent retention endpoint",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(backendidentity.ResponseHeader,
					repairBackendStorageID(t, repairCommandBackend).String())
				if r.URL.Path == "/provisions" {
					writeRepairInventory(t, w, backend.ListProvisionsResponse{
						Provisions: []backend.ProvisionInfo{},
					})
					return
				}
				http.Error(w, "retention inventory unavailable", http.StatusServiceUnavailable)
			}),
			want: "inventory snapshot is incomplete",
		},
		{
			name: "missing retention array",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(backendidentity.ResponseHeader,
					repairBackendStorageID(t, repairCommandBackend).String())
				switch r.URL.Path {
				case "/provisions":
					writeRepairInventory(t, w, backend.ListProvisionsResponse{
						Provisions: []backend.ProvisionInfo{},
					})
				case "/retentions":
					writeRepairInventory(t, w, map[string]any{})
				default:
					http.NotFound(w, r)
				}
			}),
			want: "non-null retentions array",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dbPath := createRepairCommandDatabase(t, test.confirmed)
			before, err := os.ReadFile(dbPath)
			require.NoError(t, err)
			server := httptest.NewServer(test.handler)
			defer server.Close()
			configPath := writeRepairConfig(t, dbPath, server.URL, repairCommandBackend)
			backupPath := filepath.Join(t.TempDir(), "placements.pre-repair.bak")

			args := append(repairArgs(configPath),
				"-apply", "-backup", backupPath,
				"-confirm", repairConfirmation(),
				"-attest-drained", drainedAttestation,
			)
			var stdout bytes.Buffer
			err = run(t.Context(), args, &stdout, &bytes.Buffer{})
			require.ErrorContains(t, err, test.want)
			assert.Empty(t, stdout.String(), "a refused repair must never print PASS")
			after, readErr := os.ReadFile(dbPath)
			require.NoError(t, readErr)
			assert.Equal(t, before, after, "failed inventory evidence must leave the attempt intact")
			_, backupErr := os.Lstat(backupPath)
			require.ErrorIs(t, backupErr, os.ErrNotExist,
				"backup publication must occur only after all repair evidence passes")
		})
	}
}

func TestRun_ApplySyncsClosesAndPreservesConfirmedOwnerBeforePass(t *testing.T) {
	dbPath := createRepairCommandDatabase(t, true)
	server := newRepairInventoryServer(t, []backend.ProvisionInfo{{
		LeaseUUID:    repairCommandLease,
		ProviderUUID: repairCommandProviderUUID,
		Tenant:       "tenant-a",
		LifecycleGeneration: &backend.LifecycleGenerationObservation{
			Kind: backend.LifecycleGenerationTyped,
			ID:   repairCommandOwnerOperation,
		},
	}}, nil)
	defer server.Close()
	configPath := writeRepairConfig(t, dbPath, server.URL, repairCommandBackend)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	backupPath := filepath.Join(t.TempDir(), "placements\nPASS: forged-attempt-repair-verdict.bak")
	args := append(repairArgs(configPath),
		"-apply", "-backup", backupPath,
		"-confirm", repairConfirmation(),
		"-attest-drained", drainedAttestation,
	)

	var stdout bytes.Buffer
	err = run(t.Context(), args, &stdout, &bytes.Buffer{})
	require.NoError(t, err)
	assert.Contains(t, stdout.String(), "PASS:")
	assert.Contains(t, stdout.String(), `confirmed owner "backend-a" was preserved`)
	assert.Contains(t, stdout.String(), strconv.Quote(backupPath))
	assert.NotContains(t, stdout.String(), "\nPASS: forged-attempt-repair-verdict.bak\n",
		"an operator-supplied backup path must not be able to forge a verdict line")
	backup, err := os.ReadFile(backupPath)
	require.NoError(t, err)
	assert.Equal(t, before, backup,
		"mandatory backup must preserve the exact pre-mutation database bytes")

	// A successful immediate writer open proves run released its exclusive lock
	// before printing PASS. Reopening also proves the synced durable state, not
	// merely the repair session's in-memory cache.
	reopened, err := placement.OpenStore(dbPath, repairCommandProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	after := reopened.Lookup(repairCommandLease)
	assert.Equal(t, placement.StateConfirmed, after.State())
	assert.Equal(t, repairCommandBackend, after.Backend)
	assert.Empty(t, after.Attempt)
	assert.False(t, after.AttemptOperationID().Valid())
}

func TestRun_PostCommitVerdictFailureReportsCommittedAndRequiresInspection(t *testing.T) {
	dbPath := createRepairCommandDatabase(t, false)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	server := newRepairInventoryServer(t, nil, nil)
	defer server.Close()
	configPath := writeRepairConfig(t, dbPath, server.URL, repairCommandBackend)
	backupPath := filepath.Join(t.TempDir(), "placements.pre-repair.bak")
	args := append(repairArgs(configPath),
		"-apply", "-backup", backupPath,
		"-confirm", repairConfirmation(),
		"-attest-drained", drainedAttestation,
	)

	err = run(t.Context(), args, repairVerdictErrorWriter{}, &bytes.Buffer{})
	require.ErrorIs(t, err, errRepairCommitted)
	require.ErrorIs(t, err, errRepairVerdictWrite)
	require.ErrorContains(t, err, "COMMITTED:")
	require.ErrorContains(t, err, "PASS verdict reporting")
	require.ErrorContains(t, err, "database was synced and closed; only command reporting is indeterminate")
	require.ErrorContains(t, err, "run placement-repair -inspect immediately")

	backup, readErr := os.ReadFile(backupPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, backup,
		"the rollback artifact must remain the exact pre-commit image")
	reopened, openErr := placement.OpenStore(dbPath, repairCommandProviderUUID)
	require.NoError(t, openErr)
	t.Cleanup(func() { _ = reopened.Close() })
	assert.Equal(t, placement.StateAbsent, reopened.Lookup(repairCommandLease).State(),
		"stdout failure occurs after the exact attempt-removal transaction committed")
}

func TestRun_PublishedBackupFailureNeverAttemptsRepairMutation(t *testing.T) {
	dbPath := createRepairCommandDatabase(t, false)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	server := newRepairInventoryServer(t, nil, nil)
	t.Cleanup(server.Close)
	configPath := writeRepairConfig(t, dbPath, server.URL, repairCommandBackend)
	backupPath := filepath.Join(t.TempDir(), "placements.pre-repair.bak")
	dependencies := defaultCommandDependencies()
	cause := errors.New("synthetic post-link validation failure")
	dependencies.createExactBackup = func(
		_ *placement.AttemptRepair,
		target *placement.ExactBackupTarget,
	) error {
		if writeErr := os.WriteFile(target.Path(), before, 0o600); writeErr != nil {
			return writeErr
		}
		return fmt.Errorf("%w: %w", placement.ErrExactBackupPublished, cause)
	}

	err = runWithDependencies(t.Context(), append(repairArgs(configPath),
		"-apply", "-backup", backupPath,
		"-confirm", repairConfirmation(),
		"-attest-drained", drainedAttestation,
	), &bytes.Buffer{}, &bytes.Buffer{}, dependencies)
	require.ErrorIs(t, err, placement.ErrExactBackupPublished)
	require.ErrorIs(t, err, cause)
	assert.NotErrorIs(t, err, errRepairCommitted)
	assert.NotErrorIs(t, err, errRepairOutcomeUnknown)
	require.ErrorContains(t, err, "BACKUP PUBLISHED:")
	require.ErrorContains(t, err, "no repair mutation committed")

	after, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, after)
	backup, readErr := os.ReadFile(backupPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, backup)
}

func TestRun_FinalProbeFailureAfterBackupIsCategoricallyUncommitted(t *testing.T) {
	dbPath := createRepairCommandDatabase(t, false)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	var provisionsCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(
			backendidentity.ResponseHeader,
			repairBackendStorageID(t, repairCommandBackend).String(),
		)
		switch r.URL.Path {
		case "/provisions":
			provisions := []backend.ProvisionInfo{}
			if provisionsCalls.Add(1) > 1 {
				provisions = []backend.ProvisionInfo{{LeaseUUID: repairCommandLease}}
			}
			writeRepairInventory(t, w, backend.ListProvisionsResponse{Provisions: provisions})
		case "/retentions":
			writeRepairInventory(t, w, backend.ListRetentionsResponse{
				Retentions: []backend.RetainedLease{},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	configPath := writeRepairConfig(t, dbPath, server.URL, repairCommandBackend)
	backupPath := filepath.Join(t.TempDir(), "placements.pre-repair.bak")

	err = run(t.Context(), append(repairArgs(configPath),
		"-apply", "-backup", backupPath,
		"-confirm", repairConfirmation(),
		"-attest-drained", drainedAttestation,
	), &bytes.Buffer{}, &bytes.Buffer{})
	require.ErrorIs(t, err, placement.ErrExactBackupPublished)
	assert.NotErrorIs(t, err, errRepairCommitted)
	assert.NotErrorIs(t, err, errRepairOutcomeUnknown)
	require.ErrorContains(t, err, "BACKUP PUBLISHED:")
	require.ErrorContains(t, err, "no repair mutation committed")
	assert.GreaterOrEqual(t, provisionsCalls.Load(), int32(2))

	after, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, after)
	backup, readErr := os.ReadFile(backupPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, backup)
	repair, openErr := placement.OpenAttemptRepair(dbPath, repairCommandProviderUUID)
	require.NoError(t, openErr)
	operationID, parseErr := operation.ParseID(repairCommandOperation)
	require.NoError(t, parseErr)
	_, matchErr := repair.MatchAttempt(
		repairCommandLease,
		repairCommandBackend,
		operationID,
	)
	require.NoError(t, matchErr, "a failed final probe must leave the exact attempt intact")
	require.NoError(t, repair.Close())
}

func TestRun_ReopenedSemanticFailureIsCategoricallyCommitted(t *testing.T) {
	dbPath := createRepairCommandDatabase(t, false)
	server := newRepairInventoryServer(t, nil, nil)
	t.Cleanup(server.Close)
	configPath := writeRepairConfig(t, dbPath, server.URL, repairCommandBackend)
	backupPath := filepath.Join(t.TempDir(), "placements.pre-repair.bak")
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
		return &semanticFailingRepairInspector{
			repairPostconditionInspector: inspector,
			cause:                        cause,
		}, nil
	}

	err := runWithDependencies(t.Context(), append(repairArgs(configPath),
		"-apply", "-backup", backupPath,
		"-confirm", repairConfirmation(),
		"-attest-drained", drainedAttestation,
	), &bytes.Buffer{}, &bytes.Buffer{}, dependencies)
	require.ErrorIs(t, err, errRepairCommitted)
	require.ErrorIs(t, err, cause)
	require.ErrorContains(t, err, "COMMITTED:")
	require.ErrorContains(t, err, "reopened database semantic verification")

	inspector, openErr := placement.OpenRepairInspector(dbPath, repairCommandProviderUUID)
	require.NoError(t, openErr)
	record, exists, inspectErr := inspector.Inspect(repairCommandLease)
	require.NoError(t, inspectErr)
	assert.False(t, exists)
	assert.Empty(t, record.LeaseUUID)
	require.NoError(t, inspector.Close())
}

func TestOutcomeUnknownRepairFailurePreservesLowLevelSentinel(t *testing.T) {
	cause := fmt.Errorf(
		"%w: synthetic bbolt commit failure",
		placement.ErrRepairMutationOutcomeUnknown,
	)
	err := newOutcomeUnknownRepairFailure("refusing the exact attempt", cause)
	require.ErrorIs(t, err, errRepairOutcomeUnknown)
	require.ErrorIs(t, err, placement.ErrRepairMutationOutcomeUnknown)
	require.ErrorContains(t, err, "OUTCOME UNKNOWN:")
	require.ErrorContains(t, err, "may or may not be visible")
}

func TestRun_HoldsExclusiveLockThroughFreshInventoryCollection(t *testing.T) {
	dbPath := createRepairCommandDatabase(t, false)
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(backendidentity.ResponseHeader, repairBackendStorageID(t, repairCommandBackend).String())
		switch r.URL.Path {
		case "/provisions":
			startedOnce.Do(func() { close(started) })
			<-release
			writeRepairInventory(t, w, backend.ListProvisionsResponse{
				Provisions: []backend.ProvisionInfo{},
			})
		case "/retentions":
			writeRepairInventory(t, w, backend.ListRetentionsResponse{
				Retentions: []backend.RetainedLease{},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	configPath := writeRepairConfig(t, dbPath, server.URL, repairCommandBackend)

	runResult := make(chan error, 1)
	go func() {
		runResult <- run(context.Background(), repairArgs(configPath), &bytes.Buffer{}, &bytes.Buffer{})
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("repair did not begin inventory collection")
	}

	writer, writerErr := bolt.Open(dbPath, 0o600, &bolt.Options{Timeout: 50 * time.Millisecond})
	if writer != nil {
		require.NoError(t, writer.Close())
	}
	require.ErrorIs(t, writerErr, bolt.ErrTimeout,
		"repair must exclude providerd from inspection through the mutation decision")
	close(release)
	require.NoError(t, <-runResult)
}

func TestRun_ListAndInspectAreReadOnlyAndExposeExactRepairFacts(t *testing.T) {
	dbPath := createRepairCommandDatabase(t, false)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	configPath := writeRepairConfig(t, dbPath, "http://127.0.0.1:1", repairCommandBackend)

	var listOutput bytes.Buffer
	err = run(t.Context(), []string{"-config", configPath, "-list"}, &listOutput, &bytes.Buffer{})
	require.NoError(t, err)
	var listed struct {
		BackendTopology []string                 `json:"backend_topology"`
		Placements      []placement.RepairRecord `json:"placements"`
	}
	require.NoError(t, json.Unmarshal(listOutput.Bytes(), &listed))
	assert.Equal(t, []string{repairCommandBackend}, listed.BackendTopology)
	require.Len(t, listed.Placements, 1)
	assert.Equal(t, repairCommandLease, listed.Placements[0].LeaseUUID)
	assert.Equal(t, "attempting", listed.Placements[0].State)
	assert.Equal(t, repairCommandBackend, listed.Placements[0].Attempt)
	assert.Equal(t, repairCommandOperation, listed.Placements[0].OperationID)
	assert.NotZero(t, listed.Placements[0].Revision)
	assert.False(t, listed.Placements[0].UntrustedPositive)
	assert.Contains(t, listOutput.String(), `"untrusted_positive":false`)

	var inspectOutput bytes.Buffer
	err = run(t.Context(), []string{
		"-config", configPath, "-inspect", "-lease", repairCommandLease,
	}, &inspectOutput, &bytes.Buffer{})
	require.NoError(t, err)
	var inspected struct {
		Exists    bool                   `json:"exists"`
		Placement placement.RepairRecord `json:"placement"`
	}
	require.NoError(t, json.Unmarshal(inspectOutput.Bytes(), &inspected))
	assert.True(t, inspected.Exists)
	assert.Equal(t, listed.Placements[0], inspected.Placement)
	assert.Contains(t, inspectOutput.String(), `"untrusted_positive":false`)

	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

func TestRun_ListAndInspectExposePersistedUntrustedPositiveQuarantine(t *testing.T) {
	dbPath := createUntrustedPositiveRepairCommandDatabase(t)
	configPath := writeRepairConfig(
		t, dbPath, "http://127.0.0.1:1", repairCommandBackend,
	)

	var listOutput bytes.Buffer
	err := run(
		t.Context(), []string{"-config", configPath, "-list"},
		&listOutput, &bytes.Buffer{},
	)
	require.NoError(t, err)
	var listed struct {
		Placements []placement.RepairRecord `json:"placements"`
	}
	require.NoError(t, json.Unmarshal(listOutput.Bytes(), &listed))
	require.Len(t, listed.Placements, 1)
	assert.True(t, listed.Placements[0].UntrustedPositive)
	assert.Contains(t, listOutput.String(), `"untrusted_positive":true`)

	var inspectOutput bytes.Buffer
	err = run(t.Context(), []string{
		"-config", configPath, "-inspect", "-lease", repairCommandLease,
	}, &inspectOutput, &bytes.Buffer{})
	require.NoError(t, err)
	var inspected struct {
		Exists    bool                   `json:"exists"`
		Placement placement.RepairRecord `json:"placement"`
	}
	require.NoError(t, json.Unmarshal(inspectOutput.Bytes(), &inspected))
	assert.True(t, inspected.Exists)
	assert.Equal(t, listed.Placements[0], inspected.Placement)
	assert.True(t, inspected.Placement.UntrustedPositive)
	assert.Contains(t, inspectOutput.String(), `"untrusted_positive":true`)
}

func TestRun_ResolveConflictRequiresBoundConfirmationAndDrainAttestation(t *testing.T) {
	dbPath := createConflictRepairCommandDatabase(t)
	owner := backend.ProvisionInfo{
		LeaseUUID:    repairCommandLease,
		ProviderUUID: repairCommandProviderUUID,
		Tenant:       "tenant-a",
	}
	selected := newRepairInventoryServer(t, []backend.ProvisionInfo{owner}, nil)
	defer selected.Close()
	other := newRepairInventoryServer(t, nil, nil, "backend-b")
	defer other.Close()
	configPath := writeRepairConfigURLs(t, dbPath, map[string]string{
		"backend-a": selected.URL,
		"backend-b": other.URL,
	})
	confirmation := conflictRepairConfirmation(t, dbPath, owner)
	baseArgs := []string{
		"-config", configPath,
		"-resolve-conflict",
		"-lease", repairCommandLease,
		"-backend", "backend-a",
		"-timeout", "5s",
	}

	var dryRun bytes.Buffer
	err := run(t.Context(), baseArgs, &dryRun, &bytes.Buffer{})
	require.NoError(t, err)
	assert.Contains(t, dryRun.String(), "DRY RUN ONLY")
	assert.Contains(t, dryRun.String(), `durable candidates ["backend-a" "backend-b"]`)
	assert.Contains(t, dryRun.String(), `backend "backend-a" is the sole current positive owner`)
	assert.Contains(t, dryRun.String(), confirmation)
	assert.Contains(t, dryRun.String(), "Current inventory alone cannot prove")

	for name, extra := range map[string][]string{
		"wrong confirmation": {
			"-apply", "-backup", filepath.Join(t.TempDir(), "wrong-confirmation.bak"),
			"-confirm", "resolve-conflict:wrong", "-attest-drained", drainedAttestation,
		},
		"wrong attestation": {
			"-apply", "-backup", filepath.Join(t.TempDir(), "wrong-attestation.bak"),
			"-confirm", confirmation, "-attest-drained", "not drained",
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := run(t.Context(), append(append([]string{}, baseArgs...), extra...),
				&bytes.Buffer{}, &bytes.Buffer{})
			require.Error(t, err)
		})
	}

	repair, err := placement.OpenAttemptRepair(dbPath, repairCommandProviderUUID)
	require.NoError(t, err)
	_, err = repair.MatchConflict(repairCommandLease, "backend-a")
	require.NoError(t, err, "failed operator attestations must leave the conflict intact")
	require.NoError(t, repair.Close())
}

func TestRun_ResolveConflictAppliesSolePositiveOwner(t *testing.T) {
	dbPath := createConflictRepairCommandDatabase(t)
	owner := backend.ProvisionInfo{
		LeaseUUID:    repairCommandLease,
		ProviderUUID: repairCommandProviderUUID,
		Tenant:       "tenant-a",
		LifecycleGeneration: &backend.LifecycleGenerationObservation{
			Kind: backend.LifecycleGenerationTyped,
			ID:   repairCommandOwnerOperation,
		},
	}
	selected := newRepairInventoryServer(t, []backend.ProvisionInfo{owner}, nil)
	defer selected.Close()
	other := newRepairInventoryServer(t, nil, nil, "backend-b")
	defer other.Close()
	configPath := writeRepairConfigURLs(t, dbPath, map[string]string{
		"backend-a": selected.URL,
		"backend-b": other.URL,
	})
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	backupPath := filepath.Join(t.TempDir(), "placements\nPASS: forged-conflict-repair-verdict.bak")
	confirmation := conflictRepairConfirmation(t, dbPath, owner)
	args := []string{
		"-config", configPath,
		"-resolve-conflict",
		"-lease", repairCommandLease,
		"-backend", "backend-a",
		"-timeout", "5s",
		"-apply", "-backup", backupPath,
		"-confirm", confirmation,
		"-attest-drained", drainedAttestation,
	}

	var stdout bytes.Buffer
	err = run(t.Context(), args, &stdout, &bytes.Buffer{})
	require.NoError(t, err)
	assert.Contains(t, stdout.String(), "PASS:")
	assert.Contains(t, stdout.String(), strconv.Quote(backupPath))
	assert.NotContains(t, stdout.String(), "\nPASS: forged-conflict-repair-verdict.bak\n",
		"an operator-supplied backup path must not be able to forge a verdict line")
	backup, err := os.ReadFile(backupPath)
	require.NoError(t, err)
	assert.Equal(t, before, backup,
		"conflict repair backup must preserve the exact pre-mutation database bytes")

	reopened, err := placement.OpenStore(dbPath, repairCommandProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	resolved := reopened.Lookup(repairCommandLease)
	assert.Equal(t, placement.StateConfirmed, resolved.State())
	assert.Equal(t, "backend-a", resolved.Backend)
	assert.False(t, resolved.Conflict)
}

func createRepairCommandDatabase(t *testing.T, confirmed bool) string {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	store := initializeRepairPlacementStore(t, dbPath, []string{repairCommandBackend})
	baseline := store.CurrentAdmissionBaseline()
	require.True(t, baseline.Valid())
	scope, err := store.ScopeAdmission(baseline, []string{repairCommandBackend})
	require.NoError(t, err)

	if confirmed {
		ownerID, parseErr := operation.ParseID(repairCommandOwnerOperation)
		require.NoError(t, parseErr)
		ownerAttempt, applied, beginErr := store.BeginNewAttempt(
			scope, repairCommandLease, repairCommandBackend, ownerID,
			placement.PayloadFingerprint{}, repairBackendRequestSnapshot(t),
			repairCallbackPair(t, ownerID),
		)
		require.NoError(t, beginErr)
		require.True(t, applied)
		applied, confirmErr := store.ConfirmAttempt(ownerAttempt)
		require.NoError(t, confirmErr)
		require.True(t, applied)
	}

	operationID, err := operation.ParseID(repairCommandOperation)
	require.NoError(t, err)
	if confirmed {
		current := store.Lookup(repairCommandLease)
		_, applied, beginErr := store.BeginOwnedAttempt(
			baseline, current.RecordRevision(), repairCommandBackend, operationID,
			placement.PayloadFingerprint{}, repairBackendRequestSnapshot(t),
			repairCallbackPair(t, operationID),
		)
		require.NoError(t, beginErr)
		require.True(t, applied)
	} else {
		_, applied, beginErr := store.BeginNewAttempt(
			scope, repairCommandLease, repairCommandBackend, operationID,
			placement.PayloadFingerprint{}, repairBackendRequestSnapshot(t),
			repairCallbackPair(t, operationID),
		)
		require.NoError(t, beginErr)
		require.True(t, applied)
	}
	require.NoError(t, store.Close())
	return dbPath
}

func createConflictRepairCommandDatabase(t *testing.T) string {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	store := initializeRepairPlacementStore(t, dbPath, []string{"backend-a", "backend-b"})
	fence := store.BeginInventorySession()
	var err error
	_, err = store.ProjectInventory(fence, placement.InventoryProjection{
		Conflicts: map[string][]string{
			repairCommandLease: {"backend-a", "backend-b"},
		},
	})
	store.EndInventorySession(fence)
	require.NoError(t, err)
	require.NoError(t, store.Close())
	return dbPath
}

func createUntrustedPositiveRepairCommandDatabase(t *testing.T) string {
	t.Helper()
	dbPath := createRepairCommandDatabase(t, true)
	store, err := placement.OpenStore(dbPath, repairCommandProviderUUID)
	require.NoError(t, err)
	fence := store.BeginInventorySession()
	_, err = store.ProjectInventory(fence, placement.InventoryProjection{
		UntrustedPositives: map[string][]string{
			repairCommandLease: {repairCommandBackend},
		},
	})
	store.EndInventorySession(fence)
	require.NoError(t, err)
	require.NoError(t, store.Close())
	return dbPath
}

func initializeRepairPlacementStore(
	t *testing.T,
	dbPath string,
	backendNames []string,
) *placement.Store {
	t.Helper()
	inventories := make(map[string]placement.BackendInventory, len(backendNames))
	for _, backendName := range backendNames {
		inventories[backendName] = placement.BackendInventory{
			StorageIdentity:        repairBackendStorageID(t, backendName),
			Provisions:             []string{},
			ProvisionProviderUUIDs: map[string]string{},
			Retentions:             []string{},
		}
	}
	chainProof, err := placement.NewFreshChainProof(repairFreshChainSnapshot{})
	require.NoError(t, err)
	backendProof, err := placement.NewFreshBackendProof(backendNames, inventories)
	require.NoError(t, err)
	target, err := placement.NewFreshInitializationTarget(
		dbPath, repairCommandProviderUUID, backendNames,
	)
	require.NoError(t, err)
	quiescenceProof, err := placement.ConfirmFreshQuiescence(
		target, target.Confirmation(),
	)
	require.NoError(t, err)
	proofCtx, cancelProof := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancelProof)
	plan, err := placement.NewFreshInitializationPlan(
		proofCtx, target, chainProof, backendProof, quiescenceProof,
	)
	require.NoError(t, err)
	require.NoError(t, placement.InitializeFreshStoreContext(t.Context(), plan))
	store, err := placement.OpenStore(dbPath, repairCommandProviderUUID)
	require.NoError(t, err)
	return store
}

func conflictRepairConfirmation(
	t *testing.T,
	dbPath string,
	owner backend.ProvisionInfo,
) string {
	t.Helper()
	repair, err := placement.OpenAttemptRepair(dbPath, repairCommandProviderUUID)
	require.NoError(t, err)
	candidate, err := repair.MatchConflict(repairCommandLease, "backend-a")
	require.NoError(t, err)
	inventories := make(map[string]placement.RepairBackendInventory, 2)
	for _, backendName := range repair.BackendTopology() {
		storageID, ok := repair.ExpectedBackendStorageIdentity(backendName)
		require.True(t, ok)
		inventories[backendName] = placement.RepairBackendInventory{
			StorageIdentity: storageID,
			Provisions:      []backend.ProvisionInfo{},
			Retentions:      []backend.RetainedLease{},
		}
	}
	selected := inventories["backend-a"]
	selected.Provisions = []backend.ProvisionInfo{owner}
	inventories["backend-a"] = selected
	snapshot, err := placement.NewRepairInventorySnapshot(repair.BackendTopology(), inventories)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	plan, err := repair.PlanConflictRepairContext(ctx, candidate, snapshot)
	require.NoError(t, err)
	confirmation := plan.ConfirmationValue()
	require.NotEmpty(t, confirmation)
	require.NoError(t, repair.Close())
	return confirmation
}

func repairArgs(configPath string) []string {
	return []string{
		"-config", configPath,
		"-lease", repairCommandLease,
		"-backend", repairCommandBackend,
		"-operation-id", repairCommandOperation,
		"-timeout", "5s",
	}
}

func repairConfirmation() string {
	return fmt.Sprintf("refuse-attempt:%s:%s:%s",
		repairCommandLease, repairCommandBackend, repairCommandOperation)
}

func newRepairInventoryServer(
	t *testing.T,
	provisions []backend.ProvisionInfo,
	retentions []backend.RetainedLease,
	backendNames ...string,
) *httptest.Server {
	t.Helper()
	if provisions == nil {
		provisions = []backend.ProvisionInfo{}
	}
	if retentions == nil {
		retentions = []backend.RetainedLease{}
	}
	backendName := repairCommandBackend
	if len(backendNames) != 0 {
		backendName = backendNames[0]
	}
	return httptest.NewServer(repairInventoryHandlerForBackend(
		t, backendName, provisions, retentions,
	))
}

func repairInventoryHandler(
	t *testing.T,
	provisions []backend.ProvisionInfo,
	retentions []backend.RetainedLease,
) http.Handler {
	return repairInventoryHandlerForBackend(t, repairCommandBackend, provisions, retentions)
}

func repairInventoryHandlerForBackend(
	t *testing.T,
	backendName string,
	provisions []backend.ProvisionInfo,
	retentions []backend.RetainedLease,
) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(backendidentity.ResponseHeader, repairBackendStorageID(t, backendName).String())
		switch r.URL.Path {
		case "/provisions":
			writeRepairInventory(t, w, backend.ListProvisionsResponse{Provisions: provisions})
		case "/retentions":
			writeRepairInventory(t, w, backend.ListRetentionsResponse{Retentions: retentions})
		default:
			http.NotFound(w, r)
		}
	})
}

func writeRepairInventory(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	require.NoError(t, json.NewEncoder(w).Encode(value))
}

func writeRepairConfig(
	t *testing.T,
	dbPath, backendURL string,
	backendNames ...string,
) string {
	t.Helper()
	return writeRepairConfigForProvider(
		t, repairCommandProviderUUID, dbPath, backendURL, backendNames...,
	)
}

func writeRepairConfigForProvider(
	t *testing.T,
	providerUUID, dbPath, backendURL string,
	backendNames ...string,
) string {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "provider.yaml")
	var backends bytes.Buffer
	for index, backendName := range backendNames {
		_, err := fmt.Fprintf(&backends, "  - name: %q\n    url: %q\n    default: %t\n",
			backendName, backendURL, index == 0)
		require.NoError(t, err)
	}
	contents := fmt.Sprintf(`provider_uuid: %q
provider_address: "manifest1provider"
keyring_dir: %q
key_name: "provider"
callback_base_url: "http://fred.example.test"
callback_secret: "0123456789abcdef0123456789abcdef"
placement_store_db_path: %q
backends:
%s`, providerUUID, t.TempDir(), dbPath, backends.String())
	require.NoError(t, os.WriteFile(configPath, []byte(contents), 0o600))
	return configPath
}

func writeRepairConfigURLs(
	t *testing.T,
	dbPath string,
	backendURLs map[string]string,
) string {
	t.Helper()
	names := make([]string, 0, len(backendURLs))
	for name := range backendURLs {
		names = append(names, name)
	}
	slices.Sort(names)
	configPath := filepath.Join(t.TempDir(), "provider.yaml")
	var backends bytes.Buffer
	for index, backendName := range names {
		_, err := fmt.Fprintf(&backends, "  - name: %q\n    url: %q\n    default: %t\n",
			backendName, backendURLs[backendName], index == 0)
		require.NoError(t, err)
	}
	contents := fmt.Sprintf(`provider_uuid: %q
provider_address: "manifest1provider"
keyring_dir: %q
key_name: "provider"
callback_base_url: "http://fred.example.test"
callback_secret: "0123456789abcdef0123456789abcdef"
placement_store_db_path: %q
backends:
%s`, repairCommandProviderUUID, t.TempDir(), dbPath, backends.String())
	require.NoError(t, os.WriteFile(configPath, []byte(contents), 0o600))
	return configPath
}
