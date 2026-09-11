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
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

const (
	preflightCommandProvisionLease = "018f47a2-8b1c-7def-8123-456789abcdef"
	preflightCommandRetainedLease  = "018f47a2-8b1c-7def-8123-456789abcdee"
	preflightCommandOtherLease     = "018f47a2-8b1c-7def-8123-456789abcded"
)

type preflightFreshChainSnapshot struct {
	providerUUID       string
	height             int64
	total              int
	blocking           int
	blockingLeaseUUIDs []string
	leaseUUIDs         []string
	leaseItems         map[string][]backend.LeaseItem
}

func (snapshot preflightFreshChainSnapshot) Valid() bool {
	return snapshot.providerUUID != "" && snapshot.height > 0 &&
		snapshot.total >= snapshot.blocking && snapshot.blocking >= 0
}

func (snapshot preflightFreshChainSnapshot) ProviderUUID() string { return snapshot.providerUUID }
func (snapshot preflightFreshChainSnapshot) BlockHeight() int64   { return snapshot.height }
func (snapshot preflightFreshChainSnapshot) TotalLeases() int     { return snapshot.total }
func (snapshot preflightFreshChainSnapshot) BlockingLeaseCount() int {
	return snapshot.blocking
}
func (snapshot preflightFreshChainSnapshot) BlockingLeaseUUIDs() []string {
	return append([]string(nil), snapshot.blockingLeaseUUIDs...)
}
func (snapshot preflightFreshChainSnapshot) LeaseUUIDs() []string {
	leaseUUIDs := make([]string, len(snapshot.leaseUUIDs))
	copy(leaseUUIDs, snapshot.leaseUUIDs)
	return leaseUUIDs
}
func (snapshot preflightFreshChainSnapshot) LeaseItems() map[string][]backend.LeaseItem {
	if snapshot.leaseItems == nil {
		return nil
	}
	items := make(map[string][]backend.LeaseItem, len(snapshot.leaseItems))
	for leaseUUID, leaseItems := range snapshot.leaseItems {
		items[leaseUUID] = append([]backend.LeaseItem(nil), leaseItems...)
	}
	return items
}

func preflightSnapshotItems(leaseUUIDs []string) map[string][]backend.LeaseItem {
	items := make(map[string][]backend.LeaseItem, len(leaseUUIDs))
	for _, leaseUUID := range leaseUUIDs {
		items[leaseUUID] = []backend.LeaseItem{{
			SKU: "sku-test", Quantity: 1, ServiceName: "app",
		}}
	}
	return items
}

type preflightFreshChainClient struct {
	snapshot     providerLeaseSnapshot
	snapshotErr  error
	closeErr     error
	providerUUID string
	closed       bool
}

func (client *preflightFreshChainClient) SnapshotProviderLeases(
	_ context.Context,
	providerUUID string,
) (providerLeaseSnapshot, error) {
	client.providerUUID = providerUUID
	return client.snapshot, client.snapshotErr
}

func (client *preflightFreshChainClient) Close() error {
	client.closed = true
	return client.closeErr
}

type blockingPreflightFreshChainClient struct{ closed bool }

func (client *blockingPreflightFreshChainClient) SnapshotProviderLeases(
	ctx context.Context,
	_ string,
) (providerLeaseSnapshot, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (client *blockingPreflightFreshChainClient) Close() error {
	client.closed = true
	return nil
}

type closeFailingLegacyPreparer struct {
	legacyUpgradePreparer
	cause error
}

func (preparer *closeFailingLegacyPreparer) Close() error {
	return errors.Join(preparer.legacyUpgradePreparer.Close(), preparer.cause)
}

type syncFailingLegacyPreparer struct {
	legacyUpgradePreparer
	cause error
}

type semanticFailingPreparedAuthority struct {
	preparedAuthorityInspector
	cause error
}

func (inspector *semanticFailingPreparedAuthority) VerifyLegacyPreparationPostcondition(
	string,
	[]string,
	map[string]placement.BackendInventory,
	placement.LegacyUpgradePreflightSummary,
) error {
	return inspector.cause
}

func (preparer *syncFailingLegacyPreparer) PrepareContext(
	ctx context.Context,
	providerUUID string,
	backendNames []string,
	inventories map[string]placement.BackendInventory,
	chainProof placement.LegacyUpgradeChainProof,
	capability placement.LegacyPreparationCapability,
) (placement.LegacyUpgradePreflightSummary, error) {
	summary, err := preparer.legacyUpgradePreparer.PrepareContext(
		ctx, providerUUID, backendNames, inventories, chainProof, capability,
	)
	if err != nil {
		return summary, err
	}
	return summary, fmt.Errorf("%w: %w", placement.ErrLegacyPreparationCommitted, preparer.cause)
}

type preflightVerdictErrorWriter struct{ cause error }

func (writer preflightVerdictErrorWriter) Write([]byte) (int, error) {
	return 0, writer.cause
}

type preflightRecordingWriter struct {
	bytes.Buffer
	calls int
}

func (writer *preflightRecordingWriter) Write(value []byte) (int, error) {
	writer.calls++
	return writer.Buffer.Write(value)
}

func TestWriteCommandErrorCannotForgeVerdictOrTerminalOutput(t *testing.T) {
	var output bytes.Buffer
	writeCommandError(
		&output,
		errors.New("remote failure\nPASS: forged\r\x1b]8;;https://evil.invalid\x07link"),
	)

	rendered := output.String()
	assert.True(t, strings.HasPrefix(rendered, "placement-preflight: ERROR \""))
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
		[]string{"-proof-timeout", "bad\nPASS: forged\x1b[31m"},
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
			assert.Contains(t, stderr.String(), "Usage of placement-preflight:")
			assert.Contains(t, stderr.String(), "-config")
			assert.NotContains(t, stderr.String(), "placement-preflight: ERROR")
		})
	}
}

func legacyPreflightDependencies(leaseUUIDs ...string) commandDependencies {
	dependencies := defaultCommandDependencies()
	dependencies.newFreshChainClient = func(
		chain.ReadOnlyClientConfig,
	) (freshChainClient, error) {
		return &preflightFreshChainClient{snapshot: preflightFreshChainSnapshot{
			providerUUID: "550e8400-e29b-41d4-a716-446655440000",
			height:       913,
			total:        len(leaseUUIDs),
			leaseUUIDs:   append([]string(nil), leaseUUIDs...),
			leaseItems:   preflightSnapshotItems(leaseUUIDs),
		}}, nil
	}
	return dependencies
}

func legacyPreflightChainProof(
	t *testing.T,
	leaseUUIDs ...string,
) placement.LegacyUpgradeChainProof {
	t.Helper()
	proof, err := placement.NewLegacyUpgradeChainProof(preflightFreshChainSnapshot{
		providerUUID: "550e8400-e29b-41d4-a716-446655440000",
		height:       913,
		total:        len(leaseUUIDs),
		leaseUUIDs:   append([]string(nil), leaseUUIDs...),
		leaseItems:   preflightSnapshotItems(leaseUUIDs),
	})
	require.NoError(t, err)
	return proof
}

func TestRun_CleanStoppedDatabaseAndLiveInventoriesPassReadOnly(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeLegacyPlacementDB(t, dbPath, map[string][]byte{
		preflightCommandProvisionLease: []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
		preflightCommandRetainedLease:  []byte("backend-a"),
	})
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	server := newInventoryServer(t,
		[]backend.ProvisionInfo{{LeaseUUID: preflightCommandProvisionLease}},
		[]backend.RetainedLease{{LeaseUUID: preflightCommandRetainedLease}},
	)
	defer server.Close()
	configPath := writePreflightConfig(t, dbPath, server.URL, "backend-a")

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err = runWithDependencies(
		t.Context(),
		[]string{"-config", configPath, "-proof-timeout", "5s"},
		&stdout,
		&stderr,
		legacyPreflightDependencies(preflightCommandProvisionLease, preflightCommandRetainedLease),
	)
	require.NoError(t, err)
	assert.Empty(t, stderr.String())
	assert.Contains(t, stdout.String(),
		inspectSuccessVerdict+": v0.13 placement preflight verified 2 rows against 2 leases on 1 backends")

	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, after, "the command must not migrate or rewrite the placement database")
}

func TestRun_DocumentedInspectThenPrepareSequenceCreatesRollbackBackup(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "placements.db")
	backupPath := filepath.Join(tempDir, "placements\nPASS: forged-preflight-verdict.bak")
	writeLegacyPlacementDB(t, dbPath, map[string][]byte{
		preflightCommandProvisionLease: []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
		preflightCommandRetainedLease:  []byte("backend-a"),
	})
	legacyBytes, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	server := newInventoryServer(t,
		[]backend.ProvisionInfo{{LeaseUUID: preflightCommandProvisionLease}},
		[]backend.RetainedLease{{LeaseUUID: preflightCommandRetainedLease}},
	)
	defer server.Close()
	configPath := writePreflightConfig(t, dbPath, server.URL, "backend-a")

	var inspectOut bytes.Buffer
	err = runWithDependencies(t.Context(), []string{
		"-config", configPath, "-proof-timeout", "5s",
	}, &inspectOut, &bytes.Buffer{},
		legacyPreflightDependencies(preflightCommandProvisionLease, preflightCommandRetainedLease))
	require.NoError(t, err)
	assert.Contains(t, inspectOut.String(), inspectSuccessVerdict+":")
	assert.Contains(t, inspectOut.String(), "database remained read-only")
	afterInspect, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, legacyBytes, afterInspect,
		"the documented inspection pass must leave the stopped database byte-exact")

	var prepareOut bytes.Buffer
	err = runWithDependencies(t.Context(), []string{
		"-config", configPath, "-proof-timeout", "5s", "-prepare", "-backup", backupPath,
		"-attest-drained", placement.LegacyPreparationDrainAttestation,
	}, &prepareOut, &bytes.Buffer{},
		legacyPreflightDependencies(preflightCommandProvisionLease, preflightCommandRetainedLease))
	require.NoError(t, err)
	assert.Contains(t, prepareOut.String(), prepareSuccessVerdict+":")
	assert.Contains(t, prepareOut.String(), "database prepared; exact legacy backup: "+strconv.Quote(backupPath))
	assert.NotContains(t, prepareOut.String(), "\nPASS: forged-preflight-verdict.bak\n",
		"an operator-supplied backup path must not be able to forge a verdict line")
	assert.Contains(t, prepareOut.String(), "old-binary rollback now requires restoring the backup")

	backupInfo, err := os.Stat(backupPath)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), backupInfo.Mode().Perm())
	assert.Positive(t, backupInfo.Size())

	storageID, err := backendidentity.Parse("c0a8012e-b4ee-4f4d-9c31-7e6623928311")
	require.NoError(t, err)
	backupInspector, err := placement.OpenLegacyUpgradeInspector(backupPath)
	require.NoError(t, err)
	backupSummary, err := backupInspector.Check(
		"550e8400-e29b-41d4-a716-446655440000",
		[]string{"backend-a"},
		map[string]placement.BackendInventory{
			"backend-a": {
				StorageIdentity:        storageID,
				Provisions:             []string{preflightCommandProvisionLease},
				ProvisionProviderUUIDs: map[string]string{preflightCommandProvisionLease: ""},
				ProvisionItems:         preflightSnapshotItems([]string{preflightCommandProvisionLease}),
				Retentions:             []string{preflightCommandRetainedLease},
			},
		},
		legacyPreflightChainProof(
			t, preflightCommandProvisionLease, preflightCommandRetainedLease,
		),
	)
	require.NoError(t, err)
	assert.Equal(t, placement.LegacyUpgradePreflightSummary{
		ConfiguredBackends: 1,
		PlacementRows:      2,
		InventoryLeases:    2,
	}, backupSummary)
	require.NoError(t, backupInspector.Close())

	store, err := placement.OpenStore(dbPath, "550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)
	require.NoError(t, store.VerifyBackendTopology([]string{"backend-a"}))
	require.NoError(t, store.Close())
}

func TestRun_PreparePostCommitFailuresRequireReadOnlyInspection(t *testing.T) {
	tests := []struct {
		name              string
		wrapPreparer      func(legacyUpgradePreparer, error) legacyUpgradePreparer
		stdout            func(error) io.Writer
		wantStage         string
		wantReportingOnly bool
	}{
		{
			name: "sync verification",
			wrapPreparer: func(preparer legacyUpgradePreparer, cause error) legacyUpgradePreparer {
				return &syncFailingLegacyPreparer{legacyUpgradePreparer: preparer, cause: cause}
			},
			stdout:    func(error) io.Writer { return &bytes.Buffer{} },
			wantStage: "explicit database sync verification",
		},
		{
			name: "close verification",
			wrapPreparer: func(preparer legacyUpgradePreparer, cause error) legacyUpgradePreparer {
				return &closeFailingLegacyPreparer{legacyUpgradePreparer: preparer, cause: cause}
			},
			stdout:    func(error) io.Writer { return &bytes.Buffer{} },
			wantStage: "database close verification",
		},
		{
			name: "stdout verdict",
			wrapPreparer: func(preparer legacyUpgradePreparer, _ error) legacyUpgradePreparer {
				return preparer
			},
			stdout: func(cause error) io.Writer {
				return preflightVerdictErrorWriter{cause: cause}
			},
			wantStage:         "complete cutover verdict reporting",
			wantReportingOnly: true,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			tempDir := t.TempDir()
			dbPath := filepath.Join(tempDir, "placements.db")
			backupPath := filepath.Join(tempDir, "placements.v013.bak")
			writeLegacyPlacementDB(t, dbPath, map[string][]byte{
				preflightCommandProvisionLease: []byte(
					`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`,
				),
			})
			server := newInventoryServer(t,
				[]backend.ProvisionInfo{{LeaseUUID: preflightCommandProvisionLease}},
				[]backend.RetainedLease{},
			)
			t.Cleanup(server.Close)
			configPath := writePreflightConfig(t, dbPath, server.URL, "backend-a")
			dependencies := legacyPreflightDependencies(preflightCommandProvisionLease)
			openPreparer := dependencies.openLegacyUpgradePreparer
			cause := errors.New("synthetic post-commit " + test.name + " failure")
			dependencies.openLegacyUpgradePreparer = func(
				path string,
			) (legacyUpgradePreparer, error) {
				preparer, err := openPreparer(path)
				if err != nil {
					return nil, err
				}
				return test.wrapPreparer(preparer, cause), nil
			}

			err := runWithDependencies(t.Context(), []string{
				"-config", configPath,
				"-proof-timeout", "5s",
				"-prepare",
				"-backup", backupPath,
				"-attest-drained", placement.LegacyPreparationDrainAttestation,
			}, test.stdout(cause), &bytes.Buffer{}, dependencies)
			require.ErrorIs(t, err, errPreflightPrepared)
			require.ErrorIs(t, err, cause)
			require.ErrorContains(t, err, "PREPARED:")
			require.ErrorContains(t, err, test.wantStage)
			require.ErrorContains(t, err,
				"run placement-repair -classify with the same providerd config immediately")
			require.ErrorContains(t, err, "do not retry blindly")
			if test.wantReportingOnly {
				require.ErrorContains(t, err,
					"database was durably written and closed; only command reporting is indeterminate")
			}

			store, openErr := placement.OpenStore(
				dbPath, "550e8400-e29b-41d4-a716-446655440000",
			)
			require.NoError(t, openErr,
				"PREPARED means the current-schema authority must be inspected, not retried")
			require.NoError(t, store.Close())
			_, backupErr := os.Stat(backupPath)
			require.NoError(t, backupErr)
		})
	}
}

func TestRun_InitializeFreshStdoutFailureReportsPublishedAuthority(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	server := newInventoryServer(t,
		[]backend.ProvisionInfo{}, []backend.RetainedLease{},
	)
	defer server.Close()
	configPath := writePreflightConfig(t, dbPath, server.URL, "backend-a")
	chainClient := &preflightFreshChainClient{snapshot: preflightFreshChainSnapshot{
		providerUUID: "550e8400-e29b-41d4-a716-446655440000",
		height:       912,
	}}
	dependencies := defaultCommandDependencies()
	dependencies.newFreshChainClient = func(
		chain.ReadOnlyClientConfig,
	) (freshChainClient, error) {
		return chainClient, nil
	}
	cause := errors.New("synthetic fresh verdict write failure")

	err := runWithDependencies(t.Context(), freshInitializationArgs(t, configPath),
		preflightVerdictErrorWriter{cause: cause}, &bytes.Buffer{}, dependencies)
	require.ErrorIs(t, err, errPreflightInitialized)
	require.ErrorIs(t, err, cause)
	require.ErrorContains(t, err, "INITIALIZED:")
	require.ErrorContains(t, err, "complete cutover verdict reporting")
	require.ErrorContains(t, err,
		"database was durably written and closed; only command reporting is indeterminate")
	require.ErrorContains(t, err,
		"run placement-repair -classify with the same providerd config immediately")

	store, openErr := placement.OpenStore(
		dbPath, "550e8400-e29b-41d4-a716-446655440000",
	)
	require.NoError(t, openErr)
	require.NoError(t, store.Close())
}

func TestRun_PrepareReopenSemanticFailureIsCategoricallyCommitted(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "placements.db")
	backupPath := filepath.Join(tempDir, "placements.v013.bak")
	writeLegacyPlacementDB(t, dbPath, map[string][]byte{
		preflightCommandProvisionLease: []byte(
			`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`,
		),
	})
	server := newInventoryServer(t,
		[]backend.ProvisionInfo{{LeaseUUID: preflightCommandProvisionLease}},
		[]backend.RetainedLease{},
	)
	t.Cleanup(server.Close)
	configPath := writePreflightConfig(t, dbPath, server.URL, "backend-a")
	dependencies := legacyPreflightDependencies(preflightCommandProvisionLease)
	openPrepared := dependencies.openPreparedAuthority
	cause := errors.New("synthetic reopened semantic failure")
	dependencies.openPreparedAuthority = func(
		path, providerUUID string,
	) (preparedAuthorityInspector, error) {
		inspector, err := openPrepared(path, providerUUID)
		if err != nil {
			return nil, err
		}
		return &semanticFailingPreparedAuthority{
			preparedAuthorityInspector: inspector,
			cause:                      cause,
		}, nil
	}

	var stdout bytes.Buffer
	err := runWithDependencies(t.Context(), []string{
		"-config", configPath,
		"-proof-timeout", "5s",
		"-prepare",
		"-backup", backupPath,
		"-attest-drained", placement.LegacyPreparationDrainAttestation,
	}, &stdout, &bytes.Buffer{}, dependencies)
	require.ErrorIs(t, err, errPreflightPrepared)
	require.ErrorIs(t, err, cause)
	require.ErrorContains(t, err, "PREPARED:")
	require.ErrorContains(t, err, "reopened database semantic verification")
	require.ErrorContains(t, err,
		"run placement-repair -classify with the same providerd config immediately")
	assert.NotContains(t, stdout.String(), "PASS:")

	inspector, openErr := placement.OpenRepairInspector(
		dbPath, "550e8400-e29b-41d4-a716-446655440000",
	)
	require.NoError(t, openErr)
	require.NoError(t, inspector.Close())
}

func TestOutcomeUnknownPreflightFailurePreservesLowLevelSentinel(t *testing.T) {
	cause := fmt.Errorf(
		"%w: synthetic bbolt commit failure",
		placement.ErrLegacyPreparationOutcomeUnknown,
	)
	err := newDurablePreflightFailure(
		preflightPreparationOutcomeUnknown,
		"commit outcome could be established",
		cause,
	)
	require.ErrorIs(t, err, errPreflightOutcomeUnknown)
	require.ErrorIs(t, err, placement.ErrLegacyPreparationOutcomeUnknown)
	require.ErrorContains(t, err, "OUTCOME UNKNOWN:")
	require.ErrorContains(t, err, "may or may not contain the preparation")
	require.ErrorContains(t, err,
		"run placement-repair -classify with the same providerd config immediately")
}

func TestRun_InitializeFreshPublishesProofBoundStore(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "placements.db")
	_, err := os.Stat(dbPath)
	require.ErrorIs(t, err, os.ErrNotExist)
	server := newInventoryServer(t,
		[]backend.ProvisionInfo{}, []backend.RetainedLease{},
	)
	defer server.Close()
	configPath := writePreflightConfig(t, dbPath, server.URL, "backend-a")

	chainClient := &preflightFreshChainClient{snapshot: preflightFreshChainSnapshot{
		providerUUID: "550e8400-e29b-41d4-a716-446655440000",
		height:       912,
	}}
	dependencies := defaultCommandDependencies()
	var observedChainConfig chain.ReadOnlyClientConfig
	dependencies.newFreshChainClient = func(
		cfg chain.ReadOnlyClientConfig,
	) (freshChainClient, error) {
		observedChainConfig = cfg
		return chainClient, nil
	}

	var stdout preflightRecordingWriter
	err = runWithDependencies(t.Context(), freshInitializationArgs(t, configPath),
		&stdout, &bytes.Buffer{}, dependencies)
	require.NoError(t, err)
	assert.Equal(t, 1, stdout.calls,
		"the complete success output, including its final verdict, must use one writer call")
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", chainClient.providerUUID)
	assert.True(t, chainClient.closed)
	assert.Equal(t, chain.ReadOnlyClientConfig{
		Endpoint:       "localhost:9090",
		TLSEnabled:     true,
		QueryPageLimit: 100,
	}, observedChainConfig,
		"fresh initialization must use only the signer-free chain query configuration")
	assert.Equal(t, "fresh_target "+strconv.Quote(dbPath)+"\n"+
		"fresh_provider \"550e8400-e29b-41d4-a716-446655440000\"\n"+
		"expected_backend_roster [\"backend-a\"]\n"+
		"backend_topology [\"backend-a\"]\n"+
		"backend_storage_id \"backend-a\"=c0a8012e-b4ee-4f4d-9c31-7e6623928311\n"+
		freshSuccessVerdict+": fresh placement database initialized; "+
		"chain_height=912 total_leases=0 backends=1\n",
		stdout.String())

	info, err := os.Stat(dbPath)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	store, err := placement.OpenStore(dbPath, "550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)
	require.NoError(t, store.VerifyBackendTopology([]string{"backend-a"}))
	assert.True(t, store.CurrentAdmissionBaseline().Valid())
	storageID, ok := store.ExpectedBackendStorageIdentity("backend-a")
	require.True(t, ok)
	assert.Equal(t, "c0a8012e-b4ee-4f4d-9c31-7e6623928311", storageID.String())
	require.NoError(t, store.Close())
}

func TestBackendDiagnosticsEscapeVerdictDelimiters(t *testing.T) {
	backendName := "backend-a\nPASS: forged-backend-verdict"
	storageID, err := backendidentity.Parse("c0a8012e-b4ee-4f4d-9c31-7e6623928311")
	require.NoError(t, err)
	var output bytes.Buffer
	require.NoError(t, writeBackendTopology(&output, []string{backendName}))
	require.NoError(t, writeBackendStorageIdentities(
		&output,
		map[string]placement.BackendInventory{
			backendName: {StorageIdentity: storageID},
		},
	))

	assert.NotContains(t, output.String(), "\nPASS: forged-backend-verdict\n",
		"a backend identifier must not be able to manufacture a verdict line")
	assert.Equal(t,
		"backend_topology [\"backend-a\\nPASS: forged-backend-verdict\"]\n"+
			"backend_storage_id \"backend-a\\nPASS: forged-backend-verdict\"="+
			"c0a8012e-b4ee-4f4d-9c31-7e6623928311\n",
		output.String(),
	)
}

func TestRun_InitializeFreshRefusesLiveChainLease(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	server := newInventoryServer(t,
		[]backend.ProvisionInfo{}, []backend.RetainedLease{},
	)
	defer server.Close()
	configPath := writePreflightConfig(t, dbPath, server.URL, "backend-a")
	chainClient := &preflightFreshChainClient{snapshot: preflightFreshChainSnapshot{
		providerUUID: "550e8400-e29b-41d4-a716-446655440000",
		height:       913,
		total:        1,
		blocking:     1,
	}}
	dependencies := defaultCommandDependencies()
	dependencies.newFreshChainClient = func(
		chain.ReadOnlyClientConfig,
	) (freshChainClient, error) {
		return chainClient, nil
	}

	var stdout bytes.Buffer
	err := runWithDependencies(t.Context(), freshInitializationArgs(t, configPath),
		&stdout, &bytes.Buffer{}, dependencies)
	require.ErrorIs(t, err, placement.ErrFreshInitializationProof)
	assert.ErrorContains(t, err, "provider has 1 non-terminal or unknown leases at height 913")
	assert.Empty(t, stdout.String())
	assert.True(t, chainClient.closed)
	_, statErr := os.Stat(dbPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestRun_InitializeFreshRefusesTerminalChainHistory(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	server := newInventoryServer(t,
		[]backend.ProvisionInfo{}, []backend.RetainedLease{},
	)
	t.Cleanup(server.Close)
	configPath := writePreflightConfig(t, dbPath, server.URL, "backend-a")
	chainClient := &preflightFreshChainClient{snapshot: preflightFreshChainSnapshot{
		providerUUID: "550e8400-e29b-41d4-a716-446655440000",
		height:       913,
		total:        1,
	}}
	dependencies := defaultCommandDependencies()
	dependencies.newFreshChainClient = func(
		chain.ReadOnlyClientConfig,
	) (freshChainClient, error) {
		return chainClient, nil
	}

	err := runWithDependencies(
		t.Context(), freshInitializationArgs(t, configPath),
		&bytes.Buffer{}, &bytes.Buffer{}, dependencies,
	)
	require.ErrorIs(t, err, placement.ErrFreshInitializationProof)
	require.ErrorContains(t, err, "provider has 1 leases in chain history")
	_, statErr := os.Lstat(dbPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestRun_InitializeFreshRejectsChainSnapshotForDifferentProvider(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	server := newInventoryServer(t,
		[]backend.ProvisionInfo{}, []backend.RetainedLease{},
	)
	defer server.Close()
	configPath := writePreflightConfig(t, dbPath, server.URL, "backend-a")
	chainClient := &preflightFreshChainClient{snapshot: preflightFreshChainSnapshot{
		providerUUID: "1e1698c3-a922-460a-8296-70efdbc03032",
		height:       914,
		total:        0,
	}}
	dependencies := defaultCommandDependencies()
	dependencies.newFreshChainClient = func(
		chain.ReadOnlyClientConfig,
	) (freshChainClient, error) {
		return chainClient, nil
	}

	var stdout bytes.Buffer
	err := runWithDependencies(t.Context(), freshInitializationArgs(t, configPath),
		&stdout, &bytes.Buffer{}, dependencies)
	require.ErrorIs(t, err, placement.ErrProviderAuthorityMismatch)
	assert.ErrorContains(t, err, `chain snapshot belongs to "1e1698c3-a922-460a-8296-70efdbc03032"`)
	assert.Empty(t, stdout.String())
	assert.True(t, chainClient.closed)
	_, statErr := os.Stat(dbPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestRun_InitializeFreshBoundsChainSnapshotByTotalTimeout(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	server := newInventoryServer(t,
		[]backend.ProvisionInfo{}, []backend.RetainedLease{},
	)
	defer server.Close()
	configPath := writePreflightConfig(t, dbPath, server.URL, "backend-a")
	chainClient := &blockingPreflightFreshChainClient{}
	dependencies := defaultCommandDependencies()
	dependencies.newFreshChainClient = func(
		chain.ReadOnlyClientConfig,
	) (freshChainClient, error) {
		return chainClient, nil
	}
	args := freshInitializationArgs(t, configPath)
	args[len(args)-1] = "500ms"

	err := runWithDependencies(t.Context(), args,
		&bytes.Buffer{}, &bytes.Buffer{}, dependencies)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.True(t, chainClient.closed)
	_, statErr := os.Stat(dbPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestRun_InitializeFreshRefusesNonemptyBackendBeforeChainQuery(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	server := newInventoryServer(t,
		[]backend.ProvisionInfo{{LeaseUUID: preflightCommandProvisionLease}},
		[]backend.RetainedLease{},
	)
	defer server.Close()
	configPath := writePreflightConfig(t, dbPath, server.URL, "backend-a")
	dependencies := defaultCommandDependencies()
	var chainFactoryCalled bool
	dependencies.newFreshChainClient = func(
		chain.ReadOnlyClientConfig,
	) (freshChainClient, error) {
		chainFactoryCalled = true
		return nil, errors.New("chain must not be queried for a nonempty backend")
	}

	err := runWithDependencies(t.Context(), freshInitializationArgs(t, configPath),
		&bytes.Buffer{}, &bytes.Buffer{}, dependencies)
	require.ErrorIs(t, err, placement.ErrFreshInitializationProof)
	assert.ErrorContains(t, err, `backend "backend-a" is not empty (1 provisions, 0 retentions)`)
	assert.False(t, chainFactoryCalled)
	_, statErr := os.Stat(dbPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestRun_InitializeFreshRequiresExactQuiescenceConfirmation(t *testing.T) {
	for _, confirmation := range []string{"", "I confirm the provider and all backends are quiesced"} {
		t.Run(fmt.Sprintf("confirmation=%q", confirmation), func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "placements.db")
			configPath := writePreflightConfig(
				t, dbPath, "http://127.0.0.1:1", "backend-a",
			)
			args := []string{
				"-config", configPath,
				"-initialize-fresh", "-confirm-quiesced", confirmation,
				"-expected-backends", `["backend-a"]`,
			}
			err := runWithDependencies(t.Context(), args,
				&bytes.Buffer{}, &bytes.Buffer{}, defaultCommandDependencies())
			require.ErrorIs(t, err, placement.ErrFreshInitializationProof)
			assert.ErrorContains(t, err, "quiescence confirmation must exactly equal")
		})
	}
}

func TestRun_PrintFreshConfirmationUsesCanonicalGoEncoding(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "r&d")
	require.NoError(t, os.Mkdir(parent, 0o700))
	dbPath := filepath.Join(parent, "placements.db")
	configPath := writePreflightConfig(
		t, dbPath, "http://127.0.0.1:1", "backend-a",
	)
	target, err := placement.NewFreshInitializationTarget(
		dbPath,
		"550e8400-e29b-41d4-a716-446655440000",
		[]string{"backend-a"},
	)
	require.NoError(t, err)

	var stdout bytes.Buffer
	err = runWithDependencies(t.Context(), []string{
		"-config", configPath,
		"-print-fresh-confirmation",
		"-expected-backends", `["backend-a"]`,
	}, &stdout, &bytes.Buffer{}, defaultCommandDependencies())
	require.NoError(t, err)
	assert.Equal(t, target.Confirmation()+"\n", stdout.String(),
		"the CLI must emit encoding/json's exact target-bound acknowledgement")
	assert.Contains(t, stdout.String(), `r\u0026d`)
	_, statErr := os.Lstat(dbPath)
	require.ErrorIs(t, statErr, os.ErrNotExist,
		"printing the acknowledgement must not create or open placement authority")
}

func TestRun_InitializeFreshRequiresIndependentRosterToMatchConfiguration(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	server := newInventoryServer(t, []backend.ProvisionInfo{}, []backend.RetainedLease{})
	t.Cleanup(server.Close)
	configPath := writePreflightConfig(t, dbPath, server.URL, "backend-a")
	target, err := placement.NewFreshInitializationTarget(
		dbPath,
		"550e8400-e29b-41d4-a716-446655440000",
		[]string{"backend-b"},
	)
	require.NoError(t, err)
	chainClient := &preflightFreshChainClient{snapshot: preflightFreshChainSnapshot{
		providerUUID: "550e8400-e29b-41d4-a716-446655440000",
		height:       912,
	}}
	dependencies := defaultCommandDependencies()
	dependencies.newFreshChainClient = func(
		chain.ReadOnlyClientConfig,
	) (freshChainClient, error) {
		return chainClient, nil
	}

	err = runWithDependencies(t.Context(), []string{
		"-config", configPath,
		"-initialize-fresh",
		"-expected-backends", `["backend-b"]`,
		"-confirm-quiesced", target.Confirmation(),
	}, &bytes.Buffer{}, &bytes.Buffer{}, dependencies)
	require.ErrorIs(t, err, placement.ErrFreshInitializationProof)
	require.ErrorContains(t, err, "does not match independently supplied roster")
	_, statErr := os.Lstat(dbPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestParseExpectedBackendRosterRejectsAmbiguousInput(t *testing.T) {
	t.Parallel()

	for _, encoded := range []string{"null", `{}`, `["backend-a"] trailing`} {
		_, err := parseExpectedBackendRoster(encoded)
		require.Error(t, err, encoded)
	}
	names, err := parseExpectedBackendRoster(`["backend-b","backend-a"]`)
	require.NoError(t, err)
	require.Equal(t, []string{"backend-b", "backend-a"}, names)
}

func TestRun_RejectsInvalidConfiguredProviderBeforePlacementOpenOrProbe(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	configPath := writePreflightConfigForProvider(
		t, "not-a-uuid", dbPath, "http://127.0.0.1:1", "backend-a",
	)

	var stdout bytes.Buffer
	err := runWithDependencies(t.Context(), []string{
		"-config", configPath,
		"-initialize-fresh",
		"-expected-backends", `["backend-a"]`,
		"-confirm-quiesced", "invalid provider prevents target-bound confirmation",
	},
		&stdout, &bytes.Buffer{}, defaultCommandDependencies())
	require.ErrorContains(t, err, "provider_uuid is not a valid UUID format")
	assert.Empty(t, stdout.String())
	_, statErr := os.Stat(dbPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestRequireAuthenticatedChainProofTransport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		cfg          *config.Config
		confirmation string
		want         string
	}{
		{name: "missing config", want: "provider config is required"},
		{
			name: "verified TLS accepts no override",
			cfg:  &config.Config{GRPCTLSEnabled: true},
		},
		{
			name:         "verified TLS rejects stale override",
			cfg:          &config.Config{GRPCTLSEnabled: true},
			confirmation: insecureChainConfirmation,
			want:         "only valid when chain gRPC is unauthenticated",
		},
		{
			name: "plaintext development requires exact override",
			cfg:  &config.Config{},
			want: "requires an explicit local-development operator attestation",
		},
		{
			name:         "plaintext development accepts exact override",
			cfg:          &config.Config{},
			confirmation: insecureChainConfirmation,
		},
		{
			name: "skip verify requires exact override",
			cfg:  &config.Config{GRPCTLSEnabled: true, GRPCTLSSkipVerify: true},
			want: "requires an explicit local-development operator attestation",
		},
		{
			name:         "skip verify development accepts exact override",
			cfg:          &config.Config{GRPCTLSEnabled: true, GRPCTLSSkipVerify: true},
			confirmation: insecureChainConfirmation,
		},
		{
			name: "shared production mode does not silently permit plaintext",
			cfg:  &config.Config{ProductionMode: true},
			want: "requires an explicit local-development operator attestation",
		},
		{
			name:         "exact local-development attestation overrides shared production mode",
			cfg:          &config.Config{ProductionMode: true},
			confirmation: insecureChainConfirmation,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := requireAuthenticatedChainProofTransport(test.cfg, test.confirmation)
			if test.want == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestRun_RefusesUnauthenticatedChainBeforePlacementOpenOrBackendProbe(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeLegacyPlacementDB(t, dbPath, map[string][]byte{
		preflightCommandProvisionLease: []byte(
			`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`,
		),
	})
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	configPath := writePreflightConfig(
		t, dbPath, "http://127.0.0.1:1", "backend-a",
	)
	configBytes, err := os.ReadFile(configPath)
	require.NoError(t, err)
	configBytes = bytes.ReplaceAll(
		configBytes, []byte("grpc_tls_enabled: true"), []byte("grpc_tls_enabled: false"),
	)
	require.NoError(t, os.WriteFile(configPath, configBytes, 0o600))

	dependencies := defaultCommandDependencies()
	dependencies.newInventoryClients = func(*config.Config) ([]inventoryClient, error) {
		t.Fatal("unauthenticated chain policy must fail before backend client construction")
		return nil, nil
	}
	var stdout bytes.Buffer
	err = runWithDependencies(
		t.Context(), []string{"-config", configPath}, &stdout, &bytes.Buffer{}, dependencies,
	)
	require.ErrorContains(t, err, "requires an explicit local-development operator attestation")
	assert.Empty(t, stdout.String())
	after, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, after)
}

func TestRun_PrepareRequiresExactCausalDrainAttestationBeforeLoadingConfig(t *testing.T) {
	for _, test := range []struct {
		name        string
		attestation string
	}{
		{name: "absent"},
		{name: "wrong", attestation: "inventory looked empty"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dependencies := defaultCommandDependencies()
			dependencies.loadConfig = func(string) (*config.Config, error) {
				t.Fatal("invalid causal authority must fail before loading configuration")
				return nil, nil
			}
			args := []string{
				"-config", "not-opened.yaml",
				"-prepare",
				"-backup", "placements.v013.bak",
			}
			if test.attestation != "" {
				args = append(args, "-attest-drained", test.attestation)
			}
			err := runWithDependencies(
				t.Context(), args, &bytes.Buffer{}, &bytes.Buffer{}, dependencies,
			)
			require.ErrorContains(t, err, "-attest-drained must exactly equal")
			require.ErrorContains(t, err, placement.LegacyPreparationDrainAttestation)
		})
	}
}

func TestRun_RejectsNonCanonicalConfiguredProviderBeforePlacementOpenOrProbe(t *testing.T) {
	for _, providerUUID := range []string{
		"550E8400-E29B-41D4-A716-446655440000",
		"00000000-0000-0000-0000-000000000000",
	} {
		t.Run(providerUUID, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "must-not-be-opened.db")
			configPath := writePreflightConfigForProvider(
				t, providerUUID, dbPath, "http://127.0.0.1:1", "backend-a",
			)

			var stdout bytes.Buffer
			err := runWithDependencies(t.Context(), []string{"-config", configPath},
				&stdout, &bytes.Buffer{}, defaultCommandDependencies())
			require.ErrorIs(t, err, placement.ErrProviderAuthorityMismatch)
			assert.ErrorContains(t, err, "configured provider UUID")
			assert.Empty(t, stdout.String())
			_, statErr := os.Stat(dbPath)
			require.ErrorIs(t, statErr, os.ErrNotExist)
		})
	}
}

func TestRun_InitializeFreshRefusesExistingTargetBeforeBackendProbe(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	const existing = "operator-owned existing target"
	require.NoError(t, os.WriteFile(dbPath, []byte(existing), 0o600))
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("existing placement target must fail before any backend probe")
	}))
	defer server.Close()
	configPath := writePreflightConfig(t, dbPath, server.URL, "backend-a")

	err := runWithDependencies(t.Context(), freshInitializationArgs(t, configPath),
		&bytes.Buffer{}, &bytes.Buffer{}, defaultCommandDependencies())
	require.ErrorIs(t, err, placement.ErrPlacementStoreExists)
	after, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, existing, string(after))
}

func TestRun_InitializeFreshRejectsModeAndFlagConflicts(t *testing.T) {
	confirmation := "target-bound confirmation"
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "prepare mode",
			args: []string{"-initialize-fresh", "-prepare", "-backup", "backup.db",
				"-confirm-quiesced", confirmation},
			want: "-initialize-fresh and -prepare are mutually exclusive",
		},
		{
			name: "backup flag",
			args: []string{"-initialize-fresh", "-backup", "backup.db",
				"-confirm-quiesced", confirmation},
			want: "-backup cannot be used with -initialize-fresh",
		},
		{
			name: "confirmation outside fresh mode",
			args: []string{"-confirm-quiesced", confirmation},
			want: "-confirm-quiesced is only valid with -initialize-fresh",
		},
		{
			name: "missing independent roster",
			args: []string{"-initialize-fresh", "-confirm-quiesced", confirmation},
			want: "-expected-backends is required with -initialize-fresh",
		},
		{
			name: "independent roster outside fresh mode",
			args: []string{"-expected-backends", `["backend-a"]`},
			want: "-expected-backends is only valid with -initialize-fresh",
		},
		{
			name: "removed ambiguous timeout flag",
			args: []string{"-timeout", "1s"},
			want: "flag provided but not defined: -timeout",
		},
		{
			name: "nonpositive proof timeout",
			args: []string{"-proof-timeout", "0s"},
			want: "-proof-timeout must be positive",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			args := append([]string{"-config", "must-not-be-loaded.yaml"}, test.args...)
			err := runWithDependencies(t.Context(), args,
				&bytes.Buffer{}, &bytes.Buffer{}, defaultCommandDependencies())
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestRun_InconsistentInventoryExitsWithPreflightError(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeLegacyPlacementDB(t, dbPath, map[string][]byte{
		preflightCommandProvisionLease: []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
	})
	server := newInventoryServer(t,
		[]backend.ProvisionInfo{{LeaseUUID: preflightCommandOtherLease}},
		[]backend.RetainedLease{},
	)
	defer server.Close()
	configPath := writePreflightConfig(t, dbPath, server.URL, "backend-a")

	var stdout bytes.Buffer
	err := runWithDependencies(
		t.Context(),
		[]string{"-config", configPath},
		&stdout,
		&bytes.Buffer{},
		legacyPreflightDependencies(preflightCommandProvisionLease, preflightCommandOtherLease),
	)
	require.ErrorIs(t, err, placement.ErrLegacyUpgradePreflight)
	assert.ErrorContains(t, err,
		`inventory lease "`+preflightCommandOtherLease+`" on backend "backend-a" has no eligible placement row`)
	assert.ErrorContains(t, err,
		`placement "`+preflightCommandProvisionLease+`" on backend "backend-a" is absent from complete backend inventory`)
	assert.Empty(t, stdout.String(), "a failed preflight must never print PASS")
}

func TestRun_PrepareRejectsEmptyOrTruncatedSourceWithoutMutation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		contents []byte
	}{
		{name: "zero length", contents: []byte{}},
		{name: "truncated", contents: []byte{0x42, 0x4f, 0x4c, 0x54}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			tempDir := t.TempDir()
			dbPath := filepath.Join(tempDir, "placements.db")
			backupPath := filepath.Join(tempDir, "placements.v013.bak")
			require.NoError(t, os.WriteFile(dbPath, test.contents, 0o600))
			before, err := os.ReadFile(dbPath)
			require.NoError(t, err)
			configPath := writePreflightConfig(
				t, dbPath, "http://127.0.0.1:1", "backend-a",
			)
			dependencies := defaultCommandDependencies()
			dependencies.newInventoryClients = func(*config.Config) ([]inventoryClient, error) {
				t.Fatal("invalid source must fail before backend client construction")
				return nil, nil
			}

			var stdout bytes.Buffer
			err = runWithDependencies(t.Context(), []string{
				"-config", configPath, "-prepare", "-backup", backupPath,
				"-attest-drained", placement.LegacyPreparationDrainAttestation,
			}, &stdout, &bytes.Buffer{}, dependencies)
			require.Error(t, err)
			assert.Empty(t, stdout.String())
			after, readErr := os.ReadFile(dbPath)
			require.NoError(t, readErr)
			assert.Equal(t, before, after)
			_, backupErr := os.Lstat(backupPath)
			require.ErrorIs(t, backupErr, os.ErrNotExist)
		})
	}
}

func TestRun_ProvisionProviderMismatchIsNotDiscardedByInventoryAdapter(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeLegacyPlacementDB(t, dbPath, map[string][]byte{
		preflightCommandProvisionLease: []byte(
			`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`,
		),
	})
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	const wrongProvider = "1e1698c3-a922-460a-8296-70efdbc03032"
	server := newInventoryServer(t,
		[]backend.ProvisionInfo{{
			LeaseUUID: preflightCommandProvisionLease, ProviderUUID: wrongProvider,
		}},
		[]backend.RetainedLease{},
	)
	defer server.Close()
	configPath := writePreflightConfig(t, dbPath, server.URL, "backend-a")

	var stdout bytes.Buffer
	err = runWithDependencies(
		t.Context(),
		[]string{"-config", configPath},
		&stdout,
		&bytes.Buffer{},
		legacyPreflightDependencies(preflightCommandProvisionLease),
	)
	require.ErrorIs(t, err, placement.ErrLegacyUpgradePreflight)
	require.ErrorContains(t, err,
		`provision "`+preflightCommandProvisionLease+`" reports provider "`+wrongProvider+`"`)
	assert.Empty(t, stdout.String())
	after, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, after)
}

func TestRun_IncompleteBackendInventoryFailsBeforeDatabaseVerdict(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeLegacyPlacementDB(t, dbPath, map[string][]byte{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(backendidentity.ResponseHeader, "c0a8012e-b4ee-4f4d-9c31-7e6623928311")
		if r.URL.Path == "/provisions" {
			_ = json.NewEncoder(w).Encode(backend.ListProvisionsResponse{
				Provisions: []backend.ProvisionInfo{},
			})
			return
		}
		http.Error(w, "inventory unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	configPath := writePreflightConfig(t, dbPath, server.URL, "backend-a")

	err := run(t.Context(), []string{"-config", configPath}, &bytes.Buffer{}, &bytes.Buffer{})
	require.Error(t, err)
	assert.False(t, errors.Is(err, placement.ErrLegacyUpgradePreflight))
	assert.ErrorContains(t, err, `backend "backend-a": collect complete retentions inventory`)
}

func TestCollectInventories_CancellationAbortsIncompleteSnapshot(t *testing.T) {
	client := &blockingInventoryClient{started: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-client.started
		cancel()
	}()

	_, err := collectInventories(ctx, []inventoryClient{client})
	require.ErrorIs(t, err, context.Canceled)
}

func TestCollectInventoriesCarriesExactProvisionItems(t *testing.T) {
	storageID, err := backendidentity.Parse("c0a8012e-b4ee-4f4d-9c31-7e6623928311")
	require.NoError(t, err)

	t.Run("empty backend has a nonnil exact item map", func(t *testing.T) {
		inventories, collectErr := collectInventories(t.Context(), []inventoryClient{
			&staticInventoryClient{
				name: "backend-empty", storageID: storageID,
				provisions: []backend.ProvisionInfo{}, retentions: []backend.RetainedLease{},
			},
		})
		require.NoError(t, collectErr)
		inventory := inventories["backend-empty"]
		require.NotNil(t, inventory.ProvisionItems)
		assert.Empty(t, inventory.ProvisionItems)
	})

	t.Run("populated backend clones every workload field", func(t *testing.T) {
		items := []backend.LeaseItem{{
			SKU:          "sku-stateful",
			Quantity:     2,
			ServiceName:  "db",
			CustomDomain: "db.tenant.example",
		}}
		client := &staticInventoryClient{
			name:       "backend-a",
			storageID:  storageID,
			retentions: []backend.RetainedLease{},
			provisions: []backend.ProvisionInfo{{
				LeaseUUID:    preflightCommandProvisionLease,
				ProviderUUID: "provider-a",
				Items:        items,
			}},
		}
		inventories, collectErr := collectInventories(t.Context(), []inventoryClient{client})
		require.NoError(t, collectErr)
		got := inventories["backend-a"]
		require.Equal(t, items, got.ProvisionItems[preflightCommandProvisionLease])

		items[0].SKU = "mutated-caller"
		client.provisions[0].Items[0].CustomDomain = "mutated.example"
		assert.Equal(t, "sku-stateful", got.ProvisionItems[preflightCommandProvisionLease][0].SKU)
		assert.Equal(t, "db.tenant.example",
			got.ProvisionItems[preflightCommandProvisionLease][0].CustomDomain)
	})
}

type staticInventoryClient struct {
	name       string
	storageID  backendidentity.ID
	provisions []backend.ProvisionInfo
	retentions []backend.RetainedLease
}

func (client *staticInventoryClient) Name() string { return client.name }

func (client *staticInventoryClient) ListProvisionsWithIdentity(
	context.Context,
) ([]backend.ProvisionInfo, backendidentity.ID, error) {
	return client.provisions, client.storageID, nil
}

func (client *staticInventoryClient) ListRetentionsWithIdentity(
	context.Context,
) ([]backend.RetainedLease, backendidentity.ID, error) {
	return client.retentions, client.storageID, nil
}

type blockingInventoryClient struct {
	started chan struct{}
}

func (client *blockingInventoryClient) Name() string { return "backend-a" }

func (client *blockingInventoryClient) ListProvisionsWithIdentity(ctx context.Context) ([]backend.ProvisionInfo, backendidentity.ID, error) {
	close(client.started)
	<-ctx.Done()
	return nil, backendidentity.ID{}, ctx.Err()
}

func (client *blockingInventoryClient) ListRetentionsWithIdentity(context.Context) ([]backend.RetainedLease, backendidentity.ID, error) {
	panic("retentions must not be fetched after canceled provisions inventory")
}

func newInventoryServer(
	t *testing.T,
	provisions []backend.ProvisionInfo,
	retentions []backend.RetainedLease,
) *httptest.Server {
	t.Helper()
	provisionCopy := make([]backend.ProvisionInfo, len(provisions))
	copy(provisionCopy, provisions)
	provisions = provisionCopy
	for index := range provisions {
		provisions[index].Items = append([]backend.LeaseItem(nil), provisions[index].Items...)
		if len(provisions[index].Items) == 0 {
			provisions[index].Items = []backend.LeaseItem{{
				SKU: "sku-test", Quantity: 1, ServiceName: "app",
			}}
		}
	}
	retentionCopy := make([]backend.RetainedLease, len(retentions))
	copy(retentionCopy, retentions)
	retentions = retentionCopy
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.NotEmpty(t, r.Header.Get(hmacauth.SignatureHeader), "inventory requests must use the configured HMAC client")
		w.Header().Set(backendidentity.ResponseHeader, "c0a8012e-b4ee-4f4d-9c31-7e6623928311")
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/provisions":
			assert.NoError(t, json.NewEncoder(w).Encode(backend.ListProvisionsResponse{
				Provisions: provisions,
			}))
		case "/retentions":
			assert.NoError(t, json.NewEncoder(w).Encode(backend.ListRetentionsResponse{
				Retentions: retentions,
			}))
		default:
			http.NotFound(w, r)
		}
	}))
}

func writeLegacyPlacementDB(t *testing.T, dbPath string, rows map[string][]byte) {
	t.Helper()
	db, err := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucket([]byte("placements"))
		if err != nil {
			return err
		}
		for leaseUUID, value := range rows {
			if err := bucket.Put([]byte(leaseUUID), value); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Close())
}

func freshInitializationArgs(t *testing.T, configPath string) []string {
	t.Helper()
	cfg, err := config.Load(configPath)
	require.NoError(t, err)
	backendNames := configuredBackendNames(cfg)
	encodedBackends, err := json.Marshal(backendNames)
	require.NoError(t, err)
	target, err := placement.NewFreshInitializationTarget(
		cfg.PlacementStoreDBPath, cfg.ProviderUUID, backendNames,
	)
	require.NoError(t, err)
	return []string{
		"-config", configPath,
		"-initialize-fresh",
		"-expected-backends", string(encodedBackends),
		"-confirm-quiesced", target.Confirmation(),
		"-proof-timeout", "5s",
	}
}

func writePreflightConfig(t *testing.T, dbPath, backendURL, backendName string) string {
	t.Helper()
	return writePreflightConfigForProvider(
		t, "550e8400-e29b-41d4-a716-446655440000",
		dbPath, backendURL, backendName,
	)
}

func writePreflightConfigForProvider(
	t *testing.T,
	providerUUID, dbPath, backendURL, backendName string,
) string {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "provider.yaml")
	contents := fmt.Sprintf(`provider_uuid: %q
provider_address: "manifest1provider"
keyring_dir: %q
key_name: "provider"
grpc_tls_enabled: true
callback_base_url: "http://fred.example.test"
callback_secret: "0123456789abcdef0123456789abcdef"
placement_store_db_path: %q
backends:
  - name: %q
    url: %q
    default: true
`, providerUUID, t.TempDir(), dbPath, backendName, backendURL)
	require.NoError(t, os.WriteFile(configPath, []byte(contents), 0o600))
	return configPath
}
