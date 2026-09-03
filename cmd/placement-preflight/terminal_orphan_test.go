package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

func terminalOrphanCommandDependencies(
	t *testing.T,
	snapshot preflightFreshChainSnapshot,
) (commandDependencies, *preflightFreshChainClient) {
	t.Helper()
	dependencies := defaultCommandDependencies()
	chainClient := &preflightFreshChainClient{snapshot: snapshot}
	dependencies.newFreshChainClient = func(
		chain.ReadOnlyClientConfig,
	) (freshChainClient, error) {
		return chainClient, nil
	}
	dependencies.newInventoryClients = func(*config.Config) ([]inventoryClient, error) {
		t.Fatal("terminal orphan proof must not call a backend inventory endpoint")
		return nil, nil
	}
	return dependencies, chainClient
}

func terminalOrphanCommandSnapshot(
	leaseUUIDs []string,
	blockingLeaseUUIDs ...string,
) preflightFreshChainSnapshot {
	return preflightFreshChainSnapshot{
		providerUUID:       "550e8400-e29b-41d4-a716-446655440000",
		height:             913,
		total:              len(leaseUUIDs),
		blocking:           len(blockingLeaseUUIDs),
		blockingLeaseUUIDs: append([]string(nil), blockingLeaseUUIDs...),
		leaseUUIDs:         append([]string(nil), leaseUUIDs...),
		leaseItems:         preflightSnapshotItems(leaseUUIDs),
	}
}

func terminalOrphanCommandArgs(configPath string) []string {
	return []string{
		"-config", configPath,
		"-prove-terminal-orphan", preflightCommandProvisionLease,
		"-expected-backend", "backend-a",
		"-proof-timeout", "5s",
	}
}

func TestRun_TerminalOrphanProofIsReadOnlyAndEmitsOneBoundedVerdict(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeLegacyPlacementDB(t, dbPath, map[string][]byte{
		preflightCommandOtherLease: []byte("backend-b"),
	})
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	configPath := writePreflightConfig(
		t,
		dbPath,
		"http://backend.invalid",
		"backend-a",
	)
	dependencies, chainClient := terminalOrphanCommandDependencies(
		t,
		terminalOrphanCommandSnapshot([]string{
			preflightCommandProvisionLease,
			preflightCommandOtherLease,
		}),
	)

	var stdout preflightRecordingWriter
	var stderr bytes.Buffer
	err = runWithDependencies(
		t.Context(),
		terminalOrphanCommandArgs(configPath),
		&stdout,
		&stderr,
		dependencies,
	)
	require.NoError(t, err)
	assert.Empty(t, stderr.String())
	assert.Equal(t, 1, stdout.calls)
	assert.Equal(t,
		`TERMINAL_ORPHAN_PROVED: lease="018f47a2-8b1c-7def-8123-456789abcdef" backend="backend-a" provider="550e8400-e29b-41d4-a716-446655440000" chain_height=913 placement=absent`+"\n",
		stdout.String(),
	)
	assert.True(t, chainClient.closed)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", chainClient.providerUUID)
	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, after, "terminal orphan proof must not write one database byte")
}

func TestRun_TerminalOrphanProofRejectsAbsentOrBlockingChainTarget(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		snapshot preflightFreshChainSnapshot
		want     string
	}{
		"absent": {
			snapshot: terminalOrphanCommandSnapshot([]string{preflightCommandOtherLease}),
			want:     "absent from the height-913 all-state chain snapshot",
		},
		"blocking": {
			snapshot: terminalOrphanCommandSnapshot(
				[]string{preflightCommandProvisionLease},
				preflightCommandProvisionLease,
			),
			want: "non-terminal or unknown",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dbPath := filepath.Join(t.TempDir(), "placements.db")
			writeLegacyPlacementDB(t, dbPath, map[string][]byte{})
			before, err := os.ReadFile(dbPath)
			require.NoError(t, err)
			configPath := writePreflightConfig(
				t, dbPath, "http://backend.invalid", "backend-a",
			)
			dependencies, chainClient := terminalOrphanCommandDependencies(t, test.snapshot)
			var stdout, stderr bytes.Buffer
			err = runWithDependencies(
				t.Context(), terminalOrphanCommandArgs(configPath),
				&stdout, &stderr, dependencies,
			)
			require.ErrorIs(t, err, placement.ErrTerminalOrphanProof)
			require.ErrorContains(t, err, test.want)
			assert.Empty(t, stdout.String())
			assert.Empty(t, stderr.String())
			assert.True(t, chainClient.closed)
			after, readErr := os.ReadFile(dbPath)
			require.NoError(t, readErr)
			assert.Equal(t, before, after)
		})
	}
}

func TestRun_TerminalOrphanProofClassifiesExactV013Placement(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		rows        map[string][]byte
		wantError   error
		wantMessage string
	}{
		"residual expected owner": {
			rows: map[string][]byte{
				preflightCommandProvisionLease: []byte("backend-a"),
			},
			wantError:   placement.ErrTerminalOrphanResidualPlacement,
			wantMessage: "complete matching v0.13 fleet",
		},
		"wrong owner": {
			rows: map[string][]byte{
				preflightCommandProvisionLease: []byte("backend-b"),
			},
			wantError:   placement.ErrTerminalOrphanProof,
			wantMessage: `residual v0.13 owner "backend-b"`,
		},
		"current epoch": {
			rows: map[string][]byte{
				preflightCommandProvisionLease: []byte(`{"revision":1,"confirmed_backend":"backend-a"}`),
			},
			wantError:   placement.ErrTerminalOrphanProof,
			wantMessage: "already revisioned",
		},
		"invalid unrelated row": {
			rows: map[string][]byte{
				preflightCommandOtherLease: {0},
			},
			wantError:   placement.ErrTerminalOrphanProof,
			wantMessage: "not an unambiguous v0.13 confirmed owner",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dbPath := filepath.Join(t.TempDir(), "placements.db")
			writeLegacyPlacementDB(t, dbPath, test.rows)
			configPath := writePreflightConfig(
				t, dbPath, "http://backend.invalid", "backend-a",
			)
			dependencies, _ := terminalOrphanCommandDependencies(
				t,
				terminalOrphanCommandSnapshot([]string{preflightCommandProvisionLease}),
			)
			var stdout, stderr bytes.Buffer
			err := runWithDependencies(
				t.Context(), terminalOrphanCommandArgs(configPath),
				&stdout, &stderr, dependencies,
			)
			require.ErrorIs(t, err, test.wantError)
			require.ErrorContains(t, err, test.wantMessage)
			assert.Empty(t, stdout.String())
			assert.Empty(t, stderr.String())
		})
	}
}

func TestRun_TerminalOrphanProofRejectsMissingOrLockedDatabaseBeforeChain(t *testing.T) {
	t.Run("missing path", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "missing.db")
		configPath := writePreflightConfig(
			t, dbPath, "http://backend.invalid", "backend-a",
		)
		dependencies, _ := terminalOrphanCommandDependencies(
			t,
			terminalOrphanCommandSnapshot([]string{preflightCommandProvisionLease}),
		)
		chainCalled := false
		dependencies.newFreshChainClient = func(
			chain.ReadOnlyClientConfig,
		) (freshChainClient, error) {
			chainCalled = true
			return nil, errors.New("must not be called")
		}
		var stdout, stderr bytes.Buffer
		err := runWithDependencies(
			t.Context(), terminalOrphanCommandArgs(configPath),
			&stdout, &stderr, dependencies,
		)
		require.Error(t, err)
		assert.False(t, chainCalled)
		assert.Empty(t, stdout.String())
		_, statErr := os.Lstat(dbPath)
		assert.ErrorIs(t, statErr, os.ErrNotExist)
	})

	t.Run("active writer lock", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "placements.db")
		writeLegacyPlacementDB(t, dbPath, map[string][]byte{})
		writer, err := bolt.Open(dbPath, 0o600, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, writer.Close()) }()
		configPath := writePreflightConfig(
			t, dbPath, "http://backend.invalid", "backend-a",
		)
		dependencies, _ := terminalOrphanCommandDependencies(
			t,
			terminalOrphanCommandSnapshot([]string{preflightCommandProvisionLease}),
		)
		chainCalled := false
		dependencies.newFreshChainClient = func(
			chain.ReadOnlyClientConfig,
		) (freshChainClient, error) {
			chainCalled = true
			return nil, errors.New("must not be called")
		}
		var stdout, stderr bytes.Buffer
		err = runWithDependencies(
			t.Context(), terminalOrphanCommandArgs(configPath),
			&stdout, &stderr, dependencies,
		)
		require.ErrorContains(t, err, "lock stopped placement db read-only")
		assert.False(t, chainCalled)
		assert.Empty(t, stdout.String())
	})
}

func TestRun_TerminalOrphanProofCancellationClosesStableDatabase(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeLegacyPlacementDB(t, dbPath, map[string][]byte{})
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	configPath := writePreflightConfig(
		t, dbPath, "http://backend.invalid", "backend-a",
	)
	dependencies := defaultCommandDependencies()
	chainClient := &blockingPreflightFreshChainClient{}
	dependencies.newFreshChainClient = func(
		chain.ReadOnlyClientConfig,
	) (freshChainClient, error) {
		return chainClient, nil
	}
	dependencies.newInventoryClients = func(*config.Config) ([]inventoryClient, error) {
		t.Fatal("terminal orphan proof must not call backend inventory")
		return nil, nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var stdout, stderr bytes.Buffer
	err = runWithDependencies(
		ctx, terminalOrphanCommandArgs(configPath),
		&stdout, &stderr, dependencies,
	)
	require.ErrorIs(t, err, context.Canceled)
	assert.True(t, chainClient.closed)
	assert.Empty(t, stdout.String())
	after, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, after)

	// The command released its shared lock on every canceled path.
	writer, openErr := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, openErr)
	require.NoError(t, writer.Close())
}

func TestRun_TerminalOrphanFlagsAreExclusiveAndCanonical(t *testing.T) {
	t.Parallel()

	base := []string{"-config", "unused.yaml"}
	tests := map[string][]string{
		"explicit empty pair": {
			"-prove-terminal-orphan=",
			"-expected-backend=",
		},
		"explicit empty target": {
			"-prove-terminal-orphan=",
			"-expected-backend", "backend-a",
		},
		"explicit empty backend": {
			"-prove-terminal-orphan", preflightCommandProvisionLease,
			"-expected-backend=",
		},
		"missing expected backend": {
			"-prove-terminal-orphan", preflightCommandProvisionLease,
		},
		"missing target lease": {
			"-expected-backend", "backend-a",
		},
		"noncanonical target": {
			"-prove-terminal-orphan", strings.ToUpper(preflightCommandProvisionLease),
			"-expected-backend", "backend-a",
		},
		"prepare mode": {
			"-prove-terminal-orphan", preflightCommandProvisionLease,
			"-expected-backend", "backend-a",
			"-prepare",
		},
		"fresh mode": {
			"-prove-terminal-orphan", preflightCommandProvisionLease,
			"-expected-backend", "backend-a",
			"-initialize-fresh",
		},
		"confirmation mode": {
			"-prove-terminal-orphan", preflightCommandProvisionLease,
			"-expected-backend", "backend-a",
			"-print-fresh-confirmation",
		},
		"plural expected roster": {
			"-prove-terminal-orphan", preflightCommandProvisionLease,
			"-expected-backend", "backend-a",
			"-expected-backends", `["backend-a"]`,
		},
		"backup": {
			"-prove-terminal-orphan", preflightCommandProvisionLease,
			"-expected-backend", "backend-a",
			"-backup", "backup.db",
		},
	}
	for name, extra := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var stdout, stderr bytes.Buffer
			err := runWithDependencies(
				t.Context(), append(append([]string(nil), base...), extra...),
				&stdout, &stderr, commandDependencies{},
			)
			require.Error(t, err)
			assert.Empty(t, stdout.String())
			assert.Empty(t, stderr.String())
		})
	}
}

func TestRun_TerminalOrphanProofOutputFailureNeverReturnsSuccess(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeLegacyPlacementDB(t, dbPath, map[string][]byte{})
	configPath := writePreflightConfig(
		t, dbPath, "http://backend.invalid", "backend-a",
	)
	dependencies, _ := terminalOrphanCommandDependencies(
		t,
		terminalOrphanCommandSnapshot([]string{preflightCommandProvisionLease}),
	)
	cause := errors.New("stdout unavailable")
	var stderr bytes.Buffer
	err := runWithDependencies(
		t.Context(), terminalOrphanCommandArgs(configPath),
		preflightVerdictErrorWriter{cause: cause}, &stderr, dependencies,
	)
	require.ErrorIs(t, err, cause)
	require.ErrorContains(t, err, "write terminal orphan verdict")
	assert.Empty(t, stderr.String())
}

type closeFailingTerminalOrphanInspector struct {
	legacyUpgradeInspector
	cause error
}

func (inspector *closeFailingTerminalOrphanInspector) Close() error {
	return errors.Join(inspector.legacyUpgradeInspector.Close(), inspector.cause)
}

func TestRun_TerminalOrphanProofCloseFailureSuppressesVerdict(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeLegacyPlacementDB(t, dbPath, map[string][]byte{})
	configPath := writePreflightConfig(
		t, dbPath, "http://backend.invalid", "backend-a",
	)
	dependencies, _ := terminalOrphanCommandDependencies(
		t,
		terminalOrphanCommandSnapshot([]string{preflightCommandProvisionLease}),
	)
	cause := errors.New("close verification failed")
	realOpen := dependencies.openLegacyUpgradeInspector
	dependencies.openLegacyUpgradeInspector = func(path string) (legacyUpgradeInspector, error) {
		inspector, err := realOpen(path)
		if err != nil {
			return nil, err
		}
		return &closeFailingTerminalOrphanInspector{
			legacyUpgradeInspector: inspector,
			cause:                  cause,
		}, nil
	}
	var stdout, stderr bytes.Buffer
	err := runWithDependencies(
		t.Context(), terminalOrphanCommandArgs(configPath),
		&stdout, &stderr, dependencies,
	)
	require.ErrorIs(t, err, cause)
	assert.Empty(t, stdout.String(), "a failed database close cannot emit a cleanup verdict")
	assert.Empty(t, stderr.String())
}
