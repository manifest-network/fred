package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// failingLegacyPreparer fails preparation with an exact error after the real
// capability was authorized, without running the real preparation.
type failingLegacyPreparer struct {
	legacyUpgradePreparer
	err error
}

func (preparer *failingLegacyPreparer) PrepareContext(
	context.Context,
	string,
	[]string,
	map[string]placement.BackendInventory,
	placement.LegacyUpgradeChainProof,
	placement.LegacyPreparationCapability,
) (placement.LegacyUpgradePreflightSummary, error) {
	return placement.LegacyUpgradePreflightSummary{}, preparer.err
}

func TestRun_PrepareReportsEachPreparerOutcomeClass(t *testing.T) {
	for _, test := range []struct {
		name     string
		err      error
		outcome  string
		exitCode int
	}{
		{
			name:     "backup published",
			err:      fmt.Errorf("%w: synthetic proof expiry", placement.ErrExactBackupPublished),
			outcome:  "backup_published",
			exitCode: 11,
		},
		{
			name:     "outcome unknown",
			err:      fmt.Errorf("%w: synthetic commit failure", placement.ErrLegacyPreparationOutcomeUnknown),
			outcome:  "outcome_unknown",
			exitCode: 12,
		},
		{
			name:     "before capability consumption",
			err:      errors.New("synthetic refusal before consumption"),
			outcome:  "not_mutated",
			exitCode: 10,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			tempDir := t.TempDir()
			dbPath := filepath.Join(tempDir, "placements.db")
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
			dependencies.openLegacyUpgradePreparer = func(path string) (legacyUpgradePreparer, error) {
				preparer, err := openPreparer(path)
				if err != nil {
					return nil, err
				}
				return &failingLegacyPreparer{legacyUpgradePreparer: preparer, err: test.err}, nil
			}

			var stdout bytes.Buffer
			err := runWithDependencies(t.Context(), []string{
				"-config", configPath,
				"-proof-timeout", "5s",
				"-prepare",
				"-backup", filepath.Join(tempDir, "placements.v013.bak"),
				"-attest-drained", placement.LegacyPreparationDrainAttestation,
			}, &stdout, &bytes.Buffer{}, dependencies)
			require.ErrorIs(t, err, test.err)
			assert.Equal(t, test.exitCode, commandExitCode(err))
			assert.Equal(t, `{"outcome":"`+test.outcome+`"}`+"\n", stdout.String())
		})
	}
}

func TestClassifyPreparationFailureFailsTowardTheMoreSevereOutcome(t *testing.T) {
	plain := errors.New("synthetic")
	published := fmt.Errorf("%w: synthetic", placement.ErrExactBackupPublished)
	committed := fmt.Errorf("%w: synthetic", placement.ErrLegacyPreparationCommitted)
	unknown := fmt.Errorf("%w: synthetic", placement.ErrLegacyPreparationOutcomeUnknown)
	for _, test := range []struct {
		name   string
		status durablePreflightStatus
		err    error
		want   preparationOutcome
	}{
		{"plain error", preflightNotMutated, plain, preparationNotMutated},
		{"published backup", preflightNotMutated, published, preparationBackupPublished},
		{"committed sentinel", preflightNotMutated, committed, preparationPreparedUnverified},
		{"unknown sentinel", preflightNotMutated, unknown, preparationOutcomeUnknown},
		// A plain error after the commit, such as a failed verdict render, is
		// still classified by the durable status the command recorded.
		{"plain error after commit", preflightPrepared, plain, preparationPreparedUnverified},
		{"published backup after commit", preflightPrepared, published, preparationPreparedUnverified},
		{"plain error after unknown commit", preflightPreparationOutcomeUnknown, plain, preparationOutcomeUnknown},
		{"committed sentinel after unknown commit", preflightPreparationOutcomeUnknown, committed, preparationOutcomeUnknown},
		{"both sentinels", preflightNotMutated, errors.Join(committed, unknown), preparationOutcomeUnknown},
	} {
		assert.Equal(t, test.want, classifyPreparationFailure(test.status, test.err), test.name)
	}
}

func TestPreparationOutcomeExitCodesAreDistinctAndClosed(t *testing.T) {
	codes := map[int]preparationOutcome{}
	for _, outcome := range []preparationOutcome{
		preparationPrepared, preparationNotMutated, preparationBackupPublished,
		preparationOutcomeUnknown, preparationPreparedUnverified,
	} {
		code := outcome.exitCode()
		_, duplicate := codes[code]
		require.False(t, duplicate, "exit code %d is reused", code)
		require.NotEqual(t, 1, code, "1 is reserved for failures outside -prepare")
		codes[code] = outcome
		assert.NotEqual(t, "invalid", outcome.String())
	}
	var zero preparationOutcome
	assert.Equal(t, 1, zero.exitCode())
	assert.Equal(t, "invalid", zero.String())
	assert.Equal(t, 1, preparationOutcome(99).exitCode())
	assert.Equal(t, 1, commandExitCode(errors.New("not a -prepare failure")))
}

func TestRun_InspectFailureKeepsGenericExitCodeAndPrintsNoOutcome(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	require.NoError(t, os.WriteFile(dbPath, nil, 0o600))
	configPath := writePreflightConfig(t, dbPath, "https://127.0.0.1:1", "backend-a")
	var stdout bytes.Buffer
	err := run(t.Context(), []string{"-config", configPath}, &stdout, &bytes.Buffer{})
	require.Error(t, err)
	assert.Equal(t, 1, commandExitCode(err))
	assert.Empty(t, stdout.String())
}
