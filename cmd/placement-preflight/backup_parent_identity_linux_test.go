//go:build linux

package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

type swapBackupParentAfterAuthorizationPreparer struct {
	legacyUpgradePreparer
	swap func() error
}

func (preparer *swapBackupParentAfterAuthorizationPreparer) AuthorizePreparation(
	ctx context.Context,
	providerUUID string,
	backendNames []string,
	inventories map[string]placement.BackendInventory,
	chainProof placement.LegacyUpgradeChainProof,
	target *placement.ExactBackupTarget,
	attestation string,
) (placement.LegacyPreparationCapability, error) {
	capability, err := preparer.legacyUpgradePreparer.AuthorizePreparation(
		ctx, providerUUID, backendNames, inventories, chainProof, target, attestation,
	)
	if err != nil {
		return placement.LegacyPreparationCapability{}, err
	}
	if err := preparer.swap(); err != nil {
		return placement.LegacyPreparationCapability{}, err
	}
	return capability, nil
}

func TestRun_PrepareRejectsBackupParentReplacementAfterAuthorization(t *testing.T) {
	root := t.TempDir()
	databaseDir := filepath.Join(root, "database")
	backupDir := filepath.Join(root, "backup")
	displacedBackupDir := filepath.Join(root, "displaced-backup")
	require.NoError(t, os.Mkdir(databaseDir, 0o700))
	require.NoError(t, os.Mkdir(backupDir, 0o700))

	dbPath := filepath.Join(databaseDir, "placements.db")
	backupPath := filepath.Join(backupDir, "placements.v013.bak")
	writeLegacyPlacementDB(t, dbPath, map[string][]byte{
		preflightCommandProvisionLease: []byte(
			`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`,
		),
	})
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	server := newInventoryServer(t,
		[]backend.ProvisionInfo{{LeaseUUID: preflightCommandProvisionLease}},
		[]backend.RetainedLease{},
	)
	t.Cleanup(server.Close)
	configPath := writePreflightConfig(t, dbPath, server.URL, "backend-a")
	dependencies := legacyPreflightDependencies(preflightCommandProvisionLease)
	openPreparer := dependencies.openLegacyUpgradePreparer
	dependencies.openLegacyUpgradePreparer = func(path string) (legacyUpgradePreparer, error) {
		preparer, openErr := openPreparer(path)
		if openErr != nil {
			return nil, openErr
		}
		return &swapBackupParentAfterAuthorizationPreparer{
			legacyUpgradePreparer: preparer,
			swap: func() error {
				if renameErr := os.Rename(backupDir, displacedBackupDir); renameErr != nil {
					return renameErr
				}
				return os.Mkdir(backupDir, 0o700)
			},
		}, nil
	}

	var stdout bytes.Buffer
	err = runWithDependencies(t.Context(), []string{
		"-config", configPath,
		"-proof-timeout", "5s",
		"-prepare",
		"-backup", backupPath,
		"-attest-drained", placement.LegacyPreparationDrainAttestation,
	}, &stdout, &bytes.Buffer{}, dependencies)
	require.ErrorContains(t, err, "bound exact backup target changed")
	require.ErrorContains(t, err, "directory identity changed")
	assert.NotErrorIs(t, err, placement.ErrExactBackupPublished,
		"the replaced parent must be rejected before publication")
	assert.Empty(t, stdout.String(), "a replaced backup parent must never produce a verdict")

	after, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, after,
		"backup-target rejection must leave the stopped legacy database byte-exact")
	for _, path := range []string{
		backupPath,
		filepath.Join(displacedBackupDir, filepath.Base(backupPath)),
	} {
		_, statErr := os.Lstat(path)
		require.ErrorIs(t, statErr, os.ErrNotExist,
			"authorization invalidation must fail before publishing any backup")
	}
}
