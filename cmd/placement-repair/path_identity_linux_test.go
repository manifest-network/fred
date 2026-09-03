package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/provisioner/placement"
)

func TestRun_RenameOverOpenAfterBackupCannotProducePass(t *testing.T) {
	dbPath := createRepairCommandDatabase(t, false)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	replacementPath := filepath.Join(filepath.Dir(dbPath), "replacement-placements.db")
	displacedPath := filepath.Join(filepath.Dir(dbPath), "opened-placements.db")
	require.NoError(t, os.WriteFile(replacementPath, before, 0o600))
	server := newRepairInventoryServer(t, nil, nil)
	t.Cleanup(server.Close)
	configPath := writeRepairConfig(t, dbPath, server.URL, repairCommandBackend)
	backupPath := filepath.Join(t.TempDir(), "placements.pre-repair.bak")
	dependencies := defaultCommandDependencies()
	dependencies.createExactBackup = func(
		repair *placement.AttemptRepair,
		target *placement.ExactBackupTarget,
	) error {
		if err := repair.CreateExactBackup(target); err != nil {
			return err
		}
		if err := os.Rename(dbPath, displacedPath); err != nil {
			return err
		}
		return os.Rename(replacementPath, dbPath)
	}
	var stdout bytes.Buffer
	err = runWithDependencies(t.Context(), append(repairArgs(configPath),
		"-apply", "-backup", backupPath,
		"-confirm", repairConfirmation(),
		"-attest-drained", drainedAttestation,
	), &stdout, &bytes.Buffer{}, dependencies)

	require.Error(t, err)
	require.ErrorContains(t, err, "BACKUP PUBLISHED:")
	require.ErrorContains(t, err, "no repair mutation committed")
	require.ErrorContains(t, err, "no longer identifies the opened repair database")
	assert.False(t, errors.Is(err, errRepairCommitted))
	assert.False(t, errors.Is(err, errRepairOutcomeUnknown))
	assert.Empty(t, stdout.String(), "a renamed repair target must never produce PASS")

	for _, path := range []string{dbPath, displacedPath, backupPath} {
		contents, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		assert.Equal(t, before, contents, "path %q must preserve the pre-mutation image", path)
	}
}

func TestRun_BackupParentReplacementBeforePublicationCannotMutate(t *testing.T) {
	dbPath := createRepairCommandDatabase(t, false)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	server := newRepairInventoryServer(t, nil, nil)
	t.Cleanup(server.Close)
	configPath := writeRepairConfig(t, dbPath, server.URL, repairCommandBackend)

	root := t.TempDir()
	backupDir := filepath.Join(root, "backup")
	displacedBackupDir := filepath.Join(root, "displaced-backup")
	require.NoError(t, os.Mkdir(backupDir, 0o700))
	backupPath := filepath.Join(backupDir, "placements.pre-repair.bak")
	dependencies := defaultCommandDependencies()
	dependencies.createExactBackup = func(
		repair *placement.AttemptRepair,
		target *placement.ExactBackupTarget,
	) error {
		require.NoError(t, os.Rename(backupDir, displacedBackupDir))
		require.NoError(t, os.Mkdir(backupDir, 0o700))
		return repair.CreateExactBackup(target)
	}

	var stdout bytes.Buffer
	err = runWithDependencies(t.Context(), append(repairArgs(configPath),
		"-apply", "-backup", backupPath,
		"-confirm", repairConfirmation(),
		"-attest-drained", drainedAttestation,
	), &stdout, &bytes.Buffer{}, dependencies)

	require.ErrorContains(t, err, "exact backup parent directory changed")
	require.ErrorContains(t, err, "directory identity changed")
	assert.NotErrorIs(t, err, placement.ErrExactBackupPublished,
		"the parent replacement must be rejected before publication")
	assert.NotErrorIs(t, err, errRepairCommitted)
	assert.NotErrorIs(t, err, errRepairOutcomeUnknown)
	assert.Empty(t, stdout.String(), "a replaced backup parent must never produce PASS")

	after, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, after,
		"backup-target rejection must leave the repair database byte-exact")
	for _, path := range []string{
		backupPath,
		filepath.Join(displacedBackupDir, filepath.Base(backupPath)),
	} {
		_, statErr := os.Lstat(path)
		require.ErrorIs(t, statErr, os.ErrNotExist,
			"a pre-publication rejection must not create a rollback artifact")
	}
}

func TestRun_BackupParentReplacementAfterPublicationCannotProducePass(t *testing.T) {
	dbPath := createRepairCommandDatabase(t, false)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	server := newRepairInventoryServer(t, nil, nil)
	t.Cleanup(server.Close)
	configPath := writeRepairConfig(t, dbPath, server.URL, repairCommandBackend)

	root := t.TempDir()
	backupDir := filepath.Join(root, "backup")
	displacedBackupDir := filepath.Join(root, "displaced-backup")
	require.NoError(t, os.Mkdir(backupDir, 0o700))
	backupPath := filepath.Join(backupDir, "placements.pre-repair.bak")
	displacedBackupPath := filepath.Join(displacedBackupDir, filepath.Base(backupPath))
	dependencies := defaultCommandDependencies()
	dependencies.createExactBackup = func(
		repair *placement.AttemptRepair,
		target *placement.ExactBackupTarget,
	) error {
		if backupErr := repair.CreateExactBackup(target); backupErr != nil {
			return backupErr
		}
		if renameErr := os.Rename(backupDir, displacedBackupDir); renameErr != nil {
			return renameErr
		}
		return os.Mkdir(backupDir, 0o700)
	}

	var stdout bytes.Buffer
	err = runWithDependencies(t.Context(), append(repairArgs(configPath),
		"-apply", "-backup", backupPath,
		"-confirm", repairConfirmation(),
		"-attest-drained", drainedAttestation,
	), &stdout, &bytes.Buffer{}, dependencies)

	require.ErrorIs(t, err, placement.ErrExactBackupPublished)
	require.ErrorContains(t, err, "BACKUP PUBLISHED:")
	require.ErrorContains(t, err, "no repair mutation committed")
	require.ErrorContains(t, err, "directory identity changed")
	assert.NotErrorIs(t, err, errRepairCommitted)
	assert.NotErrorIs(t, err, errRepairOutcomeUnknown)
	assert.Empty(t, stdout.String(), "a displaced published backup must never produce PASS")

	after, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, after,
		"loss of the published backup pathname must fail before repair mutation")
	_, statErr := os.Lstat(backupPath)
	require.ErrorIs(t, statErr, os.ErrNotExist,
		"the recreated pathname must not receive a second backup")
	displacedBackup, readErr := os.ReadFile(displacedBackupPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, displacedBackup,
		"the exact rollback image must remain only in the displaced original parent")
}
