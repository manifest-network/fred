package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
)

func TestParseStartupFlagsDockerEffectsInspection(t *testing.T) {
	startup, err := parseStartupFlags([]string{
		"-inspect-unsettled-docker-effects", "-config", "/etc/fred/docker.yaml",
		"-storage-identity-operation-timeout", "45m",
	}, io.Discard)
	require.NoError(t, err)
	assert.Equal(t, dockerEffectsInspect, startup.dockerEffects.mode)
	assert.Empty(t, startup.dockerEffects.acknowledgement)
	assert.Empty(t, startup.dockerEffects.backup)
	assert.Equal(t, "/etc/fred/docker.yaml", startup.configPath)
	assert.Equal(t, 45*time.Minute, startup.storageIdentityOperationTimeout)
}

func TestParseStartupFlagsDockerEffectsRepairPreservesExactAcknowledgement(t *testing.T) {
	const acknowledgement = "fenced-exact-storage-and-inspection-digest"
	startup, err := parseStartupFlags([]string{
		"-repair-unsettled-docker-effects", "-docker-effects-acknowledgement", acknowledgement,
		"-docker-effects-backup", "/var/backups/fred/before-repair.db",
	}, io.Discard)
	require.NoError(t, err)
	assert.Equal(t, dockerEffectsRepair, startup.dockerEffects.mode)
	assert.Equal(t, acknowledgement, startup.dockerEffects.acknowledgement)
	assert.Equal(t, "/var/backups/fred/before-repair.db", startup.dockerEffects.backup)
}

func TestParseStartupFlagsRejectsConflictingOfflineModes(t *testing.T) {
	modes := [][]string{
		{"-inspect-unsettled-docker-effects"},
		{"-repair-unsettled-docker-effects", "-docker-effects-acknowledgement", "exact-ack", "-docker-effects-backup", "/backup.db"},
		{"-preflight-storage-identity-adoption"},
		{"-initialize-storage-identity", "adopt"},
	}
	for i, first := range modes {
		for _, second := range modes[i+1:] {
			t.Run(first[0]+"+"+second[0], func(t *testing.T) {
				args := append(append([]string(nil), first...), second...)
				_, err := parseStartupFlags(args, io.Discard)
				require.ErrorContains(t, err, "mutually exclusive")
			})
		}
	}
}

func TestParseStartupFlagsRejectsIncompleteOrMisplacedRepairInputs(t *testing.T) {
	for _, args := range [][]string{
		{"-repair-unsettled-docker-effects"},
		{"-repair-unsettled-docker-effects", "-docker-effects-acknowledgement", "ack"},
		{"-repair-unsettled-docker-effects", "-docker-effects-backup", "/backup.db"},
		{"-repair-unsettled-docker-effects", "-docker-effects-acknowledgement", " ", "-docker-effects-backup", "/backup.db"},
		{"-inspect-unsettled-docker-effects", "-docker-effects-acknowledgement", "ack"},
		{"-inspect-unsettled-docker-effects", "-docker-effects-backup", "/backup.db"},
		{"-docker-effects-acknowledgement", ""},
		{"-docker-effects-backup", "/backup.db"},
		{"-preflight-storage-identity-adoption", "-docker-effects-backup", "/backup.db"},
	} {
		t.Run(args[0]+"_"+args[len(args)-1], func(t *testing.T) {
			_, err := parseStartupFlags(args, io.Discard)
			require.Error(t, err)
		})
	}
}

func TestWriteDockerEffectsResultPreservesStructuredInspectionAndRepair(t *testing.T) {
	inspection := shared.DockerRecoveryInspection{
		Verdict: "DOCKER_EFFECTS_INSPECTED", Backend: "docker-a", StorageID: "storage-a",
		Database: "/var/lib/fred/callbacks.db", SnapshotSHA256: "digest", Launches: 1,
		Leases: []string{"lease-a"}, Acknowledgement: "exact fencing statement",
	}
	var output bytes.Buffer
	require.NoError(t, writeDockerEffectsResult(&output, inspection))
	var decoded shared.DockerRecoveryInspection
	require.NoError(t, json.Unmarshal(output.Bytes(), &decoded))
	require.Equal(t, inspection, decoded)
	require.True(t, bytes.HasSuffix(output.Bytes(), []byte("\n")))

	output.Reset()
	repair := shared.DockerRecoveryRepairResult{
		Verdict: "REPAIR_COMMITTED", Backup: "/var/backups/fred/before-repair.db",
		SnapshotSHA256: "digest", Launches: 1,
	}
	require.NoError(t, writeDockerEffectsResult(&output, repair))
	var decodedRepair shared.DockerRecoveryRepairResult
	require.NoError(t, json.Unmarshal(output.Bytes(), &decodedRepair))
	require.Equal(t, repair, decodedRepair, "incomplete repairs must retain their backup and commit status")
}

func TestWriteDockerEffectsResultRejectsIncompleteOutput(t *testing.T) {
	result := shared.DockerRecoveryRepairResult{Verdict: "DOCKER_EFFECTS_FENCED"}
	err := writeDockerEffectsResult(preflightVerdictWriter(func(data []byte) (int, error) {
		return len(data) - 1, nil
	}), result)
	require.ErrorIs(t, err, io.ErrShortWrite)
	wantErr := errors.New("stdout unavailable")
	err = writeDockerEffectsResult(preflightVerdictWriter(func([]byte) (int, error) {
		return 0, wantErr
	}), result)
	require.ErrorIs(t, err, wantErr)
}

func TestWriteDockerEffectsRepairOutcomePreservesCommitAndBackupOnFailure(t *testing.T) {
	for _, verdict := range []string{"REPAIR_NOT_CONFIRMED", "REPAIR_COMMITTED", "DOCKER_EFFECTS_FENCED"} {
		t.Run(verdict, func(t *testing.T) {
			result := shared.DockerRecoveryRepairResult{Verdict: verdict, Backup: "/backup.db", SnapshotSHA256: "digest"}
			wantErr := errors.New("final close failed")
			var output bytes.Buffer
			err := writeDockerEffectsRepairOutcome(&output, result, wantErr)
			require.ErrorIs(t, err, wantErr)
			var decoded shared.DockerRecoveryRepairResult
			require.NoError(t, json.Unmarshal(output.Bytes(), &decoded))
			require.Equal(t, result.Backup, decoded.Backup)
			require.Equal(t, result.SnapshotSHA256, decoded.SnapshotSHA256)
			require.NotEqual(t, "DOCKER_EFFECTS_FENCED", decoded.Verdict)
		})
	}
}

func TestWriteDockerEffectsRepairOutcomeRequiresConfirmedCompleteOutput(t *testing.T) {
	for _, verdict := range []string{"", "REPAIR_NOT_CONFIRMED", "REPAIR_COMMITTED"} {
		err := writeDockerEffectsRepairOutcome(io.Discard, shared.DockerRecoveryRepairResult{Verdict: verdict}, nil)
		require.Error(t, err)
	}
	result := shared.DockerRecoveryRepairResult{Verdict: "DOCKER_EFFECTS_FENCED"}
	require.NoError(t, writeDockerEffectsRepairOutcome(io.Discard, result, nil))
	err := writeDockerEffectsRepairOutcome(preflightVerdictWriter(func(data []byte) (int, error) {
		return len(data) - 1, nil
	}), result, nil)
	require.ErrorIs(t, err, io.ErrShortWrite, "an incomplete stdout report must make the command fail")
}

func TestDockerEffectsCLIReportsRepairFailureAsJSON(t *testing.T) {
	if os.Getenv("FRED_TEST_DOCKER_EFFECTS_CLI") == "1" {
		// Exercise the real main dispatch and its process exit, without a
		// production hook. The deliberately invalid config fails before Docker.
		for i, arg := range os.Args {
			if arg == "--" {
				os.Args = append([]string{"docker-backend"}, os.Args[i+1:]...)
				main()
				return
			}
		}
		t.Fatal("missing child command arguments")
	}
	configPath := filepath.Join(t.TempDir(), "docker.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte("name: ''\n"), 0o600))
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestDockerEffectsCLIReportsRepairFailureAsJSON$", "--",
		"-config", configPath, "-repair-unsettled-docker-effects",
		"-docker-effects-acknowledgement", "exact-ack", "-docker-effects-backup", filepath.Join(t.TempDir(), "backup.db"))
	command.Env = append(os.Environ(), "FRED_TEST_DOCKER_EFFECTS_CLI=1")
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	require.Equal(t, 1, exitErr.ExitCode())
	var result shared.DockerRecoveryRepairResult
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &result), "repair stdout must remain JSON even when configuration validation fails")
	require.NotEqual(t, "DOCKER_EFFECTS_FENCED", result.Verdict)
	require.Contains(t, stderr.String(), "offline Docker-effects operation failed")
}
