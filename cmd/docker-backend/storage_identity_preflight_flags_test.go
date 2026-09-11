package main

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker"
)

type preflightVerdictWriter func([]byte) (int, error)

func (write preflightVerdictWriter) Write(value []byte) (int, error) {
	return write(value)
}

func TestParseStartupFlagsStorageIdentityAdoptionPreflight(t *testing.T) {
	startup, err := parseStartupFlags(
		[]string{"-config", "/etc/fred/docker.yaml", "-preflight-storage-identity-adoption"},
		io.Discard,
	)
	require.NoError(t, err)
	assert.Equal(t, "/etc/fred/docker.yaml", startup.configPath)
	assert.True(t, startup.preflightStorageIdentityAdoption)
	assert.Empty(t, startup.initializeStorageIdentity)
	assert.Equal(t, defaultStorageIdentityOperationTimeout, startup.storageIdentityOperationTimeout)
}

func TestParseStartupFlagsStorageIdentityOperationTimeout(t *testing.T) {
	startup, err := parseStartupFlags(
		[]string{
			"-initialize-storage-identity", "adopt",
			"-storage-identity-operation-timeout", "45m",
		},
		io.Discard,
	)
	require.NoError(t, err)
	assert.Equal(t, 45*time.Minute, startup.storageIdentityOperationTimeout)
}

func TestParseStartupFlagsRejectsNonPositiveStorageIdentityOperationTimeout(t *testing.T) {
	for _, timeout := range []string{"0", "-1s"} {
		t.Run(timeout, func(t *testing.T) {
			_, err := parseStartupFlags(
				[]string{"-storage-identity-operation-timeout", timeout},
				io.Discard,
			)
			require.ErrorContains(t, err, "must be positive")
		})
	}
}

func TestParseStartupFlagsRejectsPreflightAndInitializationTogether(t *testing.T) {
	_, err := parseStartupFlags(
		[]string{
			"-preflight-storage-identity-adoption",
			"-initialize-storage-identity", "adopt",
		},
		io.Discard,
	)
	require.Error(t, err)
	assert.ErrorContains(t, err, "mutually exclusive")
}

func TestWriteStorageIdentityAdoptionVerdict(t *testing.T) {
	var output bytes.Buffer
	require.NoError(t, writeStorageIdentityAdoptionVerdict(
		&output,
		docker.StorageIdentityAdoptionReady,
	))
	assert.Equal(t, "ready_for_v0_13_storage_identity_adoption\n", output.String())

	err := writeStorageIdentityAdoptionVerdict(&output, "unverified")
	require.Error(t, err)
	assert.ErrorContains(t, err, "invalid storage identity adoption verdict")
}

func TestWriteStorageIdentityAdoptionVerdictRejectsIncompleteOutput(t *testing.T) {
	err := writeStorageIdentityAdoptionVerdict(
		preflightVerdictWriter(func(value []byte) (int, error) {
			return len(value) - 1, nil
		}),
		docker.StorageIdentityAdoptionReady,
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, io.ErrShortWrite)

	wantErr := errors.New("stdout unavailable")
	err = writeStorageIdentityAdoptionVerdict(
		preflightVerdictWriter(func([]byte) (int, error) { return 0, wantErr }),
		docker.StorageIdentityAdoptionReady,
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr)
}
