package main

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseStartupFlagsStorageIdentityInitialization(t *testing.T) {
	startup, err := parseStartupFlags(
		[]string{"-config", "/etc/fred/docker.yaml", "-initialize-storage-identity", "new"},
		io.Discard,
	)
	require.NoError(t, err)
	assert.Equal(t, "/etc/fred/docker.yaml", startup.configPath)
	assert.Equal(t, storageIdentityModeNew, startup.initializeStorageIdentity)
	assert.Equal(t, defaultStorageIdentityOperationTimeout, startup.storageIdentityOperationTimeout)
}

// The v0.13 adoption value and the two adoption-only modes are gone: a v0.13
// lineage is adopted by the final 0.14 release.
func TestParseStartupFlagsRejectsRemovedStorageIdentityAdoption(t *testing.T) {
	_, err := parseStartupFlags([]string{"-initialize-storage-identity", "adopt"}, io.Discard)
	require.ErrorContains(t, err, `-initialize-storage-identity must be "new"`)
	assert.ErrorContains(t, err, "final 0.14 release")

	for _, removed := range []string{
		"-preflight-storage-identity-adoption",
		"-audit-storage-identity-adoption",
	} {
		_, err := parseStartupFlags([]string{removed}, io.Discard)
		require.ErrorContains(t, err, "flag provided but not defined: "+removed, removed)
	}
}

func TestParseStartupFlagsStorageIdentityOperationTimeout(t *testing.T) {
	startup, err := parseStartupFlags(
		[]string{
			"-initialize-storage-identity", "new",
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

func TestParseStartupFlagsRejectsInitializationWithAnotherOneShotMode(t *testing.T) {
	_, err := parseStartupFlags(
		[]string{"-initialize-storage-identity", "new", "-validate-config"},
		io.Discard,
	)
	require.ErrorContains(t, err, "mutually exclusive")
}
