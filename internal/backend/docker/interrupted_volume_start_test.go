package docker

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newInterruptedVolumeStartBackend(t *testing.T) (*Backend, *mockDockerClient) {
	t.Helper()
	b, _ := newBackendWithRetention(t)
	dockerClient, ok := b.docker.(*mockDockerClient)
	require.True(t, ok)
	dockerClient.PingFn = func(context.Context) error { return nil }
	bindTestStorageIdentity(t, b, dockerClient)
	t.Cleanup(b.stopCancel)
	return b, dockerClient
}

func TestStartInterruptedVolumeRecoveryFailurePreventsPublicationChecks(t *testing.T) {
	b, dockerClient := newInterruptedVolumeStartBackend(t)
	dockerClient.CloseFn = func() error { return nil }
	recoveryErr := errors.New("simulated exact stage cleanup failure")
	recoveryCalls := 0
	b.volumes = &mockVolumeManager{
		RecoverInterruptedVolumeMutationsFn: func(context.Context) error {
			recoveryCalls++
			return recoveryErr
		},
		RequireNoInterruptedVolumeMutationsFn: func(context.Context) error {
			t.Fatal("postcondition must not run after failed recovery")
			return nil
		},
		ListFn: func() ([]string, error) {
			t.Fatal("inventory must not publish after failed recovery")
			return nil, nil
		},
	}

	err := b.Start(context.Background())
	require.ErrorIs(t, err, recoveryErr)
	require.ErrorContains(t, err, "recover interrupted managed-volume mutations")
	assert.Equal(t, 1, recoveryCalls)
}

func TestStartInterruptedVolumeRecoveryRequiresCleanPostcondition(t *testing.T) {
	b, _ := newInterruptedVolumeStartBackend(t)
	remainingErr := errors.New("typed xfs stage remains")
	var calls []string
	b.volumes = &mockVolumeManager{
		RecoverInterruptedVolumeMutationsFn: func(context.Context) error {
			calls = append(calls, "recover")
			return nil
		},
		RequireNoInterruptedVolumeMutationsFn: func(context.Context) error {
			calls = append(calls, "postcondition")
			return remainingErr
		},
		ListFn: func() ([]string, error) {
			t.Fatal("inventory must not run while private evidence remains")
			return nil, nil
		},
	}

	err := b.Start(context.Background())
	require.ErrorIs(t, err, remainingErr)
	require.ErrorContains(t, err, "interrupted managed-volume mutation remains after recovery")
	assert.Equal(t, []string{"recover", "postcondition"}, calls)
}

func TestStartInterruptedVolumeRecoveryPrecedesManagedInventory(t *testing.T) {
	b, _ := newInterruptedVolumeStartBackend(t)
	var calls []string
	b.volumes = &mockVolumeManager{
		RecoverInterruptedVolumeMutationsFn: func(context.Context) error {
			calls = append(calls, "recover")
			return nil
		},
		RequireNoInterruptedVolumeMutationsFn: func(context.Context) error {
			calls = append(calls, "postcondition")
			return nil
		},
		ListFn: func() ([]string, error) {
			calls = append(calls, "inventory")
			return []string{"not-a-managed-volume"}, nil
		},
	}

	err := b.Start(context.Background())
	require.ErrorContains(t, err, "managed volume substrate validation failed")
	require.ErrorContains(t, err, "name is invalid")
	assert.Equal(t, []string{"recover", "postcondition", "inventory"}, calls)
}

func TestStartPreservesUnattributedManagedVolume(t *testing.T) {
	b, dockerClient := newInterruptedVolumeStartBackend(t)
	dockerClient.CloseFn = func() error { return nil }
	stray := canonicalVolumeName(
		"0192f1a0-1111-4abc-8def-000000000799",
		"app",
		0,
	)
	b.volumes = &mockVolumeManager{
		RecoverInterruptedVolumeMutationsFn:   func(context.Context) error { return nil },
		RequireNoInterruptedVolumeMutationsFn: func(context.Context) error { return nil },
		ListFn: func() ([]string, error) {
			return []string{stray}, nil
		},
		DestroyFn: func(context.Context, string) error {
			t.Fatal("startup must not infer destruction authority for an unattributed volume")
			return nil
		},
	}
	installTestStorageMutationAdapters(b)
	bindRetentionOrphanPrunerForTest(t, b)

	require.NoError(t, b.Start(context.Background()))
	require.NoError(t, b.Stop())
}
