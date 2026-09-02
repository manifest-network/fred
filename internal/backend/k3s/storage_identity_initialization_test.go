package k3s

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backendidentity"
)

func TestInitializeStorageIdentityCommittedRerunReattestsClusterBeforeRecovery(t *testing.T) {
	cfg := validConfig()
	dir := t.TempDir()
	cfg.CallbackDBPath = filepath.Join(dir, "callbacks.db")
	cfg.DiagnosticsDBPath = filepath.Join(dir, "diagnostics.db")
	cfg.ReleasesDBPath = filepath.Join(dir, "releases.db")

	stable := &Backend{
		cfg: cfg,
		clusterIdentity: func(context.Context) (string, error) {
			return "cluster-a", nil
		},
	}
	initialized, err := initializeStorageIdentityForConfigWithProbe(t.Context(), cfg, stable)
	require.NoError(t, err)
	require.True(t, initialized.Valid())

	primaryPath := filepath.Clean(cfg.CallbackDBPath) + ".storage-identity.json"
	interruptedPublish := filepath.Join(
		filepath.Dir(primaryPath),
		"."+filepath.Base(primaryPath)+".tmp-6ba7b813-9dad-41d1-80b4-00c04fd430c8",
	)
	require.NoError(t, os.Link(primaryPath, interruptedPublish))

	var identityReads atomic.Int64
	swapped := &Backend{
		cfg: cfg,
		clusterIdentity: func(context.Context) (string, error) {
			if identityReads.Add(1) >= 2 {
				return "cluster-b", nil
			}
			return "cluster-a", nil
		},
	}
	_, err = initializeStorageIdentityForConfigWithProbe(t.Context(), cfg, swapped)
	require.ErrorContains(t, err, "K3s cluster identity changed during lineage proof")
	_, statErr := os.Lstat(interruptedPublish)
	require.NoError(t, statErr,
		"stale substrate evidence must be rejected before marker recovery mutates the lineage")

	clean := &Backend{
		cfg: cfg,
		clusterIdentity: func(context.Context) (string, error) {
			return "cluster-a", nil
		},
	}
	rerun, err := initializeStorageIdentityForConfigWithProbe(t.Context(), cfg, clean)
	require.NoError(t, err)
	assert.Equal(t, initialized, rerun)
	_, statErr = os.Lstat(interruptedPublish)
	assert.ErrorIs(t, statErr, os.ErrNotExist,
		"a re-attested committed rerun may recover a recognized interrupted publication")

	identityReads.Store(0)
	postSealSwap := &Backend{
		cfg: cfg,
		clusterIdentity: func(context.Context) (string, error) {
			// Initial, pre-operation, and pre-reread observations still see A;
			// only the final post-seal barrier observes the replacement cluster.
			if identityReads.Add(1) >= 4 {
				return "cluster-b", nil
			}
			return "cluster-a", nil
		},
	}
	_, err = initializeStorageIdentityForConfigWithProbe(t.Context(), cfg, postSealSwap)
	require.ErrorContains(t, err, "K3s cluster identity changed during lineage proof")
}

func TestInitializeStorageIdentityRejectsSameParentReplacementBeforePublication(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "authority")
	retired := filepath.Join(root, "authority-retired")
	require.NoError(t, os.Mkdir(parent, 0o700))

	cfg := validConfig()
	cfg.CallbackDBPath = filepath.Join(parent, "callbacks.db")
	cfg.DiagnosticsDBPath = filepath.Join(parent, "diagnostics.db")
	cfg.ReleasesDBPath = filepath.Join(parent, "releases.db")

	var identityReads atomic.Int64
	probe := &Backend{
		cfg: cfg,
		clusterIdentity: func(context.Context) (string, error) {
			// The third read is the final substrate barrier after every empty
			// journal was inspected and before the pending anchor is published.
			if identityReads.Add(1) == 3 {
				if err := os.Rename(parent, retired); err != nil {
					return "", err
				}
				if err := os.Mkdir(parent, 0o700); err != nil {
					return "", err
				}
			}
			return "cluster-a", nil
		},
	}

	_, err := initializeStorageIdentityForConfigWithProbe(t.Context(), cfg, probe)
	require.Error(t, err)
	assert.ErrorContains(t, err, "storage parent changed during lineage proof")

	replacementEntries, readErr := os.ReadDir(parent)
	require.NoError(t, readErr)
	assert.Empty(t, replacementEntries, "replacement storage must remain entirely unsealed")
	retiredEntries, readErr := os.ReadDir(retired)
	require.NoError(t, readErr)
	assert.Empty(t, retiredEntries, "the final parent barrier must precede the first publication")
}

func TestInitializeStorageIdentityCommittedRerunRejectsCompleteSameParentLineageSwap(t *testing.T) {
	parent := t.TempDir()
	lineageA := k3sStorageLineageTestConfig(parent, "lineage-a")
	lineageB := k3sStorageLineageTestConfig(parent, "lineage-b")
	stableProbe := func(cfg Config) *Backend {
		return &Backend{
			cfg: cfg,
			clusterIdentity: func(context.Context) (string, error) {
				return "cluster-a", nil
			},
		}
	}

	idA, err := initializeStorageIdentityForConfigWithProbe(
		t.Context(), lineageA, stableProbe(lineageA),
	)
	require.NoError(t, err)
	idB, err := initializeStorageIdentityForConfigWithProbe(
		t.Context(), lineageB, stableProbe(lineageB),
	)
	require.NoError(t, err)
	require.NotEqual(t, idA, idB, "the regression requires two independently sealed lineages")

	var identityReads atomic.Int64
	swappingProbe := &Backend{
		cfg: lineageA,
		clusterIdentity: func(context.Context) (string, error) {
			// The committed rerun reads the cluster initially, before its first
			// marker pass, and between its two marker passes. Swap every authority
			// entry on that middle observation while retaining the same physical
			// parent and cluster identity.
			if identityReads.Add(1) == 3 {
				if err := swapK3sStorageLineages(lineageA, lineageB); err != nil {
					return "", err
				}
			}
			return "cluster-a", nil
		},
	}

	observed, err := initializeStorageIdentityForConfigWithProbe(
		t.Context(), lineageA, swappingProbe,
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, backendidentity.ErrMarkerBindingMismatch)
	assert.ErrorContains(t, err, "K3s backend storage identity changed after sealing")
	assert.Equal(t, backendidentity.ID{}, observed)
	assert.Equal(t, int64(3), identityReads.Load(),
		"the replacement must be rejected by the second lineage pass before the final cluster read")
}

func k3sStorageLineageTestConfig(parent, prefix string) Config {
	cfg := validConfig()
	cfg.CallbackDBPath = filepath.Join(parent, prefix+"-callbacks.db")
	cfg.DiagnosticsDBPath = filepath.Join(parent, prefix+"-diagnostics.db")
	cfg.ReleasesDBPath = filepath.Join(parent, prefix+"-releases.db")
	return cfg
}

func swapK3sStorageLineages(lineageA, lineageB Config) error {
	filesA := []string{
		lineageA.CallbackDBPath,
		lineageA.ReleasesDBPath,
		filepath.Clean(lineageA.CallbackDBPath) + ".storage-identity.json",
		filepath.Clean(lineageA.CallbackDBPath) + ".storage-identity-anchor.json",
	}
	filesB := []string{
		lineageB.CallbackDBPath,
		lineageB.ReleasesDBPath,
		filepath.Clean(lineageB.CallbackDBPath) + ".storage-identity.json",
		filepath.Clean(lineageB.CallbackDBPath) + ".storage-identity-anchor.json",
	}
	for i, pathA := range filesA {
		staged := pathA + ".lineage-swap"
		if err := os.Rename(pathA, staged); err != nil {
			return fmt.Errorf("stage K3s lineage entry %q: %w", pathA, err)
		}
		if err := os.Rename(filesB[i], pathA); err != nil {
			return fmt.Errorf("install alternate K3s lineage entry %q: %w", filesB[i], err)
		}
		if err := os.Rename(staged, filesB[i]); err != nil {
			return fmt.Errorf("retain original K3s lineage entry %q: %w", pathA, err)
		}
	}
	return nil
}
