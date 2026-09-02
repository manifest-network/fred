package docker

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

func TestDockerResourceProfilesPinScratchWithoutRuntimeRepricing(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.cfg.ContainerTmpfsSizeMB = 73
	b.cfg.SKUProfiles = map[string]SKUProfile{
		"diskless": {CPUCores: 0.5, MemoryMB: 384, DiskMB: 0},
	}
	items := []backend.LeaseItem{{SKU: "diskless", ServiceName: "app", Quantity: 1}}

	resourceProfiles, err := b.resolveResourceProfiles(items)
	require.NoError(t, err)
	require.Equal(t, []shared.SKUResourceSnapshot{{
		SKU: "diskless", CPUCores: 0.5, MemoryMB: 384, ScratchDiskMB: 73,
	}}, resourceProfiles)

	// Durable authority is self-contained. Neither a later scratch resize nor a
	// removed SKU may change (or invalidate) an already-admitted generation.
	b.cfg.ContainerTmpfsSizeMB = 999
	delete(b.cfg.SKUProfiles, "diskless")
	resourcesBySKU, err := resourceSnapshotMap(items, resourceProfiles)
	require.NoError(t, err)
	require.Equal(t, int64(73), resourcesBySKU["diskless"].ScratchDiskMB)
	effective, err := resourcesBySKU["diskless"].EffectiveDiskMB()
	require.NoError(t, err)
	require.Equal(t, int64(73), effective)

	// A decoded nonlegacy Docker row can never silently fall back to today's
	// config. Only a wholly absent v0.13 snapshot enters the explicit resolver.
	err = validateDockerResourceProfiles(items, []shared.SKUResourceSnapshot{{
		SKU: "diskless", CPUCores: 0.5, MemoryMB: 384,
	}})
	require.ErrorContains(t, err, "no pinned scratch disk")
}

func TestSetupVolBindsUsesPinnedScratchAfterConfigDrift(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.cfg.ContainerTmpfsSizeMB = 999
	delete(b.cfg.SKUProfiles, "diskless")

	var createdID string
	var createdSizeMB int64
	hostRoot := t.TempDir()
	b.volumes = &mockVolumeManager{CreateFn: func(
		_ context.Context,
		id string,
		sizeMB int64,
	) (string, bool, error) {
		createdID = id
		createdSizeMB = sizeMB
		return filepath.Join(hostRoot, id), true, nil
	}}

	items := []backend.LeaseItem{{SKU: "diskless", ServiceName: "app", Quantity: 1}}
	resourceProfiles := []shared.SKUResourceSnapshot{{
		SKU: "diskless", CPUCores: 0.5, MemoryMB: 384, ScratchDiskMB: 73,
	}}
	_, created, err := b.setupVolBinds(
		context.Background(),
		"lease-a",
		items,
		resourceProfiles,
		map[string]*imageSetup{"app": {WritablePaths: []string{"/var/cache/app"}}},
		map[string]*manifest.Manifest{"app": {Image: "example.invalid/app:1"}},
		b.logger,
	)
	require.NoError(t, err)
	require.Equal(t, canonicalVolumeName("lease-a", "app", 0), createdID)
	require.Equal(t, int64(73), createdSizeMB)
	require.Equal(t, []string{createdID}, created)
}

func TestRestoreDemoteGateUsesPinnedScratch(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	const sourceLease = "source-lease"
	items := []backend.LeaseItem{{SKU: "diskless", ServiceName: "app", Quantity: 1}}
	record := &shared.RetentionEntry{
		OriginalLeaseUUID: sourceLease,
		Items:             items,
		ResourceProfiles: []shared.SKUResourceSnapshot{{
			SKU: "diskless", CPUCores: 0.5, MemoryMB: 384, ScratchDiskMB: 128,
		}},
		RetainedVolumeNames: []string{
			retainedName(canonicalVolumeName(sourceLease, "app", 0)),
		},
	}
	destinationProfiles := []shared.SKUResourceSnapshot{{
		SKU: "diskless", CPUCores: 0.5, MemoryMB: 384, ScratchDiskMB: 64,
	}}

	usage := int64(65 * bytesPerMiB)
	b.volumes = &mockVolumeManager{UsageFn: func(context.Context, string) (int64, error) {
		return usage, nil
	}}
	err := b.checkDemoteFitWithResourceProfiles(
		context.Background(), record, items, destinationProfiles, b.logger,
	)
	require.ErrorIs(t, err, backend.ErrDemoteDataExceedsTier)

	usage = 64 * bytesPerMiB
	require.NoError(t, b.checkDemoteFitWithResourceProfiles(
		context.Background(), record, items, destinationProfiles, b.logger,
	), "usage exactly equal to the pinned destination scratch cap fits")
}

func TestStorageIdentityAdoptionExplainsDisklessScratchBind(t *testing.T) {
	root := t.TempDir()
	volumeName := "fred-lease-a-app-0"
	mountSource := filepath.Join(root, volumeName, writablePathSubdir, "var-cache-app")
	require.NoError(t, os.MkdirAll(mountSource, 0o700))
	cfg := DefaultConfig()
	cfg.VolumeDataPath = root
	cfg.SKUProfiles = map[string]SKUProfile{
		"diskless": {CPUCores: 0.5, MemoryMB: 384},
	}

	evidence, err := storageIdentityContainerVolumeEvidence(cfg, []ContainerInfo{{
		ContainerID: "container-a",
		SKU:         "diskless",
		Mounts: []ContainerMount{{
			Source: mountSource,
			Target: "/var/cache/app",
			Type:   "bind",
		}},
	}})
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{volumeName: {}}, evidence)

	// A diskless image that never needed writable-path scaffolding legitimately
	// has no managed bind and therefore contributes no volume evidence.
	evidence, err = storageIdentityContainerVolumeEvidence(cfg, []ContainerInfo{{
		ContainerID: "container-b",
		SKU:         "diskless",
	}})
	require.NoError(t, err)
	require.Empty(t, evidence)
}

func TestStorageIdentityAdoptionAllowsV013StatefulContainerWithoutManagedBind(t *testing.T) {
	cfg := DefaultConfig()
	cfg.VolumeDataPath = t.TempDir()
	cfg.SKUProfiles = map[string]SKUProfile{
		"stateful": {CPUCores: 0.5, MemoryMB: 384, DiskMB: 1024},
	}

	// v0.13 reserved DiskMB for the SKU but created a host volume only when
	// image VOLUME or writable-path discovery found a path that needed one.
	// Container/release topology proves this bindless cohort; the adoption
	// reverse check separately refuses any unexplained managed volume.
	evidence, err := storageIdentityContainerVolumeEvidence(cfg, []ContainerInfo{{
		ContainerID: "container-v013-root-image",
		SKU:         "stateful",
		Mounts:      nil,
	}})
	require.NoError(t, err)
	require.Empty(t, evidence)
}
