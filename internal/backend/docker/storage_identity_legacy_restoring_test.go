package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend/shared"
)

type v013ReleaseWire struct {
	Version   int       `json:"version"`
	Manifest  []byte    `json:"manifest"`
	Image     string    `json:"image"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

func TestIntegrationStorageIdentityAdoptionRejectsV013RestoringBeforeMutation(t *testing.T) {
	cfg := storageIdentityLegacyRetentionTestConfig(t)
	writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
	writeLegacyAuthorityStores(t, cfg)

	const (
		source      = "11111111-1111-4111-8111-111111111111"
		destination = "22222222-2222-4222-8222-222222222222"
	)
	// Generated from v0.13.0's RetentionEntry JSON tags. In particular, it has
	// none of the destination items/profile/operation/callback authority added
	// by the upgraded restore protocol.
	legacyRow := []byte(`{"original_lease_uuid":"` + source +
		`","tenant":"tenant-a","provider_uuid":"33333333-3333-4333-8333-333333333333",` +
		`"items":[{"sku":"sku-stateless","quantity":1,"service_name":"app"}],` +
		`"stack_manifest":{"services":{"app":{"image":"docker.io/library/alpine:3.22"}}},` +
		`"callback_url":"https://fred.example/callbacks/provision","retained_volume_names":[],` +
		`"status":"restoring","new_lease_uuid":"` + destination +
		`","generation":1,"created_at":"2026-01-01T02:03:04Z",` +
		`"restoring_since":"2026-01-02T03:04:05Z","reaping_since":"0001-01-01T00:00:00Z"}`)
	db, err := bolt.Open(cfg.RetentionDBPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("retention")).Put([]byte(source), legacyRow)
	}))
	require.NoError(t, db.Close())

	paths := []string{cfg.CallbackDBPath, cfg.ReleasesDBPath, cfg.RetentionDBPath}
	before := make(map[string][]byte, len(paths))
	for _, path := range paths {
		before[path], err = os.ReadFile(path)
		require.NoError(t, err)
	}

	dockerClient := &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		DaemonInfoFn: func(context.Context) (DaemonSecurityInfo, error) {
			return DaemonSecurityInfo{SystemID: "daemon-system-a"}, nil
		},
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			t.Fatal("container inventory must not run after the incompatible retention preflight")
			return nil, nil
		},
	}
	verdict, err := preflightStorageIdentityAdoptionWithDependencies(
		t.Context(), cfg, dockerClient, &mockVolumeManager{},
	)
	require.Error(t, err)
	assert.Empty(t, verdict)
	assert.True(t, errors.Is(err, shared.ErrLegacyRestoringRetention))
	assert.ErrorContains(t, err, "source "+source)
	assert.ErrorContains(t, err, "destination "+destination)
	assert.ErrorContains(t, err, "restart the complete matching v0.13 lineage in isolation")
	assertNoStorageIdentityMarkers(t, cfg)

	for _, path := range paths {
		after, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		assert.Equal(t, before[path], after,
			"incompatible adoption must be a read-only preflight for %s", path)

		db, openErr := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true})
		require.NoError(t, openErr)
		require.NoError(t, db.View(func(tx *bolt.Tx) error {
			assert.Nil(t, tx.Bucket([]byte("_fred_backend_storage_identity")),
				"incompatible adoption must not bind %s", path)
			return nil
		}))
		require.NoError(t, db.Close())
	}
}

func TestIntegrationStorageIdentityAdoptionPreflightExportedPathIsReadOnly(t *testing.T) {
	server := newStorageIdentityDockerServer(t, nil)
	cfg := storageIdentityIntegrationConfig(t, server.URL)
	writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
	writeLegacyAuthorityStores(t, cfg)

	const leaseUUID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
	legacyRetention := []byte(`{"original_lease_uuid":"` + leaseUUID +
		`","tenant":"tenant-a","provider_uuid":"33333333-3333-4333-8333-333333333333",` +
		`"items":[{"sku":"sku-stateless","quantity":1,"service_name":"app"}],` +
		`"stack_manifest":null,"callback_url":"https://fred.example/callbacks/provision",` +
		`"retained_volume_names":[],"status":"active","generation":0,` +
		`"created_at":"2026-01-01T02:03:04Z","restoring_since":"0001-01-01T00:00:00Z",` +
		`"reaping_since":"0001-01-01T00:00:00Z"}`)
	writeRawLegacyRetentionRow(t, cfg.RetentionDBPath, leaseUUID, legacyRetention)
	before := snapshotStorageIdentityAuthorityFiles(t, cfg)

	verdict, err := PreflightStorageIdentityAdoptionForConfig(
		t.Context(), cfg, discardStorageIdentityLogger(),
	)
	require.NoError(t, err)
	assert.Equal(t, StorageIdentityAdoptionReady, verdict)
	assertStorageIdentityAuthorityUnchanged(t, cfg, before)
}

func TestIntegrationStorageIdentityAdoptionPreflightAllowsPartiallyReapedV013Row(t *testing.T) {
	cfg := storageIdentityLegacyRetentionTestConfig(t)
	cfg.VolumeDataPath = t.TempDir()
	writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
	writeLegacyAuthorityStores(t, cfg)

	const source = "55555555-5555-4555-8555-555555555555"
	presentVolume := "fred-retained-" + source + "-app-0"
	alreadyDestroyedVolume := "fred-retained-" + source + "-app-1"
	require.NoError(t, os.Mkdir(filepath.Join(cfg.VolumeDataPath, presentVolume), 0o700))
	legacyRow := []byte(`{"original_lease_uuid":"` + source +
		`","tenant":"tenant-a","provider_uuid":"33333333-3333-4333-8333-333333333333",` +
		`"items":[{"sku":"sku-stateful","quantity":2,"service_name":"app"}],` +
		`"stack_manifest":null,"callback_url":"","retained_volume_names":["` + presentVolume +
		`","` + alreadyDestroyedVolume + `"],"status":"reaping","generation":3,` +
		`"created_at":"2026-01-01T02:03:04Z","restoring_since":"0001-01-01T00:00:00Z",` +
		`"reaping_since":"2026-01-02T03:04:05Z"}`)
	writeRawLegacyRetentionRow(t, cfg.RetentionDBPath, source, legacyRow)

	paths := []string{cfg.CallbackDBPath, cfg.ReleasesDBPath, cfg.RetentionDBPath}
	before := make(map[string][]byte, len(paths))
	for _, path := range paths {
		var err error
		before[path], err = os.ReadFile(path)
		require.NoError(t, err)
	}
	dockerClient := &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		DaemonInfoFn: func(context.Context) (DaemonSecurityInfo, error) {
			return DaemonSecurityInfo{SystemID: "daemon-system-a"}, nil
		},
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}
	volumes := &mockVolumeManager{ListFn: func() ([]string, error) {
		return []string{presentVolume}, nil
	}}

	verdict, err := preflightStorageIdentityAdoptionWithDependencies(
		t.Context(), cfg, dockerClient, volumes,
	)
	require.NoError(t, err)
	assert.Equal(t, StorageIdentityAdoptionReady, verdict)
	assert.DirExists(t, filepath.Join(cfg.VolumeDataPath, presentVolume))
	assert.NoDirExists(t, filepath.Join(cfg.VolumeDataPath, alreadyDestroyedVolume))

	primary, err := (&Backend{cfg: cfg}).storageIdentityMarkerPath()
	require.NoError(t, err)
	for _, marker := range []string{primary, filepath.Clean(cfg.CallbackDBPath) + ".storage-identity-anchor.json"} {
		_, statErr := os.Lstat(marker)
		assert.ErrorIs(t, statErr, os.ErrNotExist, "read-only preflight must not publish %s", marker)
	}
	for _, path := range paths {
		after, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		assert.Equal(t, before[path], after, "successful preflight must not rewrite %s", path)
	}
}

func TestIntegrationStorageIdentityAdoptionPreflightRejectsMissingNonReapingVolumeReadOnly(t *testing.T) {
	const (
		source      = "66666666-6666-4666-8666-666666666666"
		destination = "66666666-6666-4666-9666-666666666666"
		operationID = "11111111-1111-4111-8111-111111111111"
	)
	for _, test := range []struct {
		name                 string
		status               string
		destinationAuthority string
		generation           int
		restoringSince       string
	}{
		{
			name:           "active",
			status:         shared.RetentionStatusActive,
			restoringSince: "0001-01-01T00:00:00Z",
		},
		{
			name:   "restoring with complete current authority",
			status: shared.RetentionStatusRestoring,
			destinationAuthority: `,"new_lease_uuid":"` + destination + `"` +
				`,"destination_items":[{"sku":"sku-stateful","quantity":1,"service_name":"app"}]` +
				`,"destination_resource_profiles":[{"sku":"sku-stateful","cpu_cores":1,"memory_mb":1024,"disk_mb":4096}]` +
				`,"destination_operation_id":"` + operationID + `"` +
				`,"destination_callback_url":"https://fred.example/callbacks/provision?operation_id=` + operationID + `"` +
				`,"destination_lifecycle_callback_url":"https://fred.example/callbacks/provision?lifecycle_id=` + operationID + `"`,
			generation:     1,
			restoringSince: "2026-01-02T03:04:05Z",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := storageIdentityLegacyRetentionTestConfig(t)
			cfg.VolumeDataPath = t.TempDir()
			writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
			writeLegacyAuthorityStores(t, cfg)

			missingVolume := "fred-retained-" + source + "-app-0"
			row := []byte(`{"original_lease_uuid":"` + source +
				`","tenant":"tenant-a","provider_uuid":"33333333-3333-4333-8333-333333333333",` +
				`"items":[{"sku":"sku-stateful","quantity":1,"service_name":"app"}],` +
				`"stack_manifest":{"services":{"app":{"image":"docker.io/library/alpine:3.22"}}},` +
				`"callback_url":"https://fred.example/callbacks/provision",` +
				`"retained_volume_names":["` + missingVolume + `"],"status":"` + test.status + `"` +
				test.destinationAuthority +
				`,"generation":` + fmt.Sprintf("%d", test.generation) +
				`,"created_at":"2026-01-01T02:03:04Z","restoring_since":"` + test.restoringSince +
				`","reaping_since":"0001-01-01T00:00:00Z"}`)
			writeRawLegacyRetentionRow(t, cfg.RetentionDBPath, source, row)
			before := snapshotStorageIdentityAuthorityFiles(t, cfg)

			verdict, err := preflightStorageIdentityAdoptionWithDependencies(
				t.Context(), cfg, storageIdentityPreflightDockerMock(t), &mockVolumeManager{},
			)
			require.Error(t, err)
			assert.Empty(t, verdict)
			assert.ErrorContains(t, err, "retention "+source+" volume \""+missingVolume+"\" is not present")
			assertStorageIdentityAuthorityUnchanged(t, cfg, before)
		})
	}
}

func TestIntegrationStorageIdentityAdoptionPreflightRejectsUnexplainedManagedVolumeReadOnly(t *testing.T) {
	cfg := storageIdentityLegacyRetentionTestConfig(t)
	cfg.VolumeDataPath = t.TempDir()
	writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
	writeLegacyAuthorityStores(t, cfg)

	const source = "77777777-7777-4777-8777-777777777777"
	alreadyDestroyedVolume := "fred-retained-" + source + "-app-0"
	unexplainedVolume := canonicalVolumeName("99999999-9999-4999-8999-999999999999", "app", 0)
	require.NoError(t, os.Mkdir(filepath.Join(cfg.VolumeDataPath, unexplainedVolume), 0o700))
	legacyRow := []byte(`{"original_lease_uuid":"` + source +
		`","tenant":"tenant-a","provider_uuid":"33333333-3333-4333-8333-333333333333",` +
		`"items":[{"sku":"sku-stateful","quantity":1,"service_name":"app"}],` +
		`"stack_manifest":null,"callback_url":"","retained_volume_names":["` +
		alreadyDestroyedVolume + `"],"status":"reaping","generation":3,` +
		`"created_at":"2026-01-01T02:03:04Z","restoring_since":"0001-01-01T00:00:00Z",` +
		`"reaping_since":"2026-01-02T03:04:05Z"}`)
	writeRawLegacyRetentionRow(t, cfg.RetentionDBPath, source, legacyRow)
	before := snapshotStorageIdentityAuthorityFiles(t, cfg)

	dockerClient := storageIdentityPreflightDockerMock(t)
	volumes := &mockVolumeManager{ListFn: func() ([]string, error) {
		return []string{unexplainedVolume}, nil
	}}
	verdict, err := preflightStorageIdentityAdoptionWithDependencies(
		t.Context(), cfg, dockerClient, volumes,
	)
	require.Error(t, err)
	assert.Empty(t, verdict)
	assert.ErrorContains(t, err,
		"managed volume \""+unexplainedVolume+"\" has no strict live-container or retention evidence")
	assertStorageIdentityAuthorityUnchanged(t, cfg, before)
}

func TestIntegrationStorageIdentityAdoptionPreflightExplainsV013GiveUpFootprintReadOnly(t *testing.T) {
	cfg := storageIdentityLegacyRetentionTestConfig(t)
	cfg.VolumeDataPath = t.TempDir()
	writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
	writeLegacyAuthorityStores(t, cfg)

	const source = "88888888-8888-4888-8888-888888888888"
	canonicalVolume := canonicalVolumeName(source, "app", 0)
	retainedVolume := retainedName(canonicalVolumeName(source, "app", 1))
	for _, volumeName := range []string{canonicalVolume, retainedVolume} {
		require.NoError(t, os.Mkdir(filepath.Join(cfg.VolumeDataPath, volumeName), 0o700))
	}
	// v0.13 recordGiveUpLeak deliberately persisted an empty name list: the
	// reaping finalizer derives both canonical and retained namespaces from the
	// original lease UUID on every retry.
	legacyRow := []byte(`{"original_lease_uuid":"` + source +
		`","tenant":"tenant-a","provider_uuid":"33333333-3333-4333-8333-333333333333",` +
		`"items":[{"sku":"sku-stateful","quantity":2,"service_name":"app"}],` +
		`"stack_manifest":null,"callback_url":"","retained_volume_names":[],` +
		`"status":"reaping","generation":0,"created_at":"2026-01-01T02:03:04Z",` +
		`"restoring_since":"0001-01-01T00:00:00Z","reaping_since":"2026-01-02T03:04:05Z"}`)
	writeRawLegacyRetentionRow(t, cfg.RetentionDBPath, source, legacyRow)
	before := snapshotStorageIdentityAuthorityFiles(t, cfg)

	volumes := &mockVolumeManager{ListFn: func() ([]string, error) {
		return []string{canonicalVolume, retainedVolume}, nil
	}}
	verdict, err := preflightStorageIdentityAdoptionWithDependencies(
		t.Context(), cfg, storageIdentityPreflightDockerMock(t), volumes,
	)
	require.NoError(t, err)
	assert.Equal(t, StorageIdentityAdoptionReady, verdict)
	assertStorageIdentityAuthorityUnchanged(t, cfg, before)
	assert.DirExists(t, filepath.Join(cfg.VolumeDataPath, canonicalVolume))
	assert.DirExists(t, filepath.Join(cfg.VolumeDataPath, retainedVolume))
}

func TestIntegrationStorageIdentityAdoptionPreflightDiagnosesInterruptedV013DeprovisionReadOnly(t *testing.T) {
	for _, status := range []string{shared.RetentionStatusActive, shared.RetentionStatusReaping} {
		t.Run(status, func(t *testing.T) {
			cfg := storageIdentityLegacyRetentionTestConfig(t)
			writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
			writeLegacyAuthorityStores(t, cfg)

			const leaseUUID = "99999999-9999-4999-8999-999999999999"
			manifestJSON := []byte(`{"services":{"app":{"image":"docker.io/library/alpine:3.22"}}}`)
			writeRawV013ReleaseHistory(t, cfg.ReleasesDBPath, leaseUUID, []v013ReleaseWire{{
				Version:   1,
				Manifest:  manifestJSON,
				Image:     "stack",
				Status:    "active",
				CreatedAt: time.Date(2026, time.January, 1, 1, 2, 3, 0, time.UTC),
			}})
			callbackURL := "https://fred.example/callbacks/provision"
			reapingSince := "0001-01-01T00:00:00Z"
			if status == shared.RetentionStatusReaping {
				callbackURL = ""
				reapingSince = "2026-01-02T03:04:05Z"
			}
			legacyRetention := []byte(`{"original_lease_uuid":"` + leaseUUID +
				`","tenant":"tenant-a","provider_uuid":"33333333-3333-4333-8333-333333333333",` +
				`"items":[{"sku":"sku-stateless","quantity":1,"service_name":"app"}],` +
				`"stack_manifest":{"services":{"app":{"image":"docker.io/library/alpine:3.22"}}},` +
				`"callback_url":"` + callbackURL + `","retained_volume_names":[],"status":"` + status + `",` +
				`"generation":1,"created_at":"2026-01-01T02:03:04Z",` +
				`"restoring_since":"0001-01-01T00:00:00Z","reaping_since":"` + reapingSince + `"}`)
			writeRawLegacyRetentionRow(t, cfg.RetentionDBPath, leaseUUID, legacyRetention)
			before := snapshotStorageIdentityAuthorityFiles(t, cfg)

			verdict, err := preflightStorageIdentityAdoptionWithDependencies(
				t.Context(), cfg, storageIdentityPreflightDockerMock(t), &mockVolumeManager{
					ListFn: func() ([]string, error) {
						return nil, nil
					},
				},
			)
			require.Error(t, err)
			assert.Empty(t, verdict)
			assert.ErrorIs(t, err, ErrV013InterruptedDeprovision)
			assert.ErrorContains(t, err, "lease "+leaseUUID)
			assert.ErrorContains(t, err, "matching "+status+" retention")
			assert.ErrorContains(t, err, "replay the exact close/deprovision event or request")
			assertStorageIdentityAuthorityUnchanged(t, cfg, before)
		})
	}
}

func TestIntegrationStorageIdentityAdoptionPreflightDiagnosesUnresolvedV013CloseReadOnly(t *testing.T) {
	const leaseUUID = "abababab-abab-4bab-8bab-abababababab"
	for _, test := range []struct {
		name           string
		managedVolumes []string
		wantCount      string
	}{
		{name: "no surviving managed volume", wantCount: "0 managed volumes"},
		{
			name:           "canonical managed volume remains",
			managedVolumes: []string{canonicalVolumeName(leaseUUID, "app", 0)},
			wantCount:      "1 managed volumes",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := storageIdentityLegacyRetentionTestConfig(t)
			cfg.VolumeDataPath = t.TempDir()
			writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
			writeLegacyAuthorityStores(t, cfg)
			writeRawV013ReleaseHistory(t, cfg.ReleasesDBPath, leaseUUID, []v013ReleaseWire{{
				Version:   1,
				Manifest:  []byte(`{"services":{"app":{"image":"docker.io/library/alpine:3.22"}}}`),
				Image:     "stack",
				Status:    "active",
				CreatedAt: time.Date(2026, time.January, 1, 1, 2, 3, 0, time.UTC),
			}})
			for _, volumeName := range test.managedVolumes {
				require.NoError(t, os.Mkdir(filepath.Join(cfg.VolumeDataPath, volumeName), 0o700))
			}
			before := snapshotStorageIdentityAuthorityFiles(t, cfg)

			verdict, err := preflightStorageIdentityAdoptionWithDependencies(
				t.Context(), cfg, storageIdentityPreflightDockerMock(t), &mockVolumeManager{
					ListFn: func() ([]string, error) {
						return test.managedVolumes, nil
					},
				},
			)
			require.Error(t, err)
			assert.Empty(t, verdict)
			assert.ErrorIs(t, err, ErrV013UnresolvedClose)
			assert.NotErrorIs(t, err, ErrV013InterruptedDeprovision)
			assert.ErrorContains(t, err, "lease "+leaseUUID)
			assert.ErrorContains(t, err, test.wantCount)
			assert.ErrorContains(t, err, "replaying deprovision can purge the release while stranding tenant data")
			assert.ErrorContains(t, err, "restore the complete matching pre-close snapshot")
			assert.ErrorContains(t, err, "height-pinned chain plus provider-inventory proof")
			assertStorageIdentityAuthorityUnchanged(t, cfg, before)
		})
	}
}

func TestIntegrationStorageIdentityAdoptionPreflightDoesNotMisclassifyDivergentV013ReleaseAndRetention(t *testing.T) {
	cfg := storageIdentityLegacyRetentionTestConfig(t)
	writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
	writeLegacyAuthorityStores(t, cfg)

	const leaseUUID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	writeRawV013ReleaseHistory(t, cfg.ReleasesDBPath, leaseUUID, []v013ReleaseWire{{
		Version:   1,
		Manifest:  []byte(`{"services":{"app":{"image":"docker.io/library/alpine:3.22"}}}`),
		Image:     "stack",
		Status:    "active",
		CreatedAt: time.Date(2026, time.January, 1, 1, 2, 3, 0, time.UTC),
	}})
	legacyRetention := []byte(`{"original_lease_uuid":"` + leaseUUID +
		`","tenant":"tenant-a","provider_uuid":"33333333-3333-4333-8333-333333333333",` +
		`"items":[{"sku":"sku-stateless","quantity":1,"service_name":"app"}],` +
		`"stack_manifest":{"services":{"app":{"image":"docker.io/library/busybox:1.37"}}},` +
		`"callback_url":"https://fred.example/callbacks/provision","retained_volume_names":[],` +
		`"status":"active","generation":0,"created_at":"2026-01-01T02:03:04Z",` +
		`"restoring_since":"0001-01-01T00:00:00Z","reaping_since":"0001-01-01T00:00:00Z"}`)
	writeRawLegacyRetentionRow(t, cfg.RetentionDBPath, leaseUUID, legacyRetention)
	before := snapshotStorageIdentityAuthorityFiles(t, cfg)

	verdict, err := preflightStorageIdentityAdoptionWithDependencies(
		t.Context(), cfg, storageIdentityPreflightDockerMock(t), &mockVolumeManager{},
	)
	require.Error(t, err)
	assert.Empty(t, verdict)
	assert.NotErrorIs(t, err, ErrV013InterruptedDeprovision)
	assert.ErrorContains(t, err, "legacy active release has no managed container cohort")
	assertStorageIdentityAuthorityUnchanged(t, cfg, before)
}

func TestIntegrationStorageIdentityAdoptionPreflightRejectsUnresolvableV013RetentionSKUReadOnly(t *testing.T) {
	cfg := storageIdentityLegacyRetentionTestConfig(t)
	writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
	writeLegacyAuthorityStores(t, cfg)

	const leaseUUID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	legacyRetention := []byte(`{"original_lease_uuid":"` + leaseUUID +
		`","tenant":"tenant-a","provider_uuid":"33333333-3333-4333-8333-333333333333",` +
		`"items":[{"sku":"removed-v0.13-sku","quantity":1,"service_name":"app"}],` +
		`"stack_manifest":null,"callback_url":"https://fred.example/callbacks/provision",` +
		`"retained_volume_names":[],"status":"active","generation":0,` +
		`"created_at":"2026-01-01T02:03:04Z","restoring_since":"0001-01-01T00:00:00Z",` +
		`"reaping_since":"0001-01-01T00:00:00Z"}`)
	writeRawLegacyRetentionRow(t, cfg.RetentionDBPath, leaseUUID, legacyRetention)
	before := snapshotStorageIdentityAuthorityFiles(t, cfg)

	verdict, err := preflightStorageIdentityAdoptionWithDependencies(
		t.Context(), cfg, storageIdentityPreflightDockerMock(t), &mockVolumeManager{},
	)
	require.Error(t, err)
	assert.Empty(t, verdict)
	assert.ErrorContains(t, err, "removed-v0.13-sku")
	assert.ErrorContains(t, err, "restore the matching v0.13 SKU mapping and profile")
	assertStorageIdentityAuthorityUnchanged(t, cfg, before)
}

func TestReapingLeaseUUIDFromVolumeName(t *testing.T) {
	const leaseUUID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	for _, test := range []struct {
		name       string
		volumeName string
		wantUUID   string
		wantOK     bool
	}{
		{name: "canonical", volumeName: "fred-" + leaseUUID + "-app-0", wantUUID: leaseUUID, wantOK: true},
		{name: "retained", volumeName: "fred-retained-" + leaseUUID + "-app-0", wantUUID: leaseUUID, wantOK: true},
		{name: "unmanaged", volumeName: "other-" + leaseUUID + "-app-0"},
		{name: "noncanonical UUID", volumeName: "fred-CCCCCCCC-CCCC-4CCC-8CCC-CCCCCCCCCCCC-app-0"},
		{name: "prefix collision", volumeName: "fred-" + leaseUUID + "x-app-0"},
		{name: "retained token is not UUID", volumeName: "fred-retained-not-a-lease-app-0"},
		{name: "leading-zero index", volumeName: "fred-" + leaseUUID + "-app-01"},
		{name: "invalid service", volumeName: "fred-" + leaseUUID + "-App-0"},
		{name: "missing suffix", volumeName: "fred-" + leaseUUID + "-"},
	} {
		t.Run(test.name, func(t *testing.T) {
			gotUUID, gotOK := reapingLeaseUUIDFromVolumeName(test.volumeName)
			assert.Equal(t, test.wantOK, gotOK)
			assert.Equal(t, test.wantUUID, gotUUID)
		})
	}
}

func storageIdentityPreflightDockerMock(t *testing.T) *mockDockerClient {
	t.Helper()
	return &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		DaemonInfoFn: func(context.Context) (DaemonSecurityInfo, error) {
			return DaemonSecurityInfo{SystemID: "daemon-system-a"}, nil
		},
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}
}

func storageIdentityLegacyRetentionTestConfig(t *testing.T) Config {
	t.Helper()
	cfg := storageIdentityEvidenceTestConfig(t)
	cfg.SKUMapping = map[string]string{
		"sku-stateless": "stateless",
		"sku-stateful":  "stateful",
	}
	cfg.SKUProfiles = map[string]SKUProfile{
		"stateless": {CPUCores: 0.5, MemoryMB: 512},
		"stateful":  {CPUCores: 1, MemoryMB: 1024, DiskMB: 4096},
	}
	return cfg
}

func snapshotStorageIdentityAuthorityFiles(t *testing.T, cfg Config) map[string][]byte {
	t.Helper()
	paths := []string{cfg.CallbackDBPath, cfg.ReleasesDBPath, cfg.RetentionDBPath}
	contents := make(map[string][]byte, len(paths))
	for _, path := range paths {
		value, err := os.ReadFile(path)
		require.NoError(t, err)
		contents[path] = value
	}
	return contents
}

func assertStorageIdentityAuthorityUnchanged(
	t *testing.T,
	cfg Config,
	want map[string][]byte,
) {
	t.Helper()
	assertNoStorageIdentityMarkers(t, cfg)
	for path, before := range want {
		after, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, before, after, "read-only preflight must not rewrite %s", path)

		db, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true})
		require.NoError(t, err)
		require.NoError(t, db.View(func(tx *bolt.Tx) error {
			assert.Nil(t, tx.Bucket([]byte("_fred_backend_storage_identity")),
				"read-only preflight must not bind %s", path)
			return nil
		}))
		require.NoError(t, db.Close())
	}
}

func writeRawLegacyRetentionRow(t *testing.T, dbPath, leaseUUID string, row []byte) {
	t.Helper()
	db, err := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("retention")).Put([]byte(leaseUUID), row)
	}))
	require.NoError(t, db.Close())
}

func writeRawV013ReleaseHistory(
	t *testing.T,
	dbPath, leaseUUID string,
	history []v013ReleaseWire,
) {
	t.Helper()
	encoded, err := json.Marshal(history)
	require.NoError(t, err)
	db, err := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("releases")).Put([]byte(leaseUUID), encoded)
	}))
	require.NoError(t, db.Close())
}
