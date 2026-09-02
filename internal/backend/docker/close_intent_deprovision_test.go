package docker

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

const (
	closeDeprovisionLeaseUUID    = "123e4567-e89b-42d3-a456-426614174000"
	closeDeprovisionProviderUUID = "22222222-2222-4222-8222-222222222222"
)

func seedCloseDeprovisionLease(
	t *testing.T,
	b *Backend,
	stores closeRecoveryStores,
) {
	t.Helper()
	items := []backend.LeaseItem{{
		SKU: "docker-small", ServiceName: "app", Quantity: 1,
	}}
	payload := validStackManifestJSON(map[string]string{
		"app": "docker.io/library/nginx:1.27",
	})
	stack, err := manifest.ParsePayload(payload)
	require.NoError(t, err)
	resourceProfiles := testResourceProfiles(t, items)
	b.provisionsMu.Lock()
	b.provisions[closeDeprovisionLeaseUUID] = &provision{
		ProvisionState: leasesm.ProvisionState{
			LeaseUUID:     closeDeprovisionLeaseUUID,
			Tenant:        "tenant-a",
			ProviderUUID:  closeDeprovisionProviderUUID,
			Items:         items,
			Quantity:      1,
			StackManifest: stack,
			Status:        backend.ProvisionStatusReady,
		},
		ResourceProfiles: resourceProfiles,
	}
	b.provisionsMu.Unlock()
	require.NoError(t, stores.releases.Append(closeDeprovisionLeaseUUID, shared.Release{
		Manifest:         payload,
		Image:            "stack",
		Items:            items,
		ResourceProfiles: resourceProfiles,
		Status:           "active",
		CreatedAt:        time.Now(),
	}))
}

func TestDoDeprovision_CommitsCloseIntentBeforeTeardown(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, nil)
	seedCloseDeprovisionLease(t, b, stores)

	b.compose = &mockComposeExecutor{DownFn: func(
		context.Context,
		string,
		time.Duration,
	) error {
		_, found, err := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
		require.NoError(t, err)
		require.True(t, found,
			"the durable close barrier must commit before the first substrate mutation")
		return nil
	}}

	require.NoError(t, b.doDeprovision(context.Background(), closeDeprovisionLeaseUUID))
	_, found, err := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, err)
	require.False(t, found, "terminal settlement must consume the exact close capability")
	releases, err := stores.releases.List(closeDeprovisionLeaseUUID)
	require.NoError(t, err)
	require.Empty(t, releases)

	closeCloseRecoveryBackend(t, b, stores)
}

func TestAcquireCloseIntentUsesFencedReleaseTopology(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, nil)
	seedCloseDeprovisionLease(t, b, stores)

	// Model the actor-drain boundary: replacement substrate and its Release have
	// committed, but the queued terminal actor event has not yet promoted the
	// in-memory source projection. Close must retain the generation named by its
	// release fence, not this stale projection.
	targetItems := []backend.LeaseItem{{
		SKU: "docker-small", ServiceName: "app", Quantity: 1,
		CustomDomain: "new.example.test",
	}}
	targetPayload := validStackManifestJSON(map[string]string{
		"app": "docker.io/library/nginx:1.28",
	})
	require.NoError(t, stores.releases.Append(closeDeprovisionLeaseUUID, shared.Release{
		Manifest:         targetPayload,
		Image:            "stack",
		Items:            targetItems,
		ResourceProfiles: testResourceProfiles(t, targetItems),
		Status:           "deploying",
		CreatedAt:        time.Now().Add(time.Second),
	}))
	require.NoError(t, stores.releases.ActivateLatest(closeDeprovisionLeaseUUID))

	b.provisionsMu.RLock()
	projection := b.provisions[closeDeprovisionLeaseUUID]
	require.NotNil(t, projection)
	oldStack := projection.StackManifest
	oldItems := append([]backend.LeaseItem(nil), projection.Items...)
	b.provisionsMu.RUnlock()
	require.NotEqual(t, targetItems, oldItems)

	claim, found, err := b.acquireCloseIntent(
		context.Background(),
		closeDeprovisionLeaseUUID,
		true,
		"tenant-a",
		closeDeprovisionProviderUUID,
		oldItems,
		oldStack,
		"",
		"",
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, targetItems, claim.Items())
	require.JSONEq(t, string(targetPayload), string(claim.Manifest()))
	stack, err := manifest.ParsePayload(claim.Manifest())
	require.NoError(t, err)
	require.Equal(t, "docker.io/library/nginx:1.28", stack.Services["app"].Image)

	active, err := stores.releases.LatestActive(closeDeprovisionLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	require.Equal(t, active.Version, claim.ActiveReleaseVersion())

	closeCloseRecoveryBackend(t, b, stores)
}

func TestAcquireCloseIntentUsesFencedLegacyRuntimeAuthority(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, nil)
	seedCloseDeprovisionLease(t, b, stores)

	const callbackURL = "https://fred.example/callbacks/provision?lease_uuid=" + closeDeprovisionLeaseUUID
	lifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	authority, err := shared.NewLegacyRuntimeAuthority(
		"tenant-authoritative",
		closeDeprovisionProviderUUID,
		callbackURL,
		lifecycleCallbackURL,
	)
	require.NoError(t, err)
	active, err := stores.releases.LatestActive(closeDeprovisionLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	require.NoError(t, stores.releases.BackfillLegacyRuntimeAuthority(
		closeDeprovisionLeaseUUID, *active, authority,
	))
	stack, err := manifest.ParsePayload(active.Manifest)
	require.NoError(t, err)

	// Model a stale in-memory projection after the durable v0.13 authority was
	// frozen. Close must fence the principal and callback pair from the Release,
	// not combine the Release topology with these caller-supplied values.
	claim, found, err := b.acquireCloseIntent(
		context.Background(),
		closeDeprovisionLeaseUUID,
		true,
		"tenant-stale",
		"33333333-3333-4333-8333-333333333333",
		active.Items,
		stack,
		"https://stale.example/callback",
		"",
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, authority.Tenant(), claim.Tenant())
	require.Equal(t, authority.ProviderUUID(), claim.ProviderUUID())
	require.Equal(t, authority.CallbackURL(), claim.CallbackURL())
	require.Equal(t, authority.LifecycleCallbackURL(), claim.LifecycleCallbackURL())

	closeCloseRecoveryBackend(t, b, stores)
}

func TestDoDeprovision_UnmarkedExactRollbackUsesSelectedReleaseAuthority(t *testing.T) {
	dir := t.TempDir()
	const callbackURL = "https://fred.example/callbacks/provision?operation_id=9a72fbc1-38c8-4f31-87f7-f689979b9324"
	lifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	var removed []string
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{{
				ContainerID:          "immutable-prev-id",
				Name:                 "fred-" + closeDeprovisionLeaseUUID + "-app-0-prev",
				LeaseUUID:            closeDeprovisionLeaseUUID,
				Tenant:               "tenant-a",
				ProviderUUID:         closeDeprovisionProviderUUID,
				BackendName:          "docker",
				SKU:                  "docker-small",
				Image:                "docker.io/library/nginx:1.27",
				CallbackURL:          callbackURL,
				LifecycleCallbackURL: lifecycleCallbackURL,
				InstanceIndex:        0,
				Status:               "exited",
			}}, nil
		},
		RemoveContainerFn: func(_ context.Context, containerID string) error {
			removed = append(removed, containerID)
			return nil
		},
	}
	b, stores := openCloseRecoveryBackend(t, dir, mock, nil)
	seedCloseDeprovisionLease(t, b, stores)
	b.provisionStore.UpdateFn(closeDeprovisionLeaseUUID, func(state *leasesm.ProvisionState) {
		state.CallbackURL = callbackURL
		state.LifecycleCallbackURL = lifecycleCallbackURL
	})

	active, err := stores.releases.LatestActive(closeDeprovisionLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	require.False(t, active.LegacyMigration,
		"an exact rollback name alone must not mint migration provenance")

	b.compose = &mockComposeExecutor{DownFn: func(
		context.Context,
		string,
		time.Duration,
	) error {
		claim, found, claimErr := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
		require.NoError(t, claimErr)
		require.True(t, found)
		require.Equal(t, []shared.CloseLegacyRollbackTarget{{
			ContainerID: "immutable-prev-id",
			Name:        "fred-" + closeDeprovisionLeaseUUID + "-app-0-prev",
		}}, claim.LegacyRollbackTargets(),
			"Close must freeze the immutable ID before Compose can retire the release")
		return nil
	}}

	require.NoError(t, b.doDeprovision(context.Background(), closeDeprovisionLeaseUUID))
	require.Equal(t, []string{"immutable-prev-id"}, removed)
	history, err := stores.releases.List(closeDeprovisionLeaseUUID)
	require.NoError(t, err)
	require.Empty(t, history, "release history retires only after exact rollback cleanup")

	closeCloseRecoveryBackend(t, b, stores)
}

func TestCloseLegacyRollbackTargetsRejectsUnmarkedAmbiguity(t *testing.T) {
	const callbackURL = "https://fred.example/callbacks/provision?operation_id=9a72fbc1-38c8-4f31-87f7-f689979b9324"
	lifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	base := ContainerInfo{
		ContainerID:          "immutable-prev-id",
		Name:                 "fred-" + closeDeprovisionLeaseUUID + "-app-0-prev",
		LeaseUUID:            closeDeprovisionLeaseUUID,
		Tenant:               "tenant-a",
		ProviderUUID:         closeDeprovisionProviderUUID,
		BackendName:          "docker",
		SKU:                  "docker-small",
		Image:                "docker.io/library/nginx:1.27",
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleCallbackURL,
		InstanceIndex:        0,
		Status:               "exited",
	}
	for name, containers := range map[string][]ContainerInfo{
		"wrong exact name": func() []ContainerInfo {
			candidate := base
			candidate.Name = "fred-" + closeDeprovisionLeaseUUID + "-app-1-prev"
			return []ContainerInfo{candidate}
		}(),
		"out of range index": func() []ContainerInfo {
			candidate := base
			candidate.InstanceIndex = 1
			candidate.Name = "fred-" + closeDeprovisionLeaseUUID + "-app-1-prev"
			return []ContainerInfo{candidate}
		}(),
		"wrong SKU": func() []ContainerInfo {
			candidate := base
			candidate.SKU = "docker-large"
			return []ContainerInfo{candidate}
		}(),
		"wrong custom domain": func() []ContainerInfo {
			candidate := base
			candidate.CustomDomain = "wrong.example"
			return []ContainerInfo{candidate}
		}(),
		"wrong image": func() []ContainerInfo {
			candidate := base
			candidate.Image = "docker.io/library/alpine:3.22"
			return []ContainerInfo{candidate}
		}(),
		"wrong tenant": func() []ContainerInfo {
			candidate := base
			candidate.Tenant = "tenant-b"
			return []ContainerInfo{candidate}
		}(),
		"wrong provider": func() []ContainerInfo {
			candidate := base
			candidate.ProviderUUID = "33333333-3333-4333-8333-333333333333"
			return []ContainerInfo{candidate}
		}(),
		"running replacement": func() []ContainerInfo {
			candidate := base
			candidate.Status = "running"
			return []ContainerInfo{candidate}
		}(),
		"duplicate index": {
			base,
			func() ContainerInfo {
				candidate := base
				candidate.ContainerID = "second-prev-id"
				return candidate
			}(),
		},
		"unsafe immutable ID": func() []ContainerInfo {
			candidate := base
			candidate.ContainerID = "forged\nidentifier"
			return []ContainerInfo{candidate}
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			mock := &mockDockerClient{
				ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
					return containers, nil
				},
			}
			b, stores := openCloseRecoveryBackend(t, dir, mock, nil)
			t.Cleanup(func() { closeCloseRecoveryBackend(t, b, stores) })
			seedCloseDeprovisionLease(t, b, stores)
			history, err := stores.releases.List(closeDeprovisionLeaseUUID)
			require.NoError(t, err)

			_, err = b.closeLegacyRollbackTargets(
				context.Background(),
				closeDeprovisionLeaseUUID,
				history,
				"tenant-a",
				closeDeprovisionProviderUUID,
				callbackURL,
				lifecycleCallbackURL,
			)
			require.Error(t, err)
		})
	}
}

func TestCloseLegacyRollbackTargetsMarkedMigrationRequiresCoherentIdentity(t *testing.T) {
	const callbackURL = "https://fred.example/callbacks/provision?operation_id=9a72fbc1-38c8-4f31-87f7-f689979b9324"
	lifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	items := []backend.LeaseItem{{
		SKU: "docker-small", ServiceName: "app", Quantity: 2,
	}}
	history := []shared.Release{{
		Version:          1,
		Manifest:         validStackManifestJSON(map[string]string{"app": "docker.io/library/nginx:1.27"}),
		Image:            "stack",
		Items:            items,
		ResourceProfiles: testResourceProfiles(t, items),
		Status:           "active",
		CreatedAt:        time.Now(),
		LegacyMigration:  true,
	}}
	base := ContainerInfo{
		ContainerID:          "immutable-prev-id-0",
		Name:                 "fred-" + closeDeprovisionLeaseUUID + "-app-0-prev",
		LeaseUUID:            closeDeprovisionLeaseUUID,
		Tenant:               "tenant-a",
		ProviderUUID:         closeDeprovisionProviderUUID,
		BackendName:          "docker",
		SKU:                  "docker-small",
		Image:                "docker.io/library/nginx:1.27",
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleCallbackURL,
		InstanceIndex:        0,
		Status:               "exited",
	}

	for name, mutate := range map[string]func([]ContainerInfo) []ContainerInfo{
		"empty tenant": func(containers []ContainerInfo) []ContainerInfo {
			containers[0].Tenant = ""
			return containers
		},
		"noncanonical provider": func(containers []ContainerInfo) []ContainerInfo {
			containers[0].ProviderUUID = "provider-a"
			return containers
		},
		"divergent tenant": func(containers []ContainerInfo) []ContainerInfo {
			second := containers[0]
			second.ContainerID = "immutable-prev-id-1"
			second.Name = "fred-" + closeDeprovisionLeaseUUID + "-app-1-prev"
			second.InstanceIndex = 1
			second.Tenant = "tenant-b"
			return append(containers, second)
		},
		"divergent provider": func(containers []ContainerInfo) []ContainerInfo {
			second := containers[0]
			second.ContainerID = "immutable-prev-id-1"
			second.Name = "fred-" + closeDeprovisionLeaseUUID + "-app-1-prev"
			second.InstanceIndex = 1
			second.ProviderUUID = "33333333-3333-4333-8333-333333333333"
			return append(containers, second)
		},
	} {
		t.Run(name, func(t *testing.T) {
			containers := mutate([]ContainerInfo{base})
			b := &Backend{
				cfg: Config{Name: "docker"},
				docker: &mockDockerClient{ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
					return containers, nil
				}},
			}
			_, err := b.closeLegacyRollbackTargets(
				context.Background(), closeDeprovisionLeaseUUID, history, "", "", "", "",
			)
			require.Error(t, err)
		})
	}

	t.Run("partial coherent cohort remains exact immutable authority", func(t *testing.T) {
		partial := base
		partial.ContainerID = "immutable-prev-id-1"
		partial.Name = "fred-" + closeDeprovisionLeaseUUID + "-app-1-prev"
		partial.InstanceIndex = 1
		b := &Backend{
			cfg: Config{Name: "docker"},
			docker: &mockDockerClient{ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
				return []ContainerInfo{partial}, nil
			}},
		}
		targets, err := b.closeLegacyRollbackTargets(
			context.Background(), closeDeprovisionLeaseUUID, history, "", "", "", "",
		)
		require.NoError(t, err)
		require.Equal(t, []shared.CloseLegacyRollbackTarget{{
			ContainerID: partial.ContainerID,
			Name:        partial.Name,
		}}, targets)
	})
}

func TestCloseLegacyRollbackTargetsMarkedMigrationMatchesTypedActiveAuthority(t *testing.T) {
	const (
		operationID = shared.OperationID("9a72fbc1-38c8-4f31-87f7-f689979b9324")
		callbackURL = "https://fred.example/callbacks/provision?operation_id=9a72fbc1-38c8-4f31-87f7-f689979b9324"
	)
	lifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	authority, err := shared.NewReleaseRuntimeAuthority(
		operationID,
		"tenant-a",
		closeDeprovisionProviderUUID,
		callbackURL,
		lifecycleCallbackURL,
	)
	require.NoError(t, err)
	items := []backend.LeaseItem{{SKU: "docker-small", ServiceName: "app", Quantity: 1}}
	profiles := testResourceProfiles(t, items)
	manifestBytes := validStackManifestJSON(map[string]string{"app": "docker.io/library/nginx:1.27"})
	history := []shared.Release{
		{
			Version: 1, Manifest: manifestBytes, Image: "stack", Items: items,
			ResourceProfiles: profiles, Status: "superseded", CreatedAt: time.Now(), LegacyMigration: true,
		},
		{
			Version: 2, Manifest: manifestBytes, Image: "stack", OperationID: operationID,
			Items: items, ResourceProfiles: profiles, RuntimeAuthority: &authority,
			Status: "active", CreatedAt: time.Now().Add(time.Second),
		},
	}
	cloned := ContainerInfo{
		ContainerID:          "same-name-replacement-id",
		Name:                 "fred-" + closeDeprovisionLeaseUUID + "-app-0-prev",
		LeaseUUID:            closeDeprovisionLeaseUUID,
		Tenant:               "different-tenant",
		ProviderUUID:         "33333333-3333-4333-8333-333333333333",
		BackendName:          "docker",
		SKU:                  "docker-small",
		Image:                "docker.io/library/nginx:1.27",
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleCallbackURL,
		InstanceIndex:        0,
		Status:               "exited",
	}
	b := &Backend{
		cfg: Config{Name: "docker"},
		docker: &mockDockerClient{ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{cloned}, nil
		}},
	}
	_, err = b.closeLegacyRollbackTargets(
		context.Background(), closeDeprovisionLeaseUUID, history, "", "", "", "",
	)
	require.ErrorContains(t, err, "differs from active release authority")
}

func TestDoDeprovision_CloseSettlementDoesNotPerformCallbackIOInline(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, nil)
	seedCloseDeprovisionLease(t, b, stores)
	const operationURL = "https://fred.example/callbacks/provision?operation_id=9a72fbc1-38c8-4f31-87f7-f689979b9324"
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(operationURL, "")
	require.NoError(t, err)
	b.provisionStore.UpdateFn(closeDeprovisionLeaseUUID, func(p *leasesm.ProvisionState) {
		p.CallbackURL = operationURL
		p.LifecycleCallbackURL = lifecycleURL
	})
	var requests atomic.Int32
	b.callbackSender = shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store:   stores.callbacks,
		Secret:  "test-secret-that-is-at-least-32-bytes",
		Logger:  b.logger,
		StopCtx: b.stopCtx,
		HTTPClient: &http.Client{Transport: dockerReplayRoundTripFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return nil, errors.New("callback transport must not run in close actor")
		})},
		BeforeReplay:    func(context.Context) error { return nil },
		BeforeDelivery:  func(context.Context) error { return nil },
		StorageIdentity: b.storageIdentity,
	})

	require.NoError(t, b.doDeprovision(context.Background(), closeDeprovisionLeaseUUID))
	require.Zero(t, requests.Load(), "durable settlement must only wake the tracked replay worker")
	pending, err := stores.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, backend.CallbackStatusDeprovisioned, pending[0].Status)

	closeCloseRecoveryBackend(t, b, stores)
}

func TestDoDeprovision_ReleaseFenceRefusesChangedAuthority(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, nil)
	seedCloseDeprovisionLease(t, b, stores)

	b.compose = &mockComposeExecutor{DownFn: func(
		context.Context,
		string,
		time.Duration,
	) error {
		// The close intent has already fenced the active release. Model an
		// impossible-under-normal-admission writer to prove the destructive
		// finalizer fails closed rather than deleting changed authority.
		return stores.releases.UpdateLatestStatus(
			closeDeprovisionLeaseUUID,
			"failed",
			backend.ReasonUpdateFailed,
			"mutated after close admission",
		)
	}}

	err := b.doDeprovision(context.Background(), closeDeprovisionLeaseUUID)
	require.ErrorContains(t, err, "release history changed after close admission")
	_, found, readErr := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, readErr)
	require.True(t, found, "a fence mismatch must retain durable recovery authority")
	releases, readErr := stores.releases.List(closeDeprovisionLeaseUUID)
	require.NoError(t, readErr)
	require.Len(t, releases, 1, "changed release evidence must not be erased")
	b.provisionsMu.RLock()
	projection := b.provisions[closeDeprovisionLeaseUUID]
	b.provisionsMu.RUnlock()
	require.NotNil(t, projection, "the retry owner survives until finalization is durable")
	require.Equal(t, backend.ProvisionStatusFailed, projection.Status)

	closeCloseRecoveryBackend(t, b, stores)
}

func TestDoDeprovision_CleanupOnlyClaimCleansUnprojectedSubstrate(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		destroyErr   error
		wantAttempts int
		wantPending  bool
	}{
		{name: "success retires the journal"},
		{
			name:         "uncertain volume cleanup remains durably retryable",
			destroyErr:   errors.New("injected volume EIO"),
			wantAttempts: 1,
			wantPending:  true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			dir := t.TempDir()
			var composeDown bool
			var destroyed []string
			b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, &mockVolumeManager{
				DestroyFn: func(_ context.Context, name string) error {
					destroyed = append(destroyed, name)
					return testCase.destroyErr
				},
			})
			b.compose = &mockComposeExecutor{DownFn: func(
				context.Context,
				string,
				time.Duration,
			) error {
				composeDown = true
				return nil
			}}
			items := []backend.LeaseItem{{
				SKU: "docker-small", ServiceName: "app", Quantity: 1,
			}}
			require.NoError(t, b.pool.TryAllocate(
				closeDeprovisionLeaseUUID+"-app-0",
				"docker-small",
				"tenant-a",
			))
			payload := validStackManifestJSON(map[string]string{
				"app": "docker.io/library/nginx:1.27",
			})
			resourceProfiles := testResourceProfiles(t, items)
			require.NoError(t, stores.releases.Append(closeDeprovisionLeaseUUID, shared.Release{
				Manifest:         payload,
				Image:            "stack",
				Items:            items,
				ResourceProfiles: resourceProfiles,
				Status:           "active",
				CreatedAt:        time.Now(),
			}))

			err := b.doDeprovision(context.Background(), closeDeprovisionLeaseUUID)
			if testCase.wantPending {
				require.ErrorContains(t, err, testCase.destroyErr.Error())
			} else {
				require.NoError(t, err)
			}
			require.True(t, composeDown,
				"projection absence must not bypass Compose discovery/teardown")
			require.Equal(t, []string{
				canonicalVolumeName(closeDeprovisionLeaseUUID, "app", 0),
			}, destroyed)

			claim, found, readErr := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
			require.NoError(t, readErr)
			require.Equal(t, testCase.wantPending, found)
			if found {
				require.True(t, claim.CleanupOnly())
				require.Equal(t, testCase.wantAttempts, claim.CleanupAttempts())
			}
			releases, readErr := stores.releases.List(closeDeprovisionLeaseUUID)
			require.NoError(t, readErr)
			if testCase.wantPending {
				require.Len(t, releases, 1,
					"uncertain cleanup must preserve its exact release authority")
				require.Equal(t, 1, b.pool.Stats().AllocationCount,
					"pending cleanup-only authority must keep its topology reserved")
			} else {
				require.Empty(t, releases)
				require.Zero(t, b.pool.Stats().AllocationCount,
					"terminal cleanup-only settlement must release its topology")
			}

			closeCloseRecoveryBackend(t, b, stores)
		})
	}
}
