package docker

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// TestCharacterization_ActiveReprovisionStartupCrashStaysProvisioning pins
// what happens today when an ACTIVE lease's re-provision fails startup
// verification after its first substrate Step (the predecessor teardown).
//
// It is a characterization, not a specification. The worker outcome is
// Ambiguous (a failure after an entered Step), so the actor makes no
// Provisioning -> Failed transition. Live operation recovery then settles the
// durable intent while preserving the projection, and recoverState preserves a
// Provisioning projection wholesale. The lease therefore reads "provisioning"
// indefinitely: it is never Failed, so the consecutive-failure budget never
// sees it and providerd never re-provisions or closes it. A startup crash loop
// on an ACTIVE lease does not reach the terminal budget (ENG-799 scope limit).
func TestCharacterization_ActiveReprovisionStartupCrashStaysProvisioning(t *testing.T) {
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440101"
	payload := validManifestJSON("nginx:latest")
	mock := &mockDockerClient{
		RemoveContainerFn: func(context.Context, string) error { return nil },
		PullImageFn:       func(context.Context, string, time.Duration) error { return nil },
		InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		leaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: leaseUUID, Status: backend.ProvisionStatusFailed,
			FailCount: 2, Quantity: 1, ContainerIDs: []string{"old-container"},
		}},
	})
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	require.NoError(t, b.pool.TryAllocate(leaseUUID+"-app-0", "docker-small", "tenant-a"))
	prepareFailedProvisionReplacement(t, b, mock, leaseUUID, "tenant-a", "docker-small", payload)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	// Every container other than the predecessor exits during startup.
	exited := func(info ContainerInfo) ContainerInfo {
		if info.ContainerID != "old-container" {
			info.Status = "exited"
			info.ExitCode = 1
		}
		return info
	}
	inspect := mock.InspectContainerFn
	mock.InspectContainerFn = func(ctx context.Context, containerID string) (*ContainerInfo, error) {
		info, err := inspect(ctx, containerID)
		if err != nil || info == nil {
			return info, err
		}
		observed := exited(*info)
		return &observed, nil
	}
	list := mock.ListManagedContainersFn
	mock.ListManagedContainersFn = func(ctx context.Context) ([]ContainerInfo, error) {
		containers, err := list(ctx)
		for index := range containers {
			containers[index] = exited(containers[index])
		}
		return containers, err
	}

	var callbacksMu sync.Mutex
	var callbacks int
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callbacksMu.Lock()
		callbacks++
		callbacksMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()
	rebuildCallbackSender(b, callbackServer.Client())
	b.wg.Go(b.callbackSender.RunReplayLoop)

	req := newProvisionRequest(leaseUUID, "tenant-a", "docker-small", 1, payload)
	req.CallbackURL = testOperationCallbackURL(callbackServer.URL)
	require.NoError(t, b.Provision(context.Background(), req))
	awaitProvisionWorkerQuiescence(t, b, leaseUUID)

	status := func() backend.ProvisionStatus {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		require.Contains(t, b.provisions, leaseUUID)
		return b.provisions[leaseUUID].Status
	}
	assert.Equal(t, backend.ProvisionStatusProvisioning, status(),
		"an Ambiguous worker outcome makes no Provisioning -> Failed transition")

	// Expire the operation's recovery window so live recovery settles it.
	b.cfg.ProvisionTimeout = time.Nanosecond
	for pass := range 2 {
		require.NoError(t, b.recoverLiveOperationIntents(context.Background()), "live recovery pass %d", pass)
		require.NoError(t, b.recoverState(context.Background()), "state recovery pass %d", pass)
	}

	intents, err := b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, intents, "live recovery settles the expired operation intent")
	assert.Equal(t, backend.ProvisionStatusProvisioning, status(),
		"the settled failure preserves the Provisioning projection: never Failed, never counted")
	b.provisionsMu.RLock()
	failCount := b.provisions[leaseUUID].FailCount
	b.provisionsMu.RUnlock()
	assert.Equal(t, 2, failCount, "the startup crash is not counted in fail_count either")
}
