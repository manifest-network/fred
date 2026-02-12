package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// --- Restart tests ---

func TestRestart_NotProvisioned(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)

	err := b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID: "nonexistent",
	})
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

func TestRestart_InvalidState_Provisioning(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusProvisioning,
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)

	err := b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID: "lease-1",
	})
	assert.ErrorIs(t, err, backend.ErrInvalidState)
	assert.Contains(t, err.Error(), "provisioning")
}

func TestRestart_AllowedFromFailed(t *testing.T) {
	manifest := &DockerManifest{Image: "nginx:latest"}
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusFailed,
			Manifest:     manifest,
			ContainerIDs: []string{"old-c1"},
		},
	}

	// Block the async phase so we can observe the Restarting state.
	stopStarted := make(chan struct{})
	stopRelease := make(chan struct{})
	mock := &mockDockerClient{
		StopContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			close(stopStarted)
			<-stopRelease
			return nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "", fmt.Errorf("intentional test abort")
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited"}, nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
	}

	b := newBackendForProvisionTest(t, mock, provisions)

	err := b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID: "lease-1",
	})
	assert.NoError(t, err, "restart should be accepted from Failed status")

	<-stopStarted

	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusRestarting, prov.Status)

	close(stopRelease)
	b.stopCancel()
}

func TestRestart_InvalidState_Restarting(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusRestarting,
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)

	err := b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID: "lease-1",
	})
	assert.ErrorIs(t, err, backend.ErrInvalidState)
}

func TestRestart_InvalidState_Updating(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusUpdating,
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)

	err := b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID: "lease-1",
	})
	assert.ErrorIs(t, err, backend.ErrInvalidState)
}

func TestRestart_NoManifest(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusReady,
			Manifest:  nil, // No stored manifest
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)

	err := b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID: "lease-1",
	})
	assert.ErrorIs(t, err, backend.ErrInvalidState)
	assert.Contains(t, err.Error(), "no stored manifest")
}

func TestRestart_SetsRestartingStatus(t *testing.T) {
	manifest := &DockerManifest{Image: "nginx:latest"}
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusReady,
			Manifest:     manifest,
			ContainerIDs: []string{"old-c1"},
		},
	}

	// Block the async phase so we can observe the Restarting state.
	stopStarted := make(chan struct{})
	stopRelease := make(chan struct{})
	mock := &mockDockerClient{
		StopContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			close(stopStarted)
			<-stopRelease
			return nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
		// Provide CreateContainerFn so the goroutine exits cleanly after release
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "", fmt.Errorf("intentional test abort")
		},
		// Provide InspectContainerFn and StartContainerFn for rollback path
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited"}, nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
	}

	b := newBackendForProvisionTest(t, mock, provisions)

	err := b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: "http://localhost/callback",
	})
	require.NoError(t, err)

	// Wait for async goroutine to enter StopContainer
	<-stopStarted

	b.provisionsMu.RLock()
	status := b.provisions["lease-1"].Status
	b.provisionsMu.RUnlock()

	assert.Equal(t, backend.ProvisionStatusRestarting, status)

	// Release the goroutine and let it finish (CreateContainer will fail gracefully)
	close(stopRelease)
	b.stopCancel()
}

func TestRestart_Success(t *testing.T) {
	manifest := &DockerManifest{Image: "nginx:latest"}
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusReady,
			Manifest:     manifest,
			ContainerIDs: []string{"old-c1"},
		},
	}

	var stoppedIDs, removedIDs, createdIDs []string
	var startedIDs []string

	mock := &mockDockerClient{
		StopContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			stoppedIDs = append(stoppedIDs, containerID)
			return nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			removedIDs = append(removedIDs, containerID)
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			createdIDs = append(createdIDs, "new-c1")
			return "new-c1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			startedIDs = append(startedIDs, containerID)
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	var callbackPayload backend.CallbackPayload
	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&callbackPayload)
		w.WriteHeader(http.StatusOK)
		close(callbackReceived)
	}))
	defer callbackServer.Close()

	b := newBackendForProvisionTest(t, mock, provisions)
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	err := b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: callbackServer.URL,
	})
	require.NoError(t, err)

	select {
	case <-callbackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for callback")
	}

	// Verify container lifecycle
	assert.Equal(t, []string{"old-c1"}, stoppedIDs, "old container should be stopped")
	assert.Equal(t, []string{"old-c1"}, removedIDs, "old container should be removed")
	assert.Equal(t, []string{"new-c1"}, createdIDs, "new container should be created")
	assert.Equal(t, []string{"new-c1"}, startedIDs, "new container should be started")

	// Verify callback
	assert.Equal(t, "lease-1", callbackPayload.LeaseUUID)
	assert.Equal(t, backend.CallbackStatusSuccess, callbackPayload.Status)

	// Verify final state
	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status)
	assert.Equal(t, []string{"new-c1"}, prov.ContainerIDs)
}

func TestRestart_Failure_ContainerStartFails(t *testing.T) {
	manifest := &DockerManifest{Image: "nginx:latest"}
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusReady,
			Manifest:     manifest,
			ContainerIDs: []string{"old-c1"},
			FailCount:    0,
		},
	}

	mock := &mockDockerClient{
		StopContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "new-c1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return fmt.Errorf("container start failed: OCI runtime error")
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited"}, nil
		},
	}

	var callbackPayload backend.CallbackPayload
	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&callbackPayload)
		w.WriteHeader(http.StatusOK)
		close(callbackReceived)
	}))
	defer callbackServer.Close()

	b := newBackendForProvisionTest(t, mock, provisions)
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)

	err := b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: callbackServer.URL,
	})
	require.NoError(t, err) // Restart returns nil — failure is async

	select {
	case <-callbackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for failure callback")
	}

	// Verify failure callback
	assert.Equal(t, backend.CallbackStatusFailed, callbackPayload.Status)
	assert.Contains(t, callbackPayload.Error, "restart failed")
	assert.Contains(t, callbackPayload.Error, "rollback failed")

	// Verify failure state
	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)
	assert.Equal(t, 1, prov.FailCount)
	assert.Contains(t, prov.LastError, "container start failed")
}

func TestRestart_Failure_SKUProfileLookupFails(t *testing.T) {
	manifest := &DockerManifest{Image: "nginx:latest"}
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			SKU:          "unknown-sku",
			Status:       backend.ProvisionStatusReady,
			Manifest:     manifest,
			ContainerIDs: []string{"old-c1"},
			FailCount:    0,
		},
	}

	mock := &mockDockerClient{}

	var callbackPayload backend.CallbackPayload
	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&callbackPayload)
		w.WriteHeader(http.StatusOK)
		close(callbackReceived)
	}))
	defer callbackServer.Close()

	b := newBackendForProvisionTest(t, mock, provisions)
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)

	err := b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: callbackServer.URL,
	})
	require.NoError(t, err) // Restart returns nil — failure is async

	select {
	case <-callbackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for failure callback")
	}

	// Verify failure callback
	assert.Equal(t, backend.CallbackStatusFailed, callbackPayload.Status)
	assert.Equal(t, "restart failed", callbackPayload.Error)

	// Verify provision stays Ready (not Failed) since old containers are still running.
	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status)
	assert.Equal(t, 1, prov.FailCount)
	assert.Contains(t, prov.LastError, "SKU profile lookup failed")
}

func TestRestart_MultipleContainers(t *testing.T) {
	manifest := &DockerManifest{Image: "nginx:latest"}
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusReady,
			Manifest:     manifest,
			ContainerIDs: []string{"old-c1", "old-c2"},
			Quantity:     2,
		},
	}

	var stopCount, removeCount, createCount, startCount atomic.Int32
	mock := &mockDockerClient{
		StopContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			stopCount.Add(1)
			return nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			removeCount.Add(1)
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			n := createCount.Add(1)
			return fmt.Sprintf("new-c%d", n), nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			startCount.Add(1)
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		close(callbackReceived)
	}))
	defer callbackServer.Close()

	b := newBackendForProvisionTest(t, mock, provisions)
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	err := b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: callbackServer.URL,
	})
	require.NoError(t, err)

	select {
	case <-callbackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for callback")
	}

	assert.Equal(t, int32(2), stopCount.Load(), "both old containers should be stopped")
	assert.Equal(t, int32(2), removeCount.Load(), "both old containers should be removed")
	assert.Equal(t, int32(2), createCount.Load(), "two new containers should be created")
	assert.Equal(t, int32(2), startCount.Load(), "two new containers should be started")

	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status)
	assert.Len(t, prov.ContainerIDs, 2)
}

func TestRestart_UpdatesCallbackURL(t *testing.T) {
	manifest := &DockerManifest{Image: "nginx:latest"}
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusReady,
			Manifest:     manifest,
			ContainerIDs: []string{"old-c1"},
			CallbackURL:  "http://old-callback/url",
		},
	}

	stopStarted := make(chan struct{})
	mock := &mockDockerClient{
		StopContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			close(stopStarted)
			return nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
		// Provide CreateContainerFn so the goroutine exits cleanly
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "", fmt.Errorf("intentional test abort")
		},
		// Provide InspectContainerFn and StartContainerFn for rollback path
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited"}, nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
	}

	b := newBackendForProvisionTest(t, mock, provisions)

	err := b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: "http://new-callback/url",
	})
	require.NoError(t, err)

	<-stopStarted

	b.provisionsMu.RLock()
	callbackURL := b.provisions["lease-1"].CallbackURL
	b.provisionsMu.RUnlock()

	assert.Equal(t, "http://new-callback/url", callbackURL)

	b.stopCancel()
}

// --- Update tests ---

func TestUpdate_NotProvisioned(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)

	err := b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID: "nonexistent",
		Payload:   validManifestJSON("nginx:latest"),
	})
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

func TestUpdate_InvalidState_Provisioning(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusProvisioning,
			SKU:       "docker-small",
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)

	err := b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID: "lease-1",
		Payload:   validManifestJSON("nginx:latest"),
	})
	assert.ErrorIs(t, err, backend.ErrInvalidState)
}

func TestUpdate_InvalidState_Restarting(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusRestarting,
			SKU:       "docker-small",
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)

	err := b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID: "lease-1",
		Payload:   validManifestJSON("nginx:latest"),
	})
	assert.ErrorIs(t, err, backend.ErrInvalidState)
}

func TestUpdate_InvalidState_Updating(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusUpdating,
			SKU:       "docker-small",
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)

	err := b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID: "lease-1",
		Payload:   validManifestJSON("nginx:latest"),
	})
	assert.ErrorIs(t, err, backend.ErrInvalidState)
}

func TestUpdate_AllowedFromReady(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{"old-c1"},
			Quantity:     1,
		},
	}

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "new-c1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		close(callbackReceived)
	}))
	defer callbackServer.Close()

	b := newBackendForProvisionTest(t, mock, provisions)
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	err := b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: callbackServer.URL,
		Payload:     validManifestJSON("nginx:1.26"),
	})
	require.NoError(t, err)

	select {
	case <-callbackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for callback")
	}

	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status)
	assert.Equal(t, "nginx:1.26", prov.Image)
	assert.Equal(t, []string{"new-c1"}, prov.ContainerIDs)
	assert.NotNil(t, prov.Manifest)
}

func TestUpdate_AllowedFromFailed(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusFailed,
			ContainerIDs: []string{"old-c1"},
			Quantity:     1,
			FailCount:    2,
		},
	}

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "new-c1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		close(callbackReceived)
	}))
	defer callbackServer.Close()

	b := newBackendForProvisionTest(t, mock, provisions)
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	err := b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: callbackServer.URL,
		Payload:     validManifestJSON("redis:7"),
	})
	require.NoError(t, err)

	select {
	case <-callbackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for callback")
	}

	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status)
	assert.Equal(t, "redis:7", prov.Image)
}

func TestUpdate_InvalidManifest(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusReady,
			SKU:       "docker-small",
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)

	// Manifest with missing image
	err := b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID: "lease-1",
		Payload:   []byte(`{"ports":{"80/tcp":{}}}`),
	})
	assert.ErrorIs(t, err, backend.ErrInvalidManifest)
}

func TestUpdate_ImageNotAllowed(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusReady,
			SKU:       "docker-small",
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	// AllowedRegistries defaults to ["docker.io"] in DefaultConfig

	err := b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID: "lease-1",
		Payload:   validManifestJSON("evil.registry.com/malware:latest"),
	})
	assert.ErrorIs(t, err, backend.ErrValidation)
}

func TestUpdate_SetsUpdatingStatus(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{"old-c1"},
			Quantity:     1,
		},
	}

	pullStarted := make(chan struct{})
	pullRelease := make(chan struct{})
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			close(pullStarted)
			<-pullRelease
			// Return error so goroutine exits cleanly after release
			return fmt.Errorf("intentional test abort")
		},
	}

	b := newBackendForProvisionTest(t, mock, provisions)

	err := b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: "http://localhost/callback",
		Payload:     validManifestJSON("nginx:1.26"),
	})
	require.NoError(t, err)

	<-pullStarted

	b.provisionsMu.RLock()
	status := b.provisions["lease-1"].Status
	b.provisionsMu.RUnlock()

	assert.Equal(t, backend.ProvisionStatusUpdating, status)

	close(pullRelease)
	b.stopCancel()
}

func TestUpdate_Failure_ImagePullFails(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{"old-c1"},
			Quantity:     1,
			FailCount:    0,
		},
	}

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return fmt.Errorf("registry unreachable")
		},
	}

	var callbackPayload backend.CallbackPayload
	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&callbackPayload)
		w.WriteHeader(http.StatusOK)
		close(callbackReceived)
	}))
	defer callbackServer.Close()

	b := newBackendForProvisionTest(t, mock, provisions)
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)

	err := b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: callbackServer.URL,
		Payload:     validManifestJSON("nginx:1.26"),
	})
	require.NoError(t, err)

	select {
	case <-callbackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for failure callback")
	}

	assert.Equal(t, backend.CallbackStatusFailed, callbackPayload.Status)
	assert.Equal(t, "image pull failed", callbackPayload.Error)

	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	// Update failure always marks status as Failed (desired state was not achieved).
	assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)
	assert.Equal(t, 1, prov.FailCount)
	assert.Contains(t, prov.LastError, "registry unreachable")
}

func TestUpdate_RecordsRelease(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "releases.db")
	releaseStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer releaseStore.Close()

	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{"old-c1"},
			Quantity:     1,
		},
	}

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "new-c1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		close(callbackReceived)
	}))
	defer callbackServer.Close()

	b := newBackendForProvisionTest(t, mock, provisions)
	b.releaseStore = releaseStore
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	err = b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: callbackServer.URL,
		Payload:     validManifestJSON("nginx:1.26"),
	})
	require.NoError(t, err)

	select {
	case <-callbackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for callback")
	}

	// Verify release was recorded and marked active
	releases, err := releaseStore.List("lease-1")
	require.NoError(t, err)
	require.Len(t, releases, 1)
	assert.Equal(t, 1, releases[0].Version)
	assert.Equal(t, "nginx:1.26", releases[0].Image)
	assert.Equal(t, "active", releases[0].Status)
}

func TestUpdate_ReleaseMarkedFailedOnError(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "releases.db")
	releaseStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer releaseStore.Close()

	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{"old-c1"},
			Quantity:     1,
		},
	}

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return fmt.Errorf("pull failed")
		},
	}

	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		close(callbackReceived)
	}))
	defer callbackServer.Close()

	b := newBackendForProvisionTest(t, mock, provisions)
	b.releaseStore = releaseStore
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)

	err = b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: callbackServer.URL,
		Payload:     validManifestJSON("nginx:bad-tag"),
	})
	require.NoError(t, err)

	select {
	case <-callbackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for callback")
	}

	// Verify release was recorded and marked failed
	releases, err := releaseStore.List("lease-1")
	require.NoError(t, err)
	require.Len(t, releases, 1)
	assert.Equal(t, "failed", releases[0].Status)
	assert.Contains(t, releases[0].Error, "pull failed")
}

func TestUpdate_CleansUpNewContainersOnFailure(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{"old-c1"},
			Quantity:     1,
		},
	}

	var removedContainers []string
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			removedContainers = append(removedContainers, containerID)
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "new-c1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return fmt.Errorf("start failed")
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited"}, nil
		},
	}

	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		close(callbackReceived)
	}))
	defer callbackServer.Close()

	b := newBackendForProvisionTest(t, mock, provisions)
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)

	err := b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: callbackServer.URL,
		Payload:     validManifestJSON("nginx:1.26"),
	})
	require.NoError(t, err)

	select {
	case <-callbackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for callback")
	}

	// New container should be cleaned up on failure; old container is kept for rollback.
	assert.Contains(t, removedContainers, "new-c1", "new container should be cleaned up on failure")
	assert.NotContains(t, removedContainers, "old-c1", "old container should be kept for rollback")
}

func TestUpdate_UpdatesManifestAndImage(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{"old-c1"},
			Quantity:     1,
			Image:        "nginx:1.25",
			Manifest:     &DockerManifest{Image: "nginx:1.25"},
		},
	}

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "new-c1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		close(callbackReceived)
	}))
	defer callbackServer.Close()

	b := newBackendForProvisionTest(t, mock, provisions)
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	err := b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: callbackServer.URL,
		Payload:     validManifestJSON("redis:7"),
	})
	require.NoError(t, err)

	select {
	case <-callbackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for callback")
	}

	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()

	assert.Equal(t, "redis:7", prov.Image, "image should be updated")
	assert.Equal(t, "redis:7", prov.Manifest.Image, "manifest should be updated")
}

func TestUpdate_RollbackToReady_AllowsRestart(t *testing.T) {
	oldManifest := &DockerManifest{Image: "postgres:17", Command: []string{"postgres"}}
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusReady,
			Manifest:     oldManifest,
			Image:        "postgres:17",
			ContainerIDs: []string{"old-c1"},
			Quantity:     1,
		},
	}

	startCalls := 0
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		StopContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "new-c1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			startCalls++
			if startCalls == 1 {
				// First start (update's new container) fails
				return fmt.Errorf("postgres:18 incompatible data directory")
			}
			// Subsequent starts (rollback of old container) succeed
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	var callbackPayload backend.CallbackPayload
	updateCallbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&callbackPayload)
		w.WriteHeader(http.StatusOK)
		select {
		case updateCallbackReceived <- struct{}{}:
		default:
		}
	}))
	defer callbackServer.Close()

	b := newBackendForProvisionTest(t, mock, provisions)
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)

	// 1. Update to postgres:18 — should fail and rollback
	err := b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: callbackServer.URL,
		Payload:     validManifestJSON("postgres:18"),
	})
	require.NoError(t, err)

	select {
	case <-updateCallbackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for update failure callback")
	}

	// 2. Verify status is Ready (containers are running after rollback)
	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()

	assert.Equal(t, backend.ProvisionStatusReady, prov.Status,
		"status should be Ready because old containers were restored")
	assert.Equal(t, 1, prov.FailCount)
	assert.Contains(t, prov.LastError, "incompatible data directory",
		"LastError should describe why the update failed")
	assert.Equal(t, "postgres:17", prov.Manifest.Image,
		"manifest should still be the old image after failed update")

	// 3. Callback should indicate failure with rollback context
	assert.Equal(t, backend.CallbackStatusFailed, callbackPayload.Status)
	assert.Contains(t, callbackPayload.Error, "rolled back to previous version",
		"callback error should mention rollback")

	// 4. Restart should be accepted from Ready status
	err = b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: callbackServer.URL,
	})
	assert.NoError(t, err, "restart should be allowed from Ready status after rollback")
}

func TestUpdate_RollbackFailed_SetsStatusFailed(t *testing.T) {
	oldManifest := &DockerManifest{Image: "postgres:17", Command: []string{"postgres"}}
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusReady,
			Manifest:     oldManifest,
			Image:        "postgres:17",
			ContainerIDs: []string{"old-c1"},
			Quantity:     1,
		},
	}

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		StopContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "new-c1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			// Both new container start and rollback start fail
			return fmt.Errorf("docker daemon unavailable")
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited"}, nil
		},
	}

	var callbackPayload backend.CallbackPayload
	callbackReceived := make(chan struct{}, 1)
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&callbackPayload)
		w.WriteHeader(http.StatusOK)
		select {
		case callbackReceived <- struct{}{}:
		default:
		}
	}))
	defer callbackServer.Close()

	b := newBackendForProvisionTest(t, mock, provisions)
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)

	err := b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: callbackServer.URL,
		Payload:     validManifestJSON("postgres:18"),
	})
	require.NoError(t, err)

	select {
	case <-callbackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for callback")
	}

	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()

	assert.Equal(t, backend.ProvisionStatusFailed, prov.Status,
		"status should be Failed when rollback also fails")
	assert.Contains(t, prov.LastError, "docker daemon unavailable")

	assert.Equal(t, backend.CallbackStatusFailed, callbackPayload.Status)
	assert.Contains(t, callbackPayload.Error, "rollback failed",
		"callback error should mention rollback failure")
}

// --- GetReleases tests ---

func TestGetReleases_NotProvisioned(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)

	releases, err := b.GetReleases(context.Background(), "nonexistent")
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
	assert.Nil(t, releases)
}

func TestGetReleases_NilReleaseStore(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusReady,
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	b.releaseStore = nil

	releases, err := b.GetReleases(context.Background(), "lease-1")
	assert.NoError(t, err)
	assert.Nil(t, releases)
}

func TestGetReleases_WithReleases(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "releases.db")
	releaseStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer releaseStore.Close()

	// Seed some releases
	now := time.Now()
	require.NoError(t, releaseStore.Append("lease-1", shared.Release{
		Manifest:  []byte(`{"image":"nginx:1.25"}`),
		Image:     "nginx:1.25",
		Status:    "superseded",
		CreatedAt: now.Add(-1 * time.Hour),
	}))
	require.NoError(t, releaseStore.Append("lease-1", shared.Release{
		Manifest:  []byte(`{"image":"nginx:1.26"}`),
		Image:     "nginx:1.26",
		Status:    "active",
		CreatedAt: now,
	}))

	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusReady,
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	b.releaseStore = releaseStore

	releases, err := b.GetReleases(context.Background(), "lease-1")
	require.NoError(t, err)
	require.Len(t, releases, 2)

	assert.Equal(t, 1, releases[0].Version)
	assert.Equal(t, "nginx:1.25", releases[0].Image)
	assert.Equal(t, "superseded", releases[0].Status)

	assert.Equal(t, 2, releases[1].Version)
	assert.Equal(t, "nginx:1.26", releases[1].Image)
	assert.Equal(t, "active", releases[1].Status)
}

func TestGetReleases_EmptyHistory(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "releases.db")
	releaseStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer releaseStore.Close()

	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusReady,
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	b.releaseStore = releaseStore

	releases, err := b.GetReleases(context.Background(), "lease-1")
	require.NoError(t, err)
	assert.Empty(t, releases)
}

// --- RecoverState: Restarting/Updating preserved ---

func TestRecoverState_RestartingPreserved(t *testing.T) {
	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}
	existing := map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Tenant:    "tenant-a",
			Status:    backend.ProvisionStatusRestarting,
			CreatedAt: time.Now(),
		},
	}
	b := newBackendForTest(mock, existing)

	err := b.recoverState(context.Background())
	require.NoError(t, err)

	prov := b.provisions["lease-1"]
	require.NotNil(t, prov)
	assert.Equal(t, backend.ProvisionStatusRestarting, prov.Status,
		"restarting provision should be preserved through recoverState")
}

func TestRecoverState_UpdatingPreserved(t *testing.T) {
	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}
	existing := map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Tenant:    "tenant-a",
			Status:    backend.ProvisionStatusUpdating,
			CreatedAt: time.Now(),
		},
	}
	b := newBackendForTest(mock, existing)

	err := b.recoverState(context.Background())
	require.NoError(t, err)

	prov := b.provisions["lease-1"]
	require.NotNil(t, prov)
	assert.Equal(t, backend.ProvisionStatusUpdating, prov.Status,
		"updating provision should be preserved through recoverState")
}

// --- Deprovision cleans up releases ---

func TestDeprovision_CleansUpReleases(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "releases.db")
	releaseStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer releaseStore.Close()

	// Seed a release
	require.NoError(t, releaseStore.Append("lease-1", shared.Release{
		Manifest:  []byte(`{"image":"nginx:1.25"}`),
		Image:     "nginx:1.25",
		Status:    "active",
		CreatedAt: time.Now(),
	}))

	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{"c1"},
			Quantity:     1,
		},
	}

	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
	}

	b := newBackendForTest(mock, provisions)
	b.releaseStore = releaseStore
	b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")

	err = b.Deprovision(context.Background(), "lease-1")
	require.NoError(t, err)

	// Verify releases were cleaned up
	releases, err := releaseStore.List("lease-1")
	require.NoError(t, err)
	assert.Empty(t, releases)
}

// --- Initial release recorded on Provision success ---

func TestProvision_RecordsInitialRelease(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "releases.db")
	releaseStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer releaseStore.Close()

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "c1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		close(callbackReceived)
	}))
	defer callbackServer.Close()

	b := newBackendForProvisionTest(t, mock, nil)
	b.releaseStore = releaseStore
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	req.CallbackURL = callbackServer.URL

	err = b.Provision(context.Background(), req)
	require.NoError(t, err)

	select {
	case <-callbackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for callback")
	}

	// Verify initial release was recorded
	releases, err := releaseStore.List("lease-1")
	require.NoError(t, err)
	require.Len(t, releases, 1)
	assert.Equal(t, 1, releases[0].Version)
	assert.Equal(t, "nginx:latest", releases[0].Image)
	assert.Equal(t, "active", releases[0].Status)
}

// --- rollbackContainers tests ---

func TestRollbackContainers_AllStopped(t *testing.T) {
	var renamed, started []string
	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited"}, nil
		},
		RenameContainerFn: func(ctx context.Context, containerID string, newName string) error {
			renamed = append(renamed, containerID)
			return nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			started = append(started, containerID)
			return nil
		},
	}

	b := newBackendForTest(mock, nil)
	ok := b.rollbackContainers("lease-1", []string{"c1", "c2"}, b.logger)

	assert.True(t, ok, "rollback should succeed when all containers are restored")
	assert.Equal(t, []string{"c1", "c2"}, renamed)
	assert.Equal(t, []string{"c1", "c2"}, started)
}

func TestRollbackContainers_PartialStop_SomeRunning(t *testing.T) {
	// Simulates partial-stop scenario: c1 was stopped, c2 is still running.
	var started []string
	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			if containerID == "c1" {
				return &ContainerInfo{ContainerID: "c1", Status: "exited"}, nil
			}
			return &ContainerInfo{ContainerID: "c2", Status: "running"}, nil
		},
		RenameContainerFn: func(ctx context.Context, containerID string, newName string) error {
			return nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			started = append(started, containerID)
			return nil
		},
	}

	b := newBackendForTest(mock, nil)
	ok := b.rollbackContainers("lease-1", []string{"c1", "c2"}, b.logger)

	assert.True(t, ok, "rollback should succeed: c1 restarted, c2 already running")
	assert.Equal(t, []string{"c1"}, started, "only stopped container should be started")
}

func TestRollbackContainers_StartFails(t *testing.T) {
	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited"}, nil
		},
		RenameContainerFn: func(ctx context.Context, containerID string, newName string) error {
			return nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return fmt.Errorf("docker daemon error")
		},
	}

	b := newBackendForTest(mock, nil)
	ok := b.rollbackContainers("lease-1", []string{"c1"}, b.logger)

	assert.False(t, ok, "rollback should fail when start fails")
}

func TestRollbackContainers_InspectFails(t *testing.T) {
	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return nil, fmt.Errorf("container not found")
		},
	}

	b := newBackendForTest(mock, nil)
	ok := b.rollbackContainers("lease-1", []string{"c1"}, b.logger)

	assert.False(t, ok, "rollback should fail when inspect fails")
}

func TestRestart_RollbackClearsLastError(t *testing.T) {
	manifest := &DockerManifest{Image: "nginx:latest"}
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusReady,
			Manifest:     manifest,
			ContainerIDs: []string{"old-c1"},
		},
	}

	mock := &mockDockerClient{
		StopContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "", fmt.Errorf("create failed")
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited"}, nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
	}

	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		close(callbackReceived)
	}))
	defer callbackServer.Close()

	b := newBackendForProvisionTest(t, mock, provisions)
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)

	err := b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: callbackServer.URL,
	})
	require.NoError(t, err)

	select {
	case <-callbackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for callback")
	}

	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()

	assert.Equal(t, backend.ProvisionStatusReady, prov.Status, "should be ready after successful rollback")
	assert.Empty(t, prov.LastError, "LastError should be cleared after successful rollback")
	assert.Equal(t, 1, prov.FailCount, "FailCount should still reflect the failed attempt")
}

func TestConcurrentRestartAndUpdate_OnlyOneSucceeds(t *testing.T) {
	manifest := &DockerManifest{Image: "nginx:latest"}
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusReady,
			Manifest:     manifest,
			ContainerIDs: []string{"old-c1"},
			Quantity:     1,
		},
	}

	// Block StopContainer so the first caller holds the async phase long enough
	// for the second to attempt its call.
	stopStarted := make(chan struct{}, 2)
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		StopContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			stopStarted <- struct{}{}
			// small delay to give the racer a window
			time.Sleep(50 * time.Millisecond)
			return nil
		},
		RenameContainerFn: func(ctx context.Context, containerID string, newName string) error {
			return nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "new-c1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	b := newBackendForProvisionTest(t, mock, provisions)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	// Launch Restart and Update concurrently.
	errs := make(chan error, 2)
	go func() {
		errs <- b.Restart(context.Background(), backend.RestartRequest{
			LeaseUUID: "lease-1",
		})
	}()
	go func() {
		errs <- b.Update(context.Background(), backend.UpdateRequest{
			LeaseUUID: "lease-1",
			Payload:   validManifestJSON("nginx:1.26"),
		})
	}()

	err1 := <-errs
	err2 := <-errs

	// Exactly one should succeed and one should fail with ErrInvalidState.
	if err1 == nil && err2 == nil {
		t.Fatal("expected exactly one call to fail with ErrInvalidState, but both succeeded")
	}
	if err1 != nil && err2 != nil {
		t.Fatalf("expected exactly one call to succeed, but both failed: %v; %v", err1, err2)
	}

	var failedErr error
	if err1 != nil {
		failedErr = err1
	} else {
		failedErr = err2
	}
	assert.ErrorIs(t, failedErr, backend.ErrInvalidState)
}
