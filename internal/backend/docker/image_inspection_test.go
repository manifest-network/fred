package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
	"github.com/manifest-network/fred/internal/backendidentity"
)

// This fixture enters actual SDK Create/Inspect/Copy/Remove through the normal
// typed executor and identity-bound journal. It never fabricates a cleanup
// receipt or calls a raw deletion helper with a caller-selected container ID.
type inspectionDaemon struct {
	mu            sync.Mutex
	containers    map[string]container.InspectResponse
	creates       int
	removes       int
	volumes       int
	removeVolumes []bool
	createErr     error
	removeErr     error
	delayCreate   bool
	late          *container.InspectResponse
	copy          func(context.Context, string) (io.ReadCloser, error)
	beforeCreate  func()
	afterCreate   func()
}

func (d *inspectionDaemon) request(t *testing.T, req *http.Request) (*http.Response, error) {
	t.Helper()
	path := req.URL.Path
	switch {
	case strings.Contains(path, "/images/"):
		return imageSecurityResponse(200, fmt.Sprintf(`{"Id":%q,"Os":"linux","Architecture":"amd64","Config":{"Volumes":{"/data":{}},"User":"app"}}`, testImageID)), nil
	case strings.HasSuffix(path, "/containers/create"):
		if d.beforeCreate != nil {
			d.beforeCreate()
		}
		var config container.Config
		require.NoError(t, json.NewDecoder(req.Body).Decode(&config))
		d.mu.Lock()
		defer d.mu.Unlock()
		d.creates++
		id := fmt.Sprintf("%064x", d.creates)
		actual := container.InspectResponse{
			ContainerJSONBase: &container.ContainerJSONBase{ID: id, Name: "/" + req.URL.Query().Get("name"), Image: config.Image, State: &container.State{Status: "created"}},
			Config:            &config,
		}
		if d.delayCreate {
			d.late = &actual
		} else {
			d.containers[id] = actual
			d.volumes++
		}
		if d.afterCreate != nil {
			d.afterCreate()
		}
		if d.createErr != nil {
			return nil, d.createErr
		}
		return imageSecurityResponse(201, fmt.Sprintf(`{"Id":%q}`, id)), nil
	case strings.HasSuffix(path, "/json") && strings.Contains(path, "/containers/"):
		name := strings.TrimSuffix(path[strings.Index(path, "/containers/")+len("/containers/"):], "/json")
		d.mu.Lock()
		defer d.mu.Unlock()
		for _, actual := range d.containers {
			if name == actual.ID || name == strings.TrimPrefix(actual.Name, "/") {
				data, err := json.Marshal(actual)
				require.NoError(t, err)
				return imageSecurityResponse(200, string(data)), nil
			}
		}
		return imageSecurityResponse(404, `{"message":"No such container"}`), nil
	case strings.HasSuffix(path, "/archive"):
		reader, err := d.copy(req.Context(), req.URL.Query().Get("path"))
		if err != nil {
			return nil, err
		}
		stat, err := json.Marshal(container.PathStat{Name: "data", Mode: 0755})
		require.NoError(t, err)
		return &http.Response{StatusCode: 200, Header: http.Header{"X-Docker-Container-Path-Stat": {base64.StdEncoding.EncodeToString(stat)}}, Body: reader}, nil
	case req.Method == http.MethodDelete && strings.Contains(path, "/containers/"):
		d.mu.Lock()
		defer d.mu.Unlock()
		require.NoError(t, req.Context().Err(), "cleanup must not inherit canceled work")
		d.removes++
		withVolumes := req.URL.Query().Get("v") == "1" || req.URL.Query().Get("v") == "true"
		d.removeVolumes = append(d.removeVolumes, withVolumes)
		if d.removeErr != nil {
			return nil, d.removeErr
		}
		id := path[strings.Index(path, "/containers/")+len("/containers/"):]
		_, exists := d.containers[id]
		delete(d.containers, id)
		if exists && withVolumes {
			d.volumes--
		}
		return imageSecurityResponse(204, ""), nil
	default:
		return nil, fmt.Errorf("unexpected inspection Docker request: %s %s", req.Method, path)
	}
}

type inspectionHarness struct {
	daemon     *inspectionDaemon
	client     *DockerClient
	owner      *imageInspectionCoordinator
	callbacks  *shared.CallbackStore
	settlement *shared.OperationSettlement
	execution  shared.OperationExecutionClaim
	backend    *Backend
	image      imageexec.Image
	origin     shared.ImageInspectionOrigin
	runner     substratemutation.Runner
	subject    shared.OperationPhysicalSubject
	run        func(context.Context, shared.ImageInspectionOrigin) error
	stop       context.CancelFunc
	dbPath     string
	authority  *operationIntentTestAuthority
}

func newInspectionHarness(t *testing.T) *inspectionHarness {
	t.Helper()
	return newInspectionHarnessWithClient(t, nil)
}

func newInspectionHarnessWithClient(t *testing.T, build func(*inspectionDaemon) *DockerClient, reference ...string) *inspectionHarness {
	t.Helper()
	h := &inspectionHarness{daemon: &inspectionDaemon{containers: make(map[string]container.InspectResponse)}, dbPath: filepath.Join(t.TempDir(), "callbacks.db")}
	if build == nil {
		h.client = newImageSecurityDockerClient(t, func(req *http.Request) (*http.Response, error) { return h.daemon.request(t, req) })
	} else {
		h.client = build(h.daemon)
	}
	h.client.backendName = "docker"
	var err error
	h.callbacks, err = newBoundCallbackStoreForTest(t, shared.CallbackStoreConfig{DBPath: h.dbPath})
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.callbacks.Close() })
	_, h.settlement = operationSettlementForCallbackTest(t, h.callbacks)
	value, ok := operationIntentTestAuthorities.Load(h.callbacks)
	require.True(t, ok)
	h.authority = value.(*operationIntentTestAuthority)
	lifetime, stop := context.WithCancel(t.Context())
	h.stop = stop
	t.Cleanup(stop)
	h.backend = &Backend{stopCtx: lifetime, storageIdentity: h.authority.storage.ID(), storeAuthorityGate: h.authority.gate,
		storageVerifier: testDockerRuntimeStorageVerifier{id: h.authority.storage.ID()}}
	h.owner, err = newImageInspectionCoordinator(h.client, h.callbacks, lifetime, h.backend.authorizeStorageMutation, h.backend.completeStorageMutation, h.backend.resolveBackgroundStorageStep, h.backend.terminalStorageAuthorityError)
	require.NoError(t, err)
	imageReference := "fixture:latest"
	if len(reference) != 0 {
		imageReference = reference[0]
	}
	h.image, err = h.client.AdmitImage(t.Context(), imageReference)
	require.NoError(t, err)
	require.NoError(t, shared.BindOperationSubstrateExecutor(h.settlement,
		h.backend.authorizeStorageMutation, h.backend.completeStorageMutation,
		func(runner substratemutation.Runner, subject shared.OperationPhysicalSubject) func(context.Context) error {
			h.origin = shared.ImageInspectionForOperation(subject)
			h.runner, h.subject = runner, subject
			return func(ctx context.Context) error {
				return runner.Step(ctx, "inspect fixture image", func(ctx context.Context) error { return h.run(ctx, h.origin) })
			}
		},
		func(ctx context.Context, run func(context.Context) error, _ shared.OperationPhysicalSubject) error {
			return run(ctx)
		},
		func(context.Context, shared.OperationPhysicalSubject) (shared.OperationPhysicalEvidence, error) {
			return shared.OperationPhysicalEvidence{}, errors.New("fixture intentionally leaves originating operation pending")
		},
	))
	spec := dockerOperationIntentSpec(t, h.authority.storage.ID())
	candidate, err := h.settlement.NewOperationIntentCandidate(spec)
	require.NoError(t, err)
	admission, err := h.settlement.BeginOperationIntent(candidate)
	require.NoError(t, err)
	claim, ok := admission.CreatedClaim()
	require.True(t, ok)
	release, err := h.settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	h.execution, err = h.settlement.StartOperationExecution(release)
	require.NoError(t, err)
	return h
}

func (h *inspectionHarness) execute(t *testing.T, run func(context.Context, shared.ImageInspectionOrigin) error) {
	t.Helper()
	h.run = run
	_ = h.settlement.ExecuteOperation(t.Context(), h.execution)
}

func (h *inspectionHarness) reopen(t *testing.T) {
	t.Helper()
	h.stop()
	require.NoError(t, h.callbacks.Close())
	gate, err := backendidentity.NewStorageAuthorityGate(func(error) {})
	require.NoError(t, err)
	h.callbacks, err = shared.OpenIdentityBoundCallbackStore(shared.CallbackStoreConfig{DBPath: h.dbPath}, h.authority.storage, gate)
	require.NoError(t, err)
	lifetime, stop := context.WithCancel(t.Context())
	h.stop = stop
	t.Cleanup(stop)
	h.backend = &Backend{stopCtx: lifetime, storageIdentity: h.authority.storage.ID(), storeAuthorityGate: gate, storageVerifier: testDockerRuntimeStorageVerifier{id: h.authority.storage.ID()}}
	// A new SDK facade has the same daemon but a new admitted-image issuer and
	// a fresh session owner, exactly as process restart construction does.
	h.client = newImageSecurityDockerClient(t, func(req *http.Request) (*http.Response, error) { return h.daemon.request(t, req) })
	h.client.backendName = "docker"
	h.owner, err = newImageInspectionCoordinator(h.client, h.callbacks, lifetime, h.backend.authorizeStorageMutation, h.backend.completeStorageMutation, h.backend.resolveBackgroundStorageStep, h.backend.terminalStorageAuthorityError)
	require.NoError(t, err)
}

func inspectionTar(t *testing.T, path string) io.ReadCloser {
	t.Helper()
	var data bytes.Buffer
	tw := tar.NewWriter(&data)
	content := "app:x:1000:1000:app:/home/app:/bin/sh\n"
	name := "passwd"
	typeflag := byte(tar.TypeReg)
	if path != "/etc/passwd" {
		name = "data/"
		content = ""
		typeflag = tar.TypeDir
	}
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: name, Typeflag: typeflag, Mode: 0755, Uid: 1000, Gid: 1000, Size: int64(len(content))}))
	_, err := io.WriteString(tw, content)
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	return io.NopCloser(bytes.NewReader(data.Bytes()))
}

func TestImageInspectionCancellationOwnsCleanupAcrossAllHelpers(t *testing.T) {
	for _, name := range []string{"user", "owner", "writable", "extract"} {
		t.Run(name, func(t *testing.T) {
			h := newInspectionHarness(t)
			h.daemon.beforeCreate = func() {
				receipts, err := h.owner.journal.List()
				require.NoError(t, err)
				require.Len(t, receipts, 1)
				assert.Equal(t, h.image.ID(), receipts[0].ImageID())
				assert.Empty(t, receipts[0].ContainerID(), "write-ahead receipt precedes SDK Create")
			}
			h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()
				h.daemon.copy = func(context.Context, string) (io.ReadCloser, error) { cancel(); return nil, context.Canceled }
				var err error
				switch name {
				case "user":
					_, _, err = h.client.ResolveImageUser(ctx, h.image, "app", origin)
				case "owner":
					_, _, err = h.client.DetectVolumeOwner(ctx, h.image, []string{"/data"}, origin)
				case "writable":
					_, err = h.client.DetectWritablePaths(ctx, h.image, 1000, []string{"/data"}, origin)
				case "extract":
					err = h.client.ExtractImageContent(ctx, h.image, []string{"/data"}, t.TempDir(), 1024, 10, origin)["/data"]
				}
				require.ErrorIs(t, err, context.Canceled)
				return err
			})
			assert.Equal(t, 1, h.daemon.creates)
			assert.Equal(t, 1, h.daemon.removes)
			assert.Empty(t, h.daemon.containers)
			assert.Zero(t, h.daemon.volumes)
			assert.Equal(t, []bool{true}, h.daemon.removeVolumes)
			receipts, err := h.owner.journal.List()
			require.NoError(t, err)
			assert.Empty(t, receipts)
		})
	}
}

func TestImageInspectionRecoveryRetainsResponseLostCreateForLateAppearance(t *testing.T) {
	h := newInspectionHarness(t)
	h.daemon.createErr = errors.New("response lost after create dispatch")
	h.daemon.delayCreate = true
	h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
		_, err := h.client.readFileFromImage(ctx, h.image, "/etc/passwd", origin)
		require.ErrorContains(t, err, "response lost")
		return err
	})
	assert.Zero(t, h.daemon.removes)
	receipts, err := h.owner.journal.List()
	require.NoError(t, err)
	require.Len(t, receipts, 1)
	assert.Empty(t, receipts[0].ContainerID())
	h.reopen(t)
	report, err := h.owner.Recover(t.Context())
	require.NoError(t, err, "empty restart inventory cannot consume ambiguous Create")
	require.Len(t, report.pending, 1)
	receipts, err = h.owner.journal.List()
	require.NoError(t, err)
	require.Len(t, receipts, 1)
	h.daemon.containers[h.daemon.late.ID] = *h.daemon.late
	h.daemon.volumes++
	_, err = h.owner.Recover(t.Context())
	require.NoError(t, err)
	assert.Empty(t, h.daemon.containers)
	assert.Zero(t, h.daemon.volumes)
	receipts, err = h.owner.journal.List()
	require.NoError(t, err)
	require.Len(t, receipts, 1, "late-appearance receipt remains durable")
	_, err = h.owner.Recover(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, h.daemon.removes)
}

func TestImageInspectionRemovalFailureRetriesAfterRestart(t *testing.T) {
	h := newInspectionHarness(t)
	h.daemon.copy = func(_ context.Context, path string) (io.ReadCloser, error) { return inspectionTar(t, path), nil }
	h.daemon.removeErr = errors.New("remove unavailable")
	h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
		_, err := h.client.readFileFromImage(ctx, h.image, "/etc/passwd", origin)
		require.ErrorContains(t, err, "remove unavailable")
		return err
	})
	require.Len(t, h.daemon.containers, 1)
	receipts, err := h.owner.journal.List()
	require.NoError(t, err)
	require.Len(t, receipts, 1)
	require.NotEmpty(t, receipts[0].ContainerID())
	h.reopen(t)
	h.daemon.removeErr = nil
	_, err = h.owner.Recover(t.Context())
	require.NoError(t, err)
	assert.Empty(t, h.daemon.containers)
	assert.Zero(t, h.daemon.volumes)
	receipts, err = h.owner.journal.List()
	require.NoError(t, err)
	assert.Empty(t, receipts)
}

func TestImageInspectionRecoverySeparatesPendingDebtFromAuthorityFailure(t *testing.T) {
	for _, scenario := range []string{"remove unavailable", "recovery budget", "daemon replacement", "backend stop", "journal unavailable"} {
		t.Run(scenario, func(t *testing.T) {
			h := newInspectionHarness(t)
			h.daemon.copy = func(_ context.Context, path string) (io.ReadCloser, error) { return inspectionTar(t, path), nil }
			h.daemon.removeErr = errors.New("remove unavailable")
			h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
				_, err := h.client.readFileFromImage(ctx, h.image, "/etc/passwd", origin)
				return err
			})
			h.reopen(t)
			recoveryCtx := t.Context()
			switch scenario {
			case "recovery budget":
				var cancel context.CancelFunc
				recoveryCtx, cancel = context.WithCancel(t.Context())
				cancel()
			case "daemon replacement":
				h.backend.storageVerifier = testDockerRuntimeStorageVerifier{id: h.authority.storage.ID(), verify: func(context.Context) error { return backendidentity.ErrIdentityDrift }}
			case "backend stop":
				h.stop()
			case "journal unavailable":
				require.NoError(t, h.callbacks.Close())
			}
			var logs bytes.Buffer
			before := h.daemon.removes
			err := h.owner.RecoverAndReport(recoveryCtx, slog.New(slog.NewTextHandler(&logs, nil)))
			switch scenario {
			case "remove unavailable", "recovery budget":
				require.NoError(t, err, "independent helper debt cannot block workload recovery")
				assert.Contains(t, logs.String(), "Image inspection")
				require.NoError(t, h.backend.requireMutationAdmission(t.Context(), "continue workload recovery"))
				receipts, err := h.owner.journal.List()
				require.NoError(t, err)
				require.Len(t, receipts, 1, "continuation retains the exact cleanup obligation")
			case "daemon replacement":
				require.ErrorIs(t, err, backendidentity.ErrIdentityDrift)
				require.Error(t, h.backend.requireMutationAdmission(t.Context(), "continue workload recovery"))
				assert.Equal(t, before, h.daemon.removes)
			case "backend stop":
				require.ErrorIs(t, err, context.Canceled)
				assert.Equal(t, before, h.daemon.removes)
			case "journal unavailable":
				require.Error(t, err)
				assert.Equal(t, before, h.daemon.removes)
			}
		})
	}
}

func TestImageInspectionBackendAuthorityLossRetainsHelperForFreshOwner(t *testing.T) {
	for _, withdraw := range []string{"stop", "daemon replacement"} {
		t.Run(withdraw, func(t *testing.T) {
			h := newInspectionHarness(t)
			h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
				h.daemon.copy = func(context.Context, string) (io.ReadCloser, error) {
					if withdraw == "stop" {
						h.stop()
					} else {
						h.backend.storageVerifier = testDockerRuntimeStorageVerifier{id: h.authority.storage.ID(), verify: func(context.Context) error { return backendidentity.ErrIdentityDrift }}
					}
					return nil, context.Canceled
				}
				_, err := h.client.readFileFromImage(ctx, h.image, "/etc/passwd", origin)
				require.Error(t, err)
				return err
			})
			assert.Zero(t, h.daemon.removes, "work cancellation differs from backend authority withdrawal")
			require.Len(t, h.daemon.containers, 1)
			h.reopen(t)
			_, err := h.owner.Recover(t.Context())
			require.NoError(t, err)
			assert.Empty(t, h.daemon.containers)
		})
	}
}

func TestImageInspectionRecoveryRefusesForeignHelperIdentity(t *testing.T) {
	for _, field := range []string{"name", "image", "label", "container ID", "workload label", "running"} {
		t.Run(field, func(t *testing.T) {
			h := newInspectionHarness(t)
			h.daemon.copy = func(_ context.Context, path string) (io.ReadCloser, error) { return inspectionTar(t, path), nil }
			h.daemon.removeErr = errors.New("retain helper across crash")
			h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
				_, err := h.client.readFileFromImage(ctx, h.image, "/etc/passwd", origin)
				return err
			})
			h.reopen(t)
			h.daemon.removeErr = nil
			var id string
			var actual container.InspectResponse
			for id, actual = range h.daemon.containers {
				break
			}
			switch field {
			case "name":
				actual.Name = "/foreign-helper"
			case "image":
				actual.Image = otherTestImageID
			case "label":
				actual.Config.Labels["fred.inspection.id"] = "foreign"
			case "container ID":
				actual.ID = strings.Repeat("f", 64)
			case "workload label":
				actual.Config.Labels[LabelManaged] = "true"
			case "running":
				actual.State.Running = true
			}
			h.daemon.containers[id] = actual
			before := h.daemon.removes
			report, err := h.owner.Recover(t.Context())
			require.NoError(t, err, "foreign helper debt does not withdraw workload authority")
			require.Len(t, report.pending, 1)
			assert.Equal(t, before, h.daemon.removes)
			receipts, err := h.owner.journal.List()
			require.NoError(t, err)
			assert.Len(t, receipts, 1)
		})
	}
}

func TestImageInspectionLostCreatePersistenceRecoversFromWriteAheadIdentity(t *testing.T) {
	h := newInspectionHarness(t)
	h.daemon.afterCreate = func() {
		h.stop()
		require.NoError(t, h.callbacks.Close(), "simulate process exit after daemon Create and before response persistence")
	}
	h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
		_, err := h.client.readFileFromImage(ctx, h.image, "/etc/passwd", origin)
		require.Error(t, err)
		return err
	})
	require.Len(t, h.daemon.containers, 1)
	assert.Zero(t, h.daemon.removes)
	h.reopen(t)
	receipts, err := h.owner.journal.List()
	require.NoError(t, err)
	require.Len(t, receipts, 1)
	assert.Empty(t, receipts[0].ContainerID(), "returned ID was not required for durable ownership")
	_, err = h.owner.Recover(t.Context())
	require.NoError(t, err)
	assert.Empty(t, h.daemon.containers)
	assert.Zero(t, h.daemon.volumes)
	receipts, err = h.owner.journal.List()
	require.NoError(t, err)
	require.Len(t, receipts, 1)
}

func TestImageInspectionRecoveryCannotRemoveLiveSession(t *testing.T) {
	h := newInspectionHarness(t)
	copying, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	h.daemon.copy = func(ctx context.Context, path string) (io.ReadCloser, error) {
		close(copying)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return inspectionTar(t, path), nil
		}
	}
	go func() {
		defer close(finished)
		h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
			uid, gid, err := h.client.ResolveImageUser(ctx, h.image, "app", origin)
			assert.NoError(t, err)
			assert.Equal(t, 1000, uid)
			assert.Equal(t, 1000, gid)
			return err
		})
	}()
	select {
	case <-copying:
	case <-t.Context().Done():
		t.Fatal("inspection copy did not begin")
	}
	_, err := h.owner.Recover(t.Context())
	require.NoError(t, err)
	h.daemon.mu.Lock()
	removes := h.daemon.removes
	h.daemon.mu.Unlock()
	assert.Zero(t, removes, "recovery cannot consume the live session's receipt")
	close(release)
	select {
	case <-finished:
	case <-t.Context().Done():
		t.Fatal("inspection did not finish")
	}
	assert.Equal(t, 1, h.daemon.removes)
	receipts, err := h.owner.journal.List()
	require.NoError(t, err)
	assert.Empty(t, receipts)
}

func TestImageInspectionRejectsForeignImageBeforeJournalAllocation(t *testing.T) {
	h := newInspectionHarness(t)
	foreign := newInspectionHarness(t)
	h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
		_, err := h.client.readFileFromImage(ctx, foreign.image, "/etc/passwd", origin)
		require.ErrorIs(t, err, imageexec.ErrForeignImage)
		return err
	})
	assert.Zero(t, h.daemon.creates)
	receipts, err := h.owner.journal.List()
	require.NoError(t, err)
	assert.Empty(t, receipts)
}
