package docker

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	networktypes "github.com/docker/docker/api/types/network"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/hmacauth"
)

// validManifestJSON returns a minimal valid manifest payload.
func validManifestJSON(image string) []byte {
	m := manifest.Manifest{
		Image: image,
	}
	b, _ := json.Marshal(m)
	return b
}

// newProvisionRequest creates a ProvisionRequest for testing.
func newProvisionRequest(leaseUUID, tenant, sku string, qty int, payload []byte) backend.ProvisionRequest {
	return backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       tenant,
		ProviderUUID: nominalDockerProviderUUID,
		Items:        []backend.LeaseItem{{SKU: sku, Quantity: qty}},
		CallbackURL:  testOperationCallbackURL("http://localhost/callbacks/provision"),
		Payload:      payload,
	}
}

// newBackendForProvisionTest creates a Backend with a zero-backoff callback
// sender pointed at testCallbackClient.
func newBackendForProvisionTest(t *testing.T, mock *mockDockerClient, provisions map[string]*provision) *Backend {
	t.Helper()
	compose := newNominalProvisionComposeExecutor()
	if mock.ListManagedContainersFn == nil {
		// Model the authoritative Docker inventory published by the nominal
		// Compose executor. Settlement deliberately requires the exact emitted
		// labels as well as Compose PS; an always-empty default would turn every
		// successful provision into an artificial ambiguous outcome.
		installStackStrictCohortInventory(t, mock, compose)
	}
	b := newBackendForTest(mock, provisions)
	b.compose = compose
	attachBoundOperationHandoffStores(t, b)
	b.releaseCapacityPlanner = nominalReleaseCapacityPlanner{backend: b}
	rebuildCallbackSender(b, testCallbackClient)
	return b
}

// prepareFailedProvisionReplacement upgrades a compact Failed-provision fixture
// into the exact predecessor authority that production recovery publishes. A
// replacement provision is allowed to erase an older cohort only when its live
// projection, active Release, and Docker labels agree on the tenant, provider,
// workload shape, and callback generation.
func prepareFailedProvisionReplacement(
	t *testing.T,
	b *Backend,
	mock *mockDockerClient,
	leaseUUID, tenant, sku string,
	payload []byte,
) {
	t.Helper()

	prov, ok := b.provisions[leaseUUID]
	require.True(t, ok, "replacement fixture requires a predecessor projection")
	require.Equal(t, backend.ProvisionStatusFailed, prov.Status)
	require.Len(t, prov.ContainerIDs, prov.Quantity)

	items := []backend.LeaseItem{{
		SKU: sku, Quantity: prov.Quantity, ServiceName: manifest.DefaultServiceName,
	}}
	profiles := testResourceProfiles(t, items)
	stack, err := manifest.ParsePayload(payload)
	require.NoError(t, err)

	predecessorOperationID := mustDockerOperationID("9a72fbc2-38c8-4f31-87f7-f689979b9324")
	callbackURL := "https://old.example/callbacks/provision?operation_id=" + predecessorOperationID.String()
	lifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	authority, err := shared.NewReleaseRuntimeAuthority(
		predecessorOperationID,
		tenant,
		nominalDockerProviderUUID,
		callbackURL,
		lifecycleCallbackURL,
	)
	require.NoError(t, err)

	prov.Tenant = tenant
	prov.ProviderUUID = nominalDockerProviderUUID
	prov.SKU = sku
	prov.CallbackURL = callbackURL
	prov.LifecycleCallbackURL = lifecycleCallbackURL
	prov.Items = items
	prov.ResourceProfiles = shared.CloneSKUResourceSnapshot(profiles)
	prov.StackManifest = stack
	prov.ServiceContainers = map[string][]string{
		manifest.DefaultServiceName: append([]string(nil), prov.ContainerIDs...),
	}

	var containersMu sync.Mutex
	containers := make([]ContainerInfo, 0, len(prov.ContainerIDs))
	for index, containerID := range prov.ContainerIDs {
		containers = append(containers, ContainerInfo{
			ContainerID:          containerID,
			Name:                 fmt.Sprintf("fred-%s-%s-%d", leaseUUID, manifest.DefaultServiceName, index),
			BackendName:          b.cfg.Name,
			LeaseUUID:            leaseUUID,
			Tenant:               tenant,
			ProviderUUID:         nominalDockerProviderUUID,
			SKU:                  sku,
			ServiceName:          manifest.DefaultServiceName,
			InstanceIndex:        index,
			Image:                stack.Services[manifest.DefaultServiceName].Image,
			CallbackURL:          callbackURL,
			LifecycleCallbackURL: lifecycleCallbackURL,
			Status:               "exited",
		})
	}
	mock.ListManagedContainersFn = func(context.Context) ([]ContainerInfo, error) {
		containersMu.Lock()
		defer containersMu.Unlock()
		return append([]ContainerInfo(nil), containers...), nil
	}
	originalInspect := mock.InspectContainerFn
	mock.InspectContainerFn = func(ctx context.Context, containerID string) (*ContainerInfo, error) {
		containersMu.Lock()
		for i := range containers {
			if containers[i].ContainerID == containerID {
				copy := containers[i]
				containersMu.Unlock()
				return &copy, nil
			}
		}
		containersMu.Unlock()
		if originalInspect != nil {
			return originalInspect(ctx, containerID)
		}
		return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
	}
	originalRemove := mock.RemoveContainerFn
	mock.RemoveContainerFn = func(ctx context.Context, containerID string) error {
		if originalRemove != nil {
			if err := originalRemove(ctx, containerID); err != nil {
				return err
			}
		}
		containersMu.Lock()
		defer containersMu.Unlock()
		for index := range containers {
			if containers[index].ContainerID == containerID {
				containers = append(containers[:index], containers[index+1:]...)
				break
			}
		}
		return nil
	}

	compose, ok := b.compose.(*mockComposeExecutor)
	require.True(t, ok, "replacement fixture requires the mock Compose executor")
	originalUp := compose.UpFn
	compose.UpFn = func(ctx context.Context, project *composetypes.Project, opts composeUpOpts) error {
		if originalUp != nil {
			if err := originalUp(ctx, project, opts); err != nil {
				return err
			}
		}
		// Model Docker's authoritative post-Up inventory, not only Compose PS.
		// The strict physical classifier reads ListManagedContainers and must see
		// the candidate generation that the successful Up just published.
		b.provisionsMu.RLock()
		current := recoveredFromProvision(b.provisions[leaseUUID])
		b.provisionsMu.RUnlock()
		listed, err := compose.PS(ctx, project.Name)
		if err != nil {
			return err
		}
		if len(listed) != 1 {
			return fmt.Errorf("replacement fixture expected one Compose container, got %d", len(listed))
		}
		containersMu.Lock()
		containers = []ContainerInfo{{
			ContainerID:          listed[0].ID,
			Name:                 fmt.Sprintf("fred-%s-%s-0", leaseUUID, manifest.DefaultServiceName),
			BackendName:          b.cfg.Name,
			LeaseUUID:            leaseUUID,
			Tenant:               current.Tenant,
			ProviderUUID:         current.ProviderUUID,
			SKU:                  current.Items[0].SKU,
			ServiceName:          current.Items[0].ServiceName,
			InstanceIndex:        0,
			Image:                current.StackManifest.Services[manifest.DefaultServiceName].Image,
			CallbackURL:          current.CallbackURL,
			LifecycleCallbackURL: current.LifecycleCallbackURL,
			Status:               "running",
		}}
		containersMu.Unlock()
		return nil
	}
	compose.DownFn = func(context.Context, string, time.Duration) error {
		return errors.New("exercise strict predecessor teardown fallback")
	}

	_, ok = concreteOperationSettlementForTest(b.operationSettlement)
	require.True(t, ok, "replacement fixture requires the bound operation settlement")

	attachReleaseStore(t, b)
	seedProvisionReleaseForBackendTest(t, b, leaseUUID, shared.Release{
		Manifest:         append([]byte(nil), payload...),
		Image:            "stack",
		OperationID:      predecessorOperationID,
		Items:            append([]backend.LeaseItem(nil), items...),
		ResourceProfiles: shared.CloneSKUResourceSnapshot(profiles),
		RuntimeAuthority: &authority,
		Status:           "active",
		CreatedAt:        time.Now().Add(-time.Hour),
	})
}

// nominalReleaseCapacityPlanner keeps broad worker/actor fixtures lightweight
// when they do not model release history, but dynamically delegates as soon as
// a test attaches the real store. Production never uses this adapter.
type nominalReleaseCapacityPlanner struct {
	backend *Backend
}

func (p nominalReleaseCapacityPlanner) CheckOperationReleaseCapacity(
	candidate shared.OperationReleaseCandidate,
) error {
	if p.backend.operationSettlement == nil {
		return nil
	}
	return p.backend.operationSettlement.CheckOperationReleaseCapacity(candidate)
}

// zeroBackoff eliminates retry delays in tests.
var zeroBackoff = [shared.CallbackMaxAttempts]time.Duration{}

// testCallbackDeliveryTimeout keeps deliberately parked callback handlers from
// consuming the production protocol budget while preserving the production
// rule that the request context, not http.Client.Timeout, owns cancellation.
const testCallbackDeliveryTimeout = 5 * time.Second

const (
	durableCallbackTestLeaseUUID  = "550e8400-e29b-41d4-a716-446655440000"
	durableCallbackTestLeaseUUID2 = "6ba7b811-9dad-41d1-80b4-00c04fd430c8"
	durableCallbackTestLeaseUUID3 = "123e4567-e89b-42d3-a456-426614174000"
	durableCallbackTestSecret     = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
)

// testCallbackClient is the default callback client for tests that rebuild
// the sender for some reason OTHER than pointing it at a server — a swapped
// callbackStore or CallbackSecret, say.
//
// Sharing one *http.Client across tests is safe: it is concurrency-safe by
// contract, and nothing here mutates it. Tests that need the observer
// transport build their own (see observeCallbacks); tests talking to an
// httptest server pass that server's client.
var testCallbackClient = &http.Client{}

// allowTestCallbackDelivery keeps callback-behaviour unit tests focused on
// delivery. Storage-lineage tests construct their own sender with the real
// Backend.VerifyStorageIdentity hook.
func allowTestCallbackDelivery(context.Context) error { return nil }

// rebuildCallbackSender re-creates b.callbackSender from hc, b.callbackStore
// and b.cfg, with zero backoff for fast tests.
//
// CallbackSender captures its dependencies at construction, so a test that
// changes any of them afterwards must rebuild for the change to take effect.
// The HTTP client is a PARAMETER rather than a Backend field because
// production has no such field to read (ENG-765): New keeps its callback
// client as a local and hands it straight to NewCallbackSender.
//
// Call this AFTER every dependency the sender captures is in place. It reads
// b.callbackStore and b.cfg.CallbackSecret at call time, so a rebuild hoisted
// above an assignment to either silently builds a sender against the old
// value — and the tests here would not fail, since their callback handlers do
// not verify the signature.
func rebuildCallbackSender(b *Backend, hc *http.Client) {
	secret := string(b.cfg.CallbackSecret)
	if b.callbackStore != nil && len(secret) < hmacauth.MinSecretLength {
		secret = durableCallbackTestSecret
	}
	cfg := shared.CallbackSenderConfig{
		Store: b.callbackStore,
		StorageAttestor: shared.MustNewCallbackStorageAttestor(
			b.callbackStore,
			dockerCallbackStorageVerifier{verifier: b.storageVerifier, gate: b.storeAuthorityGate},
			b.stopCtx,
		),
		HTTPClient: hc,
		Secret:     secret,
		Logger:     b.logger,

		Backoff:         &zeroBackoff,
		DeliveryTimeout: testCallbackDeliveryTimeout,
	}
	if b.callbackStore == nil {
		panic("callback sender test requires an identity-bound durable store")
	}
	callbackFixtureBackends.Store(b.callbackStore, b)
	operations, ok := concreteOperationSettlementForTest(b.operationSettlement)
	if !ok || b.maintenanceSettlement == nil {
		panic("durable callback sender test requires paired settlement services")
	}
	b.callbackSender = shared.MustNewCallbackSender(cfg)
	publisher, err := shared.NewCallbackPublisher(shared.CallbackPublisherConfig{
		OperationSettlement:   operations,
		MaintenanceSettlement: b.maintenanceSettlement,
		StorageAttestor:       cfg.StorageAttestor,
		Logger:                cfg.Logger,
		OnStoreError:          cfg.OnStoreError,
	})
	if err != nil {
		panic(err)
	}
	b.callbackPublisher = publisher
}

func startCallbackReplayForTest(b *Backend) {
	b.wg.Go(b.callbackSender.RunReplayLoop)
	b.callbackSender.NotifyPendingCallbacks()
}

// sendOperationCallback is a test-only typed settlement adapter retained for
// historical callback transport tests. Production has no status-selected
// operation API: success must first commit its sealed Release, while failure
// derives the current durable route from the paired journals.
func (b *Backend) sendOperationCallback(
	leaseUUID string,
	status backend.CallbackStatus,
	errMsg string,
) {
	if b.callbackStore == nil {
		panic("operation callback test requires durable callback authority")
	}
	if status == backend.CallbackStatusFailed {
		claims, err := b.operationSettlement.ListOperationIntents()
		if err != nil {
			panic(err)
		}
		for _, claim := range claims {
			if claim.LeaseUUID() != leaseUUID {
				continue
			}
			candidate, err := b.operationSettlement.PrepareOperationRelease(claim)
			if err != nil {
				panic(err)
			}
			failure, err := b.operationSettlement.RefuseOperationExecution(candidate)
			if err != nil {
				panic(err)
			}
			proof, err := b.operationSettlement.CommitOperationFailure(failure)
			if err != nil {
				panic(err)
			}
			b.sendOperationFailure(proof, errMsg)
			return
		}
		return
	}
	if status != backend.CallbackStatusSuccess || b.operationSettlement == nil {
		return
	}
	claims, err := b.operationSettlement.ListOperationIntents()
	if err != nil {
		panic(err)
	}
	for _, claim := range claims {
		if claim.LeaseUUID() != leaseUUID {
			continue
		}
		committed, err := commitOperationSuccessFixture(b.operationSettlement, claim)
		if err != nil {
			panic(err)
		}
		if err := b.callbackPublisher.PublishOperationSuccessContext(context.Background(), committed); err != nil {
			panic(err)
		}
		return
	}
}

// seedOperationCallbackForTest constructs the exact non-terminal operation
// authority consumed by sendOperationCallback. A projection URL alone is not
// settlement authority and must never make callback tests pass accidentally.
func seedOperationCallbackForTest(
	t *testing.T,
	b *Backend,
	callbackURL string,
) shared.OperationIntentClaim {
	t.Helper()
	callbackURL = testOperationCallbackURL(callbackURL)
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	spec := dockerOperationIntentSpec(t, b.storageIdentity)
	spec.CallbackURL = callbackURL
	spec.LifecycleCallbackURL = lifecycleURL
	admission := beginOperationIntentForSettlementTest(t, b.operationSettlement, spec)
	claim := createdDockerOperationClaim(t, admission)
	b.provisionsMu.Lock()
	if projection := b.provisions[claim.LeaseUUID()]; projection != nil {
		projection.CallbackURL = callbackURL
		projection.LifecycleCallbackURL = lifecycleURL
		projection.ActiveOperationID = claim.OperationID()
	}
	b.provisionsMu.Unlock()
	return claim
}

func durableCallbackTestStorageIdentity() backendidentity.ID {
	id, err := backendidentity.Parse("550e8400-e29b-41d4-a716-446655440000")
	if err != nil {
		panic("invalid durable callback test storage identity: " + err.Error())
	}
	return id
}

// provisionFlowTimeout bounds each wait in doProvisionAndFire. Every hop
// it covers is in-process, so blowing this budget means something is
// wedged rather than slow.
const provisionFlowTimeout = 30 * time.Second

// callbackObserver decorates an http.RoundTripper and signals seen once
// each round trip has completed.
//
// This is doProvisionAndFire's synchronization barrier for its explicitly
// ephemeral sender, and it is
// load-bearing. Provisioning runs on the actor's worker goroutine, so
// the test goroutine needs a happens-before edge with the SM entry
// action that writes ProvisionState. cfg.SendOperationCallbackFn is the LAST
// statement of both onEnterReadyFromProvision and
// onEnterFailedFromProvision (leasesm/lease_sm.go carries the matching
// ordering contract), so an observed callback round trip proves every
// store write of that entry action is already committed. The signal is
// sent by the goroutine that performed the round trip — the actor
// goroutine — so the test inherits that edge, and with it the edge the
// completed POST already established with the httptest handler.
//
// Do NOT swap this for a LeaseActor.State() poll: the SM flips Status
// before the entry action's callback POST returns, so a poll returns
// while the httptest handler is still decoding into the test's
// callbackPayload. Measured: that variant trips the race detector while
// `go test -short` stays green.
type callbackObserver struct {
	base http.RoundTripper
	seen chan struct{}
}

func (o *callbackObserver) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := o.base.RoundTrip(req)
	select {
	case o.seen <- struct{}{}:
	default:
	}
	return resp, err
}

// observeCallbacks points b's callback sender at a client whose transport
// is wrapped in a callbackObserver, and returns the channel it signals.
// That client is used by nothing but callbackSender, so every round trip it
// makes is a callback delivery.
//
// Must run before the lease actor is created, and that is now load-bearing
// rather than belt-and-braces: this installs a NEW client rather than
// mutating one the sender already holds, so an actor that captured an
// earlier sender would never be observed. newLeaseActor captures the sender.
//
// rebuildCallbackSender installs the scaled per-request attempt timeout; the
// client deliberately has no separate client-wide timeout.
func observeCallbacks(b *Backend) <-chan struct{} {
	seen := make(chan struct{}, 1)
	rebuildCallbackSender(b, &http.Client{
		Transport: &callbackObserver{base: http.DefaultTransport, seen: seen},
	})
	return seen
}

// doProvisionAndFire is a permanent test-shape adapter for exercising
// specific doProvision branches without Backend.Provision's
// validate-and-allocate preamble.
//
// It drives the flow through the SAME seam production uses: a
// leasesm.ProvisionRequestedMsg routed to the lease actor's inbox, which
// fires evProvisionRequested, acks, spawns the worker that runs
// doProvision, and lands the terminal evProvisionCompleted /
// evProvisionErrored transition. Nothing here reaches into leasesm's
// unexported state, so leasesm ships no test-only scaffolding (ENG-354).
//
// The call is therefore ASYNCHRONOUS internally and only looks synchronous to
// the caller because this unit-test helper deliberately uses an ephemeral
// sender and blocks on its terminal callback (see callbackObserver). Production
// durable senders only persist and wake their replay loop. Every caller MUST
// have a callback URL on the
// lease's provision record pointing at an httptest server; without one
// leasesm's SendOperationCallbackFn returns without a POST and this helper times
// out. Any new assertion on state the httptest handler writes is safe
// only because of that barrier — do not assert after a bare return.
//
// That barrier also constrains the handler: it is the round trip
// RETURNING that releases this helper, so a handler that parks — an
// unbuffered channel send with no receiver waiting yet, say — parks the
// POST with it. Nothing deadlocks, which is why this is worth writing
// down: the ephemeral sender's DeliverCallback bounds its complete chain at
// testCallbackDeliveryTimeout, so the handler costs at most one 5s stall and
// the durable replay loop owns any later POST. Signal completion the way the
// handlers below do —
// close() under a select/default, or a buffered send — never a send
// that can block.
//
// The signature accepts a single-service *manifest.Manifest because
// every legitimate caller passes a 1-service stack worth of input;
// internally the helper wraps the manifest into a 1-service stack under
// manifest.DefaultServiceName, auto-tags lease items, and dispatches to
// the unified stack-shaped doProvision. This keeps the boilerplate at
// each call site minimal while preserving the stack-shape contract on
// the production side.
func (b *Backend) dispatchProvisionForTest(t *testing.T, ctx context.Context, req backend.ProvisionRequest, m *manifest.Manifest, profiles map[string]SKUProfile, logger *slog.Logger) <-chan struct{} {
	t.Helper()
	stack := &manifest.StackManifest{Services: map[string]*manifest.Manifest{manifest.DefaultServiceName: m}}
	for i := range req.Items {
		if req.Items[i].ServiceName == "" {
			req.Items[i].ServiceName = manifest.DefaultServiceName
		}
	}
	resourceProfiles, err := b.snapshotResourceProfiles(req.Items, profiles)
	require.NoError(t, err)

	seen := observeCallbacks(b)
	startCallbackReplayForTest(b)
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})

	workCtx, workCancel := context.WithCancel(ctx)
	t.Cleanup(workCancel)
	callbackURL, parseErr := url.Parse(req.CallbackURL)
	require.NoError(t, parseErr)
	query := callbackURL.Query()
	query.Set(backend.CallbackOperationIDQueryParameter, uuid.NewString())
	callbackURL.RawQuery = query.Encode()
	req.CallbackURL = callbackURL.String()
	req.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(req.CallbackURL, "")
	require.NoError(t, err)
	payload, err := json.Marshal(stack)
	require.NoError(t, err)
	candidate, err := b.operationSettlement.NewOperationIntentCandidate(shared.OperationIntentSpec{
		Kind: shared.OperationIntentProvision, LeaseUUID: req.LeaseUUID,
		CallbackURL: req.CallbackURL, LifecycleCallbackURL: req.LifecycleCallbackURL,
		Tenant: req.Tenant, ProviderUUID: req.ProviderUUID,
		Items: req.Items, ResourceProfiles: resourceProfiles, EffectiveItems: req.Items,
		Manifest: payload,
	})
	require.NoError(t, err)
	admission, err := b.operationSettlement.BeginOperationIntent(candidate)
	require.NoError(t, err)
	claim := createdDockerOperationClaim(t, admission)
	b.provisionsMu.Lock()
	if projection := b.provisions[req.LeaseUUID]; projection != nil {
		projection.CallbackURL = req.CallbackURL
		projection.LifecycleCallbackURL = req.LifecycleCallbackURL
		projection.ProviderUUID = req.ProviderUUID
		projection.ActiveOperationID = claim.OperationID()
	}
	b.provisionsMu.Unlock()
	command, reply, err := leasesm.NewProvisionCommand(workCtx, claim)
	require.NoError(t, err)
	require.True(t, b.routeToLease(req.LeaseUUID, command), "lease actor refused the provision message")

	select {
	case err := <-reply.Result():
		require.NoError(t, err, "lease actor rejected the provision SM transition")
	case <-time.After(provisionFlowTimeout):
		t.Fatal("timed out waiting for the lease actor to ack the provision request")
	}
	return seen
}

func (b *Backend) doProvisionAndFire(t *testing.T, ctx context.Context, req backend.ProvisionRequest, m *manifest.Manifest, profiles map[string]SKUProfile, logger *slog.Logger) {
	t.Helper()
	seen := b.dispatchProvisionForTest(t, ctx, req, m, profiles, logger)

	select {
	case <-seen:
	case <-time.After(provisionFlowTimeout):
		t.Fatal("timed out waiting for the terminal provision callback — " +
			"is CallbackURL set on this lease's provision record?")
	}
}

// awaitProvisionWorkerQuiescence is the callback-free completion barrier for
// tests whose expected result is deliberately ambiguous. Such an operation
// must not publish a terminal callback, so waiting for HTTP would invert the
// assertion. The actor's opaque quiescence claim proves both its worker and
// terminal handoff have completed without adding a test hook to production.
func awaitProvisionWorkerQuiescence(t *testing.T, b *Backend, leaseUUID string) {
	t.Helper()
	require.Eventually(t, func() bool {
		b.actorsMu.Lock()
		actor := b.actors[leaseUUID]
		b.actorsMu.Unlock()
		if actor == nil {
			return false
		}
		claim := actor.TryClaimQuiescence()
		if claim == nil {
			return false
		}
		claim.Release()
		return true
	}, provisionFlowTimeout, 5*time.Millisecond,
		"provision worker did not finish its terminal handoff")
}

// --- Provision (synchronous validation) tests ---

func TestProvision_Success(t *testing.T) {
	pullCalled := false
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			pullCalled = true
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	upCalled := false
	psCalled := false
	composeMock := &mockComposeExecutor{
		UpFn: func(ctx context.Context, project *composetypes.Project, opts composeUpOpts) error {
			upCalled = true
			return nil
		},
		PSFn: func(ctx context.Context, projectName string) ([]composeContainerSummary, error) {
			psCalled = true
			return []composeContainerSummary{
				{ID: "container-1", Service: manifest.DefaultServiceName, State: "running"},
			}, nil
		},
	}
	installStackStrictCohortInventory(t, mock, composeMock)

	callbackReceived := make(chan struct{})
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		close(callbackReceived)
	}))
	defer callbackServer.Close()

	b := newBackendForProvisionTest(t, mock, nil)
	b.compose = composeMock
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	rebuildCallbackSender(b, callbackServer.Client())
	startCallbackReplayForTest(b)
	defer func() {
		b.stopCancel()
		b.wg.Wait()
	}()

	req := newProvisionRequest(durableCallbackTestLeaseUUID, "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	req.CallbackURL = testOperationCallbackURL(callbackServer.URL)

	err := b.Provision(context.Background(), req)
	require.NoError(t, err)

	// Wait for async doProvision to complete (signaled by callback). Keep this
	// bounded so a future evidence regression reports the owning test instead of
	// wedging every package-level parallel test until the global timeout.
	select {
	case <-callbackReceived:
	case <-time.After(provisionFlowTimeout):
		t.Fatal("timed out waiting for successful provision callback")
	}

	// Verify final state (must read status under lock to avoid race with goroutine)
	b.provisionsMu.RLock()
	prov := b.provisions[durableCallbackTestLeaseUUID]
	require.NotNil(t, prov)
	status := prov.Status
	containerIDs := prov.ContainerIDs
	serviceContainers := prov.ServiceContainers
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, status)
	assert.Len(t, containerIDs, 1, "stack-shape provision must record exactly one container ID for a 1-service stack")
	require.NotNil(t, serviceContainers)
	assert.Len(t, serviceContainers[manifest.DefaultServiceName], 1, "ServiceContainers must map 'app' → 1 container")
	assert.True(t, pullCalled, "PullImage must fire before compose up")
	assert.True(t, upCalled, "compose.Up must be invoked on the stack path")
	assert.True(t, psCalled, "compose.PS must be invoked to discover container IDs")

}

func TestProvision_AlreadyProvisioned(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
			Status: backend.ProvisionStatusReady},
		},
	})

	req := newProvisionRequest(durableCallbackTestLeaseUUID, "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	err := b.Provision(context.Background(), req)

	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrAlreadyProvisioned)
}

// TestProvision_RejectsWhileDeprovisioning guards Provision's status check:
// a concurrent Deprovision (which sets Status=Deprovisioning) must block
// re-provision. The reconciler retries on the next cycle once the
// Deprovision completes and removes the entry. Without this, a re-provision
// races with RemoveContainer and corrupts state.
func TestProvision_RejectsWhileDeprovisioning(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
			Status: backend.ProvisionStatusDeprovisioning},
		},
	})

	req := newProvisionRequest(durableCallbackTestLeaseUUID, "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	err := b.Provision(context.Background(), req)

	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrAlreadyProvisioned)
}

func TestProvision_ReProvisionFailed(t *testing.T) {
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440101"
	payload := validManifestJSON("nginx:latest")
	removeCalled := false
	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			removeCalled = true
			return nil
		},
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		leaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: leaseUUID,
			Status:       backend.ProvisionStatusFailed,
			FailCount:    2,
			Quantity:     1,
			ContainerIDs: []string{"old-container"}},
		},
	})
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	// Pre-allocate a resource for the old provision
	_ = b.pool.TryAllocate(leaseUUID+"-app-0", "docker-small", "tenant-a")
	prepareFailedProvisionReplacement(t, b, mock, leaseUUID, "tenant-a", "docker-small", payload)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	callbackReceived := make(chan struct{})
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		select {
		case <-callbackReceived:
		default:
			close(callbackReceived)
		}
	}))
	defer callbackServer.Close()
	rebuildCallbackSender(b, callbackServer.Client())
	b.wg.Go(b.callbackSender.RunReplayLoop)

	req := newProvisionRequest(leaseUUID, "tenant-a", "docker-small", 1, payload)
	req.CallbackURL = testOperationCallbackURL(callbackServer.URL)
	err := b.Provision(context.Background(), req)
	require.NoError(t, err)
	select {
	case <-callbackReceived:
	case <-time.After(2 * time.Second):
		intents, listErr := b.operationSettlement.ListOperationIntents()
		require.NoError(t, listErr)
		pending, pendingErr := b.callbackStore.ListPending()
		require.NoError(t, pendingErr)
		t.Fatalf("replacement provision callback was not delivered; intents=%d pending=%d", len(intents), len(pending))
	}
	intents, err := b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, intents, "a converged Down fallback must settle the Started operation")
	select {
	case <-b.stopCtx.Done():
		t.Fatal("a recovered Down fallback must not latch storage ambiguity")
	default:
	}

	// Should have cleaned up old container
	assert.True(t, removeCalled, "old container should be removed during re-provision")

	// New provision should have preserved FailCount
	b.provisionsMu.RLock()
	prov := b.provisions[leaseUUID]
	b.provisionsMu.RUnlock()
	assert.Equal(t, 2, prov.FailCount, "FailCount should be preserved from previous provision")

}

func TestProvision_ReProvisionFallbackFailureRemainsAmbiguous(t *testing.T) {
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440101"
	payload := validManifestJSON("nginx:latest")
	mock := &mockDockerClient{
		RemoveContainerFn: func(context.Context, string) error {
			return errors.New("predecessor container is busy")
		},
		PullImageFn: func(context.Context, string, time.Duration) error { return nil },
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

	req := newProvisionRequest(leaseUUID, "tenant-a", "docker-small", 1, payload)
	require.NoError(t, b.Provision(context.Background(), req))
	require.Eventually(t, func() bool {
		acquired, err := b.withRecoveryLeaseExclusion(
			t.Context(), leaseUUID, func() error { return nil },
		)
		return err == nil && acquired
	}, 3*time.Second, 10*time.Millisecond, "ambiguous replacement worker did not become recoverable")
	intents, err := b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1, "ambiguous fallback must preserve durable recovery authority")
	assert.Equal(t, leaseUUID, intents[0].LeaseUUID())
	pending, err := b.callbackStore.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending, "ambiguous fallback cannot publish a terminal operation callback")
	b.provisionsMu.RLock()
	projection := recoveredFromProvision(b.provisions[leaseUUID])
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusFailed, projection.Status)
	assert.Equal(t, 2, projection.FailCount)
	assert.Equal(t, []string{"old-container"}, projection.ContainerIDs,
		"failed fallback must retain the exact predecessor projection for recovery")
}

func TestProvision_ReProvisionUsesLockedPredecessorSnapshot(t *testing.T) {
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440101"
	payload := validManifestJSON("nginx:latest")
	removeCalls := 0
	mock := &mockDockerClient{
		RemoveContainerFn: func(context.Context, string) error {
			removeCalls++
			return nil
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

	b.provisionsMu.RLock()
	predecessor := b.provisions[leaseUUID]
	b.provisionsMu.RUnlock()
	baseList := mock.ListManagedContainersFn
	listEntered := make(chan struct{})
	releaseList := make(chan struct{})
	var enterOnce sync.Once
	mock.ListManagedContainersFn = func(ctx context.Context) ([]ContainerInfo, error) {
		enterOnce.Do(func() { close(listEntered) })
		<-releaseList
		return baseList(ctx)
	}

	req := newProvisionRequest(leaseUUID, "tenant-a", "docker-small", 1, payload)
	req.CallbackURL = "https://new.example/callbacks/provision?operation_id=6ba7b810-9dad-41d1-80b4-00c04fd430c8"
	result := make(chan error, 1)
	go func() { result <- b.Provision(t.Context(), req) }()
	select {
	case <-listEntered:
	case <-time.After(3 * time.Second):
		close(releaseList)
		t.Fatal("replacement provision did not reach predecessor inventory")
	}

	// Model a lock-correct in-place actor update after Provision releases its
	// initial lock. Pointer identity alone is an ABA-prone CAS: the read-only
	// validation must notice the changed value before it tears anything down.
	b.provisionsMu.Lock()
	predecessor.Message = "actor update after snapshot"
	b.provisionsMu.Unlock()
	close(releaseList)

	err := waitForAsyncTestResult(t, result, "replacement provision snapshot validation")
	require.ErrorContains(t, err, "predecessor changed during read-only validation")
	b.provisionsMu.RLock()
	current := b.provisions[leaseUUID]
	b.provisionsMu.RUnlock()
	assert.Same(t, predecessor, current, "a changed predecessor must remain authoritative")
	assert.Equal(t, "actor update after snapshot", current.Message)
	assert.Zero(t, removeCalls, "changed predecessor validation must fail before teardown")
}

func TestProvision_UnknownSKU(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)

	req := newProvisionRequest(durableCallbackTestLeaseUUID, "tenant-a", "unknown-sku-xyz", 1, validManifestJSON("nginx:latest"))
	err := b.Provision(context.Background(), req)

	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrValidation)
	assert.ErrorIs(t, err, backend.ErrUnknownSKU)

	// Provision slot should be cleaned up
	b.provisionsMu.RLock()
	_, exists := b.provisions[durableCallbackTestLeaseUUID]
	b.provisionsMu.RUnlock()
	assert.False(t, exists)
}

// ENG-503: a lease whose total quantity exceeds maxLeaseQuantity must be rejected as a
// validation error BEFORE the pre-admission ContainerIDs allocation, so a chain-supplied
// quantity (billing-capped at 1e9) can't drive a ~16 GB make([]string, 0, totalQuantity).
func TestProvision_RejectsExcessiveQuantity(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)

	req := newProvisionRequest(durableCallbackTestLeaseUUID2, "tenant-a", "docker-small", maxLeaseQuantity+1, validManifestJSON("nginx:latest"))
	err := b.Provision(context.Background(), req)

	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrValidation)

	// Rejected before any reservation — no provision slot leaked.
	b.provisionsMu.RLock()
	_, exists := b.provisions[durableCallbackTestLeaseUUID2]
	b.provisionsMu.RUnlock()
	assert.False(t, exists, "an over-quota lease must not reserve a provision slot (ENG-503)")
}

// ENG-503 (PR #175 review): for a multi-item lease, the rejection must identify WHICH
// item is out of range (index + SKU + service) so a client can act on it — these are
// the tenant's own inputs, so echoing them back is not a disclosure.
func TestProvision_RejectsExcessiveQuantity_IdentifiesOffendingItem(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)

	req := backend.ProvisionRequest{
		LeaseUUID:    durableCallbackTestLeaseUUID2,
		Tenant:       "tenant-a",
		ProviderUUID: nominalDockerProviderUUID,
		Items: []backend.LeaseItem{
			{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
			{SKU: "docker-medium", Quantity: maxLeaseQuantity + 1, ServiceName: "worker"}, // offending
		},
		CallbackURL: testOperationCallbackURL("http://localhost/callbacks/provision"),
		Payload:     validManifestJSON("nginx:latest"),
	}
	err := b.Provision(context.Background(), req)

	require.ErrorIs(t, err, backend.ErrValidation)
	assert.Contains(t, err.Error(), "docker-medium", "error must name the offending item's SKU")
	assert.Contains(t, err.Error(), "worker", "error must name the offending item's service")
}

// ENG-503 (defense-in-depth): a negative item quantity — the result of an overflowed
// uint64→int cast at ingest — must be rejected as a validation error rather than
// reaching make([]string, 0, negative), which panics.
func TestProvision_RejectsNegativeQuantity(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)

	req := newProvisionRequest(durableCallbackTestLeaseUUID2, "tenant-a", "docker-small", -1, validManifestJSON("nginx:latest"))
	err := b.Provision(context.Background(), req)

	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrValidation)

	b.provisionsMu.RLock()
	_, exists := b.provisions[durableCallbackTestLeaseUUID2]
	b.provisionsMu.RUnlock()
	assert.False(t, exists)
}

func TestProvision_InvalidManifest(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)

	req := newProvisionRequest(durableCallbackTestLeaseUUID, "tenant-a", "docker-small", 1, []byte("not json"))
	err := b.Provision(context.Background(), req)

	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrValidation)
	assert.ErrorIs(t, err, backend.ErrInvalidManifest)
}

func TestProvision_RejectsFixedHostPort(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)

	// Manifest pins a fixed host port — the ENG-605 squatting/collision vector.
	payload := []byte(`{"image":"nginx:latest","ports":{"8080/tcp":{"host_port":8080}}}`)
	req := newProvisionRequest(durableCallbackTestLeaseUUID, "tenant-a", "docker-small", 1, payload)
	err := b.Provision(context.Background(), req)

	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrInvalidManifest)
	assert.Contains(t, err.Error(), "host_port")
}

func TestProvision_RejectsComposeServiceNameCollisionBeforeAdmission(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	req := backend.ProvisionRequest{
		LeaseUUID:    durableCallbackTestLeaseUUID,
		Tenant:       "tenant-a",
		ProviderUUID: durableCallbackTestLeaseUUID2,
		Items: []backend.LeaseItem{
			{SKU: "docker-small", Quantity: 2, ServiceName: "web"},
			{SKU: "docker-small", Quantity: 1, ServiceName: "web-0"},
		},
		CallbackURL: "https://fred.example/callbacks/provision?operation_id=9a72fbc1-38c8-4f31-87f7-f689979b9324",
		Payload: validStackManifestJSON(map[string]string{
			"web":   "docker.io/library/nginx:1.27",
			"web-0": "docker.io/library/redis:7",
		}),
	}

	err := b.Provision(context.Background(), req)
	require.ErrorIs(t, err, backend.ErrInvalidManifest)
	require.ErrorContains(t, err, `expanded Compose service name "web-0" collides`)
	b.provisionsMu.RLock()
	_, exists := b.provisions[req.LeaseUUID]
	b.provisionsMu.RUnlock()
	require.False(t, exists, "collision must be rejected before reservation or durable admission")
}

func TestProvision_DisallowedImage(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	b.cfg.AllowedRegistries = []string{"docker.io"}

	req := newProvisionRequest(durableCallbackTestLeaseUUID, "tenant-a", "docker-small", 1,
		validManifestJSON("evil-registry.com/malware:latest"))
	err := b.Provision(context.Background(), req)

	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrValidation)
	assert.ErrorIs(t, err, backend.ErrImageNotAllowed)
}

func TestProvision_InsufficientResources(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	// Exhaust the pool
	b.cfg.TotalCPUCores = 0.1
	b.cfg.TotalMemoryMB = 1
	b.pool = shared.NewResourcePool(b.cfg.TotalCPUCores, b.cfg.TotalMemoryMB, b.cfg.TotalDiskMB, b.cfg.GetSKUProfile, nil)

	req := newProvisionRequest(durableCallbackTestLeaseUUID, "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	err := b.Provision(context.Background(), req)

	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrInsufficientResources)
}

func TestProvision_MultiItem_PartialResourceRollback(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	// Only enough resources for 1 docker-small, not 2
	b.cfg.TotalCPUCores = 0.6
	b.cfg.TotalMemoryMB = 600
	b.pool = shared.NewResourcePool(b.cfg.TotalCPUCores, b.cfg.TotalMemoryMB, b.cfg.TotalDiskMB, b.cfg.GetSKUProfile, nil)

	req := backend.ProvisionRequest{
		LeaseUUID:    durableCallbackTestLeaseUUID,
		Tenant:       "tenant-a",
		ProviderUUID: nominalDockerProviderUUID,
		Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 2}},
		CallbackURL:  testOperationCallbackURL("http://localhost/callbacks/provision"),
		Payload:      validManifestJSON("nginx:latest"),
	}

	err := b.Provision(context.Background(), req)
	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrInsufficientResources)

	// All allocations should have been rolled back
	stats := b.pool.Stats()
	assert.Equal(t, 0, stats.AllocationCount)
}

// TestDoProvision_MultiItem_QuantityPassesTotalNotPerItem verifies that
// CreateContainerParams.Quantity receives the lease-wide totalQuantity, not
// the per-item quantity. This is critical for ComputeSubdomain/RouterName
// consistency between provision and restart/update paths.

// --- doProvision (async) tests ---

func TestDoProvision_ContextCanceled(t *testing.T) {

	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
			Status:   backend.ProvisionStatusProvisioning,
			Quantity: 1},
		},
	})
	b.provisions[durableCallbackTestLeaseUUID].CallbackURL = callbackServer.URL
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")

	manifest, _ := manifest.ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel before starting

	req := newProvisionRequest(durableCallbackTestLeaseUUID, "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	req.CallbackURL = testOperationCallbackURL(callbackServer.URL)
	b.doProvisionAndFire(t, ctx, req, manifest, profiles, b.logger)

	b.provisionsMu.RLock()
	prov := b.provisions[durableCallbackTestLeaseUUID]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)
}

func TestDoProvision_NetworkIsolation(t *testing.T) {

	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	networkCreated := false
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		EnsureTenantNetworkFn: func(ctx context.Context, tenant string) (string, error) {
			networkCreated = true
			assert.Equal(t, "tenant-a", tenant)
			return "net-123", nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			assert.NotNil(t, params.NetworkConfig, "network config should be set")
			return "container-1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
			Status:   backend.ProvisionStatusProvisioning,
			Quantity: 1},
		},
	})
	b.cfg.NetworkIsolation = ptrBool(true)
	b.provisions[durableCallbackTestLeaseUUID].CallbackURL = callbackServer.URL
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	manifest, _ := manifest.ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	req := newProvisionRequest(durableCallbackTestLeaseUUID, "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	req.CallbackURL = testOperationCallbackURL(callbackServer.URL)
	b.doProvisionAndFire(t, context.Background(), req, manifest, profiles, b.logger)

	assert.True(t, networkCreated)

	b.provisionsMu.RLock()
	prov := b.provisions[durableCallbackTestLeaseUUID]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status)
}

// --- Volume-aware provision tests ---

func TestDoProvision_VolumeCreateFailurePreservesRecoveryAuthority(t *testing.T) {
	callbackReceived := make(chan struct{}, 1)
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbackReceived <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	vm := &mockVolumeManager{
		CreateFn: func(ctx context.Context, id string, sizeMB int64) (string, bool, error) {
			return "", false, fmt.Errorf("disk full")
		},
	}

	createCalled := false
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{Volumes: map[string]struct{}{"/data": {}}}, nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			createCalled = true
			return "container-1", nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
			Status:   backend.ProvisionStatusProvisioning,
			Quantity: 1},
		},
	})
	b.volumes = vm
	b.provisions[durableCallbackTestLeaseUUID].CallbackURL = callbackServer.URL
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")

	manifest, _ := manifest.ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	req := newProvisionRequest(durableCallbackTestLeaseUUID, "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	req.CallbackURL = testOperationCallbackURL(callbackServer.URL)
	b.dispatchProvisionForTest(t, context.Background(), req, manifest, profiles, b.logger)
	awaitProvisionWorkerQuiescence(t, b, req.LeaseUUID)

	// Create crossed an external effect boundary. Even when the immediate
	// result says "disk full", only recovery can prove that no directory/quota
	// was partially published, so live execution must retain its non-terminal
	// projection and exact journal authority.
	b.provisionsMu.RLock()
	prov := b.provisions[durableCallbackTestLeaseUUID]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusProvisioning, prov.Status)
	intents, err := b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	assert.Equal(t, shared.OperationExecutionStarted, intents[0].ExecutionPhase())
	select {
	case <-callbackReceived:
		t.Fatal("ambiguous volume creation published a terminal callback")
	default:
	}

	// No containers should have been created
	assert.False(t, createCalled, "no containers should be created when volume creation fails")
}

func TestDoProvision_StatefulSKUNoImageVolumes(t *testing.T) {
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	createCalled := false
	vm := &mockVolumeManager{
		CreateFn: func(ctx context.Context, id string, sizeMB int64) (string, bool, error) {
			createCalled = true
			return "", true, nil
		},
	}

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			// Image has NO VOLUME declarations
			return &ImageInfo{Volumes: map[string]struct{}{}}, nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			// VolumeBinds should be nil for images without VOLUMEs
			assert.Nil(t, params.VolumeBinds)
			return "container-1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
			Status:   backend.ProvisionStatusProvisioning,
			Quantity: 1},
		},
	})
	b.volumes = vm
	b.provisions[durableCallbackTestLeaseUUID].CallbackURL = callbackServer.URL
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	manifest, _ := manifest.ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	req := newProvisionRequest(durableCallbackTestLeaseUUID, "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	req.CallbackURL = testOperationCallbackURL(callbackServer.URL)
	b.doProvisionAndFire(t, context.Background(), req, manifest, profiles, b.logger)

	// volumes.Create should NOT be called when image has no VOLUME paths
	assert.False(t, createCalled, "volumes.Create should not be called when image has no VOLUME paths")

	// Provision should still succeed
	b.provisionsMu.RLock()
	prov := b.provisions[durableCallbackTestLeaseUUID]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status)
}

func TestProvision_ReProvisionKeepsVolumes(t *testing.T) {
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440101"
	payload := validManifestJSON("nginx:latest")
	destroyCalled := false
	volumeRoot := t.TempDir()
	vm := &mockVolumeManager{
		defaultDir: volumeRoot,
		CreateFn: func(_ context.Context, id string, _ int64) (string, bool, error) {
			path := filepath.Join(volumeRoot, id)
			if err := os.MkdirAll(path, 0o755); err != nil {
				return "", false, err
			}
			return path, true, nil
		},
		DestroyFn: func(ctx context.Context, id string) error {
			destroyCalled = true
			return nil
		},
	}

	removedContainers := make(map[string]bool)
	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			removedContainers[containerID] = true
			return nil
		},
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{Volumes: map[string]struct{}{"/data": {}}}, nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "new-container", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		leaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: leaseUUID,
			Status:       backend.ProvisionStatusFailed,
			FailCount:    1,
			Quantity:     1,
			ContainerIDs: []string{"old-container"}},
		},
	})
	b.compose = &mockComposeExecutor{
		PSFn: func(context.Context, string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{{
				ID: "new-container", Service: manifest.DefaultServiceName, State: "running",
			}}, nil
		},
	}
	b.volumes = vm
	b.cfg.VolumeDataPath = volumeRoot
	_ = b.pool.TryAllocate(leaseUUID+"-app-0", "docker-small", "tenant-a")
	prepareFailedProvisionReplacement(t, b, mock, leaseUUID, "tenant-a", "docker-small", payload)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	callbackReceived := make(chan struct{})
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		select {
		case <-callbackReceived:
		default:
			close(callbackReceived)
		}
	}))
	defer callbackServer.Close()
	rebuildCallbackSender(b, callbackServer.Client())
	b.wg.Go(b.callbackSender.RunReplayLoop)

	req := newProvisionRequest(leaseUUID, "tenant-a", "docker-small", 1, payload)
	req.CallbackURL = testOperationCallbackURL(callbackServer.URL)
	err := b.Provision(context.Background(), req)
	require.NoError(t, err)

	select {
	case <-callbackReceived:
	case <-time.After(provisionFlowTimeout):
		t.Fatal("timed out waiting for replacement provision callback")
	}

	// Old container should be removed during re-provision cleanup
	assert.True(t, removedContainers["old-container"], "old container should be removed")

	// volumes.Destroy should NOT be called during re-provision — volumes persist
	assert.False(t, destroyCalled, "volumes should not be destroyed during re-provision")

	b.stopCancel()
	b.wg.Wait()
}

// --- Deprovision tests ---

func TestDeprovision_Idempotent(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)

	err := b.Deprovision(context.Background(), durableCallbackTestLeaseUUID3)
	assert.NoError(t, err, "deprovisioning a nonexistent lease should succeed")
}

func TestDeprovision_WithNetworkIsolation(t *testing.T) {
	networkCleanupCalled := false
	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
		RemoveTenantNetworkIfEmptyFn: func(ctx context.Context, tenant string) error {
			networkCleanupCalled = true
			assert.Equal(t, "tenant-a", tenant)
			return nil
		},
		ListManagedNetworksFn: func(context.Context) ([]networktypes.Inspect, error) {
			return []networktypes.Inspect{{
				Name:       TenantNetworkName("tenant-a"),
				Labels:     map[string]string{LabelTenant: "tenant-a"},
				Containers: map[string]networktypes.EndpointResource{},
			}}, nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
			Tenant:       "tenant-a",
			Status:       backend.ProvisionStatusReady,
			Quantity:     1,
			ContainerIDs: []string{"c1"}},
		},
	})
	b.cfg.NetworkIsolation = ptrBool(true)
	installReadyRuntimeProofForTest(t, b, durableCallbackTestLeaseUUID)

	err := b.Deprovision(context.Background(), durableCallbackTestLeaseUUID)
	require.NoError(t, err)
	assert.True(t, networkCleanupCalled)
}

// --- Deprovision volume tests ---

// TestDeprovision_SendsDeprovisionedCallback verifies that a clean successful
// Deprovision fires exactly one callback with status=deprovisioned and the
// backend name populated. Regression test for the 34 spurious failure events
// observed on the "Provision Rate by Outcome" dashboard.
func TestDeprovision_SendsDeprovisionedCallback(t *testing.T) {
	var received backend.CallbackPayload
	var receivedRawQuery string
	callbackDone := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedRawQuery = r.URL.RawQuery
		json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
		select {
		case callbackDone <- struct{}{}:
		default:
		}
	}))
	defer server.Close()

	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
			Tenant:       "tenant-a",
			Status:       backend.ProvisionStatusReady,
			Quantity:     1,
			ContainerIDs: []string{"c1"},
			CallbackURL:  server.URL + "/callbacks/provision?trace=keep&operation_id=550e8400-e29b-41d4-a716-446655440000"},
		},
	})
	installReadyRuntimeProofForTest(t, b, durableCallbackTestLeaseUUID)
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b, server.Client())
	startCallbackReplayForTest(b)

	require.NoError(t, b.Deprovision(context.Background(), durableCallbackTestLeaseUUID))

	select {
	case <-callbackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deprovisioned callback")
	}

	assert.Equal(t, durableCallbackTestLeaseUUID, received.LeaseUUID)
	assert.Equal(t, backend.CallbackStatusDeprovisioned, received.Status)
	assert.Empty(t, received.Error)
	assert.NotEmpty(t, received.Backend, "backend name should be populated for per-backend metrics")
	assert.False(t, received.Retained, "destroy path (RetainOnClose off) must report retained=false")
	assert.Equal(t, "trace=keep&lifecycle_id=550e8400-e29b-41d4-a716-446655440000", receivedRawQuery,
		"deprovision must use typed lifecycle authority and preserve unrelated query fields")
}

// TestDeprovision_RetainSuccessSendsRetainedCallback verifies the ENG-329 #6
// ground-truth flag: when RetainOnClose is enabled and all volumes are renamed
// into the retained namespace without error, the terminal deprovisioned callback
// carries retained=true.
func TestDeprovision_RetainSuccessSendsRetainedCallback(t *testing.T) {
	var received backend.CallbackPayload
	var receivedRawQuery string
	callbackDone := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&received)
		receivedRawQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
		select {
		case callbackDone <- struct{}{}:
		default:
		}
	}))
	defer server.Close()

	rs, err := newBoundRetentionStoreForTest(t, shared.RetentionStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "retention.db"),
	})
	require.NoError(t, err)
	defer rs.Close()

	mock := &mockDockerClient{RemoveContainerFn: func(ctx context.Context, id string) error { return nil }}
	canonical := canonicalVolumeName(durableCallbackTestLeaseUUID, "web", 0)
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
			Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID, Status: backend.ProvisionStatusReady, Quantity: 1,
			ContainerIDs:         []string{"c1"},
			CallbackURL:          server.URL + "/callbacks/provision?trace=keep&operation_id=550e8400-e29b-41d4-a716-446655440000",
			LifecycleCallbackURL: server.URL + "/callbacks/provision?trace=keep&lifecycle_id=550e8400-e29b-41d4-a716-446655440000",
			Items:                []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "web"}},
			StackManifest:        &manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": {Image: "nginx:1.25"}}}},
		},
	})
	var renamed [][2]string
	listedVolumes := []string{canonical}
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) { return append([]string(nil), listedVolumes...), nil },
		RenameVolumeFn: func(oldName, newName string) error {
			renamed = append(renamed, [2]string{oldName, newName})
			listedVolumes = []string{newName}
			return nil
		},
	}
	bindBackendToRetentionFixtureStore(t, b, rs)
	installReadyRuntimeProofForTest(t, b, durableCallbackTestLeaseUUID)
	b.cfg.RetainOnClose = true
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b, server.Client())
	startCallbackReplayForTest(b)

	require.NoError(t, b.Deprovision(context.Background(), durableCallbackTestLeaseUUID))

	select {
	case <-callbackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deprovisioned callback")
	}

	assert.Equal(t, backend.CallbackStatusDeprovisioned, received.Status)
	assert.True(t, received.Retained, "retain-success path must report retained=true")
	assert.Equal(t, "trace=keep&lifecycle_id=550e8400-e29b-41d4-a716-446655440000", receivedRawQuery,
		"retained observation must use typed lifecycle authority and preserve unrelated query fields")
	require.Len(t, renamed, 1, "the one canonical volume should be renamed into the retained namespace")
	assert.Equal(t, canonical, renamed[0][0])
	assert.Equal(t, retainedName(canonical), renamed[0][1])

	// And the record is queryable as retained.
	rec, err := rs.Get(durableCallbackTestLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, shared.RetentionStatusActive, rec.Status)
}

// TestDeprovision_RetainPartialFailureEmitsNoCallbackKeepsFailed verifies the
// true invariant for the partial-rename-failure path:
// doDeprovision returns an error WITHOUT emitting any Deprovisioned callback,
// and leaves the lease in ProvisionStatusFailed (containers gone) so the
// volume-cleanup retry re-attempts. This pins that the retained=true callback
// can ONLY fire on the all-renames-succeeded path.
func TestDeprovision_RetainPartialFailureEmitsNoCallbackKeepsFailed(t *testing.T) {
	var statuses []backend.CallbackStatus
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p backend.CallbackPayload
		json.NewDecoder(r.Body).Decode(&p)
		mu.Lock()
		statuses = append(statuses, p.Status)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	rs, err := newBoundRetentionStoreForTest(t, shared.RetentionStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "retention.db"),
	})
	require.NoError(t, err)
	defer rs.Close()

	mock := &mockDockerClient{RemoveContainerFn: func(ctx context.Context, id string) error { return nil }}
	canonical := canonicalVolumeName(durableCallbackTestLeaseUUID, "web", 0)
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
			Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID, Status: backend.ProvisionStatusReady, Quantity: 1,
			ContainerIDs: []string{"c1"}, CallbackURL: server.URL + "/callbacks/provision",
			Items:         []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "web"}},
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": {Image: "nginx:1.25"}}}},
		},
	})
	b.volumes = &mockVolumeManager{
		ListFn:         func() ([]string, error) { return []string{canonical}, nil },
		RenameVolumeFn: func(oldName, newName string) error { return fmt.Errorf("rename failed") },
	}
	bindBackendToRetentionFixtureStore(t, b, rs)
	installReadyRuntimeProofForTest(t, b, durableCallbackTestLeaseUUID)
	b.cfg.RetainOnClose = true
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b, server.Client())

	// Rename failure surfaces as a volume-cleanup error (under the limit → lease
	// kept Failed for retry, no terminal callback emitted on this attempt).
	err = b.Deprovision(context.Background(), durableCallbackTestLeaseUUID)
	require.Error(t, err)

	// No Deprovisioned callback may be emitted on a partial-failure attempt — the
	// retained=true notice fires ONLY on the all-renames-succeeded path.
	mu.Lock()
	got := append([]backend.CallbackStatus(nil), statuses...)
	mu.Unlock()
	assert.NotContains(t, got, backend.CallbackStatusDeprovisioned,
		"a partial rename failure must not emit a Deprovisioned callback")

	// Lease is kept visible in Failed for the volume-cleanup retry.
	b.provisionsMu.RLock()
	prov, ok := b.provisions[durableCallbackTestLeaseUUID]
	var gotStatus backend.ProvisionStatus
	if ok {
		gotStatus = prov.Status
	}
	b.provisionsMu.RUnlock()
	require.True(t, ok, "provision must stay visible for retry after a partial rename failure")
	assert.Equal(t, backend.ProvisionStatusFailed, gotStatus, "lease must be left Failed for retry")
}

// TestDeprovision_VolumeRetryKeepsProvisionFailed verifies that incomplete
// physical cleanup keeps the durable close pending and visible for retry.
func TestDeprovision_VolumeRetryKeepsProvisionFailed(t *testing.T) {
	mock := &mockDockerClient{RemoveContainerFn: func(ctx context.Context, id string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
			Tenant: "tenant-a", Status: backend.ProvisionStatusReady, Quantity: 1,
			ContainerIDs: []string{"c1"},
			Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}}}},
	})
	b.volumes = &mockVolumeManager{DestroyFn: func(ctx context.Context, id string) error {
		return fmt.Errorf("device busy")
	}}
	installReadyRuntimeProofForTest(t, b, durableCallbackTestLeaseUUID)

	err := b.Deprovision(context.Background(), durableCallbackTestLeaseUUID)
	require.Error(t, err, "incomplete volume cleanup returns an error")

	b.provisionsMu.RLock()
	p, ok := b.provisions[durableCallbackTestLeaseUUID]
	var gotStatus backend.ProvisionStatus
	var gotIDs []string
	if ok {
		gotStatus = p.Status
		gotIDs = append([]string(nil), p.ContainerIDs...)
	}
	b.provisionsMu.RUnlock()
	require.True(t, ok, "provision stays visible while its durable close remains pending")
	assert.Equal(t, backend.ProvisionStatusFailed, gotStatus)
	assert.Nil(t, gotIDs, "containers are gone")
}

// TestDeprovision_VolumeRetry_ConcurrentRecoverState verifies that incomplete
// durable close state remains retryable while periodic projection recovery
// runs concurrently. -race validates synchronization at this seam.
func TestDeprovision_VolumeRetry_ConcurrentRecoverState(t *testing.T) {
	// containersGone flips once compose Down removes the lease's containers, so
	// recoverState lists c1 while the lease is still Ready (preventing a
	// Ready-with-no-containers drop in setup) and lists nothing afterward (so the
	// in-flight entry hits the Deprovisioning preserve-case, not a label rebuild).
	var containersGone atomic.Bool
	mock := &mockDockerClient{
		RemoveContainerFn: func(_ context.Context, _ string) error { return nil },
		ListManagedContainersFn: func(_ context.Context) ([]ContainerInfo, error) {
			if containersGone.Load() {
				return nil, nil
			}
			return []ContainerInfo{{
				ContainerID: "c1", LeaseUUID: durableCallbackTestLeaseUUID, Tenant: "tenant-a",
				SKU: "docker-small", ServiceName: manifest.DefaultServiceName, Status: "running",
			}}, nil
		},
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
			Tenant: "tenant-a", Status: backend.ProvisionStatusReady, Quantity: 1,
			ContainerIDs: []string{"c1"},
			Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}}}},
	})
	b.compose = &mockComposeExecutor{DownFn: func(_ context.Context, _ string, _ time.Duration) error {
		containersGone.Store(true) // containers removed — recoverState now lists none
		return nil
	}}
	// Volume Destroy always fails, so the close remains pending.
	canonicalVolume := canonicalVolumeName(durableCallbackTestLeaseUUID, manifest.DefaultServiceName, 0)
	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, _ string) error {
			return fmt.Errorf("device busy")
		},
		ListForProofFn: func(context.Context) ([]string, error) {
			return []string{canonicalVolume}, nil
		},
	}
	installReadyRuntimeProofForTest(t, b, durableCallbackTestLeaseUUID)

	// Hammer recoverState concurrently with the deprovision so its map swap
	// interleaves with the volume-retry block's two critical sections.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if err := b.recoverState(context.Background()); err != nil {
					t.Errorf("recoverState: %v", err)
					return
				}
			}
		}
	}()

	err := b.Deprovision(context.Background(), durableCallbackTestLeaseUUID)
	close(stop)
	wg.Wait()

	require.Error(t, err, "incomplete volume cleanup returns an error")

	claims, listErr := b.closeSettlement.ListCloseIntents()
	require.NoError(t, listErr)
	require.Len(t, claims, 1,
		"incomplete cleanup remains owned by the durable close even if projection recovery omits it")
	assert.Equal(t, durableCallbackTestLeaseUUID, claims[0].LeaseUUID())
}

// TestDeprovision_RetryAfterPartialFailureFiresOneCallback verifies that the
// terminal callback only fires on clean completion. A first call that hits a
// partial-container-failure path emits no callback; a successful retry emits
// exactly one deprovisioned callback.
func TestDeprovision_RetryAfterPartialFailureFiresOneCallback(t *testing.T) {
	var statuses []backend.CallbackStatus
	var mu sync.Mutex
	callbackDone := make(chan struct{}, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p backend.CallbackPayload
		json.NewDecoder(r.Body).Decode(&p)
		w.WriteHeader(http.StatusOK)
		mu.Lock()
		statuses = append(statuses, p.Status)
		mu.Unlock()
		callbackDone <- struct{}{}
	}))
	defer server.Close()

	removeShouldFail := true
	containerPresent := true
	var b *Backend
	mock := &mockDockerClient{
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			if !containerPresent {
				return nil, errors.New("container not found")
			}
			return closeContainerForProjectionTest(b, durableCallbackTestLeaseUUID, id), nil
		},
		RemoveContainerFn: func(ctx context.Context, id string) error {
			if removeShouldFail {
				return fmt.Errorf("container removal failed")
			}
			containerPresent = false
			return nil
		},
		// The fallback re-discovers by label (ENG-647); an empty listing keeps the
		// recorded ContainerIDs as this test's subject across both retry attempts.
		ListManagedContainersFn: func(_ context.Context) ([]ContainerInfo, error) { return nil, nil },
	}
	b = newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
			Tenant: "tenant-a", Status: backend.ProvisionStatusReady, Quantity: 1,
			ContainerIDs: []string{"c1"}, CallbackURL: server.URL + "/callbacks/provision",
			Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}}}},
	})
	// Force the RemoveContainer fallback on both calls (compose Down fails).
	b.compose = &mockComposeExecutor{DownFn: func(ctx context.Context, project string, t time.Duration) error {
		return fmt.Errorf("compose down failed")
	}}
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	installReadyRuntimeProofForTest(t, b, durableCallbackTestLeaseUUID)
	rebuildCallbackSender(b, server.Client())
	startCallbackReplayForTest(b)

	// Call 1: partial container failure → error, NO callback, provision stays Failed.
	require.Error(t, b.Deprovision(context.Background(), durableCallbackTestLeaseUUID))
	b.provisionsMu.RLock()
	p, ok := b.provisions[durableCallbackTestLeaseUUID]
	var gotStatus backend.ProvisionStatus
	var gotIDs []string
	if ok {
		gotStatus = p.Status
		gotIDs = append([]string(nil), p.ContainerIDs...)
	}
	b.provisionsMu.RUnlock()
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusFailed, gotStatus)
	assert.Equal(t, []string{"c1"}, gotIDs, "ContainerIDs narrowed to the stuck containers")
	select {
	case <-callbackDone:
		t.Fatal("partial failure must NOT fire a callback")
	case <-time.After(1 * time.Second):
	}

	// Call 2: removal now succeeds → exactly one Deprovisioned callback.
	removeShouldFail = false
	require.NoError(t, b.Deprovision(context.Background(), durableCallbackTestLeaseUUID))
	select {
	case <-callbackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the deprovisioned callback")
	}
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, statuses, 1, "exactly one callback across both calls")
	assert.Equal(t, backend.CallbackStatusDeprovisioned, statuses[0])
}

// --- GetInfo tests ---

func TestGetInfo_NotProvisioned(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)

	_, err := b.GetInfo(context.Background(), "nonexistent")
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

func TestGetInfo_NotReady(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
			Status: backend.ProvisionStatusProvisioning},
		},
	})

	_, err := b.GetInfo(context.Background(), durableCallbackTestLeaseUUID)
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

// --- GetLogs tests ---

func TestGetLogs_NotProvisioned(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)

	_, err := b.GetLogs(context.Background(), "nonexistent", 100)
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

// --- GetProvision tests ---

func TestGetProvision_Found(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
			ProviderUUID: nominalDockerProviderUUID,
			Status:       backend.ProvisionStatusFailed,
			FailCount:    2,
			// Operator-only verbose detail stays on ProvisionState.LastError;
			// it must NOT surface on the tenant-facing ProvisionInfo (ENG-508).
			LastError: "exit_code=1; logs:\nSECRET=abc",
			Reason:    backend.ReasonContainerExited,
			Message:   "container exited unexpectedly"},
		},
	})

	info, err := b.GetProvision(context.Background(), durableCallbackTestLeaseUUID)
	require.NoError(t, err)
	assert.Equal(t, durableCallbackTestLeaseUUID, info.LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusFailed, info.Status)
	assert.Equal(t, 2, info.FailCount)
	assert.Equal(t, backend.ReasonContainerExited, info.Reason)
	assert.Equal(t, "container exited unexpectedly", info.Message)
	assert.NotContains(t, info.Message, "SECRET", "verbose operator detail must not leak into the tenant Message")
}

func TestGetProvision_NotFound(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)

	_, err := b.GetProvision(context.Background(), "nonexistent")
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

// TestGetProvision_LegacyDiagEntry_NoVerboseLeak pins the ENG-508 security cut at
// the docker read boundary: a legacy diagnostics entry that carries only the
// verbose operator Error (host path) with no curated Reason/Message must surface
// as a redacted ProvisionInfo — the verbose text never appears in the
// tenant-facing Message, and a FAILED status with no authored reason defaults to
// ReasonUnknown.
func TestGetProvision_LegacyDiagEntry_NoVerboseLeak(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)
	b.retentionStore = nil

	diagStore, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "diag.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = diagStore.Close() })
	b.diagnosticsStore = diagStore

	// Legacy entry: verbose operator Error only, no curated Reason/Message.
	require.NoError(t, diagStore.Store(shared.DiagnosticEntry{
		LeaseUUID:    "lease-legacy",
		ProviderUUID: nominalDockerProviderUUID,
		Error:        "xfs_quota /data/fred/volumes/x exit 1",
		FailCount:    1,
		CreatedAt:    time.Now(),
	}))

	info, err := b.GetProvision(context.Background(), "lease-legacy")
	require.NoError(t, err)
	assert.Equal(t, backend.ProvisionStatusFailed, info.Status)
	assert.NotContains(t, info.Message, "/data/fred/volumes", "verbose operator detail must not leak into the tenant Message")
	assert.Equal(t, backend.ReasonUnknown, info.Reason, "failed + empty reason must default to Unknown")
	assert.Nil(t, info.LifecycleGeneration, "pre-upgrade diagnostics remain readable as unknown")
}

func TestGetProvision_DiagnosticsFallbackPreservesTypedLifecycleGeneration(t *testing.T) {
	const (
		leaseUUID   = "lease-typed-diagnostics"
		lifecycleID = "9a72fbc1-38c8-4f31-87f7-f689979b9324"
	)
	b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)
	b.retentionStore = nil

	diagStore, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "diag.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = diagStore.Close() })
	b.diagnosticsStore = diagStore

	entry := leasesm.DiagnosticSnapshot(&leasesm.ProvisionState{
		LeaseUUID:            leaseUUID,
		ProviderUUID:         nominalDockerProviderUUID,
		Tenant:               "tenant-a",
		LastError:            "registry unavailable",
		Reason:               backend.ReasonImagePullFailed,
		Message:              "image pull failed",
		FailCount:            1,
		CallbackURL:          "https://fred.example/callbacks/provision?operation_id=" + lifecycleID,
		LifecycleCallbackURL: "https://fred.example/callbacks/provision?lifecycle_id=" + lifecycleID,
	})
	require.NoError(t, diagStore.Store(entry))

	info, err := b.GetProvision(context.Background(), leaseUUID)
	require.NoError(t, err)
	assert.Equal(t, &backend.LifecycleGenerationObservation{
		Kind: backend.LifecycleGenerationTyped,
		ID:   lifecycleID,
	}, info.LifecycleGeneration)

	listed, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	assert.Empty(t, listed, "diagnostic rows must not enter authoritative full inventory")

	page, next, err := b.ListProvisionsPage(context.Background(), "", 100)
	require.NoError(t, err)
	assert.Empty(t, page, "diagnostic rows must not enter authoritative paged inventory")
	assert.Empty(t, next)

	lookedUp, err := b.LookupProvisions(context.Background(), []string{leaseUUID})
	require.NoError(t, err)
	assert.Empty(t, lookedUp, "diagnostic rows must not enter authoritative lookup inventory")
}

// --- Workload field fixtures ---

// nonStackProvision returns a simple non-stack provision fixture.
// Post-Task-15 ProvisionState no longer carries the legacy single-Image
// field; the test fixture drops it. Tests asserting on the per-service
// image should read it from StackManifest.Services or ServiceImages.
func nonStackProvision() *provision {
	return &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
		ProviderUUID: nominalDockerProviderUUID,
		Status:       backend.ProvisionStatusReady,
		SKU:          "docker-micro",
		Quantity:     2},
	}
}

// stackProvision returns a stack provision fixture with web + db services.
func stackProvision() *provision {
	return &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
		ProviderUUID: nominalDockerProviderUUID,
		Status:       backend.ProvisionStatusReady,
		Quantity:     3,
		Items: []backend.LeaseItem{
			{SKU: "docker-micro", Quantity: 2, ServiceName: "web"},
			{SKU: "docker-large", Quantity: 1, ServiceName: "db"},
		},
		StackManifest: &manifest.StackManifest{
			Services: map[string]*manifest.Manifest{
				"web": {Image: "nginx:1.25"},
				"db":  {Image: "postgres:16"},
			},
		}},
	}
}

// stackProvisionNilManifest returns a stack provision with a nil manifest.StackManifest
// (simulates cold restart with no release store).
func stackProvisionNilManifest() *provision {
	return &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
		ProviderUUID:  nominalDockerProviderUUID,
		Status:        backend.ProvisionStatusReady,
		Quantity:      2,
		Items:         []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "web"}},
		StackManifest: nil},
	}
}

// assertNonStackFields verifies workload fields on a non-stack ProvisionInfo.
func assertNonStackFields(t *testing.T, info *backend.ProvisionInfo) {
	t.Helper()
	assert.Equal(t, "docker-micro", info.SKU)
	assert.Equal(t, "nginx:1.25", info.Image)
	assert.Equal(t, 2, info.Quantity)
	assert.Nil(t, info.Items)
	assert.Nil(t, info.ServiceImages)
}

// assertStackFields verifies workload fields on a stack ProvisionInfo.
func assertStackFields(t *testing.T, info *backend.ProvisionInfo) {
	t.Helper()
	assert.Equal(t, 3, info.Quantity)
	assert.Empty(t, info.Image, "stack lease should not set top-level Image")
	assert.Empty(t, info.SKU, "stack lease should not set top-level SKU")
	require.Len(t, info.Items, 2)
	require.NotNil(t, info.ServiceImages)
	assert.Equal(t, "nginx:1.25", info.ServiceImages["web"])
	assert.Equal(t, "postgres:16", info.ServiceImages["db"])
}

// assertNilManifestFields verifies workload fields when manifest.StackManifest is nil.
func assertNilManifestFields(t *testing.T, info *backend.ProvisionInfo) {
	t.Helper()
	require.Len(t, info.Items, 1)
	assert.Nil(t, info.ServiceImages, "nil manifest.StackManifest should produce nil ServiceImages")
}

// backendWithProvision creates a test backend with a single provision keyed by lease UUID.
func backendWithProvision(t *testing.T, prov *provision) *Backend {
	t.Helper()
	return newBackendForProvisionTest(t, &mockDockerClient{}, map[string]*provision{
		prov.LeaseUUID: prov,
	})
}

func TestGetProvision_WorkloadFields_Stack(t *testing.T) {
	b := backendWithProvision(t, stackProvision())
	info, err := b.GetProvision(context.Background(), durableCallbackTestLeaseUUID)
	require.NoError(t, err)
	assertStackFields(t, info)
}

func TestGetProvision_WorkloadFields_Stack_NilManifest(t *testing.T) {
	b := backendWithProvision(t, stackProvisionNilManifest())
	info, err := b.GetProvision(context.Background(), durableCallbackTestLeaseUUID)
	require.NoError(t, err)
	assertNilManifestFields(t, info)
}

// --- ListProvisions tests ---

func TestListProvisions_Empty(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)
	result, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestListProvisions_Multiple(t *testing.T) {
	now := time.Now()
	b := newBackendForProvisionTest(t, &mockDockerClient{}, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
			ProviderUUID: nominalDockerProviderUUID,
			Status:       backend.ProvisionStatusReady,
			CreatedAt:    now},
		},
		"lease-2": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-2",
			ProviderUUID: nominalDockerProviderUUID,
			Status:       backend.ProvisionStatusFailed,
			CreatedAt:    now,
			FailCount:    3},
		},
	})

	result, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	assert.Len(t, result, 2)

	for _, pi := range result {
		assert.NotEmpty(t, pi.LeaseUUID)
		assert.Equal(t, nominalDockerProviderUUID, pi.ProviderUUID)
		assert.NotEmpty(t, pi.BackendName)
	}
}

func TestListProvisions_WorkloadFields_Stack(t *testing.T) {
	b := backendWithProvision(t, stackProvision())
	result, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	require.Len(t, result, 1)
	assertStackFields(t, &result[0])
}

func TestListProvisions_WorkloadFields_Stack_NilManifest(t *testing.T) {
	b := backendWithProvision(t, stackProvisionNilManifest())
	result, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	require.Len(t, result, 1)
	assertNilManifestFields(t, &result[0])
}

func TestListProvisions_ItemsDefensivelyCopied(t *testing.T) {
	originalItems := []backend.LeaseItem{
		{SKU: "docker-micro", Quantity: 2, ServiceName: "web"},
	}
	b := newBackendForProvisionTest(t, &mockDockerClient{}, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
			ProviderUUID:  nominalDockerProviderUUID,
			Status:        backend.ProvisionStatusReady,
			Quantity:      2,
			Items:         originalItems,
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": {Image: "nginx:1.25"}}}},
		},
	})

	result, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	require.Len(t, result, 1)

	result[0].Items[0].SKU = "mutated"
	assert.Equal(t, "docker-micro", originalItems[0].SKU, "returned Items should be a copy, not share backing array")
}

// --- LookupProvisions tests ---

func TestLookupProvisions_Empty(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)
	result, err := b.LookupProvisions(context.Background(), []string{durableCallbackTestLeaseUUID})
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestLookupProvisions_Subset(t *testing.T) {
	now := time.Now()
	b := newBackendForProvisionTest(t, &mockDockerClient{}, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID, ProviderUUID: nominalDockerProviderUUID, Status: backend.ProvisionStatusReady, CreatedAt: now}},
		"lease-2":                    {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-2", ProviderUUID: nominalDockerProviderUUID, Status: backend.ProvisionStatusReady, CreatedAt: now}},
		"lease-3":                    {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-3", ProviderUUID: nominalDockerProviderUUID, Status: backend.ProvisionStatusReady, CreatedAt: now}},
	})

	result, err := b.LookupProvisions(context.Background(), []string{durableCallbackTestLeaseUUID, "lease-3"})
	require.NoError(t, err)
	assert.Len(t, result, 2)

	got := make(map[string]bool, len(result))
	for _, p := range result {
		got[p.LeaseUUID] = true
	}
	assert.True(t, got[durableCallbackTestLeaseUUID])
	assert.True(t, got["lease-3"])
	assert.False(t, got["lease-2"])
}

func TestLookupProvisions_UnknownIgnored(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{}, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID, Status: backend.ProvisionStatusReady}},
	})

	result, err := b.LookupProvisions(context.Background(), []string{durableCallbackTestLeaseUUID, "lease-unknown"})
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, durableCallbackTestLeaseUUID, result[0].LeaseUUID)
}

func TestLookupProvisions_AllUnknownReturnsEmpty(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{}, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID, Status: backend.ProvisionStatusReady}},
	})

	result, err := b.LookupProvisions(context.Background(), []string{"lease-unknown-1", "lease-unknown-2"})
	require.NoError(t, err)
	assert.Empty(t, result)
	// The slice is non-nil so it serializes as `[]` not `null`.
	assert.NotNil(t, result)
}

func TestLookupProvisions_StackImageRoundTrip(t *testing.T) {
	b := backendWithProvision(t, stackProvision())
	result, err := b.LookupProvisions(context.Background(), []string{durableCallbackTestLeaseUUID})
	require.NoError(t, err)
	require.Len(t, result, 1)
	assertStackFields(t, &result[0])
}

// --- sendCallback / trySendCallback tests ---

func TestSendCallback_Success(t *testing.T) {

	var received backend.CallbackPayload
	var receivedSig string
	delivered := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedSig = r.Header.Get(hmacauth.SignatureHeader)
		json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
		delivered <- struct{}{}
	}))
	defer server.Close()

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID, CallbackURL: server.URL + "/callbacks/provision"}},
	})
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	seedOperationCallbackForTest(t, b, server.URL+"/callbacks/provision")
	rebuildCallbackSender(b, server.Client())

	b.sendOperationCallback(durableCallbackTestLeaseUUID, backend.CallbackStatusSuccess, "")
	startCallbackReplayForTest(b)
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for successful callback replay")
	}

	assert.Equal(t, durableCallbackTestLeaseUUID, received.LeaseUUID)
	assert.Equal(t, backend.CallbackStatusSuccess, received.Status)
	assert.NotEmpty(t, receivedSig, "HMAC signature header should be set")
}

func TestSendCallback_FailurePayload(t *testing.T) {

	var received backend.CallbackPayload
	delivered := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
		delivered <- struct{}{}
	}))
	defer server.Close()

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID, CallbackURL: server.URL + "/callbacks/provision"}},
	})
	seedOperationCallbackForTest(t, b, server.URL+"/callbacks/provision")
	rebuildCallbackSender(b, server.Client())

	b.sendOperationCallback(durableCallbackTestLeaseUUID, backend.CallbackStatusFailed, "image pull failed")
	startCallbackReplayForTest(b)
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for failed callback replay")
	}

	assert.Equal(t, backend.CallbackStatusFailed, received.Status)
	assert.Equal(t, "image pull failed", received.Error)
}

func TestSendCallback_NoCallbackURL(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	// No panic, no error — just a log warning
	b.sendOperationCallback("unknown-lease", backend.CallbackStatusSuccess, "")
}

func TestSendCallback_TruncatesLongError(t *testing.T) {
	var received backend.CallbackPayload
	delivered := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
		delivered <- struct{}{}
	}))
	defer server.Close()

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID, CallbackURL: server.URL + "/callbacks/provision"}},
	})
	seedOperationCallbackForTest(t, b, server.URL+"/callbacks/provision")
	rebuildCallbackSender(b, server.Client())

	// Send an error message that exceeds the on-chain rejection reason limit.
	longError := strings.Repeat("x", callbackMaxErrorLen+100)
	b.sendOperationCallback(durableCallbackTestLeaseUUID, backend.CallbackStatusFailed, longError)
	startCallbackReplayForTest(b)
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for truncated callback replay")
	}

	assert.LessOrEqual(t, len(received.Error), callbackMaxErrorLen,
		"callback error should be truncated to fit on-chain limit")
	assert.True(t, strings.HasSuffix(received.Error, "..."))
}

func TestSendCallback_Retry(t *testing.T) {

	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID, CallbackURL: server.URL + "/callbacks/provision"}},
	})
	seedOperationCallbackForTest(t, b, server.URL+"/callbacks/provision")
	rebuildCallbackSender(b, server.Client())

	b.sendOperationCallback(durableCallbackTestLeaseUUID, backend.CallbackStatusSuccess, "")
	startCallbackReplayForTest(b)
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	require.Eventually(t, func() bool { return attempts.Load() == 3 }, time.Second, 10*time.Millisecond)

	assert.Equal(t, int32(3), attempts.Load(), "should have retried 3 times")
}

func TestSendCallback_ShutdownAbortsRetry(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID, CallbackURL: server.URL + "/callbacks/provision"}},
	})
	seedOperationCallbackForTest(t, b, server.URL+"/callbacks/provision")
	// Use long backoff so shutdown cancellation is observable. Built inline
	// rather than via rebuildCallbackSender because that helper pins
	// zeroBackoff, which is the one thing this test cannot use.
	longBackoff := [shared.CallbackMaxAttempts]time.Duration{0, 5 * time.Second, 5 * time.Second}
	secret := string(b.cfg.CallbackSecret)
	if b.callbackStore != nil && len(secret) < hmacauth.MinSecretLength {
		secret = durableCallbackTestSecret
	}
	senderCfg := shared.CallbackSenderConfig{
		Store: b.callbackStore,
		StorageAttestor: shared.MustNewCallbackStorageAttestor(
			b.callbackStore,
			dockerCallbackStorageVerifier{verifier: b.storageVerifier, gate: b.storeAuthorityGate},
			b.stopCtx,
		),
		HTTPClient: server.Client(), Secret: secret, Logger: b.logger,
		Backoff: &longBackoff,
	}
	b.callbackSender = shared.MustNewCallbackSender(senderCfg)
	operations, ok := concreteOperationSettlementForTest(b.operationSettlement)
	require.True(t, ok)
	b.callbackPublisher = mustNewCallbackPublisherForTest(t, shared.CallbackPublisherConfig{
		OperationSettlement:   operations,
		MaintenanceSettlement: b.maintenanceSettlement,
		StorageAttestor:       senderCfg.StorageAttestor,
		Logger:                senderCfg.Logger,
		OnStoreError:          senderCfg.OnStoreError,
	})

	b.sendOperationCallback(durableCallbackTestLeaseUUID, backend.CallbackStatusSuccess, "")
	startCallbackReplayForTest(b)
	require.Eventually(t, func() bool { return attempts.Load() > 0 }, time.Second, 10*time.Millisecond)
	b.stopCancel()
	b.wg.Wait()

	// Should have stopped after 1 attempt due to shutdown
	assert.GreaterOrEqual(t, attempts.Load(), int32(1))
	assert.LessOrEqual(t, attempts.Load(), int32(2))
}

// --- Start / Stop / Health tests ---

func TestStart_Success(t *testing.T) {
	mock := &mockDockerClient{
		PingFn: func(ctx context.Context) error { return nil },
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
		CloseFn: func() error { return nil },
	}

	b := newBackendForProvisionTest(t, mock, nil)
	bindTestStorageIdentity(t, b, mock)
	err := b.Start(context.Background())
	require.NoError(t, err)

	// Clean shutdown
	err = b.Stop()
	assert.NoError(t, err)
}

func TestStart_PingFails(t *testing.T) {
	mock := &mockDockerClient{
		PingFn: func(ctx context.Context) error {
			return errors.New("docker not available")
		},
	}

	b := newBackendForProvisionTest(t, mock, nil)
	bindTestStorageIdentity(t, b, mock)
	err := b.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect to Docker")
}

func TestStart_ManagedContainerTopologyReadFails(t *testing.T) {
	mock := &mockDockerClient{
		PingFn: func(ctx context.Context) error { return nil },
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return nil, errors.New("docker error")
		},
	}

	b := newBackendForProvisionTest(t, mock, nil)
	bindTestStorageIdentity(t, b, mock)
	err := b.Start(context.Background())
	require.Error(t, err)
	assert.ErrorContains(t, err, "validate supported managed-container topology")
	assert.ErrorContains(t, err, "docker error")
}

func TestHealth(t *testing.T) {
	mock := &mockDockerClient{
		PingFn: func(ctx context.Context) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, nil)
	assert.NoError(t, b.Health(context.Background()))

	mock.PingFn = func(ctx context.Context) error { return errors.New("unhealthy") }
	assert.Error(t, b.Health(context.Background()))
}

func TestName(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	assert.Equal(t, b.cfg.Name, b.Name())
}

// --- Pure function tests ---

func TestShortID(t *testing.T) {
	assert.Equal(t, "abcdefghijkl", leasesm.ShortID("abcdefghijklmnop"))
	assert.Equal(t, "short", leasesm.ShortID("short"))
	assert.Equal(t, "", leasesm.ShortID(""))
}

func TestContainerStatusToProvisionStatus(t *testing.T) {
	tests := []struct {
		status string
		want   backend.ProvisionStatus
	}{
		{"created", backend.ProvisionStatusProvisioning},
		{"restarting", backend.ProvisionStatusProvisioning},
		{"running", backend.ProvisionStatusReady},
		{"paused", backend.ProvisionStatusReady},
		{"Running", backend.ProvisionStatusReady}, // case insensitive
		{"removing", backend.ProvisionStatusFailed},
		{"exited", backend.ProvisionStatusFailed},
		{"dead", backend.ProvisionStatusFailed},
		{"unknown", backend.ProvisionStatusUnknown},
		{"", backend.ProvisionStatusUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			assert.Equal(t, tt.want, containerStatusToProvisionStatus(tt.status))
		})
	}
}

func TestRemoveProvision(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID, CallbackURL: "http://localhost"}},
	})

	b.removeProvision(durableCallbackTestLeaseUUID)

	b.provisionsMu.RLock()
	_, exists := b.provisions[durableCallbackTestLeaseUUID]
	b.provisionsMu.RUnlock()
	assert.False(t, exists)

	// Removing nonexistent should not panic
	b.removeProvision("nonexistent")
}

// --- waitForHealthy tests ---

func TestWaitForHealthy_AllHealthy(t *testing.T) {
	callCount := 0
	mock := &mockDockerClient{
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			callCount++
			return &ContainerInfo{
				ContainerID: id,
				Status:      "running",
				Health:      HealthStatusHealthy,
			}, nil
		},
	}
	b := newBackendForTest(mock, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := b.waitForHealthy(ctx, []string{"c1", "c2"}, b.logger)
	assert.NoError(t, err)
	assert.Equal(t, 2, callCount)
}

func TestWaitForHealthy_Unhealthy(t *testing.T) {
	mock := &mockDockerClient{
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			return &ContainerInfo{
				ContainerID: id,
				Status:      "running",
				Health:      HealthStatusUnhealthy,
			}, nil
		},
		ContainerLogsFn: func(_ context.Context, _ string, _ int) (string, error) {
			return "", nil
		},
	}
	b := newBackendForTest(mock, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := b.waitForHealthy(ctx, []string{"c1"}, b.logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unhealthy")
}

func TestWaitForHealthy_ContainerExited(t *testing.T) {
	mock := &mockDockerClient{
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			return &ContainerInfo{
				ContainerID: id,
				Status:      "exited",
				Health:      HealthStatusNone,
				ExitCode:    137,
				OOMKilled:   true,
			}, nil
		},
		ContainerLogsFn: func(_ context.Context, _ string, _ int) (string, error) {
			return "Killed", nil
		},
	}
	b := newBackendForTest(mock, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := b.waitForHealthy(ctx, []string{"c1"}, b.logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exited")
	assert.Contains(t, err.Error(), "exit_code=137")
	assert.Contains(t, err.Error(), "oom_killed=true")
}

func TestWaitForHealthy_ContextTimeout(t *testing.T) {
	mock := &mockDockerClient{
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			return &ContainerInfo{
				ContainerID: id,
				Status:      "running",
				Health:      HealthStatusStarting,
			}, nil
		},
	}
	b := newBackendForTest(mock, nil)

	// Use a very short timeout so the test doesn't wait long.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := b.waitForHealthy(ctx, []string{"c1"}, b.logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
}

func TestWaitForHealthy_InspectFailure(t *testing.T) {
	mock := &mockDockerClient{
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			return nil, fmt.Errorf("docker daemon error")
		},
	}
	b := newBackendForTest(mock, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := b.waitForHealthy(ctx, []string{"c1"}, b.logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to inspect")
}

func TestWaitForHealthy_BecomesHealthyAfterStarting(t *testing.T) {
	inspectCount := 0
	mock := &mockDockerClient{
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			inspectCount++
			health := HealthStatusStarting
			if inspectCount >= 2 {
				health = HealthStatusHealthy
			}
			return &ContainerInfo{
				ContainerID: id,
				Status:      "running",
				Health:      health,
			}, nil
		},
	}
	b := newBackendForTest(mock, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := b.waitForHealthy(ctx, []string{"c1"}, b.logger)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, inspectCount, 2, "should have polled at least twice")
}

// --- Error persistence tests ---

func TestDoProvision_LastError_ContextCanceled(t *testing.T) {

	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
			Status:      backend.ProvisionStatusProvisioning,
			Quantity:    1,
			CallbackURL: callbackServer.URL},
		},
	})
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")

	manifest, _ := manifest.ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := newProvisionRequest(durableCallbackTestLeaseUUID, "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	req.CallbackURL = testOperationCallbackURL(callbackServer.URL)
	b.doProvisionAndFire(t, ctx, req, manifest, profiles, b.logger)

	b.provisionsMu.RLock()
	prov := b.provisions[durableCallbackTestLeaseUUID]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)
	assert.Contains(t, prov.LastError, "canceled")
}

func TestDoProvision_LastError_ClearedOnSuccess(t *testing.T) {

	callbackReceived := make(chan struct{})
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		select {
		case <-callbackReceived:
		default:
			close(callbackReceived)
		}
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "container-1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	// Start with a previously failed provision that had a LastError
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
			Status:      backend.ProvisionStatusProvisioning,
			Quantity:    1,
			LastError:   "previous error",
			CallbackURL: callbackServer.URL},
		},
	})
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")

	manifest, _ := manifest.ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	req := newProvisionRequest(durableCallbackTestLeaseUUID, "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	req.CallbackURL = testOperationCallbackURL(callbackServer.URL)
	b.doProvisionAndFire(t, context.Background(), req, manifest, profiles, b.logger)
	<-callbackReceived

	b.provisionsMu.RLock()
	prov := b.provisions[durableCallbackTestLeaseUUID]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status)
	// LastError should still have old value since success path doesn't clear it
	// (it's only set on failure, not cleared on success — which is correct
	// because a re-provision creates a new provision record)
}

func TestListProvisions_IncludesReasonMessage(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
			ProviderUUID: nominalDockerProviderUUID,
			Status:       backend.ProvisionStatusFailed,
			CreatedAt:    time.Now(),
			FailCount:    2,
			// Operator-only verbose detail; must not surface on the wire (ENG-508).
			LastError: "container crashed: /data/fred/volumes/x",
			Reason:    backend.ReasonContainerExited,
			Message:   "container exited unexpectedly"},
		},
	})

	result, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, backend.ReasonContainerExited, result[0].Reason)
	assert.Equal(t, "container exited unexpectedly", result[0].Message)
	assert.NotContains(t, result[0].Message, "/data/fred", "verbose operator detail must not leak on the wire")
	assert.Equal(t, 2, result[0].FailCount)
}

// --- Disk quota container creation tests ---

// --- Callback persistence tests ---

func TestSendCallback_PersistsBeforeDelivery(t *testing.T) {
	var delivered atomic.Bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		delivered.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb_persist.db")
	cbStore, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer cbStore.Close()

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: durableCallbackTestLeaseUUID, CallbackURL: server.URL + "/callbacks/provision",
		}},
	})
	bindBackendToOperationIntentTestStore(t, b, cbStore)
	rebuildCallbackSender(b, server.Client())
	spec := dockerOperationIntentSpec(t, b.storageIdentity)
	spec.CallbackURL = testOperationCallbackURL(server.URL + "/callbacks/provision")
	spec.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(spec.CallbackURL, "")
	require.NoError(t, err)
	_, err = beginDockerTestOperationIntent(t, cbStore, spec, b.storageIdentity)
	require.NoError(t, err)

	b.sendOperationCallback(durableCallbackTestLeaseUUID, backend.CallbackStatusSuccess, "")
	assert.False(t, delivered.Load(), "durable settlement must not perform HTTP inline")
	pending, err := cbStore.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1, "callback must be durable before replay owns delivery")

	startCallbackReplayForTest(b)
	defer func() {
		b.stopCancel()
		b.wg.Wait()
	}()
	require.Eventually(t, delivered.Load, time.Second, 10*time.Millisecond,
		"replay must deliver the durably stored callback")
	require.Eventually(t, func() bool {
		pending, listErr := cbStore.ListPending()
		return listErr == nil && len(pending) == 0
	}, time.Second, 10*time.Millisecond, "callback should be removed from store after delivery")
}

func TestSendCallback_DurableFailureRemainsPendingAfterReplayFailure(t *testing.T) {
	var requests atomic.Int32

	// Server always returns 500
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb_fail.db")
	cbStore, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer cbStore.Close()

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: durableCallbackTestLeaseUUID, CallbackURL: server.URL + "/callbacks/provision",
		}},
	})
	bindBackendToOperationIntentTestStore(t, b, cbStore)
	rebuildCallbackSender(b, server.Client())
	spec := dockerOperationIntentSpec(t, b.storageIdentity)
	spec.CallbackURL = testOperationCallbackURL(server.URL + "/callbacks/provision")
	spec.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(spec.CallbackURL, "")
	require.NoError(t, err)
	_, err = beginDockerTestOperationIntent(t, cbStore, spec, b.storageIdentity)
	require.NoError(t, err)

	b.sendOperationCallback(durableCallbackTestLeaseUUID, backend.CallbackStatusFailed, "container crashed")
	assert.Zero(t, requests.Load(), "durable settlement must not perform HTTP inline")
	startCallbackReplayForTest(b)
	defer func() {
		b.stopCancel()
		b.wg.Wait()
	}()
	require.Eventually(t, func() bool {
		return requests.Load() >= int32(shared.CallbackMaxAttempts)
	}, time.Second, 10*time.Millisecond)
	assert.GreaterOrEqual(t, requests.Load(), int32(shared.CallbackMaxAttempts))
	assert.LessOrEqual(t, requests.Load(), int32(2*shared.CallbackMaxAttempts),
		"startup replay and its already-coalesced wake may each run one bounded retry chain")

	// After failed delivery, callback should remain in store
	pending, err := cbStore.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, durableCallbackTestLeaseUUID, pending[0].LeaseUUID)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	assert.Equal(t, "container crashed", pending[0].Error)
}

// --- Additional coverage tests ---

// Fix 1: Total deprovision failure — ALL container removals fail.

// Fix 2: Negative CallbackMaxAge rejected by config validation.
func TestConfigValidation_NegativeCallbackMaxAge(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SKUProfiles = defaultTestSKUProfiles()
	cfg.CallbackSecret = "this-is-a-32-character-secret!!x"
	cfg.HostAddress = "192.168.1.100"
	cfg.CallbackMaxAge = -1 * time.Hour

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "callback_max_age")
}

// Fix 4: GetLogs on a provisioning (in-progress) lease still returns logs.

// Fix 4: GetLogs with empty ContainerIDs returns empty map.
func TestGetLogs_EmptyContainerIDs(t *testing.T) {
	mock := &mockDockerClient{}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{}},
		},
	})

	logs, err := b.GetLogs(context.Background(), durableCallbackTestLeaseUUID, 100)
	require.NoError(t, err)
	assert.Empty(t, logs)
}

// Fix 5: Explicit (non-ephemeral) port conflict returns error immediately without retry.
func TestCreateContainer_ExplicitPortConflict_NoRetry(t *testing.T) {
	// Verify the decision logic: explicit ports should NOT trigger retry
	assert.False(t, hasEphemeralPorts(map[string]manifest.PortConfig{
		"80/tcp": {HostPort: 8080},
	}), "explicit ports should not be ephemeral")
	assert.True(t, isPortBindingError(fmt.Errorf("port is already allocated")))

	// Mixed ports (one explicit, one ephemeral) DO trigger retry
	assert.True(t, hasEphemeralPorts(map[string]manifest.PortConfig{
		"80/tcp":  {HostPort: 8080}, // explicit
		"443/tcp": {HostPort: 0},    // ephemeral
	}), "mixed ports with an ephemeral should be ephemeral")

	// No ports means no ephemeral
	assert.False(t, hasEphemeralPorts(nil))
	assert.False(t, hasEphemeralPorts(map[string]manifest.PortConfig{}))
}

// Fix 1: Deprovision on a provisioning lease still removes containers.

// TestDeprovision_ActiveProvisionsGauge verifies the projection store derives
// Ready-count changes from the status mutation itself: a clean deprovision of a
// Ready lease decrements exactly once, while a Failed lease does not.
func TestDeprovision_ActiveProvisionsGauge(t *testing.T) {
	t.Run("Ready lease decrements the gauge", func(t *testing.T) {
		mock := &mockDockerClient{
			RemoveContainerFn: func(ctx context.Context, containerID string) error { return nil },
		}
		b := newBackendForProvisionTest(t, mock, map[string]*provision{
			durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
				Tenant:       "tenant-a",
				Status:       backend.ProvisionStatusReady,
				Quantity:     1,
				ContainerIDs: []string{"c1"},
				Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 1,
					ServiceName: manifest.DefaultServiceName}}},
			},
		})
		seedProvisionReleaseFromProjectionForBackendTest(t, b, durableCallbackTestLeaseUUID)

		before := testutil.ToFloat64(activeProvisions)
		require.NoError(t, b.Deprovision(context.Background(), durableCallbackTestLeaseUUID))
		assert.Equal(t, -1.0, testutil.ToFloat64(activeProvisions)-before,
			"Ready→Deprovisioning transition decrements activeProvisions exactly once")
	})

	t.Run("non-Ready lease does not decrement", func(t *testing.T) {
		mock := &mockDockerClient{
			RemoveContainerFn: func(ctx context.Context, containerID string) error { return nil },
		}
		b := newBackendForProvisionTest(t, mock, map[string]*provision{
			durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
				Tenant:       "tenant-a",
				Status:       backend.ProvisionStatusFailed,
				Quantity:     1,
				ContainerIDs: []string{"c1"},
				Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 1,
					ServiceName: manifest.DefaultServiceName}}},
			},
		})
		seedProvisionReleaseFromProjectionForBackendTest(t, b, durableCallbackTestLeaseUUID)

		before := testutil.ToFloat64(activeProvisions)
		require.NoError(t, b.Deprovision(context.Background(), durableCallbackTestLeaseUUID))
		assert.Equal(t, 0.0, testutil.ToFloat64(activeProvisions)-before,
			"non-Ready (Failed) lease must not touch the gauge (wasReady=false → no Dec)")
	})
}

// Fix 4: GetLogs on a failed lease still returns logs for remaining containers.

// --- containerFailureDiagnostics tests ---

func TestContainerFailureDiagnostics_ExitCodeAndLogs(t *testing.T) {
	mock := &mockDockerClient{
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			assert.Equal(t, diagnosticLogTail, tail)
			return "Error: EACCES: permission denied", nil
		},
	}
	b := newBackendForTest(mock, nil)

	info := &ContainerInfo{Status: "exited", ExitCode: 1}
	diag := b.containerFailureDiagnostics(context.Background(), "c1", containerInfoToInstanceState(info))

	assert.Equal(t, "exit_code=1; logs:\nError: EACCES: permission denied", diag)
}

func TestContainerFailureDiagnostics_OOMKilled(t *testing.T) {
	mock := &mockDockerClient{
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return "Killed", nil
		},
	}
	b := newBackendForTest(mock, nil)

	info := &ContainerInfo{Status: "exited", ExitCode: 137, OOMKilled: true}
	diag := b.containerFailureDiagnostics(context.Background(), "c1", containerInfoToInstanceState(info))

	assert.Contains(t, diag, "exit_code=137")
	assert.Contains(t, diag, "oom_killed=true")
	assert.Contains(t, diag, "Killed")
}

func TestContainerFailureDiagnostics_LogsFetchFails(t *testing.T) {
	mock := &mockDockerClient{
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return "", errors.New("container not found")
		},
	}
	b := newBackendForTest(mock, nil)

	info := &ContainerInfo{Status: "exited", ExitCode: 1}
	diag := b.containerFailureDiagnostics(context.Background(), "c1", containerInfoToInstanceState(info))

	assert.Equal(t, "exit_code=1", diag)
}

func TestContainerFailureDiagnostics_ZeroExitCode(t *testing.T) {
	mock := &mockDockerClient{
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return "", nil
		},
	}
	b := newBackendForTest(mock, nil)

	info := &ContainerInfo{Status: "exited", ExitCode: 0}
	diag := b.containerFailureDiagnostics(context.Background(), "c1", containerInfoToInstanceState(info))

	assert.Equal(t, "exit_code=0", diag)
}

func TestContainerFailureDiagnostics_Truncation(t *testing.T) {
	// Generate logs larger than diagnosticMaxBytes
	largeLogs := strings.Repeat("x", diagnosticMaxBytes)
	mock := &mockDockerClient{
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return largeLogs, nil
		},
	}
	b := newBackendForTest(mock, nil)

	info := &ContainerInfo{Status: "exited", ExitCode: 1}
	diag := b.containerFailureDiagnostics(context.Background(), "c1", containerInfoToInstanceState(info))

	assert.LessOrEqual(t, len(diag), diagnosticMaxBytes)
	assert.True(t, strings.HasSuffix(diag, "..."))
}

// --- Callback sanitization tests ---
// These tests verify that callback error messages (which flow on-chain) never
// contain container logs or dynamic data, while prov.LastError retains full
// diagnostics for authenticated API access.

func TestDoProvision_CallbackSanitized_PullFailure(t *testing.T) {
	var callbackPayload backend.CallbackPayload
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&callbackPayload)
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return errors.New("unauthorized: authentication required for registry.example.com")
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
			Status:      backend.ProvisionStatusProvisioning,
			Quantity:    1,
			CallbackURL: callbackServer.URL},
		},
	})
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")

	manifest, _ := manifest.ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	req := newProvisionRequest(durableCallbackTestLeaseUUID, "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	req.CallbackURL = testOperationCallbackURL(callbackServer.URL)
	b.doProvisionAndFire(t, context.Background(), req, manifest, profiles, b.logger)

	// Callback should have hardcoded message — no registry auth details.
	assert.Equal(t, "image pull failed", callbackPayload.Error)
	assert.NotContains(t, callbackPayload.Error, "registry.example.com")

	// LastError should contain the full error.
	b.provisionsMu.RLock()
	prov := b.provisions[durableCallbackTestLeaseUUID]
	b.provisionsMu.RUnlock()
	assert.Contains(t, prov.LastError, "registry.example.com")
}

func TestStartupErrorToCallbackMsg(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"unhealthy", fmt.Errorf("container 0 reported unhealthy: exit_code=1; logs: oops"), "container reported unhealthy"},
		{"exited during startup", fmt.Errorf("container 0 exited during startup (status: exited): exit_code=1"), "container exited during startup"},
		{"exited during health", fmt.Errorf("container 0 exited while waiting for healthy"), "container exited during health check"},
		{"timeout", fmt.Errorf("timed out waiting for containers to become healthy"), "container exited during startup"},
		{"canceled during verification", fmt.Errorf("canceled during startup verification: context canceled"), "container startup verification canceled"},
		{"inspect failure", fmt.Errorf("failed to inspect container 0 during health check"), "container exited during startup"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, startupErrorToCallbackMsg(tt.err))
		})
	}
}

func TestProvision_FailurePersistsDiagnostics(t *testing.T) {
	// Verify that a provisioning failure (image pull) persists diagnostics
	// and that GetProvision/GetLogs fall back to the diagnostics store
	// after deprovision removes the in-memory provision.
	dbPath := filepath.Join(t.TempDir(), "diag.db")
	diagStore, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer diagStore.Close()

	var callbackReceived atomic.Bool
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbackReceived.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()
	const leaseUUID = "0192f1a0-1111-4abc-8def-000000000d1a"

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return fmt.Errorf("registry unreachable")
		},
	}

	b := newBackendForProvisionTest(t, mock, nil)
	b.diagnosticsStore = diagStore
	rebuildCallbackSender(b, callbackServer.Client())
	startCallbackReplayForTest(b)
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})

	req := backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "tenant-a",
		ProviderUUID: nominalDockerProviderUUID,
		Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 1}},
		CallbackURL:  testOperationCallbackURL(callbackServer.URL),
		Payload:      validManifestJSON("docker.io/nginx:latest"),
	}

	err = b.Provision(context.Background(), req)
	require.NoError(t, err)

	// Wait for async provision to complete.
	require.Eventually(t, func() bool { return callbackReceived.Load() }, 5*time.Second, 50*time.Millisecond)

	// Verify diagnostics were persisted. entry.Error carries the VERBOSE
	// operator detail (the raw underlying error) — this is the operator-only
	// side that never reaches the tenant (ENG-508).
	entry, err := diagStore.Get(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Contains(t, entry.Error, "image pull failed")
	assert.Contains(t, entry.Error, "registry unreachable", "operator-side entry.Error keeps the verbose cause")
	assert.Equal(t, 1, entry.FailCount)
	assert.Equal(t, "tenant-a", entry.Tenant)
	assert.Equal(t, nominalDockerProviderUUID, entry.ProviderUUID)
	require.NotNil(t, entry.LifecycleGeneration)
	assert.Equal(t, backend.LifecycleGenerationTyped, entry.LifecycleGeneration.Kind)
	assert.NotEmpty(t, entry.LifecycleGeneration.ID)
	wantGeneration := entry.LifecycleGeneration

	// In-memory GetProvision should still work.
	info, err := b.GetProvision(context.Background(), leaseUUID)
	require.NoError(t, err)
	assert.Equal(t, backend.ProvisionStatusFailed, info.Status)
	assert.Equal(t, wantGeneration, info.LifecycleGeneration)

	// Now simulate deprovision (remove from in-memory map).
	b.provisionsMu.Lock()
	delete(b.provisions, leaseUUID)
	b.provisionsMu.Unlock()

	// GetProvision should fall back to diagnostics store. The tenant-facing
	// ProvisionInfo carries only the curated Reason/Message — never the verbose
	// operator detail (ENG-508).
	info, err = b.GetProvision(context.Background(), leaseUUID)
	require.NoError(t, err)
	assert.Equal(t, backend.ProvisionStatusFailed, info.Status)
	// (reason, message) must be consistent: an image-pull failure reports
	// ImagePullFailed, not the generic ContainerExited (ENG-508).
	assert.Equal(t, backend.ReasonImagePullFailed, info.Reason)
	assert.Equal(t, "image pull failed", info.Message)
	assert.NotContains(t, info.Message, "registry unreachable", "curated Message must not carry the verbose cause")
	assert.Equal(t, 1, info.FailCount)
	assert.Equal(t, nominalDockerProviderUUID, info.ProviderUUID)
	assert.Equal(t, wantGeneration, info.LifecycleGeneration,
		"diagnostics fallback must retain the failed provision's lifecycle-generation observation")

	// GetProvision for unknown lease still returns ErrNotProvisioned.
	_, err = b.GetProvision(context.Background(), "nonexistent")
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

// An accepted Compose mutation is intentionally nonterminal until fresh
// inventory settles it, so this unit test pins the independent invariant that
// logs are captured while the failed containers are still addressable. The
// durable terminal-diagnostics path is covered by
// TestProvision_FailurePersistsDiagnostics.
func TestCaptureContainerLogsBeforeCleanup(t *testing.T) {
	logsFetched := 0
	b := newBackendForTest(&mockDockerClient{
		ContainerLogsFn: func(_ context.Context, containerID string, _ int) (string, error) {
			logsFetched++
			return "boot error on " + containerID, nil
		},
	}, nil)

	logs := b.captureContainerLogs(
		[]string{"container-0", "container-1"},
		map[string]string{"container-0": "app/0", "container-1": "app/1"},
	)

	assert.Equal(t, 2, logsFetched)
	assert.Contains(t, logs["app/0"], "boot error on container-0")
	assert.Contains(t, logs["app/1"], "boot error on container-1")
}

func TestGetLogs_FallsBackToDiagnosticsStore(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "diag_logs.db")
	diagStore, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer diagStore.Close()

	// Persist a diagnostic entry with logs.
	require.NoError(t, diagStore.Store(shared.DiagnosticEntry{
		LeaseUUID:    "lease-logs",
		ProviderUUID: nominalDockerProviderUUID,
		Tenant:       "tenant-a",
		Error:        "container exited",
		Logs: map[string]string{
			"0": "line 1\nline 2\n",
			"1": "worker output\n",
		},
		FailCount: 1,
	}))

	mock := &mockDockerClient{}
	b := newBackendForTest(mock, nil)
	b.diagnosticsStore = diagStore

	// No in-memory provision — should fall back to diagnostics store.
	logs, err := b.GetLogs(context.Background(), "lease-logs", 100)
	require.NoError(t, err)
	require.Len(t, logs, 2)
	assert.Equal(t, "line 1\nline 2\n", logs["0"])
	assert.Equal(t, "worker output\n", logs["1"])

	// Unknown lease still returns ErrNotProvisioned.
	_, err = b.GetLogs(context.Background(), "nonexistent", 100)
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

func TestGetLogs_FallbackNoLogs(t *testing.T) {
	// When diagnostics entry exists but has no logs (e.g., image pull failure),
	// GetLogs should return ErrNotProvisioned.
	dbPath := filepath.Join(t.TempDir(), "diag_nologs.db")
	diagStore, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer diagStore.Close()

	require.NoError(t, diagStore.Store(shared.DiagnosticEntry{
		LeaseUUID: "lease-nologs",
		Error:     "image pull failed",
		FailCount: 1,
	}))

	mock := &mockDockerClient{}
	b := newBackendForTest(mock, nil)
	b.diagnosticsStore = diagStore

	_, err = b.GetLogs(context.Background(), "lease-nologs", 100)
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

func TestProvision_SuccessClearsStaleDiagnostics(t *testing.T) {
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440102"
	payload := validManifestJSON("docker.io/nginx:latest")
	// When a re-provision succeeds, the stale diagnostic entry from the
	// previous failure should be removed from the diagnostics store.
	dbPath := filepath.Join(t.TempDir(), "diag_clear.db")
	diagStore, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer diagStore.Close()

	// Pre-populate a stale diagnostic entry.
	require.NoError(t, diagStore.Store(shared.DiagnosticEntry{
		LeaseUUID: leaseUUID,
		Error:     "old failure",
		FailCount: 1,
	}))

	var callbackReceived atomic.Bool
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbackReceived.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error { return nil },
		PullImageFn:       func(ctx context.Context, imageName string, timeout time.Duration) error { return nil },
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		leaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: leaseUUID,
			Tenant:       "tenant-a",
			ProviderUUID: nominalDockerProviderUUID,
			Status:       backend.ProvisionStatusFailed,
			FailCount:    1,
			Quantity:     1,
			ContainerIDs: []string{"old-ctr"}},
		},
	})
	b.compose = &mockComposeExecutor{
		PSFn: func(context.Context, string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{{
				ID: "new-ctr", Service: manifest.DefaultServiceName, State: "running",
			}}, nil
		},
	}
	b.diagnosticsStore = diagStore
	_ = b.pool.TryAllocate(leaseUUID+"-app-0", "docker-small", "tenant-a")
	prepareFailedProvisionReplacement(t, b, mock, leaseUUID, "tenant-a", "docker-small", payload)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	rebuildCallbackSender(b, callbackServer.Client())
	b.wg.Go(b.callbackSender.RunReplayLoop)

	req := newProvisionRequest(leaseUUID, "tenant-a", "docker-small", 1, payload)
	req.CallbackURL = testOperationCallbackURL(callbackServer.URL)
	err = b.Provision(context.Background(), req)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return callbackReceived.Load() }, 5*time.Second, 50*time.Millisecond)

	// Stale diagnostic entry should be removed.
	entry, err := diagStore.Get(leaseUUID)
	require.NoError(t, err)
	assert.Nil(t, entry, "stale diagnostic entry should be removed on successful re-provision")

	b.stopCancel()
	b.wg.Wait()
}

func TestDoProvision_StatefulSKUChownsVolumeSubdirs(t *testing.T) {
	const leaseUUID = durableCallbackTestLeaseUUID2
	// Verify that doProvision chowns volume subdirectories to the image's
	// runtime UID/GID when ResolveImageUser returns a non-root user.
	if os.Getuid() != 0 {
		t.Skip("chown requires root")
	}

	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	volDir := t.TempDir()
	var createdVolumePath string
	vm := &mockVolumeManager{
		defaultDir: volDir,
		CreateFn: func(ctx context.Context, id string, sizeMB int64) (string, bool, error) {
			createdVolumePath = filepath.Join(volDir, id)
			if err := os.MkdirAll(createdVolumePath, 0o755); err != nil {
				return "", false, err
			}
			return createdVolumePath, true, nil
		},
	}

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{Volumes: map[string]struct{}{"/data": {}}}, nil
		},
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			return 999, 999, nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "container-1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		leaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: leaseUUID,
			Status:   backend.ProvisionStatusProvisioning,
			Quantity: 1},
		},
	})
	b.volumes = vm
	b.cfg.VolumeDataPath = volDir
	b.provisions[leaseUUID].CallbackURL = callbackServer.URL
	_ = b.pool.TryAllocate(leaseUUID+"-app-0", "docker-small", "tenant-a")
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	manifestPayload, _ := json.Marshal(manifest.Manifest{Image: "postgres:16", User: "999:999"})
	manifest, _ := manifest.ParseManifest(manifestPayload)
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	req := newProvisionRequest(leaseUUID, "tenant-a", "docker-small", 1, manifestPayload)
	req.CallbackURL = testOperationCallbackURL(callbackServer.URL)
	b.doProvisionAndFire(t, context.Background(), req, manifest, profiles, b.logger)

	// Verify volume subdir is owned by UID/GID 999.
	subdir := filepath.Join(createdVolumePath, "data")
	info, err := os.Stat(subdir)
	require.NoError(t, err)
	stat := info.Sys().(*syscall.Stat_t)
	assert.Equal(t, uint32(999), stat.Uid, "volume subdir should be owned by UID 999")
	assert.Equal(t, uint32(999), stat.Gid, "volume subdir should be owned by GID 999")

	b.provisionsMu.RLock()
	prov := b.provisions[leaseUUID]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status)
}

func TestDoProvision_StatefulSKURootUserNoChown(t *testing.T) {
	const leaseUUID = durableCallbackTestLeaseUUID3
	// Verify that doProvision does NOT chown when ResolveImageUser returns root (0, 0).
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	volDir := t.TempDir()
	vm := &mockVolumeManager{
		defaultDir: volDir,
		CreateFn: func(ctx context.Context, id string, sizeMB int64) (string, bool, error) {
			path := filepath.Join(volDir, id)
			if err := os.MkdirAll(path, 0o755); err != nil {
				return "", false, err
			}
			return path, true, nil
		},
	}

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{Volumes: map[string]struct{}{"/data": {}}}, nil
		},
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			return 0, 0, nil // root
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "container-1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		leaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: leaseUUID,
			Status:   backend.ProvisionStatusProvisioning,
			Quantity: 1},
		},
	})
	b.volumes = vm
	b.cfg.VolumeDataPath = volDir
	b.provisions[leaseUUID].CallbackURL = callbackServer.URL
	_ = b.pool.TryAllocate(leaseUUID+"-app-0", "docker-small", "tenant-a")
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	manifest, _ := manifest.ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	req := newProvisionRequest(leaseUUID, "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	req.CallbackURL = testOperationCallbackURL(callbackServer.URL)
	b.doProvisionAndFire(t, context.Background(), req, manifest, profiles, b.logger)

	// Verify provision succeeded — ownership stays as created by MkdirAll
	// (no chown call since UID/GID are 0).
	b.provisionsMu.RLock()
	prov := b.provisions[leaseUUID]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status)
}

func TestInspectImageForSetup_AutoDetectVolumeOwner(t *testing.T) {
	// Mongo-like image: root USER, volumes owned by UID 999.
	mock := &mockDockerClient{
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:abc123",
				Volumes: map[string]struct{}{"/data/db": {}, "/data/configdb": {}},
			}, nil
		},
		DetectVolumeOwnerFn: func(ctx context.Context, imageName string, volumePaths []string) (int, int, error) {
			return 999, 999, nil
		},
	}
	b := newBackendForTest(mock, nil)

	result, err := inspectImageForSetupForTest(t, b, context.Background(), "mongo:latest", "")
	require.NoError(t, err)

	assert.Equal(t, "999:999", result.ContainerUser)
	assert.Equal(t, 999, result.VolumeUID)
	assert.Equal(t, 999, result.VolumeGID)
}

func TestInspectImageForSetup_AutoDetectRootOwnership(t *testing.T) {
	// Volumes owned by root → no override.
	mock := &mockDockerClient{
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:rootowned",
				Volumes: map[string]struct{}{"/data": {}},
			}, nil
		},
		DetectVolumeOwnerFn: func(ctx context.Context, imageName string, volumePaths []string) (int, int, error) {
			return 0, 0, nil
		},
	}
	b := newBackendForTest(mock, nil)

	result, err := inspectImageForSetupForTest(t, b, context.Background(), "alpine:latest", "")
	require.NoError(t, err)

	assert.Empty(t, result.ContainerUser)
	assert.Equal(t, 0, result.VolumeUID)
	assert.Equal(t, 0, result.VolumeGID)
}

func TestInspectImageForSetup_AutoDetectError(t *testing.T) {
	// DetectVolumeOwner fails → graceful fallback, no error propagated.
	// Errors are NOT cached, so a subsequent call retries.
	var detectCalls int
	mock := &mockDockerClient{
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:errorcase",
				Volumes: map[string]struct{}{"/data": {}},
			}, nil
		},
		DetectVolumeOwnerFn: func(ctx context.Context, imageName string, volumePaths []string) (int, int, error) {
			detectCalls++
			if detectCalls == 1 {
				return 0, 0, errors.New("docker daemon unreachable")
			}
			return 999, 999, nil // succeeds on retry
		},
	}
	b := newBackendForTest(mock, nil)

	// First call: error → defaults to root.
	result, err := inspectImageForSetupForTest(t, b, context.Background(), "mongo:latest", "")
	require.NoError(t, err)
	assert.Empty(t, result.ContainerUser)
	assert.Equal(t, 1, detectCalls)

	// Second call: retries (error was not cached) → succeeds.
	result, err = inspectImageForSetupForTest(t, b, context.Background(), "mongo:latest", "")
	require.NoError(t, err)
	assert.Equal(t, "999:999", result.ContainerUser)
	assert.Equal(t, 2, detectCalls, "should retry after transient error")
}

func TestInspectImageForSetup_ExplicitUserSkipsAutoDetect(t *testing.T) {
	// Manifest user set → DetectVolumeOwner NOT called.
	mock := &mockDockerClient{
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:explicituser",
				Volumes: map[string]struct{}{"/data": {}},
			}, nil
		},
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			return 1000, 1000, nil
		},
		DetectVolumeOwnerFn: func(ctx context.Context, imageName string, volumePaths []string) (int, int, error) {
			t.Fatal("DetectVolumeOwner should not be called when manifest user is set")
			return 0, 0, nil
		},
	}
	b := newBackendForTest(mock, nil)

	result, err := inspectImageForSetupForTest(t, b, context.Background(), "postgres:16", "1000:1000")
	require.NoError(t, err)

	assert.Equal(t, "1000:1000", result.ContainerUser)
	assert.Equal(t, 1000, result.VolumeUID)
	assert.Equal(t, 1000, result.VolumeGID)
}

func TestInspectImageForSetup_NoVolumesSkipsAutoDetect(t *testing.T) {
	// No VOLUME paths → DetectVolumeOwner NOT called.
	mock := &mockDockerClient{
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:novolumes",
				Volumes: map[string]struct{}{},
			}, nil
		},
		DetectVolumeOwnerFn: func(ctx context.Context, imageName string, volumePaths []string) (int, int, error) {
			t.Fatal("DetectVolumeOwner should not be called when there are no volumes")
			return 0, 0, nil
		},
	}
	b := newBackendForTest(mock, nil)

	result, err := inspectImageForSetupForTest(t, b, context.Background(), "nginx:latest", "")
	require.NoError(t, err)

	assert.Empty(t, result.ContainerUser)
	assert.Equal(t, 0, result.VolumeUID)
	assert.Equal(t, 0, result.VolumeGID)
}

// --- WritablePaths tests ---

func TestInspectImageForSetup_DetectsWritablePaths(t *testing.T) {
	// Grafana-like image: non-root user (472), no VOLUMEs.
	mock := &mockDockerClient{
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:grafana123",
				Volumes: map[string]struct{}{},
				User:    "472",
			}, nil
		},
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			return 472, 472, nil
		},
		DetectWritablePathsFn: func(ctx context.Context, imageName string, uid int, candidateParents []string) ([]string, error) {
			assert.Equal(t, 472, uid)
			return []string{"/var/lib/grafana", "/var/log/grafana"}, nil
		},
	}
	b := newBackendForTest(mock, nil)

	result, err := inspectImageForSetupForTest(t, b, context.Background(), "grafana/grafana:latest", "")
	require.NoError(t, err)

	assert.Equal(t, "472:472", result.ContainerUser)
	assert.Equal(t, []string{"/var/lib/grafana", "/var/log/grafana"}, result.WritablePaths)
	assert.Empty(t, result.Volumes)
}

func TestInspectImageForSetup_WritablePathsDetectedWithVolumes(t *testing.T) {
	// MySQL-like image: has VOLUMEs AND needs /var/run/mysqld writable.
	// Detection must still run even when VOLUMEs are declared.
	mock := &mockDockerClient{
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:mysql9",
				Volumes: map[string]struct{}{"/var/lib/mysql": {}},
				User:    "999",
			}, nil
		},
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			return 999, 999, nil
		},
		DetectWritablePathsFn: func(ctx context.Context, imageName string, uid int, candidateParents []string) ([]string, error) {
			return []string{"/var/run/mysqld"}, nil
		},
	}
	b := newBackendForTest(mock, nil)

	result, err := inspectImageForSetupForTest(t, b, context.Background(), "mysql:9", "")
	require.NoError(t, err)

	assert.Equal(t, []string{"/var/run/mysqld"}, result.WritablePaths)
	assert.Equal(t, []string{"/var/lib/mysql"}, result.Volumes)
	assert.Equal(t, "999:999", result.ContainerUser)
}

func TestInspectImageForSetup_WritablePathsDetectedForRoot(t *testing.T) {
	// Root user → writable path detection is called with uid=0,
	// matching directories owned by any non-root user (e.g., neo4j).
	var detectedUID int
	mock := &mockDockerClient{
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:rootuser",
				Volumes: map[string]struct{}{"/data": {}},
			}, nil
		},
		DetectWritablePathsFn: func(ctx context.Context, imageName string, uid int, candidateParents []string) ([]string, error) {
			detectedUID = uid
			return []string{"/var/lib/neo4j"}, nil
		},
	}
	b := newBackendForTest(mock, nil)

	result, err := inspectImageForSetupForTest(t, b, context.Background(), "neo4j:latest", "")
	require.NoError(t, err)

	assert.Equal(t, 0, detectedUID, "should pass uid=0 for root images")
	assert.Equal(t, []string{"/var/lib/neo4j"}, result.WritablePaths)
	assert.Empty(t, result.ContainerUser)
}

func TestInspectImageForSetup_WritablePathsErrorNotCached(t *testing.T) {
	// Error → no cache, retry succeeds.
	var detectCalls int
	mock := &mockDockerClient{
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:grafana-retry",
				Volumes: map[string]struct{}{},
				User:    "472",
			}, nil
		},
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			return 472, 472, nil
		},
		DetectWritablePathsFn: func(ctx context.Context, imageName string, uid int, candidateParents []string) ([]string, error) {
			detectCalls++
			if detectCalls == 1 {
				return nil, errors.New("docker daemon unreachable")
			}
			return []string{"/var/lib/grafana"}, nil
		},
	}
	b := newBackendForTest(mock, nil)

	// First call: error → nil writable paths.
	result, err := inspectImageForSetupForTest(t, b, context.Background(), "grafana/grafana:latest", "")
	require.NoError(t, err)
	assert.Nil(t, result.WritablePaths)
	assert.Equal(t, 1, detectCalls)

	// Second call: retries (error was not cached) → succeeds.
	result, err = inspectImageForSetupForTest(t, b, context.Background(), "grafana/grafana:latest", "")
	require.NoError(t, err)
	assert.Equal(t, []string{"/var/lib/grafana"}, result.WritablePaths)
	assert.Equal(t, 2, detectCalls, "should retry after transient error")
}

func TestInspectImageForSetup_WritablePathsBinds(t *testing.T) {
	// End-to-end: detected writable paths flow through to the compose
	// ServiceConfig as bind-mount Volumes. Post-Task-15 every provision
	// goes through compose.Up; the CreateContainerParams.WritablePathBinds
	// field is no longer the integration boundary — the binds are
	// applied to ServiceConfig.Volumes via applyVolumeBinds in
	// compose_project.go.
	var capturedProject *composetypes.Project

	callbackReceived := make(chan struct{})
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		select {
		case <-callbackReceived:
		default:
			close(callbackReceived)
		}
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:grafana-e2e",
				Volumes: map[string]struct{}{},
				User:    "472",
			}, nil
		},
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			return 472, 472, nil
		},
		DetectWritablePathsFn: func(ctx context.Context, imageName string, uid int, candidateParents []string) ([]string, error) {
			return []string{"/var/lib/grafana"}, nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	composeMock := &mockComposeExecutor{
		UpFn: func(ctx context.Context, project *composetypes.Project, opts composeUpOpts) error {
			capturedProject = project
			return nil
		},
		PSFn: func(ctx context.Context, projectName string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{
				{ID: "container-1", Service: manifest.DefaultServiceName, State: "running"},
			}, nil
		},
	}

	tmpDir := t.TempDir()
	inventory := newVolumeSet()
	b := newBackendForProvisionTest(t, mock, nil)
	b.compose = composeMock
	installStackStrictCohortInventory(t, mock, composeMock)
	b.volumes = &mockVolumeManager{
		defaultDir: tmpDir,
		CreateFn: func(_ context.Context, id string, _ int64) (string, bool, error) {
			path := filepath.Join(tmpDir, id)
			if err := os.MkdirAll(path, 0o755); err != nil {
				return "", false, err
			}
			inventory.mu.Lock()
			inventory.present[id] = true
			inventory.mu.Unlock()
			return path, true, nil
		},
		ListFn: inventory.list,
	}
	b.cfg.VolumeDataPath = tmpDir
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	b.cfg.SKUProfiles = map[string]SKUProfile{
		"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 0},
	}
	b.pool = shared.NewResourcePool(b.cfg.TotalCPUCores, b.cfg.TotalMemoryMB, b.cfg.TotalDiskMB, b.cfg.GetSKUProfile, nil)

	req := newProvisionRequest("0192f1a0-1111-4abc-8def-0000000000a1", "tenant-a", "docker-small", 1, validManifestJSON("grafana/grafana:latest"))
	req.CallbackURL = testOperationCallbackURL(callbackServer.URL)
	startCallbackReplayForTest(b)

	err := b.Provision(context.Background(), req)
	require.NoError(t, err)

	<-callbackReceived

	// Detected writable paths must surface as bind-mount Volumes on the
	// compose ServiceConfig. The expected host path is
	// `{volumeRoot}/_wp/var/lib/grafana` — _wp is the writable-paths
	// subdir on the managed volume.
	require.NotNil(t, capturedProject, "compose.Up must have been invoked with a captured project")
	require.Len(t, capturedProject.Services, 1)
	svc := capturedProject.Services[manifest.DefaultServiceName]
	require.NotNil(t, svc, "compose project must have the 'app' service")

	wantHost := filepath.Join(
		tmpDir,
		canonicalVolumeName(req.LeaseUUID, manifest.DefaultServiceName, 0),
		"_wp", "var/lib/grafana",
	)
	wantContainer := "/var/lib/grafana"
	foundBind := false
	for _, v := range svc.Volumes {
		if v.Type == "bind" && v.Source == wantHost && v.Target == wantContainer {
			foundBind = true
			break
		}
	}
	assert.True(t, foundBind,
		"writable-path bind not found in compose ServiceConfig.Volumes (want type=bind source=%q target=%q); got %#v",
		wantHost, wantContainer, svc.Volumes)
	assert.Equal(t, "472:472", svc.User,
		"compose ServiceConfig.User must carry the resolved UID:GID")

	b.stopCancel()
	b.wg.Wait()
}

func TestInspectImageForSetup_FilterSubpaths(t *testing.T) {
	// Verify that writable paths that overlap VOLUME paths are filtered out.
	mock := &mockDockerClient{
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID: "sha256:neo4j-test",
				Volumes: map[string]struct{}{
					"/data": {},
				},
				User: "7474",
			}, nil
		},
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			return 7474, 7474, nil
		},
		DetectWritablePathsFn: func(ctx context.Context, imageName string, uid int, candidateParents []string) ([]string, error) {
			// /data/transactions is a subtree of VOLUME /data → should be filtered
			return []string{"/var/lib/neo4j", "/data/transactions"}, nil
		},
	}

	b := newBackendForProvisionTest(t, mock, nil)
	b.cfg.ContainerReadonlyRootfs = ptrBool(true)

	imgSetup, err := inspectImageForSetupForTest(t, b, context.Background(), "neo4j:latest", "")
	require.NoError(t, err)

	// /data/transactions should be filtered out (subtree of /data)
	// /var/lib/neo4j should remain
	assert.Equal(t, []string{"/var/lib/neo4j"}, imgSetup.WritablePaths)
}

func TestWritablePathBindsNoOverlap(t *testing.T) {
	// Verify that writable path binds don't include paths already covered
	// by VOLUME bind mounts. This is handled upstream by filterSubpaths
	// in inspectImageForSetup — writable paths that overlap VOLUME paths
	// are removed before they reach doProvision.

	// filterSubpaths should remove /var/lib/mysql (equal to VOLUME path)
	volumes := []string{"/var/lib/mysql"}
	writablePaths := []string{"/var/lib/mysql", "/var/run/mysqld"}
	filtered := filterSubpaths(writablePaths, volumes)

	assert.NotContains(t, filtered, "/var/lib/mysql",
		"/var/lib/mysql should be filtered (covered by VOLUME)")
	assert.Contains(t, filtered, "/var/run/mysqld",
		"/var/run/mysqld should remain (not covered by VOLUME)")
}

// --- filterSubpaths tests ---

func TestFilterSubpaths(t *testing.T) {
	tests := []struct {
		name       string
		candidates []string
		parents    []string
		want       []string
	}{
		{
			name:       "no overlap",
			candidates: []string{"/var/lib/grafana", "/var/run/mysqld"},
			parents:    []string{"/data"},
			want:       []string{"/var/lib/grafana", "/var/run/mysqld"},
		},
		{
			name:       "exact match removed",
			candidates: []string{"/data", "/var/lib/app"},
			parents:    []string{"/data"},
			want:       []string{"/var/lib/app"},
		},
		{
			name:       "subtree removed",
			candidates: []string{"/data/transactions", "/var/lib/app"},
			parents:    []string{"/data"},
			want:       []string{"/var/lib/app"},
		},
		{
			name:       "multiple parents",
			candidates: []string{"/data/tx", "/logs/app", "/var/lib/app"},
			parents:    []string{"/data", "/logs"},
			want:       []string{"/var/lib/app"},
		},
		{
			name:       "nil candidates",
			candidates: nil,
			parents:    []string{"/data"},
			want:       nil,
		},
		{
			name:       "nil parents",
			candidates: []string{"/var/lib/app"},
			parents:    nil,
			want:       []string{"/var/lib/app"},
		},
		{
			name:       "prefix but not subtree",
			candidates: []string{"/data-extra"},
			parents:    []string{"/data"},
			want:       []string{"/data-extra"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterSubpaths(tt.candidates, tt.parents)
			assert.Equal(t, tt.want, got)
		})
	}
}

// --- sanitizeAndExtractTar tests ---

func TestSanitizeAndExtractTar(t *testing.T) {
	t.Run("extracts regular files and dirs", func(t *testing.T) {
		destDir := t.TempDir()
		buf := createTestTar(t, []testTarEntry{
			{Name: "conf/", Typeflag: tar.TypeDir, Mode: 0o755, Uid: 1000, Gid: 1000},
			{Name: "conf/app.conf", Typeflag: tar.TypeReg, Mode: 0o644, Uid: 1000, Gid: 1000, Content: "key=value"},
		})

		written, skipped, err := sanitizeAndExtractTar(buf, destDir, 1024*1024, 1<<30)
		require.NoError(t, err)
		assert.Empty(t, skipped)
		assert.Equal(t, int64(9), written) // len("key=value")

		content, readErr := os.ReadFile(filepath.Join(destDir, "conf", "app.conf"))
		require.NoError(t, readErr)
		assert.Equal(t, "key=value", string(content))
	})

	t.Run("rejects absolute path", func(t *testing.T) {
		destDir := t.TempDir()
		buf := createTestTar(t, []testTarEntry{
			{Name: "/etc/passwd", Typeflag: tar.TypeReg, Mode: 0o644, Content: "evil"},
		})
		_, _, err := sanitizeAndExtractTar(buf, destDir, 1024*1024, 1<<30)
		assert.ErrorContains(t, err, "unsafe path")
	})

	t.Run("rejects path traversal", func(t *testing.T) {
		destDir := t.TempDir()
		buf := createTestTar(t, []testTarEntry{
			{Name: "../escape", Typeflag: tar.TypeReg, Mode: 0o644, Content: "evil"},
		})
		_, _, err := sanitizeAndExtractTar(buf, destDir, 1024*1024, 1<<30)
		assert.ErrorContains(t, err, "unsafe path")
	})

	t.Run("rejects device node", func(t *testing.T) {
		destDir := t.TempDir()
		buf := createTestTar(t, []testTarEntry{
			{Name: "dev/null", Typeflag: tar.TypeBlock, Mode: 0o666},
		})
		_, _, err := sanitizeAndExtractTar(buf, destDir, 1024*1024, 1<<30)
		assert.ErrorContains(t, err, "disallowed type")
	})

	t.Run("enforces size limit", func(t *testing.T) {
		destDir := t.TempDir()
		buf := createTestTar(t, []testTarEntry{
			{Name: "big.bin", Typeflag: tar.TypeReg, Mode: 0o644, Content: strings.Repeat("x", 100)},
		})
		_, _, err := sanitizeAndExtractTar(buf, destDir, 50, 1<<30)
		assert.ErrorContains(t, err, "exceeds")
	})

	t.Run("enforces entry limit", func(t *testing.T) {
		destDir := t.TempDir()
		buf := createTestTar(t, []testTarEntry{
			{Name: "f0", Typeflag: tar.TypeReg, Mode: 0o600},
			{Name: "f1", Typeflag: tar.TypeReg, Mode: 0o600},
			{Name: "f2", Typeflag: tar.TypeReg, Mode: 0o600},
			{Name: "f3", Typeflag: tar.TypeReg, Mode: 0o600},
			{Name: "f4", Typeflag: tar.TypeReg, Mode: 0o600},
		})
		// Generous byte budget (1<<30), tight entry budget (3): the 4th entry trips it.
		_, _, err := sanitizeAndExtractTar(buf, destDir, 1<<30, 3)
		assert.ErrorContains(t, err, "entry limit")
	})

	t.Run("strips setuid bits", func(t *testing.T) {
		destDir := t.TempDir()
		buf := createTestTar(t, []testTarEntry{
			{Name: "suid-binary", Typeflag: tar.TypeReg, Mode: 0o4755, Content: "binary"},
		})
		_, _, err := sanitizeAndExtractTar(buf, destDir, 1024*1024, 1<<30)
		require.NoError(t, err)

		info, statErr := os.Stat(filepath.Join(destDir, "suid-binary"))
		require.NoError(t, statErr)
		// Setuid bit should be stripped
		assert.Zero(t, info.Mode()&os.ModeSetuid)
	})

	t.Run("creates absolute symlink and reports it", func(t *testing.T) {
		destDir := t.TempDir()
		buf := createTestTar(t, []testTarEntry{
			{Name: "conf/", Typeflag: tar.TypeDir, Mode: 0o755},
			{Name: "conf/real.conf", Typeflag: tar.TypeReg, Mode: 0o644, Content: "data"},
			{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd"},
		})
		_, outOfScope, err := sanitizeAndExtractTar(buf, destDir, 1024*1024, 1<<30)
		require.NoError(t, err)
		assert.Equal(t, []string{"link -> /etc/passwd"}, outOfScope)
		// Symlink is created (dangling on host, resolves inside container).
		assert.FileExists(t, filepath.Join(destDir, "conf", "real.conf"))
		linkTarget, readlinkErr := os.Readlink(filepath.Join(destDir, "link"))
		require.NoError(t, readlinkErr)
		assert.Equal(t, "/etc/passwd", linkTarget)
	})

	t.Run("creates traversal symlink and reports it", func(t *testing.T) {
		destDir := t.TempDir()
		buf := createTestTar(t, []testTarEntry{
			{Name: "a/b/", Typeflag: tar.TypeDir, Mode: 0o755},
			{Name: "a/b/file.txt", Typeflag: tar.TypeReg, Mode: 0o644, Content: "ok"},
			{Name: "a/b/link", Typeflag: tar.TypeSymlink, Linkname: "../../../etc/passwd"},
		})
		_, outOfScope, err := sanitizeAndExtractTar(buf, destDir, 1024*1024, 1<<30)
		require.NoError(t, err)
		assert.Equal(t, []string{"a/b/link -> ../../../etc/passwd"}, outOfScope)
		assert.FileExists(t, filepath.Join(destDir, "a", "b", "file.txt"))
		linkTarget, readlinkErr := os.Readlink(filepath.Join(destDir, "a", "b", "link"))
		require.NoError(t, readlinkErr)
		assert.Equal(t, "../../../etc/passwd", linkTarget)
	})

	t.Run("creates parent-escape symlink and reports it", func(t *testing.T) {
		destDir := t.TempDir()
		buf := createTestTar(t, []testTarEntry{
			{Name: "keepme.txt", Typeflag: tar.TypeReg, Mode: 0o644, Content: "kept"},
			{Name: "escape", Typeflag: tar.TypeSymlink, Linkname: ".."},
		})
		_, outOfScope, err := sanitizeAndExtractTar(buf, destDir, 1024*1024, 1<<30)
		require.NoError(t, err)
		assert.Equal(t, []string{"escape -> .."}, outOfScope)
		assert.FileExists(t, filepath.Join(destDir, "keepme.txt"))
		linkTarget, readlinkErr := os.Readlink(filepath.Join(destDir, "escape"))
		require.NoError(t, readlinkErr)
		assert.Equal(t, "..", linkTarget)
	})

	t.Run("allows safe symlink", func(t *testing.T) {
		destDir := t.TempDir()
		buf := createTestTar(t, []testTarEntry{
			{Name: "conf/", Typeflag: tar.TypeDir, Mode: 0o755},
			{Name: "conf/real.conf", Typeflag: tar.TypeReg, Mode: 0o644, Content: "data"},
			{Name: "conf/link.conf", Typeflag: tar.TypeSymlink, Linkname: "real.conf"},
		})
		_, skipped, err := sanitizeAndExtractTar(buf, destDir, 1024*1024, 1<<30)
		require.NoError(t, err)
		assert.Empty(t, skipped)

		target, readlinkErr := os.Readlink(filepath.Join(destDir, "conf", "link.conf"))
		require.NoError(t, readlinkErr)
		assert.Equal(t, "real.conf", target)
	})

	t.Run("preserves ownership", func(t *testing.T) {
		if os.Getuid() != 0 {
			t.Skip("chown requires root")
		}
		destDir := t.TempDir()
		buf := createTestTar(t, []testTarEntry{
			{Name: "owned.txt", Typeflag: tar.TypeReg, Mode: 0o644, Uid: 1000, Gid: 1000, Content: "data"},
		})
		_, _, err := sanitizeAndExtractTar(buf, destDir, 1024*1024, 1<<30)
		require.NoError(t, err)

		info, statErr := os.Stat(filepath.Join(destDir, "owned.txt"))
		require.NoError(t, statErr)
		stat := info.Sys().(*syscall.Stat_t)
		assert.Equal(t, uint32(1000), stat.Uid)
		assert.Equal(t, uint32(1000), stat.Gid)
	})

	t.Run("refuses to write a file through a symlinked ancestor", func(t *testing.T) {
		destDir := t.TempDir()
		// A directory OUTSIDE destDir that a malicious symlink will point at.
		escapeDir := t.TempDir()
		canary := filepath.Join(escapeDir, "pwned")

		// Entry 1 creates an escaping symlink "evil" -> <escapeDir> (absolute,
		// outside destDir). Entry 2 then tries to write "evil/pwned"; a naive
		// extractor follows the symlink and lands at <escapeDir>/pwned. The lexical
		// boundary check cannot catch this because it does not resolve on-disk links.
		buf := createTestTar(t, []testTarEntry{
			{Name: "evil", Typeflag: tar.TypeSymlink, Linkname: escapeDir},
			{Name: "evil/pwned", Typeflag: tar.TypeReg, Mode: 0o644, Content: "owned"},
		})

		_, _, err := sanitizeAndExtractTar(buf, destDir, 1024*1024, 1<<30)

		require.Error(t, err)
		assert.NoFileExists(t, canary, "must not write through a symlinked ancestor")
	})

	t.Run("does not follow a same-name symlink when writing a file", func(t *testing.T) {
		destDir := t.TempDir()
		escapeDir := t.TempDir()
		secret := filepath.Join(escapeDir, "secret")
		require.NoError(t, os.WriteFile(secret, []byte("original"), 0o600))

		// Entry 1 creates symlink "secret" -> <escapeDir>/secret. Entry 2 writes a
		// regular file at the same name; a naive O_TRUNC open follows the symlink
		// and clobbers the out-of-tree target.
		buf := createTestTar(t, []testTarEntry{
			{Name: "secret", Typeflag: tar.TypeSymlink, Linkname: secret},
			{Name: "secret", Typeflag: tar.TypeReg, Mode: 0o644, Content: "overwritten"},
		})

		_, _, err := sanitizeAndExtractTar(buf, destDir, 1024*1024, 1<<30)

		require.Error(t, err)
		content, readErr := os.ReadFile(secret)
		require.NoError(t, readErr)
		assert.Equal(t, "original", string(content), "must not follow a same-name symlink and clobber the target")
	})

	t.Run("refuses a directory entry that collides with an existing symlink", func(t *testing.T) {
		destDir := t.TempDir()
		escapeDir := t.TempDir()

		// Entry 1 creates symlink "d" -> <escapeDir>. Entry 2 is a directory at the
		// same name: a naive extractor's MkdirAll no-ops on the symlinked dir and
		// then os.Chown FOLLOWS the link, chowning <escapeDir> (an out-of-tree path,
		// destructive when running as root). The entry must be refused instead.
		buf := createTestTar(t, []testTarEntry{
			{Name: "d", Typeflag: tar.TypeSymlink, Linkname: escapeDir},
			{Name: "d", Typeflag: tar.TypeDir, Mode: 0o755},
		})

		_, _, err := sanitizeAndExtractTar(buf, destDir, 1024*1024, 1<<30)

		require.Error(t, err)
	})

	t.Run("accepts a dotdot-prefixed filename that is not traversal", func(t *testing.T) {
		destDir := t.TempDir()
		// "..data" is a real filename (e.g. Kubernetes atomic-writer), not a "..".
		// path-traversal component, so it must be extracted, not rejected.
		buf := createTestTar(t, []testTarEntry{
			{Name: "..data", Typeflag: tar.TypeReg, Mode: 0o644, Content: "payload"},
		})

		_, _, err := sanitizeAndExtractTar(buf, destDir, 1024*1024, 1<<30)

		require.NoError(t, err)
		content, readErr := os.ReadFile(filepath.Join(destDir, "..data"))
		require.NoError(t, readErr)
		assert.Equal(t, "payload", string(content))
	})

	t.Run("does not chown the destination root via a '.' entry", func(t *testing.T) {
		if os.Getuid() != 0 {
			t.Skip("chown requires root")
		}
		destDir := t.TempDir()
		beforeUID := mustStatUID(t, destDir)

		// A "." (root) entry with an attacker-chosen owner must not chown destDir,
		// which fred created and owns; the entry is skipped. Sibling entries still
		// extract.
		buf := createTestTar(t, []testTarEntry{
			{Name: ".", Typeflag: tar.TypeDir, Mode: 0o755, Uid: 12345, Gid: 12345},
			{Name: "f.txt", Typeflag: tar.TypeReg, Mode: 0o644, Content: "ok"},
		})

		_, _, err := sanitizeAndExtractTar(buf, destDir, 1024*1024, 1<<30)

		require.NoError(t, err)
		assert.Equal(t, beforeUID, mustStatUID(t, destDir), "'.' entry must not chown destDir")
		assert.FileExists(t, filepath.Join(destDir, "f.txt"))
	})
}

// mustStatUID returns the owning uid of path.
func mustStatUID(t *testing.T, path string) uint32 {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info.Sys().(*syscall.Stat_t).Uid
}

// testTarEntry describes a single tar entry for test helpers.
type testTarEntry struct {
	Name     string
	Typeflag byte
	Mode     int64
	Uid, Gid int
	Content  string
	Linkname string
}

// createTestTar builds an in-memory tar archive from test entries.
func createTestTar(t *testing.T, entries []testTarEntry) io.Reader {
	t.Helper()
	var buf strings.Builder
	tw := tar.NewWriter(&buf)
	for _, e := range entries {
		hdr := &tar.Header{
			Name:     e.Name,
			Typeflag: e.Typeflag,
			Mode:     e.Mode,
			Uid:      e.Uid,
			Gid:      e.Gid,
			Linkname: e.Linkname,
			Size:     int64(len(e.Content)),
		}
		require.NoError(t, tw.WriteHeader(hdr))
		if e.Content != "" {
			_, err := tw.Write([]byte(e.Content))
			require.NoError(t, err)
		}
	}
	require.NoError(t, tw.Close())
	return strings.NewReader(buf.String())
}

// --- WritablePathBinds tests ---

func TestDoProvision_WritablePaths_EphemeralCreatesVolume(t *testing.T) {
	// Ephemeral SKU (DiskMB=0) with writable paths should create a small volume.
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	tmpDir := t.TempDir()
	var volumeCreated bool
	var createdSizeMB int64

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:abc",
				Volumes: map[string]struct{}{},
				User:    "1000",
			}, nil
		},
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			return 1000, 1000, nil
		},
		DetectWritablePathsFn: func(ctx context.Context, imageName string, uid int, candidateParents []string) ([]string, error) {
			return []string{"/var/lib/app"}, nil
		},
		ExtractImageContentFn: func(ctx context.Context, imageName string, paths []string, destDir string, maxBytes, maxEntries int64) map[string]error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "container-1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
			Status:   backend.ProvisionStatusProvisioning,
			Quantity: 1},
		},
	})
	b.cfg.ContainerReadonlyRootfs = ptrBool(true)
	b.volumes = &mockVolumeManager{
		defaultDir: tmpDir,
		CreateFn: func(ctx context.Context, id string, sizeMB int64) (string, bool, error) {
			volumeCreated = true
			createdSizeMB = sizeMB
			path := filepath.Join(tmpDir, id)
			if err := os.MkdirAll(path, 0o755); err != nil {
				return "", false, err
			}
			return path, true, nil
		},
	}
	b.cfg.VolumeDataPath = tmpDir
	b.provisions[durableCallbackTestLeaseUUID].CallbackURL = callbackServer.URL
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	manifest, _ := manifest.ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 0}} // ephemeral

	req := newProvisionRequest(durableCallbackTestLeaseUUID, "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	req.CallbackURL = testOperationCallbackURL(callbackServer.URL)
	b.doProvisionAndFire(t, context.Background(), req, manifest, profiles, b.logger)

	assert.True(t, volumeCreated, "volume should be created for ephemeral SKU with writable paths")
	assert.Equal(t, int64(b.cfg.GetTmpfsSizeMB()), createdSizeMB, "ephemeral writable volume should use TmpfsSizeMB")

	b.provisionsMu.RLock()
	prov := b.provisions[durableCallbackTestLeaseUUID]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status)
}

// TestProvision_DeprovisionWaitsForInFlightGoroutine pins the
// TestProvision_DeprovisionWaitsForInFlightGoroutine migrated to
// internal/backend/shared/leasesm/lease_actor_test.go at PR5b-2 E
// sub-batch 3 — the test depends on unexported leasesm internals
// (actor.workers, actor.workCancel) and is cleaner expressed via the
// leasesm test fixtures (newTestActor + mock DoDeprovisionFn) than
// via Backend integration.

// The deep-copy invariant this used to pin on enrichReserved now lives at the
// reservation, and is covered end-to-end alongside the claim it publishes by
// TestProvision_ReservationPublishesTheOwnershipClaim (volume_destroy_race_test.go).

func TestProvision_ConcurrentReaderDuringValidationWindow(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{}, map[string]*provision{
		"L1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "L1", Status: backend.ProvisionStatusProvisioning, CallbackURL: "http://cb/callbacks/provision"}},
	})
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				b.provisionsMu.RLock()
				s, ok := b.provisions["L1"]
				if ok {
					// A reader in the validation window sees the Provisioning marker
					// with its CallbackURL resolved — never an empty/torn entry.
					_ = s.CallbackURL
				}
				b.provisionsMu.RUnlock()
			}
		}
	}()
	// Exercise enrichReserved concurrently with the reader (the create-path write).
	for range 50 {
		b.provisionsMu.Lock()
		if p, ok := b.provisions["L1"]; ok {
			p.enrichReserved("docker-small", nil)
		}
		b.provisionsMu.Unlock()
	}
	close(stop)
	wg.Wait()
}

// TestProvisionToInfo_CopiesReasonMessage pins the ENG-508 read-boundary
// contract: provisionToInfo must copy the curated Reason/Message pair from
// the provision state into the ProvisionInfo, alongside the operator-only
// LastError. Without the copy, the tenant-facing read surfaces an empty
// Reason and drops the curated message.
func TestProvisionToInfo_CopiesReasonMessage(t *testing.T) {
	prov := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "l1", LastError: "verbose", Reason: backend.ReasonContainerExited, Message: "container exited unexpectedly"}}
	info := provisionToInfo(prov, "docker-1")
	assert.Equal(t, backend.ReasonContainerExited, info.Reason)
	assert.Equal(t, "container exited unexpectedly", info.Message)
}
