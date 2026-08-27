package restore

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

const (
	testProvider = "provider-1"
	testTarget   = "target-lease"
	testSource   = "source-lease"
	testTenant   = "tenant-1"
	testBackend  = "backend-a"
)

func TestRestoreAdmissionFailure_BackendOutsideTopologyIsServiceUnavailable(t *testing.T) {
	cause := fmt.Errorf("retained source is unreachable: %w", placement.ErrBackendNotInTopology)

	result := (&Service{}).restoreAdmissionFailure(cause)

	assert.Equal(t, OutcomeServiceUnavailable, result.Outcome)
	assert.ErrorIs(t, result.cause, placement.ErrBackendNotInTopology)
}

type targetReader struct {
	mu     sync.Mutex
	leases map[string]*billingtypes.Lease
	err    error
	hook   func(string)
	calls  int
}

func (reader *targetReader) GetLease(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
	reader.mu.Lock()
	reader.calls++
	hook := reader.hook
	err := reader.err
	lease := reader.leases[leaseUUID]
	reader.mu.Unlock()
	if hook != nil {
		hook(leaseUUID)
	}
	if err != nil || lease == nil {
		return nil, err
	}
	copy := *lease
	copy.Items = append([]billingtypes.LeaseItem(nil), lease.Items...)
	return &copy, nil
}

type fakeBackend struct {
	mu      sync.Mutex
	name    string
	err     error
	restore func(context.Context, backend.RestoreRequest) error
	request backend.RestoreRequest
	calls   int
}

func (fake *fakeBackend) Name() string { return fake.name }

func (fake *fakeBackend) Restore(ctx context.Context, request backend.RestoreRequest) error {
	fake.mu.Lock()
	fake.calls++
	fake.request = request
	restore := fake.restore
	err := fake.err
	fake.mu.Unlock()
	if restore != nil {
		return restore(ctx, request)
	}
	return err
}

func (fake *fakeBackend) setRestore(restore func(context.Context, backend.RestoreRequest) error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.restore = restore
}

func (fake *fakeBackend) callCount() int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.calls
}

func (fake *fakeBackend) lastRequest() backend.RestoreRequest {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.request
}

type backendLookup map[string]RestoreBackend

func (lookup backendLookup) ResolveRestoreBackend(name string) RestoreBackend {
	return lookup[name]
}

type registrySpy struct {
	registry         *operation.Registry
	bindFailure      bool
	beginCallFailure bool
	invalid          bool
	mu               sync.Mutex
	started          operation.Initiation
}

func (spy *registrySpy) TryClaimLeaseNow(leaseUUID string) operation.LeaseClaimResult {
	return spy.registry.TryClaimLeaseNow(leaseUUID)
}

func (spy *registrySpy) ReleaseLease(claim operation.LeaseClaim) bool {
	return spy.registry.ReleaseLease(claim)
}

func (spy *registrySpy) TryInitiateClaimed(
	claim operation.LeaseClaim,
	spec operation.TrackSpec,
) operation.InitiationResult {
	if spy.invalid {
		return operation.InitiationResult{}
	}
	result := spy.registry.TryInitiateClaimed(claim, spec)
	spy.mu.Lock()
	spy.started = result.Capability()
	spy.mu.Unlock()
	return result
}

func (spy *registrySpy) BindBackend(initiation operation.Initiation, backendName string) bool {
	if spy.bindFailure {
		return false
	}
	return spy.registry.BindBackend(initiation, backendName)
}

func (spy *registrySpy) BeginCall(initiation operation.Initiation) bool {
	if spy.beginCallFailure {
		return false
	}
	return spy.registry.BeginCall(initiation)
}

func (spy *registrySpy) Activate(initiation operation.Initiation) operation.InitiationCompletion {
	return spy.registry.Activate(initiation)
}

func (spy *registrySpy) AbortInitiation(
	initiation operation.Initiation,
) operation.InitiationCompletion {
	return spy.registry.AbortInitiation(initiation)
}

func (spy *registrySpy) initiation() operation.Initiation {
	spy.mu.Lock()
	defer spy.mu.Unlock()
	return spy.started
}

type authoritySpy struct {
	store      *placement.Store
	beginErr   error
	confirmErr error
	refuseErr  error
	abandonErr error
	invalid    bool
	mu         sync.Mutex
	begins     int
	confirms   int
	refuses    int
	abandons   int
	beginHook  func()
}

func (spy *authoritySpy) BeginRestore(
	baseline placement.AdmissionBaseline,
	sourceLeaseUUID, targetLeaseUUID string,
	id operation.OperationID,
) (placement.RestoreClaim, error) {
	spy.mu.Lock()
	spy.begins++
	err := spy.beginErr
	invalid := spy.invalid
	hook := spy.beginHook
	spy.mu.Unlock()
	if hook != nil {
		hook()
	}
	if err != nil {
		return placement.RestoreClaim{}, err
	}
	if invalid {
		return placement.RestoreClaim{}, nil
	}
	return spy.store.BeginRestore(baseline, sourceLeaseUUID, targetLeaseUUID, id)
}

func (spy *authoritySpy) CurrentAdmissionBaseline() placement.AdmissionBaseline {
	if spy.store == nil {
		return placement.AdmissionBaseline{}
	}
	return spy.store.CurrentAdmissionBaseline()
}

func (spy *authoritySpy) ConfirmRestore(claim placement.RestoreClaim) (bool, error) {
	spy.mu.Lock()
	spy.confirms++
	err := spy.confirmErr
	spy.mu.Unlock()
	if err != nil {
		return false, err
	}
	return spy.store.ConfirmRestore(claim)
}

func (spy *authoritySpy) RefuseRestore(claim placement.RestoreClaim) (bool, error) {
	spy.mu.Lock()
	spy.refuses++
	err := spy.refuseErr
	spy.mu.Unlock()
	if err != nil {
		return false, err
	}
	return spy.store.RefuseRestore(claim)
}

func (spy *authoritySpy) AbandonRestore(claim placement.RestoreClaim) (bool, error) {
	spy.mu.Lock()
	spy.abandons++
	err := spy.abandonErr
	spy.mu.Unlock()
	if err != nil {
		return false, err
	}
	return spy.store.AbandonRestore(claim)
}

func (spy *authoritySpy) counts() (begin, confirm, refuse, abandon int) {
	spy.mu.Lock()
	defer spy.mu.Unlock()
	return spy.begins, spy.confirms, spy.refuses, spy.abandons
}

type eventSink struct {
	mu     sync.Mutex
	events []backend.LeaseStatusEvent
	hook   func(backend.LeaseStatusEvent)
}

func (sink *eventSink) Publish(event backend.LeaseStatusEvent) {
	sink.mu.Lock()
	sink.events = append(sink.events, event)
	hook := sink.hook
	sink.mu.Unlock()
	if hook != nil {
		hook(event)
	}
}

func (sink *eventSink) snapshot() []backend.LeaseStatusEvent {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return append([]backend.LeaseStatusEvent(nil), sink.events...)
}

func testCallbackURL(operationID operation.OperationID) (string, error) {
	text, err := operationID.MarshalText()
	if err != nil {
		return "", err
	}
	return "https://fred.example.test/base/callbacks/provision?" +
		operation.QueryParameter + "=" + string(text), nil
}

type fixture struct {
	service   *Service
	targets   *targetReader
	backend   *fakeBackend
	backends  backendLookup
	registry  *registrySpy
	authority *authoritySpy
	store     *placement.Store
	events    *eventSink
}

func pendingLease(uuid string) *billingtypes.Lease {
	return &billingtypes.Lease{
		Uuid:         uuid,
		Tenant:       testTenant,
		ProviderUuid: testProvider,
		State:        billingtypes.LEASE_STATE_PENDING,
		Items: []billingtypes.LeaseItem{{
			SkuUuid:      "sku-1",
			Quantity:     2,
			ServiceName:  "web",
			CustomDomain: "tenant.example.test",
		}},
	}
}

func newFixture(t *testing.T, inventoryReady bool) *fixture {
	t.Helper()
	store, err := placement.NewStore(filepath.Join(t.TempDir(), "placements.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.ConfigureBackendTopology([]string{testBackend, "backend-b"}))
	projectRestoreTestPlacements(t, store, inventoryReady, map[string]string{
		testSource: testBackend,
	})
	if inventoryReady {
		require.True(t, store.CurrentAdmissionBaseline().Valid())
	}
	backendClient := &fakeBackend{name: testBackend}
	result := &fixture{
		targets: &targetReader{leases: map[string]*billingtypes.Lease{
			testTarget: pendingLease(testTarget),
		}},
		backend:   backendClient,
		backends:  backendLookup{testBackend: backendClient},
		registry:  &registrySpy{registry: operation.NewRegistry()},
		authority: &authoritySpy{store: store},
		store:     store,
		events:    &eventSink{},
	}
	result.service, err = NewService(Config{
		ProviderUUID: testProvider,
		CallbackURL:  testCallbackURL,
		Targets:      result.targets,
		Backends:     result.backends,
		Operations:   result.registry,
		Authority:    result.authority,
		Events:       result.events,
		Now:          func() time.Time { return time.Unix(123, 0).UTC() },
	})
	require.NoError(t, err)
	return result
}

func projectRestoreTestPlacements(
	t *testing.T,
	store *placement.Store,
	complete bool,
	placements map[string]string,
) placement.ProjectionResult {
	t.Helper()
	fence := store.BeginInventorySession()
	defer store.EndInventorySession(fence)
	result, err := store.ProjectInventory(fence, placement.InventoryProjection{
		Complete:   complete,
		Placements: placements,
	})
	require.NoError(t, err)
	return result
}

func validCommand() Command {
	return Command{
		TargetLeaseUUID: testTarget,
		Tenant:          testTenant,
		SourceLeaseUUID: testSource,
	}
}

func testOperationID(t *testing.T, value uint64) operation.OperationID {
	t.Helper()
	id, err := operation.ParseID(fmt.Sprintf("00000000-0000-4000-8000-%012x", value))
	require.NoError(t, err)
	return id
}

func requireSourceReusable(t *testing.T, fixture *fixture, target string) {
	t.Helper()
	claim, err := fixture.store.BeginRestore(
		fixture.store.CurrentAdmissionBaseline(),
		testSource,
		target,
		testOperationID(t, 9001),
	)
	require.NoError(t, err)
	require.True(t, claim.Valid())
	settled, err := fixture.store.AbandonRestore(claim)
	require.NoError(t, err)
	require.True(t, settled)
}

func requireLeaseClaimAvailable(t *testing.T, registry *operation.Registry, leaseUUID string) {
	t.Helper()
	result := registry.TryClaimLeaseNow(leaseUUID)
	require.True(t, result.Acquired(), "lease claim %q leaked (outcome %d)", leaseUUID, result.Outcome())
	require.True(t, registry.ReleaseLease(result.Claim()))
}

func TestNewServiceNormalizesNilCapabilities(t *testing.T) {
	t.Parallel()
	valid := Config{
		ProviderUUID: testProvider,
		CallbackURL:  testCallbackURL,
		Targets:      &targetReader{},
		Backends:     backendLookup{},
		Operations:   operation.NewRegistry(),
		Authority:    &authoritySpy{},
	}

	var (
		typedNilTargets    *targetReader
		typedNilBackends   backendLookup
		typedNilOperations *operation.Registry
		typedNilAuthority  *placement.Store
		typedNilEvents     *eventSink
	)
	tests := []struct {
		name string
		edit func(*Config)
		want string
	}{
		{name: "provider", edit: func(c *Config) { c.ProviderUUID = "" }, want: "provider UUID"},
		{name: "callback", edit: func(c *Config) { c.CallbackURL = nil }, want: "callback URL"},
		{name: "target reader", edit: func(c *Config) { c.Targets = nil }, want: "target lease reader"},
		{name: "typed nil target reader", edit: func(c *Config) { c.Targets = typedNilTargets }, want: "target lease reader"},
		{name: "backends", edit: func(c *Config) { c.Backends = nil }, want: "backend resolver"},
		{name: "typed nil backends", edit: func(c *Config) { c.Backends = typedNilBackends }, want: "backend resolver"},
		{name: "typed nil resolver func", edit: func(c *Config) { c.Backends = BackendResolverFunc(nil) }, want: "backend resolver"},
		{name: "operations", edit: func(c *Config) { c.Operations = nil }, want: "operation registry"},
		{name: "typed nil operations", edit: func(c *Config) { c.Operations = typedNilOperations }, want: "operation registry"},
		{name: "authority", edit: func(c *Config) { c.Authority = nil }, want: "placement authority"},
		{name: "typed nil authority", edit: func(c *Config) { c.Authority = typedNilAuthority }, want: "placement authority"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := valid
			test.edit(&config)
			service, err := NewService(config)
			assert.Nil(t, service)
			assert.ErrorContains(t, err, test.want)
		})
	}

	valid.Events = typedNilEvents
	service, err := NewService(valid)
	require.NoError(t, err)
	assert.Nil(t, service.events, "typed-nil optional events must normalize to nil")
}

func TestServiceRereadsAndValidatesTargetUnderBothClaims(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*fixture)
		command func() Command
		outcome Outcome
	}{
		{name: "incomplete", command: func() Command { return Command{} }, outcome: OutcomeInvalid},
		{name: "same source and target", command: func() Command {
			command := validCommand()
			command.SourceLeaseUUID = command.TargetLeaseUUID
			return command
		}, outcome: OutcomeInvalid},
		{name: "read error", prepare: func(f *fixture) { f.targets.err = errors.New("chain unavailable") }, command: validCommand, outcome: OutcomeServiceUnavailable},
		{name: "missing", prepare: func(f *fixture) { delete(f.targets.leases, testTarget) }, command: validCommand, outcome: OutcomeTargetNotPending},
		{name: "wrong UUID", prepare: func(f *fixture) { f.targets.leases[testTarget].Uuid = "other" }, command: validCommand, outcome: OutcomeTargetNotPending},
		{name: "tenant changed", prepare: func(f *fixture) { f.targets.leases[testTarget].Tenant = "other" }, command: validCommand, outcome: OutcomeInvalid},
		{name: "provider changed", prepare: func(f *fixture) { f.targets.leases[testTarget].ProviderUuid = "other" }, command: validCommand, outcome: OutcomeInvalid},
		{name: "pending became terminal", prepare: func(f *fixture) { f.targets.leases[testTarget].State = billingtypes.LEASE_STATE_CLOSED }, command: validCommand, outcome: OutcomeTargetNotPending},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t, true)
			if test.prepare != nil {
				test.prepare(fixture)
			}
			result := fixture.service.Execute(t.Context(), test.command())
			assert.Equal(t, test.outcome, result.Outcome)
			assert.Zero(t, fixture.backend.callCount())
			assert.Equal(t, placement.StateAbsent, fixture.store.Lookup(testTarget).State())
			assert.False(t, fixture.registry.registry.Contains(testTarget))
			requireLeaseClaimAvailable(t, fixture.registry.registry, testSource)
			requireLeaseClaimAvailable(t, fixture.registry.registry, testTarget)
		})
	}

	t.Run("read executes under source and target claims", func(t *testing.T) {
		fixture := newFixture(t, true)
		fixture.targets.hook = func(string) {
			assert.Equal(t, operation.LeaseClaimBusy,
				fixture.registry.registry.TryClaimLeaseNow(testSource).Outcome())
			assert.Equal(t, operation.LeaseClaimBusy,
				fixture.registry.registry.TryClaimLeaseNow(testTarget).Outcome())
		}
		result := fixture.service.Execute(t.Context(), validCommand())
		require.Equal(t, OutcomeAccepted, result.Outcome)
	})
}

func TestServiceAcceptedBindsOneIdentityAndAuthority(t *testing.T) {
	fixture := newFixture(t, true)
	fixture.authority.beginHook = func() {
		record, exists := fixture.registry.registry.Lookup(testTarget)
		require.True(t, exists)
		assert.Equal(t, operation.PhasePreparing, record.Phase)
		assert.Empty(t, record.Backend,
			"only placement authority may choose and bind the restore backend")
	}
	fixture.events.hook = func(event backend.LeaseStatusEvent) {
		if event.Status != backend.ProvisionStatusRestarting {
			return
		}
		record, exists := fixture.registry.registry.Lookup(testTarget)
		require.True(t, exists)
		assert.Equal(t, operation.PhaseCalling, record.Phase,
			"the call barrier must precede the Restarting event")
	}
	fixture.backend.setRestore(func(_ context.Context, request backend.RestoreRequest) error {
		record, exists := fixture.registry.registry.Lookup(testTarget)
		require.True(t, exists)
		assert.Equal(t, operation.PhaseCalling, record.Phase)
		assert.Equal(t, testBackend, record.Backend)
		return nil
	})

	result := fixture.service.Execute(t.Context(), validCommand())

	require.Equal(t, OutcomeAccepted, result.Outcome)
	assert.Equal(t, testBackend, result.BackendName)
	assert.Equal(t, 1, fixture.backend.callCount())
	request := fixture.backend.lastRequest()
	assert.Equal(t, testTarget, request.LeaseUUID)
	assert.Equal(t, testSource, request.FromLeaseUUID)
	assert.Equal(t, testTenant, request.Tenant)
	assert.Equal(t, testProvider, request.ProviderUUID)
	require.Equal(t, []backend.LeaseItem{{
		SKU: "sku-1", Quantity: 2, ServiceName: "web", CustomDomain: "tenant.example.test",
	}}, request.Items)
	callbackURL, err := url.Parse(request.CallbackURL)
	require.NoError(t, err)
	callbackID, present, err := operation.ParseQuery(callbackURL.Query())
	require.NoError(t, err)
	require.True(t, present)
	assert.Equal(t, fixture.registry.initiation().ID(), callbackID)
	wantLifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(request.CallbackURL, "")
	require.NoError(t, err)
	assert.Equal(t, wantLifecycleCallbackURL, request.LifecycleCallbackURL)

	target := fixture.store.Lookup(testTarget)
	assert.Equal(t, placement.StateConfirmed, target.State())
	assert.Equal(t, testBackend, target.Backend)
	record, exists := fixture.registry.registry.Lookup(testTarget)
	require.True(t, exists)
	assert.Equal(t, callbackID, record.ID)
	assert.Equal(t, operation.KindRestore, record.Kind)
	assert.Equal(t, operation.PhaseActive, record.Phase)
	begin, confirm, refuse, abandon := fixture.authority.counts()
	assert.Equal(t, 1, begin)
	assert.Equal(t, 1, confirm)
	assert.Zero(t, refuse)
	assert.Zero(t, abandon)
	events := fixture.events.snapshot()
	require.Len(t, events, 1)
	assert.Equal(t, backend.ProvisionStatusRestarting, events[0].Status)
	assert.Equal(t, time.Unix(123, 0).UTC(), events[0].Timestamp)
	requireSourceReusable(t, fixture, "probe-target")
}

func TestServiceInlineCallbackSettlementClaimOverridesLaterError(t *testing.T) {
	fixture := newFixture(t, true)
	var callbackClaim operation.SettlementClaim
	fixture.backend.setRestore(func(_ context.Context, request backend.RestoreRequest) error {
		callbackURL, err := url.Parse(request.CallbackURL)
		require.NoError(t, err)
		id, present, err := operation.ParseQuery(callbackURL.Query())
		require.NoError(t, err)
		require.True(t, present)
		claimed := fixture.registry.registry.TryClaimCallback(testTarget, id)
		require.True(t, claimed.Claimed())
		callbackClaim = claimed.Claim()
		settled, err := fixture.store.ConfirmOperation(testTarget, testBackend, id)
		require.NoError(t, err)
		require.True(t, settled)
		return backend.ErrValidation
	})

	result := fixture.service.Execute(t.Context(), validCommand())

	require.Equal(t, OutcomeAccepted, result.Outcome)
	assert.Equal(t, placement.StateConfirmed, fixture.store.Lookup(testTarget).State())
	_, confirm, refuse, abandon := fixture.authority.counts()
	assert.Zero(t, confirm)
	assert.Zero(t, refuse)
	assert.Equal(t, 1, abandon)
	record, exists := fixture.registry.registry.Lookup(testTarget)
	require.True(t, exists)
	assert.Equal(t, operation.PhaseActive, record.Phase)
	require.True(t, fixture.registry.registry.FinishSettlement(callbackClaim))
	assert.False(t, fixture.registry.registry.Contains(testTarget))
	requireSourceReusable(t, fixture, "probe-inline-settling")
}

func TestServiceTwoConcurrentTargetsOneSourceDispatchesOnce(t *testing.T) {
	fixture := newFixture(t, true)
	secondTarget := "target-lease-2"
	fixture.targets.leases[secondTarget] = pendingLease(secondTarget)
	entered := make(chan struct{})
	release := make(chan struct{})
	fixture.backend.setRestore(func(context.Context, backend.RestoreRequest) error {
		close(entered)
		<-release
		return nil
	})

	firstResult := make(chan Result, 1)
	go func() { firstResult <- fixture.service.Execute(t.Context(), validCommand()) }()
	<-entered
	secondCommand := validCommand()
	secondCommand.TargetLeaseUUID = secondTarget
	second := fixture.service.Execute(t.Context(), secondCommand)
	assert.Equal(t, OutcomeAlreadyInProgress, second.Outcome)
	assert.Equal(t, 1, fixture.backend.callCount())
	assert.Equal(t, placement.StateAbsent, fixture.store.Lookup(secondTarget).State())
	close(release)
	assert.Equal(t, OutcomeAccepted, (<-firstResult).Outcome)
}

func TestServiceClaimsAndProjectionFenceSynchronousRestore(t *testing.T) {
	fixture := newFixture(t, true)
	entered := make(chan struct{})
	release := make(chan struct{})
	fixture.backend.setRestore(func(context.Context, backend.RestoreRequest) error {
		close(entered)
		<-release
		return nil
	})
	resultCh := make(chan Result, 1)
	go func() { resultCh <- fixture.service.Execute(t.Context(), validCommand()) }()
	<-entered

	assert.Equal(t, operation.LeaseClaimBusy,
		fixture.registry.registry.TryClaimLeaseNow(testSource).Outcome())
	assert.Equal(t, operation.LeaseClaimBusy,
		fixture.registry.registry.TryClaimLeaseNow(testTarget).Outcome())
	record, exists := fixture.registry.registry.Lookup(testTarget)
	require.True(t, exists)
	assert.Equal(t, operation.SettlementBusy,
		fixture.registry.registry.TryClaimDeprovision(testTarget, record.ID).Outcome())

	fence := fixture.store.BeginInventorySession()
	projection, err := fixture.store.ProjectInventory(fence, placement.InventoryProjection{
		Placements: map[string]string{testSource: "backend-b"},
	})
	require.NoError(t, err)
	fixture.store.EndInventorySession(fence)
	assert.Contains(t, projection.Fenced, testSource)
	_, _, err = fixture.store.BeginOwnedAttempt(
		fixture.store.CurrentAdmissionBaseline(),
		fixture.store.Lookup(testSource).RecordRevision(),
		testBackend,
		testOperationID(t, 9010),
	)
	require.ErrorIs(t, err, placement.ErrRestoreSourceClaimed)

	close(release)
	assert.Equal(t, OutcomeAccepted, (<-resultCh).Outcome)
	assert.Equal(t, testBackend, fixture.store.Lookup(testSource).Backend)
	requireLeaseClaimAvailable(t, fixture.registry.registry, testSource)
}

func TestServiceTargetPlacementAdmissionFailsBeforeDispatch(t *testing.T) {
	fixture := newFixture(t, true)
	projectRestoreTestPlacements(t, fixture.store, false, map[string]string{
		testTarget: testBackend,
	})
	before := fixture.store.Lookup(testTarget)

	result := fixture.service.Execute(t.Context(), validCommand())

	assert.Equal(t, OutcomeAlreadyInProgress, result.Outcome)
	assert.ErrorIs(t, result.Cause(), placement.ErrRestoreTargetUnavailable)
	assert.Zero(t, fixture.backend.callCount())
	assert.Equal(t, before, fixture.store.Lookup(testTarget))
	assert.False(t, fixture.registry.registry.Contains(testTarget))
	requireLeaseClaimAvailable(t, fixture.registry.registry, testSource)
	requireLeaseClaimAvailable(t, fixture.registry.registry, testTarget)
}

func TestServiceRequiresCurrentAdmissionBaselineBeforeDispatch(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*fixture)
	}{
		{name: "complete inventory not yet committed"},
		{
			name: "backend topology changed after baseline",
			prepare: func(fixture *fixture) {
				fence := fixture.store.BeginInventorySession()
				_, err := fixture.store.ProjectInventory(
					fence, placement.InventoryProjection{Complete: true},
				)
				fixture.store.EndInventorySession(fence)
				require.NoError(t, err)
				require.True(t, fixture.store.CurrentAdmissionBaseline().Valid())
				require.NoError(t, fixture.store.ConfigureBackendTopology(
					[]string{testBackend, "backend-b", "backend-c"},
				))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t, false)
			if test.prepare != nil {
				test.prepare(fixture)
			}

			result := fixture.service.Execute(t.Context(), validCommand())

			assert.Equal(t, OutcomeServiceUnavailable, result.Outcome)
			assert.ErrorIs(t, result.Cause(), placement.ErrInvalidAdmissionBaseline)
			assert.Zero(t, fixture.backend.callCount())
			assert.Equal(t, placement.StateAbsent, fixture.store.Lookup(testTarget).State())
			assert.False(t, fixture.registry.registry.Contains(testTarget))
			requireLeaseClaimAvailable(t, fixture.registry.registry, testSource)
			requireLeaseClaimAvailable(t, fixture.registry.registry, testTarget)
		})
	}
}

func TestServicePreDispatchFailuresRefuseAtomicRestore(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*fixture)
	}{
		{name: "backend binding", prepare: func(f *fixture) { f.registry.bindFailure = true }},
		{name: "missing backend", prepare: func(f *fixture) { f.service.backends = backendLookup{} }},
		{name: "typed nil backend", prepare: func(f *fixture) {
			var typedNil *fakeBackend
			f.service.backends = backendLookup{testBackend: typedNil}
		}},
		{name: "backend name mismatch", prepare: func(f *fixture) {
			f.service.backends = backendLookup{testBackend: &fakeBackend{name: "backend-b"}}
		}},
		{name: "callback URL", prepare: func(f *fixture) {
			f.service.callbackURL = func(operation.OperationID) (string, error) {
				return "", errors.New("invalid callback URL")
			}
		}},
		{name: "calling phase", prepare: func(f *fixture) { f.registry.beginCallFailure = true }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t, true)
			test.prepare(fixture)
			result := fixture.service.Execute(t.Context(), validCommand())
			assert.Equal(t, OutcomeServiceUnavailable, result.Outcome)
			assert.Zero(t, fixture.backend.callCount())
			assert.Equal(t, placement.StateAbsent, fixture.store.Lookup(testTarget).State())
			assert.False(t, fixture.registry.registry.Contains(testTarget))
			_, _, refuse, _ := fixture.authority.counts()
			assert.Equal(t, 1, refuse)
			requireSourceReusable(t, fixture, "probe-"+test.name)
			requireLeaseClaimAvailable(t, fixture.registry.registry, testSource)
			requireLeaseClaimAvailable(t, fixture.registry.registry, testTarget)
		})
	}
}

func TestServiceEventSinkPanicDoesNotPreventRestoreDispatch(t *testing.T) {
	fixture := newFixture(t, true)
	fixture.events.hook = func(backend.LeaseStatusEvent) {
		panic("event sink fault")
	}
	panics := metrics.LifecycleEventSinkPanicsTotal.WithLabelValues(
		metrics.LifecycleEventRestoreRestarting,
	)
	before := promtestutil.ToFloat64(panics)

	result := fixture.service.Execute(t.Context(), validCommand())

	require.Equal(t, OutcomeAccepted, result.Outcome)
	assert.Equal(t, 1, fixture.backend.callCount(),
		"best-effort event delivery must not suppress restore dispatch")
	assert.Equal(t, placement.StateConfirmed, fixture.store.Lookup(testTarget).State())
	record, exists := fixture.registry.registry.Lookup(testTarget)
	require.True(t, exists)
	assert.Equal(t, operation.PhaseActive, record.Phase,
		"the recovered panic must not strand the restore operation in Calling")
	assert.Equal(t, before+1, promtestutil.ToFloat64(panics))
	requireSourceReusable(t, fixture, "probe-after-event-panic")
}

func TestServiceRefusalEventSinkPanicDoesNotUndoDefinitiveSettlement(t *testing.T) {
	fixture := newFixture(t, true)
	fixture.backend.err = backend.ErrValidation
	fixture.events.hook = func(event backend.LeaseStatusEvent) {
		if event.Status == backend.ProvisionStatusFailed {
			panic("refusal event sink fault")
		}
	}
	panics := metrics.LifecycleEventSinkPanicsTotal.WithLabelValues(
		metrics.LifecycleEventRestoreRefused,
	)
	before := promtestutil.ToFloat64(panics)

	result := fixture.service.Execute(t.Context(), validCommand())

	require.Equal(t, OutcomeInvalidRequest, result.Outcome)
	assert.Equal(t, placement.StateAbsent, fixture.store.Lookup(testTarget).State(),
		"event delivery cannot roll back an exact refusal settlement")
	assert.False(t, fixture.registry.registry.Contains(testTarget))
	assert.Equal(t, before+1, promtestutil.ToFloat64(panics))
	events := fixture.events.snapshot()
	require.Len(t, events, 2)
	assert.Equal(t, backend.ProvisionStatusRestarting, events[0].Status)
	assert.Equal(t, backend.ProvisionStatusFailed, events[1].Status)
	requireSourceReusable(t, fixture, "probe-after-refusal-event-panic")
}

func TestServiceInvalidRestoreClaimFailsClosed(t *testing.T) {
	fixture := newFixture(t, true)
	fixture.authority.invalid = true
	result := fixture.service.Execute(t.Context(), validCommand())
	assert.Equal(t, OutcomeServiceUnavailable, result.Outcome)
	assert.ErrorIs(t, result.Cause(), placement.ErrInvalidRestoreClaim)
	assert.Zero(t, fixture.backend.callCount())
	assert.False(t, fixture.registry.registry.Contains(testTarget))
}

func TestServiceSynchronousSettlementModes(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		outcome    Outcome
		wantState  placement.State
		settlement string
		wantEvents []backend.ProvisionStatus
		verdict    string
	}{
		{name: "accepted", outcome: OutcomeAccepted, wantState: placement.StateConfirmed, settlement: "confirm", wantEvents: []backend.ProvisionStatus{backend.ProvisionStatusRestarting}},
		{name: "already provisioned", err: backend.ErrAlreadyProvisioned, outcome: OutcomeAlreadyProvisioned, wantState: placement.StateConfirmed, settlement: "confirm", wantEvents: []backend.ProvisionStatus{backend.ProvisionStatusRestarting}},
		{name: "definitive refusal", err: backend.ErrValidation, outcome: OutcomeInvalidRequest, wantState: placement.StateAbsent, settlement: "refuse", wantEvents: []backend.ProvisionStatus{backend.ProvisionStatusRestarting, backend.ProvisionStatusFailed}},
		{name: "coded capacity refusal", err: backend.ErrCapacityRefused, outcome: OutcomeInsufficientResources, wantState: placement.StateAbsent, settlement: "refuse", wantEvents: []backend.ProvisionStatus{backend.ProvisionStatusRestarting, backend.ProvisionStatusFailed}, verdict: metrics.CapacityVerdictCodedRefusal},
		{name: "ambiguous capacity response", err: backend.ErrInsufficientResources, outcome: OutcomeInsufficientResources, wantState: placement.StateAttempting, settlement: "abandon", wantEvents: []backend.ProvisionStatus{backend.ProvisionStatusRestarting}, verdict: metrics.CapacityVerdictAmbiguous},
		{name: "ambiguous error", err: context.DeadlineExceeded, outcome: OutcomeInternalFailure, wantState: placement.StateAttempting, settlement: "abandon", wantEvents: []backend.ProvisionStatus{backend.ProvisionStatusRestarting}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t, true)
			fixture.backend.err = test.err
			var capacityCounter prometheus.Counter
			var capacityBefore float64
			if test.verdict != "" {
				capacityCounter = metrics.BackendInsufficientResourcesTotal.WithLabelValues(
					testBackend, test.verdict,
				)
				capacityBefore = promtestutil.ToFloat64(capacityCounter)
			}
			result := fixture.service.Execute(t.Context(), validCommand())
			assert.Equal(t, test.outcome, result.Outcome)
			assert.Equal(t, test.wantState, fixture.store.Lookup(testTarget).State())
			begin, confirm, refuse, abandon := fixture.authority.counts()
			assert.Equal(t, 1, begin)
			assert.Equal(t, map[string][3]int{
				"confirm": {1, 0, 0},
				"refuse":  {0, 1, 0},
				"abandon": {0, 0, 1},
			}[test.settlement], [3]int{confirm, refuse, abandon})
			assert.Equal(t, test.outcome == OutcomeAccepted,
				fixture.registry.registry.Contains(testTarget))
			events := fixture.events.snapshot()
			require.Len(t, events, len(test.wantEvents))
			for index, status := range test.wantEvents {
				assert.Equal(t, status, events[index].Status)
				assert.Equal(t, time.Unix(123, 0).UTC(), events[index].Timestamp)
			}
			if len(events) == 2 {
				assert.Equal(t, "restore did not start", events[1].Error)
			}
			if capacityCounter != nil {
				assert.Equal(t, capacityBefore+1, promtestutil.ToFloat64(capacityCounter))
			}
		})
	}
}

func TestServiceInlineCallbackOverridesLaterSynchronousError(t *testing.T) {
	tests := []struct {
		name      string
		success   bool
		wantState placement.State
		status    backend.ProvisionStatus
	}{
		{name: "success callback plus error", success: true, wantState: placement.StateConfirmed, status: backend.ProvisionStatusReady},
		{name: "failure callback plus error", wantState: placement.StateAbsent, status: backend.ProvisionStatusFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t, true)
			fixture.backend.setRestore(func(_ context.Context, request backend.RestoreRequest) error {
				callbackURL, err := url.Parse(request.CallbackURL)
				require.NoError(t, err)
				id, present, err := operation.ParseQuery(callbackURL.Query())
				require.NoError(t, err)
				require.True(t, present)
				claimed := fixture.registry.registry.TryClaimCallback(testTarget, id)
				require.True(t, claimed.Claimed())
				if test.success {
					settled, err := fixture.store.ConfirmOperation(testTarget, testBackend, id)
					require.NoError(t, err)
					require.True(t, settled)
				} else {
					settled, err := fixture.store.RefuseOperation(testTarget, testBackend, id)
					require.NoError(t, err)
					require.True(t, settled)
				}
				fixture.events.Publish(backend.LeaseStatusEvent{LeaseUUID: testTarget, Status: test.status})
				require.True(t, fixture.registry.registry.FinishSettlement(claimed.Claim()))
				return backend.ErrValidation
			})

			result := fixture.service.Execute(t.Context(), validCommand())
			require.Equal(t, OutcomeAccepted, result.Outcome)
			assert.Equal(t, test.wantState, fixture.store.Lookup(testTarget).State())
			assert.False(t, fixture.registry.registry.Contains(testTarget))
			_, confirm, refuse, abandon := fixture.authority.counts()
			assert.Zero(t, confirm)
			assert.Zero(t, refuse)
			assert.Equal(t, 1, abandon)
			events := fixture.events.snapshot()
			require.Len(t, events, 2)
			assert.Equal(t, backend.ProvisionStatusRestarting, events[0].Status)
			assert.Equal(t, test.status, events[1].Status)
			requireSourceReusable(t, fixture, "probe-inline-"+test.name)
		})
	}
}

func TestServiceBackendPanicReleasesClaimsAndRetainsAttempt(t *testing.T) {
	fixture := newFixture(t, true)
	fixture.backend.setRestore(func(context.Context, backend.RestoreRequest) error {
		panic("restore exploded")
	})

	result := fixture.service.Execute(t.Context(), validCommand())

	assert.Equal(t, OutcomeInternalFailure, result.Outcome)
	assert.ErrorContains(t, result.Cause(), "panicked")
	assert.Equal(t, placement.StateAttempting, fixture.store.Lookup(testTarget).State())
	assert.False(t, fixture.registry.registry.Contains(testTarget))
	_, confirm, refuse, abandon := fixture.authority.counts()
	assert.Zero(t, confirm)
	assert.Zero(t, refuse)
	assert.Equal(t, 1, abandon)
	requireLeaseClaimAvailable(t, fixture.registry.registry, testSource)
	requireLeaseClaimAvailable(t, fixture.registry.registry, testTarget)
	requireSourceReusable(t, fixture, "probe-after-panic")
}

func TestDefinitelyRefusedIsExhaustiveForSynchronousProofs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		err  error
		want bool
	}{
		{err: backend.ErrNotRetained, want: true},
		{err: backend.ErrInvalidState, want: true},
		{err: backend.ErrAlreadyProvisioned, want: false},
		{err: backend.ErrInsufficientResources, want: false},
		{err: backend.ErrCapacityRefused, want: true},
		{err: backend.ErrCircuitOpen, want: true},
		{err: backend.ErrDemoteDataExceedsTier, want: true},
		{err: backend.ErrValidation, want: true},
		{err: backend.ErrRestoreRefused, want: true},
		{err: backend.ErrMalformedErrorBody, want: false},
		{err: context.DeadlineExceeded, want: false},
		{err: fmt.Errorf("generic"), want: false},
	}
	for index, test := range tests {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.want, definitelyRefused(test.err))
		})
	}
}
