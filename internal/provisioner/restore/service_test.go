package restore

import (
	"context"
	"crypto/sha256"
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
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
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
	errs   map[string]error
	hook   func(string)
	calls  int
}

func (reader *targetReader) GetLease(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
	reader.mu.Lock()
	reader.calls++
	hook := reader.hook
	err := reader.errs[leaseUUID]
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
	getInfo *backend.ProvisionInfo
	getErr  error
	get     func(context.Context, string) (*backend.ProvisionInfo, error)
	restore func(context.Context, backend.RestoreRequest) error
	request backend.RestoreRequest
	calls   int
	gets    int
}

func (fake *fakeBackend) Name() string { return fake.name }

func (fake *fakeBackend) GetProvision(
	ctx context.Context,
	leaseUUID string,
) (*backend.ProvisionInfo, error) {
	fake.mu.Lock()
	fake.gets++
	get := fake.get
	getErr := fake.getErr
	getInfo := fake.getInfo
	fake.mu.Unlock()
	if get != nil {
		return get(ctx, leaseUUID)
	}
	if getErr != nil || getInfo == nil {
		return nil, getErr
	}
	info := *getInfo
	return &info, nil
}

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
	leaseClaimCalls  int
}

func (spy *registrySpy) TryClaimLeaseNow(leaseUUID string) operation.LeaseClaimResult {
	spy.mu.Lock()
	spy.leaseClaimCalls++
	spy.mu.Unlock()
	return spy.registry.TryClaimLeaseNow(leaseUUID)
}

func (spy *registrySpy) claimCallCount() int {
	spy.mu.Lock()
	defer spy.mu.Unlock()
	return spy.leaseClaimCalls
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

func (spy *authoritySpy) Lookup(leaseUUID string) placement.Placement {
	return spy.store.Lookup(leaseUUID)
}

func (spy *authoritySpy) BeginAuthorizedRestore(
	baseline placement.AdmissionBaseline,
	sourceRevision placement.RecordRevision,
	targetLeaseUUID string,
	id operation.OperationID,
	requestSnapshot placement.BackendRequestSnapshot,
	callbackPair placement.CallbackPair,
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
	return spy.store.BeginAuthorizedRestore(
		baseline, sourceRevision, targetLeaseUUID, id, requestSnapshot, callbackPair,
	)
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

func sourceLease(uuid string) *billingtypes.Lease {
	lease := pendingLease(uuid)
	lease.State = billingtypes.LEASE_STATE_CLOSED
	return lease
}

func newFixture(t *testing.T, inventoryReady bool) *fixture {
	t.Helper()
	store, err := placementstore.NewStore(filepath.Join(t.TempDir(), "placements.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.ConfigureBackendTopologyWithStorageIdentities(
		[]string{testBackend, "backend-b"},
		restoreTestBackendStorageIDs(testBackend, "backend-b"),
	))
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
			testSource: sourceLease(testSource),
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
		Leases:       result.targets,
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
	projection := placement.InventoryProjection{Complete: complete, Placements: placements}
	if complete {
		backendNames := []string{testBackend, "backend-b"}
		projection.BackendStorageIdentities = restoreTestBackendStorageIDs(backendNames...)
		nonempty := make(map[string]struct{}, len(placements))
		for _, backendName := range placements {
			nonempty[backendName] = struct{}{}
		}
		projection.EmptyBackends = make([]string, 0, len(backendNames))
		for _, backendName := range backendNames {
			if _, present := nonempty[backendName]; !present {
				projection.EmptyBackends = append(projection.EmptyBackends, backendName)
			}
		}
	}
	result, err := store.ProjectInventory(fence, projection)
	require.NoError(t, err)
	return result
}

func restoreTestBackendStorageID(name string) backendidentity.ID {
	digest := sha256.Sum256([]byte("fred-restore-test-storage:" + name))
	digest[6] = (digest[6] & 0x0f) | 0x40
	digest[8] = (digest[8] & 0x3f) | 0x80
	id, err := backendidentity.Parse(fmt.Sprintf("%x-%x-%x-%x-%x",
		digest[0:4], digest[4:6], digest[6:8], digest[8:10], digest[10:16]))
	if err != nil {
		panic(err)
	}
	return id
}

func restoreTestBackendStorageIDs(names ...string) map[string]backendidentity.ID {
	identities := make(map[string]backendidentity.ID, len(names))
	for _, name := range names {
		identities[name] = restoreTestBackendStorageID(name)
	}
	return identities
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

func testCallbackPair(t *testing.T, id operation.OperationID) placement.CallbackPair {
	t.Helper()
	pair, err := placement.NewCallbackPair(
		id,
		"https://provider.test/callbacks/provision?operation_id="+id.String(),
		"https://provider.test/callbacks/provision?lifecycle_id="+id.String(),
	)
	require.NoError(t, err)
	return pair
}

func testBackendRequestSnapshot(t *testing.T) placement.BackendRequestSnapshot {
	t.Helper()
	snapshot, err := placement.NewBackendRequestSnapshot(
		testTenant, testProvider,
		[]backend.LeaseItem{{
			SKU: "sku-1", Quantity: 2, ServiceName: "web",
			CustomDomain: "tenant.example.test",
		}},
	)
	require.NoError(t, err)
	return snapshot
}

func requireSourceReusable(t *testing.T, fixture *fixture, target string) {
	t.Helper()
	id := testOperationID(t, 9001)
	claim, err := fixture.store.BeginAuthorizedRestore(
		fixture.store.CurrentAdmissionBaseline(),
		fixture.store.Lookup(testSource).RecordRevision(),
		target,
		id,
		testBackendRequestSnapshot(t),
		testCallbackPair(t, id),
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
		Leases:       &targetReader{},
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
		{name: "lease reader", edit: func(c *Config) { c.Leases = nil }, want: "lease reader"},
		{name: "typed nil lease reader", edit: func(c *Config) { c.Leases = typedNilTargets }, want: "lease reader"},
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
		{name: "read error", prepare: func(f *fixture) {
			f.targets.errs = map[string]error{testTarget: errors.New("chain unavailable")}
		}, command: validCommand, outcome: OutcomeServiceUnavailable},
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

	t.Run("target read executes under source and target claims", func(t *testing.T) {
		fixture := newFixture(t, true)
		fixture.targets.hook = func(leaseUUID string) {
			if leaseUUID != testTarget {
				return
			}
			assert.Equal(t, operation.LeaseClaimBusy,
				fixture.registry.registry.TryClaimLeaseNow(testSource).Outcome())
			assert.Equal(t, operation.LeaseClaimBusy,
				fixture.registry.registry.TryClaimLeaseNow(testTarget).Outcome())
		}
		result := fixture.service.Execute(t.Context(), validCommand())
		require.Equal(t, OutcomeAccepted, result.Outcome)
	})
}

func TestServiceAuthorizesSourceBeforeTakingLeaseClaims(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*fixture)
		outcome Outcome
	}{
		{name: "source read error", prepare: func(f *fixture) {
			f.targets.errs = map[string]error{testSource: errors.New("chain unavailable")}
		}, outcome: OutcomeSourceUnavailable},
		{name: "source missing", prepare: func(f *fixture) {
			delete(f.targets.leases, testSource)
		}, outcome: OutcomeSourceNotFound},
		{name: "source UUID mismatch", prepare: func(f *fixture) {
			f.targets.leases[testSource].Uuid = "other"
		}, outcome: OutcomeSourceUnavailable},
		{name: "source belongs to another tenant", prepare: func(f *fixture) {
			f.targets.leases[testSource].Tenant = "tenant-2"
		}, outcome: OutcomeSourceNotFound},
		{name: "source belongs to another provider", prepare: func(f *fixture) {
			f.targets.leases[testSource].ProviderUuid = "provider-2"
		}, outcome: OutcomeSourceNotFound},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t, true)
			test.prepare(fixture)
			fixture.targets.hook = func(leaseUUID string) {
				if leaseUUID != testSource {
					return
				}
				for _, candidate := range []string{testSource, testTarget} {
					claim := fixture.registry.registry.TryClaimLeaseNow(candidate)
					require.True(t, claim.Acquired(), "source authorization must precede claim %q", candidate)
					require.True(t, fixture.registry.registry.ReleaseLease(claim.Claim()))
				}
			}

			result := fixture.service.Execute(t.Context(), validCommand())

			assert.Equal(t, test.outcome, result.Outcome)
			assert.Zero(t, fixture.backend.callCount())
			assert.Equal(t, placement.StateAbsent, fixture.store.Lookup(testTarget).State())
			assert.False(t, fixture.registry.registry.Contains(testTarget))
			requireLeaseClaimAvailable(t, fixture.registry.registry, testSource)
			requireLeaseClaimAvailable(t, fixture.registry.registry, testTarget)
		})
	}
}

func retainedSourceInfo(tenant string) *backend.ProvisionInfo {
	return &backend.ProvisionInfo{
		LeaseUUID:    testSource,
		Tenant:       tenant,
		ProviderUUID: testProvider,
		Status:       backend.ProvisionStatusRetained,
	}
}

func TestServicePrunedSourceUsesPlacementSelectedRetainedOwnershipBeforeClaims(t *testing.T) {
	fixture := newFixture(t, true)
	fixture.targets.errs = map[string]error{testSource: billingtypes.ErrLeaseNotFound}
	fixture.backend.getInfo = retainedSourceInfo(testTenant)
	require.Equal(t, placement.LifecycleVerdictUnusable,
		fixture.store.CurrentLifecycle(testSource).Verdict(),
		"retention-only inventory must not establish runtime lifecycle authority")

	result := fixture.service.Execute(t.Context(), validCommand())

	require.Equal(t, OutcomeAccepted, result.Outcome)
	assert.Equal(t, testBackend, result.BackendName)
	assert.Equal(t, 1, fixture.backend.callCount())
	assert.Equal(t, placement.StateConfirmed, fixture.store.Lookup(testTarget).State())
	assert.Equal(t, placement.LifecycleVerdictUnusable,
		fixture.store.CurrentLifecycle(testSource).Verdict(),
		"read-only retained ownership authorization must not repair lifecycle authority")
}

func TestServicePrunedSourceCrossTenantIsNotFoundBeforeClaims(t *testing.T) {
	fixture := newFixture(t, true)
	delete(fixture.targets.leases, testSource)
	fixture.backend.getInfo = retainedSourceInfo("tenant-other")

	result := fixture.service.Execute(t.Context(), validCommand())

	assert.Equal(t, OutcomeSourceNotFound, result.Outcome)
	assert.Zero(t, fixture.registry.claimCallCount())
	assert.Zero(t, fixture.backend.callCount())
	assert.Equal(t, placement.StateAbsent, fixture.store.Lookup(testTarget).State())
}

func TestServicePrunedSourceBackendMissOrErrorNeverClaims(t *testing.T) {
	tests := []struct {
		name    string
		getInfo *backend.ProvisionInfo
		getErr  error
		outcome Outcome
	}{
		{name: "silent miss", outcome: OutcomeSourceNotFound},
		{name: "explicit miss", getErr: backend.ErrNotProvisioned, outcome: OutcomeSourceNotFound},
		{name: "backend error", getErr: errors.New("backend unavailable"), outcome: OutcomeSourceUnavailable},
		{name: "live record is not retained", getInfo: &backend.ProvisionInfo{
			LeaseUUID: testSource, Tenant: testTenant, ProviderUUID: testProvider,
			Status: backend.ProvisionStatusReady,
		}, outcome: OutcomeNotRetained},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t, true)
			delete(fixture.targets.leases, testSource)
			fixture.backend.getInfo = test.getInfo
			fixture.backend.getErr = test.getErr

			result := fixture.service.Execute(t.Context(), validCommand())

			assert.Equal(t, test.outcome, result.Outcome)
			assert.Zero(t, fixture.registry.claimCallCount())
			assert.Zero(t, fixture.backend.callCount())
			assert.Equal(t, placement.StateAbsent, fixture.store.Lookup(testTarget).State())
		})
	}
}

func TestServicePrunedSourcePlacementRaceFailsExactAuthorization(t *testing.T) {
	fixture := newFixture(t, true)
	delete(fixture.targets.leases, testSource)
	fixture.backend.get = func(context.Context, string) (*backend.ProvisionInfo, error) {
		source := fixture.store.Lookup(testSource)
		deleted, err := fixture.store.DeleteRecord(source.RecordRevision())
		require.NoError(t, err)
		require.True(t, deleted)
		projectRestoreTestPlacements(t, fixture.store, false, map[string]string{
			testSource: testBackend,
		})
		return retainedSourceInfo(testTenant), nil
	}

	result := fixture.service.Execute(t.Context(), validCommand())

	assert.Equal(t, OutcomeSourceUnavailable, result.Outcome)
	assert.ErrorIs(t, result.Cause(), placement.ErrRestoreSourceUnavailable)
	assert.Zero(t, fixture.backend.callCount(),
		"a retained authorization for an older placement revision cannot dispatch")
	assert.Equal(t, placement.StateAbsent, fixture.store.Lookup(testTarget).State())
	requireLeaseClaimAvailable(t, fixture.registry.registry, testSource)
	requireLeaseClaimAvailable(t, fixture.registry.registry, testTarget)
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
	id := testOperationID(t, 9010)
	_, _, err = fixture.store.BeginOwnedAttempt(
		fixture.store.CurrentAdmissionBaseline(),
		fixture.store.Lookup(testSource).RecordRevision(),
		testBackend,
		id,
		placement.PayloadFingerprint{},
		testBackendRequestSnapshot(t),
		testCallbackPair(t, id),
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
					fence, placement.InventoryProjection{
						Complete:      true,
						EmptyBackends: []string{testBackend, "backend-b"},
						BackendStorageIdentities: restoreTestBackendStorageIDs(
							testBackend, "backend-b",
						),
					},
				)
				fixture.store.EndInventorySession(fence)
				require.NoError(t, err)
				require.True(t, fixture.store.CurrentAdmissionBaseline().Valid())
				require.NoError(t, fixture.store.ConfigureBackendTopologyWithStorageIdentities(
					[]string{testBackend, "backend-b", "backend-c"},
					restoreTestBackendStorageIDs(testBackend, "backend-b", "backend-c"),
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
		name       string
		prepare    func(*fixture)
		wantRefuse int
	}{
		{name: "backend binding", prepare: func(f *fixture) { f.registry.bindFailure = true }, wantRefuse: 1},
		{name: "missing backend", prepare: func(f *fixture) { f.service.backends = backendLookup{} }, wantRefuse: 1},
		{name: "typed nil backend", prepare: func(f *fixture) {
			var typedNil *fakeBackend
			f.service.backends = backendLookup{testBackend: typedNil}
		}, wantRefuse: 1},
		{name: "backend name mismatch", prepare: func(f *fixture) {
			f.service.backends = backendLookup{testBackend: &fakeBackend{name: "backend-b"}}
		}, wantRefuse: 1},
		{name: "callback URL", prepare: func(f *fixture) {
			f.service.callbackURL = func(operation.OperationID) (string, error) {
				return "", errors.New("invalid callback URL")
			}
		}},
		{name: "calling phase", prepare: func(f *fixture) { f.registry.beginCallFailure = true }, wantRefuse: 1},
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
			assert.Equal(t, test.wantRefuse, refuse)
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
		{name: "already provisioned", err: backend.ErrAlreadyProvisioned, outcome: OutcomeAlreadyProvisioned, wantState: placement.StateAttempting, settlement: "abandon", wantEvents: []backend.ProvisionStatus{backend.ProvisionStatusRestarting}},
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

func TestServiceAlreadyProvisionedAwaitsExactObservedGeneration(t *testing.T) {
	fixture := newFixture(t, true)
	fixture.backend.err = backend.ErrAlreadyProvisioned

	result := fixture.service.Execute(t.Context(), validCommand())

	require.Equal(t, OutcomeAlreadyProvisioned, result.Outcome)
	pending := fixture.store.Lookup(testTarget)
	require.Equal(t, placement.StateAttempting, pending.State())
	operationID := pending.AttemptOperationID()
	require.True(t, operationID.Valid())
	attemptedID, err := lifecycle.FromOperationID(operationID)
	require.NoError(t, err)
	olderID, err := lifecycle.ParseID("550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)
	require.NotEqual(t, attemptedID, olderID)

	// A duplicate backend may already hold an older generation. The upgraded
	// inventory observation establishes that actual current authority while the
	// newer 409 attempt remains recoverable; it never mints the attempted ID.
	projectRestoreTestPlacements(t, fixture.store, false, map[string]string{
		testTarget: testBackend,
	})
	fence := fixture.store.BeginInventorySession()
	_, err = fixture.store.ProjectInventory(fence, placement.InventoryProjection{
		Placements: map[string]string{testTarget: testBackend},
		Lifecycles: map[string]placement.LifecycleObservation{
			testTarget: {Kind: placement.LifecycleObservationTyped, ID: olderID},
		},
	})
	fixture.store.EndInventorySession(fence)
	require.NoError(t, err)
	pending = fixture.store.Lookup(testTarget)
	assert.Equal(t, testBackend, pending.Backend)
	assert.Equal(t, testBackend, pending.Attempt)
	assert.Equal(t, operationID, pending.AttemptOperationID())
	assert.Equal(t, placement.LifecycleVerdictAuthorized,
		fixture.store.AuthorizeLifecycle(testTarget, olderID).Verdict())
	assert.Equal(t, placement.LifecycleVerdictStale,
		fixture.store.AuthorizeLifecycle(testTarget, attemptedID).Verdict())

	fence = fixture.store.BeginInventorySession()
	_, err = fixture.store.ProjectInventory(fence, placement.InventoryProjection{
		Placements: map[string]string{testTarget: testBackend},
		Lifecycles: map[string]placement.LifecycleObservation{
			testTarget: {Kind: placement.LifecycleObservationTyped, ID: attemptedID},
		},
	})
	fixture.store.EndInventorySession(fence)
	require.NoError(t, err)
	assert.Empty(t, fixture.store.Lookup(testTarget).Attempt)
	assert.Equal(t, placement.LifecycleVerdictAuthorized,
		fixture.store.AuthorizeLifecycle(testTarget, attemptedID).Verdict())
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
