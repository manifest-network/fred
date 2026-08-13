package provisioner

// Multi-backend reconcile harness.
//
// Every other reconciler test in this package drives in-process interface
// mocks, so the fred->backend transport has never been exercised at the
// reconciler tier. That transport is exactly where "a backend failed to answer
// this sweep" is produced: HMAC signing, the per-request timeout, the keyset
// pagination walk, the response-size cap, and the gobreaker circuit breaker all
// live in backend.HTTPClient and all of them can turn a healthy fleet into a
// partial one.
//
// This harness stands up N real httptest servers speaking the real backend
// contract, each behind a real backend.HTTPClient inside a real backend.Router,
// and lets a test fault any single server mid-run. It deliberately carries no
// build tag: it needs no Docker, no root and no network beyond loopback, so it
// belongs in the ordinary `go test -short ./...` job, where the whole suite
// runs in well under a second on every PR. Behind the `integration` tag it
// would instead ride the privileged root+btrfs+Docker workflow — tens of
// minutes, and gated on an environment none of these tests need.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// fleetSecret is the shared HMAC secret for every backend in the harness. Real
// length matters: the client signs unconditionally and the servers verify, so a
// signing regression fails these tests rather than passing silently.
const fleetSecret = "fleet-harness-secret-at-least-32-bytes-long"

// faultKind enumerates the ways a backend can fail to answer a sweep. These are
// the observed production failure modes, not an abstract taxonomy: each one
// reaches the reconciler through a different branch of backend.HTTPClient.
type faultKind string

const (
	// faultNone answers normally.
	faultNone faultKind = "none"
	// faultConnReset hijacks and drops the connection, i.e. the backend process
	// died mid-request. Reversible, unlike killing the listener, so a test can
	// fault and then recover the same server.
	faultConnReset faultKind = "conn_reset"
	// faultHang blocks past the client timeout: TCP is up, HTTP is dead. This is
	// the partition case, and it is the one that costs wall-clock rather than
	// failing fast.
	faultHang faultKind = "hang"
	// faultHTTP500 / faultHTTP503 are plain server errors. Neither is a mapped
	// sentinel on the list path, so both surface as untyped errors.
	faultHTTP500 faultKind = "http_500"
	faultHTTP503 faultKind = "http_503"
	// faultGarbage returns 200 with a body that is not valid JSON.
	faultGarbage faultKind = "garbage"
	// faultOversize returns a body larger than the client's response cap.
	faultOversize faultKind = "oversize"
	// faultPage2 serves the first keyset page and fails the second, exercising
	// walkKeysetPages' complete-or-error contract: a mid-walk failure must
	// discard the pages already collected rather than yield a short list.
	faultPage2 faultKind = "page2_fail"
	// faultSlowOK answers correctly but slowly, within the timeout. This is the
	// false-positive guard: a slow backend must NOT be counted as unanswered.
	faultSlowOK faultKind = "slow_ok"
	// faultRetentionsOnly answers /provisions but fails /retentions, so sweep
	// completeness and retention completeness are provably independent signals.
	faultRetentionsOnly faultKind = "retentions_only"
)

// fakeBackendServer is one backend in the harness: a real HTTP server speaking
// the backend contract, with state supplied by backend.MockBackend so the
// lifecycle semantics (already-provisioned, not-provisioned, status) are the
// production ones rather than a second implementation that can drift.
type fakeBackendServer struct {
	name string
	mock *backend.MockBackend
	srv  *httptest.Server

	mu                sync.Mutex
	fault             faultKind
	hangFor           time.Duration
	killed            bool
	listCalls         int
	retentionCalls    int
	provisionCalls    map[string]int
	deprovisionCalls  map[string]int
	customDomainCalls map[string]int
}

func newFakeBackendServer(t *testing.T, name string) *fakeBackendServer {
	t.Helper()

	f := &fakeBackendServer{
		name:              name,
		mock:              backend.NewMockBackend(backend.MockBackendConfig{Name: name}),
		fault:             faultNone,
		hangFor:           2 * time.Second,
		provisionCalls:    make(map[string]int),
		deprovisionCalls:  make(map[string]int),
		customDomainCalls: make(map[string]int),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /provisions", f.handleListProvisions)
	mux.HandleFunc("GET /provisions/{lease_uuid}", f.handleGetProvision)
	mux.HandleFunc("GET /retentions", f.handleListRetentions)
	mux.HandleFunc("GET /stats", f.handleStats)
	mux.HandleFunc("GET /health", f.handleHealth)
	mux.HandleFunc("POST /provision", f.handleProvision)
	mux.HandleFunc("POST /deprovision", f.handleDeprovision)
	mux.HandleFunc("POST /reconcile_custom_domain", f.handleReconcileCustomDomain)

	f.srv = httptest.NewServer(f.verifySignature(mux))
	t.Cleanup(f.srv.Close)
	return f
}

// verifySignature rejects any request fred did not sign correctly. Without this
// the harness would pass even if signing broke, which would defeat the point of
// running the real client.
func (f *fakeBackendServer) verifySignature(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sig := r.Header.Get(hmacauth.SignatureHeader)
		if sig == "" {
			http.Error(w, "missing signature", http.StatusUnauthorized)
			return
		}
		body := drainAndRestoreBody(r)
		if err := hmacauth.VerifyRequest(fleetSecret, r, body, sig, 5*time.Minute); err != nil {
			http.Error(w, "bad signature: "+err.Error(), http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// drainAndRestoreBody reads the request body for signature verification and
// puts it back so the downstream handler can decode it. hmacauth.VerifyRequest
// deliberately does not touch r.Body, leaving that to the caller.
func drainAndRestoreBody(r *http.Request) []byte {
	if r.Body == nil {
		return nil
	}
	b, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return nil
	}
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(b))
	return b
}

// setFault changes the server's behaviour for subsequent requests.
func (f *fakeBackendServer) setFault(k faultKind) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fault = k
}

// kill closes the listener outright, producing genuine connection-refused. It
// is terminal: use setFault(faultConnReset) for a fault the test intends to
// recover from.
func (f *fakeBackendServer) kill() {
	f.mu.Lock()
	if f.killed {
		f.mu.Unlock()
		return
	}
	f.killed = true
	f.mu.Unlock()
	f.srv.Close()
}

func (f *fakeBackendServer) currentFault() (faultKind, time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fault, f.hangFor
}

// applyFault runs the configured fault for a list endpoint. It reports true
// when it has already written a response (or destroyed the connection) and the
// caller must not continue. faultRetentionsOnly is scoped to /retentions; every
// other fault applies to both list endpoints, because a backend that cannot
// answer one generally cannot answer the other.
func (f *fakeBackendServer) applyFault(w http.ResponseWriter, r *http.Request, isRetentions bool) bool {
	kind, hangFor := f.currentFault()

	if kind == faultRetentionsOnly {
		if !isRetentions {
			return false
		}
		http.Error(w, "retentions unavailable", http.StatusInternalServerError)
		return true
	}

	switch kind {
	case faultNone:
		return false

	case faultSlowOK:
		select {
		case <-time.After(20 * time.Millisecond):
		case <-r.Context().Done():
		}
		return false

	case faultConnReset:
		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "cannot hijack", http.StatusInternalServerError)
			return true
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			http.Error(w, "hijack failed", http.StatusInternalServerError)
			return true
		}
		_ = conn.Close()
		return true

	case faultHang:
		// Honour the request context so the server goroutine unwinds as soon as
		// the client gives up, instead of outliving the test.
		select {
		case <-time.After(hangFor):
		case <-r.Context().Done():
		}
		return true

	case faultHTTP500:
		http.Error(w, "internal error", http.StatusInternalServerError)
		return true

	case faultHTTP503:
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return true

	case faultGarbage:
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"provisions": [ this is not json`))
		return true

	case faultOversize:
		w.Header().Set("Content-Type", "application/json")
		// Valid JSON, but past the client's per-response cap.
		_, _ = fmt.Fprintf(w, `{"provisions":[],"pad":%q}`, strings.Repeat("a", 64*1024))
		return true

	case faultPage2:
		// Serve page 1 (no continue token), fail every subsequent page.
		if r.URL.Query().Get("continue") != "" {
			http.Error(w, "page fetch failed", http.StatusInternalServerError)
			return true
		}
		return false

	default:
		return false
	}
}

func (f *fakeBackendServer) handleListProvisions(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.listCalls++
	f.mu.Unlock()

	if f.applyFault(w, r, false) {
		return
	}

	limit, cont, err := backend.ParsePageParams(r.URL.Query())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	all, err := f.mock.ListProvisions(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	page, next := backend.PaginateProvisions(all, cont, limit)
	if page == nil {
		page = []backend.ProvisionInfo{}
	}
	writeJSON(w, backend.ListProvisionsResponse{Provisions: page, Continue: next})
}

func (f *fakeBackendServer) handleListRetentions(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.retentionCalls++
	f.mu.Unlock()

	if f.applyFault(w, r, true) {
		return
	}

	limit, cont, err := backend.ParsePageParams(r.URL.Query())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	all, err := f.mock.ListRetentions(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	page, next := backend.PaginateRetentions(all, cont, limit)
	if page == nil {
		page = []backend.RetainedLease{}
	}
	writeJSON(w, backend.ListRetentionsResponse{Retentions: page, Continue: next})
}

func (f *fakeBackendServer) handleGetProvision(w http.ResponseWriter, r *http.Request) {
	info, err := f.mock.GetProvision(r.Context(), r.PathValue("lease_uuid"))
	if err != nil {
		http.Error(w, "not provisioned", http.StatusNotFound)
		return
	}
	writeJSON(w, info)
}

func (f *fakeBackendServer) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := f.mock.GetLoadStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if stats == nil {
		stats = &backend.LoadStats{}
	}
	writeJSON(w, stats)
}

func (f *fakeBackendServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := f.mock.Health(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, map[string]string{"status": "healthy"})
}

func (f *fakeBackendServer) handleProvision(w http.ResponseWriter, r *http.Request) {
	var req backend.ProvisionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	f.mu.Lock()
	f.provisionCalls[req.LeaseUUID]++
	f.mu.Unlock()

	if err := f.mock.Provision(r.Context(), req); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (f *fakeBackendServer) handleDeprovision(w http.ResponseWriter, r *http.Request) {
	var req struct {
		LeaseUUID string `json:"lease_uuid"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	f.mu.Lock()
	f.deprovisionCalls[req.LeaseUUID]++
	f.mu.Unlock()

	if err := f.mock.Deprovision(r.Context(), req.LeaseUUID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (f *fakeBackendServer) handleReconcileCustomDomain(w http.ResponseWriter, r *http.Request) {
	var req backend.ReconcileCustomDomainRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	f.mu.Lock()
	f.customDomainCalls[req.LeaseUUID]++
	f.mu.Unlock()

	w.WriteHeader(http.StatusNoContent)
}

// seedProvision puts a lease on this backend in the given status, as if it had
// been provisioned here earlier.
func (f *fakeBackendServer) seedProvision(t *testing.T, leaseUUID, providerUUID string, status backend.ProvisionStatus) {
	t.Helper()
	require.NoError(t, f.mock.Provision(t.Context(), backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		ProviderUUID: providerUUID,
	}))
	f.mock.SetProvisionStatus(leaseUUID, status)

	// Seeding is not a reconciler action; do not let it pollute the call counts
	// the invariant helper asserts on.
	f.mu.Lock()
	delete(f.provisionCalls, leaseUUID)
	f.mu.Unlock()
}

func (f *fakeBackendServer) seedRetention(leaseUUID string) {
	existing, _ := f.mock.ListRetentions(context.Background())
	f.mock.SetRetentions(append(existing, backend.RetainedLease{LeaseUUID: leaseUUID}))
}

func (f *fakeBackendServer) provisionCount(leaseUUID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.provisionCalls[leaseUUID]
}

func (f *fakeBackendServer) deprovisionCount(leaseUUID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deprovisionCalls[leaseUUID]
}

func (f *fakeBackendServer) totalProvisionCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.provisionCalls {
		n += c
	}
	return n
}

// listCallCount is how many times fred actually dialed /provisions. It goes to
// zero-growth once the circuit breaker opens, which is the only externally
// visible difference between "failing" and "short-circuited".
func (f *fakeBackendServer) listCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listCalls
}

// ---------------------------------------------------------------------------
// The fleet
// ---------------------------------------------------------------------------

// fleetOptions tunes the harness. Zero values give the common case: three
// backends, generous page sizes, placement tracking on.
type fleetOptions struct {
	backendCount int
	// pageLimit sets the client's requested page size for both list endpoints.
	// Set it to 1 to force multi-page walks (needed by faultPage2).
	pageLimit int
	// clientTimeout bounds every backend request. Kept short so faultHang costs
	// milliseconds rather than the production 30s.
	clientTimeout time.Duration
	// maxProvisionsBytes caps the /provisions response so faultOversize is
	// cheap to trigger.
	maxProvisionsBytes int64
	// noPlacement disables the placement store, modelling a deployment that has
	// not enabled it.
	noPlacement bool
	// interval is the reconcile interval. Tests drive sweeps explicitly, so its
	// only live effect is the placement pruner's grace window, which is
	// 2 x interval. It defaults to an hour precisely so the pruner cannot fire
	// by accident; a pruner test must shorten it deliberately.
	interval time.Duration
	// placementAge back-dates placement records by this much via the store's
	// injectable clock. The pruner compares a record's SetAt against the
	// sweep's real wall-clock start, so back-dating is the only deterministic
	// way to age a record past the grace window without sleeping.
	placementAge time.Duration
}

type fleet struct {
	t            *testing.T
	servers      []*fakeBackendServer
	byName       map[string]*fakeBackendServer
	router       *backend.Router
	reconciler   *Reconciler
	chain        *chaintest.MockClient
	placement    *placement.Store
	payloads     *payload.Store
	tracker      *mockInFlightTracker
	providerUUID string

	chainMu  sync.Mutex
	acked    []string
	rejected []string
	closed   []string

	leaseMu     sync.Mutex
	leases      map[string]billingtypes.Lease
	getLeaseErr error
}

func newFleet(t *testing.T, opts fleetOptions) *fleet {
	t.Helper()

	if opts.backendCount == 0 {
		opts.backendCount = 3
	}
	if opts.pageLimit == 0 {
		opts.pageLimit = 1000
	}
	if opts.clientTimeout == 0 {
		opts.clientTimeout = 300 * time.Millisecond
	}
	if opts.maxProvisionsBytes == 0 {
		opts.maxProvisionsBytes = 32 * 1024
	}
	if opts.interval == 0 {
		opts.interval = 1 * time.Hour
	}

	f := &fleet{
		t:            t,
		byName:       make(map[string]*fakeBackendServer),
		providerUUID: "provider-1",
		leases:       make(map[string]billingtypes.Lease),
	}

	entries := make([]backend.BackendEntry, 0, opts.backendCount)
	for i := range opts.backendCount {
		name := fmt.Sprintf("backend-%d", i+1)
		srv := newFakeBackendServer(t, name)
		f.servers = append(f.servers, srv)
		f.byName[name] = srv

		client := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:                name,
			BaseURL:             srv.srv.URL,
			Secret:              fleetSecret,
			Timeout:             opts.clientTimeout,
			ProvisionsPageLimit: opts.pageLimit,
			RetentionsPageLimit: opts.pageLimit,
			MaxProvisionsBytes:  opts.maxProvisionsBytes,
			MaxRetentionsBytes:  opts.maxProvisionsBytes,
		})

		entries = append(entries, backend.BackendEntry{Backend: client, IsDefault: i == 0})
	}

	router, err := backend.NewRouter(backend.RouterConfig{Backends: entries})
	require.NoError(t, err)
	f.router = router

	payloads, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(t.TempDir(), "payloads.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = payloads.Close() })
	f.payloads = payloads
	f.tracker = newMockInFlightTracker(payloads)

	var placementStore PlacementStore
	if !opts.noPlacement {
		age := opts.placementAge
		ps, err := placement.NewStore(
			filepath.Join(t.TempDir(), "placements.db"),
			placement.WithClock(func() time.Time { return time.Now().Add(-age) }),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = ps.Close() })
		f.placement = ps
		placementStore = ps
	}

	f.chain = &chaintest.MockClient{
		GetPendingLeasesFunc: func(_ context.Context, _ string) ([]billingtypes.Lease, error) {
			return f.leasesInState(billingtypes.LEASE_STATE_PENDING), nil
		},
		GetActiveLeasesByProviderFunc: func(_ context.Context, _ string) ([]billingtypes.Lease, error) {
			return f.leasesInState(billingtypes.LEASE_STATE_ACTIVE), nil
		},
		// Per-lease lookups see every lease the chain holds, in whatever state —
		// including the terminal ones the two list queries filter out. That
		// difference is the whole point of the re-check (ENG-654), so the
		// harness must model it: closeLease keeps a lease here, removeLease
		// makes the chain deny all knowledge of it, and getLeaseErr models an
		// unreachable chain.
		GetLeaseFunc: func(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			f.leaseMu.Lock()
			defer f.leaseMu.Unlock()
			if f.getLeaseErr != nil {
				return nil, f.getLeaseErr
			}
			lease, ok := f.leases[leaseUUID]
			if !ok {
				return nil, nil
			}
			return &lease, nil
		},
		RejectLeasesFunc: func(_ context.Context, uuids []string, _ string) (uint64, []string, error) {
			f.chainMu.Lock()
			f.rejected = append(f.rejected, uuids...)
			f.chainMu.Unlock()
			return uint64(len(uuids)), nil, nil
		},
		CloseLeasesFunc: func(_ context.Context, uuids []string, _ string) (uint64, []string, error) {
			f.chainMu.Lock()
			f.closed = append(f.closed, uuids...)
			f.chainMu.Unlock()
			return uint64(len(uuids)), nil, nil
		},
	}

	// Acks are routed through the batcher, not the chain client, so recording
	// only on chaintest.MockClient would miss every acknowledge.
	ack := &mockAcknowledger{
		acknowledgeFn: func(_ context.Context, leaseUUID string) (bool, string, error) {
			f.chainMu.Lock()
			f.acked = append(f.acked, leaseUUID)
			f.chainMu.Unlock()
			return true, "tx-hash", nil
		},
	}

	rec, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:           f.providerUUID,
		CallbackBaseURL:        "http://fred.invalid",
		Interval:               opts.interval,
		MaxReprovisionAttempts: 3,
	}, f.chain, ack, router, f.tracker, placementStore)
	require.NoError(t, err)
	f.reconciler = rec

	return f
}

// backendAt returns the i-th backend server (1-indexed to match its name).
func (f *fleet) backendAt(i int) *fakeBackendServer {
	f.t.Helper()
	require.GreaterOrEqual(f.t, i, 1)
	require.LessOrEqual(f.t, i, len(f.servers))
	return f.servers[i-1]
}

// sweep runs one reconcile pass.
func (f *fleet) sweep() error {
	return f.reconciler.ReconcileAll(f.t.Context())
}

// sweepN runs n reconcile passes, returning the last error. Reconciliation is
// level-triggered and detect->act is asynchronous, so a single pass can observe
// a condition without having acted on it yet.
func (f *fleet) sweepN(n int) error {
	var err error
	for range n {
		err = f.sweep()
	}
	return err
}

func (f *fleet) chainCalls() (acked, rejected, closed []string) {
	f.chainMu.Lock()
	defer f.chainMu.Unlock()
	return append([]string(nil), f.acked...),
		append([]string(nil), f.rejected...),
		append([]string(nil), f.closed...)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// ---------------------------------------------------------------------------
// Chain lease bookkeeping
// ---------------------------------------------------------------------------

// addLease puts a lease on the chain in the given state. skuUUIDs is optional;
// when supplied the lease carries matching items so SKU routing applies.
func (f *fleet) addLease(uuid string, state billingtypes.LeaseState, skuUUIDs ...string) {
	lease := chaintest.NewMockLeaseWithSKU(uuid, "tenant-1", f.providerUUID, state, skuUUIDs...)
	f.leaseMu.Lock()
	defer f.leaseMu.Unlock()
	f.leases[uuid] = *lease
}

// closeLease moves a lease to CLOSED. It drops out of the PENDING/ACTIVE list
// queries but a per-lease lookup still finds it, which is the normal end of a
// lease's life and the only thing that authorises destroying its state.
func (f *fleet) closeLease(uuid string) {
	f.leaseMu.Lock()
	defer f.leaseMu.Unlock()
	lease, ok := f.leases[uuid]
	if !ok {
		f.t.Fatalf("closeLease: no lease %q on chain", uuid)
	}
	lease.State = billingtypes.LEASE_STATE_CLOSED
	f.leases[uuid] = lease
}

// removeLease makes the chain deny all knowledge of a lease. Note this is NOT
// how a lease ends on the real chain — x/billing never deletes one — so it
// models a phantom provision, a wrong/reset chain, or an RPC node behind the
// head. Fred must refuse to clean up after it; use closeLease for an ordinary
// terminal lease.
func (f *fleet) removeLease(uuid string) {
	f.leaseMu.Lock()
	defer f.leaseMu.Unlock()
	delete(f.leases, uuid)
}

// setGetLeaseErr makes every per-lease chain lookup fail, modelling a chain fred
// cannot reach while its cached list results are still usable.
func (f *fleet) setGetLeaseErr(err error) {
	f.leaseMu.Lock()
	defer f.leaseMu.Unlock()
	f.getLeaseErr = err
}

func (f *fleet) leasesInState(state billingtypes.LeaseState) []billingtypes.Lease {
	f.leaseMu.Lock()
	defer f.leaseMu.Unlock()
	out := make([]billingtypes.Lease, 0, len(f.leases))
	for _, l := range f.leases {
		if l.State == state {
			out = append(out, l)
		}
	}
	// Deterministic order keeps failure output readable; the reconciler itself
	// does not depend on it.
	slices.SortFunc(out, func(a, b billingtypes.Lease) int { return strings.Compare(a.Uuid, b.Uuid) })
	return out
}

// ---------------------------------------------------------------------------
// The destructive-action invariant
// ---------------------------------------------------------------------------

// worldState captures everything a sweep could destroy, so a test can
// assert that a degraded sweep changed none of it.
type worldState struct {
	placements     map[string]string
	placementSetAt map[string]time.Time
	payloadPresent map[string]bool
	provisionCalls map[string]int // "backend/lease" -> count
	deprovCalls    map[string]int
	acked          []string
	rejected       []string
	closed         []string
}

// captureState records the current world. Take one before the sweep under test
// and pass it to assertNothingDestroyed afterwards.
func (f *fleet) captureState(leaseUUIDs []string) worldState {
	f.t.Helper()

	st := worldState{
		placements:     map[string]string{},
		placementSetAt: map[string]time.Time{},
		payloadPresent: map[string]bool{},
		provisionCalls: map[string]int{},
		deprovCalls:    map[string]int{},
	}

	for _, uuid := range leaseUUIDs {
		if f.placement != nil {
			st.placements[uuid] = f.placement.Get(uuid)
			if at, ok := f.placement.SetAt(uuid); ok {
				st.placementSetAt[uuid] = at
			}
		}
		has, err := f.payloads.Has(uuid)
		require.NoError(f.t, err)
		st.payloadPresent[uuid] = has

		for _, srv := range f.servers {
			st.provisionCalls[srv.name+"/"+uuid] = srv.provisionCount(uuid)
			st.deprovCalls[srv.name+"/"+uuid] = srv.deprovisionCount(uuid)
		}
	}

	st.acked, st.rejected, st.closed = f.chainCalls()
	return st
}

// assertNothingDestroyed is the invariant every degraded-sweep scenario shares:
// a sweep that could not see the whole fleet must not have provisioned,
// deprovisioned, acknowledged, rejected, closed, or forgotten anything about
// the given leases.
//
// It asserts call COUNTS rather than the absence of an error, because the
// failure this guards against — laying an empty volume over live data — reports
// success to its caller.
func (f *fleet) assertNothingDestroyed(before worldState, leaseUUIDs []string) {
	f.t.Helper()

	after := f.captureState(leaseUUIDs)

	for _, uuid := range leaseUUIDs {
		for _, srv := range f.servers {
			key := srv.name + "/" + uuid
			require.Equalf(f.t, before.provisionCalls[key], after.provisionCalls[key],
				"lease %s was provisioned on %s during a degraded sweep", uuid, srv.name)
			require.Equalf(f.t, before.deprovCalls[key], after.deprovCalls[key],
				"lease %s was deprovisioned on %s during a degraded sweep", uuid, srv.name)
		}

		require.Equalf(f.t, before.placements[uuid], after.placements[uuid],
			"placement record for lease %s changed during a degraded sweep", uuid)
		require.Equalf(f.t, before.payloadPresent[uuid], after.payloadPresent[uuid],
			"payload presence for lease %s changed during a degraded sweep", uuid)

		require.NotContainsf(f.t, sliceDiff(after.acked, before.acked), uuid,
			"lease %s was acknowledged on chain during a degraded sweep", uuid)
		require.NotContainsf(f.t, sliceDiff(after.rejected, before.rejected), uuid,
			"lease %s was rejected on chain during a degraded sweep", uuid)
		require.NotContainsf(f.t, sliceDiff(after.closed, before.closed), uuid,
			"lease %s was closed on chain during a degraded sweep", uuid)
	}
}

// sliceDiff returns the elements appended to after relative to before.
func sliceDiff(after, before []string) []string {
	if len(after) <= len(before) {
		return nil
	}
	return after[len(before):]
}

// assertPlacementPinned asserts the placement index still points a lease at the
// backend that actually holds its data. This is the record the deferral depends
// on, so losing it converts a transient outage into a permanently unplaced
// lease.
func (f *fleet) assertPlacementPinned(leaseUUID, backendName string) {
	f.t.Helper()
	require.NotNil(f.t, f.placement, "placement store is disabled in this fleet")
	require.Equal(f.t, backendName, f.placement.Get(leaseUUID),
		"lease %s should still be pinned to %s", leaseUUID, backendName)
}

// assertProvisionedExactlyOnce guards the recovery path: a lease deferred
// through an outage and reconciled afterwards must not end up provisioned twice
// across the whole run.
func (f *fleet) assertProvisionedExactlyOnce(leaseUUID string) {
	f.t.Helper()
	total := 0
	var where []string
	for _, srv := range f.servers {
		if n := srv.provisionCount(leaseUUID); n > 0 {
			total += n
			where = append(where, fmt.Sprintf("%s=%d", srv.name, n))
		}
	}
	require.Equalf(f.t, 1, total,
		"lease %s should have been provisioned exactly once across the fleet, got %v", leaseUUID, where)
}
