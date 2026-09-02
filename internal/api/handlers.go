package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/gorilla/websocket"
	"golang.org/x/sync/errgroup"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	restoreapp "github.com/manifest-network/fred/internal/provisioner/restore"
	"github.com/manifest-network/fred/internal/util"
)

// ChainClient defines the chain operations needed by handlers.
type ChainClient interface {
	GetLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error)
	GetActiveLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error)
	Ping(ctx context.Context) error
}

// TokenTrackerInterface defines the interface for token replay protection.
// This interface allows for testing with mock implementations.
type TokenTrackerInterface interface {
	TryUse(signature string) error
	Healthy() error
	Close() error
}

// PlacementLookup provides lease→backend mapping for read-path routing.
type PlacementLookup interface {
	Lookup(leaseUUID string) placement.Placement
	Healthy() error
}

// LifecycleCallbackAuthority exposes only the current durable callback route
// needed for trusted Fred-to-backend maintenance requests. Authorization of an
// inbound callback remains inside the provisioner callback service.
type LifecycleCallbackAuthority interface {
	CurrentLifecycle(leaseUUID string) placement.LifecycleAuthorization
}

// MaintenanceClaims serializes restart/update effects with provisioning,
// restore, deprovision, and reconciliation. The opaque claim is held through
// accepted local settlement (including payload persistence for /update).
type MaintenanceClaims interface {
	TryClaimLeaseNow(string) operation.LeaseClaimResult
	ReleaseLease(operation.LeaseClaim) bool
}

// PlacementBootstrap reports whether this process has completed at least one
// authoritative placement projection since startup. It is intentionally a
// separate optional capability from PlacementLookup: ordinary read routing can
// remain available while new multi-backend side effects are still fail-closed.
type PlacementBootstrap interface {
	InventoryBootstrapped() bool
}

// PayloadStoreHealth reports whether the payload store's bbolt database is
// readable and carries the buckets the store expects. The store has always had
// a Healthy method; nothing called it, so a payloads.db that had been lost,
// truncated or opened against the wrong path was invisible to both the health
// endpoint and the metrics.
//
// Note the probe is a read-only bbolt View: it proves the database opens and
// its buckets exist, not that a write would succeed. A full or read-only
// filesystem passes it and still fails /update.
//
// Optional: a providerd without payload_store_db_path has no store to probe and
// omits the check entirely.
type PayloadStoreHealth interface {
	Healthy() error
}

// PayloadPersister persists a tenant's updated deployment payload to the store
// the reconciler replays from on reprovision.
//
// This is REQUIRED, not optional, and deliberately unlike the optional
// collaborators below: an /update that reaches the backend but not the payload
// store looks completely successful and then silently reverts the tenant to
// their as-created manifest on the next reboot or crash-restart (ENG-619). A nil
// persister would reintroduce exactly that, so UpdateLease refuses the update
// rather than half-applying it.
type PayloadPersister interface {
	// OverwritePayload replaces the stored payload for a lease. It returns an
	// error — rather than a "was it already there" bool like the create path —
	// because its caller has already applied the update to the backend, so a
	// failure here is a durability failure the tenant must be told about.
	OverwritePayload(leaseUUID string, payload []byte) error
}

// RestoreService is the HTTP layer's sole restore dependency. The application
// service owns routing and all lifecycle mutations; handlers only authenticate,
// parse, invoke, and translate its typed outcome.
type RestoreService interface {
	Execute(ctx context.Context, command restoreapp.Command) restoreapp.Result
}

// Handlers contains HTTP request handlers.
type Handlers struct {
	client             ChainClient
	backendRouter      *backend.Router
	tokenTracker       TokenTrackerInterface
	statusChecker      StatusChecker
	placementLookup    PlacementLookup
	lifecycleCallbacks LifecycleCallbackAuthority
	maintenanceClaims  MaintenanceClaims
	placementBootstrap PlacementBootstrap
	restoreService     RestoreService
	payloadPersister   PayloadPersister
	payloadStoreHealth PayloadStoreHealth
	eventBroker        *EventBroker
	wsUpgrader         websocket.Upgrader
	wsMaxMessageSize   int64         // max bytes the server will read from a client message on /events
	wsMaxConnLifetime  time.Duration // max lifetime of an /events subscription before forced reconnect
	providerUUID       string
	bech32Prefix       string
	callbackBaseURL    string
}

// HandlersConfig configures a Handlers instance.
type HandlersConfig struct {
	Client             ChainClient
	BackendRouter      *backend.Router
	TokenTracker       TokenTrackerInterface      // optional but recommended for replay attack protection
	StatusChecker      StatusChecker              // optional but required for the /status endpoint
	PlacementLookup    PlacementLookup            // optional — used for routing reads to the correct backend
	LifecycleCallbacks LifecycleCallbackAuthority // required for restart/update; legacy owners are explicit store verdicts
	MaintenanceClaims  MaintenanceClaims          // required for restart/update serialization with reconciliation
	RestoreService     RestoreService             // required by /restore; owns typed operation and placement capabilities
	PayloadPersister   PayloadPersister           // REQUIRED for /update — without it an update cannot be made durable (ENG-619)
	PayloadStoreHealth PayloadStoreHealth         // optional — health probe for the payload store's bbolt DB
	EventBroker        *EventBroker               // optional — if nil, the events endpoint will return 501
	ProviderUUID       string
	Bech32Prefix       string
	CallbackBaseURL    string // used for restart/update callbacks to the backend
}

// NewHandlers creates a new Handlers instance.
func NewHandlers(cfg HandlersConfig) *Handlers {
	var placementBootstrap PlacementBootstrap
	if bootstrap, ok := cfg.PlacementLookup.(PlacementBootstrap); ok {
		placementBootstrap = bootstrap
	}
	restoreService := cfg.RestoreService
	if util.IsNilInterface(restoreService) {
		restoreService = nil
	}
	lifecycleCallbacks := cfg.LifecycleCallbacks
	if util.IsNilInterface(lifecycleCallbacks) {
		lifecycleCallbacks = nil
	}
	if lifecycleCallbacks == nil {
		if authority, ok := cfg.PlacementLookup.(LifecycleCallbackAuthority); ok &&
			!util.IsNilInterface(authority) {
			lifecycleCallbacks = authority
		}
	}
	maintenanceClaims := cfg.MaintenanceClaims
	if util.IsNilInterface(maintenanceClaims) {
		maintenanceClaims = nil
	}
	return &Handlers{
		client:             cfg.Client,
		backendRouter:      cfg.BackendRouter,
		tokenTracker:       cfg.TokenTracker,
		statusChecker:      cfg.StatusChecker,
		placementLookup:    cfg.PlacementLookup,
		lifecycleCallbacks: lifecycleCallbacks,
		maintenanceClaims:  maintenanceClaims,
		placementBootstrap: placementBootstrap,
		restoreService:     restoreService,
		payloadPersister:   cfg.PayloadPersister,
		payloadStoreHealth: cfg.PayloadStoreHealth,
		eventBroker:        cfg.EventBroker,
		wsUpgrader: websocket.Upgrader{
			// Allow all origins: this API is not browser-facing. Clients are
			// CLI tools and services that authenticate with cryptographically
			// signed ADR-036 tokens (no cookies/sessions). Origin checks would
			// break non-browser clients that don't send Origin headers.
			CheckOrigin: func(r *http.Request) bool { return true },
		},
		wsMaxMessageSize:  wsDefaultMaxMessageSize,
		wsMaxConnLifetime: wsDefaultMaxConnLifetime,
		providerUUID:      cfg.ProviderUUID,
		bech32Prefix:      cfg.Bech32Prefix,
		callbackBaseURL:   cfg.CallbackBaseURL,
	}
}

func (h *Handlers) maintenanceCallbackURL(
	leaseUUID, backendName string,
) (string, error) {
	if h.lifecycleCallbacks == nil {
		return "", errors.New("lifecycle callback authority is unavailable")
	}

	authorization := h.lifecycleCallbacks.CurrentLifecycle(leaseUUID)
	switch authorization.Verdict() {
	case placement.LifecycleVerdictAuthorized:
		if authorization.Backend() != backendName {
			return "", fmt.Errorf(
				"lifecycle callback backend %q does not match routed backend %q",
				authorization.Backend(), backendName,
			)
		}
		callbackURL, err := provisioner.BuildCallbackURLForLifecycle(
			h.callbackBaseURL, authorization.ID(),
		)
		if err != nil {
			return "", fmt.Errorf("build typed lifecycle callback URL: %w", err)
		}
		return callbackURL, nil
	case placement.LifecycleVerdictLegacy:
		if authorization.Backend() != backendName {
			return "", fmt.Errorf(
				"legacy lifecycle callback backend %q does not match routed backend %q",
				authorization.Backend(), backendName,
			)
		}
		callbackURL, err := provisioner.BuildCallbackURL(h.callbackBaseURL)
		if err != nil {
			return "", fmt.Errorf("build legacy lifecycle callback URL: %w", err)
		}
		return callbackURL, nil
	case placement.LifecycleVerdictInvalid,
		placement.LifecycleVerdictMissing,
		placement.LifecycleVerdictStale,
		placement.LifecycleVerdictUnusable,
		placement.LifecycleVerdictTeardownOnly,
		placement.LifecycleVerdictRetired:
		return "", fmt.Errorf(
			"lifecycle callback authority is not current: verdict %d",
			authorization.Verdict(),
		)
	default:
		return "", fmt.Errorf(
			"unknown lifecycle callback authority verdict %d",
			authorization.Verdict(),
		)
	}
}

// dispatchWithOrderedStart preserves subscriber-visible causality across the
// asynchronous backend boundary. A backend may complete quickly enough to post
// its callback before its acceptance response reaches this handler (both an
// embedded actor and a remote HTTP backend can do so). EventBroker queues that
// callback without blocking it, then publishes the accepted start first. A
// synchronous refusal emits no start event.
func (h *Handlers) dispatchWithOrderedStart(
	leaseUUID string,
	status backend.ProvisionStatus,
	dispatch func() (accepted bool, err error),
) (bool, error) {
	if h.eventBroker == nil {
		return dispatch()
	}
	return h.eventBroker.DispatchWithOrderedSettlement(backend.LeaseStatusEvent{
		LeaseUUID: leaseUUID,
		Status:    status,
		Timestamp: time.Now(),
	}, dispatch)
}

func (h *Handlers) claimMaintenanceLease(
	w http.ResponseWriter,
	leaseUUID string,
) (operation.LeaseClaim, bool) {
	if h.maintenanceClaims == nil {
		slog.Error("maintenance rejected: lifecycle claim authority unavailable",
			"lease_uuid", leaseUUID,
		)
		writeError(w, errMsgServiceUnavailable, http.StatusServiceUnavailable)
		return operation.LeaseClaim{}, false
	}
	result := h.maintenanceClaims.TryClaimLeaseNow(leaseUUID)
	if result.Acquired() {
		return result.Claim(), true
	}
	if result.Outcome() == operation.LeaseClaimBusy {
		writeError(w, "lease is already undergoing a lifecycle operation", http.StatusConflict)
		return operation.LeaseClaim{}, false
	}
	slog.Error("maintenance rejected: failed to claim lease lifecycle",
		"lease_uuid", leaseUUID,
		"outcome", result.Outcome(),
	)
	writeError(w, errMsgServiceUnavailable, http.StatusServiceUnavailable)
	return operation.LeaseClaim{}, false
}

func (h *Handlers) releaseMaintenanceLease(leaseUUID string, claim operation.LeaseClaim) {
	if !h.maintenanceClaims.ReleaseLease(claim) {
		slog.Error("failed to release maintenance lifecycle claim", "lease_uuid", leaseUUID)
	}
}

const maintenanceChainConfirmationTimeout = 10 * time.Second

// confirmMaintenanceLease closes the authentication-to-dispatch race for
// restart and update. Authentication's ACTIVE lookup happens before the local
// lifecycle claim; once that claim is held, re-read the unfiltered exact lease
// so a close, rejection, expiry, ownership change, or inconsistent response
// cannot be followed by a backend mutation based on the stale authentication
// snapshot.
func (h *Handlers) confirmMaintenanceLease(
	w http.ResponseWriter,
	ctx context.Context,
	leaseUUID, tenant string,
) (*billingtypes.Lease, bool) {
	confirmCtx, cancel := context.WithTimeout(ctx, maintenanceChainConfirmationTimeout)
	defer cancel()
	lease, err := h.client.GetLease(confirmCtx, leaseUUID)
	if err != nil {
		slog.Warn("maintenance rejected: exact lease confirmation unavailable",
			"error", err, "lease_uuid", leaseUUID)
		writeError(w, errMsgServiceUnavailable, http.StatusServiceUnavailable)
		return nil, false
	}
	if lease == nil {
		writeError(w, errMsgLeaseNotFound, http.StatusNotFound)
		return nil, false
	}
	if lease.Uuid != leaseUUID {
		slog.Error("maintenance rejected: exact lease query returned another lease",
			"requested_lease_uuid", leaseUUID, "returned_lease_uuid", lease.Uuid)
		writeError(w, errMsgServiceUnavailable, http.StatusServiceUnavailable)
		return nil, false
	}
	if lease.Tenant != tenant || lease.ProviderUuid != h.providerUUID {
		slog.Warn("maintenance rejected: exact lease authority changed",
			"lease_uuid", leaseUUID,
			"token_tenant", tenant,
			"lease_tenant", lease.Tenant,
			"lease_provider_uuid", lease.ProviderUuid,
			"our_provider_uuid", h.providerUUID,
		)
		writeError(w, errMsgForbidden, http.StatusForbidden)
		return nil, false
	}
	if lease.State != billingtypes.LEASE_STATE_ACTIVE {
		writeError(w, "lease is no longer active", http.StatusConflict)
		return nil, false
	}
	return lease, true
}

func (h *Handlers) resolveMaintenanceBackend(
	w http.ResponseWriter,
	leaseUUID string,
	lease *billingtypes.Lease,
) (backend.Backend, bool) {
	if h.backendRouter == nil {
		slog.Error("backend router not configured")
		writeError(w, errMsgServiceNotConfigured, http.StatusServiceUnavailable)
		return nil, false
	}
	sku := provisioner.ExtractRoutingSKU(lease)
	resolved := h.resolveBackend(leaseUUID, sku)
	if resolved == nil {
		slog.Error("no backend available", "sku", sku, "lease_uuid", leaseUUID)
		writeError(w, errMsgServiceUnavailable, http.StatusServiceUnavailable)
		return nil, false
	}
	return resolved, true
}

func (h *Handlers) requireMaintenanceRouter(w http.ResponseWriter) bool {
	if h.backendRouter != nil {
		return true
	}
	slog.Error("backend router not configured")
	writeError(w, errMsgServiceNotConfigured, http.StatusServiceUnavailable)
	return false
}

// AuthenticatedRequest contains the result of a successful authentication.
type AuthenticatedRequest struct {
	Token *AuthToken
	Lease *billingtypes.Lease
}

// AuthenticateLeaseRequest performs common authentication and authorization for lease endpoints.
// It extracts and validates the bearer token, optionally checks for replay attacks,
// queries the lease from chain, and verifies tenant and provider ownership.
//
// Parameters:
//   - r: the HTTP request
//   - leaseUUID: the lease UUID from the URL path
//   - checkReplay: whether to check for token replay (set false for idempotent/read-heavy endpoints like status)
//   - requireActive: if true, only ACTIVE leases are accepted; if false, any state is allowed
//
// Returns AuthenticatedRequest on success, or an error with the appropriate HTTP status code.
func (h *Handlers) AuthenticateLeaseRequest(r *http.Request, leaseUUID string, checkReplay bool, requireActive bool) (*AuthenticatedRequest, int, error) {
	token, status, err := h.authenticateLeaseToken(r, leaseUUID, checkReplay)
	if err != nil {
		return nil, status, err
	}

	lease, status, err := verifyLeaseAccess(r.Context(), h.client, h.providerUUID, leaseUUID, token.Tenant, requireActive)
	if err != nil {
		return nil, status, err
	}

	return &AuthenticatedRequest{
		Token: token,
		Lease: lease,
	}, http.StatusOK, nil
}

// authenticateLeaseToken performs the pre-chain half of AuthenticateLeaseRequest:
// validate the lease UUID format, extract/validate the ADR-036-signed bearer
// token, run the optional replay check, and confirm the token's lease UUID
// matches the request. It does NOT query the chain — callers needing the
// closed-lease authz fallback (ENG-329) verify the lease separately so they can
// fall back to the retained record's tenant when the chain has pruned the lease.
func (h *Handlers) authenticateLeaseToken(r *http.Request, leaseUUID string, checkReplay bool) (*AuthToken, int, error) {
	// Validate lease UUID format
	if !config.IsValidUUID(leaseUUID) {
		return nil, http.StatusBadRequest, errors.New(errMsgInvalidLeaseUUID)
	}

	// Use pre-validated token from middleware context if available (avoids redundant
	// ECDSA verification). Falls back to self-validate when rate limiting is disabled.
	token := AuthTokenFromContext(r.Context())
	if token == nil {
		var err error
		token, err = h.extractToken(r)
		if err != nil {
			return nil, http.StatusUnauthorized, errors.New(errMsgUnauthorized)
		}
		if err := token.Validate(h.bech32Prefix); err != nil {
			return nil, http.StatusUnauthorized, errors.New(errMsgUnauthorized)
		}
	}

	// Check for token replay attack (if tracker is configured and checkReplay is true)
	if checkReplay && h.tokenTracker != nil {
		if err := h.tokenTracker.TryUse(token.Signature); err != nil {
			if errors.Is(err, ErrTokenAlreadyUsed) {
				slog.Warn("token replay detected",
					"lease_uuid", leaseUUID,
					"tenant", token.Tenant,
				)
				return nil, http.StatusUnauthorized, errors.New(errMsgUnauthorized)
			}
			// Database error - fail closed to prevent potential replay attacks.
			// Token lifetime is short (30s), so clients can retry with a fresh token.
			slog.Error("token tracker unavailable", "error", err)
			return nil, http.StatusServiceUnavailable, errors.New(errMsgServiceUnavailable)
		}
	}

	// Verify the token's lease UUID matches the request
	if token.LeaseUUID != leaseUUID {
		slog.Warn("lease UUID mismatch",
			"token_lease_uuid", token.LeaseUUID,
			"request_lease_uuid", leaseUUID,
		)
		return nil, http.StatusUnauthorized, errors.New(errMsgUnauthorized)
	}

	return token, http.StatusOK, nil
}

// authenticateAndResolve performs the common handler preamble: extract lease UUID
// from the path, authenticate, verify the backend router is configured, and resolve
// the correct backend for the lease. On failure it writes the appropriate HTTP error
// and returns ok=false; the caller should return immediately.
func (h *Handlers) authenticateAndResolve(w http.ResponseWriter, r *http.Request, checkReplay, requireActive bool) (auth *AuthenticatedRequest, leaseUUID string, b backend.Backend, ok bool) {
	leaseUUID = r.PathValue("lease_uuid")

	auth, status, err := h.AuthenticateLeaseRequest(r, leaseUUID, checkReplay, requireActive)
	if err != nil {
		writeError(w, err.Error(), status)
		return nil, leaseUUID, nil, false
	}

	if h.backendRouter == nil {
		slog.Error("backend router not configured")
		writeError(w, errMsgServiceNotConfigured, http.StatusServiceUnavailable)
		return nil, leaseUUID, nil, false
	}

	sku := provisioner.ExtractRoutingSKU(auth.Lease)
	b = h.resolveBackend(leaseUUID, sku)
	if b == nil {
		slog.Error("no backend available", "sku", sku, "lease_uuid", leaseUUID)
		writeError(w, "service unavailable", http.StatusServiceUnavailable)
		return nil, leaseUUID, nil, false
	}

	return auth, leaseUUID, b, true
}

// authenticateLease authenticates a lease request WITHOUT resolving a backend.
// Restore uses this because its application service resolves the backend from
// the SOURCE lease's durable placement, not the path-param target lease.
func (h *Handlers) authenticateLease(w http.ResponseWriter, r *http.Request, checkReplay, requireActive bool) (auth *AuthenticatedRequest, leaseUUID string, ok bool) {
	leaseUUID = r.PathValue("lease_uuid")

	auth, status, err := h.AuthenticateLeaseRequest(r, leaseUUID, checkReplay, requireActive)
	if err != nil {
		writeError(w, err.Error(), status)
		return nil, leaseUUID, false
	}

	return auth, leaseUUID, true
}

// resolveBackend determines the correct backend for a lease.
// Checks placement first (handles round-robin routing), falls back to SKU routing
// only for a lease with no placement record.
//
// A record naming a backend the router does not know returns nil, which the
// caller surfaces as 503 (ENG-635). It deliberately does NOT fall back to SKU
// routing: that answers the tenant's read from a machine which never held their
// lease, so a running deployment reads back as absent. Returning nil is not a
// degradation here — it is the difference between "temporarily unavailable" and
// a confidently wrong answer.
func (h *Handlers) resolveBackend(leaseUUID, sku string) backend.Backend {
	if h.placementLookup != nil {
		p := h.placementLookup.Lookup(leaseUUID)
		switch p.State() {
		case placement.StateConfirmed:
			name := p.Backend
			b := h.backendRouter.GetBackendByName(name)
			if b == nil {
				slog.Error("placement backend not found in router; refusing to serve from another backend",
					"lease_uuid", leaseUUID,
					"placement_backend", name,
				)
			}
			return b
		case placement.StateUnusable:
			// A corrupt record is not absence. Refuse to guess a backend until a
			// durable conflict is reconciled or an operator repairs a structurally
			// unreadable record.
			slog.Error("placement record is unusable; refusing to serve from a guessed backend",
				"lease_uuid", leaseUUID,
			)
			return nil
		case placement.StateAttempting:
			// The attempted backend may already hold the lease even though its
			// response was lost. SKU routing could select a different backend and
			// turn that uncertainty into a false not-found response. Wait for
			// authoritative inventory to confirm or disprove the attempt (ENG-632).
			slog.Error("placement attempt is unresolved; refusing to serve from a guessed backend",
				"lease_uuid", leaseUUID,
				"placement_attempt", p.Attempt,
			)
			return nil
		case placement.StateAbsent:
			// A genuinely absent record has no backend evidence, so ordinary SKU
			// routing remains safe.
		}
	}
	return h.backendRouter.Route(sku)
}

// restoreHint is the short, human-readable next-step surfaced alongside a
// retained provision so a returning tenant knows how to recover their data.
const restoreHint = "to restore before retained_until: create a fresh PENDING lease of matching shape (the items above), then POST /v1/leases/{new_lease_uuid}/restore with from_lease_uuid set to this lease's UUID"

// findProvisionAcrossBackends performs the bounded fan-out (ENG-329 #4): the
// fallback used when a confirmed placement candidate did not hold the lease, or
// when the caller has no durable owner candidate. It queries
// GetProvision across every backend that could hold the lease: RouteAll(sku) when
// the SKU is known, else Backends(), skipping names already queried from durable
// placement. Returns the first found provision (live or retained); non-holders
// return ErrNotProvisioned and are skipped; a single backend is one call.
//
// Returns:
//   - (info, nil)  when some backend holds the lease;
//   - (nil, nil)   when every candidate reported ErrNotProvisioned (truly absent);
//   - (nil, err)   when no backend held it AND at least one returned a genuine
//     (non-ErrNotProvisioned) error — surfaced so the caller can 500 rather than
//     mask a transient backend failure as a 404.
func (h *Handlers) findProvisionAcrossBackends(
	ctx context.Context,
	leaseUUID, sku string,
	previouslyQueried ...map[string]struct{},
) (*backend.ProvisionInfo, error) {
	if h.backendRouter == nil {
		return nil, nil
	}
	candidates := h.backendRouter.RouteAll(sku)
	if len(candidates) == 0 {
		// Unknown/unmatched SKU (e.g. the chain pruned the lease, so we have no
		// item to derive it from): fan out over every backend.
		candidates = h.backendRouter.Backends()
	}
	var alreadyQueried map[string]struct{}
	if len(previouslyQueried) > 0 {
		alreadyQueried = previouslyQueried[0]
	}
	var firstErr error
	for _, b := range candidates {
		if _, seen := alreadyQueried[b.Name()]; seen {
			continue
		}
		info, err := b.GetProvision(ctx, leaseUUID)
		if err != nil {
			if !errors.Is(err, backend.ErrNotProvisioned) {
				slog.Warn("fan-out GetProvision error, skipping backend",
					"lease_uuid", leaseUUID,
					"backend", b.Name(),
					"error", err,
				)
				if firstErr == nil {
					firstErr = err
				}
			}
			continue
		}
		if info != nil {
			// Stamp the answering backend (BackendName is json:"-", empty over
			// the wire) so downstream logging/diagnostics identify it.
			info.BackendName = b.Name()
			return info, nil
		}
	}
	return nil, firstErr
}

// findProvision locates a lease's provision by querying every configured backend
// named by durable placement before bounded SKU fan-out. This includes confirmed,
// attempted, and conflicting candidates because discovery is read-only; it avoids
// false 404s for holders drained from SKU routing. If all searches miss while the
// durable evidence remains unresolved, the caller receives
// ErrPlacementUnresolvable rather than absence.
func (h *Handlers) findProvision(ctx context.Context, leaseUUID, sku string) (*backend.ProvisionInfo, error) {
	var fastErr error
	queried := make(map[string]struct{})
	placedBackends, placementUnresolved := h.resolveReadBackendsByPlacement(leaseUUID)
	for _, b := range placedBackends {
		queried[b.Name()] = struct{}{}
		info, err := b.GetProvision(ctx, leaseUUID)
		if err == nil && info != nil {
			// BackendName is json:"-", so HTTP backends leave it empty on the
			// wire — stamp the answering backend so downstream logging/diagnostics
			// identify it (mirrors reconciler.go:638).
			info.BackendName = b.Name()
			return info, nil
		}
		// Miss on the placed backend. ErrNotProvisioned (a stale placement pointing
		// at a backend that no longer holds the lease) is benign — fall through to
		// the fan-out. A GENUINE error must NOT be swallowed: retain it
		// so it can be surfaced if the fan-out finds nothing and has no error of
		// its own (preserves the 500-not-404 error-surfacing contract).
		if err != nil && !errors.Is(err, backend.ErrNotProvisioned) && fastErr == nil {
			fastErr = err
		}
	}

	info, fanErr := h.findProvisionAcrossBackends(ctx, leaseUUID, sku, queried)
	if info != nil {
		// A fan-out hit (e.g. stale placement but found elsewhere) wins even over a
		// genuine placed-backend error.
		return info, nil
	}
	if fanErr != nil {
		// A fan-out error takes precedence over the placed-backend's error.
		return nil, fanErr
	}
	if fastErr != nil {
		return nil, fastErr
	}
	if placementUnresolved {
		return nil, fmt.Errorf("%w: lease %s has unresolved placement evidence",
			provisioner.ErrPlacementUnresolvable, leaseUUID)
	}
	return nil, nil
}

// resolveReadBackendsByPlacement returns every configured backend named by a
// durable placement for read-only provision discovery. Reads may safely query a
// confirmed owner, unresolved attempt, or conflict candidate: doing so cannot
// create/move data and avoids false 404s when a possible holder has been drained
// from SKU routing. The boolean remains true when unresolved evidence exists, so
// an all-miss search surfaces temporary unavailability rather than absence.
func (h *Handlers) resolveReadBackendsByPlacement(leaseUUID string) ([]backend.Backend, bool) {
	if h.placementLookup == nil || h.backendRouter == nil {
		return nil, false
	}
	p := h.placementLookup.Lookup(leaseUUID)
	if p.State() == placement.StateAbsent {
		return nil, false
	}

	names := make([]string, 0, 2+len(p.ConflictBackends))
	seen := make(map[string]struct{}, cap(names))
	addName := func(name string) {
		if name == "" {
			return
		}
		if _, exists := seen[name]; exists {
			return
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	addName(p.Backend)
	addName(p.Attempt)
	for _, name := range p.ConflictBackends {
		addName(name)
	}

	unresolved := p.State() != placement.StateConfirmed || p.Attempt != ""
	backends := make([]backend.Backend, 0, len(names))
	for _, name := range names {
		b := h.backendRouter.GetBackendByName(name)
		if b == nil {
			unresolved = true
			continue
		}
		backends = append(backends, b)
	}
	return backends, unresolved
}

// ConnectionResponse represents the response for connection details.
type ConnectionResponse struct {
	LeaseUUID    string            `json:"lease_uuid"`
	Tenant       string            `json:"tenant"`
	ProviderUUID string            `json:"provider_uuid"`
	Connection   ConnectionDetails `json:"connection"`
}

// ConnectionDetails contains the connection information for a lease.
// For multi-instance leases, the Instances array contains per-instance details.
// For stack (multi-service) leases, the Services map contains per-service details.
type ConnectionDetails struct {
	Host      string                              `json:"host"`
	FQDN      string                              `json:"fqdn,omitempty"`
	Ports     map[string]PortMapping              `json:"ports,omitempty"`
	Instances []InstanceInfo                      `json:"instances,omitempty"`
	Services  map[string]ServiceConnectionDetails `json:"services,omitempty"`
	Protocol  string                              `json:"protocol,omitempty"`
	Metadata  map[string]string                   `json:"metadata,omitempty"`
}

// ServiceConnectionDetails contains connection details for a single service in a stack.
type ServiceConnectionDetails struct {
	FQDN      string         `json:"fqdn,omitempty"`
	Instances []InstanceInfo `json:"instances"`
}

// InstanceInfo contains connection details for a single instance in a multi-instance lease.
type InstanceInfo struct {
	InstanceIndex int                    `json:"instance_index"`
	ContainerID   string                 `json:"container_id,omitempty"`
	Image         string                 `json:"image,omitempty"`
	Status        string                 `json:"status,omitempty"`
	FQDN          string                 `json:"fqdn,omitempty"`
	Ports         map[string]PortMapping `json:"ports,omitempty"`
}

// PortMapping represents a port binding from container to host.
type PortMapping struct {
	HostIP   string `json:"host_ip"`
	HostPort int    `json:"host_port"`
}

// ErrorResponse represents an error response.
type ErrorResponse struct {
	Error string `json:"error"`
	Code  int    `json:"code"`
}

// CallbackResponse represents the response for backend callbacks.
// Used to provide debugging information to authenticated backends.
type CallbackResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// Common error messages for API responses.
// These constants ensure consistency across handlers and simplify testing.
const (
	errMsgUnauthorized         = "unauthorized"
	errMsgForbidden            = "forbidden"
	errMsgInternalServerError  = "internal server error"
	errMsgServiceNotConfigured = "service not configured"
	errMsgServiceUnavailable   = "service temporarily unavailable"
	errMsgInvalidLeaseUUID     = "invalid lease UUID format"
	errMsgLeaseNotFound        = "lease not found"
	// errMsgBackendUnusableError is returned when the backend rejected the
	// request but its error body was off-contract, so fred has no diagnostic
	// to relay. Paired with 502: the fault is upstream of the tenant.
	errMsgBackendUnusableError = "the provider backend returned an unusable error; the request was not applied"
)

// maxTenantDetailBytes bounds the endpoint-provided detail fred relays in a 4xx
// body. The client already caps a backend error body at 4 KiB, which is far
// more than a tenant needs to fix a manifest and more than belongs in one
// error line.
const maxTenantDetailBytes = 512

// tenantDetail renders the tenant-facing message for a backend-originated 4xx.
//
// It returns ONLY the declared detail inside a validated error
// envelope — never err.Error() of the whole wrapped chain, which is how a
// stray verbose string reaches a tenant (ENG-508's rule, applied to the
// synchronous path). An error carrying no such detail falls back to fred's own
// curated message rather than to the chain's text.
//
// The detail is bounded and stripped of control characters: it is relayed
// text, and a 4xx body is a place where a newline or an ANSI escape has been
// used to forge log lines and terminal output elsewhere (CVE-2021-25743).
func tenantDetail(err error, fallback string) string {
	detail, ok := backend.Detail(err)
	if !ok || detail == "" {
		return fallback
	}
	return tenantDetailText(detail, fallback)
}

// tenantDetailText applies the tenant-boundary policy to detail already
// extracted from a validated backend error envelope by an application service.
func tenantDetailText(detail, fallback string) string {
	if detail == "" {
		return fallback
	}
	detail = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' {
			return ' '
		}
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, detail)
	if len(detail) > maxTenantDetailBytes {
		// Trim back to a rune boundary so a multi-byte sequence is not split.
		limit := maxTenantDetailBytes
		for limit > 0 && !utf8.RuneStart(detail[limit]) {
			limit--
		}
		detail = detail[:limit] + "…"
	}
	if detail == "" {
		return fallback
	}
	return detail
}

// GetLeaseConnection handles GET /v1/leases/{lease_uuid}/connection
func (h *Handlers) GetLeaseConnection(w http.ResponseWriter, r *http.Request) {
	auth, leaseUUID, backendClient, ok := h.authenticateAndResolve(w, r, true, true)
	if !ok {
		return
	}

	info, err := backendClient.GetInfo(r.Context(), leaseUUID)
	if err != nil {
		if errors.Is(err, backend.ErrNotProvisioned) {
			slog.Warn("lease not yet provisioned", "lease_uuid", leaseUUID)
			writeError(w, "lease not yet provisioned", http.StatusNotFound)
			return
		}
		slog.Error("failed to get info from backend", "error", err, "lease_uuid", leaseUUID)
		writeError(w, errMsgInternalServerError, http.StatusInternalServerError)
		return
	}

	// Build response with lease info from backend
	response := ConnectionResponse{
		LeaseUUID:    leaseUUID,
		Tenant:       auth.Lease.Tenant,
		ProviderUUID: h.providerUUID,
		Connection:   extractConnectionDetails(*info),
	}

	slog.Info("lease info served",
		"lease_uuid", leaseUUID,
		"tenant", auth.Token.Tenant,
		"backend", backendClient.Name(),
	)

	writeJSON(w, response, http.StatusOK)
}

// LeaseStatusResponse represents the response for lease status.
// Includes tenant and provider_uuid for consistency with ConnectionResponse.
type LeaseStatusResponse struct {
	LeaseUUID           string `json:"lease_uuid"`
	Tenant              string `json:"tenant"`
	ProviderUUID        string `json:"provider_uuid"`
	State               string `json:"state"`
	RequiresPayload     bool   `json:"requires_payload"`
	MetaHashHex         string `json:"meta_hash_hex,omitempty"` // For debugging - shows the expected payload hash
	PayloadReceived     bool   `json:"payload_received"`
	ProvisioningStarted bool   `json:"provisioning_started"`
	ProvisionStatus     string `json:"provision_status,omitempty"`
	FailCount           int    `json:"fail_count,omitempty"`
	// Reason/Message are the curated, tenant-safe failure signal (ENG-508): a
	// stable machine-readable category code and a short human message. They
	// REPLACE the former verbose last_error field, which leaked exec output and
	// host paths. A FAILED provision always surfaces a Reason (defaulting to
	// "Unknown"); Message may be empty.
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
	// Retention fields (populated when provision_status=retained, ENG-329).
	// RetainedUntil is RFC3339; Items is the restore shape (service name + SKU +
	// quantity) the tenant uses to build a matching fresh PENDING lease;
	// RestoreHint is a short human-readable next-step. The owning Tenant from
	// ProvisionInfo is intentionally NOT surfaced here (cross-tenant leak guard);
	// Partition IS surfaced — it is the owning tenant's own sub-grouping key, shown
	// only to that tenant (this response is owner-authenticated), so no cross-tenant
	// disclosure. Empty (omitempty absent) for non-partitioned retained records.
	RetainedUntil string              `json:"retained_until,omitempty"`
	Items         []backend.LeaseItem `json:"items,omitempty"`
	RestoreHint   string              `json:"restore_hint,omitempty"`
	Partition     string              `json:"partition,omitempty"`
}

// GetLeaseStatus handles GET /v1/leases/{lease_uuid}/status
//
// Authz is chain-primary with a retained-record fallback (ENG-329 #5): the
// ADR-036-signed token is validated first, then the chain lease is queried
// (any state). If the chain still has the lease, tenant/provider ownership is
// verified against it (the existing path). If the chain has PRUNED the lease
// (auto-closed cohort), the request is authorized iff the signed caller's
// tenant equals the retained record's Tenant, surfaced via the bounded fan-out
// GetProvision — mirroring restore's cross-tenant guard. A cross-tenant caller,
// or an absent retained record, is rejected.
func (h *Handlers) GetLeaseStatus(w http.ResponseWriter, r *http.Request) {
	leaseUUID := r.PathValue("lease_uuid")

	// Pre-chain auth (no replay check for this read-heavy endpoint).
	token, status, err := h.authenticateLeaseToken(r, leaseUUID, false)
	if err != nil {
		writeError(w, err.Error(), status)
		return
	}

	// Query the chain (any lease state). nil means the chain has pruned a
	// closed/expired lease — fall back to the retained record below.
	lease, status, err := verifyLeaseAccess(r.Context(), h.client, h.providerUUID, leaseUUID, token.Tenant, false)
	if err != nil && status != http.StatusNotFound {
		writeError(w, err.Error(), status)
		return
	}

	if lease == nil {
		// Chain-absent: authorize and answer solely from the retained record.
		h.serveRetainedStatusFallback(w, r, leaseUUID, token.Tenant)
		return
	}

	// Build status response from the chain lease.
	hasMetaHash := len(lease.MetaHash) > 0
	response := LeaseStatusResponse{
		LeaseUUID:       leaseUUID,
		Tenant:          token.Tenant,
		ProviderUUID:    h.providerUUID,
		State:           lease.State.String(),
		RequiresPayload: hasMetaHash,
	}
	if hasMetaHash {
		response.MetaHashHex = hex.EncodeToString(lease.MetaHash)
	}

	// Check provisioning status if checker is available
	if h.statusChecker != nil {
		hasPayload, err := h.statusChecker.HasPayload(leaseUUID)
		if err != nil {
			slog.Warn("failed to check payload status", "lease_uuid", leaseUUID, "error", err)
		}
		response.PayloadReceived = hasPayload
		response.ProvisioningStarted = h.statusChecker.IsInFlight(leaseUUID)
	}

	// Include provision status from the backend for ANY lease state (ENG-329
	// lifts the former ACTIVE-only gate so a non-ACTIVE lease whose data was
	// retained surfaces provision_status=retained). findProvision takes the O(1)
	// placement fast-path (ACTIVE leases, and retained leases whose placement
	// ENG-333 keeps alive on close) and falls back to the bounded fan-out otherwise.
	// Errors are intentionally ignored — provision status on /status is
	// best-effort and ErrNotProvisioned during initial setup is expected.
	if h.backendRouter != nil {
		sku := provisioner.ExtractRoutingSKU(lease)
		info, fanErr := h.findProvision(r.Context(), leaseUUID, sku)
		if fanErr != nil {
			slog.Warn("provision status lookup failed (best-effort)", "lease_uuid", leaseUUID, "error", fanErr)
		}
		if info != nil {
			response.ProvisionStatus = string(info.Status)
			response.FailCount = info.FailCount
			response.Reason = provisionReason(info)
			response.Message = info.Message
			applyRetentionFields(&response, info)
		}
	}

	slog.Info("lease status served",
		"lease_uuid", leaseUUID,
		"tenant", token.Tenant,
		"state", response.State,
	)

	writeJSON(w, response, http.StatusOK)
}

// serveRetainedStatusFallback answers GET /status for a lease the chain has
// pruned. It authorizes via the retained record's tenant (ENG-329 #5) and, on a
// match, returns provision_status=retained with the restore shape. A
// cross-tenant caller or an absent/non-retained record yields 404 (never
// authorize on an empty/mismatched tenant).
func (h *Handlers) serveRetainedStatusFallback(w http.ResponseWriter, r *http.Request, leaseUUID, callerTenant string) {
	// SKU is unknown (no chain lease to derive it from): fan out over all backends.
	// A genuine fan-out error is logged but treated as "not found" — we must not
	// confirm a lease's existence to an unauthenticated-against-chain caller.
	info, fanErr := h.findProvisionAcrossBackends(r.Context(), leaseUUID, "")
	if fanErr != nil {
		slog.Warn("retained fallback fan-out failed", "lease_uuid", leaseUUID, "error", fanErr)
	}
	if !authorizeRetained(info, callerTenant) {
		// Indistinguishable from a genuinely unknown lease — do not leak existence.
		writeError(w, errMsgLeaseNotFound, http.StatusNotFound)
		return
	}

	response := LeaseStatusResponse{
		LeaseUUID:       leaseUUID,
		Tenant:          callerTenant,
		ProviderUUID:    h.providerUUID,
		State:           billingtypes.LEASE_STATE_UNSPECIFIED.String(),
		ProvisionStatus: string(info.Status),
		FailCount:       info.FailCount,
		Reason:          provisionReason(info),
		Message:         info.Message,
	}
	applyRetentionFields(&response, info)

	slog.Info("lease status served from retained record (chain-pruned lease)",
		"lease_uuid", leaseUUID,
		"tenant", callerTenant,
	)

	writeJSON(w, response, http.StatusOK)
}

// authorizeRetained returns true iff info is a retained record owned by
// callerTenant. It rejects a nil/non-retained record and a mismatched OR empty
// tenant — never authorize on an empty tenant (the isolation boundary).
func authorizeRetained(info *backend.ProvisionInfo, callerTenant string) bool {
	if info == nil || info.Status != backend.ProvisionStatusRetained {
		return false
	}
	if callerTenant == "" || info.Tenant == "" {
		return false
	}
	return info.Tenant == callerTenant
}

// provisionReason returns the tenant-facing machine reason for a provision,
// applying the ReasonUnknown read-boundary default so a FAILED provision never
// surfaces an empty reason to the tenant (ENG-508). The docker/k3s store
// boundaries already default, but a legacy or misbehaving HTTP backend may hand
// back a failed status with no authored reason — this is the fail-closed
// backstop at the API boundary. Non-failed statuses pass through unchanged
// (retained/ready/etc. legitimately carry an empty reason).
func provisionReason(info *backend.ProvisionInfo) string {
	if info.Reason == "" && info.Status == backend.ProvisionStatusFailed {
		return string(backend.ReasonUnknown)
	}
	return string(info.Reason)
}

// applyRetentionFields populates the retention-specific response fields when the
// provision is retained. Tenant from ProvisionInfo is intentionally NOT copied.
func applyRetentionFields(resp *LeaseStatusResponse, info *backend.ProvisionInfo) {
	if info.Status != backend.ProvisionStatusRetained {
		return
	}
	if !info.RetainedUntil.IsZero() {
		resp.RetainedUntil = info.RetainedUntil.Format(time.RFC3339)
	}
	resp.Items = info.Items
	resp.RestoreHint = restoreHint
	resp.Partition = info.Partition
}

// LeaseProvisionResponse represents the response for provision diagnostics.
type LeaseProvisionResponse struct {
	LeaseUUID    string `json:"lease_uuid"`
	Tenant       string `json:"tenant"`
	ProviderUUID string `json:"provider_uuid"`
	Status       string `json:"status"`
	FailCount    int    `json:"fail_count"`
	// Reason/Message are the curated, tenant-safe failure signal (ENG-508),
	// replacing the former verbose last_error. See LeaseStatusResponse.
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
	// Retention fields (populated when status=retained, ENG-329). See
	// LeaseStatusResponse; the owning Tenant from ProvisionInfo is NOT surfaced,
	// but Partition IS (the owner's own sub-grouping key, shown only to the owner).
	RetainedUntil string              `json:"retained_until,omitempty"`
	Items         []backend.LeaseItem `json:"items,omitempty"`
	RestoreHint   string              `json:"restore_hint,omitempty"`
	Partition     string              `json:"partition,omitempty"`
}

// LeaseLogsResponse represents the response for container logs.
type LeaseLogsResponse struct {
	LeaseUUID    string            `json:"lease_uuid"`
	Tenant       string            `json:"tenant"`
	ProviderUUID string            `json:"provider_uuid"`
	Logs         map[string]string `json:"logs"`
}

// GetLeaseProvision handles GET /v1/leases/{lease_uuid}/provision
//
// Like GetLeaseStatus, authz is chain-primary with a retained-record fallback
// (ENG-329 #5), and provision discovery uses findProvision (placement fast-path,
// bounded fan-out fallback). Within the grace window a retained lease returns
// status=retained (with retained_until + items), not 404.
func (h *Handlers) GetLeaseProvision(w http.ResponseWriter, r *http.Request) {
	leaseUUID := r.PathValue("lease_uuid")

	token, status, err := h.authenticateLeaseToken(r, leaseUUID, false)
	if err != nil {
		writeError(w, err.Error(), status)
		return
	}

	// Query the chain (any state). A tenant/provider mismatch (403) or other
	// non-NotFound error surfaces here; nil lease means the chain pruned it.
	lease, status, err := verifyLeaseAccess(r.Context(), h.client, h.providerUUID, leaseUUID, token.Tenant, false)
	if err != nil && status != http.StatusNotFound {
		writeError(w, err.Error(), status)
		return
	}

	if h.backendRouter == nil {
		slog.Error("backend router not configured")
		writeError(w, errMsgServiceNotConfigured, http.StatusServiceUnavailable)
		return
	}

	var sku string
	if lease != nil {
		sku = provisioner.ExtractRoutingSKU(lease)
	}
	// Placement fast-path for the ACTIVE common case; bounded fan-out otherwise.
	info, fanErr := h.findProvision(r.Context(), leaseUUID, sku)

	switch {
	case lease == nil:
		// Chain-pruned: the caller is NOT yet authorized (the chain can no longer
		// vouch for ownership). Authorize solely against the retained record's
		// tenant. A genuine fan-out error is logged but treated as "not found" —
		// surfacing a 500 here would leak backend-health/existence to a caller who
		// has not proven ownership (symmetric with serveRetainedStatusFallback).
		if fanErr != nil {
			slog.Warn("provision fan-out failed during chain-pruned authz", "lease_uuid", leaseUUID, "error", fanErr)
		}
		if !authorizeRetained(info, token.Tenant) {
			writeError(w, errMsgLeaseNotFound, http.StatusNotFound)
			return
		}
	case fanErr != nil:
		// Chain has the lease and the caller is already authorized: a genuine
		// backend error (not ErrNotProvisioned) should surface as 500 rather than
		// masking a transient failure as a 404.
		slog.Error("failed to get provision from backend", "error", fanErr, "lease_uuid", leaseUUID)
		if errors.Is(fanErr, provisioner.ErrPlacementUnresolvable) {
			writeError(w, errMsgServiceUnavailable, http.StatusServiceUnavailable)
			return
		}
		writeError(w, errMsgInternalServerError, http.StatusInternalServerError)
		return
	case info == nil:
		// Chain has the lease but no backend holds a provision (live or retained).
		writeError(w, "provision not found", http.StatusNotFound)
		return
	}

	response := LeaseProvisionResponse{
		LeaseUUID:    leaseUUID,
		Tenant:       token.Tenant,
		ProviderUUID: h.providerUUID,
		Status:       string(info.Status),
		FailCount:    info.FailCount,
		Reason:       provisionReason(info),
		Message:      info.Message,
	}
	if info.Status == backend.ProvisionStatusRetained {
		if !info.RetainedUntil.IsZero() {
			response.RetainedUntil = info.RetainedUntil.Format(time.RFC3339)
		}
		response.Items = info.Items
		response.RestoreHint = restoreHint
		response.Partition = info.Partition
	}

	slog.Info("lease provision info served",
		"lease_uuid", leaseUUID,
		"tenant", token.Tenant,
		"status", info.Status,
		"backend", info.BackendName,
	)

	writeJSON(w, response, http.StatusOK)
}

// GetLeaseLogs handles GET /v1/leases/{lease_uuid}/logs
func (h *Handlers) GetLeaseLogs(w http.ResponseWriter, r *http.Request) {
	auth, leaseUUID, backendClient, ok := h.authenticateAndResolve(w, r, false, false)
	if !ok {
		return
	}

	// Parse tail parameter
	tail := 100 // default
	if v := r.URL.Query().Get("tail"); v != "" {
		n, parseErr := strconv.Atoi(v)
		if parseErr != nil || n < 1 {
			writeError(w, "tail must be a positive integer", http.StatusBadRequest)
			return
		}
		if n > 10000 {
			writeError(w, "tail must not exceed 10000", http.StatusBadRequest)
			return
		}
		tail = n
	}

	logs, err := backendClient.GetLogs(r.Context(), leaseUUID, tail)
	if err != nil {
		if errors.Is(err, backend.ErrNotProvisioned) {
			writeError(w, "logs not found", http.StatusNotFound)
			return
		}
		slog.Error("failed to get logs from backend", "error", err, "lease_uuid", leaseUUID)
		writeError(w, errMsgInternalServerError, http.StatusInternalServerError)
		return
	}

	response := LeaseLogsResponse{
		LeaseUUID:    leaseUUID,
		Tenant:       auth.Token.Tenant,
		ProviderUUID: h.providerUUID,
		Logs:         logs,
	}

	slog.Info("lease logs served",
		"lease_uuid", leaseUUID,
		"tenant", auth.Token.Tenant,
		"backend", backendClient.Name(),
	)

	writeJSON(w, response, http.StatusOK)
}

// LeaseReleasesResponse represents the response for release history.
type LeaseReleasesResponse struct {
	LeaseUUID    string                `json:"lease_uuid"`
	Tenant       string                `json:"tenant"`
	ProviderUUID string                `json:"provider_uuid"`
	Releases     []backend.ReleaseInfo `json:"releases"`
}

// RestartLease handles POST /v1/leases/{lease_uuid}/restart
func (h *Handlers) RestartLease(w http.ResponseWriter, r *http.Request) {
	auth, leaseUUID, ok := h.authenticateLease(w, r, true, true)
	if !ok {
		return
	}
	if !h.requireMaintenanceRouter(w) {
		return
	}
	claim, ok := h.claimMaintenanceLease(w, leaseUUID)
	if !ok {
		return
	}
	defer h.releaseMaintenanceLease(leaseUUID, claim)
	exactLease, ok := h.confirmMaintenanceLease(w, r.Context(), leaseUUID, auth.Token.Tenant)
	if !ok {
		return
	}
	backendClient, ok := h.resolveMaintenanceBackend(w, leaseUUID, exactLease)
	if !ok {
		return
	}

	callbackURL, err := h.maintenanceCallbackURL(leaseUUID, backendClient.Name())
	if err != nil {
		slog.Error("restart rejected: lifecycle callback authority unavailable",
			"error", err,
			"lease_uuid", leaseUUID,
			"backend", backendClient.Name(),
		)
		writeError(w, errMsgServiceUnavailable, http.StatusServiceUnavailable)
		return
	}

	_, err = h.dispatchWithOrderedStart(
		leaseUUID,
		backend.ProvisionStatusRestarting,
		func() (bool, error) {
			err := backendClient.Restart(r.Context(), backend.RestartRequest{
				LeaseUUID:   leaseUUID,
				CallbackURL: callbackURL,
			})
			return err == nil, err
		},
	)
	if err != nil {
		if errors.Is(err, backend.ErrNotProvisioned) {
			writeError(w, "lease not yet provisioned", http.StatusNotFound)
			return
		}
		if errors.Is(err, backend.ErrInvalidState) {
			writeError(w, "invalid state for restart", http.StatusConflict)
			return
		}
		slog.Error("failed to restart lease", "error", err, "lease_uuid", leaseUUID)
		writeError(w, errMsgInternalServerError, http.StatusInternalServerError)
		return
	}

	slog.Info("lease restart initiated",
		"lease_uuid", leaseUUID,
		"tenant", auth.Token.Tenant,
		"backend", backendClient.Name(),
	)

	writeJSON(w, map[string]string{"status": "restarting"}, http.StatusAccepted)
}

// RestoreLease handles POST /v1/leases/{lease_uuid}/restore
// (lease_uuid is the NEW, fresh lease; the body names the original retained lease).
func (h *Handlers) RestoreLease(w http.ResponseWriter, r *http.Request) {
	// A fresh restore target is PENDING, so authenticate against GetLease rather
	// than GetActiveLease. Source routing belongs to the restore service.
	auth, leaseUUID, ok := h.authenticateLease(w, r, true, false)
	if !ok {
		return
	}

	// Preserve the request-boundary ordering and error contract: an invalid target
	// state is rejected before the body is considered. The service repeats this
	// invariant so non-HTTP callers cannot bypass it.
	if auth.Lease.State != billingtypes.LEASE_STATE_PENDING {
		writeError(w, "lease is not pending; only a fresh lease can be restored into", http.StatusConflict)
		return
	}

	var body struct {
		FromLeaseUUID string `json:"from_lease_uuid"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if body.FromLeaseUUID == "" {
		writeError(w, "from_lease_uuid is required", http.StatusBadRequest)
		return
	}
	if !config.IsValidUUID(body.FromLeaseUUID) {
		writeError(w, "from_lease_uuid is not a valid UUID", http.StatusBadRequest)
		return
	}
	if body.FromLeaseUUID == leaseUUID {
		writeError(w, "from_lease_uuid must differ from the restore target UUID", http.StatusBadRequest)
		return
	}

	if h.restoreService == nil {
		slog.Error("restore service not configured")
		writeError(w, errMsgServiceNotConfigured, http.StatusServiceUnavailable)
		return
	}

	result := h.restoreService.Execute(r.Context(), restoreapp.Command{
		TargetLeaseUUID: leaseUUID,
		Tenant:          auth.Token.Tenant,
		SourceLeaseUUID: body.FromLeaseUUID,
	})
	h.writeRestoreResult(w, leaseUUID, body.FromLeaseUUID, result)
}

func (h *Handlers) writeRestoreResult(
	w http.ResponseWriter,
	leaseUUID, sourceLeaseUUID string,
	result restoreapp.Result,
) {
	switch result.Outcome {
	case restoreapp.OutcomeAccepted:
		// Tenant-facing restore remains "provisioning" even though the backend
		// actor and event stream use the internal Restarting state.
		writeJSON(w, map[string]string{"status": "provisioning"}, http.StatusAccepted)
	case restoreapp.OutcomeTargetNotPending:
		writeError(w, "lease is not pending; only a fresh lease can be restored into", http.StatusConflict)
	case restoreapp.OutcomeSourceNotFound, restoreapp.OutcomeNotRetained:
		writeError(w, "no retained data found for that lease", http.StatusNotFound)
	case restoreapp.OutcomeSourceUnavailable:
		writeError(w, errMsgServiceUnavailable, http.StatusServiceUnavailable)
	case restoreapp.OutcomeAlreadyInProgress:
		writeError(w, "lease is already being provisioned or restored", http.StatusConflict)
	case restoreapp.OutcomeServiceUnavailable:
		writeError(w, errMsgServiceUnavailable, http.StatusServiceUnavailable)
	case restoreapp.OutcomeBackendInvalidState:
		writeError(w, "lease not in a restorable state", http.StatusConflict)
	case restoreapp.OutcomeAlreadyProvisioned:
		writeError(w, "lease already provisioned", http.StatusConflict)
	case restoreapp.OutcomeInsufficientResources:
		writeError(w, "insufficient resources to restore", http.StatusServiceUnavailable)
	case restoreapp.OutcomeBackendUnavailable:
		writeError(w, "restore backend is temporarily unavailable", http.StatusServiceUnavailable)
	case restoreapp.OutcomeTierTooSmall:
		writeError(w, tenantDetailText(result.Detail(), "retained data exceeds the requested smaller tier"),
			http.StatusUnprocessableEntity)
	case restoreapp.OutcomeInvalidRequest:
		writeError(w, tenantDetailText(result.Detail(), "the restore request was rejected as invalid"),
			http.StatusBadRequest)
	case restoreapp.OutcomeBackendRefused:
		writeError(w, tenantDetailText(result.Detail(), "the provider backend refused the restore"),
			http.StatusUnprocessableEntity)
	case restoreapp.OutcomeBackendMalformedResponse:
		slog.Warn("restore rejected with an unusable backend error body",
			"error", result.Cause(), "lease_uuid", leaseUUID, "from_lease", sourceLeaseUUID)
		writeError(w, errMsgBackendUnusableError, http.StatusBadGateway)
	case restoreapp.OutcomeInvalid, restoreapp.OutcomeInternalFailure:
		slog.Error("failed to restore lease",
			"error", result.Cause(), "lease_uuid", leaseUUID, "from_lease", sourceLeaseUUID)
		writeError(w, errMsgInternalServerError, http.StatusInternalServerError)
	default:
		slog.Error("restore service returned an unknown outcome",
			"outcome", result.Outcome, "lease_uuid", leaseUUID, "from_lease", sourceLeaseUUID)
		writeError(w, errMsgInternalServerError, http.StatusInternalServerError)
	}
}

// UpdateLease handles POST /v1/leases/{lease_uuid}/update
func (h *Handlers) UpdateLease(w http.ResponseWriter, r *http.Request) {
	auth, leaseUUID, ok := h.authenticateLease(w, r, true, true)
	if !ok {
		return
	}
	if !h.requireMaintenanceRouter(w) {
		return
	}

	// Read the request body (new manifest payload)
	var updateReq struct {
		Payload []byte `json:"payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&updateReq); err != nil {
		writeError(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if len(updateReq.Payload) == 0 {
		writeError(w, "payload is required", http.StatusBadRequest)
		return
	}
	// Refuse before touching the backend rather than after. An update we cannot
	// persist is one the next reprovision silently undoes (ENG-619), so a lease
	// left running a manifest fred has no durable record of is strictly worse
	// than an update that never happened — the tenant can retry the latter.
	if h.payloadPersister == nil {
		slog.Error("update rejected: no payload persister configured — /update cannot be made durable",
			"lease_uuid", leaseUUID,
		)
		writeError(w, errMsgInternalServerError, http.StatusInternalServerError)
		return
	}
	claim, ok := h.claimMaintenanceLease(w, leaseUUID)
	if !ok {
		return
	}
	defer h.releaseMaintenanceLease(leaseUUID, claim)
	exactLease, ok := h.confirmMaintenanceLease(w, r.Context(), leaseUUID, auth.Token.Tenant)
	if !ok {
		return
	}
	backendClient, ok := h.resolveMaintenanceBackend(w, leaseUUID, exactLease)
	if !ok {
		return
	}

	// Resolve lifecycle authority only after taking the exclusive lease claim.
	// Otherwise an operation that finishes between this read and the claim can
	// rotate the lifecycle generation, leaving the accepted update with a stale
	// callback URL that Fred will correctly discard.
	callbackURL, err := h.maintenanceCallbackURL(leaseUUID, backendClient.Name())
	if err != nil {
		slog.Error("update rejected: lifecycle callback authority unavailable",
			"error", err,
			"lease_uuid", leaseUUID,
			"backend", backendClient.Name(),
		)
		writeError(w, errMsgServiceUnavailable, http.StatusServiceUnavailable)
		return
	}

	// payload_hash has always been part of the documented /update request
	// (README, BACKEND_GUIDE) but was never populated, so a third-party backend
	// implementing the contract received a payload it could not check. Send the
	// hash of the payload actually being sent — the same value the store records
	// and the reprovision path later verifies against.
	accepted, err := h.dispatchWithOrderedStart(
		leaseUUID,
		backend.ProvisionStatusUpdating,
		func() (bool, error) {
			err := backendClient.Update(r.Context(), backend.UpdateRequest{
				LeaseUUID:   leaseUUID,
				CallbackURL: callbackURL,
				Payload:     updateReq.Payload,
				PayloadHash: hex.EncodeToString(payload.ComputeHash(updateReq.Payload)),
			})
			if err != nil {
				return false, err
			}
			return true, h.payloadPersister.OverwritePayload(leaseUUID, updateReq.Payload)
		},
	)
	if err != nil {
		if accepted {
			// The backend accepted the update, so the ordered start remains true
			// even though durable settlement failed. Holding both the event gate
			// and lifecycle claim through this write prevents another update or
			// reconciliation pass from overtaking it.
			slog.Error("lease updated on backend but payload persistence failed — reprovision would revert this lease",
				"error", err,
				"lease_uuid", leaseUUID,
				"tenant", auth.Token.Tenant,
				"backend", backendClient.Name(),
			)
			metrics.PayloadPersistFailuresTotal.WithLabelValues("update").Inc()
			writeError(w, errMsgInternalServerError, http.StatusInternalServerError)
			return
		}
		if errors.Is(err, backend.ErrNotProvisioned) {
			writeError(w, "lease not yet provisioned", http.StatusNotFound)
			return
		}
		if errors.Is(err, backend.ErrInvalidState) {
			writeError(w, "invalid state for update", http.StatusConflict)
			return
		}
		if errors.Is(err, backend.ErrValidation) {
			writeError(w, tenantDetail(err, "the update request was rejected as invalid"),
				http.StatusBadRequest)
			return
		}
		if errors.Is(err, backend.ErrMalformedErrorBody) {
			slog.Warn("update rejected with an unusable backend error body",
				"error", err, "lease_uuid", leaseUUID)
			writeError(w, errMsgBackendUnusableError, http.StatusBadGateway)
			return
		}
		slog.Error("failed to update lease", "error", err, "lease_uuid", leaseUUID)
		writeError(w, errMsgInternalServerError, http.StatusInternalServerError)
		return
	}

	slog.Info("lease update initiated",
		"lease_uuid", leaseUUID,
		"tenant", auth.Token.Tenant,
		"backend", backendClient.Name(),
		"payload_size", len(updateReq.Payload),
	)

	writeJSON(w, map[string]string{"status": "updating"}, http.StatusAccepted)
}

// GetLeaseReleases handles GET /v1/leases/{lease_uuid}/releases
func (h *Handlers) GetLeaseReleases(w http.ResponseWriter, r *http.Request) {
	auth, leaseUUID, backendClient, ok := h.authenticateAndResolve(w, r, false, false)
	if !ok {
		return
	}

	releases, err := backendClient.GetReleases(r.Context(), leaseUUID)
	if err != nil {
		if errors.Is(err, backend.ErrNotProvisioned) {
			writeError(w, "lease not yet provisioned", http.StatusNotFound)
			return
		}
		slog.Error("failed to get releases from backend", "error", err, "lease_uuid", leaseUUID)
		writeError(w, errMsgInternalServerError, http.StatusInternalServerError)
		return
	}

	// API-layer Unknown backstop (ENG-508), symmetric with the provision
	// path's provisionReason backstop: a failed release with no authored
	// Reason surfaces the machine-readable Unknown default rather than an
	// empty code, so the read boundary never emits a failed-but-reasonless
	// release.
	for i := range releases {
		if releases[i].Reason == "" && releases[i].Status == "failed" {
			releases[i].Reason = backend.ReasonUnknown
		}
	}

	response := LeaseReleasesResponse{
		LeaseUUID:    leaseUUID,
		Tenant:       auth.Token.Tenant,
		ProviderUUID: h.providerUUID,
		Releases:     releases,
	}

	slog.Info("lease releases served",
		"lease_uuid", leaseUUID,
		"tenant", auth.Token.Tenant,
		"release_count", len(releases),
		"backend", backendClient.Name(),
	)

	writeJSON(w, response, http.StatusOK)
}

// Aggregate health verdicts, in increasing order of severity.
//
// Three tiers, not two. The old boolean folded every dependency fault into one
// answer, and that answer was a 503 — which removed providerd from its load
// balancer, taking down the tenant API *and* the backend callback route with it
// (ENG-522). The tiers below draw the line the load balancer actually cares
// about: whether the impaired thing is remote and shared, or local to this
// process.
const (
	// healthStatusHealthy — every configured check passed.
	healthStatusHealthy = "healthy"
	// healthStatusDegraded — a remote, shared dependency (the chain, one or
	// more backends) is impaired. What survives depends on which:
	//
	//   - A backend down: existing leases on healthy backends still serve, and
	//     positively evidenced acknowledgement/cleanup may continue. A prior
	//     complete topology baseline still permits recordless PENDING leases to
	//     target backends whose provision and retention inventories both answered;
	//     work pinned to the silent backend remains deferred lease-locally.
	//   - The chain down: considerably less. Every tenant endpoint that resolves
	//     a lease fails, and reconciliation halts outright — RunOnce returns as
	//     soon as GetPendingLeases or GetActiveLeasesByProvider errors, because
	//     the whole sweep treats "absent from chain" as ground truth.
	//
	// What holds in BOTH cases, and is the reason this is not "unhealthy": the
	// HTTP callback ingress remains reachable. Applying an exact callback may
	// still need placement or chain access and can retry asynchronously, but
	// staying in rotation preserves the only delivery path. Dropping out would
	// sever it for no gain — there is no peer to shed load onto.
	healthStatusDegraded = "degraded"
	// healthStatusUnhealthy — a local, process-owned resource (one of the bbolt
	// stores) is unusable. Restart-worthy, and the operator's problem rather
	// than the load balancer's — see the Readyz doc comment.
	healthStatusUnhealthy = "unhealthy"
)

// Per-check verdicts. Unlike the aggregate these stay binary: a single probe
// either answered or it did not.
const (
	checkStatusHealthy   = "healthy"
	checkStatusUnhealthy = "unhealthy"
)

// healthProbeBudget caps how long the whole dependency sweep may take.
//
// Without it the liveness contract is a lie. A backend that REFUSES connections
// fails in milliseconds, which is why both real incidents stayed inside any
// budget; a backend that ACCEPTS the connection and never answers instead burns
// its full client timeout, 30s by default. Unbounded, that has three separate
// ways to recreate the very outage this endpoint exists to prevent:
//
//   - The prober gives up first. This is the binding constraint: Traefik's
//     healthCheck timeout defaults to 5s and the reference deployment sets only
//     path and interval, so a probe slower than 5s marks the only server DOWN
//     no matter what this handler would eventually have answered.
//   - The server severs the response. http_write_timeout defaults to 15s.
//   - The handler answers 503 itself. /health and /readyz are wrapped in
//     requestTimeoutMiddleware, i.e. http.TimeoutHandler, whose timeout response
//     body is a 503 — the exact status code this endpoint exists never to send.
//
// So the budget is set under the SMALLEST of those, not merely under fred's own
// timeouts. It holds only because Router.HealthCheck probes concurrently:
// serially, N hung backends would still need N × 30s.
//
// Note this is shorter than the chain client's own 5s Ping cap, which it
// therefore dominates — a chain round-trip slower than this budget reports
// unhealthy. That is the intended reading: a chain that cannot answer inside a
// health probe's window is not healthy, and it still yields degraded + 200.
const healthProbeBudget = 3 * time.Second

// HealthStats contains operational statistics for the health response.
type HealthStats struct {
	InFlightProvisions int `json:"in_flight_provisions"`
}

// HealthResponse represents the health check response.
type HealthResponse struct {
	Status       string                  `json:"status"`
	ProviderUUID string                  `json:"provider_uuid"`
	Checks       map[string]*CheckResult `json:"checks"`
	Stats        *HealthStats            `json:"stats,omitempty"`
}

// CheckResult represents the result of a single health check.
type CheckResult struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// evaluateHealth runs every configured dependency probe and folds the results
// into one aggregate verdict. Shared by GET /health and GET /readyz, which
// differ only in how they map that verdict onto an HTTP status code.
//
// A check absent from the response means an optional dependency is not
// configured, not that it passed. Providerd always reports the mandatory
// placement store and placement-inventory bootstrap checks; token-tracker and
// payload-store checks may be absent when those optional stores are omitted.
func (h *Handlers) evaluateHealth(ctx context.Context) HealthResponse {
	// Bound the whole sweep so a slow dependency can never reach the status
	// code — see healthProbeBudget. Derived from the request context, so a
	// client that disconnects still cancels the work: Router.HealthCheck
	// distinguishes that cancellation from a genuine deadline and declines to
	// record health for it.
	ctx, cancel := context.WithTimeout(ctx, healthProbeBudget)
	defer cancel()

	checks := make(map[string]*CheckResult)

	// Tracked separately because they map to different verdicts. Collapsing
	// them back into one boolean reinstates ENG-522.
	remoteImpaired := false // the chain or a backend — shared, off-box
	localImpaired := false  // a process-owned store or admission-readiness invariant

	// record folds one probe into the response: gauge, sanitized client-facing
	// result, and the matching impairment tier. Raw errors are logged but never
	// returned — they can name internal hosts and paths.
	//
	// logMsg is passed through verbatim rather than unified into one structured
	// line: "health check: chain unhealthy" is quoted in ENG-522's incident
	// timeline and is what operators grep for.
	record := func(name string, local bool, logMsg, clientMsg string, err error) {
		if err != nil {
			slog.Warn(logMsg, "error", err)
			// Same rule as the per-backend gauge in Router.HealthCheck: a probe
			// that failed because the CALLER went away is not evidence about the
			// dependency. Without this an unauthenticated client could drive
			// these series to 0 — and their alerts with them — simply by opening
			// /health and aborting mid-probe, since the endpoint needs no auth.
			// A blown DEADLINE is excluded from the exclusion: that one is
			// genuine, and is the case the budget above deliberately produces.
			if !errors.Is(ctx.Err(), context.Canceled) {
				metrics.HealthCheckHealthy.WithLabelValues(name).Set(0)
			}
			checks[name] = &CheckResult{
				Status:  checkStatusUnhealthy,
				Message: clientMsg,
			}
			if local {
				localImpaired = true
			} else {
				remoteImpaired = true
			}
			return
		}
		metrics.HealthCheckHealthy.WithLabelValues(name).Set(1)
		checks[name] = &CheckResult{Status: checkStatusHealthy}
	}

	// Check chain connectivity
	if h.client != nil {
		record("chain", false,
			"health check: chain unhealthy", "chain connectivity failed",
			h.client.Ping(ctx))
	}

	// Check all backends.
	//
	// This probe must stay here even though it no longer moves the status code:
	// Router.HealthCheck is the ONLY writer of fred_backend_healthy, and this is
	// its only non-test caller, so the deployment's 30s poll of /health is the
	// clock driving that gauge and its three consumers (the BackendUnhealthy
	// alert and two dashboards). Stop calling it and the series latches at a
	// plausible wrong value instead of going absent.
	//
	// Backends get no HealthCheckHealthy series: they already have a per-backend
	// gauge, which this one's single "check" label could not express.
	if h.backendRouter != nil {
		backendResults, backendsHealthy := h.backendRouter.HealthCheck(ctx)
		for _, result := range backendResults {
			checkKey := "backend:" + result.Name
			if result.Healthy {
				checks[checkKey] = &CheckResult{
					Status: checkStatusHealthy,
				}
			} else {
				slog.Warn("health check: backend unhealthy", "backend", result.Name, "error", result.Error)
				checks[checkKey] = &CheckResult{
					Status:  checkStatusUnhealthy,
					Message: "backend health check failed",
				}
			}
		}
		if !backendsHealthy {
			remoteImpaired = true
		}
	}

	// Check token tracker (bbolt database)
	if h.tokenTracker != nil {
		record("token_tracker", true,
			"health check: token tracker unhealthy", "token tracker unavailable",
			h.tokenTracker.Healthy())
	}

	// Check placement store (bbolt database)
	if h.placementLookup != nil {
		record("placement_store", true,
			"health check: placement store unhealthy", "placement store unavailable",
			h.placementLookup.Healthy())
	}
	if h.placementBootstrap != nil {
		var err error
		if !h.placementBootstrap.InventoryBootstrapped() {
			err = errors.New("authoritative placement inventory has not completed")
		}
		record("placement_inventory", true,
			"health check: placement inventory not bootstrapped",
			"placement inventory not ready", err)
	}

	// Check payload store (bbolt database)
	if h.payloadStoreHealth != nil {
		record("payload_store", true,
			"health check: payload store unhealthy", "payload store unavailable",
			h.payloadStoreHealth.Healthy())
	}

	status := healthStatusHealthy
	switch {
	case localImpaired:
		status = healthStatusUnhealthy
	case remoteImpaired:
		status = healthStatusDegraded
	}

	response := HealthResponse{
		Status:       status,
		ProviderUUID: h.providerUUID,
		Checks:       checks,
	}

	if h.statusChecker != nil {
		response.Stats = &HealthStats{
			InFlightProvisions: h.statusChecker.InFlightCount(),
		}
	}

	return response
}

// HealthCheck handles GET /health.
//
// Always 200 while the process can answer at all. This is a liveness contract,
// deliberately: the endpoint is what load balancers poll, and no dependency
// probed here justifies pulling providerd out of rotation.
//
// The reason is topology, not taste. providerd sits behind a single-server
// load-balancer pool with no fail-open floor, so an unhealthy verdict does not
// shed load onto a peer — there is no peer. It takes the whole provider down.
// And the backends' completion callbacks arrive on this same listener, so a
// backend-triggered 503 severs the exact channel a *recovering* backend needs
// to report that it finished. That is not a hypothetical: it is both ENG-522
// (a one-minute chain blip on mainnet) and the 2026-08-17 dev outage (one
// backend of three stopped, 15 minutes of tenant-wide 503s, one orphaned
// workload). Accepting a callback touches neither the chain, nor a backend, nor
// any bbolt store, so there is no probe here whose failure means "cannot serve".
//
// The dependency detail is not lost, it moves: the per-check map is still in the
// body, every non-backend check now exports fred_health_check_healthy, and
// backends keep fred_backend_healthy. Alert on those, not on this status code.
func (h *Handlers) HealthCheck(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, h.evaluateHealth(r.Context()), http.StatusOK)
}

// Readyz handles GET /readyz.
//
// Same body as /health, but the aggregate verdict reaches the status code: 503
// when a local, process-owned resource is unusable. Remote impairment (chain,
// backends) stays 200 here too — "degraded" means providerd is still ready to
// serve, which is exactly what it is.
//
// Nothing routes on this endpoint, by design. It exists so the "unhealthy" tier
// has a wire expression for uptime monitoring and blackbox probes, and so a
// human can curl one path for the deep verdict. Do NOT point a load balancer at
// it: the failure it reports is one a process supervisor can fix by restarting
// and a load balancer cannot fix at all, and pulling the single server would
// again take the callback route down with it.
func (h *Handlers) Readyz(w http.ResponseWriter, r *http.Request) {
	response := h.evaluateHealth(r.Context())

	httpStatus := http.StatusOK
	if response.Status == healthStatusUnhealthy {
		httpStatus = http.StatusServiceUnavailable
	}

	writeJSON(w, response, httpStatus)
}

// WorkloadLookupResponse is the response from the GET /workloads endpoint.
// Workloads is a map keyed by lease_uuid so callers can join by UUID without
// building a client-side index. Unknown leases are absent from the map.
// Warnings is non-nil and may be empty (initialized as []string{} for stable
// JSON serialization as `[]` rather than `null`).
type WorkloadLookupResponse struct {
	Workloads map[string]WorkloadEntry `json:"workloads"`
	Warnings  []string                 `json:"warnings"`
}

// WorkloadEntry describes a single lease's workload for observability.
// LeaseUUID is intentionally absent — the map key in WorkloadLookupResponse
// carries it.
//
// WorkloadEntry is served UNAUTHENTICATED (/workloads). Tenant and Partition
// (an aggregator's customer identifiers) must NEVER be added here — this
// endpoint is world-readable; the same invariant covers the backend's
// unauthenticated /stats and /metrics (no partition-valued metric labels).
type WorkloadEntry struct {
	Status      backend.ProvisionStatus `json:"status"`
	CreatedAt   time.Time               `json:"created_at"`
	BackendName string                  `json:"backend_name"`
	Items       []WorkloadItem          `json:"items"`
}

// WorkloadItem describes a single SKU+image within a workload.
type WorkloadItem struct {
	ServiceName string `json:"service_name,omitempty"`
	SKU         string `json:"sku"`
	Image       string `json:"image,omitempty"`
	Count       int    `json:"count"`
}

// GetWorkloads handles GET /workloads?lease_uuid=<u1>&lease_uuid=<u2>...
//
// Returns workload metadata for the requested lease UUIDs from any backend
// that knows about them. Caller is the manifest-admin SPA (cross-origin via
// CORS); the visible page of leases on the admin's leases pages drives the
// UUID list. The 1..MaxLookupUUIDs cap matches the admin's PAGE_SIZE (25)
// with headroom.
//
// Per-backend errors are surfaced via the Warnings slice rather than failing
// the whole request, so a single broken backend doesn't blank the admin's
// image column. This is a deliberate divergence from reconciler.go's
// fetchAllProvisions, which aborts on any backend error to avoid mistaking
// a transient failure for "lease no longer exists" — that concern doesn't
// apply here since /workloads is read-only metadata for display.
func (h *Handlers) GetWorkloads(w http.ResponseWriter, r *http.Request) {
	uuids := r.URL.Query()["lease_uuid"]
	if len(uuids) == 0 {
		writeError(w, "lease_uuid query parameter required", http.StatusBadRequest)
		return
	}
	if len(uuids) > backend.MaxLookupUUIDs {
		writeError(w, fmt.Sprintf("too many lease_uuid values (max %d)", backend.MaxLookupUUIDs), http.StatusBadRequest)
		return
	}
	for _, u := range uuids {
		if !config.IsValidUUID(u) {
			writeError(w, "invalid lease_uuid", http.StatusBadRequest)
			return
		}
	}

	// Dedupe input UUIDs. Repeated values would otherwise propagate to each
	// backend's LookupProvisions, which iterates the input slice and emits the
	// matching ProvisionInfo once per occurrence — wasted work, and the merge
	// loop below would log a misleading "lease reported by multiple backends"
	// warning even though it was the same backend reporting the same lease twice.
	if len(uuids) > 1 {
		seen := make(map[string]struct{}, len(uuids))
		deduped := make([]string, 0, len(uuids))
		for _, u := range uuids {
			if _, dup := seen[u]; dup {
				continue
			}
			seen[u] = struct{}{}
			deduped = append(deduped, u)
		}
		if len(deduped) < len(uuids) {
			// Forensic breadcrumb: a client sending duplicates is either buggy
			// or hammering the endpoint. Debug-level so it's silent in normal
			// operation but available when investigating misuse.
			slog.Debug("workloads: deduped lease_uuid input",
				"raw", len(uuids),
				"deduped", len(deduped),
			)
		}
		uuids = deduped
	}

	// Initialize with non-nil zero values so JSON serialization is `{}` and `[]`
	// rather than `null` even when no backend returns anything.
	merged := make(map[string]WorkloadEntry)
	warnings := []string{}
	var mu sync.Mutex

	if h.backendRouter != nil {
		backends := h.backendRouter.Backends()
		g, gctx := errgroup.WithContext(r.Context())
		for _, b := range backends {
			g.Go(func() error {
				provisions, err := b.LookupProvisions(gctx, uuids)
				if err != nil {
					// If the parent context was canceled (client went away), gctx.Err()
					// is non-nil and the error is the propagation of that cancel.
					// Skip logging — the post-Wait check below discards the response.
					// We check gctx.Err() rather than errors.Is(err, context.Canceled)
					// because the backend HTTP client has its own Timeout, which also
					// produces a context.DeadlineExceeded that we DO want to surface
					// as a backend failure.
					if gctx.Err() != nil {
						return nil
					}
					slog.Error("workloads: backend LookupProvisions failed",
						"backend", b.Name(), "error", err)
					mu.Lock()
					warnings = append(warnings, fmt.Sprintf("backend %q unavailable", b.Name()))
					mu.Unlock()
					// Return nil so a single backend failure doesn't cancel sibling
					// fetches via gctx (same convention as fetchAllProvisions in
					// internal/provisioner/reconciler.go).
					return nil
				}

				mu.Lock()
				defer mu.Unlock()
				for _, p := range provisions {
					if existing, dup := merged[p.LeaseUUID]; dup {
						// Two distinct violations to log differently:
						//   - Same backend twice: a backend impl bug (its LookupProvisions
						//     should return at most one entry per requested UUID).
						//   - Different backends: chain invariant violation
						//     (single-provider-per-lease).
						// Both keep the first entry and skip; neither is surfaced in
						// the response.
						if existing.BackendName == b.Name() {
							slog.Warn("workloads: backend returned duplicate lease",
								"lease_uuid", p.LeaseUUID,
								"backend", b.Name(),
							)
						} else {
							slog.Warn("workloads: lease reported by multiple backends",
								"lease_uuid", p.LeaseUUID,
								"first_backend", existing.BackendName,
								"duplicate_backend", b.Name(),
							)
						}
						continue
					}
					merged[p.LeaseUUID] = provisionToWorkloadEntry(p, b.Name())
				}
				return nil
			})
		}
		// Goroutines always return nil (errors are collected into `warnings`),
		// so g.Wait() is expected to return nil. Defensively log if a future
		// change ever returns an error from a goroutine.
		if err := g.Wait(); err != nil {
			slog.Error("workloads: errgroup returned unexpected error", "error", err)
		}
	}

	// If the client canceled mid-fan-out, the partial response would silently
	// mask the cancellation. Bail without writing a body — the Go HTTP server
	// handles the dead connection. (Reconciler.fetchAllProvisions doesn't do
	// this check because it's not user-facing; we add it here so admin doesn't
	// see partial data on a flaky connection.)
	if err := r.Context().Err(); err != nil {
		return
	}

	writeJSON(w, WorkloadLookupResponse{
		Workloads: merged,
		Warnings:  warnings,
	}, http.StatusOK)
}

// provisionToWorkloadEntry converts a ProvisionInfo into a WorkloadEntry.
// Stack leases produce one WorkloadItem per service; non-stack leases produce
// a single item with the top-level SKU/Image/Quantity.
//
// Image references are passed through stripImageTag so /workloads exposes
// only the repository name, not the tag or digest. See stripImageTag for the
// reasoning.
func provisionToWorkloadEntry(p backend.ProvisionInfo, backendName string) WorkloadEntry {
	entry := WorkloadEntry{
		Status:      p.Status,
		CreatedAt:   p.CreatedAt,
		BackendName: backendName,
	}
	if len(p.Items) > 0 {
		entry.Items = make([]WorkloadItem, len(p.Items))
		for i, item := range p.Items {
			entry.Items[i] = WorkloadItem{
				ServiceName: item.ServiceName,
				SKU:         item.SKU,
				Image:       stripImageTag(p.ServiceImages[item.ServiceName]),
				Count:       item.Quantity,
			}
		}
	} else {
		entry.Items = []WorkloadItem{{
			SKU:   p.SKU,
			Image: stripImageTag(p.Image),
			Count: p.Quantity,
		}}
	}
	return entry
}

// stripImageTag returns the image reference with its tag and digest removed,
// preserving registry "host:port" syntax (e.g. "registry:5000/foo:tag" →
// "registry:5000/foo"). Used to redact patch-level version info from the
// unauthenticated /workloads response so it can't be used as an authoritative
// CVE-version oracle across every lease on the provider. LastError and
// internal callers still see full references; only the public response is
// redacted.
func stripImageTag(image string) string {
	if image == "" {
		return ""
	}
	// A digest ("@sha256:...") follows any tag, so drop it first. This
	// always truncates at the *first* "@" — multiple "@" in a reference is
	// not grammar-valid, but if one appears anyway we prefer to strip more,
	// not less.
	if at := strings.Index(image, "@"); at >= 0 {
		image = image[:at]
	}
	// The tag separator is the last ":" that appears after the last "/".
	// An earlier ":" is part of a "host:port" registry prefix and must be
	// preserved — otherwise "registry:5000/foo" would collapse to "registry".
	slash := strings.LastIndex(image, "/")
	colon := strings.LastIndex(image, ":")
	if colon > slash {
		image = image[:colon]
	}
	return image
}

// verifyLeaseAccess queries a lease from chain and verifies tenant and provider ownership.
func verifyLeaseAccess(ctx context.Context, client ChainClient, providerUUID, leaseUUID, tenant string, requireActive bool) (*billingtypes.Lease, int, error) {
	var lease *billingtypes.Lease
	var err error
	if requireActive {
		lease, err = client.GetActiveLease(ctx, leaseUUID)
	} else {
		lease, err = client.GetLease(ctx, leaseUUID)
	}
	if err != nil {
		slog.Error("failed to query lease", "error", err, "lease_uuid", leaseUUID)
		return nil, http.StatusInternalServerError, errors.New(errMsgInternalServerError)
	}
	if lease == nil {
		if requireActive {
			return nil, http.StatusNotFound, errors.New(errMsgLeaseNotFound + " or not active")
		}
		return nil, http.StatusNotFound, errors.New(errMsgLeaseNotFound)
	}
	if lease.Tenant != tenant {
		slog.Warn("tenant mismatch", "token_tenant", tenant, "lease_tenant", lease.Tenant)
		return nil, http.StatusForbidden, errors.New(errMsgForbidden)
	}
	if lease.ProviderUuid != providerUUID {
		slog.Warn("provider UUID mismatch", "lease_provider_uuid", lease.ProviderUuid, "our_provider_uuid", providerUUID)
		return nil, http.StatusForbidden, errors.New(errMsgForbidden)
	}
	return lease, http.StatusOK, nil
}

// extractToken extracts and parses the bearer token from the Authorization header.
func (h *Handlers) extractToken(r *http.Request) (*AuthToken, error) {
	tokenStr, err := extractBearerToken(r)
	if err != nil {
		return nil, err
	}
	return ParseAuthToken(tokenStr)
}

// writeJSON writes a JSON response.
// Pre-encodes to buffer to catch encoding errors before writing headers.
func writeJSON(w http.ResponseWriter, data any, status int) {
	// Encode first to catch errors before writing headers
	encoded, err := json.Marshal(data)
	if err != nil {
		slog.Error("failed to encode response", "error", err)
		http.Error(w, `{"error":"internal encoding error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(encoded)
	_, _ = w.Write([]byte("\n")) // Match json.Encoder behavior (adds newline)
}

// writeError writes an error response.
func writeError(w http.ResponseWriter, message string, status int) {
	response := ErrorResponse{
		Error: message,
		Code:  status,
	}
	writeJSON(w, response, status)
}

// convertPortBinding converts a backend PortBinding (string host_port) to an API PortMapping (int host_port).
func convertPortBinding(b backend.PortBinding) PortMapping {
	var port int
	if b.HostPort != "" {
		port, _ = strconv.Atoi(b.HostPort)
	}
	return PortMapping{HostIP: b.HostIP, HostPort: port}
}

// convertInstance converts a backend LeaseInstance to an API InstanceInfo.
func convertInstance(inst backend.LeaseInstance) InstanceInfo {
	ii := InstanceInfo{
		InstanceIndex: inst.InstanceIndex,
		ContainerID:   inst.ContainerID,
		Image:         inst.Image,
		Status:        inst.Status,
		FQDN:          inst.FQDN,
	}
	if len(inst.Ports) > 0 {
		ii.Ports = make(map[string]PortMapping, len(inst.Ports))
		for k, v := range inst.Ports {
			ii.Ports[k] = convertPortBinding(v)
		}
	}
	return ii
}

// extractConnectionDetails converts a backend LeaseInfo into API ConnectionDetails.
func extractConnectionDetails(info backend.LeaseInfo) ConnectionDetails {
	details := ConnectionDetails{
		Host:     info.Host,
		FQDN:     info.FQDN,
		Protocol: info.Protocol,
		Metadata: info.Metadata,
	}
	if details.Metadata == nil {
		details.Metadata = make(map[string]string)
	}

	// Convert top-level ports.
	if len(info.Ports) > 0 {
		details.Ports = make(map[string]PortMapping, len(info.Ports))
		for k, v := range info.Ports {
			details.Ports[k] = convertPortBinding(v)
		}
	}

	// Convert flat instances.
	for _, inst := range info.Instances {
		details.Instances = append(details.Instances, convertInstance(inst))
	}

	// Propagate FQDN from the first instance to the top level.
	if details.FQDN == "" && len(details.Instances) > 0 {
		details.FQDN = details.Instances[0].FQDN
	}

	// Convert services (stack leases).
	if len(info.Services) > 0 {
		details.Services = make(map[string]ServiceConnectionDetails, len(info.Services))
		for name, svc := range info.Services {
			svcDetails := ServiceConnectionDetails{FQDN: svc.FQDN}
			for _, inst := range svc.Instances {
				svcDetails.Instances = append(svcDetails.Instances, convertInstance(inst))
			}
			if svcDetails.FQDN == "" && len(svcDetails.Instances) > 0 {
				svcDetails.FQDN = svcDetails.Instances[0].FQDN
			}
			details.Services[name] = svcDetails
		}
	}

	return details
}

// Sentinel errors for authentication (unexported - internal to package)
var (
	errMissingAuth       = errors.New("missing authorization header")
	errInvalidAuthFormat = errors.New("invalid authorization format, expected 'Bearer <token>'")
)

// WebSocket subscription tunables for StreamLeaseEvents. Production values
// are conservative for an event-only stream where the client never sends
// application data. Tests override the per-Handlers fields (either through
// newTestHandlers or by direct assignment after construction); these consts
// are the production defaults wired in by NewHandlers.
const (
	// wsDefaultMaxMessageSize bounds the size of any message the server will
	// read from a client (gorilla's SetReadLimit caps the assembled message
	// size, summed across fragments). The lease events stream is push-only —
	// clients only ever send tiny control frames (close, ping/pong) — so a
	// small limit is sufficient and prevents memory exhaustion via oversized
	// messages. Matches the gorilla/websocket chat example.
	wsDefaultMaxMessageSize int64 = 512

	// wsDefaultMaxConnLifetime caps the total lifetime of a single
	// subscription. On expiry the server sends a clean close frame and the
	// client must reconnect (and re-authenticate). This prevents a single
	// tenant from holding a slot in the per-lease subscription pool
	// indefinitely.
	wsDefaultMaxConnLifetime = time.Hour
)

// StreamLeaseEvents serves a WebSocket stream of lease status events.
// GET /v1/leases/{lease_uuid}/events
func (h *Handlers) StreamLeaseEvents(w http.ResponseWriter, r *http.Request) {
	if h.eventBroker == nil {
		writeError(w, "events not enabled", http.StatusNotImplemented)
		return
	}

	leaseUUID := r.PathValue("lease_uuid")

	// Token promotion and stripping is handled by WSTokenPromoter middleware.

	// Authenticate BEFORE upgrading so auth failures return normal HTTP errors.
	_, statusCode, err := h.AuthenticateLeaseRequest(r, leaseUUID, false, false)
	if err != nil {
		writeError(w, err.Error(), statusCode)
		return
	}

	conn, err := h.wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("websocket upgrade failed", "lease_uuid", leaseUUID, "error", err)
		return // Upgrade writes its own HTTP error response.
	}
	defer func() { _ = conn.Close() }()

	// Bound the size of messages the server will read. Per the application
	// contract this endpoint is server-push only, so any client message
	// larger than the limit indicates a buggy or malicious client and must
	// not be allowed to allocate memory.
	//
	// Defensive fallback: gorilla treats SetReadLimit(0) as "no limit", so
	// a zero-valued field (e.g., from a Handlers literal that bypassed
	// NewHandlers) would silently disable the protection. Treat anything
	// non-positive as misconfiguration: log loudly and fall back to the
	// production default rather than serve traffic without the cap.
	maxMessageSize := h.wsMaxMessageSize
	if maxMessageSize <= 0 {
		slog.Error("websocket read limit misconfigured, falling back to default",
			"configured", maxMessageSize,
			"default", wsDefaultMaxMessageSize,
		)
		maxMessageSize = wsDefaultMaxMessageSize
	}
	conn.SetReadLimit(maxMessageSize)

	ch, subErr := h.eventBroker.Subscribe(leaseUUID)
	if subErr != nil {
		slog.Warn("subscription rejected", "lease_uuid", leaseUUID, "error", subErr)
		_ = conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "too many connections"))
		return
	}
	if ch == nil {
		_ = conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseGoingAway, "broker closed"))
		return
	}
	defer h.eventBroker.Unsubscribe(leaseUUID, ch)

	const (
		pingInterval = 30 * time.Second
		writeWait    = 10 * time.Second
		pongWait     = 40 * time.Second
	)

	// Read pump. gorilla handles ping/pong control frames silently and
	// surfaces a peer-initiated close as a *websocket.CloseError from
	// ReadMessage — neither path appears as a successful data-message read.
	// On this push-only endpoint a successful ReadMessage therefore means
	// the client violated the application contract by sending application
	// data; close with 1008 ClosePolicyViolation to prevent CPU/slot
	// exhaustion via small-message spam. An error means one of three things:
	// oversized message (ErrReadLimit — logged; gorilla also auto-sends a
	// 1009 frame back), a benign disconnect (peer close, read deadline
	// expiry, TCP drop — silently ignored), or a protocol violation / other
	// abuse signal (logged).
	closeCh := make(chan struct{})
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	go func() {
		defer close(closeCh)
		_, _, err := conn.ReadMessage()
		if err != nil {
			switch {
			case errors.Is(err, websocket.ErrReadLimit):
				slog.Warn("websocket oversized message from client",
					"lease_uuid", leaseUUID,
					"remote_addr", r.RemoteAddr,
					"limit", maxMessageSize,
				)
			case isBenignReadError(err):
				// Normal disconnect — silent.
			default:
				// Protocol violation or unexpected error; log as abuse
				// signal so weird client behavior is visible in operator
				// logs rather than vanishing into the benign bucket.
				slog.Warn("websocket read error from client",
					"lease_uuid", leaseUUID,
					"remote_addr", r.RemoteAddr,
					"error", err,
				)
			}
			return
		}
		// Successful read = unexpected client data message. Per gorilla's
		// docs WriteControl is concurrent-safe with the main goroutine's
		// writes, so it's OK to send the close frame from here.
		slog.Warn("websocket unexpected client message",
			"lease_uuid", leaseUUID,
			"remote_addr", r.RemoteAddr,
		)
		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "client messages not allowed"),
			time.Now().Add(writeWait))
	}()

	// Write pump: send events + ping frames.
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	// Cap the total connection lifetime so a single tenant cannot hold a
	// per-lease subscription slot indefinitely. On expiry we send a clean
	// close frame; the client is expected to reconnect with a fresh token.
	// CloseTryAgainLater (1013) tells the client this is a transient,
	// server-initiated rotation — distinct from CloseGoingAway used for
	// broker shutdown below — so reconnect logic can be unconditional.
	//
	// Defensive fallback (same shape as the read-limit guard above):
	// time.NewTimer(0) — or any non-positive duration — fires immediately,
	// which would close every /events connection right after subscribe.
	// Treat <= 0 as misconfiguration and fall back to the production
	// default rather than introduce a "disable the cap" knob.
	maxLifetime := h.wsMaxConnLifetime
	if maxLifetime <= 0 {
		slog.Error("websocket lifetime misconfigured, falling back to default",
			"configured", maxLifetime,
			"default", wsDefaultMaxConnLifetime,
		)
		maxLifetime = wsDefaultMaxConnLifetime
	}
	lifetimeTimer := time.NewTimer(maxLifetime)
	defer lifetimeTimer.Stop()

	for {
		select {
		case <-closeCh:
			return
		case <-lifetimeTimer.C:
			slog.Info("websocket max lifetime reached", "lease_uuid", leaseUUID)
			_ = conn.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "max connection lifetime reached"),
				time.Now().Add(writeWait))
			return
		case event, ok := <-ch:
			if !ok {
				// Broker closed — send clean close frame.
				_ = conn.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseGoingAway, ""),
					time.Now().Add(writeWait))
				return
			}
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteJSON(event); err != nil {
				slog.Debug("websocket write failed", "lease_uuid", leaseUUID, "error", err)
				return
			}
		case <-ticker.C:
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				slog.Debug("websocket ping failed", "lease_uuid", leaseUUID, "error", err)
				return
			}
		}
	}
}

// isBenignReadError returns true for error categories that represent a
// normal client disconnect on an /events WebSocket: a peer-initiated close
// (any code), any net.Error (timeout, "use of closed network connection",
// etc.), or a plain EOF. Everything else — including gorilla's plain
// `errors.New("websocket: ...")` protocol-violation errors (bad opcode,
// bad mask, invalid utf8 in close frame, continuation after FIN, etc.) —
// is treated as a potential abuse signal by the caller and logged.
func isBenignReadError(err error) bool {
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

// extractBearerToken extracts the raw token string from the Authorization header.
// Expects "Bearer <token>" format. Returns errMissingAuth if no header is present.
func extractBearerToken(r *http.Request) (string, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return "", errMissingAuth
	}
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
		return "", errInvalidAuthFormat
	}
	return parts[1], nil
}
