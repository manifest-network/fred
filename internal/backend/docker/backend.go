package docker

import (
	"cmp"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	networktypes "github.com/docker/docker/api/types/network"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/metrics"
)

// dockerClient abstracts the Docker API surface used by Backend,
// enabling unit tests to substitute a lightweight mock.
type dockerClient interface {
	Ping(ctx context.Context) error
	DaemonInfo(ctx context.Context) (DaemonSecurityInfo, error)
	Close() error
	PullImage(ctx context.Context, imageName string, timeout time.Duration) error
	InspectImage(ctx context.Context, imageName string) (*ImageInfo, error)
	ResolveImageUser(ctx context.Context, imageName string, userOverride string) (uid, gid int, err error)
	CreateContainer(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error)
	StartContainer(ctx context.Context, containerID string, timeout time.Duration) error
	StopContainer(ctx context.Context, containerID string, timeout time.Duration) error
	RenameContainer(ctx context.Context, containerID string, newName string) error
	RemoveContainer(ctx context.Context, containerID string) error
	InspectContainer(ctx context.Context, containerID string) (*ContainerInfo, error)
	ContainerLogs(ctx context.Context, containerID string, tail int) (string, error)
	ListManagedContainers(ctx context.Context) ([]ContainerInfo, error)
	EnsureTenantNetwork(ctx context.Context, tenant string) (string, error)
	RemoveTenantNetworkIfEmpty(ctx context.Context, tenant string) error
	ListManagedNetworks(ctx context.Context) ([]networktypes.Inspect, error)
	DetectVolumeOwner(ctx context.Context, imageName string, volumePaths []string) (uid, gid int, err error)
	DetectWritablePaths(ctx context.Context, imageName string, uid int, candidateParents []string) ([]string, error)
	ExtractImageContent(ctx context.Context, imageName string, paths []string, destDir string, maxBytes int64) map[string]error
	ContainerEvents(ctx context.Context) (<-chan ContainerEvent, <-chan error)
}

// ContainerEvent represents a container lifecycle event from the Docker daemon.
// This keeps Docker SDK types out of the interface boundary.
type ContainerEvent struct {
	ContainerID string
	Action      string // "die", "stop", etc.
}

// Backend implements the backend.Backend interface for Docker containers.
type Backend struct {
	cfg     Config
	docker  dockerClient
	compose composeExecutor
	pool    *shared.ResourcePool
	volumes volumeManager
	logger  *slog.Logger

	// customDomainDNSReady gates HTTP-01 cert issuance on the custom domain
	// resolving to this host (ENG-266). Set in New() to a public-resolver
	// quorum check, unless the gate is disabled in config. nil == always
	// ready (tests, and the disabled path); see dnsGateAllows.
	customDomainDNSReady func(ctx context.Context, domain string) bool

	// provisions tracks active provisions by lease UUID
	provisions   map[string]*provision
	provisionsMu sync.RWMutex

	// tenantNetworkStripes serializes EnsureTenantNetwork and
	// RemoveTenantNetworkIfEmpty per tenant. Tenant networks are shared
	// across every lease for that tenant, so a concurrent provision of
	// lease B and deprovision of lease A on the same tenant can otherwise
	// race: A's removal lands between B's ensure and B's ContainerCreate,
	// and B fails with "network not found". Per-tenant serialization plus
	// scanning b.provisions before removing keeps the decision and Docker
	// call atomic per tenant.
	//
	// Striped lock (fixed-size array, tenant → hash-modulo slot) rather
	// than a map[tenant]*Mutex to keep memory bounded — tenants are
	// Cosmos addresses that can be created by anyone with gas, and a
	// map would grow without bound. With tenantNetworkStripeCount slots,
	// two tenants share a stripe with probability 1/N; the only effect
	// of a collision is minor serialization between unrelated tenants'
	// network ops, which are infrequent (once per provision / deprovision).
	//
	// Lock ordering: stripe mutex -> provisionsMu (RLock).
	tenantNetworkStripes [tenantNetworkStripeCount]sync.Mutex

	// recoverMu serializes recoverState calls. The reconcile loop and
	// external RefreshState (called by Fred's reconciler) both invoke
	// recoverState. Without serialization, concurrent calls can detect
	// the same ready→failed transitions and send duplicate callbacks.
	recoverMu sync.Mutex

	// retentionAccountingMu serializes refreshRetentionAccounting's
	// recompute-from-store + SetRetainedDisk so a stale snapshot can never
	// clobber a fresher one (which would under-count → over-admit).
	retentionAccountingMu sync.Mutex

	// callbackStore persists pending callbacks in bbolt
	callbackStore *shared.CallbackStore

	// diagnosticsStore persists failure diagnostics in bbolt
	diagnosticsStore *shared.DiagnosticsStore

	// releaseStore persists release history in bbolt
	releaseStore *shared.ReleaseStore

	// retentionStore persists soft-deleted leases awaiting restore or reaping
	retentionStore *shared.RetentionStore

	// orphanStreaks counts consecutive retention sweeps an ACTIVE record's volumes
	// were all absent (ENG-370). Two invariants protect it:
	//   1. Single-writer confinement: touched ONLY by reconcileOrphanedRetentions,
	//      reachable only via runRetentionSweep on the single StartCleanupLoop
	//      goroutine (boot-eager retention work runs before that goroutine starts
	//      and never touches it) — so no mutex is needed. Do not add a second writer.
	//   2. In-memory by design: a restart resets it so a cold boot can never prune
	//      on its first sweep (the boot-before-mount fail-safe). Do not persist it.
	// Separately, the prune itself relies on DeleteIfActive's in-txn CAS as the
	// load-bearing guard against a concurrent restore (ClaimForRestore
	// active→restoring on a request goroutine) — do not "simplify" it into an
	// unconditional Delete.
	orphanStreaks map[string]int

	// callbackSender handles callback delivery with retry and HMAC
	callbackSender *shared.CallbackSender

	// httpClient is used by callbackSender for delivery; exposed for test replacement.
	httpClient *http.Client

	// volumeOwnerCache caches detected volume UID/GID per image ID
	// (content-addressable sha256 digest). Zero-value ready; no init needed.
	volumeOwnerCache sync.Map // image ID → volumeOwnerEntry

	// writablePathCache caches auto-detected writable paths per image ID
	// for non-root images. Zero-value ready.
	writablePathCache sync.Map // image ID → []string

	// stopCtx is canceled on shutdown; stopCancel triggers it.
	stopCtx    context.Context
	stopCancel context.CancelFunc
	wg         sync.WaitGroup

	// actors routes per-lease messages to a goroutine that serializes all
	// state transitions for that lease. Entries are created lazily via
	// routeToLease and live until the actor's run loop exits (which also
	// deletes the entry under actorsMu). Guarded by actorsMu so registry
	// membership and actor lifecycle are atomic with respect to each
	// other — eliminating the stale-pointer / orphan-worker race class
	// the prior sync.Map design allowed.
	actorsMu sync.Mutex
	actors   map[string]*leasesm.LeaseActor // leaseUUID → *leasesm.LeaseActor

	// inspector / gatherer / provisionStore are the substrate-agnostic
	// seams the lease state machine consumes via leaseActor.cfg. Wired
	// at backend construction (NewBackend and the test helpers
	// newBackendForTest / newBackendForProvisionTest) and remain stable
	// for the backend's lifetime. PR5 will inject these directly into
	// the actor instead of routing through Backend; for PR4 the Backend
	// is the canonical owner so test helpers can override them via the
	// same mock surface that already exists.
	inspector      leasesm.InstanceInspector
	gatherer       leasesm.DiagnosticsGatherer
	provisionStore leasesm.LeaseProvisionStore
}

// provision wraps the substrate-agnostic leasesm.ProvisionState with
// Docker-private state. The lease state machine reasons about the
// embedded ProvisionState exclusively; substrate-private fields
// (currently VolumeCleanupAttempts) live alongside it on this wrapper
// so the lifecycle is structural — allocating a fresh *provision
// resets every Docker-private counter, and deleting from b.provisions
// drops the Docker-private state at the same time.
//
// Promoted-field access keeps existing call sites working: `p.LeaseUUID`,
// `p.Status`, etc. resolve to the embedded ProvisionState fields via Go's
// embedding rules. Sites that need the *ProvisionState pointer (e.g., the
// backendProvisionStore adapter passing it to a LeaseProvisionStore.UpdateFn
// closure) take &p.ProvisionState.
//
// History: prior to ENG-148 follow-up (commit superseding fde8633), this
// was a type alias plus a parallel `b.volumeCleanupAttempts map[string]int`
// guarded by b.provisionsMu. The parallel-map pattern required every
// site that created or deleted a provisions entry to also handle the
// parallel map under the same lock; provision.go's re-provision path
// missed that invariant, leaking stale attempt counts across
// re-provisions and causing premature give-ups on subsequent
// Deprovision. The wrapper-struct pattern makes that bug class
// structurally impossible.
type provision struct {
	leasesm.ProvisionState

	// VolumeCleanupAttempts tracks how many times Deprovision has
	// retried volume cleanup for this lease before either succeeding
	// or hitting maxVolumeCleanupAttempts and giving up. Docker-private
	// because volume cleanup is Docker-specific — K3s would implement
	// deprovision retry differently.
	VolumeCleanupAttempts int
}

// shortID, diagnosticSnapshot, and containerLogKeys moved to
// internal/backend/shared/leasesm at PR5b-2 BC-3 dedupe (task #19).
// Docker callers now reach the canonical leasesm.{ShortID,
// DiagnosticSnapshot, ContainerLogKeys} versions; the docker-side
// duplicates that previously lived here have been removed.

const (
	diagnosticLogTail  = 20
	diagnosticMaxBytes = 4096
	persistedLogTail   = 100 // lines per container stored in diagnostics

	// callbackMaxErrorLen is the maximum length of an error message sent in
	// a callback payload. The on-chain rejection reason has a 256-character
	// hard limit; exceeding it causes the transaction to fail and triggers
	// an infinite retry loop. Truncating here keeps full diagnostics in
	// LastError (for ListProvisions) while ensuring callbacks succeed.
	callbackMaxErrorLen = 256

	// maxVolumeCleanupAttempts is the maximum number of times Deprovision will
	// retry volume destruction before giving up and removing the provision from
	// the map. This prevents infinite retries when volumes cannot be removed
	// (e.g., permission denied on files created by the container process).
	// Stuck volumes require manual cleanup.
	maxVolumeCleanupAttempts = 3
)

// errMsgContainerExited / errMsgInternal moved to
// internal/backend/shared/leasesm at PR5b-2 D — both strings are
// on-chain callback payloads per the callback-error-sanitization
// invariant; divergence between docker/ and leasesm/ copies could emit
// different on-chain strings for the same failure. The canonical
// constants live in leasesm/lease_sm.go (unexported sources) with
// exported aliases leasesm.ErrMsgContainerExited / leasesm.ErrMsgInternal
// that substrate adapters reach for.

// containerFailureDiagnostics builds a diagnostic string from a failed
// container's exit state and recent logs. Takes a substrate-agnostic
// *leasesm.InstanceState so callers in lease_sm/lease_actor/recover
// don't need to handle the Docker-shaped *ContainerInfo at the seam;
// non-SM callers (provision.go startup verify, waitForHealthy) convert
// their *ContainerInfo via containerInfoToInstanceState before calling.
//
// state.ExitCode is dereferenced; a nil ExitCode is treated as 0 to
// preserve the existing string format ("exit_code=0") for cases where
// the container hasn't actually exited but diagnostics are gathered
// anyway. The Docker-specific log fetch lives here because
// b.docker.ContainerLogs is substrate-private.
func (b *Backend) containerFailureDiagnostics(ctx context.Context, containerID string, state *leasesm.InstanceState) string {
	var buf strings.Builder
	exitCode := 0
	if state != nil && state.ExitCode != nil {
		exitCode = *state.ExitCode
	}
	fmt.Fprintf(&buf, "exit_code=%d", exitCode)
	if state != nil && state.OOMKilled {
		buf.WriteString(", oom_killed=true")
	}

	logs, err := b.docker.ContainerLogs(ctx, containerID, diagnosticLogTail)
	if err != nil {
		b.logger.Warn("failed to fetch container logs for diagnostics",
			"container_id", leasesm.ShortID(containerID), "error", err)
		return buf.String()
	}
	if logs != "" {
		buf.WriteString("; logs:\n")
		buf.WriteString(logs)
	}

	s := buf.String()
	if len(s) > diagnosticMaxBytes {
		s = s[:diagnosticMaxBytes-3] + "..."
	}
	return s
}

// captureContainerLogs fetches logs from the given containerIDs for
// diagnostics persistence. Must be called WHILE the containers still
// exist — failure-path workers (doProvision, doReplace*) must call this
// BEFORE their cleanup defer removes the containers, otherwise Docker
// returns "no such container" and the logs are lost. Optional
// containerKeys map overrides the default index-based log key (e.g.,
// "web/0" for stack services).
//
// Uses context.Background() with a 30s timeout rather than deriving
// from stopCtx, so log capture still succeeds during shutdown (the
// whole point is diagnostic durability). Consequence: shutdown can be
// delayed up to 30s per worker in the pathological case of a wedged
// Docker log endpoint. Combined with the sequential 30s cleanup budget
// that follows in the failure defer, this fits within the actor's
// workExitWaitTimeout so actors still exit cleanly — but operators
// should be aware the budget exists.
func (b *Backend) captureContainerLogs(containerIDs []string, containerKeys map[string]string) map[string]string {
	if len(containerIDs) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	logs := make(map[string]string, len(containerIDs))
	for i, cid := range containerIDs {
		logOutput, err := b.docker.ContainerLogs(ctx, cid, persistedLogTail)
		if err != nil {
			b.logger.Debug("failed to fetch container logs for diagnostics persistence",
				"container_id", leasesm.ShortID(cid), "error", err)
			continue
		}
		key := fmt.Sprintf("%d", i)
		if containerKeys != nil {
			if k, ok := containerKeys[cid]; ok {
				key = k
			}
		}
		logs[key] = logOutput
	}
	if len(logs) == 0 {
		return nil
	}
	return logs
}

// persistDiagnostics saves failure diagnostics and container logs to the
// diagnostics store. It performs I/O (container log fetching, bbolt write)
// and must NOT be called while holding provisionsMu.
// It is a best-effort operation: errors are logged but not propagated.
// An optional containerKeys map overrides the default index-based log keys
// (e.g., "web/0" for stack services).
func (b *Backend) persistDiagnostics(entry shared.DiagnosticEntry, containerIDs []string, containerKeys ...map[string]string) {
	if b.diagnosticsStore == nil {
		return
	}
	// Guard against zero-value entries reaching the store: callers that
	// build diagSnap conditionally (e.g., deprovision.go's `if p, ok :=
	// b.provisions[leaseUUID]; ok { diagSnap = ... }`) can fall through
	// with entry.LeaseUUID == "" if the provision entry is missing. In
	// practice the invariants prevent this today, but guarding here
	// matches the lease_sm.go call sites' own "if diagSnap.LeaseUUID
	// != ''" checks and keeps an empty-key record out of the store if
	// a future refactor weakens the invariant.
	if entry.LeaseUUID == "" {
		return
	}
	var keys map[string]string
	if len(containerKeys) > 0 {
		keys = containerKeys[0]
	}
	if logs := b.captureContainerLogs(containerIDs, keys); logs != nil {
		entry.Logs = logs
	}
	if err := b.diagnosticsStore.Store(entry); err != nil {
		b.logger.Warn("failed to persist failure diagnostics",
			"lease_uuid", entry.LeaseUUID, "error", err)
	}
}

// persistDiagnosticsWithLogs saves pre-captured logs to the diagnostics
// store. Used by failure-path workers that capture logs before cleanup
// (when the containers are about to be removed). The entry's Logs field
// is set from the supplied map, bypassing the re-fetch path.
func (b *Backend) persistDiagnosticsWithLogs(entry shared.DiagnosticEntry, logs map[string]string) {
	if b.diagnosticsStore == nil {
		return
	}
	// See persistDiagnostics for rationale — skip zero-value entries.
	if entry.LeaseUUID == "" {
		return
	}
	if len(logs) > 0 {
		entry.Logs = logs
	}
	if err := b.diagnosticsStore.Store(entry); err != nil {
		b.logger.Warn("failed to persist failure diagnostics",
			"lease_uuid", entry.LeaseUUID, "error", err)
	}
}

// stackContainerLogKeys builds a containerID → display key mapping from a
// service containers map. Used by stack error paths that have a local
// serviceContainers map but no provision pointer.
func stackContainerLogKeys(serviceContainers map[string][]string) map[string]string {
	if len(serviceContainers) == 0 {
		return nil
	}
	keys := make(map[string]string)
	for svcName, cids := range serviceContainers {
		for i, cid := range cids {
			keys[cid] = fmt.Sprintf("%s/%d", svcName, i)
		}
	}
	return keys
}

// customDomainDNSCheckTimeout bounds a single readiness check (all resolvers).
const customDomainDNSCheckTimeout = 5 * time.Second

// dnsGateAllows reports whether the custom domain may be emitted now. A nil
// checker (tests, or gate disabled) allows everything.
func (b *Backend) dnsGateAllows(ctx context.Context, domain string) bool {
	if b.customDomainDNSReady == nil {
		return true
	}
	return b.customDomainDNSReady(ctx, domain)
}

// New creates a new Docker backend.
func New(cfg Config, logger *slog.Logger) (*Backend, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	docker, err := NewDockerClient(cfg.DockerHost, cfg.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to create Docker client: %w", err)
	}

	pool := shared.NewResourcePool(
		cfg.TotalCPUCores,
		cfg.TotalMemoryMB,
		cfg.TotalDiskMB,
		cfg.GetSKUProfile, // Use Config's resolver to avoid duplicating SKU mapping logic
		cfg.TenantQuota,
	)
	setStaticPoolMetrics(cfg) // ENG-360: export static pool/cap denominators for dashboards
	if retentionCapNeedsTenantLever(cfg) {
		logger.Warn("max_retained_disk_mb is set but max_retained_leases_per_tenant is 0 (unlimited): one tenant can fill the retained pool, degrading others to refuse-to-retain; set a per-tenant count cap")
	}
	if retentionCapSetButDisabled(cfg) {
		logger.Warn("retention cap/limit configured (max_retained_disk_mb / max_retained_leases_per_tenant) but retain_on_close=false: the cap has no effect; enable retain_on_close or remove the cap")
	}

	// Create HTTP client for callbacks
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Configure TLS if needed
	if cfg.CallbackInsecureSkipVerify {
		logger.Error("INSECURE: callback TLS verification disabled — do NOT use in production")
		httpClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, //nolint:gosec // Intentional for development
			},
		}
	}

	cbStore, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath:         cfg.CallbackDBPath,
		MaxAge:         cfg.CallbackMaxAge,
		OnCleanupPanic: func(any) { metrics.CleanupPanicsTotal.WithLabelValues("callback").Inc() },
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open callback store: %w", err)
	}

	diagStore, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{
		DBPath:         cfg.DiagnosticsDBPath,
		MaxAge:         cfg.DiagnosticsMaxAge,
		OnCleanupPanic: func(any) { metrics.CleanupPanicsTotal.WithLabelValues("diagnostics").Inc() },
	})
	if err != nil {
		_ = cbStore.Close()
		return nil, fmt.Errorf("failed to open diagnostics store: %w", err)
	}

	releaseStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{
		DBPath:         cfg.ReleasesDBPath,
		MaxAge:         cfg.ReleasesMaxAge,
		OnCleanupPanic: func(any) { metrics.CleanupPanicsTotal.WithLabelValues("releases").Inc() },
	})
	if err != nil {
		_ = cbStore.Close()
		_ = diagStore.Close()
		return nil, fmt.Errorf("failed to open release store: %w", err)
	}

	retentionStore, err := shared.NewRetentionStore(shared.RetentionStoreConfig{
		DBPath: cfg.RetentionDBPath,
		OnReindex: func(count int, dur time.Duration, trigger string) {
			retentionIndexReindexTotal.WithLabelValues(trigger).Inc()
			logger.Info("retention index rebuilt", "records", count, "duration", dur, "trigger", trigger)
		},
	})
	if err != nil {
		_ = cbStore.Close()
		_ = diagStore.Close()
		_ = releaseStore.Close()
		return nil, fmt.Errorf("failed to open retention store: %w", err)
	}

	volumes, err := newVolumeManager(cfg.VolumeDataPath, cfg.VolumeFilesystem, cfg.GetMinAvgFileBytes(), logger)
	if err != nil {
		_ = cbStore.Close()
		_ = diagStore.Close()
		_ = releaseStore.Close()
		_ = retentionStore.Close()
		return nil, fmt.Errorf("failed to create volume manager: %w", err)
	}

	composeSvc, err := newComposeService(cfg.DockerHost)
	if err != nil {
		_ = cbStore.Close()
		_ = diagStore.Close()
		_ = releaseStore.Close()
		_ = retentionStore.Close()
		return nil, fmt.Errorf("init compose service: %w", err)
	}

	b := &Backend{
		cfg:              cfg,
		docker:           docker,
		compose:          composeSvc,
		pool:             pool,
		volumes:          volumes,
		logger:           logger.With("backend", cfg.Name),
		provisions:       make(map[string]*provision),
		actors:           make(map[string]*leasesm.LeaseActor),
		callbackStore:    cbStore,
		diagnosticsStore: diagStore,
		releaseStore:     releaseStore,
		retentionStore:   retentionStore,
		orphanStreaks:    make(map[string]int),
		httpClient:       httpClient,
		// tenantNetworkStripes is a fixed-size array embedded in Backend;
		// the zero value is ready to use (N unlocked sync.Mutexes).
	}

	// Pre-initialize the orphan-skip counter series to 0 (ENG-370): the reason
	// set is closed and known, so alert queries see 0 instead of no-data before
	// the first skip event.
	for _, r := range orphanSkipReasons {
		retentionOrphanSkipsTotal.WithLabelValues(r).Add(0)
	}

	b.stopCtx, b.stopCancel = context.WithCancel(context.Background())

	b.callbackSender = shared.NewCallbackSender(shared.CallbackSenderConfig{
		Store:      cbStore,
		HTTPClient: httpClient,
		Secret:     string(cfg.CallbackSecret),
		Logger:     b.logger,
		StopCtx:    b.stopCtx,
		OnDelivery: func(outcome string) {
			callbackDeliveryTotal.WithLabelValues(outcome).Inc()
		},
		OnStoreError: func() {
			callbackStoreErrorsTotal.Inc()
		},
	})

	// Wire the substrate-agnostic seams the lease state machine consumes.
	// Each adapter is a thin pass-through to the existing Docker-specific
	// methods; they exist so the SM/actor can depend on leasesm.* without
	// reaching back through *Backend at the seam.
	b.inspector = &dockerInstanceInspector{docker: b.docker}
	b.gatherer = &dockerDiagnosticsGatherer{backend: b}
	b.provisionStore = &backendProvisionStore{backend: b}

	// Gate custom-domain HTTP-01 issuance on the domain resolving to this host
	// (ENG-266). A quorum of public resolvers mirrors what the ACME CA sees.
	// Left nil when disabled, which dnsGateAllows treats as always-ready.
	if b.cfg.Ingress.Enabled && !b.cfg.Ingress.CustomDomainDNSCheckDisabled {
		resolvers := newResolvers(b.cfg.Ingress.dnsResolvers())
		quorum := b.cfg.Ingress.dnsQuorum(len(resolvers))
		hostAddr := b.cfg.HostAddress
		b.customDomainDNSReady = func(ctx context.Context, domain string) bool {
			cctx, cancel := context.WithTimeout(ctx, customDomainDNSCheckTimeout)
			defer cancel()
			ready, hostErr := customDomainReadyByQuorum(cctx, resolvers, domain, hostAddr, quorum)
			if !ready && hostErr != nil {
				// host_address couldn't be resolved via the configured resolvers
				// (misconfig, resolver outage, or network) — without this log we'd
				// silently defer every custom domain with no operator signal.
				b.logger.Warn("custom-domain DNS readiness: could not resolve host_address via the configured resolvers; deferring issuance (check host_address, the resolvers, and network reachability)",
					"custom_domain", domain, "host_address", hostAddr, "error", hostErr)
			}
			return ready
		}
	}

	return b, nil
}

// Start initializes the backend, recovers state, and starts background tasks.
func (b *Backend) Start(ctx context.Context) error {
	// Verify Docker connectivity
	if err := b.docker.Ping(ctx); err != nil {
		return fmt.Errorf("failed to connect to Docker: %w", err)
	}

	// Validate volume manager (filesystem support, permissions)
	if err := b.volumes.Validate(); err != nil {
		return fmt.Errorf("volume manager validation failed: %w", err)
	}

	// ENG-360: warn loudly if the operator over-sized the disk pool relative to
	// physical capacity (the invariant the hard-quota-sum model depends on).
	b.warnIfOverProvisioned()

	// Check daemon capabilities for hardening configuration
	b.checkDaemonCapabilities(ctx)

	// Recover state from existing containers
	if err := b.recoverState(ctx); err != nil {
		return fmt.Errorf("failed to recover state: %w", err)
	}

	// Reconcile crash-interrupted soft-deletes and restores. MUST run AFTER
	// recoverState (so b.provisions reflects live containers) and BEFORE
	// cleanupOrphanedVolumes (so any mid-rename canonical volume is moved back
	// into the fred-retained- namespace before the orphan reaper sees it).
	if err := b.reconcileRetentions(b.stopCtx); err != nil {
		b.logger.Warn("retention reconciliation failed", "error", err)
	}

	// Backfill per-volume quotas onto existing volumes. Volumes provisioned
	// before the daemon held CAP_SYS_ADMIN were created untagged/un-limited;
	// once the capability is granted, this re-applies enforcement without a
	// re-provision. Best-effort (never fatal). Runs after reconcileRetentions so
	// the fred-retained- namespace matches the retention records (ENG-454).
	b.reconcileVolumeQuotas(ctx)

	// Clean up orphaned volumes (created but no matching provision).
	// Must run after recoverState so the provision map is populated.
	if err := b.cleanupOrphanedVolumes(ctx); err != nil {
		return fmt.Errorf("orphaned volume cleanup failed: %w", err)
	}

	// Boot-eager reap: destroy volumes that expired while fred was offline.
	// The periodic sweep handles ongoing reaping; this catches the gap between
	// the last reap and the restart.
	if _, err := b.reapExpiredRetentions(b.stopCtx); err != nil {
		b.logger.Warn("retention boot reap failed", "error", err)
	}
	// Belt-and-suspenders: recoverState already rebuilt the projection and the
	// boot reap (now wired in Step 5) self-refreshes; this final call guarantees
	// a correct projection before serving traffic even if either changes.
	b.refreshRetentionAccounting()
	b.startRetentionReaper()

	// Replay any pending callbacks from a previous run
	b.callbackSender.ReplayPendingCallbacks()

	// Start periodic reconciliation (using WaitGroup.Go for Go 1.25+)
	b.wg.Go(b.reconcileLoop)

	// Start real-time container event listener for instant crash detection.
	// reconcileLoop stays as safety net for missed events.
	b.wg.Go(b.containerEventLoop)

	// Sample actor inbox depth and stuck-seconds on a ticker for the
	// fred_docker_backend_lease_actor_* observability gauges.
	b.wg.Go(b.actorMetricsSampleLoop)

	b.logger.Info("Docker backend started",
		"host", b.cfg.DockerHost,
		"recovered_containers", len(b.provisions),
	)

	return nil
}

// checkDaemonCapabilities inspects the Docker daemon and logs warnings for
// misconfigured hardening features. Non-fatal: failures are logged and startup
// continues.
func (b *Backend) checkDaemonCapabilities(ctx context.Context) {
	info, err := b.docker.DaemonInfo(ctx)
	if err != nil {
		b.logger.Warn("failed to query daemon info for capability checks", "error", err)
		return
	}

	// Check seccomp availability
	hasSeccomp := false
	for _, opt := range info.SecurityOptions {
		if strings.HasPrefix(opt, "name=seccomp") {
			hasSeccomp = true
			break
		}
	}
	if !hasSeccomp {
		b.logger.Warn("Docker daemon has seccomp disabled; containers will not have syscall filtering")
	}

	// Check IPv4 forwarding — required for container networking (outbound
	// internet, port bindings, and inter-container communication).
	if !info.IPv4Forwarding {
		b.logger.Warn("IPv4 forwarding is disabled; container networking will not function correctly (enable with: sysctl net.ipv4.ip_forward=1)")
	}

	// Surface any daemon warnings (e.g., iptables misconfiguration).
	// Docker's DOCKER-ISOLATION iptables chains provide cross-tenant
	// network isolation; if iptables is disabled, tenants can reach
	// each other directly.
	for _, w := range info.Warnings {
		b.logger.Warn("Docker daemon warning", "message", w)
	}
}

// Stop shuts down the backend gracefully.
func (b *Backend) Stop() error {
	b.stopCancel()
	b.wg.Wait()
	var errs []error
	if b.callbackStore != nil {
		if err := b.callbackStore.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing callback store: %w", err))
		}
	}
	if b.diagnosticsStore != nil {
		if err := b.diagnosticsStore.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing diagnostics store: %w", err))
		}
	}
	if b.releaseStore != nil {
		if err := b.releaseStore.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing release store: %w", err))
		}
	}
	if b.retentionStore != nil {
		if err := b.retentionStore.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing retention store: %w", err))
		}
	}
	if err := b.docker.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing docker client: %w", err))
	}
	return errors.Join(errs...)
}

// Name returns the backend name.
func (b *Backend) Name() string {
	return b.cfg.Name
}

// Health checks that the Docker daemon is reachable AND the persistence stores
// are readable. Probing the bbolt stores (not just docker.Ping) means a
// locked/corrupt/read-only retention or release store surfaces as unhealthy
// instead of the backend reporting healthy while soft-delete/restore silently
// fail — the most data-loss-sensitive subsystem must not be the unmonitored
// one. (ENG-448 / F31)
func (b *Backend) Health(ctx context.Context) error {
	if err := b.docker.Ping(ctx); err != nil {
		return err
	}
	if b.callbackStore != nil {
		if err := b.callbackStore.Healthy(); err != nil {
			return fmt.Errorf("callback store unhealthy: %w", err)
		}
	}
	if b.diagnosticsStore != nil {
		if err := b.diagnosticsStore.Healthy(); err != nil {
			return fmt.Errorf("diagnostics store unhealthy: %w", err)
		}
	}
	if b.releaseStore != nil {
		if err := b.releaseStore.Healthy(); err != nil {
			return fmt.Errorf("release store unhealthy: %w", err)
		}
	}
	if b.retentionStore != nil {
		if err := b.retentionStore.Healthy(); err != nil {
			return fmt.Errorf("retention store unhealthy: %w", err)
		}
	}
	return nil
}

// sendCallback resolves the callback URL from the provisions map and delegates
// to sendCallbackWithURL. Use this when the provision is still in the map. It
// always passes retained=false: only the deprovision retain-success path (which
// uses sendCallbackWithURL directly, since the map entry is being deleted)
// carries retained=true.
func (b *Backend) sendCallback(leaseUUID string, status backend.CallbackStatus, errMsg string) {
	b.provisionsMu.RLock()
	var callbackURL string
	if prov, ok := b.provisions[leaseUUID]; ok {
		callbackURL = prov.CallbackURL
	}
	b.provisionsMu.RUnlock()

	b.sendCallbackWithURL(leaseUUID, callbackURL, status, errMsg, false)
}

// sendCallbackWithURL dispatches a callback using a caller-provided URL,
// bypassing the provisions map. Use when the map entry is about to be deleted
// (or is being deleted under the write lock) and the URL has been captured
// earlier. retained is best-effort ground truth threaded into the payload; it
// is true only on the deprovision retain-success path.
func (b *Backend) sendCallbackWithURL(leaseUUID, callbackURL string, status backend.CallbackStatus, errMsg string, retained bool) {
	// Truncate error to fit the on-chain rejection reason limit.
	if len(errMsg) > callbackMaxErrorLen {
		errMsg = errMsg[:callbackMaxErrorLen-3] + "..."
	}

	b.callbackSender.SendCallback(leaseUUID, callbackURL, b.Name(), status, errMsg, retained)
}

// removeProvision removes a provision reservation. Used when pre-flight
// validation fails after the slot was reserved. Because Docker-private
// state (VolumeCleanupAttempts) is a field on the *provision wrapper,
// the single map delete also drops every per-lease counter — no parallel
// cleanup required.
func (b *Backend) removeProvision(leaseUUID string) {
	b.provisionsMu.Lock()
	delete(b.provisions, leaseUUID)
	b.provisionsMu.Unlock()
}

// shutdownAwareContext returns a context that cancels on either:
//  1. Provision timeout exceeded (cfg.ProvisionTimeout or 10m default)
//  2. Backend shutdown (stopCtx canceled)
//
// The caller must call the returned cancel function when done.
func (b *Backend) shutdownAwareContext() (context.Context, context.CancelFunc) {
	provisionTimeout := cmp.Or(b.cfg.ProvisionTimeout, 10*time.Minute)
	return context.WithTimeout(b.stopCtx, provisionTimeout)
}
