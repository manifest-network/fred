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
	"github.com/docker/docker/pkg/stringid"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
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

	// provisions tracks active provisions by lease UUID
	provisions   map[string]*provision
	provisionsMu sync.RWMutex

	// tenantNetworkMus serializes EnsureTenantNetwork and
	// RemoveTenantNetworkIfEmpty per tenant. Tenant networks are shared
	// across every lease for that tenant, so a concurrent provision of
	// lease B and deprovision of lease A on the same tenant can otherwise
	// race: A's removal lands between B's ensure and B's ContainerCreate,
	// and B fails with "network not found". Per-tenant serialization plus
	// scanning b.provisions before removing keeps the decision and Docker
	// call atomic per tenant. Different tenants never block each other.
	// Mutexes are created lazily and never removed (one *sync.Mutex per
	// unique tenant is ~24 bytes; tenants recycle slowly). Lock ordering:
	// tenantNetworkMu -> provisionsMu (RLock).
	tenantNetworkMus   map[string]*sync.Mutex
	tenantNetworkMusMu sync.Mutex

	// recoverMu serializes recoverState calls. The reconcile loop and
	// external RefreshState (called by Fred's reconciler) both invoke
	// recoverState. Without serialization, concurrent calls can detect
	// the same ready→failed transitions and send duplicate callbacks.
	recoverMu sync.Mutex

	// callbackStore persists pending callbacks in bbolt
	callbackStore *shared.CallbackStore

	// diagnosticsStore persists failure diagnostics in bbolt
	diagnosticsStore *shared.DiagnosticsStore

	// releaseStore persists release history in bbolt
	releaseStore *shared.ReleaseStore

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
	// actorFor and live until backend shutdown.
	actors sync.Map // leaseUUID → *leaseActor
}

// provision tracks provisioned containers for a lease.
// A single lease may have multiple containers based on quantity.
type provision struct {
	LeaseUUID             string
	Tenant                string
	ProviderUUID          string
	SKU                   string
	ContainerIDs          []string // Multiple containers for multi-unit leases
	Image                 string
	Manifest              *DockerManifest // Stored for restart/update operations (legacy)
	Status                backend.ProvisionStatus
	Quantity              int // Expected number of containers
	CreatedAt             time.Time
	FailCount             int    // Number of times provisioning has failed for this lease
	VolumeCleanupAttempts int    // Number of failed volume cleanup attempts during deprovision
	LastError             string // Last error message, queryable after failure
	CallbackURL           string // URL to notify on provision completion

	// Stack fields (set when IsStack() returns true)
	StackManifest     *StackManifest      // Per-service manifests for stack deployments
	ServiceContainers map[string][]string // service name → container IDs
	Items             []backend.LeaseItem // Original lease items (preserved for stack operations)
}

// IsStack returns true when this provision represents a stack deployment.
func (p *provision) IsStack() bool {
	return p.StackManifest != nil
}

// shortID truncates a container/network ID to 12 characters for logging.
// Uses Docker's stringid.TruncateID for consistency with Docker CLI output.
func shortID(id string) string {
	return stringid.TruncateID(id)
}

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

	// errMsgContainerExited is the base error message for containers that
	// exit unexpectedly, used by recoverState and failure callbacks.
	errMsgContainerExited = "container exited unexpectedly"
)

// containerFailureDiagnostics builds a diagnostic string from a failed
// container's exit state and recent logs.
func (b *Backend) containerFailureDiagnostics(ctx context.Context, containerID string, info *ContainerInfo) string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "exit_code=%d", info.ExitCode)
	if info.OOMKilled {
		buf.WriteString(", oom_killed=true")
	}

	logs, err := b.docker.ContainerLogs(ctx, containerID, diagnosticLogTail)
	if err != nil {
		b.logger.Warn("failed to fetch container logs for diagnostics",
			"container_id", shortID(containerID), "error", err)
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

// diagnosticSnapshot captures provision fields needed for diagnostics persistence.
// Built under provisionsMu so that persistDiagnostics can run without holding the lock.
func diagnosticSnapshot(prov *provision) shared.DiagnosticEntry {
	return shared.DiagnosticEntry{
		LeaseUUID:    prov.LeaseUUID,
		ProviderUUID: prov.ProviderUUID,
		Tenant:       prov.Tenant,
		Error:        prov.LastError,
		FailCount:    prov.FailCount,
		CreatedAt:    time.Now(),
	}
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

	var keys map[string]string
	if len(containerKeys) > 0 {
		keys = containerKeys[0]
	}

	// Fetch logs from containers that still exist.
	if len(containerIDs) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		logs := make(map[string]string, len(containerIDs))
		for i, cid := range containerIDs {
			logOutput, err := b.docker.ContainerLogs(ctx, cid, persistedLogTail)
			if err != nil {
				b.logger.Debug("failed to fetch container logs for diagnostics persistence",
					"container_id", shortID(cid), "error", err)
				continue
			}
			key := fmt.Sprintf("%d", i)
			if keys != nil {
				if k, ok := keys[cid]; ok {
					key = k
				}
			}
			logs[key] = logOutput
		}
		if len(logs) > 0 {
			entry.Logs = logs
		}
	}

	if err := b.diagnosticsStore.Store(entry); err != nil {
		b.logger.Warn("failed to persist failure diagnostics",
			"lease_uuid", entry.LeaseUUID, "error", err)
	}
}

// containerLogKeys builds a containerID → display key mapping for stack provisions.
// Returns nil for non-stack provisions.
func containerLogKeys(prov *provision) map[string]string {
	if prov == nil || !prov.IsStack() {
		return nil
	}
	return stackContainerLogKeys(prov.ServiceContainers)
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
		DBPath: cfg.CallbackDBPath,
		MaxAge: cfg.CallbackMaxAge,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open callback store: %w", err)
	}

	diagStore, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{
		DBPath: cfg.DiagnosticsDBPath,
		MaxAge: cfg.DiagnosticsMaxAge,
	})
	if err != nil {
		_ = cbStore.Close()
		return nil, fmt.Errorf("failed to open diagnostics store: %w", err)
	}

	releaseStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{
		DBPath: cfg.ReleasesDBPath,
		MaxAge: cfg.ReleasesMaxAge,
	})
	if err != nil {
		_ = cbStore.Close()
		_ = diagStore.Close()
		return nil, fmt.Errorf("failed to open release store: %w", err)
	}

	volumes, err := newVolumeManager(cfg.VolumeDataPath, cfg.VolumeFilesystem, logger)
	if err != nil {
		_ = cbStore.Close()
		_ = diagStore.Close()
		_ = releaseStore.Close()
		return nil, fmt.Errorf("failed to create volume manager: %w", err)
	}

	composeSvc, err := newComposeService(cfg.DockerHost)
	if err != nil {
		_ = cbStore.Close()
		_ = diagStore.Close()
		_ = releaseStore.Close()
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
		tenantNetworkMus: make(map[string]*sync.Mutex),
		callbackStore:    cbStore,
		diagnosticsStore: diagStore,
		releaseStore:     releaseStore,
		httpClient:       httpClient,
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

	// Check daemon capabilities for hardening configuration
	b.checkDaemonCapabilities(ctx)

	// Recover state from existing containers
	if err := b.recoverState(ctx); err != nil {
		return fmt.Errorf("failed to recover state: %w", err)
	}

	// Clean up orphaned volumes (created but no matching provision).
	// Must run after recoverState so the provision map is populated.
	if err := b.cleanupOrphanedVolumes(ctx); err != nil {
		return fmt.Errorf("orphaned volume cleanup failed: %w", err)
	}

	// Replay any pending callbacks from a previous run
	b.callbackSender.ReplayPendingCallbacks()

	// Start periodic reconciliation (using WaitGroup.Go for Go 1.25+)
	b.wg.Go(b.reconcileLoop)

	// Start real-time container event listener for instant crash detection.
	// reconcileLoop stays as safety net for missed events.
	b.wg.Go(b.containerEventLoop)

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
	if err := b.docker.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing docker client: %w", err))
	}
	return errors.Join(errs...)
}

// Name returns the backend name.
func (b *Backend) Name() string {
	return b.cfg.Name
}

// Health checks if the Docker daemon is reachable.
func (b *Backend) Health(ctx context.Context) error {
	return b.docker.Ping(ctx)
}

// prevContainerName returns the temporary name used for old containers during
// updates/restarts while the replacement is being verified.
func prevContainerName(leaseUUID string, instanceIndex int) string {
	return fmt.Sprintf("fred-%s-%d-prev", leaseUUID, instanceIndex)
}

// rollbackContainers renames old containers back to their canonical names and
// restarts them. Handles partial-stop scenarios where some containers may still
// be running (e.g., stop failed mid-loop for multi-container leases).
// Returns true only if every container is confirmed running after rollback.
func (b *Backend) rollbackContainers(leaseUUID string, containerIDs []string, logger *slog.Logger) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	restored := 0
	for i, cid := range containerIDs {
		// Check if container needs rollback — it might still be running if
		// stop failed mid-loop for a multi-container lease.
		info, inspectErr := b.docker.InspectContainer(ctx, cid)
		if inspectErr != nil {
			logger.Error("rollback: failed to inspect container", "container_id", shortID(cid), "error", inspectErr)
			continue
		}
		if info.Status == "running" {
			restored++ // Already running — no rollback needed
			continue
		}

		canonicalName := fmt.Sprintf("fred-%s-%d", leaseUUID, i)
		if renameErr := b.docker.RenameContainer(ctx, cid, canonicalName); renameErr != nil {
			logger.Error("rollback: failed to rename container", "container_id", shortID(cid), "error", renameErr)
		}
		if err := b.docker.StartContainer(ctx, cid, 30*time.Second); err != nil {
			logger.Error("rollback: failed to restart container", "container_id", shortID(cid), "error", err)
		} else {
			restored++
		}
	}
	return restored == len(containerIDs)
}

// sendCallback resolves the callback URL from the provisions map and delegates
// to sendCallbackWithURL. Use this when the provision is still in the map.
func (b *Backend) sendCallback(leaseUUID string, status backend.CallbackStatus, errMsg string) {
	b.provisionsMu.RLock()
	var callbackURL string
	if prov, ok := b.provisions[leaseUUID]; ok {
		callbackURL = prov.CallbackURL
	}
	b.provisionsMu.RUnlock()

	b.sendCallbackWithURL(leaseUUID, callbackURL, status, errMsg)
}

// sendCallbackWithURL dispatches a callback using a caller-provided URL,
// bypassing the provisions map. Use when the map entry is about to be deleted
// (or is being deleted under the write lock) and the URL has been captured
// earlier.
func (b *Backend) sendCallbackWithURL(leaseUUID, callbackURL string, status backend.CallbackStatus, errMsg string) {
	// Truncate error to fit the on-chain rejection reason limit.
	if len(errMsg) > callbackMaxErrorLen {
		errMsg = errMsg[:callbackMaxErrorLen-3] + "..."
	}

	b.callbackSender.SendCallback(leaseUUID, callbackURL, b.Name(), status, errMsg)
}

// removeProvision removes a provision reservation and its callback URL.
// Used when pre-flight validation fails after the slot was reserved.
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
