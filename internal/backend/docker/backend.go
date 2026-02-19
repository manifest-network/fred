package docker

import (
	"cmp"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
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
		logger.Warn("callback TLS verification disabled - only use for development")
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
		callbackStore:    cbStore,
		diagnosticsStore: diagStore,
		releaseStore:     releaseStore,
		httpClient:       httpClient,
	}

	b.stopCtx, b.stopCancel = context.WithCancel(context.Background())

	b.callbackSender = shared.NewCallbackSender(shared.CallbackSenderConfig{
		Store:      cbStore,
		HTTPClient: httpClient,
		Secret:     cfg.CallbackSecret,
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

// Provision starts async provisioning of containers.
// For multi-unit leases (quantity > 1), multiple containers are created.
// For multi-SKU leases, containers are created with the appropriate profile for each SKU.
//
// Pre-flight validation errors (unknown SKU, invalid manifest, disallowed image,
// insufficient resources) are returned synchronously so the caller can respond
// with an appropriate HTTP status. Only truly asynchronous failures (image pull,
// container create/start) are communicated via callback.
func (b *Backend) Provision(ctx context.Context, req backend.ProvisionRequest) error {
	totalQuantity := req.TotalQuantity()

	logger := b.logger.With(
		"lease_uuid", req.LeaseUUID,
		"tenant", req.Tenant,
		"items", len(req.Items),
		"total_quantity", totalQuantity,
	)

	// Atomically check-and-reserve the provision slot (fixes TOCTOU race).
	// Allow re-provisioning if the existing provision has failed (e.g., container
	// crashed and reconciler is retrying).
	var prevFailCount int
	var oldContainerIDs []string
	var oldQuantity int
	var oldItems []backend.LeaseItem // non-nil for stacks, needed for service-aware release
	b.provisionsMu.Lock()
	if existing, exists := b.provisions[req.LeaseUUID]; exists {
		if existing.Status != backend.ProvisionStatusFailed {
			b.provisionsMu.Unlock()
			return fmt.Errorf("%w: %s", backend.ErrAlreadyProvisioned, req.LeaseUUID)
		}
		// Capture data needed for cleanup, then release lock before Docker API calls.
		prevFailCount = existing.FailCount
		oldContainerIDs = existing.ContainerIDs
		oldQuantity = existing.Quantity
		oldItems = existing.Items
		delete(b.provisions, req.LeaseUUID)
	}
	b.provisions[req.LeaseUUID] = &provision{
		LeaseUUID:    req.LeaseUUID,
		Tenant:       req.Tenant,
		ProviderUUID: req.ProviderUUID,
		Status:       backend.ProvisionStatusProvisioning,
		Quantity:     totalQuantity,
		ContainerIDs: make([]string, 0, totalQuantity),
		CreatedAt:    time.Now(),
		FailCount:    prevFailCount,
		CallbackURL:  req.CallbackURL,
	}
	b.provisionsMu.Unlock()

	// Clean up old failed provision resources outside the lock.
	if oldQuantity > 0 {
		if len(oldItems) > 0 {
			// Stack: release service-aware allocation IDs.
			for _, item := range oldItems {
				for i := 0; i < item.Quantity; i++ {
					b.pool.Release(fmt.Sprintf("%s-%s-%d", req.LeaseUUID, item.ServiceName, i))
				}
			}
		} else {
			// Legacy: release index-based allocation IDs.
			for i := 0; i < oldQuantity; i++ {
				b.pool.Release(fmt.Sprintf("%s-%d", req.LeaseUUID, i))
			}
		}
		// Remove old containers but keep volumes — stateful data persists
		// across re-provisions. Volumes are reused via idempotent Create in
		// doProvision, and only destroyed on explicit deprovision.
		for _, cid := range oldContainerIDs {
			if err := b.docker.RemoveContainer(ctx, cid); err != nil {
				logger.Warn("failed to remove old container during re-provision",
					"container_id", shortID(cid), "error", err)
			}
		}
		logger.Info("replacing failed provision",
			"fail_count", prevFailCount,
		)
	}

	// Validate all SKUs upfront and build profile map.
	// On failure, remove the reservation and return error synchronously.
	profiles := make(map[string]SKUProfile)
	for _, item := range req.Items {
		if _, ok := profiles[item.SKU]; ok {
			continue // Already validated
		}
		profile, err := b.cfg.GetSKUProfile(item.SKU)
		if err != nil {
			b.removeProvision(req.LeaseUUID)
			return fmt.Errorf("%w: %w", backend.ErrValidation, err)
		}
		profiles[item.SKU] = profile
	}

	// Parse payload — auto-detects single manifest vs stack manifest.
	isStack, err := backend.IsStack(req.Items)
	if err != nil {
		b.removeProvision(req.LeaseUUID)
		return fmt.Errorf("%w: %w", backend.ErrValidation, err)
	}
	manifest, stackManifest, parseErr := ParsePayload(req.Payload)
	if parseErr != nil {
		b.removeProvision(req.LeaseUUID)
		return fmt.Errorf("%w: %w", backend.ErrInvalidManifest, parseErr)
	}

	// Validate payload type matches lease items.
	if isStack && stackManifest == nil {
		b.removeProvision(req.LeaseUUID)
		return fmt.Errorf("%w: lease items have service names but payload is not a stack manifest", backend.ErrInvalidManifest)
	}
	if !isStack && stackManifest != nil {
		b.removeProvision(req.LeaseUUID)
		return fmt.Errorf("%w: payload is a stack manifest but lease items have no service names", backend.ErrInvalidManifest)
	}

	// Validate images against registry allowlist.
	if isStack {
		if err := ValidateStackAgainstItems(stackManifest, req.Items); err != nil {
			b.removeProvision(req.LeaseUUID)
			return fmt.Errorf("%w: %w", backend.ErrInvalidManifest, err)
		}
		for svcName, svc := range stackManifest.Services {
			if err := shared.ValidateImage(svc.Image, b.cfg.AllowedRegistries); err != nil {
				b.removeProvision(req.LeaseUUID)
				return fmt.Errorf("%w: service %s: %w", backend.ErrValidation, svcName, err)
			}
		}
	} else {
		if err := shared.ValidateImage(manifest.Image, b.cfg.AllowedRegistries); err != nil {
			b.removeProvision(req.LeaseUUID)
			return fmt.Errorf("%w: %w", backend.ErrValidation, err)
		}
	}

	// Try to allocate resources for all instances.
	// Stack uses service-aware allocation IDs: {leaseUUID}-{serviceName}-{instanceIndex}
	var allocatedIDs []string
	if isStack {
		for _, item := range req.Items {
			for i := 0; i < item.Quantity; i++ {
				instanceID := fmt.Sprintf("%s-%s-%d", req.LeaseUUID, item.ServiceName, i)
				if err := b.pool.TryAllocate(instanceID, item.SKU, req.Tenant); err != nil {
					for _, id := range allocatedIDs {
						b.pool.Release(id)
					}
					b.removeProvision(req.LeaseUUID)
					return fmt.Errorf("%w: %w", backend.ErrInsufficientResources, err)
				}
				allocatedIDs = append(allocatedIDs, instanceID)
			}
		}
	} else {
		instanceIdx := 0
		for _, item := range req.Items {
			for i := 0; i < item.Quantity; i++ {
				instanceID := fmt.Sprintf("%s-%d", req.LeaseUUID, instanceIdx)
				if err := b.pool.TryAllocate(instanceID, item.SKU, req.Tenant); err != nil {
					for _, id := range allocatedIDs {
						b.pool.Release(id)
					}
					b.removeProvision(req.LeaseUUID)
					return fmt.Errorf("%w: %w", backend.ErrInsufficientResources, err)
				}
				allocatedIDs = append(allocatedIDs, instanceID)
				instanceIdx++
			}
		}
	}

	// Update the reservation with full details now that validation passed.
	b.provisionsMu.Lock()
	if prov, ok := b.provisions[req.LeaseUUID]; ok {
		prov.SKU = req.RoutingSKU()
		if isStack {
			prov.StackManifest = stackManifest
			prov.Items = req.Items
		} else {
			prov.Image = manifest.Image
		}
	}
	b.provisionsMu.Unlock()

	// Start async provisioning with shutdown-aware context.
	// The context cancels on either:
	// 1. Provision timeout exceeded
	// 2. Backend shutdown (stopCtx canceled)
	b.wg.Go(func() {
		provisionTimeout := cmp.Or(b.cfg.ProvisionTimeout, 10*time.Minute)
		ctx, cancel := context.WithTimeout(context.Background(), provisionTimeout)
		defer cancel()

		stop := context.AfterFunc(b.stopCtx, cancel)
		defer stop()

		if isStack {
			b.doProvisionStack(ctx, req, stackManifest, profiles, logger)
		} else {
			b.doProvision(ctx, req, manifest, profiles, logger)
		}
	})

	return nil
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

// sendCallback resolves the callback URL from the provisions map and delegates to callbackSender.
func (b *Backend) sendCallback(leaseUUID string, success bool, errMsg string) {
	b.provisionsMu.RLock()
	prov, ok := b.provisions[leaseUUID]
	var callbackURL string
	if ok {
		callbackURL = prov.CallbackURL
	}
	b.provisionsMu.RUnlock()

	// Truncate error to fit the on-chain rejection reason limit.
	if len(errMsg) > callbackMaxErrorLen {
		errMsg = errMsg[:callbackMaxErrorLen-3] + "..."
	}

	b.callbackSender.SendCallback(leaseUUID, callbackURL, success, errMsg)
}

// volumeOwnerEntry caches the detected UID/GID for an image's VOLUME directories.
type volumeOwnerEntry struct {
	UID int
	GID int
}

// detectVolumeOwnerCached returns the detected volume owner for an image,
// using the cache keyed by image ID. On error, logs a warning and returns
// (0, 0) without caching so the next call retries (transient errors
// self-heal). Successful results are cached permanently since image IDs
// are immutable content-addressable digests.
func (b *Backend) detectVolumeOwnerCached(ctx context.Context, imageID, imageName string, volumePaths []string) (uid, gid int) {
	if v, ok := b.volumeOwnerCache.Load(imageID); ok {
		if entry, ok := v.(volumeOwnerEntry); ok {
			return entry.UID, entry.GID
		}
	}

	detectedUID, detectedGID, err := b.docker.DetectVolumeOwner(ctx, imageName, volumePaths)
	if err != nil {
		b.logger.Warn("failed to detect volume owner, defaulting to root (not cached)",
			"image", imageName, "error", err)
		return 0, 0
	}

	b.volumeOwnerCache.Store(imageID, volumeOwnerEntry{UID: detectedUID, GID: detectedGID})
	return detectedUID, detectedGID
}

// detectWritablePathsCached returns auto-detected writable paths for an image,
// using the cache keyed by image ID. On error, logs a warning and returns nil
// without caching so the next call retries. Successful results (including
// empty slices) are cached permanently since image IDs are immutable.
func (b *Backend) detectWritablePathsCached(ctx context.Context, imageID, imageName string, uid int) []string {
	if v, ok := b.writablePathCache.Load(imageID); ok {
		if paths, ok := v.([]string); ok {
			return paths
		}
	}

	paths, err := b.docker.DetectWritablePaths(ctx, imageName, uid, candidateWritableParents)
	if err != nil {
		b.logger.Warn("failed to detect writable paths, skipping (not cached)",
			"image", imageName, "error", err)
		return nil
	}

	b.writablePathCache.Store(imageID, paths)
	return paths
}

// imageSetup holds the results of image inspection needed for container creation.
type imageSetup struct {
	Volumes       []string // sorted VOLUME paths declared by the image
	ContainerUser string   // numeric "uid:gid" or "" for root
	VolumeUID     int      // UID for volume ownership
	VolumeGID     int      // GID for volume ownership
	WritablePaths []string // auto-detected writable paths for non-root images
}

// inspectImageForSetup inspects an image and resolves its VOLUME declarations
// and container user. This combines the image inspect, volume discovery, and
// user resolution steps that are common to doProvision, doRestart, and doUpdate.
func (b *Backend) inspectImageForSetup(ctx context.Context, image string, manifestUser string) (*imageSetup, error) {
	imageInfo, err := b.docker.InspectImage(ctx, image)
	if err != nil {
		return nil, fmt.Errorf("image inspect failed: %w", err)
	}

	var volumes []string
	for v := range imageInfo.Volumes {
		volumes = append(volumes, v)
	}
	slices.Sort(volumes)

	result := &imageSetup{Volumes: volumes}

	if manifestUser != "" || imageInfo.User != "" {
		uid, gid, resolveErr := b.docker.ResolveImageUser(ctx, image, manifestUser)
		if resolveErr != nil {
			return nil, fmt.Errorf("image user resolution failed: %w", resolveErr)
		}
		result.VolumeUID = uid
		result.VolumeGID = gid
		if uid != 0 || gid != 0 {
			result.ContainerUser = fmt.Sprintf("%d:%d", uid, gid)
		}
	} else if len(volumes) > 0 {
		// No explicit user set — auto-detect from VOLUME directory ownership.
		// Images like mongo/postgres pre-chown their VOLUME dirs during build;
		// detecting the owner lets us pre-chown host volumes and run as that
		// user, bypassing the entrypoint's chown+gosu (which requires
		// CAP_CHOWN that we drop).
		uid, gid := b.detectVolumeOwnerCached(ctx, imageInfo.ID, image, volumes)
		if uid != 0 || gid != 0 {
			result.VolumeUID = uid
			result.VolumeGID = gid
			result.ContainerUser = fmt.Sprintf("%d:%d", uid, gid)
		}
	}

	// Scan for writable paths owned by the container user (or any non-root
	// user for root images). Images like grafana/grafana (non-root, no VOLUMEs)
	// chown /var/lib/grafana during build; images like neo4j (root, has VOLUMEs)
	// chown /var/lib/neo4j to a service user. These paths get bind mounts from
	// managed volumes so the container has image content on a read-only rootfs.
	// Skipped when ReadonlyRootfs is disabled since the detection creates a temp
	// container and the results are only used for writable path mounting.
	if b.cfg.IsReadonlyRootfs() {
		result.WritablePaths = b.detectWritablePathsCached(ctx, imageInfo.ID, image, result.VolumeUID)
		result.WritablePaths = filterSubpaths(result.WritablePaths, result.Volumes)
	}

	return result, nil
}

// filterSubpaths removes candidates that are equal to or children of any parent path.
// This prevents writable paths from overlapping with VOLUME bind mounts
// (e.g., /data/transactions is a subtree of /data).
func filterSubpaths(candidates, parents []string) []string {
	if len(candidates) == 0 || len(parents) == 0 {
		return candidates
	}
	var result []string
	for _, c := range candidates {
		covered := false
		for _, p := range parents {
			if c == p || strings.HasPrefix(c, p+"/") {
				covered = true
				break
			}
		}
		if !covered {
			result = append(result, c)
		}
	}
	return result
}

// ensureNetworkConfig sets up per-tenant network isolation if enabled.
// Returns nil config when isolation is disabled.
func (b *Backend) ensureNetworkConfig(ctx context.Context, tenant string) (*networktypes.NetworkingConfig, error) {
	if !b.cfg.IsNetworkIsolation() {
		return nil, nil
	}
	networkID, err := b.docker.EnsureTenantNetwork(ctx, tenant)
	if err != nil {
		return nil, fmt.Errorf("tenant network setup failed: %w", err)
	}
	return buildNetworkConfig(networkID), nil
}

// setupVolumeBinds creates volume bind mounts for a single container instance.
// Returns nil when no volumes are needed (diskMB <= 0 or no image volumes).
// Volumes are created idempotently — existing volumes are reused.
func (b *Backend) setupVolumeBinds(ctx context.Context, leaseUUID string, instanceIndex int, diskMB int64, imageVolumes []string, volumeUID, volumeGID int) (map[string]string, error) {
	if diskMB <= 0 || len(imageVolumes) == 0 {
		return nil, nil
	}

	volumeID := fmt.Sprintf("fred-%s-%d", leaseUUID, instanceIndex)
	hostPath, _, err := b.volumes.Create(ctx, volumeID, diskMB)
	if err != nil {
		return nil, fmt.Errorf("volume access failed (instance %d): %w", instanceIndex, err)
	}

	binds := make(map[string]string, len(imageVolumes))
	for _, volPath := range imageVolumes {
		sanitized := sanitizeVolumePath(volPath)
		if sanitized == "" {
			continue
		}
		subdir := filepath.Join(hostPath, sanitized)
		if mkErr := os.MkdirAll(subdir, 0o700); mkErr != nil {
			return nil, fmt.Errorf("volume subdir creation failed (instance %d): %w", instanceIndex, mkErr)
		}
		if volumeUID != 0 || volumeGID != 0 {
			if chownErr := os.Chown(subdir, volumeUID, volumeGID); chownErr != nil {
				return nil, fmt.Errorf("volume subdir chown failed (instance %d): %w", instanceIndex, chownErr)
			}
		}
		binds[subdir] = volPath
	}
	return binds, nil
}

// buildStatefulVolumeBinds creates subdirectories for each image VOLUME path
// under hostPath and returns bind mount mappings. Returns an error if any
// VOLUME path cannot be sanitized (unsupported path format).
func buildStatefulVolumeBinds(hostPath string, imageVolumes []string, uid, gid int) (map[string]string, error) {
	binds := make(map[string]string, len(imageVolumes))
	for _, volPath := range imageVolumes {
		sanitized := sanitizeVolumePath(volPath)
		if sanitized == "" {
			return nil, fmt.Errorf("image declares unsupported VOLUME path %q", volPath)
		}
		subdir := filepath.Join(hostPath, sanitized)
		if err := os.MkdirAll(subdir, 0o700); err != nil {
			return nil, fmt.Errorf("volume subdir %q: %w", subdir, err)
		}
		if uid != 0 || gid != 0 {
			if err := os.Chown(subdir, uid, gid); err != nil {
				return nil, fmt.Errorf("chown volume subdir %q: %w", subdir, err)
			}
		}
		binds[subdir] = volPath
	}
	return binds, nil
}

// setupWritablePathBinds extracts image content for writable paths into
// a managed volume subdirectory and returns a bind map for container creation.
// Extraction failures are logged but don't fail the overall operation;
// paths that fail are simply omitted from the bind map.
func (b *Backend) setupWritablePathBinds(ctx context.Context, image string, writablePaths []string, hostVolumePath string, maxBytes int64) map[string]string {
	if len(writablePaths) == 0 {
		return nil
	}

	wpDir := filepath.Join(hostVolumePath, "_wp")
	// Remove stale content from prior extractions so files deleted
	// in a newer image don't persist.
	if err := os.RemoveAll(wpDir); err != nil {
		b.logger.Warn("failed to clean up old writable path content, extraction may contain stale files",
			"path", wpDir, "error", err)
	}
	failures := b.docker.ExtractImageContent(ctx, image, writablePaths, wpDir, maxBytes)

	binds := make(map[string]string, len(writablePaths))
	for _, wp := range writablePaths {
		if failures != nil {
			if pathErr, ok := failures[wp]; ok {
				b.logger.Warn("failed to extract writable path content",
					"path", wp, "image", image, "error", pathErr)
				continue
			}
		}
		sanitized := sanitizeVolumePath(wp)
		if sanitized == "" {
			b.logger.Warn("writable path rejected by sanitization", "path", wp, "image", image)
			continue
		}
		binds[filepath.Join(wpDir, sanitized)] = wp
	}

	return binds
}

// setupStackVolBinds creates volume bind mounts for all services/instances of a stack.
// It returns the volume binds map, a list of newly created volume IDs, and any fatal error.
// Non-fatal failures (writable-path-only volume creation) are logged as warnings.
func (b *Backend) setupStackVolBinds(
	ctx context.Context,
	leaseUUID string,
	items []backend.LeaseItem,
	profiles map[string]SKUProfile,
	imageSetups map[string]*imageSetup,
	services map[string]*DockerManifest,
	logger *slog.Logger,
) (map[string]map[int]serviceVolBinds, []string, error) {
	volBinds := make(map[string]map[int]serviceVolBinds)
	var createdVolumeIDs []string

	for _, item := range items {
		svcName := item.ServiceName
		profile := profiles[item.SKU]
		imgSetup := imageSetups[svcName]

		for i := 0; i < item.Quantity; i++ {
			needsStatefulVolume := profile.DiskMB > 0 && len(imgSetup.Volumes) > 0
			needsWritableVolume := len(imgSetup.WritablePaths) > 0

			if needsStatefulVolume || needsWritableVolume {
				volumeID := fmt.Sprintf("fred-%s-%s-%d", leaseUUID, svcName, i)
				sizeMB := profile.DiskMB
				if sizeMB <= 0 {
					sizeMB = int64(b.cfg.GetTmpfsSizeMB())
				}
				hostPath, volCreated, volErr := b.volumes.Create(ctx, volumeID, sizeMB)
				if volErr != nil {
					if needsStatefulVolume {
						return nil, createdVolumeIDs, fmt.Errorf("volume creation failed (service %s, instance %d): %w", svcName, i, volErr)
					}
					logger.Warn("writable path content seeding unavailable (volume creation failed)", "service", svcName, "error", volErr)
					continue
				}
				if volCreated {
					createdVolumeIDs = append(createdVolumeIDs, volumeID)
				}
				binds := serviceVolBinds{}
				if needsStatefulVolume {
					var buildErr error
					binds.StatefulBinds, buildErr = buildStatefulVolumeBinds(hostPath, imgSetup.Volumes, imgSetup.VolumeUID, imgSetup.VolumeGID)
					if buildErr != nil {
						return nil, createdVolumeIDs, fmt.Errorf("volume setup failed (service %s, instance %d): %w", svcName, i, buildErr)
					}
				}
				if needsWritableVolume {
					binds.WritableBinds = b.setupWritablePathBinds(ctx, services[svcName].Image, imgSetup.WritablePaths, hostPath, sizeMB*1024*1024)
				}
				if volBinds[svcName] == nil {
					volBinds[svcName] = make(map[int]serviceVolBinds)
				}
				volBinds[svcName][i] = binds
			}
		}
	}
	return volBinds, createdVolumeIDs, nil
}

// verifyStartup checks that containers started successfully.
// Uses health-check-aware polling when the manifest declares an active health check,
// otherwise falls back to a fixed-wait + inspect check.
func (b *Backend) verifyStartup(ctx context.Context, manifest *DockerManifest, containerIDs []string, logger *slog.Logger) error {
	if manifest.HasActiveHealthCheck() {
		return b.waitForHealthy(ctx, containerIDs, logger)
	}

	startupVerify := cmp.Or(b.cfg.StartupVerifyDuration, 5*time.Second)
	select {
	case <-ctx.Done():
		return fmt.Errorf("canceled during startup verification: %w", ctx.Err())
	case <-time.After(startupVerify):
	}

	for i, containerID := range containerIDs {
		info, err := b.docker.InspectContainer(ctx, containerID)
		if err != nil {
			return fmt.Errorf("failed to verify container %d after startup: %w", i, err)
		}
		status := containerStatusToProvisionStatus(info.Status)
		if status != backend.ProvisionStatusReady {
			diag := b.containerFailureDiagnostics(ctx, containerID, info)
			return fmt.Errorf("container %d exited during startup (status: %s): %s", i, info.Status, diag)
		}
	}
	return nil
}

// doProvision performs the actual container creation asynchronously.
// For multi-unit leases, it creates multiple containers.
// For multi-SKU leases, each container gets the appropriate resource profile.
func (b *Backend) doProvision(ctx context.Context, req backend.ProvisionRequest, manifest *DockerManifest, profiles map[string]SKUProfile, logger *slog.Logger) {
	totalQuantity := req.TotalQuantity()
	var containerIDs []string
	var createdVolumeIDs []string // tracks volumes actually created for accurate cleanup
	var err error
	var callbackErr string // hardcoded message for callbacks (safe for on-chain)
	provisionStart := time.Now()

	// Single cleanup path via defer - handles both early cancellation and runtime errors
	defer func() {
		provisionDurationSeconds.Observe(time.Since(provisionStart).Seconds())
		if err != nil {
			logger.Error("provision failed", "lease_uuid", req.LeaseUUID, "error", err)
			provisionsTotal.WithLabelValues("failure").Inc()
			// Clean up on failure - release all allocated resources
			for i := 0; i < totalQuantity; i++ {
				b.pool.Release(fmt.Sprintf("%s-%d", req.LeaseUUID, i))
			}

			var diagSnap shared.DiagnosticEntry
			b.provisionsMu.Lock()
			if prov, ok := b.provisions[req.LeaseUUID]; ok {
				prov.Status = backend.ProvisionStatusFailed
				prov.FailCount++
				prov.LastError = err.Error()
				diagSnap = diagnosticSnapshot(prov)
			}
			b.provisionsMu.Unlock()
			// Persist diagnostics outside lock — fetches container logs (I/O).
			b.persistDiagnostics(diagSnap, containerIDs)

			// Clean up any containers that were created.
			// Use a fresh context since the original may be canceled.
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			for _, cid := range containerIDs {
				if rmErr := b.docker.RemoveContainer(cleanupCtx, cid); rmErr != nil {
					logger.Warn("failed to cleanup container after error", "container_id", shortID(cid), "error", rmErr)
				}
			}

			// Destroy only volumes that were actually created during this attempt.
			for _, volumeID := range createdVolumeIDs {
				if volErr := b.volumes.Destroy(cleanupCtx, volumeID); volErr != nil {
					logger.Warn("failed to cleanup volume after error", "volume_id", volumeID, "error", volErr)
				}
			}

			// Send hardcoded callback message — never includes logs or dynamic data.
			// Full diagnostics remain in prov.LastError for authenticated API access.
			b.sendCallback(req.LeaseUUID, false, callbackErr)
			return
		}

		// Update provision record on success
		provisionsTotal.WithLabelValues("success").Inc()
		activeProvisions.Inc()
		b.provisionsMu.Lock()
		if prov, ok := b.provisions[req.LeaseUUID]; ok {
			prov.ContainerIDs = containerIDs
			prov.Status = backend.ProvisionStatusReady
			prov.Manifest = manifest
			prov.LastError = ""
		}
		b.provisionsMu.Unlock()

		// Record initial release
		if b.releaseStore != nil {
			if relErr := b.releaseStore.Append(req.LeaseUUID, shared.Release{
				Manifest:  req.Payload,
				Image:     manifest.Image,
				Status:    "active",
				CreatedAt: time.Now(),
			}); relErr != nil {
				b.logger.Warn("failed to record initial release", "lease", req.LeaseUUID, "error", relErr)
			}
		}

		// Remove any stale diagnostic entry from a previous failure
		// (e.g., re-provision after a crash).
		if b.diagnosticsStore != nil {
			if delErr := b.diagnosticsStore.Delete(req.LeaseUUID); delErr != nil {
				b.logger.Warn("failed to remove stale diagnostic entry", "lease", req.LeaseUUID, "error", delErr)
			}
		}

		updateResourceMetrics(b.pool.Stats())
		b.sendCallback(req.LeaseUUID, true, "")
	}()

	// Check for early cancellation (e.g., shutdown requested before we started)
	if ctx.Err() != nil {
		logger.Warn("provisioning canceled before start", "error", ctx.Err())
		err = fmt.Errorf("provisioning canceled: %w", ctx.Err())
		callbackErr = "provisioning canceled"
		return
	}

	// Pull image (only once, shared by all containers)
	logger.Info("pulling image", "image", manifest.Image)
	pullStart := time.Now()
	if err = b.docker.PullImage(ctx, manifest.Image, b.cfg.ImagePullTimeout); err != nil {
		logger.Error("failed to pull image", "error", err)
		err = fmt.Errorf("image pull failed: %w", err)
		callbackErr = "image pull failed"
		return
	}
	imagePullDurationSeconds.Observe(time.Since(pullStart).Seconds())

	// Inspect image and resolve user
	imgSetup, setupErr := b.inspectImageForSetup(ctx, manifest.Image, manifest.User)
	if setupErr != nil {
		logger.Error("image setup failed", "error", setupErr)
		err = setupErr
		callbackErr = "image inspect failed"
		return
	}
	if imgSetup.ContainerUser != "" {
		logger.Info("resolved container user", "uid", imgSetup.VolumeUID, "gid", imgSetup.VolumeGID)
	}
	if len(imgSetup.WritablePaths) > 0 {
		logger.Info("auto-detected writable paths", "paths", imgSetup.WritablePaths, "uid", imgSetup.VolumeUID)
	}

	// Set up per-tenant network isolation
	networkConfig, netErr := b.ensureNetworkConfig(ctx, req.Tenant)
	if netErr != nil {
		logger.Error("failed to create tenant network", "error", netErr)
		err = netErr
		callbackErr = "tenant network setup failed"
		return
	}
	if networkConfig != nil {
		logger.Info("tenant network ready")
	}

	// Create and start containers for each item/unit
	containerIDs = make([]string, 0, totalQuantity)
	instanceIndex := 0
	for _, item := range req.Items {
		profile := profiles[item.SKU]

		for i := 0; i < item.Quantity; i++ {
			instanceLogger := logger.With("instance", instanceIndex, "sku", item.SKU)

			// Create container with instance index for unique naming
			instanceLogger.Info("creating container")
			// Read the current fail count so it's persisted in the container label.
			b.provisionsMu.RLock()
			failCount := 0
			if prov, ok := b.provisions[req.LeaseUUID]; ok {
				failCount = prov.FailCount
			}
			b.provisionsMu.RUnlock()

			// Create managed volume for stateful SKUs (disk_mb > 0) or writable paths.
			// doProvision uses inline volume creation to track newly-created volumes
			// for selective cleanup (reused volumes from a previous provision must not
			// be destroyed). doRestart/doUpdate use setupVolumeBinds which treats
			// volumes as idempotent.
			var volumeBinds map[string]string
			var writablePathBinds map[string]string
			needsStatefulVolume := profile.DiskMB > 0 && len(imgSetup.Volumes) > 0
			needsWritableVolume := len(imgSetup.WritablePaths) > 0

			if needsStatefulVolume || needsWritableVolume {
				volumeID := fmt.Sprintf("fred-%s-%d", req.LeaseUUID, instanceIndex)
				sizeMB := profile.DiskMB
				if sizeMB <= 0 {
					sizeMB = int64(b.cfg.GetTmpfsSizeMB())
				}
				hostPath, volCreated, volErr := b.volumes.Create(ctx, volumeID, sizeMB)
				if volErr != nil {
					if needsStatefulVolume {
						instanceLogger.Error("failed to create volume", "error", volErr)
						err = fmt.Errorf("volume creation failed (instance %d, sku %s): %w", instanceIndex, item.SKU, volErr)
						callbackErr = "volume creation failed"
						return
					}
					// Writable paths only — degrade: skip mounts, container may fail at startup.
					instanceLogger.Warn("writable path content seeding unavailable (volume creation failed)", "error", volErr)
				} else {
					if volCreated {
						createdVolumeIDs = append(createdVolumeIDs, volumeID)
					}

					// Set up VOLUME path subdirs (if stateful)
					if needsStatefulVolume {
						var buildErr error
						volumeBinds, buildErr = buildStatefulVolumeBinds(hostPath, imgSetup.Volumes, imgSetup.VolumeUID, imgSetup.VolumeGID)
						if buildErr != nil {
							err = fmt.Errorf("volume setup failed (instance %d, sku %s): %w", instanceIndex, item.SKU, buildErr)
							callbackErr = "volume creation failed"
							return
						}
					}

					// Set up writable path binds
					if needsWritableVolume {
						writablePathBinds = b.setupWritablePathBinds(ctx, manifest.Image, imgSetup.WritablePaths, hostPath, sizeMB*1024*1024)
					}
				}
			} else if profile.DiskMB > 0 && len(imgSetup.Volumes) == 0 {
				instanceLogger.Warn("stateful SKU has disk_mb > 0 but image declares no VOLUME paths; disk budget is allocated but unused",
					"disk_mb", profile.DiskMB)
			}

			createStart := time.Now()
			containerID, createErr := b.docker.CreateContainer(ctx, CreateContainerParams{
				LeaseUUID:         req.LeaseUUID,
				Tenant:            req.Tenant,
				ProviderUUID:      req.ProviderUUID,
				SKU:               item.SKU,
				Manifest:          manifest,
				Profile:           profile,
				InstanceIndex:     instanceIndex,
				FailCount:         failCount,
				CallbackURL:       req.CallbackURL,
				HostBindIP:        b.cfg.GetHostBindIP(),
				ReadonlyRootfs:    b.cfg.IsReadonlyRootfs(),
				PidsLimit:         b.cfg.GetPidsLimit(),
				TmpfsSizeMB:       b.cfg.GetTmpfsSizeMB(),
				NetworkConfig:     networkConfig,
				VolumeBinds:       volumeBinds,
				ImageVolumes:      imgSetup.Volumes,
				WritablePathBinds: writablePathBinds,
				User:              imgSetup.ContainerUser,
				BackendName:       b.cfg.Name,
			}, b.cfg.ContainerCreateTimeout)
			containerCreateDurationSeconds.Observe(time.Since(createStart).Seconds())
			if createErr != nil {
				instanceLogger.Error("failed to create container", "error", createErr)
				err = fmt.Errorf("container creation failed (instance %d, sku %s): %w", instanceIndex, item.SKU, createErr)
				callbackErr = "container creation failed"
				return
			}
			containerIDs = append(containerIDs, containerID)

			// Start container
			instanceLogger.Info("starting container", "container_id", shortID(containerID))
			if startErr := b.docker.StartContainer(ctx, containerID, b.cfg.ContainerStartTimeout); startErr != nil {
				instanceLogger.Error("failed to start container", "error", startErr)
				err = fmt.Errorf("container start failed (instance %d, sku %s): %w", instanceIndex, item.SKU, startErr)
				callbackErr = "container start failed"
				return
			}

			instanceLogger.Info("container provisioned successfully", "container_id", shortID(containerID))
			instanceIndex++
		}
	}

	// Startup verification
	if err = b.verifyStartup(ctx, manifest, containerIDs, logger); err != nil {
		callbackErr = startupErrorToCallbackMsg(err)
		return
	}

	logger.Info("all containers provisioned and verified", "count", len(containerIDs))
}

// doProvisionStack performs container creation for a stack (multi-service) lease
// using Docker Compose. Compose handles container creation, start ordering, and
// network attachment atomically via a single Up call.
func (b *Backend) doProvisionStack(ctx context.Context, req backend.ProvisionRequest, stack *StackManifest, profiles map[string]SKUProfile, logger *slog.Logger) {
	var containerIDs []string
	var createdVolumeIDs []string
	var err error
	var callbackErr string
	provisionStart := time.Now()
	serviceContainers := make(map[string][]string)
	projectName := composeProjectName(req.LeaseUUID)

	defer func() {
		provisionDurationSeconds.Observe(time.Since(provisionStart).Seconds())
		if err != nil {
			logger.Error("stack provision failed", "lease_uuid", req.LeaseUUID, "error", err)
			provisionsTotal.WithLabelValues("failure").Inc()

			// Release all service-aware allocation IDs.
			for _, item := range req.Items {
				for i := 0; i < item.Quantity; i++ {
					b.pool.Release(fmt.Sprintf("%s-%s-%d", req.LeaseUUID, item.ServiceName, i))
				}
			}

			var diagSnap shared.DiagnosticEntry
			b.provisionsMu.Lock()
			if prov, ok := b.provisions[req.LeaseUUID]; ok {
				prov.Status = backend.ProvisionStatusFailed
				prov.FailCount++
				prov.LastError = err.Error()
				diagSnap = diagnosticSnapshot(prov)
			}
			b.provisionsMu.Unlock()
			b.persistDiagnostics(diagSnap, containerIDs, stackContainerLogKeys(serviceContainers))

			// Clean up via Compose Down (removes all project containers).
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			if downErr := b.compose.Down(cleanupCtx, projectName, 10*time.Second); downErr != nil {
				logger.Warn("compose down failed during cleanup, falling back to individual removal", "error", downErr)
				for _, cid := range containerIDs {
					if rmErr := b.docker.RemoveContainer(cleanupCtx, cid); rmErr != nil {
						logger.Warn("failed to cleanup container after error", "container_id", shortID(cid), "error", rmErr)
					}
				}
			}
			for _, volumeID := range createdVolumeIDs {
				if volErr := b.volumes.Destroy(cleanupCtx, volumeID); volErr != nil {
					logger.Warn("failed to cleanup volume after error", "volume_id", volumeID, "error", volErr)
				}
			}
			b.sendCallback(req.LeaseUUID, false, callbackErr)
			return
		}

		provisionsTotal.WithLabelValues("success").Inc()
		activeProvisions.Inc()
		b.provisionsMu.Lock()
		if prov, ok := b.provisions[req.LeaseUUID]; ok {
			prov.ContainerIDs = containerIDs
			prov.ServiceContainers = serviceContainers
			prov.Status = backend.ProvisionStatusReady
			prov.LastError = ""
		}
		b.provisionsMu.Unlock()

		if b.releaseStore != nil {
			if relErr := b.releaseStore.Append(req.LeaseUUID, shared.Release{
				Manifest:  req.Payload,
				Image:     "stack",
				Status:    "active",
				CreatedAt: time.Now(),
			}); relErr != nil {
				b.logger.Warn("failed to record initial release", "lease", req.LeaseUUID, "error", relErr)
			}
		}

		if b.diagnosticsStore != nil {
			if delErr := b.diagnosticsStore.Delete(req.LeaseUUID); delErr != nil {
				b.logger.Warn("failed to remove stale diagnostic entry", "lease", req.LeaseUUID, "error", delErr)
			}
		}

		updateResourceMetrics(b.pool.Stats())
		b.sendCallback(req.LeaseUUID, true, "")
	}()

	if ctx.Err() != nil {
		logger.Warn("provisioning canceled before start", "error", ctx.Err())
		err = fmt.Errorf("provisioning canceled: %w", ctx.Err())
		callbackErr = "provisioning canceled"
		return
	}

	// Pull each unique image (deduplicated across services).
	pulledImages := make(map[string]bool)
	for svcName, svc := range stack.Services {
		if pulledImages[svc.Image] {
			continue
		}
		logger.Info("pulling image", "service", svcName, "image", svc.Image)
		pullStart := time.Now()
		if err = b.docker.PullImage(ctx, svc.Image, b.cfg.ImagePullTimeout); err != nil {
			logger.Error("failed to pull image", "service", svcName, "error", err)
			err = fmt.Errorf("image pull failed for service %s: %w", svcName, err)
			callbackErr = "image pull failed"
			return
		}
		imagePullDurationSeconds.Observe(time.Since(pullStart).Seconds())
		pulledImages[svc.Image] = true
	}

	// Per-service image setup (inspect, user resolution, writable paths).
	imageSetups := make(map[string]*imageSetup)
	for svcName, svc := range stack.Services {
		imgSetup, setupErr := b.inspectImageForSetup(ctx, svc.Image, svc.User)
		if setupErr != nil {
			logger.Error("image setup failed", "service", svcName, "error", setupErr)
			err = setupErr
			callbackErr = "image inspect failed"
			return
		}
		imageSetups[svcName] = imgSetup
	}

	// Resolve tenant network name (not Docker network ID — Compose needs the name).
	var networkName string
	if b.cfg.IsNetworkIsolation() {
		_, netErr := b.docker.EnsureTenantNetwork(ctx, req.Tenant)
		if netErr != nil {
			logger.Error("failed to create tenant network", "error", netErr)
			err = netErr
			callbackErr = "tenant network setup failed"
			return
		}
		networkName = TenantNetworkName(req.Tenant)
	}

	// Pre-create volumes and build volume binds per service/instance.
	b.provisionsMu.RLock()
	failCount := 0
	if prov, ok := b.provisions[req.LeaseUUID]; ok {
		failCount = prov.FailCount
	}
	b.provisionsMu.RUnlock()

	var volBinds map[string]map[int]serviceVolBinds
	volBinds, createdVolumeIDs, err = b.setupStackVolBinds(ctx, req.LeaseUUID, req.Items, profiles, imageSetups, stack.Services, logger)
	if err != nil {
		callbackErr = "volume creation failed"
		return
	}

	// Build Compose project and bring it up.
	project := buildComposeProject(composeProjectParams{
		LeaseUUID:    req.LeaseUUID,
		Tenant:       req.Tenant,
		ProviderUUID: req.ProviderUUID,
		CallbackURL:  req.CallbackURL,
		BackendName:  b.cfg.Name,
		FailCount:    failCount,
		Stack:        stack,
		Items:        req.Items,
		Profiles:     profiles,
		ImageSetups:  imageSetups,
		NetworkName:  networkName,
		VolBinds:     volBinds,
		Cfg:          &b.cfg,
	})

	logger.Info("compose up", "project", projectName, "services", len(project.Services))
	if upErr := b.compose.Up(ctx, project, composeUpOpts{}); upErr != nil {
		err = fmt.Errorf("compose up failed: %w", upErr)
		callbackErr = "container creation failed"
		return
	}

	// Discover container IDs via Compose PS.
	containers, psErr := b.compose.PS(ctx, projectName)
	if psErr != nil {
		err = fmt.Errorf("compose ps failed: %w", psErr)
		callbackErr = "container creation failed"
		return
	}

	containerIDs, serviceContainers = mapComposeContainers(containers, req.Items)

	// Verify startup per-service so each service uses its own health check config.
	for svcName, svcCIDs := range serviceContainers {
		svc := stack.Services[svcName]
		if err = b.verifyStartup(ctx, svc, svcCIDs, logger.With("service", svcName)); err != nil {
			callbackErr = startupErrorToCallbackMsg(err)
			return
		}
	}

	logger.Info("all stack containers provisioned and verified", "count", len(containerIDs), "services", len(stack.Services))
}

// mapComposeContainers maps Compose PS output to containerIDs and serviceContainers.
// For fanned-out services (web-0, web-1), it strips the instance suffix to recover
// the original service name.
func mapComposeContainers(containers []composeContainerSummary, items []backend.LeaseItem) ([]string, map[string][]string) {
	// Build a set of base service names for fan-out detection.
	svcQuantities := make(map[string]int, len(items))
	for _, item := range items {
		svcQuantities[item.ServiceName] = item.Quantity
	}

	var containerIDs []string
	serviceContainers := make(map[string][]string)

	for _, c := range containers {
		containerIDs = append(containerIDs, c.ID)
		// Recover original service name from Compose service name.
		// Fan-out: "web-0" → "web", single: "web" → "web".
		// We verify the suffix is a valid integer to avoid prefix collisions
		// (e.g., "web-extra" must not match "web" with qty>1).
		baseName := c.Service
		for svcName, qty := range svcQuantities {
			if qty > 1 && strings.HasPrefix(c.Service, svcName+"-") {
				suffix := strings.TrimPrefix(c.Service, svcName+"-")
				if _, parseErr := strconv.Atoi(suffix); parseErr == nil {
					baseName = svcName
					break
				}
			}
		}
		serviceContainers[baseName] = append(serviceContainers[baseName], c.ID)
	}
	return containerIDs, serviceContainers
}

// healthPollInterval is the interval between health check polls during startup verification.
const healthPollInterval = 2 * time.Second

// startupErrorToCallbackMsg maps a verifyStartup or waitForHealthy error to a
// hardcoded callback message safe for on-chain surfacing.
func startupErrorToCallbackMsg(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "unhealthy"):
		return "container reported unhealthy"
	case strings.Contains(msg, "exited during startup"):
		return "container exited during startup"
	case strings.Contains(msg, "canceled during startup verification"):
		return "container startup verification canceled"
	case strings.Contains(msg, "exited"):
		return "container exited during health check"
	default:
		return "container exited during startup"
	}
}

// waitForHealthy polls container health status until all containers report
// "healthy". It fails immediately if any container becomes "unhealthy" or
// exits. The method is bounded by the caller's context (typically the
// ProvisionTimeout).
func (b *Backend) waitForHealthy(ctx context.Context, containerIDs []string, logger *slog.Logger) error {
	pending := make(map[int]struct{}, len(containerIDs))
	for i := range containerIDs {
		pending[i] = struct{}{}
	}

	ticker := time.NewTicker(healthPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for containers to become healthy: %w", ctx.Err())
		case <-ticker.C:
			for i := range pending {
				info, err := b.docker.InspectContainer(ctx, containerIDs[i])
				if err != nil {
					return fmt.Errorf("failed to inspect container %d during health check: %w", i, err)
				}

				// Check if container has exited.
				status := containerStatusToProvisionStatus(info.Status)
				if status == backend.ProvisionStatusFailed {
					diag := b.containerFailureDiagnostics(ctx, containerIDs[i], info)
					return fmt.Errorf("container %d exited while waiting for healthy (status: %s): %s", i, info.Status, diag)
				}

				switch info.Health {
				case HealthStatusHealthy:
					logger.Info("container healthy", "instance", i, "container_id", shortID(containerIDs[i]))
					delete(pending, i)
				case HealthStatusUnhealthy:
					diag := b.containerFailureDiagnostics(ctx, containerIDs[i], info)
					return fmt.Errorf("container %d reported unhealthy: %s", i, diag)
				default:
					// "starting" or other — keep polling
				}
			}

			if len(pending) == 0 {
				return nil
			}
		}
	}
}

// Restart restarts containers for a lease without changing the manifest.
// State machine: Ready|Failed → Restarting → Ready|Failed
func (b *Backend) Restart(ctx context.Context, req backend.RestartRequest) error {
	logger := b.logger.With("lease_uuid", req.LeaseUUID)

	// Synchronous phase: validate state and transition to Restarting
	b.provisionsMu.Lock()
	prov, exists := b.provisions[req.LeaseUUID]
	if !exists {
		b.provisionsMu.Unlock()
		return backend.ErrNotProvisioned
	}
	if prov.Status != backend.ProvisionStatusReady && prov.Status != backend.ProvisionStatusFailed {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: cannot restart from status %s", backend.ErrInvalidState, prov.Status)
	}
	if prov.Manifest == nil && prov.StackManifest == nil {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: no stored manifest for restart", backend.ErrInvalidState)
	}
	isStack := prov.IsStack()
	prevStatus := prov.Status
	prov.Status = backend.ProvisionStatusRestarting
	if req.CallbackURL != "" {
		prov.CallbackURL = req.CallbackURL
	}
	manifest := prov.Manifest
	stackManifest := prov.StackManifest
	containerIDs := prov.ContainerIDs
	serviceContainers := prov.ServiceContainers
	items := prov.Items
	sku := prov.SKU
	b.provisionsMu.Unlock()

	// Record restart release as deploying. Abort if this fails — without a
	// release record, ActivateLatest after success is a no-op, and a cold
	// restart would recover the previous manifest (silently rolling back).
	if b.releaseStore != nil {
		var manifestBytes []byte
		var marshalErr error
		var releaseImage string
		if isStack {
			manifestBytes, marshalErr = json.Marshal(stackManifest)
			releaseImage = "stack"
		} else {
			manifestBytes, marshalErr = json.Marshal(manifest)
			releaseImage = manifest.Image
		}
		if marshalErr != nil {
			b.provisionsMu.Lock()
			prov.Status = prevStatus
			b.provisionsMu.Unlock()
			return fmt.Errorf("failed to marshal manifest for release: %w", marshalErr)
		}
		if relErr := b.releaseStore.Append(req.LeaseUUID, shared.Release{
			Manifest:  manifestBytes,
			Image:     releaseImage,
			Status:    "deploying",
			CreatedAt: time.Now(),
		}); relErr != nil {
			b.provisionsMu.Lock()
			prov.Status = prevStatus
			b.provisionsMu.Unlock()
			return fmt.Errorf("failed to record release: %w", relErr)
		}
	}

	// Async phase
	b.wg.Go(func() {
		provisionTimeout := cmp.Or(b.cfg.ProvisionTimeout, 10*time.Minute)
		ctx, cancel := context.WithTimeout(context.Background(), provisionTimeout)
		defer cancel()

		stop := context.AfterFunc(b.stopCtx, cancel)
		defer stop()

		if isStack {
			b.doRestartStack(ctx, req.LeaseUUID, stackManifest, containerIDs, serviceContainers, items, logger)
		} else {
			b.doRestart(ctx, req.LeaseUUID, manifest, containerIDs, sku, logger)
		}
	})

	return nil
}

// doRestart performs the actual container restart asynchronously.
func (b *Backend) doRestart(ctx context.Context, leaseUUID string, manifest *DockerManifest, oldContainerIDs []string, sku string, logger *slog.Logger) {
	profile, profErr := b.cfg.GetSKUProfile(sku)
	if profErr != nil {
		b.recordPreflightFailure(leaseUUID, "restart failed",
			fmt.Errorf("SKU profile lookup failed: %w", profErr),
			backend.ProvisionStatusReady, logger)
		return
	}

	b.doReplaceContainers(ctx, replaceContainersOp{
		LeaseUUID:       leaseUUID,
		Manifest:        manifest,
		SKU:             sku,
		Profile:         profile,
		OldContainerIDs: oldContainerIDs,
		Quantity:        len(oldContainerIDs),
		Operation:       "restart",
		Logger:          logger,
	})
}

// doRestartStack performs an async stack restart: stops all service containers
// and recreates them from the stored StackManifest.
func (b *Backend) doRestartStack(ctx context.Context, leaseUUID string, stack *StackManifest, oldContainerIDs []string, serviceContainers map[string][]string, items []backend.LeaseItem, logger *slog.Logger) {
	profiles := make(map[string]SKUProfile, len(items))
	for _, item := range items {
		if _, ok := profiles[item.SKU]; ok {
			continue
		}
		profile, profErr := b.cfg.GetSKUProfile(item.SKU)
		if profErr != nil {
			b.recordPreflightFailure(leaseUUID, "restart failed",
				fmt.Errorf("SKU profile lookup failed for %s: %w", item.SKU, profErr),
				backend.ProvisionStatusReady, logger)
			return
		}
		profiles[item.SKU] = profile
	}

	b.doReplaceStackContainers(ctx, replaceStackContainersOp{
		LeaseUUID:         leaseUUID,
		Stack:             stack,
		Items:             items,
		Profiles:          profiles,
		OldContainerIDs:   oldContainerIDs,
		ServiceContainers: serviceContainers,
		Operation:         "restart",
		Logger:            logger,
	})
}

// replaceStackContainersOp describes a stack container replacement operation.
type replaceStackContainersOp struct {
	LeaseUUID         string
	Stack             *StackManifest
	Items             []backend.LeaseItem
	Profiles          map[string]SKUProfile
	OldContainerIDs   []string
	ServiceContainers map[string][]string // old service → container IDs mapping
	Operation         string              // "restart" or "update"
	Logger            *slog.Logger

	// OnSuccess is called under provisionsMu lock after successful replacement.
	OnSuccess func(prov *provision)
}

// doReplaceStackContainers performs the stack container replacement lifecycle
// using Docker Compose. Compose handles stopping old containers and starting
// new ones via a single Up call, with rollback via Up with the previous manifest.
func (b *Backend) doReplaceStackContainers(ctx context.Context, op replaceStackContainersOp) {
	var err error
	var callbackErr string
	var newContainerIDs []string
	var imageSetups map[string]*imageSetup
	newServiceContainers := make(map[string][]string)
	projectName := composeProjectName(op.LeaseUUID)

	defer func() {
		if err != nil {
			op.Logger.Error(op.Operation+" failed (stack)", "error", err)

			var diagSnap shared.DiagnosticEntry
			b.provisionsMu.Lock()
			if prov, ok := b.provisions[op.LeaseUUID]; ok {
				prov.LastError = err.Error()
				prov.FailCount++
				diagSnap = diagnosticSnapshot(prov)
			}
			b.provisionsMu.Unlock()
			b.persistDiagnostics(diagSnap, newContainerIDs, stackContainerLogKeys(newServiceContainers))

			if b.releaseStore != nil {
				if relErr := b.releaseStore.UpdateLatestStatus(op.LeaseUUID, "failed", err.Error()); relErr != nil {
					op.Logger.Warn("failed to update release status", "error", relErr)
				}
			}

			// Rollback: rebuild the Project from the previous StackManifest and
			// Compose Up to restore the old containers.
			restored := b.rollbackStackViaCompose(op)
			b.provisionsMu.Lock()
			if prov, ok := b.provisions[op.LeaseUUID]; ok {
				if restored {
					prov.Status = backend.ProvisionStatusReady
					if op.Operation == "restart" {
						prov.LastError = ""
					}
					op.Logger.Info("rolled back to previous containers via compose (stack)")
				} else {
					prov.Status = backend.ProvisionStatusFailed
				}
			}
			b.provisionsMu.Unlock()

			if restored {
				callbackErr += "; rolled back to previous version"
			} else {
				callbackErr += "; rollback failed"
			}

			b.sendCallback(op.LeaseUUID, false, callbackErr)
			return
		}

		// Success: update provision (Compose already replaced old containers).
		b.provisionsMu.Lock()
		if prov, ok := b.provisions[op.LeaseUUID]; ok {
			prov.ContainerIDs = newContainerIDs
			prov.ServiceContainers = newServiceContainers
			prov.Status = backend.ProvisionStatusReady
			prov.LastError = ""
			if op.OnSuccess != nil {
				op.OnSuccess(prov)
			}
		}
		b.provisionsMu.Unlock()

		if b.releaseStore != nil {
			if relErr := b.releaseStore.ActivateLatest(op.LeaseUUID); relErr != nil {
				op.Logger.Warn("failed to update release status", "error", relErr)
			}
		}

		b.sendCallback(op.LeaseUUID, true, "")
	}()

	// Per-service image setup.
	imageSetups = make(map[string]*imageSetup)
	for svcName, svc := range op.Stack.Services {
		imgSetup, setupErr := b.inspectImageForSetup(ctx, svc.Image, svc.User)
		if setupErr != nil {
			err = setupErr
			callbackErr = op.Operation + " failed"
			return
		}
		imageSetups[svcName] = imgSetup
	}

	// Read provision metadata.
	b.provisionsMu.RLock()
	failCount := 0
	tenant := ""
	providerUUID := ""
	callbackURL := ""
	if prov, ok := b.provisions[op.LeaseUUID]; ok {
		failCount = prov.FailCount
		tenant = prov.Tenant
		providerUUID = prov.ProviderUUID
		callbackURL = prov.CallbackURL
	}
	b.provisionsMu.RUnlock()

	// Resolve tenant network name.
	var networkName string
	if b.cfg.IsNetworkIsolation() {
		if _, netErr := b.docker.EnsureTenantNetwork(ctx, tenant); netErr != nil {
			err = netErr
			callbackErr = op.Operation + " failed"
			return
		}
		networkName = TenantNetworkName(tenant)
	}

	// Ensure volumes exist for all services/instances.
	volBinds, _, volErr := b.setupStackVolBinds(ctx, op.LeaseUUID, op.Items, op.Profiles, imageSetups, op.Stack.Services, op.Logger)
	if volErr != nil {
		err = volErr
		callbackErr = op.Operation + " failed"
		return
	}

	// Build Compose project and bring it up.
	// ForceRecreate is used for restarts (config unchanged but containers need replacing).
	project := buildComposeProject(composeProjectParams{
		LeaseUUID:    op.LeaseUUID,
		Tenant:       tenant,
		ProviderUUID: providerUUID,
		CallbackURL:  callbackURL,
		BackendName:  b.cfg.Name,
		FailCount:    failCount,
		Stack:        op.Stack,
		Items:        op.Items,
		Profiles:     op.Profiles,
		ImageSetups:  imageSetups,
		NetworkName:  networkName,
		VolBinds:     volBinds,
		Cfg:          &b.cfg,
	})

	op.Logger.Info("compose up for "+op.Operation, "project", projectName, "services", len(project.Services))
	forceRecreate := op.Operation == "restart"
	if upErr := b.compose.Up(ctx, project, composeUpOpts{ForceRecreate: forceRecreate}); upErr != nil {
		err = fmt.Errorf("compose up failed: %w", upErr)
		callbackErr = op.Operation + " failed"
		return
	}

	// Discover new container IDs via Compose PS.
	containers, psErr := b.compose.PS(ctx, projectName)
	if psErr != nil {
		err = fmt.Errorf("compose ps failed: %w", psErr)
		callbackErr = op.Operation + " failed"
		return
	}

	newContainerIDs, newServiceContainers = mapComposeContainers(containers, op.Items)

	// Verify startup per-service so each service uses its own health check config.
	for svcName, svcCIDs := range newServiceContainers {
		svc := op.Stack.Services[svcName]
		if err = b.verifyStartup(ctx, svc, svcCIDs, op.Logger.With("service", svcName)); err != nil {
			callbackErr = startupErrorToCallbackMsg(err)
			return
		}
	}

	op.Logger.Info(op.Operation+" completed (stack)", "containers", len(newContainerIDs))
}

// rollbackStackViaCompose restores the previous stack state by rebuilding a
// Compose project from the previous StackManifest (still in the provision,
// since OnSuccess hasn't run) and calling Compose Up. Returns true on success.
func (b *Backend) rollbackStackViaCompose(op replaceStackContainersOp) bool {
	rollbackCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Read previous manifest from provision (OnSuccess hasn't run, so
	// prov.StackManifest is still the old manifest).
	b.provisionsMu.RLock()
	prov, ok := b.provisions[op.LeaseUUID]
	if !ok {
		b.provisionsMu.RUnlock()
		op.Logger.Error("rollback: provision not found")
		return false
	}
	prevStack := prov.StackManifest
	tenant := prov.Tenant
	providerUUID := prov.ProviderUUID
	callbackURL := prov.CallbackURL
	failCount := prov.FailCount
	b.provisionsMu.RUnlock()

	if prevStack == nil {
		op.Logger.Error("rollback: no previous stack manifest available")
		return false
	}

	// Inspect images for the previous manifest.
	prevImageSetups := make(map[string]*imageSetup)
	for svcName, svc := range prevStack.Services {
		imgSetup, setupErr := b.inspectImageForSetup(rollbackCtx, svc.Image, svc.User)
		if setupErr != nil {
			op.Logger.Error("rollback: image inspection failed", "service", svcName, "error", setupErr)
			return false
		}
		prevImageSetups[svcName] = imgSetup
	}

	// Resolve network name.
	var networkName string
	if b.cfg.IsNetworkIsolation() {
		networkName = TenantNetworkName(tenant)
	}

	// Re-use existing volumes (already created during original provision).
	volBinds, _, volErr := b.setupStackVolBinds(rollbackCtx, op.LeaseUUID, op.Items, op.Profiles, prevImageSetups, prevStack.Services, op.Logger)
	if volErr != nil {
		op.Logger.Error("rollback: volume setup failed", "error", volErr)
		return false
	}

	// Build project from previous manifest.
	project := buildComposeProject(composeProjectParams{
		LeaseUUID:    op.LeaseUUID,
		Tenant:       tenant,
		ProviderUUID: providerUUID,
		CallbackURL:  callbackURL,
		BackendName:  b.cfg.Name,
		FailCount:    failCount,
		Stack:        prevStack,
		Items:        op.Items,
		Profiles:     op.Profiles,
		ImageSetups:  prevImageSetups,
		NetworkName:  networkName,
		VolBinds:     volBinds,
		Cfg:          &b.cfg,
	})

	// Compose Up with ForceRecreate to restore previous containers.
	if upErr := b.compose.Up(rollbackCtx, project, composeUpOpts{ForceRecreate: true}); upErr != nil {
		op.Logger.Error("rollback: compose up failed", "error", upErr)
		return false
	}

	// Discover restored container IDs and update provision.
	containers, psErr := b.compose.PS(rollbackCtx, composeProjectName(op.LeaseUUID))
	if psErr != nil {
		op.Logger.Error("rollback: compose ps failed", "error", psErr)
		return false
	}

	containerIDs, serviceContainers := mapComposeContainers(containers, op.Items)
	b.provisionsMu.Lock()
	if p, ok := b.provisions[op.LeaseUUID]; ok {
		p.ContainerIDs = containerIDs
		p.ServiceContainers = serviceContainers
	}
	b.provisionsMu.Unlock()

	return true
}

// replaceContainersOp describes a container replacement operation with rollback.
// Used by both restart and update to share the stop → create → verify lifecycle.
type replaceContainersOp struct {
	LeaseUUID       string
	Manifest        *DockerManifest
	SKU             string
	Profile         SKUProfile
	OldContainerIDs []string
	Quantity        int    // Number of new containers to create
	Operation       string // "restart" or "update" — used in log and callback messages
	Logger          *slog.Logger

	// OnSuccess is called under provisionsMu lock after successful replacement.
	// Used by update to set Image/Manifest on the provision. May be nil.
	OnSuccess func(prov *provision)
}

// recordPreflightFailure handles errors that occur before any containers are modified
// (e.g., profile lookup, image pull). It persists diagnostics, updates release status,
// and sends a failure callback with callbackMsg.
func (b *Backend) recordPreflightFailure(leaseUUID string, callbackMsg string, err error, failStatus backend.ProvisionStatus, logger *slog.Logger) {
	logger.Error("preflight failed", "error", err)

	var diagSnap shared.DiagnosticEntry
	b.provisionsMu.Lock()
	if prov, ok := b.provisions[leaseUUID]; ok {
		prov.LastError = err.Error()
		prov.FailCount++
		prov.Status = failStatus
		diagSnap = diagnosticSnapshot(prov)
	}
	b.provisionsMu.Unlock()
	b.persistDiagnostics(diagSnap, nil)

	if b.releaseStore != nil {
		if relErr := b.releaseStore.UpdateLatestStatus(leaseUUID, "failed", err.Error()); relErr != nil {
			logger.Warn("failed to update release status", "error", relErr)
		}
	}

	b.sendCallback(leaseUUID, false, callbackMsg)
}

// doReplaceContainers performs the container replacement lifecycle:
// inspect image → read metadata → setup networking → stop and rename old →
// create and start new → verify startup.
// Old containers are kept stopped for rollback on failure.
func (b *Backend) doReplaceContainers(ctx context.Context, op replaceContainersOp) {
	var err error
	var callbackErr string
	var newContainerIDs []string
	var oldStopped bool

	defer func() {
		if err != nil {
			op.Logger.Error(op.Operation+" failed", "error", err)

			// Snapshot diagnostics under lock, then persist outside (I/O).
			var diagSnap shared.DiagnosticEntry
			b.provisionsMu.Lock()
			if prov, ok := b.provisions[op.LeaseUUID]; ok {
				prov.LastError = err.Error()
				prov.FailCount++
				diagSnap = diagnosticSnapshot(prov)
			}
			b.provisionsMu.Unlock()
			b.persistDiagnostics(diagSnap, newContainerIDs)

			// Mark release as failed.
			if b.releaseStore != nil {
				if relErr := b.releaseStore.UpdateLatestStatus(op.LeaseUUID, "failed", err.Error()); relErr != nil {
					op.Logger.Warn("failed to update release status", "error", relErr)
				}
			}

			// Clean up failed new containers.
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			for _, cid := range newContainerIDs {
				if rmErr := b.docker.RemoveContainer(cleanupCtx, cid); rmErr != nil {
					op.Logger.Warn("failed to cleanup container after "+op.Operation+" error", "container_id", shortID(cid), "error", rmErr)
				}
			}

			// Rollback: restart old containers to restore service.
			restored := !oldStopped || b.rollbackContainers(op.LeaseUUID, op.OldContainerIDs, op.Logger)
			b.provisionsMu.Lock()
			if prov, ok := b.provisions[op.LeaseUUID]; ok {
				if restored {
					prov.Status = backend.ProvisionStatusReady
					// Restart: clear error — we're back to the exact same state.
					// Update: keep LastError so the UI shows why the update failed.
					if oldStopped && op.Operation == "restart" {
						prov.LastError = ""
					}
				} else {
					prov.Status = backend.ProvisionStatusFailed
				}
				if restored {
					op.Logger.Info("rolled back to previous containers", "containers", len(op.OldContainerIDs))
				}
			}
			b.provisionsMu.Unlock()

			if restored {
				callbackErr += "; rolled back to previous version"
			} else if oldStopped {
				callbackErr += "; rollback failed"
			}

			b.sendCallback(op.LeaseUUID, false, callbackErr)
			return
		}

		// Success: remove old containers and update provision.
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		for _, cid := range op.OldContainerIDs {
			if rmErr := b.docker.RemoveContainer(cleanupCtx, cid); rmErr != nil {
				op.Logger.Warn("failed to remove old container after "+op.Operation, "container_id", shortID(cid), "error", rmErr)
			}
		}

		b.provisionsMu.Lock()
		if prov, ok := b.provisions[op.LeaseUUID]; ok {
			prov.ContainerIDs = newContainerIDs
			prov.Status = backend.ProvisionStatusReady
			prov.LastError = ""
			if op.OnSuccess != nil {
				op.OnSuccess(prov)
			}
		}
		b.provisionsMu.Unlock()

		// Mark release as active, previous as superseded.
		if b.releaseStore != nil {
			if relErr := b.releaseStore.ActivateLatest(op.LeaseUUID); relErr != nil {
				op.Logger.Warn("failed to update release status", "error", relErr)
			}
		}

		b.sendCallback(op.LeaseUUID, true, "")
	}()

	// Inspect image and resolve user.
	imgSetup, setupErr := b.inspectImageForSetup(ctx, op.Manifest.Image, op.Manifest.User)
	if setupErr != nil {
		err = setupErr
		callbackErr = op.Operation + " failed"
		return
	}
	if len(imgSetup.WritablePaths) > 0 {
		op.Logger.Info("auto-detected writable paths", "paths", imgSetup.WritablePaths, "uid", imgSetup.VolumeUID)
	}

	// Read provision metadata.
	b.provisionsMu.RLock()
	failCount := 0
	tenant := ""
	providerUUID := ""
	callbackURL := ""
	if prov, ok := b.provisions[op.LeaseUUID]; ok {
		failCount = prov.FailCount
		tenant = prov.Tenant
		providerUUID = prov.ProviderUUID
		callbackURL = prov.CallbackURL
	}
	b.provisionsMu.RUnlock()

	// Set up networking.
	networkConfig, netErr := b.ensureNetworkConfig(ctx, tenant)
	if netErr != nil {
		err = netErr
		callbackErr = op.Operation + " failed"
		return
	}

	// Stop and rename old containers to free the canonical name for replacements.
	// Old containers are kept stopped for rollback on failure.
	stopTimeout := cmp.Or(b.cfg.ContainerStopTimeout, 30*time.Second)
	for i, cid := range op.OldContainerIDs {
		op.Logger.Info("stopping container for "+op.Operation, "container_id", shortID(cid))
		if stopErr := b.docker.StopContainer(ctx, cid, stopTimeout); stopErr != nil {
			err = fmt.Errorf("failed to stop container %s: %w", shortID(cid), stopErr)
			callbackErr = op.Operation + " failed"
			return
		}
		oldStopped = true
		if renameErr := b.docker.RenameContainer(ctx, cid, prevContainerName(op.LeaseUUID, i)); renameErr != nil {
			err = fmt.Errorf("failed to rename old container %s: %w", shortID(cid), renameErr)
			callbackErr = op.Operation + " failed"
			return
		}
	}

	// Create and start new containers.
	newContainerIDs = make([]string, 0, op.Quantity)
	for i := range op.Quantity {
		volumeBinds, volErr := b.setupVolumeBinds(ctx, op.LeaseUUID, i, op.Profile.DiskMB, imgSetup.Volumes, imgSetup.VolumeUID, imgSetup.VolumeGID)
		if volErr != nil {
			err = volErr
			callbackErr = op.Operation + " failed"
			return
		}

		var writablePathBinds map[string]string
		if len(imgSetup.WritablePaths) > 0 {
			volumeID := fmt.Sprintf("fred-%s-%d", op.LeaseUUID, i)
			sizeMB := op.Profile.DiskMB
			if sizeMB <= 0 {
				sizeMB = int64(b.cfg.GetTmpfsSizeMB())
			}
			hostPath, _, wpVolErr := b.volumes.Create(ctx, volumeID, sizeMB)
			if wpVolErr == nil {
				writablePathBinds = b.setupWritablePathBinds(ctx, op.Manifest.Image, imgSetup.WritablePaths, hostPath, sizeMB*1024*1024)
			} else {
				op.Logger.Warn("writable path content seeding unavailable on "+op.Operation, "error", wpVolErr)
			}
		}

		containerID, createErr := b.docker.CreateContainer(ctx, CreateContainerParams{
			LeaseUUID:         op.LeaseUUID,
			Tenant:            tenant,
			ProviderUUID:      providerUUID,
			SKU:               op.SKU,
			Manifest:          op.Manifest,
			Profile:           op.Profile,
			InstanceIndex:     i,
			FailCount:         failCount,
			CallbackURL:       callbackURL,
			HostBindIP:        b.cfg.GetHostBindIP(),
			ReadonlyRootfs:    b.cfg.IsReadonlyRootfs(),
			PidsLimit:         b.cfg.GetPidsLimit(),
			TmpfsSizeMB:       b.cfg.GetTmpfsSizeMB(),
			NetworkConfig:     networkConfig,
			VolumeBinds:       volumeBinds,
			ImageVolumes:      imgSetup.Volumes,
			WritablePathBinds: writablePathBinds,
			User:              imgSetup.ContainerUser,
			BackendName:       b.cfg.Name,
		}, b.cfg.ContainerCreateTimeout)
		if createErr != nil {
			err = fmt.Errorf("container creation failed (instance %d): %w", i, createErr)
			callbackErr = op.Operation + " failed"
			return
		}
		newContainerIDs = append(newContainerIDs, containerID)

		if startErr := b.docker.StartContainer(ctx, containerID, b.cfg.ContainerStartTimeout); startErr != nil {
			err = fmt.Errorf("container start failed (instance %d): %w", i, startErr)
			callbackErr = op.Operation + " failed"
			return
		}
	}

	// Startup verification.
	if err = b.verifyStartup(ctx, op.Manifest, newContainerIDs, op.Logger); err != nil {
		callbackErr = startupErrorToCallbackMsg(err)
		return
	}

	op.Logger.Info(op.Operation+" completed", "containers", len(newContainerIDs))
}

// Update deploys a new manifest for a lease, replacing containers.
// State machine: Ready|Failed → Updating → Ready|Failed
func (b *Backend) Update(ctx context.Context, req backend.UpdateRequest) error {
	logger := b.logger.With("lease_uuid", req.LeaseUUID)

	// Synchronous phase: validate state and new manifest
	b.provisionsMu.Lock()
	prov, exists := b.provisions[req.LeaseUUID]
	if !exists {
		b.provisionsMu.Unlock()
		return backend.ErrNotProvisioned
	}
	if prov.Status != backend.ProvisionStatusReady && prov.Status != backend.ProvisionStatusFailed {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: cannot update from status %s", backend.ErrInvalidState, prov.Status)
	}

	isStack := prov.IsStack()

	// Parse new payload (auto-detects single vs stack).
	manifest, stackManifest, parseErr := ParsePayload(req.Payload)
	if parseErr != nil {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: %w", backend.ErrInvalidManifest, parseErr)
	}

	// Ensure payload type matches existing provision type.
	if isStack && stackManifest == nil {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: stack lease requires a stack manifest (with services key)", backend.ErrInvalidManifest)
	}
	if !isStack && stackManifest != nil {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: non-stack lease cannot be updated with a stack manifest", backend.ErrInvalidManifest)
	}

	if isStack {
		// Validate stack against stored items.
		if valErr := ValidateStackAgainstItems(stackManifest, prov.Items); valErr != nil {
			b.provisionsMu.Unlock()
			return fmt.Errorf("%w: %w", backend.ErrValidation, valErr)
		}
		// Validate all images.
		for svcName, svc := range stackManifest.Services {
			if imgErr := shared.ValidateImage(svc.Image, b.cfg.AllowedRegistries); imgErr != nil {
				b.provisionsMu.Unlock()
				return fmt.Errorf("%w: service %s: %w", backend.ErrValidation, svcName, imgErr)
			}
		}
		// Validate all SKU profiles.
		profiles := make(map[string]SKUProfile, len(prov.Items))
		for _, item := range prov.Items {
			if _, ok := profiles[item.SKU]; ok {
				continue
			}
			profile, profErr := b.cfg.GetSKUProfile(item.SKU)
			if profErr != nil {
				b.provisionsMu.Unlock()
				return fmt.Errorf("%w: %w", backend.ErrValidation, profErr)
			}
			profiles[item.SKU] = profile
		}

		oldContainerIDs := prov.ContainerIDs
		serviceContainers := prov.ServiceContainers
		items := prov.Items
		prevStatus := prov.Status
		prov.Status = backend.ProvisionStatusUpdating
		if req.CallbackURL != "" {
			prov.CallbackURL = req.CallbackURL
		}
		b.provisionsMu.Unlock()

		// Record release.
		releaseImage := "stack"
		if b.releaseStore != nil {
			if relErr := b.releaseStore.Append(req.LeaseUUID, shared.Release{
				Manifest:  req.Payload,
				Image:     releaseImage,
				Status:    "deploying",
				CreatedAt: time.Now(),
			}); relErr != nil {
				b.provisionsMu.Lock()
				prov.Status = prevStatus
				b.provisionsMu.Unlock()
				return fmt.Errorf("failed to record release: %w", relErr)
			}
		}

		// Async phase
		b.wg.Go(func() {
			provisionTimeout := cmp.Or(b.cfg.ProvisionTimeout, 10*time.Minute)
			ctx, cancel := context.WithTimeout(context.Background(), provisionTimeout)
			defer cancel()

			stop := context.AfterFunc(b.stopCtx, cancel)
			defer stop()

			b.doUpdateStack(ctx, req.LeaseUUID, stackManifest, profiles, oldContainerIDs, serviceContainers, items, logger)
		})
		return nil
	}

	// Legacy single-manifest path.
	if imgErr := shared.ValidateImage(manifest.Image, b.cfg.AllowedRegistries); imgErr != nil {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: %w", backend.ErrValidation, imgErr)
	}

	profile, profErr := b.cfg.GetSKUProfile(prov.SKU)
	if profErr != nil {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: %w", backend.ErrValidation, profErr)
	}

	oldContainerIDs := prov.ContainerIDs
	prevStatus := prov.Status
	prov.Status = backend.ProvisionStatusUpdating
	if req.CallbackURL != "" {
		prov.CallbackURL = req.CallbackURL
	}
	b.provisionsMu.Unlock()

	if b.releaseStore != nil {
		if relErr := b.releaseStore.Append(req.LeaseUUID, shared.Release{
			Manifest:  req.Payload,
			Image:     manifest.Image,
			Status:    "deploying",
			CreatedAt: time.Now(),
		}); relErr != nil {
			b.provisionsMu.Lock()
			prov.Status = prevStatus
			b.provisionsMu.Unlock()
			return fmt.Errorf("failed to record release: %w", relErr)
		}
	}

	b.wg.Go(func() {
		provisionTimeout := cmp.Or(b.cfg.ProvisionTimeout, 10*time.Minute)
		ctx, cancel := context.WithTimeout(context.Background(), provisionTimeout)
		defer cancel()

		stop := context.AfterFunc(b.stopCtx, cancel)
		defer stop()

		b.doUpdate(ctx, req.LeaseUUID, manifest, profile, oldContainerIDs, logger)
	})

	return nil
}

// doUpdate performs the actual container update asynchronously.
func (b *Backend) doUpdate(ctx context.Context, leaseUUID string, manifest *DockerManifest, profile SKUProfile, oldContainerIDs []string, logger *slog.Logger) {
	// Pull new image — this is the only update-specific pre-flight step.
	logger.Info("pulling image for update", "image", manifest.Image)
	if pullErr := b.docker.PullImage(ctx, manifest.Image, b.cfg.ImagePullTimeout); pullErr != nil {
		b.recordPreflightFailure(leaseUUID, "image pull failed",
			fmt.Errorf("image pull failed: %w", pullErr),
			backend.ProvisionStatusFailed, logger)
		return
	}

	// Read SKU and quantity from provision (may differ from old container count).
	b.provisionsMu.RLock()
	sku := ""
	quantity := len(oldContainerIDs)
	if prov, ok := b.provisions[leaseUUID]; ok {
		sku = prov.SKU
		quantity = prov.Quantity
	}
	b.provisionsMu.RUnlock()

	b.doReplaceContainers(ctx, replaceContainersOp{
		LeaseUUID:       leaseUUID,
		Manifest:        manifest,
		SKU:             sku,
		Profile:         profile,
		OldContainerIDs: oldContainerIDs,
		Quantity:        quantity,
		Operation:       "update",
		Logger:          logger,
		OnSuccess: func(prov *provision) {
			prov.Image = manifest.Image
			prov.Manifest = manifest
		},
	})
}

// doUpdateStack performs the actual stack container update asynchronously.
func (b *Backend) doUpdateStack(ctx context.Context, leaseUUID string, stack *StackManifest, profiles map[string]SKUProfile, oldContainerIDs []string, serviceContainers map[string][]string, items []backend.LeaseItem, logger *slog.Logger) {
	// Pull each unique image (deduplicated).
	pulledImages := make(map[string]bool)
	for svcName, svc := range stack.Services {
		if pulledImages[svc.Image] {
			continue
		}
		logger.Info("pulling image for update", "service", svcName, "image", svc.Image)
		if pullErr := b.docker.PullImage(ctx, svc.Image, b.cfg.ImagePullTimeout); pullErr != nil {
			b.recordPreflightFailure(leaseUUID, "image pull failed",
				fmt.Errorf("image pull failed for service %s: %w", svcName, pullErr),
				backend.ProvisionStatusFailed, logger)
			return
		}
		pulledImages[svc.Image] = true
	}

	b.doReplaceStackContainers(ctx, replaceStackContainersOp{
		LeaseUUID:         leaseUUID,
		Stack:             stack,
		Items:             items,
		Profiles:          profiles,
		OldContainerIDs:   oldContainerIDs,
		ServiceContainers: serviceContainers,
		Operation:         "update",
		Logger:            logger,
		OnSuccess: func(prov *provision) {
			prov.StackManifest = stack
		},
	})
}

// GetReleases returns the release history for a lease.
func (b *Backend) GetReleases(_ context.Context, leaseUUID string) ([]backend.ReleaseInfo, error) {
	b.provisionsMu.RLock()
	_, exists := b.provisions[leaseUUID]
	b.provisionsMu.RUnlock()

	if !exists {
		return nil, backend.ErrNotProvisioned
	}

	if b.releaseStore == nil {
		return nil, nil
	}

	releases, err := b.releaseStore.List(leaseUUID)
	if err != nil {
		return nil, fmt.Errorf("failed to list releases: %w", err)
	}

	result := make([]backend.ReleaseInfo, len(releases))
	for i, r := range releases {
		result[i] = backend.ReleaseInfo{
			Version:   r.Version,
			Image:     r.Image,
			Status:    r.Status,
			CreatedAt: r.CreatedAt,
			Error:     r.Error,
			Manifest:  r.Manifest,
		}
	}
	return result, nil
}

// GetInfo returns lease information including connection details.
// For multi-unit leases, returns an "instances" array with each container's info.
// For stack leases, returns a "services" map grouping instances by service name.
func (b *Backend) GetInfo(ctx context.Context, leaseUUID string) (*backend.LeaseInfo, error) {
	b.provisionsMu.RLock()
	prov, exists := b.provisions[leaseUUID]
	b.provisionsMu.RUnlock()

	if !exists {
		return nil, backend.ErrNotProvisioned
	}

	if prov.Status != backend.ProvisionStatusReady {
		return nil, backend.ErrNotProvisioned
	}

	// Stack response: group instances by service name.
	if prov.IsStack() {
		services := make(map[string]any)
		for svcName, containerIDs := range prov.ServiceContainers {
			var instances []map[string]any
			for _, containerID := range containerIDs {
				info, err := b.docker.InspectContainer(ctx, containerID)
				if err != nil {
					return nil, fmt.Errorf("failed to inspect container: %w", err)
				}
				ports := make(map[string]map[string]string)
				for portSpec, binding := range info.Ports {
					ports[portSpec] = map[string]string{
						"host_ip":   binding.HostIP,
						"host_port": binding.HostPort,
					}
				}
				instances = append(instances, map[string]any{
					"instance_index": info.InstanceIndex,
					"container_id":   shortID(info.ContainerID),
					"image":          info.Image,
					"status":         info.Status,
					"ports":          ports,
				})
			}
			services[svcName] = map[string]any{
				"instances": instances,
			}
		}
		leaseInfo := backend.LeaseInfo{
			"host":     b.cfg.HostAddress,
			"services": services,
		}
		return &leaseInfo, nil
	}

	// Legacy response: flat instances array.
	var instances []map[string]any
	for _, containerID := range prov.ContainerIDs {
		info, err := b.docker.InspectContainer(ctx, containerID)
		if err != nil {
			return nil, fmt.Errorf("failed to inspect container: %w", err)
		}

		ports := make(map[string]map[string]string)
		for portSpec, binding := range info.Ports {
			ports[portSpec] = map[string]string{
				"host_ip":   binding.HostIP,
				"host_port": binding.HostPort,
			}
		}

		instances = append(instances, map[string]any{
			"instance_index": info.InstanceIndex,
			"container_id":   shortID(info.ContainerID),
			"image":          info.Image,
			"status":         info.Status,
			"ports":          ports,
		})
	}

	leaseInfo := backend.LeaseInfo{
		"host":      b.cfg.HostAddress,
		"instances": instances,
	}

	return &leaseInfo, nil
}

// Deprovision releases resources for a lease. Must be idempotent.
// For multi-unit leases, removes all containers.
// Returns an error if any container removal fails for a reason other than
// the container already being gone (which is handled idempotently by
// RemoveContainer).
//
// On partial failure (some containers removed, some stuck), the provision
// is kept in the map with Status=Failed and ContainerIDs narrowed to only
// the failed removals. Resource pool allocations are still released (the
// lease is being abandoned). On retry, only the stuck containers are
// attempted.
func (b *Backend) Deprovision(ctx context.Context, leaseUUID string) error {
	logger := b.logger.With("lease_uuid", leaseUUID)

	b.provisionsMu.Lock()
	prov, exists := b.provisions[leaseUUID]
	if !exists {
		b.provisionsMu.Unlock()
		// Already deprovisioned - idempotent success
		return nil
	}
	// Mark as failed before removing containers to prevent handleContainerDeath
	// from racing with removal. Die events emitted during RemoveContainer will
	// see a non-Ready status and be skipped.
	prov.Status = backend.ProvisionStatusFailed
	b.provisionsMu.Unlock()

	// Remove all containers.
	// For stacks, use Compose Down for atomic cleanup; fall back to individual
	// removal if Compose fails. For single-container leases, use RemoveContainer.
	var errs []error
	var failedIDs []string
	if prov.IsStack() {
		stopTimeout := cmp.Or(b.cfg.ContainerStopTimeout, 30*time.Second)
		if downErr := b.compose.Down(ctx, composeProjectName(leaseUUID), stopTimeout); downErr != nil {
			logger.Warn("compose down failed, falling back to individual removal", "error", downErr)
			for _, containerID := range prov.ContainerIDs {
				if err := b.docker.RemoveContainer(ctx, containerID); err != nil {
					logger.Error("failed to remove container", "container_id", shortID(containerID), "error", err)
					errs = append(errs, fmt.Errorf("container %s: %w", shortID(containerID), err))
					failedIDs = append(failedIDs, containerID)
				} else {
					logger.Info("container removed", "container_id", shortID(containerID))
				}
			}
		} else {
			logger.Info("compose down completed", "project", composeProjectName(leaseUUID))
		}
	} else {
		for _, containerID := range prov.ContainerIDs {
			if err := b.docker.RemoveContainer(ctx, containerID); err != nil {
				logger.Error("failed to remove container", "container_id", shortID(containerID), "error", err)
				errs = append(errs, fmt.Errorf("container %s: %w", shortID(containerID), err))
				failedIDs = append(failedIDs, containerID)
			} else {
				logger.Info("container removed", "container_id", shortID(containerID))
			}
		}
	}

	// Release resource pool allocations regardless of outcome — the lease
	// is being abandoned and these resources should be freed.
	if prov.IsStack() {
		for _, item := range prov.Items {
			for i := 0; i < item.Quantity; i++ {
				b.pool.Release(fmt.Sprintf("%s-%s-%d", leaseUUID, item.ServiceName, i))
			}
		}
	} else {
		for i := 0; i < prov.Quantity; i++ {
			b.pool.Release(fmt.Sprintf("%s-%d", leaseUUID, i))
		}
	}

	if len(errs) > 0 {
		// Partial failure: keep provision visible with only the stuck containers
		// so the reconciler (or a retry) can see and re-attempt them.
		var diagSnap shared.DiagnosticEntry
		b.provisionsMu.Lock()
		if p, ok := b.provisions[leaseUUID]; ok {
			p.Status = backend.ProvisionStatusFailed
			p.ContainerIDs = failedIDs
			p.LastError = fmt.Sprintf("deprovision partially failed: %s", errors.Join(errs...))
			diagSnap = diagnosticSnapshot(p)
		}
		b.provisionsMu.Unlock()
		b.persistDiagnostics(diagSnap, failedIDs)
		return fmt.Errorf("deprovision partially failed: %w", errors.Join(errs...))
	}

	// Destroy managed volumes for all instances.
	var volumeErrs []error
	if prov.IsStack() {
		for _, item := range prov.Items {
			for i := 0; i < item.Quantity; i++ {
				volumeID := fmt.Sprintf("fred-%s-%s-%d", leaseUUID, item.ServiceName, i)
				if volErr := b.volumes.Destroy(ctx, volumeID); volErr != nil {
					logger.Error("failed to destroy volume", "volume_id", volumeID, "error", volErr)
					volumeErrs = append(volumeErrs, fmt.Errorf("volume %s: %w", volumeID, volErr))
				}
			}
		}
	} else {
		for i := 0; i < prov.Quantity; i++ {
			volumeID := fmt.Sprintf("fred-%s-%d", leaseUUID, i)
			if volErr := b.volumes.Destroy(ctx, volumeID); volErr != nil {
				logger.Error("failed to destroy volume", "volume_id", volumeID, "error", volErr)
				volumeErrs = append(volumeErrs, fmt.Errorf("volume %s: %w", volumeID, volErr))
			}
		}
	}

	if len(volumeErrs) > 0 {
		b.provisionsMu.Lock()
		if p, ok := b.provisions[leaseUUID]; ok {
			p.VolumeCleanupAttempts++
			p.ContainerIDs = nil // containers are gone

			if p.VolumeCleanupAttempts >= maxVolumeCleanupAttempts {
				// Too many failed attempts — give up and remove the provision.
				// The leaked volumes require manual cleanup by the operator.
				tenant := p.Tenant
				delete(b.provisions, leaseUUID)
				b.provisionsMu.Unlock()

				// Perform the same cleanup as the normal success path.
				if b.releaseStore != nil {
					if err := b.releaseStore.Delete(leaseUUID); err != nil {
						logger.Warn("failed to delete release history", "error", err)
					}
				}
				if b.cfg.IsNetworkIsolation() {
					if err := b.docker.RemoveTenantNetworkIfEmpty(ctx, tenant); err != nil {
						logger.Warn("failed to remove tenant network", "tenant", tenant, "error", err)
					}
				}
				deprovisionsTotal.Inc()
				activeProvisions.Dec()
				updateResourceMetrics(b.pool.Stats())

				logger.Error("MANUAL CLEANUP REQUIRED: volume cleanup failed after max attempts, giving up",
					"attempts", p.VolumeCleanupAttempts,
					"errors", errors.Join(volumeErrs...),
				)
				return nil
			}

			// Under the limit — keep provision visible for retry.
			p.Status = backend.ProvisionStatusFailed
			p.LastError = fmt.Sprintf("volume cleanup failed: %s", errors.Join(volumeErrs...))
		}
		b.provisionsMu.Unlock()
		return fmt.Errorf("volume cleanup failed: %w", errors.Join(volumeErrs...))
	}

	// Clean up release history
	if b.releaseStore != nil {
		if err := b.releaseStore.Delete(leaseUUID); err != nil {
			logger.Warn("failed to delete release history", "error", err)
		}
	}

	// All containers and volumes removed — delete provision from map.
	b.provisionsMu.Lock()
	delete(b.provisions, leaseUUID)
	b.provisionsMu.Unlock()

	// Clean up tenant network if isolation is enabled
	if b.cfg.IsNetworkIsolation() {
		if err := b.docker.RemoveTenantNetworkIfEmpty(ctx, prov.Tenant); err != nil {
			logger.Warn("failed to remove tenant network", "tenant", prov.Tenant, "error", err)
		}
	}

	deprovisionsTotal.Inc()
	activeProvisions.Dec()
	updateResourceMetrics(b.pool.Stats())
	logger.Info("deprovisioned", "containers_removed", len(prov.ContainerIDs))
	return nil
}

// RefreshState synchronizes in-memory provision state with Docker.
func (b *Backend) RefreshState(ctx context.Context) error {
	return b.recoverState(ctx)
}

// GetProvision returns a single provision by lease UUID.
// Falls back to the diagnostics store when the provision is not in memory
// (e.g., after deprovision). Returns ErrNotProvisioned only if both miss.
func (b *Backend) GetProvision(_ context.Context, leaseUUID string) (*backend.ProvisionInfo, error) {
	b.provisionsMu.RLock()
	prov, ok := b.provisions[leaseUUID]
	b.provisionsMu.RUnlock()

	if ok {
		return &backend.ProvisionInfo{
			LeaseUUID:    prov.LeaseUUID,
			ProviderUUID: prov.ProviderUUID,
			Status:       prov.Status,
			CreatedAt:    prov.CreatedAt,
			BackendName:  b.cfg.Name,
			FailCount:    prov.FailCount,
			LastError:    prov.LastError,
		}, nil
	}

	// Fall back to persisted diagnostics.
	if b.diagnosticsStore != nil {
		entry, err := b.diagnosticsStore.Get(leaseUUID)
		if err != nil {
			b.logger.Warn("diagnostics store lookup failed", "lease_uuid", leaseUUID, "error", err)
		}
		if entry != nil {
			return &backend.ProvisionInfo{
				LeaseUUID:    entry.LeaseUUID,
				ProviderUUID: entry.ProviderUUID,
				Status:       backend.ProvisionStatusFailed,
				CreatedAt:    entry.CreatedAt,
				BackendName:  b.cfg.Name,
				FailCount:    entry.FailCount,
				LastError:    entry.Error,
			}, nil
		}
	}

	return nil, backend.ErrNotProvisioned
}

// ListProvisions returns all currently provisioned resources.
func (b *Backend) ListProvisions(_ context.Context) ([]backend.ProvisionInfo, error) {
	b.provisionsMu.RLock()
	defer b.provisionsMu.RUnlock()

	result := make([]backend.ProvisionInfo, 0, len(b.provisions))
	for _, prov := range b.provisions {
		result = append(result, backend.ProvisionInfo{
			LeaseUUID:    prov.LeaseUUID,
			ProviderUUID: prov.ProviderUUID,
			Status:       prov.Status,
			CreatedAt:    prov.CreatedAt,
			BackendName:  b.cfg.Name,
			FailCount:    prov.FailCount,
			LastError:    prov.LastError,
		})
	}

	return result, nil
}

// removeProvision removes a provision reservation and its callback URL.
// Used when pre-flight validation fails after the slot was reserved.
func (b *Backend) removeProvision(leaseUUID string) {
	b.provisionsMu.Lock()
	delete(b.provisions, leaseUUID)
	b.provisionsMu.Unlock()
}

// recoverState rebuilds in-memory state from Docker containers.
// Handles multi-unit leases by grouping containers by lease UUID.
// Merges with existing state to preserve in-flight provisions.
//
// Serialized by recoverMu to prevent concurrent calls (from the
// reconcile loop and RefreshState) from duplicating transition
// detection and failure callbacks.
func (b *Backend) recoverState(ctx context.Context) error {
	b.recoverMu.Lock()
	defer b.recoverMu.Unlock()

	containers, err := b.docker.ListManagedContainers(ctx)
	if err != nil {
		return err
	}

	allocsByLease := make(map[string][]shared.ResourceAllocation)
	recovered := make(map[string]*provision)
	skippedUnknownSKU := 0

	// Group containers by lease UUID
	for _, c := range containers {
		// Skip containers without required labels
		if c.LeaseUUID == "" || c.SKU == "" {
			b.logger.Warn("skipping container with missing labels", "container_id", shortID(c.ContainerID))
			continue
		}

		// Look up SKU profile for resource allocation
		profile, err := b.cfg.GetSKUProfile(c.SKU)
		if err != nil {
			b.logger.Error("skipping container with unknown SKU — container is running but untracked",
				"container_id", shortID(c.ContainerID),
				"sku", c.SKU,
			)
			skippedUnknownSKU++
			continue
		}

		// Check if we already have a provision record for this lease
		prov, exists := recovered[c.LeaseUUID]
		if !exists {
			prov = &provision{
				LeaseUUID:    c.LeaseUUID,
				Tenant:       c.Tenant,
				ProviderUUID: c.ProviderUUID,
				SKU:          c.SKU,
				Image:        c.Image,
				Status:       containerStatusToProvisionStatus(c.Status),
				CreatedAt:    c.CreatedAt,
				FailCount:    c.FailCount,
				CallbackURL:  c.CallbackURL,
				ContainerIDs: make([]string, 0),
			}

			// Restore manifest from the last successful (active) release so
			// restart/update work after a cold start (manifest is not stored
			// in labels). Using LatestActive avoids picking up a failed
			// release (e.g., a failed update to a newer image).
			if b.releaseStore != nil {
				if rel, relErr := b.releaseStore.LatestActive(c.LeaseUUID); relErr == nil && rel != nil && len(rel.Manifest) > 0 {
					// Use ParsePayload to auto-detect single vs stack manifest.
					legacyM, stackM, payloadErr := ParsePayload(rel.Manifest)
					switch {
					case payloadErr != nil:
						b.logger.Warn("failed to parse recovered manifest",
							"lease_uuid", c.LeaseUUID, "error", payloadErr)
					case stackM != nil:
						prov.StackManifest = stackM
					case legacyM != nil:
						prov.Manifest = legacyM
					}
				}
			}

			recovered[c.LeaseUUID] = prov
		}

		// Add container ID to the provision
		prov.ContainerIDs = append(prov.ContainerIDs, c.ContainerID)
		prov.Quantity = len(prov.ContainerIDs)

		// Build ServiceContainers map and Items for stack containers.
		if c.ServiceName != "" {
			if prov.ServiceContainers == nil {
				prov.ServiceContainers = make(map[string][]string)
			}
			prov.ServiceContainers[c.ServiceName] = append(prov.ServiceContainers[c.ServiceName], c.ContainerID)

			// Rebuild Items from container labels (SKU + ServiceName per container).
			// Use a dedup map keyed by service name since multiple containers
			// belong to the same item.
			found := false
			for idx := range prov.Items {
				if prov.Items[idx].ServiceName == c.ServiceName {
					prov.Items[idx].Quantity = len(prov.ServiceContainers[c.ServiceName])
					found = true
					break
				}
			}
			if !found {
				prov.Items = append(prov.Items, backend.LeaseItem{
					SKU:         c.SKU,
					Quantity:    1,
					ServiceName: c.ServiceName,
				})
			}
		}

		// Use the highest FailCount across containers. Labels are normally
		// identical, but can diverge after a partial re-provision.
		if c.FailCount > prov.FailCount {
			prov.FailCount = c.FailCount
		}

		// If any container is not ready, mark the whole provision as not ready
		status := containerStatusToProvisionStatus(c.Status)
		if status != backend.ProvisionStatusReady && prov.Status == backend.ProvisionStatusReady {
			prov.Status = status
		}

		// Use instance-specific allocation ID, grouped by lease for filtering.
		// Stack uses service-aware IDs: {leaseUUID}-{serviceName}-{instanceIndex}
		var instanceID string
		if c.ServiceName != "" {
			instanceID = fmt.Sprintf("%s-%s-%d", c.LeaseUUID, c.ServiceName, c.InstanceIndex)
		} else {
			instanceID = fmt.Sprintf("%s-%d", c.LeaseUUID, c.InstanceIndex)
		}
		allocsByLease[c.LeaseUUID] = append(allocsByLease[c.LeaseUUID], shared.ResourceAllocation{
			LeaseUUID: instanceID,
			Tenant:    c.Tenant,
			SKU:       c.SKU,
			CPUCores:  profile.CPUCores,
			MemoryMB:  profile.MemoryMB,
			DiskMB:    profile.DiskMB,
		})
	}

	// Merge with existing state and detect status transitions.
	b.provisionsMu.Lock()

	// Detect ready→failed transitions: containers that were running but have since crashed.
	// We must notify Fred so the lease doesn't remain active for a dead container.
	var failedLeases []string
	for uuid, existing := range b.provisions {
		if existing.Status == backend.ProvisionStatusReady {
			if rec, ok := recovered[uuid]; ok && rec.Status == backend.ProvisionStatusFailed {
				// Carry over FailCount and increment for this failure.
				rec.FailCount = existing.FailCount + 1
				rec.LastError = errMsgContainerExited
				failedLeases = append(failedLeases, uuid)
				b.logger.Warn("container crashed after provisioning",
					"lease_uuid", uuid,
					"tenant", existing.Tenant,
					"fail_count", rec.FailCount,
				)
			}
		}
	}

	// Cold-start correction: provisions recovered as failed with no prior
	// in-memory state have a FailCount from the container label that was
	// written at creation time (before this failure occurred). Increment
	// it to account for the failure evidenced by the container being dead.
	var coldStartFailed []string
	for uuid, rec := range recovered {
		if rec.Status == backend.ProvisionStatusFailed {
			if _, hasExisting := b.provisions[uuid]; !hasExisting {
				rec.FailCount++
				rec.LastError = errMsgContainerExited
				coldStartFailed = append(coldStartFailed, uuid)
				b.logger.Info("cold-start: adjusted FailCount for already-failed provision",
					"lease_uuid", uuid,
					"fail_count", rec.FailCount,
				)
			}
		}
	}

	// Preserve provisions without containers that need to remain visible
	// to fred's reconciler.
	for uuid, existing := range b.provisions {
		if rec, hasContainers := recovered[uuid]; hasContainers {
			if existing.Status == backend.ProvisionStatusProvisioning || existing.Status == backend.ProvisionStatusRestarting || existing.Status == backend.ProvisionStatusUpdating {
				// In-flight re-provision: the containers in recovered belong to the
				// previous (failed) provision and carry stale FailCount labels. The
				// old containers are being removed by Provision() concurrently.
				// Preserve the in-flight entry with its correct FailCount so the
				// next container creation picks up the right value.
				recovered[uuid] = existing
			} else if existing.FailCount > rec.FailCount {
				// Container labels carry the FailCount from creation time. When
				// recoverState increments FailCount in-memory (e.g., ready→failed
				// transition), subsequent recoverState calls re-read the stale label
				// value. Preserve the higher in-memory count to prevent regression.
				rec.FailCount = existing.FailCount
			}
			continue // Already recovered from containers
		}
		switch existing.Status {
		case backend.ProvisionStatusProvisioning, backend.ProvisionStatusRestarting, backend.ProvisionStatusUpdating:
			// In-flight operation that hasn't produced containers yet — preserve it.
			recovered[uuid] = existing
		case backend.ProvisionStatusFailed:
			// Failed provision whose containers have been cleaned up (e.g., after
			// a failed re-provision attempt). Preserve so fred's reconciler can
			// see the failure and its FailCount for retry/close decisions.
			recovered[uuid] = existing
		}
	}
	b.provisions = recovered

	// Build the final allocations list, excluding leases with in-flight
	// operations (provisioning/restarting/updating). Their old containers
	// are being cleaned up concurrently; including stale allocations would
	// race with TryAllocate in Provision/Restart/Update.
	var allocations []shared.ResourceAllocation
	for uuid, allocs := range allocsByLease {
		if prov, ok := recovered[uuid]; ok {
			switch prov.Status {
			case backend.ProvisionStatusProvisioning, backend.ProvisionStatusRestarting, backend.ProvisionStatusUpdating:
				continue
			}
		}
		allocations = append(allocations, allocs...)
	}
	b.pool.Reset(allocations)
	b.provisionsMu.Unlock()

	// Reset the active provisions gauge from the recovered map. Without this,
	// the gauge drifts (and can go negative) because Inc/Dec are only called
	// during normal Provision/Deprovision, but recoverState replaces the map.
	var readyCount float64
	for _, p := range recovered {
		if p.Status == backend.ProvisionStatusReady {
			readyCount++
		}
	}
	activeProvisions.Set(readyCount)
	updateResourceMetrics(b.pool.Stats())

	// Gather diagnostics for failed leases (I/O outside the lock).
	allFailed := slices.Concat(failedLeases, coldStartFailed)
	failedDiagnostics := make(map[string]string, len(allFailed))
	for _, uuid := range allFailed {
		b.provisionsMu.RLock()
		prov, ok := b.provisions[uuid]
		if !ok {
			b.provisionsMu.RUnlock()
			continue
		}
		containerIDs := prov.ContainerIDs
		b.provisionsMu.RUnlock()

		for _, cid := range containerIDs {
			info, inspErr := b.docker.InspectContainer(ctx, cid)
			if inspErr != nil {
				b.logger.Warn("failed to inspect container during diagnostics gathering", "lease", uuid, "container_id", shortID(cid), "error", inspErr)
				continue
			}
			if containerStatusToProvisionStatus(info.Status) == backend.ProvisionStatusFailed {
				failedDiagnostics[uuid] = b.containerFailureDiagnostics(ctx, cid, info)
				break
			}
		}
	}

	// Update LastError with enriched diagnostics.
	if len(failedDiagnostics) > 0 {
		b.provisionsMu.Lock()
		for uuid, diag := range failedDiagnostics {
			if prov, ok := b.provisions[uuid]; ok {
				prov.LastError = errMsgContainerExited + ": " + diag
			}
		}
		b.provisionsMu.Unlock()
	}

	// Snapshot diagnostics under lock, then persist outside (I/O).
	type diagItem struct {
		entry        shared.DiagnosticEntry
		containerIDs []string
		keys         map[string]string
	}
	diagItems := make([]diagItem, 0, len(allFailed))
	b.provisionsMu.RLock()
	for _, uuid := range allFailed {
		if prov, ok := b.provisions[uuid]; ok {
			diagItems = append(diagItems, diagItem{
				entry:        diagnosticSnapshot(prov),
				containerIDs: append([]string(nil), prov.ContainerIDs...),
				keys:         containerLogKeys(prov),
			})
		}
	}
	b.provisionsMu.RUnlock()
	for _, item := range diagItems {
		b.persistDiagnostics(item.entry, item.containerIDs, item.keys)
	}

	// Send failure callbacks with hardcoded message — never includes logs
	// or dynamic data. Full diagnostics are in prov.LastError for
	// authenticated API access.
	for _, uuid := range failedLeases {
		b.sendCallback(uuid, false, errMsgContainerExited)
	}

	totalContainers := 0
	for _, p := range recovered {
		totalContainers += len(p.ContainerIDs)
	}

	stats := b.pool.Stats()
	logAttrs := []any{
		"leases", len(recovered),
		"containers", totalContainers,
		"cpu_allocated", stats.AllocatedCPU,
		"memory_allocated_mb", stats.AllocatedMemoryMB,
	}
	if skippedUnknownSKU > 0 {
		logAttrs = append(logAttrs, "untracked_unknown_sku", skippedUnknownSKU)
	}
	b.logger.Info("state recovered", logAttrs...)

	// Clean up orphaned tenant networks if network isolation is enabled
	if b.cfg.IsNetworkIsolation() {
		b.cleanupOrphanedNetworks(ctx, recovered)
	}

	return nil
}

// cleanupOrphanedVolumes destroys volumes on disk that have no matching provision.
// This catches volumes leaked by crashes between volume creation and container creation,
// or between container removal and volume destruction. Called once at startup after
// recoverState populates the provision map.
func (b *Backend) cleanupOrphanedVolumes(ctx context.Context) error {
	volumeIDs, err := b.volumes.List()
	if err != nil {
		return fmt.Errorf("list volumes: %w", err)
	}
	if len(volumeIDs) == 0 {
		return nil
	}

	// Build set of expected volume IDs from recovered provisions.
	expected := make(map[string]bool)
	b.provisionsMu.RLock()
	for leaseUUID, prov := range b.provisions {
		if prov.IsStack() {
			for _, item := range prov.Items {
				for i := 0; i < item.Quantity; i++ {
					expected[fmt.Sprintf("fred-%s-%s-%d", leaseUUID, item.ServiceName, i)] = true
				}
			}
		} else {
			for i := 0; i < prov.Quantity; i++ {
				expected[fmt.Sprintf("fred-%s-%d", leaseUUID, i)] = true
			}
		}
	}
	b.provisionsMu.RUnlock()

	var orphanCount, failCount int
	for _, id := range volumeIDs {
		if expected[id] {
			continue
		}
		b.logger.Info("destroying orphaned volume", "volume_id", id)
		if err := b.volumes.Destroy(ctx, id); err != nil {
			b.logger.Error("failed to destroy orphaned volume", "volume_id", id, "error", err)
			failCount++
		} else {
			orphanCount++
		}
	}
	if orphanCount > 0 || failCount > 0 {
		b.logger.Info("orphaned volume cleanup complete", "destroyed", orphanCount, "failed", failCount)
	}
	return nil
}

// cleanupOrphanedNetworks removes managed networks whose tenant has no active provisions.
func (b *Backend) cleanupOrphanedNetworks(ctx context.Context, provisions map[string]*provision) {
	networks, err := b.docker.ListManagedNetworks(ctx)
	if err != nil {
		b.logger.Warn("failed to list managed networks for cleanup", "error", err)
		return
	}

	// Build set of active tenants
	activeTenants := make(map[string]bool)
	for _, p := range provisions {
		activeTenants[p.Tenant] = true
	}

	for _, n := range networks {
		tenant := n.Labels[LabelTenant]
		if tenant != "" && !activeTenants[tenant] && len(n.Containers) == 0 {
			if err := b.docker.RemoveTenantNetworkIfEmpty(ctx, tenant); err != nil {
				b.logger.Warn("failed to remove orphaned network", "network", n.Name, "error", err)
			} else {
				b.logger.Info("removed orphaned tenant network", "network", n.Name, "tenant", tenant)
			}
		}
	}
}

// reconcileLoop periodically reconciles the in-memory state with Docker.
// Note: WaitGroup.Done is handled by the caller via wg.Go() (Go 1.25+).
func (b *Backend) reconcileLoop() {
	ticker := time.NewTicker(b.cfg.ReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.stopCtx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := b.recoverState(ctx); err != nil {
				b.logger.Error("reconciliation failed", "error", err)
				reconciliationTotal.WithLabelValues("error").Inc()
			} else {
				reconciliationTotal.WithLabelValues("success").Inc()
			}
			cancel()
		}
	}
}

// containerEventLoop subscribes to Docker container "die" events and triggers
// immediate failure handling. This provides near-instant detection of container
// crashes, complementing the 5-minute reconcileLoop safety net.
func (b *Backend) containerEventLoop() {
	for {
		select {
		case <-b.stopCtx.Done():
			return
		default:
		}

		eventCh, errCh := b.docker.ContainerEvents(b.stopCtx)

	consume:
		for {
			select {
			case <-b.stopCtx.Done():
				return
			case event, ok := <-eventCh:
				if !ok {
					break consume
				}
				if event.Action == "die" {
					b.handleContainerDeath(event.ContainerID)
				}
			case err, ok := <-errCh:
				if !ok {
					break consume
				}
				b.logger.Warn("container event stream error, reconnecting", "error", err)
				break consume
			}
		}

		// Backoff before reconnecting to avoid tight loop on persistent errors.
		select {
		case <-b.stopCtx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

// handleContainerDeath processes a single container death event. If the
// container belongs to a lease in Ready status, it transitions the lease to
// Failed and sends a callback — the same transition that recoverState
// performs, but for a single container in real time.
func (b *Backend) handleContainerDeath(containerID string) {
	leaseUUID, _, found := b.findLeaseByContainerID(containerID)
	if !found {
		return
	}

	// Only transition ready→failed. Other states (provisioning, restarting,
	// updating, already failed) are managed by other code paths.
	// Read status under lock to avoid data race with concurrent writers.
	b.provisionsMu.RLock()
	prov, exists := b.provisions[leaseUUID]
	if !exists || prov.Status != backend.ProvisionStatusReady {
		b.provisionsMu.RUnlock()
		return
	}
	b.provisionsMu.RUnlock()

	// Defensive: verify the container is actually dead via inspect.
	// Docker events can be duplicated or arrive out of order.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	info, err := b.docker.InspectContainer(ctx, containerID)
	if err != nil {
		b.logger.Warn("failed to inspect container after die event",
			"container_id", shortID(containerID),
			"lease_uuid", leaseUUID,
			"error", err,
		)
		return
	}
	if containerStatusToProvisionStatus(info.Status) != backend.ProvisionStatusFailed {
		return // Container restarted or state changed before we got here
	}

	// Transition to failed under write lock.
	b.provisionsMu.Lock()
	// Re-check status under write lock to avoid racing with recoverState or
	// another die event for a multi-container lease.
	currentProv, exists := b.provisions[leaseUUID]
	if !exists || currentProv.Status != backend.ProvisionStatusReady {
		b.provisionsMu.Unlock()
		return
	}
	currentProv.Status = backend.ProvisionStatusFailed
	currentProv.FailCount++
	currentProv.LastError = errMsgContainerExited
	b.provisionsMu.Unlock()

	// Gather diagnostics (I/O outside the lock, same pattern as recoverState).
	diag := b.containerFailureDiagnostics(ctx, containerID, info)
	if diag != "" {
		b.provisionsMu.Lock()
		if p, ok := b.provisions[leaseUUID]; ok {
			p.LastError = errMsgContainerExited + ": " + diag
		}
		b.provisionsMu.Unlock()
	}

	// Persist diagnostics (snapshot under lock, I/O outside).
	var diagSnap shared.DiagnosticEntry
	var diagContainerIDs []string
	var diagKeys map[string]string
	b.provisionsMu.RLock()
	if p, ok := b.provisions[leaseUUID]; ok {
		diagSnap = diagnosticSnapshot(p)
		diagContainerIDs = append([]string(nil), p.ContainerIDs...)
		diagKeys = containerLogKeys(p)
	}
	b.provisionsMu.RUnlock()
	b.persistDiagnostics(diagSnap, diagContainerIDs, diagKeys)

	// Update metrics and send callback.
	activeProvisions.Dec()
	b.sendCallback(leaseUUID, false, errMsgContainerExited)

	logAttrs := []any{
		"lease_uuid", leaseUUID,
		"container_id", shortID(containerID),
		"fail_count", currentProv.FailCount,
	}
	if info.ServiceName != "" {
		logAttrs = append(logAttrs, "service_name", info.ServiceName)
	}
	b.logger.Warn("container death detected via events API", logAttrs...)
}

// findLeaseByContainerID returns the lease UUID, provision, and true if a
// provision containing the given container ID is found. Returns ("", nil, false)
// otherwise. Called under no lock; acquires read lock internally.
func (b *Backend) findLeaseByContainerID(containerID string) (string, *provision, bool) {
	b.provisionsMu.RLock()
	defer b.provisionsMu.RUnlock()

	for uuid, prov := range b.provisions {
		for _, cid := range prov.ContainerIDs {
			if cid == containerID {
				return uuid, prov, true
			}
		}
	}
	return "", nil, false
}

// GetLogs returns the last N lines of stdout/stderr for each container in
// a lease, keyed by instance index (e.g., "0", "1").
// Falls back to the diagnostics store when the provision is not in memory
// (e.g., after deprovision). Returns ErrNotProvisioned only if both miss.
// On partial failure (some containers succeed, some fail), the successful logs
// are returned along with error placeholders, and the errors are logged.
func (b *Backend) GetLogs(ctx context.Context, leaseUUID string, tail int) (map[string]string, error) {
	b.provisionsMu.RLock()
	prov, exists := b.provisions[leaseUUID]
	b.provisionsMu.RUnlock()

	if exists {
		// Stack logs: key by "serviceName/instanceIndex" (e.g., "web/0", "db/0").
		if prov.IsStack() {
			result := make(map[string]string, len(prov.ContainerIDs))
			for svcName, containerIDs := range prov.ServiceContainers {
				for i, containerID := range containerIDs {
					key := fmt.Sprintf("%s/%d", svcName, i)
					logs, err := b.docker.ContainerLogs(ctx, containerID, tail)
					if err != nil {
						b.logger.Warn("failed to retrieve container logs",
							"lease_uuid", leaseUUID,
							"service", svcName,
							"instance", i,
							"container_id", shortID(containerID),
							"error", err,
						)
						result[key] = fmt.Sprintf("<error: %s>", err)
						continue
					}
					result[key] = logs
				}
			}
			return result, nil
		}

		// Legacy logs: key by instance index ("0", "1", ...).
		result := make(map[string]string, len(prov.ContainerIDs))
		for i, containerID := range prov.ContainerIDs {
			logs, err := b.docker.ContainerLogs(ctx, containerID, tail)
			if err != nil {
				b.logger.Warn("failed to retrieve container logs",
					"lease_uuid", leaseUUID,
					"instance", i,
					"container_id", shortID(containerID),
					"error", err,
				)
				result[fmt.Sprintf("%d", i)] = fmt.Sprintf("<error: %s>", err)
				continue
			}
			result[fmt.Sprintf("%d", i)] = logs
		}
		return result, nil
	}

	// Fall back to persisted diagnostics.
	if b.diagnosticsStore != nil {
		entry, err := b.diagnosticsStore.Get(leaseUUID)
		if err != nil {
			b.logger.Warn("diagnostics store lookup failed", "lease_uuid", leaseUUID, "error", err)
		}
		if entry != nil && len(entry.Logs) > 0 {
			return entry.Logs, nil
		}
	}

	return nil, backend.ErrNotProvisioned
}

// Stats returns current resource usage statistics.
func (b *Backend) Stats() shared.ResourceStats {
	return b.pool.Stats()
}
