package docker

import (
	"cmp"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sort"
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
	RemoveContainer(ctx context.Context, containerID string) error
	InspectContainer(ctx context.Context, containerID string) (*ContainerInfo, error)
	ContainerLogs(ctx context.Context, containerID string, tail int) (string, error)
	ListManagedContainers(ctx context.Context) ([]ContainerInfo, error)
	EnsureTenantNetwork(ctx context.Context, tenant string) (string, error)
	RemoveTenantNetworkIfEmpty(ctx context.Context, tenant string) error
	ListManagedNetworks(ctx context.Context) ([]networktypes.Inspect, error)
}

// Backend implements the backend.Backend interface for Docker containers.
type Backend struct {
	cfg     Config
	docker  dockerClient
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

	// callbackSender handles callback delivery with retry and HMAC
	callbackSender *shared.CallbackSender

	// httpClient for sending callbacks (kept for test injection)
	httpClient *http.Client

	// stopCtx is canceled on shutdown; stopCancel triggers it.
	stopCtx    context.Context
	stopCancel context.CancelFunc
	wg         sync.WaitGroup
}

// provision tracks provisioned containers for a lease.
// A single lease may have multiple containers based on quantity.
type provision struct {
	LeaseUUID    string
	Tenant       string
	ProviderUUID string
	SKU          string
	ContainerIDs []string // Multiple containers for multi-unit leases
	Image        string
	Status       backend.ProvisionStatus
	Quantity     int // Expected number of containers
	CreatedAt    time.Time
	FailCount    int    // Number of times provisioning has failed for this lease
	LastError    string // Last error message, queryable after failure
	CallbackURL  string // URL to notify on provision completion
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

// persistDiagnostics saves failure diagnostics and container logs to the
// diagnostics store. It is a best-effort operation: errors are logged but
// not propagated. Skipped if diagnosticsStore is nil (e.g., in tests).
func (b *Backend) persistDiagnostics(leaseUUID string, prov *provision, containerIDs []string) {
	if b.diagnosticsStore == nil {
		return
	}

	entry := shared.DiagnosticEntry{
		LeaseUUID:    prov.LeaseUUID,
		ProviderUUID: prov.ProviderUUID,
		Tenant:       prov.Tenant,
		Error:        prov.LastError,
		FailCount:    prov.FailCount,
		CreatedAt:    time.Now(),
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
			logs[fmt.Sprintf("%d", i)] = logOutput
		}
		if len(logs) > 0 {
			entry.Logs = logs
		}
	}

	if err := b.diagnosticsStore.Store(entry); err != nil {
		b.logger.Warn("failed to persist failure diagnostics",
			"lease_uuid", leaseUUID, "error", err)
	}
}

// New creates a new Docker backend.
func New(cfg Config, logger *slog.Logger) (*Backend, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	docker, err := NewDockerClient(cfg.DockerHost)
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

	volumes, err := newVolumeManager(cfg.VolumeDataPath, cfg.VolumeFilesystem, logger)
	if err != nil {
		_ = cbStore.Close()
		_ = diagStore.Close()
		return nil, fmt.Errorf("failed to create volume manager: %w", err)
	}

	b := &Backend{
		cfg:              cfg,
		docker:           docker,
		pool:             pool,
		volumes:          volumes,
		logger:           logger.With("backend", cfg.Name),
		provisions:       make(map[string]*provision),
		callbackStore:    cbStore,
		diagnosticsStore: diagStore,
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
		for i := 0; i < oldQuantity; i++ {
			b.pool.Release(fmt.Sprintf("%s-%d", req.LeaseUUID, i))
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

	// Parse manifest
	manifest, err := ParseManifest(req.Payload)
	if err != nil {
		b.removeProvision(req.LeaseUUID)
		return fmt.Errorf("%w: %w", backend.ErrInvalidManifest, err)
	}

	// Validate image against registry allowlist
	if err := shared.ValidateImage(manifest.Image, b.cfg.AllowedRegistries); err != nil {
		b.removeProvision(req.LeaseUUID)
		return fmt.Errorf("%w: %w", backend.ErrValidation, err)
	}

	// Try to allocate resources for all instances
	// Track allocations so we can roll back on failure
	var allocatedIDs []string
	instanceIdx := 0
	for _, item := range req.Items {
		for i := 0; i < item.Quantity; i++ {
			instanceID := fmt.Sprintf("%s-%d", req.LeaseUUID, instanceIdx)
			if err := b.pool.TryAllocate(instanceID, item.SKU, req.Tenant); err != nil {
				// Release any previously allocated resources
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

	// Update the reservation with full details now that validation passed
	b.provisionsMu.Lock()
	if prov, ok := b.provisions[req.LeaseUUID]; ok {
		prov.SKU = req.RoutingSKU()
		prov.Image = manifest.Image
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

		b.doProvision(ctx, req, manifest, profiles, logger)
	})

	return nil
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
			provisionsTotal.WithLabelValues("failure").Inc()
			// Clean up on failure - release all allocated resources
			for i := 0; i < totalQuantity; i++ {
				b.pool.Release(fmt.Sprintf("%s-%d", req.LeaseUUID, i))
			}

			b.provisionsMu.Lock()
			if prov, ok := b.provisions[req.LeaseUUID]; ok {
				prov.Status = backend.ProvisionStatusFailed
				prov.FailCount++
				prov.LastError = err.Error()
				// Persist diagnostics while containers still exist for log access.
				b.persistDiagnostics(req.LeaseUUID, prov, containerIDs)
			}
			b.provisionsMu.Unlock()

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
		}
		b.provisionsMu.Unlock()

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

	// Inspect image to discover VOLUME declarations
	imageInfo, inspectErr := b.docker.InspectImage(ctx, manifest.Image)
	if inspectErr != nil {
		logger.Error("failed to inspect image", "error", inspectErr)
		err = fmt.Errorf("image inspect failed: %w", inspectErr)
		callbackErr = "image inspect failed"
		return
	}
	var imageVolumes []string
	for v := range imageInfo.Volumes {
		imageVolumes = append(imageVolumes, v)
	}
	sort.Strings(imageVolumes)

	// Resolve container user to numeric UID/GID.
	// When the manifest specifies a user (e.g., "postgres" or "999:999"),
	// the container runs as that user directly and volumes are pre-chowned.
	// This is required for images whose entrypoint starts as root and tries
	// to chown data directories — CapDrop ALL removes CAP_CHOWN.
	var volumeUID, volumeGID int
	var containerUser string
	if manifest.User != "" || imageInfo.User != "" {
		volumeUID, volumeGID, err = b.docker.ResolveImageUser(ctx, manifest.Image, manifest.User)
		if err != nil {
			logger.Error("failed to resolve image user", "error", err)
			err = fmt.Errorf("image user resolution failed: %w", err)
			callbackErr = "image user resolution failed"
			return
		}
		if volumeUID != 0 || volumeGID != 0 {
			containerUser = fmt.Sprintf("%d:%d", volumeUID, volumeGID)
			logger.Info("resolved container user", "uid", volumeUID, "gid", volumeGID)
		}
	}

	// Set up per-tenant network isolation if enabled
	var networkConfig *networktypes.NetworkingConfig
	if b.cfg.IsNetworkIsolation() {
		networkID, netErr := b.docker.EnsureTenantNetwork(ctx, req.Tenant)
		if netErr != nil {
			logger.Error("failed to create tenant network", "error", netErr)
			err = fmt.Errorf("tenant network setup failed: %w", netErr)
			callbackErr = "tenant network setup failed"
			return
		}
		networkConfig = buildNetworkConfig(networkID)
		logger.Info("tenant network ready", "network_id", shortID(networkID))
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

			// Create managed volume for stateful SKUs (disk_mb > 0).
			var volumeBinds map[string]string
			if profile.DiskMB > 0 && len(imageVolumes) > 0 {
				volumeID := fmt.Sprintf("fred-%s-%d", req.LeaseUUID, instanceIndex)
				hostPath, volCreated, volErr := b.volumes.Create(ctx, volumeID, profile.DiskMB)
				if volErr != nil {
					instanceLogger.Error("failed to create volume", "error", volErr)
					err = fmt.Errorf("volume creation failed (instance %d, sku %s): %w", instanceIndex, item.SKU, volErr)
					callbackErr = "volume creation failed"
					return
				}
				// Only track newly created volumes for cleanup on failure.
				// Reused volumes (from a previous provision) must not be destroyed.
				if volCreated {
					createdVolumeIDs = append(createdVolumeIDs, volumeID)
				}
				volumeBinds = make(map[string]string, len(imageVolumes))
				for _, volPath := range imageVolumes {
					sanitized := sanitizeVolumePath(volPath)
					if sanitized == "" {
						err = fmt.Errorf("image declares unsupported VOLUME path %q that cannot be quota-enforced (instance %d, sku %s)", volPath, instanceIndex, item.SKU)
						callbackErr = "image has unsupported VOLUME path"
						return
					}
					subdir := filepath.Join(hostPath, sanitized)
					if mkErr := os.MkdirAll(subdir, 0o700); mkErr != nil {
						instanceLogger.Error("failed to create volume subdir", "path", subdir, "error", mkErr)
						err = fmt.Errorf("volume subdir creation failed (instance %d, sku %s): %w", instanceIndex, item.SKU, mkErr)
						callbackErr = "volume creation failed"
						return
					}
					// Pre-chown for non-root container users (CapDrop ALL prevents in-container chown).
					if volumeUID != 0 || volumeGID != 0 {
						if chownErr := os.Chown(subdir, volumeUID, volumeGID); chownErr != nil {
							instanceLogger.Error("failed to chown volume subdir", "path", subdir, "uid", volumeUID, "gid", volumeGID, "error", chownErr)
							err = fmt.Errorf("volume subdir chown failed (instance %d, sku %s): %w", instanceIndex, item.SKU, chownErr)
							callbackErr = "volume creation failed"
							return
						}
					}
					volumeBinds[subdir] = volPath
				}
			} else if profile.DiskMB > 0 && len(imageVolumes) == 0 {
				instanceLogger.Warn("stateful SKU has disk_mb > 0 but image declares no VOLUME paths; disk budget is allocated but unused",
					"disk_mb", profile.DiskMB)
			}

			createStart := time.Now()
			containerID, createErr := b.docker.CreateContainer(ctx, CreateContainerParams{
				LeaseUUID:      req.LeaseUUID,
				Tenant:         req.Tenant,
				ProviderUUID:   req.ProviderUUID,
				SKU:            item.SKU,
				Manifest:       manifest,
				Profile:        profile,
				InstanceIndex:  instanceIndex,
				FailCount:      failCount,
				CallbackURL:    req.CallbackURL,
				HostBindIP:     b.cfg.GetHostBindIP(),
				ReadonlyRootfs: b.cfg.IsReadonlyRootfs(),
				PidsLimit:      b.cfg.GetPidsLimit(),
				TmpfsSizeMB:    b.cfg.GetTmpfsSizeMB(),
				NetworkConfig:  networkConfig,
				VolumeBinds:    volumeBinds,
				ImageVolumes:   imageVolumes,
				User:           containerUser,
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

	// Startup verification: two paths based on whether the manifest declares
	// an active health check.
	if manifest.HasActiveHealthCheck() {
		// Health-aware path: poll until all containers report "healthy".
		// Bounded by the existing ProvisionTimeout context.
		logger.Info("waiting for health checks to pass")
		if err = b.waitForHealthy(ctx, containerIDs, logger); err != nil {
			callbackErr = healthErrorToCallbackMsg(err)
			return
		}
	} else {
		// Fixed-wait path: wait StartupVerifyDuration then check containers
		// are still running. Catches immediate crashes before sending a
		// success callback.
		startupVerify := cmp.Or(b.cfg.StartupVerifyDuration, 5*time.Second)
		logger.Info("waiting for startup verification", "duration", startupVerify)

		select {
		case <-ctx.Done():
			err = fmt.Errorf("provisioning canceled during startup verification: %w", ctx.Err())
			callbackErr = "provisioning canceled"
			return
		case <-time.After(startupVerify):
		}

		for i, containerID := range containerIDs {
			info, inspectErr := b.docker.InspectContainer(ctx, containerID)
			if inspectErr != nil {
				err = fmt.Errorf("failed to verify container %d after startup: %w", i, inspectErr)
				callbackErr = "container exited during startup"
				return
			}
			status := containerStatusToProvisionStatus(info.Status)
			if status != backend.ProvisionStatusReady {
				diag := b.containerFailureDiagnostics(ctx, containerID, info)
				err = fmt.Errorf("container %d exited during startup (status: %s): %s", i, info.Status, diag)
				callbackErr = "container exited during startup"
				return
			}
		}
	}

	logger.Info("all containers provisioned and verified", "count", len(containerIDs))
}

// healthPollInterval is the interval between health check polls during startup verification.
const healthPollInterval = 2 * time.Second

// healthErrorToCallbackMsg maps a waitForHealthy error to a hardcoded callback
// message safe for on-chain surfacing.
func healthErrorToCallbackMsg(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "unhealthy"):
		return "container reported unhealthy"
	case strings.Contains(msg, "exited"):
		return "container exited during health check"
	default:
		return "container exited during health check"
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

// GetInfo returns lease information including connection details.
// For multi-unit leases, returns an "instances" array with each container's info.
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

	// Get container details from Docker for all instances
	var instances []map[string]any
	for _, containerID := range prov.ContainerIDs {
		info, err := b.docker.InspectContainer(ctx, containerID)
		if err != nil {
			return nil, fmt.Errorf("failed to inspect container: %w", err)
		}

		// Build port mapping for this instance
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

	b.provisionsMu.RLock()
	prov, exists := b.provisions[leaseUUID]
	b.provisionsMu.RUnlock()

	if !exists {
		// Already deprovisioned - idempotent success
		return nil
	}

	// Remove all containers, collecting successes and failures.
	// RemoveContainer is idempotent (already-removed containers return nil),
	// so non-nil errors indicate actual failures (I/O, permissions, etc.).
	var errs []error
	var failedIDs []string
	for _, containerID := range prov.ContainerIDs {
		if err := b.docker.RemoveContainer(ctx, containerID); err != nil {
			logger.Error("failed to remove container", "container_id", shortID(containerID), "error", err)
			errs = append(errs, fmt.Errorf("container %s: %w", shortID(containerID), err))
			failedIDs = append(failedIDs, containerID)
		} else {
			logger.Info("container removed", "container_id", shortID(containerID))
		}
	}

	// Release resource pool allocations regardless of outcome — the lease
	// is being abandoned and these resources should be freed.
	for i := 0; i < prov.Quantity; i++ {
		b.pool.Release(fmt.Sprintf("%s-%d", leaseUUID, i))
	}

	if len(errs) > 0 {
		// Partial failure: keep provision visible with only the stuck containers
		// so the reconciler (or a retry) can see and re-attempt them.
		b.provisionsMu.Lock()
		if p, ok := b.provisions[leaseUUID]; ok {
			p.Status = backend.ProvisionStatusFailed
			p.ContainerIDs = failedIDs
			p.LastError = fmt.Sprintf("deprovision partially failed: %s", errors.Join(errs...))
			b.persistDiagnostics(leaseUUID, p, failedIDs)
		}
		b.provisionsMu.Unlock()
		return fmt.Errorf("deprovision partially failed: %w", errors.Join(errs...))
	}

	// Destroy managed volumes for all instances.
	var volumeErrs []error
	for i := 0; i < prov.Quantity; i++ {
		volumeID := fmt.Sprintf("fred-%s-%d", leaseUUID, i)
		if volErr := b.volumes.Destroy(ctx, volumeID); volErr != nil {
			logger.Error("failed to destroy volume", "volume_id", volumeID, "error", volErr)
			volumeErrs = append(volumeErrs, fmt.Errorf("volume %s: %w", volumeID, volErr))
		}
	}

	if len(volumeErrs) > 0 {
		// Volume cleanup failed — keep provision visible for retry.
		b.provisionsMu.Lock()
		if p, ok := b.provisions[leaseUUID]; ok {
			p.Status = backend.ProvisionStatusFailed
			p.ContainerIDs = nil // containers are gone
			p.LastError = fmt.Sprintf("volume cleanup failed: %s", errors.Join(volumeErrs...))
		}
		b.provisionsMu.Unlock()
		return fmt.Errorf("volume cleanup failed: %w", errors.Join(volumeErrs...))
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
func (b *Backend) ListProvisions(ctx context.Context) ([]backend.ProvisionInfo, error) {
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

	var allocations []shared.ResourceAllocation
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
			recovered[c.LeaseUUID] = prov
		}

		// Add container ID to the provision
		prov.ContainerIDs = append(prov.ContainerIDs, c.ContainerID)
		prov.Quantity = len(prov.ContainerIDs)

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

		// Use instance-specific allocation ID
		instanceID := fmt.Sprintf("%s-%d", c.LeaseUUID, c.InstanceIndex)
		allocations = append(allocations, shared.ResourceAllocation{
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
			if existing.Status == backend.ProvisionStatusProvisioning {
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
		case backend.ProvisionStatusProvisioning:
			// In-flight provision that hasn't produced containers yet — preserve it.
			recovered[uuid] = existing
		case backend.ProvisionStatusFailed:
			// Failed provision whose containers have been cleaned up (e.g., after
			// a failed re-provision attempt). Preserve so fred's reconciler can
			// see the failure and its FailCount for retry/close decisions.
			recovered[uuid] = existing
		}
	}
	b.provisions = recovered
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

	// Persist diagnostics for all failed leases (containers are dead but not removed).
	b.provisionsMu.RLock()
	for _, uuid := range allFailed {
		if prov, ok := b.provisions[uuid]; ok {
			b.persistDiagnostics(uuid, prov, prov.ContainerIDs)
		}
	}
	b.provisionsMu.RUnlock()

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
		for i := 0; i < prov.Quantity; i++ {
			expected[fmt.Sprintf("fred-%s-%d", leaseUUID, i)] = true
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
