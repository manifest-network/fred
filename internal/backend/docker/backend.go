package docker

import (
	"bytes"
	"cmp"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	networktypes "github.com/docker/docker/api/types/network"
	"github.com/docker/docker/pkg/stringid"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/hmacauth"
)

// dockerClient abstracts the Docker API surface used by Backend,
// enabling unit tests to substitute a lightweight mock.
type dockerClient interface {
	Ping(ctx context.Context) error
	Close() error
	PullImage(ctx context.Context, imageName string, timeout time.Duration) error
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

const (
	// callbackMaxAttempts is the number of times to attempt callback delivery.
	callbackMaxAttempts = 3

	// callbackTimeout is the per-attempt timeout for callback HTTP requests.
	callbackTimeout = 10 * time.Second
)

// callbackBackoff defines the delay before each retry attempt (index 0 = first attempt, no delay).
var callbackBackoff = [callbackMaxAttempts]time.Duration{0, 1 * time.Second, 5 * time.Second}

// Backend implements the backend.Backend interface for Docker containers.
type Backend struct {
	cfg    Config
	docker dockerClient
	pool   *ResourcePool
	logger *slog.Logger

	// provisions tracks active provisions by lease UUID
	provisions   map[string]*provision
	provisionsMu sync.RWMutex

	// recoverMu serializes recoverState calls. The reconcile loop and
	// external RefreshState (called by Fred's reconciler) both invoke
	// recoverState. Without serialization, concurrent calls can detect
	// the same ready→failed transitions and send duplicate callbacks.
	recoverMu sync.Mutex

	// callbackStore persists pending callbacks in bbolt
	callbackStore *CallbackStore

	// httpClient for sending callbacks
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

// New creates a new Docker backend.
func New(cfg Config, logger *slog.Logger) (*Backend, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	docker, err := NewDockerClient(cfg.DockerHost)
	if err != nil {
		return nil, fmt.Errorf("failed to create Docker client: %w", err)
	}

	pool := NewResourcePool(
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

	cbStore, err := NewCallbackStore(CallbackStoreConfig{
		DBPath: cfg.CallbackDBPath,
		MaxAge: cfg.CallbackMaxAge,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open callback store: %w", err)
	}

	b := &Backend{
		cfg:           cfg,
		docker:        docker,
		pool:          pool,
		logger:        logger.With("backend", cfg.Name),
		provisions:    make(map[string]*provision),
		callbackStore: cbStore,
		httpClient:    httpClient,
	}

	b.stopCtx, b.stopCancel = context.WithCancel(context.Background())

	return b, nil
}

// Start initializes the backend, recovers state, and starts background tasks.
func (b *Backend) Start(ctx context.Context) error {
	// Verify Docker connectivity
	if err := b.docker.Ping(ctx); err != nil {
		return fmt.Errorf("failed to connect to Docker: %w", err)
	}

	// Recover state from existing containers
	if err := b.recoverState(ctx); err != nil {
		return fmt.Errorf("failed to recover state: %w", err)
	}

	// Replay any pending callbacks from a previous run
	b.replayPendingCallbacks()

	// Start periodic reconciliation (using WaitGroup.Go for Go 1.25+)
	b.wg.Go(b.reconcileLoop)

	b.logger.Info("Docker backend started",
		"host", b.cfg.DockerHost,
		"recovered_containers", len(b.provisions),
	)

	return nil
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
	if err := b.docker.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing docker client: %w", err))
	}
	return errors.Join(errs...)
}

// replayPendingCallbacks replays any callbacks that were persisted but not
// successfully delivered before the previous shutdown.
// Expired entries are already removed by the CallbackStore's initial cleanup
// (run during NewCallbackStore), so only deliverable entries remain.
func (b *Backend) replayPendingCallbacks() {
	if b.callbackStore == nil {
		return
	}

	entries, err := b.callbackStore.ListPending()
	if err != nil {
		b.logger.Error("failed to list pending callbacks", "error", err)
		return
	}
	if len(entries) == 0 {
		return
	}

	b.logger.Info("replaying pending callbacks", "count", len(entries))
	for _, entry := range entries {
		status := backend.CallbackStatusSuccess
		if !entry.Success {
			status = backend.CallbackStatusFailed
		}
		payload := backend.CallbackPayload{
			LeaseUUID: entry.LeaseUUID,
			Status:    status,
			Error:     entry.Error,
		}
		body, marshalErr := json.Marshal(payload)
		if marshalErr != nil {
			b.logger.Error("removing malformed callback entry", "error", marshalErr, "lease_uuid", entry.LeaseUUID)
			_ = b.callbackStore.Remove(entry.LeaseUUID)
			continue
		}

		if b.deliverCallback(entry.LeaseUUID, entry.CallbackURL, body) {
			if rmErr := b.callbackStore.Remove(entry.LeaseUUID); rmErr != nil {
				b.logger.Error("failed to remove replayed callback", "error", rmErr, "lease_uuid", entry.LeaseUUID)
			}
		}
	}
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
		return fmt.Errorf("%w: invalid manifest: %w", backend.ErrValidation, err)
	}

	// Validate image against registry allowlist
	if err := ValidateImage(manifest.Image, b.cfg.AllowedRegistries); err != nil {
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

// doProvision performs the actual container creation asynchronously.
// For multi-unit leases, it creates multiple containers.
// For multi-SKU leases, each container gets the appropriate resource profile.
func (b *Backend) doProvision(ctx context.Context, req backend.ProvisionRequest, manifest *DockerManifest, profiles map[string]SKUProfile, logger *slog.Logger) {
	totalQuantity := req.TotalQuantity()
	var containerIDs []string
	var err error
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

			b.sendCallback(req.LeaseUUID, false, err.Error())
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

		updateResourceMetrics(b.pool.Stats())
		b.sendCallback(req.LeaseUUID, true, "")
	}()

	// Check for early cancellation (e.g., shutdown requested before we started)
	if ctx.Err() != nil {
		logger.Warn("provisioning canceled before start", "error", ctx.Err())
		err = fmt.Errorf("provisioning canceled: %w", ctx.Err())
		return
	}

	// Pull image (only once, shared by all containers)
	logger.Info("pulling image", "image", manifest.Image)
	pullStart := time.Now()
	if err = b.docker.PullImage(ctx, manifest.Image, b.cfg.ImagePullTimeout); err != nil {
		logger.Error("failed to pull image", "error", err)
		err = fmt.Errorf("image pull failed: %w", err)
		return
	}
	imagePullDurationSeconds.Observe(time.Since(pullStart).Seconds())

	// Set up per-tenant network isolation if enabled
	var networkConfig *networktypes.NetworkingConfig
	if b.cfg.IsNetworkIsolation() {
		networkID, netErr := b.docker.EnsureTenantNetwork(ctx, req.Tenant)
		if netErr != nil {
			logger.Error("failed to create tenant network", "error", netErr)
			err = fmt.Errorf("tenant network setup failed: %w", netErr)
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
				HostBindIP:     b.cfg.GetHostBindIP(),
				ReadonlyRootfs: b.cfg.IsReadonlyRootfs(),
				PidsLimit:      b.cfg.GetPidsLimit(),
				TmpfsSizeMB:    b.cfg.GetTmpfsSizeMB(),
				DiskQuota:      b.cfg.IsDiskQuota(),
				NetworkConfig:  networkConfig,
			}, b.cfg.ContainerCreateTimeout)
			containerCreateDurationSeconds.Observe(time.Since(createStart).Seconds())
			if createErr != nil {
				instanceLogger.Error("failed to create container", "error", createErr)
				err = fmt.Errorf("container creation failed (instance %d, sku %s): %w", instanceIndex, item.SKU, createErr)
				return
			}
			containerIDs = append(containerIDs, containerID)

			// Start container
			instanceLogger.Info("starting container", "container_id", shortID(containerID))
			if startErr := b.docker.StartContainer(ctx, containerID, b.cfg.ContainerStartTimeout); startErr != nil {
				instanceLogger.Error("failed to start container", "error", startErr)
				err = fmt.Errorf("container start failed (instance %d, sku %s): %w", instanceIndex, item.SKU, startErr)
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
			return
		case <-time.After(startupVerify):
		}

		for i, containerID := range containerIDs {
			info, inspectErr := b.docker.InspectContainer(ctx, containerID)
			if inspectErr != nil {
				err = fmt.Errorf("failed to verify container %d after startup: %w", i, inspectErr)
				return
			}
			status := containerStatusToProvisionStatus(info.Status)
			if status != backend.ProvisionStatusReady {
				err = fmt.Errorf("container %d exited during startup (status: %s)", i, info.Status)
				return
			}
		}
	}

	logger.Info("all containers provisioned and verified", "count", len(containerIDs))
}

// healthPollInterval is the interval between health check polls during startup verification.
const healthPollInterval = 2 * time.Second

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
					return fmt.Errorf("container %d exited while waiting for healthy (status: %s)", i, info.Status)
				}

				switch info.Health {
				case HealthStatusHealthy:
					logger.Info("container healthy", "instance", i, "container_id", shortID(containerIDs[i]))
					delete(pending, i)
				case HealthStatusUnhealthy:
					return fmt.Errorf("container %d reported unhealthy", i)
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
		}
		b.provisionsMu.Unlock()
		return fmt.Errorf("deprovision partially failed: %w", errors.Join(errs...))
	}

	// All containers removed successfully — delete provision from map.
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

	var allocations []ResourceAllocation
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
		allocations = append(allocations, ResourceAllocation{
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
				rec.LastError = "container exited unexpectedly"
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
	for uuid, rec := range recovered {
		if rec.Status == backend.ProvisionStatusFailed {
			if _, hasExisting := b.provisions[uuid]; !hasExisting {
				rec.FailCount++
				rec.LastError = "container exited unexpectedly"
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
		if _, hasContainers := recovered[uuid]; hasContainers {
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

	// Send failure callbacks outside the lock to avoid holding it during I/O.
	for _, uuid := range failedLeases {
		b.sendCallback(uuid, false, "container exited unexpectedly")
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
		"memory_allocated_mb", stats.AllocatedMemory,
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

// sendCallback sends a provision result callback to Fred with HMAC signature.
// It retries up to callbackMaxAttempts times with exponential backoff.
// Retries are aborted if the backend is shutting down.
func (b *Backend) sendCallback(leaseUUID string, success bool, errMsg string) {
	b.provisionsMu.RLock()
	prov, ok := b.provisions[leaseUUID]
	var callbackURL string
	if ok {
		callbackURL = prov.CallbackURL
	}
	b.provisionsMu.RUnlock()

	if callbackURL == "" {
		b.logger.Warn("no callback URL for lease", "lease_uuid", leaseUUID)
		return
	}

	status := backend.CallbackStatusSuccess
	if !success {
		status = backend.CallbackStatusFailed
	}

	payload := backend.CallbackPayload{
		LeaseUUID: leaseUUID,
		Status:    status,
		Error:     errMsg,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		b.logger.Error("failed to marshal callback payload", "error", err)
		return
	}

	// Persist callback before attempting delivery so it survives restarts
	if b.callbackStore != nil {
		if storeErr := b.callbackStore.Store(CallbackEntry{
			LeaseUUID:   leaseUUID,
			CallbackURL: callbackURL,
			Success:     success,
			Error:       errMsg,
			CreatedAt:   time.Now(),
		}); storeErr != nil {
			b.logger.Error("failed to persist callback", "error", storeErr, "lease_uuid", leaseUUID)
		}
	}

	delivered := b.deliverCallback(leaseUUID, callbackURL, body)

	// Remove from persistent store on successful delivery
	if delivered && b.callbackStore != nil {
		if rmErr := b.callbackStore.Remove(leaseUUID); rmErr != nil {
			b.logger.Error("failed to remove delivered callback from store", "error", rmErr, "lease_uuid", leaseUUID)
		}
	}
}

// deliverCallback attempts to deliver a callback with retries.
// Returns true if delivery succeeded.
func (b *Backend) deliverCallback(leaseUUID, callbackURL string, body []byte) bool {
	for attempt := range callbackMaxAttempts {
		if attempt > 0 {
			// Wait with backoff, but abort if shutting down
			select {
			case <-b.stopCtx.Done():
				b.logger.Warn("callback retry aborted by shutdown",
					"lease_uuid", leaseUUID,
					"attempt", attempt+1,
				)
				callbackDeliveryTotal.WithLabelValues("failure").Inc()
				return false
			case <-time.After(callbackBackoff[attempt]):
			}
		}

		if b.trySendCallback(leaseUUID, callbackURL, body) {
			callbackDeliveryTotal.WithLabelValues("success").Inc()
			return true
		}
	}

	callbackDeliveryTotal.WithLabelValues("failure").Inc()
	b.logger.Error("callback delivery failed after retries",
		"lease_uuid", leaseUUID,
		"attempts", callbackMaxAttempts,
	)
	return false
}

// trySendCallback makes a single callback attempt. Returns true on success.
func (b *Backend) trySendCallback(leaseUUID, callbackURL string, body []byte) bool {
	ctx, cancel := context.WithTimeout(b.stopCtx, callbackTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, callbackURL, bytes.NewReader(body))
	if err != nil {
		b.logger.Error("failed to create callback request", "error", err, "lease_uuid", leaseUUID)
		return false
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign(b.cfg.CallbackSecret, body))

	resp, err := b.httpClient.Do(req)
	if err != nil {
		b.logger.Warn("callback attempt failed",
			"error", err,
			"lease_uuid", leaseUUID,
		)
		return false
	}

	// Always read and close the response body to allow connection reuse.
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		b.logger.Debug("callback sent", "lease_uuid", leaseUUID)
		return true
	}

	b.logger.Warn("callback returned error status",
		"status", resp.StatusCode,
		"lease_uuid", leaseUUID,
		"body", string(respBody),
	)
	return false
}

// GetLogs returns the last N lines of stdout/stderr for each container in
// a lease, keyed by instance index (e.g., "0", "1").
func (b *Backend) GetLogs(ctx context.Context, leaseUUID string, tail int) (map[string]string, error) {
	b.provisionsMu.RLock()
	prov, exists := b.provisions[leaseUUID]
	b.provisionsMu.RUnlock()

	if !exists {
		return nil, backend.ErrNotProvisioned
	}

	result := make(map[string]string, len(prov.ContainerIDs))
	for i, containerID := range prov.ContainerIDs {
		logs, err := b.docker.ContainerLogs(ctx, containerID, tail)
		if err != nil {
			result[fmt.Sprintf("%d", i)] = fmt.Sprintf("<error: %s>", err)
			continue
		}
		result[fmt.Sprintf("%d", i)] = logs
	}
	return result, nil
}

// Stats returns current resource usage statistics.
func (b *Backend) Stats() ResourceStats {
	return b.pool.Stats()
}
