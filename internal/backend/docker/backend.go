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

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/hmacauth"
)

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
	docker *DockerClient
	pool   *ResourcePool
	logger *slog.Logger

	// provisions tracks active provisions by lease UUID
	provisions   map[string]*provision
	provisionsMu sync.RWMutex

	// callbackURLs stores per-lease callback URLs
	callbackURLs   map[string]string
	callbackURLsMu sync.Mutex

	// httpClient for sending callbacks
	httpClient *http.Client

	// stopCh signals shutdown
	stopCh chan struct{}
	wg     sync.WaitGroup
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
	Status       string
	Quantity     int // Expected number of containers
	CreatedAt    time.Time
}

// shortID safely truncates an ID to 12 characters for logging.
func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
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

	b := &Backend{
		cfg:          cfg,
		docker:       docker,
		pool:         pool,
		logger:       logger.With("backend", cfg.Name),
		provisions:   make(map[string]*provision),
		callbackURLs: make(map[string]string),
		httpClient:   httpClient,
		stopCh:       make(chan struct{}),
	}

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
	close(b.stopCh)
	b.wg.Wait()
	return b.docker.Close()
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

	// Store callback URL
	b.callbackURLsMu.Lock()
	b.callbackURLs[req.LeaseUUID] = req.CallbackURL
	b.callbackURLsMu.Unlock()

	// Atomically check-and-reserve the provision slot (fixes TOCTOU race).
	b.provisionsMu.Lock()
	if _, exists := b.provisions[req.LeaseUUID]; exists {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: %s", backend.ErrAlreadyProvisioned, req.LeaseUUID)
	}
	b.provisions[req.LeaseUUID] = &provision{
		LeaseUUID:    req.LeaseUUID,
		Tenant:       req.Tenant,
		ProviderUUID: req.ProviderUUID,
		Status:       backend.ProvisionStatusProvisioning,
		Quantity:     totalQuantity,
		ContainerIDs: make([]string, 0, totalQuantity),
		CreatedAt:    time.Now(),
	}
	b.provisionsMu.Unlock()

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
			if err := b.pool.TryAllocate(instanceID, item.SKU); err != nil {
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
	// 2. Backend shutdown (stopCh closed)
	b.wg.Go(func() {
		// Create context with provision timeout (use cmp.Or for consistency with codebase)
		provisionTimeout := cmp.Or(b.cfg.ProvisionTimeout, 10*time.Minute)
		ctx, cancel := context.WithTimeout(context.Background(), provisionTimeout)
		defer cancel()

		// Cancel on shutdown — tracked by WaitGroup for clean shutdown.
		b.wg.Go(func() {
			select {
			case <-b.stopCh:
				cancel()
			case <-ctx.Done():
			}
		})

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

	// Single cleanup path via defer - handles both early cancellation and runtime errors
	defer func() {
		if err != nil {
			// Clean up on failure - release all allocated resources
			for i := 0; i < totalQuantity; i++ {
				b.pool.Release(fmt.Sprintf("%s-%d", req.LeaseUUID, i))
			}

			b.provisionsMu.Lock()
			if prov, ok := b.provisions[req.LeaseUUID]; ok {
				prov.Status = backend.ProvisionStatusFailed
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
		b.provisionsMu.Lock()
		if prov, ok := b.provisions[req.LeaseUUID]; ok {
			prov.ContainerIDs = containerIDs
			prov.Status = backend.ProvisionStatusReady
		}
		b.provisionsMu.Unlock()

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
	if err = b.docker.PullImage(ctx, manifest.Image, b.cfg.ImagePullTimeout); err != nil {
		logger.Error("failed to pull image", "error", err)
		err = fmt.Errorf("image pull failed: %w", err)
		return
	}

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
			containerID, createErr := b.docker.CreateContainer(ctx, CreateContainerParams{
				LeaseUUID:      req.LeaseUUID,
				Tenant:         req.Tenant,
				ProviderUUID:   req.ProviderUUID,
				SKU:            item.SKU,
				Manifest:       manifest,
				Profile:        profile,
				InstanceIndex:  instanceIndex,
				HostBindIP:     b.cfg.GetHostBindIP(),
				ReadonlyRootfs: b.cfg.IsReadonlyRootfs(),
				PidsLimit:      b.cfg.GetPidsLimit(),
				TmpfsSizeMB:    b.cfg.GetTmpfsSizeMB(),
				NetworkConfig:  networkConfig,
			}, b.cfg.ContainerCreateTimeout)
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

	logger.Info("all containers provisioned successfully", "count", len(containerIDs))
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
func (b *Backend) Deprovision(ctx context.Context, leaseUUID string) error {
	logger := b.logger.With("lease_uuid", leaseUUID)

	b.provisionsMu.Lock()
	prov, exists := b.provisions[leaseUUID]
	if exists {
		delete(b.provisions, leaseUUID)
	}
	b.provisionsMu.Unlock()

	// Clean up callback URL
	b.callbackURLsMu.Lock()
	delete(b.callbackURLs, leaseUUID)
	b.callbackURLsMu.Unlock()

	// Release resources for all instances
	if exists {
		for i := 0; i < prov.Quantity; i++ {
			b.pool.Release(fmt.Sprintf("%s-%d", leaseUUID, i))
		}
	}

	if !exists {
		// Already deprovisioned - idempotent success
		return nil
	}

	// Remove all containers, collecting any real errors.
	// RemoveContainer is idempotent (already-removed containers return nil),
	// so non-nil errors indicate actual failures (I/O, permissions, etc.).
	var errs []error
	for _, containerID := range prov.ContainerIDs {
		if err := b.docker.RemoveContainer(ctx, containerID); err != nil {
			logger.Error("failed to remove container", "container_id", shortID(containerID), "error", err)
			errs = append(errs, fmt.Errorf("container %s: %w", shortID(containerID), err))
		} else {
			logger.Info("container removed", "container_id", shortID(containerID))
		}
	}

	// Clean up tenant network if isolation is enabled
	if b.cfg.IsNetworkIsolation() {
		if err := b.docker.RemoveTenantNetworkIfEmpty(ctx, prov.Tenant); err != nil {
			logger.Warn("failed to remove tenant network", "tenant", prov.Tenant, "error", err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("deprovision partially failed: %w", errors.Join(errs...))
	}

	logger.Info("deprovisioned", "containers_removed", len(prov.ContainerIDs))
	return nil
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

	b.callbackURLsMu.Lock()
	delete(b.callbackURLs, leaseUUID)
	b.callbackURLsMu.Unlock()
}

// recoverState rebuilds in-memory state from Docker containers.
// Handles multi-unit leases by grouping containers by lease UUID.
// Merges with existing state to preserve in-flight provisions.
func (b *Backend) recoverState(ctx context.Context) error {
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
				ContainerIDs: make([]string, 0),
			}
			recovered[c.LeaseUUID] = prov
		}

		// Add container ID to the provision
		prov.ContainerIDs = append(prov.ContainerIDs, c.ContainerID)
		prov.Quantity = len(prov.ContainerIDs)

		// If any container is not ready, mark the whole provision as not ready
		status := containerStatusToProvisionStatus(c.Status)
		if status != backend.ProvisionStatusReady && prov.Status == backend.ProvisionStatusReady {
			prov.Status = status
		}

		// Use instance-specific allocation ID
		instanceID := fmt.Sprintf("%s-%d", c.LeaseUUID, c.InstanceIndex)
		allocations = append(allocations, ResourceAllocation{
			LeaseUUID: instanceID,
			SKU:       c.SKU,
			CPUCores:  profile.CPUCores,
			MemoryMB:  profile.MemoryMB,
			DiskMB:    profile.DiskMB,
		})
	}

	// Merge with existing state: preserve in-flight provisions (status=provisioning)
	// that haven't yet produced containers.
	b.provisionsMu.Lock()
	for uuid, existing := range b.provisions {
		if existing.Status == backend.ProvisionStatusProvisioning {
			if _, hasContainers := recovered[uuid]; !hasContainers {
				// In-flight provision with no containers yet — preserve it
				recovered[uuid] = existing
			}
		}
	}
	b.provisions = recovered
	b.pool.Reset(allocations)
	b.provisionsMu.Unlock()

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
		case <-b.stopCh:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := b.recoverState(ctx); err != nil {
				b.logger.Error("reconciliation failed", "error", err)
			}
			cancel()
		}
	}
}

// sendCallback sends a provision result callback to Fred with HMAC signature.
// It retries up to callbackMaxAttempts times with exponential backoff.
// Retries are aborted if the backend is shutting down.
func (b *Backend) sendCallback(leaseUUID string, success bool, errMsg string) {
	b.callbackURLsMu.Lock()
	callbackURL, ok := b.callbackURLs[leaseUUID]
	b.callbackURLsMu.Unlock()

	if !ok {
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

	for attempt := range callbackMaxAttempts {
		if attempt > 0 {
			// Wait with backoff, but abort if shutting down
			select {
			case <-b.stopCh:
				b.logger.Warn("callback retry aborted by shutdown",
					"lease_uuid", leaseUUID,
					"attempt", attempt+1,
				)
				return
			case <-time.After(callbackBackoff[attempt]):
			}
		}

		if b.trySendCallback(leaseUUID, callbackURL, body) {
			return
		}
	}

	b.logger.Error("callback delivery failed after retries",
		"lease_uuid", leaseUUID,
		"attempts", callbackMaxAttempts,
		"success", success,
	)
}

// trySendCallback makes a single callback attempt. Returns true on success.
func (b *Backend) trySendCallback(leaseUUID, callbackURL string, body []byte) bool {
	ctx, cancel := context.WithTimeout(context.Background(), callbackTimeout)
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

// Stats returns current resource usage statistics.
func (b *Backend) Stats() ResourceStats {
	return b.pool.Stats()
}
