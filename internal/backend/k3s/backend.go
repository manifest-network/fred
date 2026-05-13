package k3s

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/client-go/kubernetes"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/metrics"
)

// provision is the in-memory record for a single lease. The k3s scaffold
// keeps this minimal; Pod / Deployment / Service handles will be added
// in ENG-134+. Status uses the backend.ProvisionStatus* string values;
// the stub provisioner (T4) cycles every record from "provisioning"
// straight to "failed" and posts the canonical "not implemented" callback.
//
// Deliberately does not embed shared/leasesm.ProvisionState — the design
// plan §3 forbids touching shared/leasesm in ENG-133. ENG-134+ will rework
// this struct to embed the SM state when the real lease actor lands.
type provision struct {
	LeaseUUID    string
	Tenant       string
	ProviderUUID string
	Status       backend.ProvisionStatus
	CallbackURL  string
	LastError    string
	// FailCount mirrors leasesm.ProvisionState.FailCount in docker. The
	// stub provisioner increments it (not sets to 1) so retry-after-failure
	// cycles accumulate — Provision carries prevFailCount forward when
	// replacing a failed entry, and runStubProvisioner adds 1 on top under
	// the same lock as the Status=Failed mutation. Map-path and
	// diagnostics-fallback wire returns from GetProvision agree on
	// fail_count (BACKEND_GUIDE documents fail_count as a wire field).
	FailCount int
	CreatedAt time.Time
}

// Backend implements the Fred backend protocol for K3s. The ENG-133
// scaffold provides only the lifecycle skeleton (New / Start / Stop /
// Name) and a /health probe; substantive provisioning logic is deferred
// to ENG-134+. *Backend structurally satisfies the backendService
// interface declared in cmd/k3s-backend/server.go, verified by the
// compile-time guard at the bottom of provision_stub.go.
type Backend struct {
	cfg    Config
	logger *slog.Logger

	pool *shared.ResourcePool

	// provisions tracks active provisions by lease UUID. ENG-133's stub
	// provisioner (T4) writes "provisioning" entries here and a goroutine
	// flips them to "failed".
	provisions   map[string]*provision
	provisionsMu sync.RWMutex

	callbackStore    *shared.CallbackStore
	diagnosticsStore *shared.DiagnosticsStore
	releaseStore     *shared.ReleaseStore

	callbackSender *shared.CallbackSender

	// httpClient is used by callbackSender for outbound callback delivery.
	httpClient *http.Client

	// stopCtx is canceled on shutdown; stopCancel triggers it. Canceling
	// aborts in-flight callback retries (see shared.CallbackSender).
	stopCtx    context.Context
	stopCancel context.CancelFunc

	// wg waits for any goroutines the backend spawns. T2 spawns none;
	// T4's stub provisioner uses it.
	wg sync.WaitGroup

	// kube is the lazily-built typed clientset. atomic.Pointer keeps the
	// read path lock-free. ENG-133 has exactly one consumer (Health);
	// ENG-134+ may share it across provisioner methods.
	kube atomic.Pointer[kubernetes.Clientset]

	// kubeBuildOnce serializes the one-shot lazy build. A failed build
	// is cached in kubeBuildErr — fixing a bad kubeconfig requires a
	// process restart. Acceptable for the /health-only consumer in
	// ENG-133; ENG-134+ may revisit rebuildability when provisioner
	// methods start using the client.
	kubeBuildOnce sync.Once

	// kubeBuildErr captures the build outcome under kubeBuildOnce so
	// subsequent Health() calls return the same wrapped error.
	kubeBuildErr error
}

// New creates a new K3s backend.
//
// Mirrors docker.New's lifecycle shape (validate config → build pool →
// build http client → open three bbolt stores → build callback sender)
// minus every Docker-specific dependency (no docker client, no compose
// service, no volume manager, no leasesm seams). The CallbackSender's
// OnDelivery and OnStoreError hooks are wired to the fred_k3s_backend_*
// Prometheus counters defined in metrics.go.
func New(cfg Config, logger *slog.Logger) (*Backend, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// cfg.GetSKUProfile satisfies shared.SKUResolver
	// (func(sku string) (SKUProfile, error)).
	pool := shared.NewResourcePool(
		cfg.TotalCPUCores,
		cfg.TotalMemoryMB,
		cfg.TotalDiskMB,
		cfg.GetSKUProfile,
		cfg.TenantQuota,
	)

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}
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
		OnCleanupPanic: func(any) {
			metrics.CleanupPanicsTotal.WithLabelValues("callback").Inc()
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open callback store: %w", err)
	}

	diagStore, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{
		DBPath: cfg.DiagnosticsDBPath,
		MaxAge: cfg.DiagnosticsMaxAge,
		OnCleanupPanic: func(any) {
			metrics.CleanupPanicsTotal.WithLabelValues("diagnostics").Inc()
		},
	})
	if err != nil {
		_ = cbStore.Close()
		return nil, fmt.Errorf("failed to open diagnostics store: %w", err)
	}

	releaseStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{
		DBPath: cfg.ReleasesDBPath,
		MaxAge: cfg.ReleasesMaxAge,
		OnCleanupPanic: func(any) {
			metrics.CleanupPanicsTotal.WithLabelValues("releases").Inc()
		},
	})
	if err != nil {
		_ = cbStore.Close()
		_ = diagStore.Close()
		return nil, fmt.Errorf("failed to open release store: %w", err)
	}

	b := &Backend{
		cfg:              cfg,
		logger:           logger.With("backend", cfg.Name),
		pool:             pool,
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

// Start initializes the backend. The ENG-133 scaffold has nothing to
// recover from the cluster (no cluster-side resources yet) and no
// reconcile / event loop, so the only startup action is replaying any
// pending callbacks persisted by a previous run.
//
// The ctx parameter is kept on the signature to match docker.Backend.Start
// for HTTP-server consumer parity; ENG-134+ will use it when state recovery
// from the cluster lands.
func (b *Backend) Start(ctx context.Context) error {
	_ = ctx
	b.callbackSender.ReplayPendingCallbacks()
	// The provisions map is always empty here in the ENG-133 scaffold —
	// k3s-backend does not recover lease state from the cluster on boot
	// (unlike docker.recoverState which scans the daemon). ENG-134+'s
	// real state-recovery loop will repopulate the map before Start
	// returns; until then there's no useful count to log.
	b.logger.Info("k3s backend started",
		"kubeconfig_path", b.cfg.KubeconfigPath,
	)
	return nil
}

// Stop shuts down the backend gracefully. Mirrors docker.Backend.Stop
// minus the docker-client close (no docker client in the k3s scaffold).
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
	return errors.Join(errs...)
}

// Name returns the backend name.
func (b *Backend) Name() string {
	return b.cfg.Name
}

// Health lives in health.go (T3): it builds a typed K8s clientset
// lazily on first call and round-trips Discovery().ServerVersion()
// against the configured K3s API server.
