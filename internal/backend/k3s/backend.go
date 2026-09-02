package k3s

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/metrics/background"
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
	LeaseUUID            string
	Tenant               string
	ProviderUUID         string
	Status               backend.ProvisionStatus
	CallbackURL          string
	LifecycleCallbackURL string
	LastError            string
	// Reason/Message are the ENG-508 curated, tenant-safe failure signal.
	// LastError stays operator-only (verbose). runStubProvisioner authors
	// both at the failure source (ReasonInternal / "not implemented"); the
	// GetProvision map path and diagnostics fallback copy them through to
	// ProvisionInfo so both read paths agree on the wire shape. Mirrors
	// leasesm.ProvisionState.{Reason,Message} in the docker backend.
	Reason  backend.Reason
	Message string
	// FailCount mirrors leasesm.ProvisionState.FailCount in docker. The
	// stub provisioner increments it (not sets to 1) so retry-after-failure
	// cycles accumulate — Provision carries prevFailCount forward when
	// replacing a failed entry, and runStubProvisioner adds 1 on top under
	// the same lock as the Status=Failed mutation. Map-path and
	// diagnostics-fallback wire returns from GetProvision agree on
	// fail_count (BACKEND_GUIDE documents fail_count as a wire field).
	FailCount int
	CreatedAt time.Time

	// ctx / cancel form the per-lease cancellable lifecycle wired in
	// ENG-189. Provision creates the pair as a child of b.stopCtx and
	// stores both here; Deprovision calls cancel() inside provisionsMu
	// before deleting the map entry; runStubProvisioner captures ctx
	// under the lock and checks ctx.Err() before each external write
	// (diagnosticsStore.Store, callbackSender.SendOperationCallback) so a
	// concurrent Deprovision that wins the lock between the worker's
	// unlock and its post-unlock store touches still aborts the writes
	// for a torn-down lease. Mirrors docker-backend's leasesm.OnExit
	// pattern (PR #79 / commit cc62f3b).
	ctx    context.Context
	cancel context.CancelFunc
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

	storageIdentity  backendidentity.ID
	storageAuthority backendidentity.VerifiedStorage
	storageVerifier  k3sStorageIdentityVerifier
	clusterIdentity  func(context.Context) (string, error)
	identityVerifyMu sync.Mutex
	identityDriftErr error
	// storeAuthorityGate is the backend-wide terminal cause and the commit
	// linearization point shared by every identity-bound journal. Its lock is
	// distinct from identityVerifyMu because a store hook can fire while
	// VerifyStorageIdentity already holds that mutex.
	storeAuthorityGate *backendidentity.StorageAuthorityGate

	pool *shared.ResourcePool

	// provisions tracks active provisions by lease UUID. ENG-133's stub
	// provisioner (T4) writes "provisioning" entries here and a goroutine
	// flips them to "failed".
	provisions   map[string]*provision
	provisionsMu sync.RWMutex

	callbackStore    *shared.CallbackStore
	operationIntents operationIntentJournal
	commandFence     shared.CommandFence
	diagnosticsStore *shared.DiagnosticsStore
	releaseStore     *shared.ReleaseStore

	callbackSender *shared.CallbackSender

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

// newCallbackHTTPClient builds the HTTP client the CallbackSender uses for
// outbound callback delivery.
//
// Extracted from New so the CallbackInsecureSkipVerify branch is reachable
// from a test: the client is a local that New hands straight to
// NewCallbackSender, and the Backend deliberately keeps no field pointing
// at it (ENG-765 — a field only tests read is test scaffolding in a
// production struct).
func newCallbackHTTPClient(cfg Config, logger *slog.Logger) *http.Client {
	// CallbackSender installs the protocol delivery deadline on every request.
	// Keep the client-wide timeout unset so that request context is the only
	// authority and cannot be shortened independently.
	c := &http.Client{CheckRedirect: shared.RejectCallbackRedirect}
	if cfg.CallbackInsecureSkipVerify {
		logger.Error("INSECURE: callback TLS verification disabled — do NOT use in production")
		c.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, //nolint:gosec // Intentional for development
			},
		}
	}
	return c
}

// New creates a production K3s backend only after the configured cluster and
// durable marker have been identity-attested. No backend database or cleanup
// goroutine exists before that proof.
//
// Mirrors docker.New's lifecycle shape (validate config → build pool →
// build http client → open three bbolt stores → build callback sender)
// minus every Docker-specific dependency (no docker client, no compose
// service, no volume manager, no leasesm seams). The CallbackSender's
// OnDelivery and OnStoreError hooks are wired to the fred_k3s_backend_*
// Prometheus counters defined in metrics.go.
func New(cfg Config, logger *slog.Logger) (*Backend, error) {
	return NewWithContext(context.Background(), cfg, logger)
}

// NewWithContext is New with a caller-owned deadline for cluster identity
// attestation. The context is not retained after construction.
func NewWithContext(ctx context.Context, cfg Config, logger *slog.Logger) (*Backend, error) {
	return newBackend(ctx, cfg, logger, existingK3sStorageIdentity{})
}

// k3sStorageIdentityResolver is the construction-time capability that turns
// the configured cluster into a verified storage identity. Production has
// exactly one implementation. Tests can supply an explicit fake implementation
// from a _test.go file without exposing an unbound runtime constructor.
type k3sStorageIdentityResolver interface {
	resolve(context.Context, Config) (backendidentity.VerifiedStorage, error)
}

type k3sStorageIdentityVerifier interface {
	StorageIdentity() backendidentity.ID
	Verify(context.Context) error
}

type productionK3sStorageIdentityVerifier struct {
	backend   *Backend
	authority backendidentity.VerifiedStorage
}

func (verifier productionK3sStorageIdentityVerifier) StorageIdentity() backendidentity.ID {
	return verifier.authority.ID()
}

func (verifier productionK3sStorageIdentityVerifier) Verify(ctx context.Context) error {
	return verifier.backend.verifyStorageIdentity(ctx, verifier.authority)
}

type existingK3sStorageIdentity struct{}

func (existingK3sStorageIdentity) resolve(ctx context.Context, cfg Config) (backendidentity.VerifiedStorage, error) {
	probe := &Backend{cfg: cfg}
	if err := probe.loadStorageIdentity(ctx); err != nil {
		return backendidentity.VerifiedStorage{}, err
	}
	return probe.storageAuthority, nil
}

// InitializeStorageIdentityForConfig is the explicit, one-shot cluster
// lineage sealing operation. After proving the current kube-system UID, it
// binds the callback and release journals to one crash-resumable marker pair.
// The diagnostics database is deliberately outside this authority set.
type StorageIdentityInitializationMode string

const StorageIdentityInitializeNew StorageIdentityInitializationMode = "new"

type k3sStorageInitializationPaths struct {
	markers   *backendidentity.BoundMarkerPair
	callbacks *shared.BoundAuthoritativeStorePath
	releases  *shared.BoundAuthoritativeStorePath
}

func bindK3sStorageInitializationPaths(
	cfg Config,
	markerPath, anchorPath string,
) (*k3sStorageInitializationPaths, error) {
	paths := &k3sStorageInitializationPaths{}
	var err error
	paths.markers, err = backendidentity.BindMarkerPair(markerPath, anchorPath)
	if err != nil {
		return nil, err
	}
	paths.callbacks, err = shared.BindAuthoritativeStorePath(cfg.CallbackDBPath)
	if err != nil {
		_ = paths.Close()
		return nil, fmt.Errorf("bind callback journal parent: %w", err)
	}
	paths.releases, err = shared.BindAuthoritativeStorePath(cfg.ReleasesDBPath)
	if err != nil {
		_ = paths.Close()
		return nil, fmt.Errorf("bind release journal parent: %w", err)
	}
	if err := paths.Verify(); err != nil {
		_ = paths.Close()
		return nil, err
	}
	return paths, nil
}

func (paths *k3sStorageInitializationPaths) Verify() error {
	if paths == nil || paths.markers == nil || paths.callbacks == nil || paths.releases == nil {
		return errors.New("K3s storage initialization paths are not bound")
	}
	checks := []struct {
		label  string
		verify func() error
	}{
		{label: "markers", verify: paths.markers.VerifyPaths},
		{label: "callback", verify: paths.callbacks.VerifyPath},
		{label: "releases", verify: paths.releases.VerifyPath},
	}
	for _, check := range checks {
		if err := check.verify(); err != nil {
			return fmt.Errorf("%s storage parent changed during lineage proof: %w", check.label, err)
		}
	}
	return nil
}

func (paths *k3sStorageInitializationPaths) Close() error {
	if paths == nil {
		return nil
	}
	var errs []error
	if paths.markers != nil {
		errs = append(errs, paths.markers.Close())
	}
	for _, path := range []*shared.BoundAuthoritativeStorePath{paths.callbacks, paths.releases} {
		if path != nil {
			errs = append(errs, path.Close())
		}
	}
	return errors.Join(errs...)
}

func InitializeStorageIdentityForConfig(
	ctx context.Context,
	cfg Config,
	mode StorageIdentityInitializationMode,
) (backendidentity.ID, error) {
	if ctx == nil {
		return backendidentity.ID{}, errors.New("storage identity initialization context is required")
	}
	if err := cfg.Validate(); err != nil {
		return backendidentity.ID{}, fmt.Errorf("invalid config: %w", err)
	}
	if mode != StorageIdentityInitializeNew {
		return backendidentity.ID{}, fmt.Errorf("K3s storage identity initialization mode must be %q", StorageIdentityInitializeNew)
	}
	return initializeStorageIdentityForConfigWithProbe(ctx, cfg, &Backend{cfg: cfg})
}

// initializeStorageIdentityForConfigWithProbe contains the durable
// initialization protocol after public-input validation. Keeping the cluster
// identity capability on the probe lets tests deterministically exercise a
// cluster swap between the protocol's two read barriers.
func initializeStorageIdentityForConfigWithProbe(
	ctx context.Context,
	cfg Config,
	probe *Backend,
) (backendidentity.ID, error) {
	markerPath := filepath.Clean(cfg.CallbackDBPath) + ".storage-identity.json"
	anchorPath := probe.storageIdentityAnchorPath()
	if err := shared.ValidateDistinctStorePaths(map[string]string{
		"callback": cfg.CallbackDBPath, "diagnostics": cfg.DiagnosticsDBPath,
		"releases": cfg.ReleasesDBPath, "primary marker": markerPath, "anchor marker": anchorPath,
	}); err != nil {
		return backendidentity.ID{}, err
	}
	paths, err := bindK3sStorageInitializationPaths(cfg, markerPath, anchorPath)
	if err != nil {
		return backendidentity.ID{}, err
	}
	defer func() { _ = paths.Close() }()
	clusterUID, err := probe.currentClusterIdentity(ctx)
	if err != nil {
		return backendidentity.ID{}, err
	}
	if strings.TrimSpace(clusterUID) == "" {
		return backendidentity.ID{}, errors.New("kube-system namespace returned an empty UID")
	}
	verifyStableCluster := func() error {
		if err := paths.Verify(); err != nil {
			return err
		}
		currentUID, err := probe.currentClusterIdentity(ctx)
		if err != nil {
			return err
		}
		if currentUID != clusterUID {
			return fmt.Errorf("K3s cluster identity changed during lineage proof (%q != %q)",
				clusterUID, currentUID)
		}
		return paths.Verify()
	}
	// Verify the cluster before the committed-only marker operation. That
	// operation may recover a recognized interrupted publication, so it must
	// never run on evidence already known to have changed.
	if err := verifyStableCluster(); err != nil {
		return backendidentity.ID{}, err
	}
	sealedID, committed, inspectErr := paths.markers.VerifyCommittedWithStores(
		cfg.Name, clusterUID,
		func(storage backendidentity.VerifiedStorage) error {
			return verifyBoundK3sAuthoritativeStoreSet(paths, storage)
		},
	)
	if inspectErr != nil {
		return backendidentity.ID{}, fmt.Errorf("inspect committed K3s backend storage identity: %w", inspectErr)
	}
	verifySealedIdentity := func(expected backendidentity.ID) error {
		observed, stillCommitted, err := paths.markers.VerifyCommittedWithStores(
			cfg.Name, clusterUID,
			func(storage backendidentity.VerifiedStorage) error {
				return verifyBoundK3sAuthoritativeStoreSet(paths, storage)
			},
		)
		if err != nil {
			return fmt.Errorf("reverify sealed K3s backend storage identity: %w", err)
		}
		if !stillCommitted || observed != expected {
			return fmt.Errorf(
				"%w: K3s backend storage identity changed after sealing (%s != %s)",
				backendidentity.ErrMarkerBindingMismatch, observed, expected,
			)
		}
		return nil
	}
	if committed {
		if err := verifyStableCluster(); err != nil {
			return backendidentity.ID{}, err
		}
		if err := verifySealedIdentity(sealedID); err != nil {
			return backendidentity.ID{}, err
		}
		if err := verifyStableCluster(); err != nil {
			return backendidentity.ID{}, err
		}
		return sealedID, nil
	}
	profile, resuming, err := k3sStorageInitializationProfile(cfg, paths, clusterUID)
	if err != nil {
		return backendidentity.ID{}, err
	}
	callbackStore, err := shared.InspectBoundCallbackStoreReadOnly(paths.callbacks)
	if err != nil {
		return backendidentity.ID{}, fmt.Errorf("inspect callback outbox before storage identity initialization: %w", err)
	}
	if callbackStore.UpgradedSchema && !resuming {
		return backendidentity.ID{}, errors.New("storage identity initialization refuses an already-upgraded callback store; restore the sealed marker pair instead of resealing this lineage")
	}
	if callbackStore.Pending != 0 {
		return backendidentity.ID{}, fmt.Errorf(
			"storage identity initialization requires a drained callback outbox; %d pending callbacks remain",
			callbackStore.Pending,
		)
	}
	releases, err := shared.InspectBoundReleaseStoreReadOnly(paths.releases)
	if err != nil {
		return backendidentity.ID{}, fmt.Errorf("inspect release journal before storage identity initialization: %w", err)
	}
	if profile == backendidentity.InitializationProfileExisting &&
		(!callbackStore.Exists || !callbackStore.LegacySchema || !releases.Exists) {
		return backendidentity.ID{}, errors.New("existing K3s storage identity requires complete v0.13 callback and release journals")
	}
	if len(releases.ActiveLeaseUUIDs) != 0 {
		return backendidentity.ID{}, errors.New("K3s new-mode initialization refuses active release authority")
	}
	// Bind the final cluster observation and every physical parent before the
	// pending anchor is the first durable publication.
	if err := verifyStableCluster(); err != nil {
		return backendidentity.ID{}, err
	}
	hooks := backendidentity.MarkerPairStoreHooks{
		Profile: profile,
		Prepare: func(storage backendidentity.PendingStorage, profile backendidentity.InitializationProfile) error {
			if err := shared.PrepareBoundCallbackStoreStorage(paths.callbacks, storage, profile); err != nil {
				return err
			}
			return shared.PrepareBoundReleaseStoreStorage(paths.releases, storage, profile)
		},
		Check: func(storage backendidentity.PendingStorage) error {
			if err := shared.CheckBoundCallbackStoreStorage(paths.callbacks, storage); err != nil {
				return err
			}
			if err := shared.CheckBoundReleaseStoreStorage(paths.releases, storage); err != nil {
				return err
			}
			callbacks, err := shared.InspectBoundCallbackStoreReadOnly(paths.callbacks)
			if err != nil {
				return err
			}
			if callbacks.Pending != 0 || !callbacks.UpgradedSchema {
				return errors.New("prepared K3s callback journal is incomplete or not drained")
			}
			releases, err := shared.InspectBoundReleaseStoreReadOnly(paths.releases)
			if err != nil {
				return err
			}
			if !releases.Exists || len(releases.ActiveLeaseUUIDs) != 0 {
				return errors.New("prepared K3s release journal contains active authority")
			}
			currentUID, err := probe.currentClusterIdentity(ctx)
			if err != nil {
				return err
			}
			if currentUID != clusterUID {
				return errors.New("K3s cluster identity changed while binding authoritative stores")
			}
			return paths.Verify()
		},
		Verify: func(storage backendidentity.VerifiedStorage) error {
			return verifyBoundK3sAuthoritativeStoreSet(paths, storage)
		},
	}
	sealedStorage, err := paths.markers.InitializeWithStores(cfg.Name, clusterUID, hooks)
	if err != nil {
		return backendidentity.ID{}, fmt.Errorf("initialize K3s backend storage identity: %w", err)
	}
	sealedID = sealedStorage.ID()
	if err := verifyStableCluster(); err != nil {
		return backendidentity.ID{}, err
	}
	if err := verifySealedIdentity(sealedID); err != nil {
		return backendidentity.ID{}, err
	}
	if err := verifyStableCluster(); err != nil {
		return backendidentity.ID{}, err
	}
	return sealedID, nil
}

func k3sStorageInitializationProfile(
	cfg Config,
	paths *k3sStorageInitializationPaths,
	clusterUID string,
) (backendidentity.InitializationProfile, bool, error) {
	if paths == nil || paths.markers == nil {
		return "", false, errors.New("K3s storage initialization paths are required")
	}
	profile, pending, err := paths.markers.PendingInitializationProfile(cfg.Name, clusterUID)
	if err != nil {
		return "", false, err
	}
	if pending {
		if profile != backendidentity.InitializationProfileFresh &&
			profile != backendidentity.InitializationProfileExisting {
			return "", false, errors.New("pending K3s initialization has no store profile")
		}
		return profile, true, nil
	}
	callbackStore, err := shared.InspectBoundCallbackStoreReadOnly(paths.callbacks)
	if err != nil {
		return "", false, err
	}
	releases, err := shared.InspectBoundReleaseStoreReadOnly(paths.releases)
	if err != nil {
		return "", false, err
	}
	if callbackStore.IdentityBound || releases.IdentityBound {
		return "", false, errors.New("K3s authoritative journal is already identity-bound; restore its marker pair instead of resealing")
	}
	switch {
	case !callbackStore.Exists && !releases.Exists:
		return backendidentity.InitializationProfileFresh, false, nil
	case callbackStore.Exists && releases.Exists:
		return backendidentity.InitializationProfileExisting, false, nil
	default:
		return "", false, errors.New("K3s authoritative journals are incomplete")
	}
}

func newBackend(
	ctx context.Context,
	cfg Config,
	logger *slog.Logger,
	identityResolver k3sStorageIdentityResolver,
) (*Backend, error) {
	if ctx == nil {
		return nil, errors.New("backend construction context is required")
	}
	if logger == nil {
		return nil, errors.New("backend logger is required")
	}
	if identityResolver == nil {
		return nil, errors.New("K3s storage identity resolver is required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	storage, err := identityResolver.resolve(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if !storage.Valid() {
		return nil, errors.New("K3s storage identity resolver returned an invalid identity")
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

	httpClient := newCallbackHTTPClient(cfg, logger)
	stopCtx, stopCancel := context.WithCancel(context.Background())
	constructionComplete := false
	defer func() {
		if !constructionComplete {
			stopCancel()
		}
	}()
	storeAuthorityGate, err := backendidentity.NewStorageAuthorityGate(func(error) { stopCancel() })
	if err != nil {
		return nil, fmt.Errorf("construct backend storage authority gate: %w", err)
	}

	cbStore, err := shared.OpenIdentityBoundCallbackStore(shared.CallbackStoreConfig{
		DBPath: cfg.CallbackDBPath,
		MaxAge: cfg.CallbackMaxAge,
		OnCleanupPanic: func(any) {
			background.CleanupPanicsTotal.WithLabelValues("callback").Inc()
		},
	}, storage, storeAuthorityGate)
	if err != nil {
		return nil, fmt.Errorf("failed to open callback store: %w", err)
	}

	diagStore, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{
		DBPath: cfg.DiagnosticsDBPath,
		MaxAge: cfg.DiagnosticsMaxAge,
		OnCleanupPanic: func(any) {
			background.CleanupPanicsTotal.WithLabelValues("diagnostics").Inc()
		},
	})
	if err != nil {
		_ = cbStore.Close()
		return nil, fmt.Errorf("failed to open diagnostics store: %w", err)
	}

	releaseStore, err := shared.OpenIdentityBoundReleaseStore(shared.ReleaseStoreConfig{
		DBPath: cfg.ReleasesDBPath,
		MaxAge: cfg.ReleasesMaxAge,
		OnCleanupPanic: func(any) {
			background.CleanupPanicsTotal.WithLabelValues("releases").Inc()
		},
	}, storage, storeAuthorityGate)
	if err != nil {
		_ = cbStore.Close()
		_ = diagStore.Close()
		return nil, fmt.Errorf("failed to open release store: %w", err)
	}

	b := &Backend{
		cfg:                cfg,
		logger:             logger.With("backend", cfg.Name),
		pool:               pool,
		provisions:         make(map[string]*provision),
		callbackStore:      cbStore,
		operationIntents:   cbStore,
		diagnosticsStore:   diagStore,
		releaseStore:       releaseStore,
		storageIdentity:    storage.ID(),
		storageAuthority:   storage,
		storeAuthorityGate: storeAuthorityGate,
		stopCtx:            stopCtx,
		stopCancel:         stopCancel,
	}

	b.storageVerifier = productionK3sStorageIdentityVerifier{backend: b, authority: storage}

	callbackSender, err := shared.NewCallbackSender(shared.CallbackSenderConfig{
		Store:           cbStore,
		HTTPClient:      httpClient,
		Secret:          string(cfg.CallbackSecret),
		Logger:          b.logger,
		StopCtx:         b.stopCtx,
		BeforeReplay:    b.VerifyStorageIdentity,
		BeforeDelivery:  b.VerifyStorageIdentity,
		StorageIdentity: storage.ID(),
		OnDelivery: func(outcome string) {
			callbackDeliveryTotal.WithLabelValues(outcome).Inc()
		},
		OnStoreError: func() {
			callbackStoreErrorsTotal.Inc()
		},
		OnReplayPanic: func(any) {
			background.GoroutinePanicsTotal.WithLabelValues("callback_replay").Inc()
		},
	})
	if err != nil {
		_ = cbStore.Close()
		_ = diagStore.Close()
		_ = releaseStore.Close()
		return nil, fmt.Errorf("configure durable callback sender: %w", err)
	}
	b.callbackSender = callbackSender

	constructionComplete = true
	return b, nil
}

// Start initializes the backend. The ENG-133 scaffold has nothing to recover
// from the cluster, so it starts the durable callback replay lifecycle.
//
// The ctx parameter is kept on the signature to match docker.Backend.Start
// for HTTP-server consumer parity. ENG-134+ will use it for real cluster state
// recovery; until then an already-canceled startup is still rejected.
func (b *Backend) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := b.VerifyStorageIdentity(ctx); err != nil {
		return err
	}
	if err := b.recoverOperationIntents(); err != nil {
		callbackStoreErrorsTotal.Inc()
		return fmt.Errorf("recover interrupted operations: %w", err)
	}
	if err := b.VerifyStorageIdentity(ctx); err != nil {
		return fmt.Errorf("storage identity lost during K3s startup recovery: %w", err)
	}
	b.callbackStore.StartMaintenance()
	b.releaseStore.StartMaintenance()
	// A Fred outage can consume the full delivery retry budget. Run both the
	// initial and periodic replay on the tracked backend lifecycle so Start can
	// return ready while Stop still cancels and joins in-flight delivery.
	b.wg.Go(b.storageIdentityWatchLoop)
	b.wg.Go(b.callbackSender.RunReplayLoop)
	// The provisions map is always empty here in the ENG-133 scaffold —
	// k3s-backend does not recover lease state from the cluster on boot
	// (unlike docker.recoverState which scans the daemon). ENG-134+'s
	// real state-recovery loop will repopulate the map before Start
	// returns; until then there's no useful count to log.
	// Log both KubeconfigPath and KubeconfigPathList for the same reason
	// the startup banner in cmd/k3s-backend/main.go does: multi-path
	// KUBECONFIG populates only the list, so surfacing KubeconfigPath
	// alone would obscure which resolver tier the backend will fire.
	b.logger.Info("k3s backend started",
		"kubeconfig_path", b.cfg.KubeconfigPath,
		"kubeconfig_path_list", b.cfg.KubeconfigPathList,
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

// InitializeStorageIdentity loads an already sealed marker pair bound to the
// current kube-system UID. It never creates or repairs marker state.
func (b *Backend) InitializeStorageIdentity(ctx context.Context) error {
	return b.loadStorageIdentity(ctx)
}

func (b *Backend) loadStorageIdentity(ctx context.Context) error {
	if b == nil {
		return errors.New("k3s backend is required")
	}
	clusterUID, err := b.currentClusterIdentity(ctx)
	if err != nil {
		return err
	}
	if strings.TrimSpace(clusterUID) == "" {
		return errors.New("kube-system namespace returned an empty UID")
	}
	markerPath := filepath.Clean(b.cfg.CallbackDBPath) + ".storage-identity.json"
	parentInfo, err := os.Stat(filepath.Dir(markerPath))
	if err != nil {
		return fmt.Errorf("stat K3s identity marker directory: %w", err)
	}
	if !parentInfo.IsDir() {
		return fmt.Errorf("K3s identity marker parent is not a directory: %s", filepath.Dir(markerPath))
	}
	if err := shared.ValidateDistinctStorePaths(map[string]string{
		"callback": b.cfg.CallbackDBPath, "diagnostics": b.cfg.DiagnosticsDBPath,
		"releases": b.cfg.ReleasesDBPath, "primary marker": markerPath,
		"anchor marker": b.storageIdentityAnchorPath(),
	}); err != nil {
		return err
	}
	authority, err := backendidentity.LoadVerifiedMarkerPair(
		markerPath, b.storageIdentityAnchorPath(), b.cfg.Name, clusterUID,
	)
	if err != nil {
		return fmt.Errorf("load K3s backend storage identity: %w", err)
	}
	if err := verifyK3sAuthoritativeStoreSet(b.cfg, authority); err != nil {
		return fmt.Errorf("verify K3s authoritative stores: %w", err)
	}
	b.storageAuthority = authority
	b.storageIdentity = authority.ID()
	return nil
}

func verifyK3sAuthoritativeStoreSet(cfg Config, storage backendidentity.VerifiedStorage) error {
	if err := shared.VerifyCallbackStoreStorage(cfg.CallbackDBPath, storage); err != nil {
		return err
	}
	return shared.VerifyReleaseStoreStorage(cfg.ReleasesDBPath, storage)
}

func verifyBoundK3sAuthoritativeStoreSet(
	paths *k3sStorageInitializationPaths,
	storage backendidentity.VerifiedStorage,
) error {
	if paths == nil {
		return errors.New("K3s storage initialization paths are required")
	}
	if err := shared.VerifyBoundCallbackStoreStorage(paths.callbacks, storage); err != nil {
		return err
	}
	return shared.VerifyBoundReleaseStoreStorage(paths.releases, storage)
}

func (b *Backend) storageIdentityAnchorPath() string {
	return filepath.Clean(b.cfg.CallbackDBPath) + ".storage-identity-anchor.json"
}

func (b *Backend) currentClusterIdentity(ctx context.Context) (string, error) {
	if b.clusterIdentity != nil {
		return b.clusterIdentity(ctx)
	}
	clientset, err := buildKubeClient(b.cfg)
	if err != nil {
		return "", fmt.Errorf("build K3s client for storage identity: %w", err)
	}
	namespace, err := clientset.CoreV1().Namespaces().Get(ctx, "kube-system", metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("read kube-system namespace identity: %w", err)
	}
	return string(namespace.UID), nil
}

// VerifyStorageIdentity re-attests the marker and current cluster UID. It never
// creates a missing marker, so a kubeconfig/cluster swap fails closed.
func (b *Backend) VerifyStorageIdentity(ctx context.Context) error {
	if b == nil || b.storageVerifier == nil {
		return errors.New("K3s backend storage identity is not initialized")
	}
	if err := b.terminalStorageAuthorityError(); err != nil {
		return err
	}
	if identity := b.storageVerifier.StorageIdentity(); !identity.Valid() || identity != b.storageIdentity {
		return errors.New("K3s backend storage verifier returned a divergent or invalid identity")
	}
	err := b.storageVerifier.Verify(ctx)
	if errors.Is(err, backendidentity.ErrIdentityDrift) ||
		errors.Is(err, backendidentity.ErrMutationOutcomeAmbiguous) {
		_ = b.latchTerminalStorageAuthority(err)
	}
	return err
}

func (b *Backend) verifyStorageIdentity(
	ctx context.Context,
	storage backendidentity.VerifiedStorage,
) error {
	if b == nil || !storage.Valid() || storage.ID() != b.storageIdentity {
		return errors.New("K3s backend verified storage authority is invalid")
	}
	if err := b.terminalStorageAuthorityError(); err != nil {
		return err
	}
	b.identityVerifyMu.Lock()
	defer b.identityVerifyMu.Unlock()
	if err := b.terminalStorageAuthorityError(); err != nil {
		return err
	}
	if b.identityDriftErr != nil {
		return b.latchTerminalStorageAuthority(b.identityDriftErr)
	}
	clusterUID, err := b.currentClusterIdentity(ctx)
	if err != nil {
		return err
	}
	if strings.TrimSpace(clusterUID) == "" {
		return errors.New("kube-system namespace returned an empty UID")
	}
	markerPath := filepath.Clean(b.cfg.CallbackDBPath) + ".storage-identity.json"
	if err := backendidentity.VerifyMarkerPair(
		markerPath, b.storageIdentityAnchorPath(), b.cfg.Name, clusterUID, b.storageIdentity,
	); err != nil {
		wrapped := fmt.Errorf("verify K3s backend storage identity: %w", err)
		if errors.Is(err, backendidentity.ErrMarkerBindingMismatch) ||
			errors.Is(err, backendidentity.ErrInvalidMarker) {
			wrapped = fmt.Errorf("%w: %w", backendidentity.ErrIdentityDrift, wrapped)
			return b.latchIdentityVerificationFailureLocked(wrapped)
		}
		return wrapped
	}
	for name, verify := range map[string]func() error{
		"callback": func() error {
			if b.callbackStore == nil {
				return errors.New("callback store is missing")
			}
			return b.callbackStore.VerifyStorageIdentity(storage)
		},
		"releases": func() error {
			if b.releaseStore == nil {
				return errors.New("release store is missing")
			}
			return b.releaseStore.VerifyStorageIdentity(storage)
		},
	} {
		if err := verify(); err != nil {
			wrapped := fmt.Errorf("%w: verify K3s %s store identity: %w",
				backendidentity.ErrIdentityDrift, name, err)
			_ = b.latchIdentityVerificationFailureLocked(wrapped)
			return wrapped
		}
	}
	return nil
}

func (b *Backend) requireStorageIdentity(ctx context.Context) error {
	if b == nil {
		return errors.New("K3s backend is required")
	}
	if b.storageVerifier == nil {
		return errors.New("K3s backend storage verifier is required")
	}
	return b.VerifyStorageIdentity(ctx)
}

func (b *Backend) storageIdentityWatchLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-b.stopCtx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(b.stopCtx, 10*time.Second)
			err := b.requireStorageIdentity(ctx)
			cancel()
			if err != nil {
				b.logger.Error("periodic backend storage identity verification failed", "error", err)
			}
		}
	}
}

// StorageIdentity returns the initialized immutable cluster storage identity.
func (b *Backend) StorageIdentity() backendidentity.ID {
	if b == nil {
		return backendidentity.ID{}
	}
	return b.storageIdentity
}

// Health lives in health.go (T3): it builds a typed K8s clientset
// lazily on first call and round-trips Discovery().ServerVersion()
// against the configured K3s API server.
