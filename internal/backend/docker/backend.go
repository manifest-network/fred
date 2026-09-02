package docker

import (
	"bytes"
	"cmp"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	networktypes "github.com/docker/docker/api/types/network"
	"github.com/moby/sys/mountinfo"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/metrics/background"
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
	ListManagedContainersStrict(ctx context.Context) ([]ContainerInfo, error)
	EnsureTenantNetwork(ctx context.Context, tenant string) (string, error)
	RemoveTenantNetworkIfEmpty(ctx context.Context, tenant string) error
	ListManagedNetworks(ctx context.Context) ([]networktypes.Inspect, error)
	DetectVolumeOwner(ctx context.Context, imageName string, volumePaths []string) (uid, gid int, err error)
	DetectWritablePaths(ctx context.Context, imageName string, uid int, candidateParents []string) ([]string, error)
	ExtractImageContent(ctx context.Context, imageName string, paths []string, destDir string, maxBytes, maxEntries int64) map[string]error
	ContainerEvents(ctx context.Context) (<-chan ContainerEvent, <-chan error)
}

// ContainerEvent represents a container lifecycle event from the Docker daemon.
// This keeps Docker SDK types out of the interface boundary.
type ContainerEvent struct {
	ContainerID string
	Action      string // "die", "stop", etc.
}

// releaseHistoryCapacityPlanner is the read-only admission half of the release
// journal. Production uses the ReleaseStore itself. Keeping the capability
// narrow makes the pre-substrate refusal boundary directly testable with tiny
// synthetic limits instead of manufacturing a 32 MiB history.
type releaseHistoryCapacityPlanner interface {
	CheckAppendActiveCapacity(string, shared.Release) error
	CheckRecordLegacyMigrationCapacity(
		string,
		[]byte,
		[]backend.LeaseItem,
		[]shared.SKUResourceSnapshot,
		shared.LegacyRuntimeAuthority,
		time.Time,
	) error
}

// Backend implements the backend.Backend interface for Docker containers.
type Backend struct {
	cfg     Config
	docker  dockerClient
	compose composeExecutor
	// mutations is the sole capability for tenant-substrate writes. Raw clients
	// remain private implementation details used for reads and identity probes.
	mutations storageMutationAdapters
	pool      *shared.ResourcePool
	volumes   volumeManager
	logger    *slog.Logger

	storageIdentity  backendidentity.ID
	storageAuthority backendidentity.VerifiedStorage
	storageVerifier  dockerStorageIdentityVerifier
	identityVerifyMu sync.Mutex
	identityDriftErr error
	// storeAuthorityGate is the backend-wide terminal cause and the commit
	// linearization point shared by every identity-bound journal. Its lock is
	// distinct from identityVerifyMu because a store hook can fire while
	// VerifyStorageIdentity already holds that mutex.
	storeAuthorityGate *backendidentity.StorageAuthorityGate
	// terminalStorageAuthorityFailure publishes the first exact cause that
	// permanently withdrew this Backend's storage authority. It is buffered so
	// the gate's failure hook can never block a journal boundary while the daemon
	// is busy or already shutting down. The channel is deliberately never closed:
	// a concurrent late latch must not race Stop into a send-on-closed panic.
	terminalStorageAuthorityFailure <-chan error

	// partitionSource is the parsed cfg.RetentionPartitionSource, resolved once
	// in New() (a malformed source is a startup failure, never a close-time
	// surprise). Zero value (PartitionSourceNone) means retention partitions are
	// never extracted — the legacy whole-tenant behavior.
	partitionSource shared.PartitionSource

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

	// volumeNameStripes serializes the two operations that can act on the
	// same managed volume name at the same time: the create-or-reuse in
	// setupVolBinds and the destroy in volumeOp.destroy. Without it the
	// ownership check and the RemoveAll it authorizes are two steps, so a
	// destroy could be decided against a claim that no longer holds and then
	// delete a directory a re-provision had already adopted (ENG-681).
	// volumeOp.destroy re-reads the live claim under this lock, which is what
	// makes the decision and the delete one atomic step per name.
	//
	// Striped for the same reason tenantNetworkStripes is: volume names embed
	// lease UUIDs, and a map[name]*Mutex would grow with every lease the node
	// has ever seen. Two unrelated volumes sharing a stripe just serialize a
	// create against a destroy, which are both infrequent per volume.
	//
	// Lock ordering: volume-name stripe -> provisionsMu (RLock), matching the
	// tenant-network rule above. Neither holder takes provisionsMu on entry,
	// and a volume stripe is never held across a tenant-network acquisition.
	volumeNameStripes [volumeNameStripeCount]sync.Mutex

	// recoverMu serializes recoverState calls. The reconcile loop and
	// external RefreshState (called by Fred's reconciler) both invoke
	// recoverState. Without serialization, concurrent calls can detect
	// the same ready→failed transitions and send duplicate callbacks.
	recoverMu sync.Mutex
	// closeSnapshotMu makes Docker inventory plus the close-intent journal one
	// authoritative recovery snapshot without serializing slow substrate cleanup.
	// Recovery holds the write side only through provisions+pool publication;
	// close admission and exact settlement hold the read side only around their
	// durable transactions. Destructive work holds neither side.
	closeSnapshotMu sync.RWMutex

	// retentionAccountingMu serializes refreshRetentionAccounting's
	// recompute-from-store + SetRetainedDisk so a stale snapshot can never
	// clobber a fresher one (which would under-count → over-admit).
	retentionAccountingMu sync.Mutex

	// callbackStore persists pending callbacks in bbolt
	callbackStore    *shared.CallbackStore
	operationIntents operationIntentJournal
	// commandFence closes the admission-to-reservation window against an
	// overlapping teardown for the same lease. It is keyed and zero-value ready;
	// idle per-lease mutexes are removed after their final waiter releases.
	commandFence shared.CommandFence

	// diagnosticsStore persists failure diagnostics in bbolt
	diagnosticsStore *shared.DiagnosticsStore

	// releaseStore persists release history in bbolt
	releaseStore *shared.ReleaseStore
	// releaseCapacityPlanner is explicitly wired to releaseStore in production.
	// Tests may provide the narrower capability to pin definitive refusal before
	// any Docker/volume mutation without changing the production 32 MiB contract.
	releaseCapacityPlanner releaseHistoryCapacityPlanner

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
	// load-bearing guard against a concurrent restore (ClaimForRestoreWithAuthority
	// active→restoring on a request goroutine) — do not "simplify" it into an
	// unconditional Delete.
	orphanStreaks map[string]int

	// callbackSender handles callback delivery with retry and HMAC
	callbackSender *shared.CallbackSender

	// volumeOwnerCache caches detected volume UID/GID per image ID
	// (content-addressable sha256 digest). Zero-value ready; no init needed.
	volumeOwnerCache sync.Map // image ID → volumeOwnerEntry

	// writablePathCache caches auto-detected writable paths per image ID
	// for non-root images. Zero-value ready.
	writablePathCache sync.Map // image ID → []string

	// stopCtx is canceled on shutdown; stopCancel triggers it.
	stopCtx    context.Context
	stopCancel context.CancelFunc
	// recoveryDockerReadTimeout bounds each Docker inventory/inspection call
	// made while rebuilding durable state. Zero selects the production default;
	// tests may use a shorter value to exercise cancellation deterministically.
	recoveryDockerReadTimeout time.Duration
	// startupRecoveryTimeout bounds the complete startup convergence pass. It is
	// intentionally independent of the caller's short readiness context because
	// legacy migrations may legitimately outlive that context, but it must still
	// be finite if a daemon accepts requests and then stops responding.
	startupRecoveryTimeout time.Duration
	// startupPhaseTimeout bounds each best-effort cleanup/reconciliation phase
	// inside the overall startup budget so fleet-sized loops cannot consume one
	// per-object Docker timeout indefinitely.
	startupPhaseTimeout time.Duration
	wg                  sync.WaitGroup
	// shutdownWaitDone is closed by the single waiter installed by Stop when
	// every backend-owned goroutine has returned. sync.WaitGroup has no native
	// context-aware wait, so this one-shot channel lets Stop enforce a deadline
	// without spawning one leaked waiter per retry.
	shutdownWaitOnce     sync.Once
	shutdownWaitDone     chan struct{}
	shutdownDrainTimeout time.Duration

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

// ErrShutdownDrainTimeout means backend-owned goroutines did not quiesce by
// the shutdown safety deadline. Stop deliberately leaves the Docker client and
// durable stores open in this case because a late worker may still use them.
// The serving binary treats this as a forced, non-zero process exit; embedded
// callers may retry Stop only after they can prove the workers have drained.
var ErrShutdownDrainTimeout = errors.New("docker backend workers did not drain before shutdown deadline")

const defaultShutdownDrainTimeout = 90 * time.Second

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

	// ResourceProfiles is the immutable resource authority used to admit and
	// create this live lease. It stays Docker-private because the substrate-
	// agnostic lifecycle state machine never interprets SKU capacities.
	ResourceProfiles []shared.SKUResourceSnapshot

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
	return b.captureContainerLogsContext(ctx, containerIDs, containerKeys)
}

// captureContainerLogsContext is the caller-budgeted form used by fleet
// recovery. Unlike captureContainerLogs, it never manufactures a fresh timeout:
// every lease in one recovery pass must share the same aggregate Docker-read
// budget rather than multiplying 30 seconds by the number of failed leases.
func (b *Backend) captureContainerLogsContext(
	ctx context.Context,
	containerIDs []string,
	containerKeys map[string]string,
) map[string]string {
	if len(containerIDs) == 0 {
		return nil
	}
	logs := make(map[string]string, len(containerIDs))
	// Aggregate byte budget across all captured containers, mirroring GetLogs
	// (ENG-590): a persisted diagnostic entry must not grow to gigabytes for a
	// many-container lease. Unlike GetLogs (which placeholders over-budget
	// containers for a live tenant view), this best-effort diagnostics snapshot
	// simply stops once the budget is spent — a lean persisted record beats
	// padding bbolt with hundreds of placeholder entries, and with
	// persistedLogTail-bounded output the budget is effectively never reached
	// here in practice.
	remaining := maxTotalLogBytes
	for i, cid := range containerIDs {
		if remaining <= 0 || ctx.Err() != nil {
			break
		}
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
		trimmed, consumed := trimLogToBudget(logOutput, remaining)
		remaining -= consumed
		logs[key] = trimmed
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	b.persistDiagnosticsContext(ctx, entry, containerIDs, containerKeys...)
}

// persistDiagnosticsContext persists a diagnostic under a caller-owned log
// capture budget. Recovery uses one context for the complete failed-lease set;
// worker failure paths use persistDiagnostics above so shutdown cannot erase the
// only opportunity to capture logs before container cleanup.
func (b *Backend) persistDiagnosticsContext(
	ctx context.Context,
	entry shared.DiagnosticEntry,
	containerIDs []string,
	containerKeys ...map[string]string,
) {
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
	if logs := b.captureContainerLogsContext(ctx, containerIDs, keys); logs != nil {
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

// New creates a production Docker backend only after the configured Docker
// daemon, volume root, and durable marker have been identity-attested. No
// callback/diagnostic/release/retention database is opened before that proof.
func New(cfg Config, logger *slog.Logger) (*Backend, error) {
	return newBackendWithConstructionTimeout(
		cfg,
		logger,
		defaultBackendConstructionTimeout,
		existingDockerStorageIdentity{},
	)
}

// NewWithContext is New with a caller-owned deadline for substrate
// attestation. The context is not retained after construction.
func NewWithContext(ctx context.Context, cfg Config, logger *slog.Logger) (*Backend, error) {
	return newBackend(ctx, cfg, logger, existingDockerStorageIdentity{})
}

// dockerStorageIdentityResolver is the construction-time capability that
// turns an opened Docker/volume substrate into a verified storage identity.
// Production has exactly one implementation. Tests can supply an explicit
// fake implementation from a _test.go file without adding an unbound runtime
// constructor or weakening CallbackSender's durable authority requirements.
type dockerStorageIdentityResolver interface {
	resolve(context.Context, Config, dockerClient, volumeManager) (backendidentity.VerifiedStorage, error)
}

type dockerStorageIdentityVerifier interface {
	StorageIdentity() backendidentity.ID
	Verify(context.Context) error
}

type productionDockerStorageIdentityVerifier struct {
	backend   *Backend
	authority backendidentity.VerifiedStorage
}

func (verifier productionDockerStorageIdentityVerifier) StorageIdentity() backendidentity.ID {
	return verifier.authority.ID()
}

func (verifier productionDockerStorageIdentityVerifier) Verify(ctx context.Context) error {
	return verifier.backend.verifyStorageIdentity(ctx, verifier.authority)
}

type existingDockerStorageIdentity struct{}

// newBackendWithConstructionTimeout is the shared implementation behind New's
// finite default. Keeping the timeout injection at this narrow seam lets tests
// prove that a stalled identity resolver is canceled without weakening the
// production resolver capability or adding mutable package globals.
func newBackendWithConstructionTimeout(
	cfg Config,
	logger *slog.Logger,
	timeout time.Duration,
	identityResolver dockerStorageIdentityResolver,
) (*Backend, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return newBackend(ctx, cfg, logger, identityResolver)
}

func (existingDockerStorageIdentity) resolve(
	ctx context.Context,
	cfg Config,
	docker dockerClient,
	volumes volumeManager,
) (backendidentity.VerifiedStorage, error) {
	probe := &Backend{cfg: cfg, docker: docker, volumes: volumes}
	if err := probe.loadStorageIdentity(ctx); err != nil {
		return backendidentity.VerifiedStorage{}, err
	}
	return probe.storageAuthority, nil
}

// StorageIdentityInitializationMode makes the operator's first-adoption claim
// explicit. The zero value is invalid.
type StorageIdentityInitializationMode string

const (
	StorageIdentityInitializeNew   StorageIdentityInitializationMode = "new"
	StorageIdentityInitializeAdopt StorageIdentityInitializationMode = "adopt"
)

type dockerStorageInitializationPaths struct {
	markers   *backendidentity.BoundMarkerPair
	callbacks *shared.BoundAuthoritativeStorePath
	releases  *shared.BoundAuthoritativeStorePath
	retention *shared.BoundAuthoritativeStorePath
}

func bindDockerStorageInitializationPaths(
	cfg Config,
	markerPath, anchorPath string,
) (*dockerStorageInitializationPaths, error) {
	paths := &dockerStorageInitializationPaths{}
	var err error
	paths.markers, err = backendidentity.BindMarkerPair(markerPath, anchorPath)
	if err != nil {
		return nil, err
	}
	bind := func(destination **shared.BoundAuthoritativeStorePath, path, label string) error {
		*destination, err = shared.BindAuthoritativeStorePath(path)
		if err != nil {
			return fmt.Errorf("bind %s journal parent: %w", label, err)
		}
		return nil
	}
	if err := bind(&paths.callbacks, cfg.CallbackDBPath, "callback"); err != nil {
		_ = paths.Close()
		return nil, err
	}
	if err := bind(&paths.releases, cfg.ReleasesDBPath, "release"); err != nil {
		_ = paths.Close()
		return nil, err
	}
	if err := bind(&paths.retention, cfg.RetentionDBPath, "retention"); err != nil {
		_ = paths.Close()
		return nil, err
	}
	if err := paths.Verify(); err != nil {
		_ = paths.Close()
		return nil, err
	}
	return paths, nil
}

func (paths *dockerStorageInitializationPaths) Verify() error {
	if paths == nil || paths.markers == nil || paths.callbacks == nil ||
		paths.releases == nil || paths.retention == nil {
		return errors.New("docker storage initialization paths are not bound")
	}
	checks := []struct {
		label  string
		verify func() error
	}{
		{label: "markers", verify: paths.markers.VerifyPaths},
		{label: "callback", verify: paths.callbacks.VerifyPath},
		{label: "releases", verify: paths.releases.VerifyPath},
		{label: "retention", verify: paths.retention.VerifyPath},
	}
	for _, check := range checks {
		if err := check.verify(); err != nil {
			return fmt.Errorf("%s storage parent changed during lineage proof: %w", check.label, err)
		}
	}
	return nil
}

func (paths *dockerStorageInitializationPaths) Close() error {
	if paths == nil {
		return nil
	}
	var errs []error
	if paths.markers != nil {
		errs = append(errs, paths.markers.Close())
	}
	for _, path := range []*shared.BoundAuthoritativeStorePath{
		paths.callbacks, paths.releases, paths.retention,
	} {
		if path != nil {
			errs = append(errs, path.Close())
		}
	}
	return errors.Join(errs...)
}

// StorageIdentityAdoptionVerdict is the stable machine-readable result of the
// stopped-v0.13 read-only preflight. Its zero value is never returned on
// success, so callers cannot mistake an incomplete check for admission.
type StorageIdentityAdoptionVerdict string

const (
	// StorageIdentityAdoptionReady proves that the same descriptor-bound
	// evidence used by adopt currently permits a seal. Initialization must still
	// repeat the proof after the operator's backup because the substrate can
	// change between commands.
	StorageIdentityAdoptionReady StorageIdentityAdoptionVerdict = "ready_for_v0_13_storage_identity_adoption"
)

// ErrV013InterruptedDeprovision identifies a stopped legacy lineage that must
// finish its exact close operation under v0.13 before it can be sealed.
var ErrV013InterruptedDeprovision = errors.New("v0.13 lineage contains an interrupted deprovision finalization")

// ErrV013UnresolvedClose identifies a stopped legacy lineage whose active
// release outlived its container cohort without a retention finalizer. That
// shape is compatible with more than one v0.13 close boundary, so neither
// replay nor automatic journal repair is safe.
var ErrV013UnresolvedClose = errors.New("v0.13 lineage contains an unresolved close boundary")

// ErrV013InterruptedMigration identifies one of v0.13's durable
// stop/rename/Compose migration crash boundaries. The read-only preflight can
// diagnose these shapes, but it never manufactures the missing release or
// callback authority needed to seal them.
var ErrV013InterruptedMigration = errors.New("v0.13 lineage contains an interrupted legacy migration")

// ErrV013OrphanRollbackRemnant identifies an exact stopped `-prev` container
// whose release, retention, and volume authority are all absent. It is not
// automatically ignored: terminal chain/provider evidence is required before
// an operator may remove the remnant and rerun preflight.
var ErrV013OrphanRollbackRemnant = errors.New("v0.13 lineage contains an orphan rollback remnant")

type dockerStorageIdentityProof struct {
	paths         *dockerStorageInitializationPaths
	initialDaemon DaemonSecurityInfo
}

// storageIdentityProofClient is deliberately read-only: neither preflight nor
// the initializer's evidence phase can obtain container mutation authority.
type storageIdentityProofClient interface {
	storageIdentityEvidenceClient
	Ping(context.Context) error
	DaemonInfo(context.Context) (DaemonSecurityInfo, error)
}

type storageIdentityProofVolumes interface {
	Validate() error
	ListForProof(context.Context) ([]string, error)
	AttestManagedVolume(context.Context, managedVolumeName) error
	RequireNoInterruptedVolumeMutations(context.Context) error
}

// attestManagedVolumeInventory returns one exact, duplicate-free inventory
// only after every name has crossed both proof boundaries: the managed-volume
// grammar and the concrete manager's read-only substrate attestation. A plain
// directory named like a volume is therefore never enough to seal or load a
// storage identity.
func attestManagedVolumeInventory(
	ctx context.Context,
	volumes storageIdentityProofVolumes,
) (map[string]managedVolumeName, error) {
	if ctx == nil {
		return nil, errors.New("managed volume inventory proof context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("attest managed volume inventory: %w", err)
	}
	managedVolumes, err := volumes.ListForProof(ctx)
	if err != nil {
		return nil, fmt.Errorf("enumerate managed volumes: %w", err)
	}
	managedSet := make(map[string]managedVolumeName, len(managedVolumes))
	for _, volumeName := range managedVolumes {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("attest managed volume inventory: %w", err)
		}
		managedName, parseErr := parseManagedVolumeName(volumeName)
		if parseErr != nil {
			return nil, fmt.Errorf(
				"managed volume %q name is invalid: %w",
				volumeName,
				parseErr,
			)
		}
		if _, duplicate := managedSet[volumeName]; duplicate {
			return nil, fmt.Errorf("managed volume inventory contains duplicate name %q", volumeName)
		}
		if err := volumes.AttestManagedVolume(ctx, managedName); err != nil {
			return nil, fmt.Errorf("attest managed volume %q: %w", volumeName, err)
		}
		managedSet[volumeName] = managedName
	}
	return managedSet, nil
}

func acquireDockerStorageIdentityProof(
	ctx context.Context,
	cfg Config,
	dockerClient storageIdentityProofClient,
	volumes storageIdentityProofVolumes,
) (*dockerStorageIdentityProof, error) {
	if err := dockerClient.Ping(ctx); err != nil {
		return nil, fmt.Errorf("verify Docker connectivity before lineage proof: %w", err)
	}
	if err := volumes.Validate(); err != nil {
		return nil, fmt.Errorf("validate volume substrate before lineage proof: %w", err)
	}
	// One-shot new/adopt/preflight commands are deliberately read-only until
	// marker publication. Private mutation debris cannot be silently normalized
	// into the lineage being measured; the stopped operator must first recover
	// it with the already-sealed backend or remove it under an explicit runbook.
	if err := volumes.RequireNoInterruptedVolumeMutations(ctx); err != nil {
		return nil, fmt.Errorf("interrupted managed-volume mutation blocks lineage proof: %w", err)
	}
	if pinner, ok := volumes.(identityRootPinner); ok {
		if err := pinner.PinIdentityRoot(); err != nil {
			return nil, fmt.Errorf("pin volume substrate before lineage proof: %w", err)
		}
	}
	markerPath, err := dockerStorageIdentityMarkerPath(cfg)
	if err != nil {
		return nil, err
	}
	anchorPath := dockerStorageIdentityAnchorPath(cfg)
	if err := shared.ValidateDistinctStorePaths(map[string]string{
		"callback": cfg.CallbackDBPath, "diagnostics": cfg.DiagnosticsDBPath,
		"releases": cfg.ReleasesDBPath, "retention": cfg.RetentionDBPath,
		"primary marker": markerPath, "anchor marker": anchorPath,
	}); err != nil {
		return nil, err
	}
	paths, err := bindDockerStorageInitializationPaths(cfg, markerPath, anchorPath)
	if err != nil {
		return nil, err
	}
	initialDaemon, err := dockerClient.DaemonInfo(ctx)
	if err != nil {
		_ = paths.Close()
		return nil, fmt.Errorf("read Docker daemon identity before lineage proof: %w", err)
	}
	if strings.TrimSpace(initialDaemon.SystemID) == "" {
		_ = paths.Close()
		return nil, errors.New("docker daemon returned an empty system ID")
	}
	return &dockerStorageIdentityProof{paths: paths, initialDaemon: initialDaemon}, nil
}

func (proof *dockerStorageIdentityProof) Close() error {
	if proof == nil || proof.paths == nil {
		return nil
	}
	err := proof.paths.Close()
	proof.paths = nil
	return err
}

func (proof *dockerStorageIdentityProof) verifyStableSubstrate(
	ctx context.Context,
	dockerClient storageIdentityProofClient,
	volumes storageIdentityProofVolumes,
) error {
	if proof == nil || proof.paths == nil {
		return errors.New("docker storage identity proof is not bound")
	}
	if err := proof.paths.Verify(); err != nil {
		return err
	}
	if pinner, ok := volumes.(identityRootPinner); ok {
		if err := pinner.VerifyIdentityRoot(); err != nil {
			return fmt.Errorf("volume substrate changed during lineage proof: %w", err)
		}
	}
	currentDaemon, err := dockerClient.DaemonInfo(ctx)
	if err != nil {
		return fmt.Errorf("re-read Docker daemon identity after lineage proof: %w", err)
	}
	if currentDaemon.SystemID != proof.initialDaemon.SystemID {
		return fmt.Errorf("docker daemon identity changed during lineage proof (%q != %q)",
			proof.initialDaemon.SystemID, currentDaemon.SystemID)
	}
	return proof.paths.Verify()
}

// PreflightStorageIdentityAdoptionForConfig performs the exact stopped-v0.13
// adoption evidence proof without publishing or recovering markers and without
// binding or normalizing a journal. It is safe to run before the cutover backup;
// a successful initializer repeats the proof at its own publication boundary.
func PreflightStorageIdentityAdoptionForConfig(
	ctx context.Context,
	cfg Config,
	logger *slog.Logger,
) (StorageIdentityAdoptionVerdict, error) {
	if ctx == nil {
		return "", errors.New("storage identity adoption preflight context is required")
	}
	if logger == nil {
		return "", errors.New("storage identity adoption preflight logger is required")
	}
	if err := cfg.Validate(); err != nil {
		return "", fmt.Errorf("invalid config: %w", err)
	}
	if err := verifyConfiguredVolumeMount(cfg); err != nil {
		return "", err
	}
	dockerClient, err := NewDockerClient(cfg.DockerHost, cfg.Name)
	if err != nil {
		return "", fmt.Errorf("create Docker client for storage identity preflight: %w", err)
	}
	defer func() { _ = dockerClient.Close() }()
	volumes, err := newVolumeManager(
		cfg.VolumeDataPath, cfg.VolumeFilesystem, cfg.GetMinAvgFileBytes(), logger,
	)
	if err != nil {
		return "", fmt.Errorf("create volume manager for storage identity preflight: %w", err)
	}
	return preflightStorageIdentityAdoptionWithDependencies(
		ctx, cfg, dockerClient, volumes,
	)
}

func preflightStorageIdentityAdoptionWithDependencies(
	ctx context.Context,
	cfg Config,
	dockerClient storageIdentityProofClient,
	volumes storageIdentityProofVolumes,
) (StorageIdentityAdoptionVerdict, error) {
	proof, err := acquireDockerStorageIdentityProof(ctx, cfg, dockerClient, volumes)
	if err != nil {
		return "", err
	}
	defer func() { _ = proof.Close() }()
	if err := proof.verifyStableSubstrate(ctx, dockerClient, volumes); err != nil {
		return "", err
	}
	if err := proof.paths.markers.VerifyAbsent(); err != nil {
		return "", fmt.Errorf("preflight requires an unsealed v0.13 marker pair: %w", err)
	}
	profile, resuming, err := dockerStorageInitializationProfile(
		cfg,
		proof.paths,
		proof.initialDaemon.SystemID,
		StorageIdentityInitializeAdopt,
	)
	if err != nil {
		return "", err
	}
	if resuming {
		return "", errors.New("read-only adoption preflight refuses a pending storage identity initialization")
	}
	if profile != backendidentity.InitializationProfileExisting {
		return "", errors.New("adoption preflight requires a complete existing v0.13 lineage")
	}
	if err := verifyStorageIdentityInitializationEvidence(
		ctx,
		cfg,
		dockerClient,
		volumes,
		StorageIdentityInitializeAdopt,
		profile,
		false,
		proof.paths,
	); err != nil {
		return "", err
	}
	if err := proof.verifyStableSubstrate(ctx, dockerClient, volumes); err != nil {
		return "", err
	}
	if err := proof.paths.markers.VerifyAbsent(); err != nil {
		return "", fmt.Errorf("marker pair changed during read-only adoption preflight: %w", err)
	}
	return StorageIdentityAdoptionReady, nil
}

// InitializeStorageIdentityForConfig is the explicit, one-shot storage-lineage
// sealing operation. It validates and pins the Docker substrate, binds the
// callback, release, and retention journals to one crash-resumable marker pair,
// and closes every temporary handle. The diagnostics database is deliberately
// outside this authority set. Normal New/Start paths are verify-only and refuse
// an absent, incomplete, or foreign member.
func InitializeStorageIdentityForConfig(
	ctx context.Context,
	cfg Config,
	logger *slog.Logger,
	mode StorageIdentityInitializationMode,
) (backendidentity.ID, error) {
	if ctx == nil {
		return backendidentity.ID{}, errors.New("storage identity initialization context is required")
	}
	if logger == nil {
		return backendidentity.ID{}, errors.New("storage identity initialization logger is required")
	}
	if mode != StorageIdentityInitializeNew && mode != StorageIdentityInitializeAdopt {
		return backendidentity.ID{}, fmt.Errorf("storage identity initialization mode must be %q or %q",
			StorageIdentityInitializeNew, StorageIdentityInitializeAdopt)
	}
	if err := cfg.Validate(); err != nil {
		return backendidentity.ID{}, fmt.Errorf("invalid config: %w", err)
	}
	if err := verifyConfiguredVolumeMount(cfg); err != nil {
		return backendidentity.ID{}, err
	}
	dockerClient, err := NewDockerClient(cfg.DockerHost, cfg.Name)
	if err != nil {
		return backendidentity.ID{}, fmt.Errorf("create Docker client for storage identity: %w", err)
	}
	defer func() { _ = dockerClient.Close() }()
	volumes, err := newVolumeManager(
		cfg.VolumeDataPath, cfg.VolumeFilesystem, cfg.GetMinAvgFileBytes(), logger,
	)
	if err != nil {
		return backendidentity.ID{}, fmt.Errorf("create volume manager for storage identity: %w", err)
	}
	return initializeStorageIdentityWithDependencies(ctx, cfg, mode, dockerClient, volumes)
}

// initializeStorageIdentityWithDependencies contains the proof-and-publication
// protocol after public configuration validation and dependency construction.
// The narrow dependency boundary lets deterministic tests drive a parent-path
// replacement at a real evidence barrier without adding mutable production
// hooks.
func initializeStorageIdentityWithDependencies(
	ctx context.Context,
	cfg Config,
	mode StorageIdentityInitializationMode,
	dockerClient storageIdentityProofClient,
	volumes storageIdentityProofVolumes,
) (backendidentity.ID, error) {
	proof, err := acquireDockerStorageIdentityProof(ctx, cfg, dockerClient, volumes)
	if err != nil {
		return backendidentity.ID{}, err
	}
	defer func() { _ = proof.Close() }()
	paths := proof.paths
	initialDaemon := proof.initialDaemon
	verifyStableSubstrate := func() error {
		return proof.verifyStableSubstrate(ctx, dockerClient, volumes)
	}
	// Verify the substrate before the committed-only marker operation. That
	// operation may recover a recognized interrupted publication, so it must
	// never run on evidence already known to have changed.
	if err := verifyStableSubstrate(); err != nil {
		return backendidentity.ID{}, err
	}
	sealedID, committed, inspectErr := paths.markers.VerifyCommittedWithStores(
		cfg.Name, initialDaemon.SystemID,
		func(storage backendidentity.VerifiedStorage) error {
			return verifyBoundDockerAuthoritativeStoreSet(paths, storage)
		},
	)
	if inspectErr != nil {
		return backendidentity.ID{}, fmt.Errorf("inspect committed Docker backend storage identity: %w", inspectErr)
	}
	verifySealedIdentity := func(expected backendidentity.ID) error {
		observed, stillCommitted, err := paths.markers.VerifyCommittedWithStores(
			cfg.Name, initialDaemon.SystemID,
			func(storage backendidentity.VerifiedStorage) error {
				return verifyBoundDockerAuthoritativeStoreSet(paths, storage)
			},
		)
		if err != nil {
			return fmt.Errorf("reverify sealed Docker backend storage identity: %w", err)
		}
		if !stillCommitted || observed != expected {
			return fmt.Errorf(
				"%w: Docker backend storage identity changed after sealing (%s != %s)",
				backendidentity.ErrMarkerBindingMismatch, observed, expected,
			)
		}
		return nil
	}
	if committed {
		if err := verifyStableSubstrate(); err != nil {
			return backendidentity.ID{}, err
		}
		if err := verifySealedIdentity(sealedID); err != nil {
			return backendidentity.ID{}, err
		}
		if err := verifyStableSubstrate(); err != nil {
			return backendidentity.ID{}, err
		}
		return sealedID, nil
	}
	profile, resuming, err := dockerStorageInitializationProfile(
		cfg, paths, initialDaemon.SystemID, mode,
	)
	if err != nil {
		return backendidentity.ID{}, err
	}
	if err := verifyStorageIdentityInitializationEvidence(
		ctx, cfg, dockerClient, volumes, mode, profile, resuming, paths,
	); err != nil {
		return backendidentity.ID{}, err
	}
	if err := verifyStableSubstrate(); err != nil {
		return backendidentity.ID{}, err
	}
	hooks := backendidentity.MarkerPairStoreHooks{
		Profile: profile,
		Prepare: func(storage backendidentity.PendingStorage, profile backendidentity.InitializationProfile) error {
			if err := shared.PrepareBoundCallbackStoreStorage(paths.callbacks, storage, profile); err != nil {
				return err
			}
			if err := shared.PrepareBoundReleaseStoreStorage(paths.releases, storage, profile); err != nil {
				return err
			}
			return shared.PrepareBoundRetentionStoreStorage(paths.retention, storage, profile)
		},
		Check: func(storage backendidentity.PendingStorage) error {
			if err := shared.CheckBoundCallbackStoreStorage(paths.callbacks, storage); err != nil {
				return err
			}
			if err := shared.CheckBoundReleaseStoreStorage(paths.releases, storage); err != nil {
				return err
			}
			if err := shared.CheckBoundRetentionStoreStorage(paths.retention, storage); err != nil {
				return err
			}
			if err := verifyStorageIdentityInitializationEvidence(
				ctx, cfg, dockerClient, volumes, mode, profile, true, paths,
			); err != nil {
				return err
			}
			if pinner, ok := volumes.(identityRootPinner); ok {
				if err := pinner.VerifyIdentityRoot(); err != nil {
					return err
				}
			}
			daemon, err := dockerClient.DaemonInfo(ctx)
			if err != nil {
				return err
			}
			if daemon.SystemID != initialDaemon.SystemID {
				return errors.New("docker daemon identity changed while binding authoritative stores")
			}
			return paths.Verify()
		},
		Verify: func(storage backendidentity.VerifiedStorage) error {
			return verifyBoundDockerAuthoritativeStoreSet(paths, storage)
		},
	}
	sealedStorage, err := paths.markers.InitializeWithStores(cfg.Name, initialDaemon.SystemID, hooks)
	if err != nil {
		return backendidentity.ID{}, fmt.Errorf("initialize Docker backend storage identity: %w", err)
	}
	sealedID = sealedStorage.ID()
	if err := verifyStableSubstrate(); err != nil {
		return backendidentity.ID{}, err
	}
	if err := verifySealedIdentity(sealedID); err != nil {
		return backendidentity.ID{}, err
	}
	if err := verifyStableSubstrate(); err != nil {
		return backendidentity.ID{}, err
	}
	return sealedID, nil
}

// storageIdentityEvidenceClient is the read-only Docker capability required to
// prove whether a substrate is empty or belongs to the stopped v0.13 lineage.
// Keeping the initialization proof on this narrow interface prevents it from
// acquiring container-mutation authority by accident.
type storageIdentityEvidenceClient interface {
	ListManagedContainersStrict(context.Context) ([]ContainerInfo, error)
}

func verifyStorageIdentityInitializationEvidence(
	ctx context.Context,
	cfg Config,
	dockerClient storageIdentityEvidenceClient,
	volumes storageIdentityProofVolumes,
	mode StorageIdentityInitializationMode,
	profile backendidentity.InitializationProfile,
	prepared bool,
	paths *dockerStorageInitializationPaths,
) error {
	if paths == nil {
		return errors.New("docker storage initialization paths are required")
	}
	callbackStore, err := shared.InspectBoundCallbackStoreReadOnly(paths.callbacks)
	if err != nil {
		return fmt.Errorf("inspect callback outbox before storage identity initialization: %w", err)
	}
	if callbackStore.UpgradedSchema && !prepared {
		return errors.New("storage identity initialization refuses an already-upgraded callback store; restore the sealed marker pair instead of resealing this lineage")
	}
	if callbackStore.Pending != 0 {
		return fmt.Errorf(
			"storage identity initialization requires a drained callback outbox; %d pending callbacks remain",
			callbackStore.Pending,
		)
	}
	if profile == backendidentity.InitializationProfileExisting &&
		(!callbackStore.Exists || !callbackStore.LegacySchema) {
		return errors.New("existing storage identity requires an existing drained v0.13 callback store")
	}
	releases, err := shared.InspectBoundLegacyReleaseStoreReadOnly(paths.releases)
	if err != nil {
		return fmt.Errorf("inspect release journal before storage identity initialization: %w", err)
	}
	retentionStore, err := shared.InspectBoundRetentionStoreReadOnly(paths.retention)
	if err != nil {
		return fmt.Errorf("inspect retention journal before storage identity initialization: %w", err)
	}
	if profile == backendidentity.InitializationProfileExisting &&
		(!releases.Exists || !retentionStore.Exists) {
		return errors.New("existing storage identity requires complete v0.13 callback, release, and retention journals")
	}
	containers, err := dockerClient.ListManagedContainersStrict(ctx)
	if err != nil {
		return fmt.Errorf("inspect managed containers before storage identity initialization: %w", err)
	}
	if _, err := recoveredCallbackPairs(containers); err != nil {
		return fmt.Errorf("validate managed callback cohorts before storage identity initialization: %w", err)
	}
	containersByLease := make(map[string][]ContainerInfo, len(releases.ActiveReleases))
	for _, container := range containers {
		containersByLease[container.LeaseUUID] = append(
			containersByLease[container.LeaseUUID],
			container,
		)
	}
	retentions := retentionStore.Entries
	retentionsByLease := make(map[string]*shared.RetentionEntry, len(retentions))
	for index := range retentions {
		retentionsByLease[retentions[index].OriginalLeaseUUID] = &retentions[index]
	}
	managedSet, err := attestManagedVolumeInventory(ctx, volumes)
	if err != nil {
		return fmt.Errorf("prove managed volume substrate before storage identity initialization: %w", err)
	}
	managedVolumeCountsByLease := make(map[string]int)
	for _, volumeName := range managedSet {
		managedVolumeCountsByLease[managedVolumeLeaseUUID(volumeName)]++
	}
	for _, leaseUUID := range slices.Sorted(maps.Keys(containersByLease)) {
		cohort := containersByLease[leaseUUID]
		if _, active := releases.ActiveLeaseUUIDs[leaseUUID]; active ||
			retentionsByLease[leaseUUID] != nil ||
			managedVolumeCountsByLease[leaseUUID] != 0 {
			continue
		}
		orphanProof, err := proveStoppedV013RollbackCohort(cohort, cfg.Name)
		if err != nil {
			return fmt.Errorf(
				"lease %s has managed containers but no release, retention, or managed-volume authority and is not an exact stopped v0.13 rollback cohort: %w",
				leaseUUID,
				err,
			)
		}
		encodedContainerIDs, err := json.Marshal(orphanProof.containerIDs)
		if err != nil {
			return fmt.Errorf(
				"%w: encode immutable IDs for stopped -prev cohort %s: %w",
				ErrV013OrphanRollbackRemnant,
				leaseUUID,
				err,
			)
		}
		if len(cfg.Name) == 0 || len(cfg.Name) > maxDiagnosticBackendNameBytes {
			return fmt.Errorf(
				"%w: lease %s has an exact stopped -prev cohort but its backend identity is outside the 1..%d-byte diagnostic bound",
				ErrV013OrphanRollbackRemnant,
				leaseUUID,
				maxDiagnosticBackendNameBytes,
			)
		}
		encodedBackendName, err := json.Marshal(cfg.Name)
		if err != nil {
			return fmt.Errorf(
				"%w: encode backend identity for stopped -prev cohort %s: %w",
				ErrV013OrphanRollbackRemnant,
				leaseUUID,
				err,
			)
		}
		encodedProviderUUID, err := json.Marshal(orphanProof.providerUUID)
		if err != nil {
			return fmt.Errorf(
				"%w: encode provider identity for stopped -prev cohort %s: %w",
				ErrV013OrphanRollbackRemnant,
				leaseUUID,
				err,
			)
		}
		return fmt.Errorf(
			"%w: lease %s backend=%s provider=%s has only one exact dense, identity-coherent, non-running -prev cohort and no active release, retention, or managed-volume authority; immutable_container_ids=%s. This local classification is necessary but not sufficient: do not ignore or delete any ID until placement-preflight -prove-terminal-orphan positively proves the same provider's exact lease terminal and its v0.13 placement absent. Then back up the stopped lineage, re-inspect and remove only these exact immutable IDs, rerun both read-only preflights, and take a fresh cutover backup; if terminal ownership cannot be proved, restore the complete matching v0.13 lineage instead",
			ErrV013OrphanRollbackRemnant,
			leaseUUID,
			encodedBackendName,
			encodedProviderUUID,
			encodedContainerIDs,
		)
	}
	interruptedMigrationLeases := make(map[string]struct{})
	for _, leaseUUID := range slices.Sorted(maps.Keys(releases.ActiveReleases)) {
		release := releases.ActiveReleases[leaseUUID]
		cohort := containersByLease[leaseUUID]
		if len(cohort) == 0 {
			retention := retentionsByLease[leaseUUID]
			matches, matchErr := legacyReleaseMatchesInterruptedDeprovisionRetention(&release, retention)
			if matchErr != nil {
				return fmt.Errorf(
					"compare active v0.13 release %s with retention finalizer: %w",
					leaseUUID,
					matchErr,
				)
			}
			if matches {
				return fmt.Errorf(
					"%w: lease %s has an active release, no managed containers, and a matching %s retention; restart the complete matching v0.13 lineage in isolation and replay the exact close/deprovision event or request until it purges the stale active release while preserving the retention, then drain callbacks, stop it, take a new backup, and rerun the read-only preflight",
					ErrV013InterruptedDeprovision,
					leaseUUID,
					retention.Status,
				)
			}
			if retention == nil {
				return fmt.Errorf(
					"%w: lease %s has an active release, no managed container cohort, no retention finalizer, and %d managed volumes in its exact canonical/retained namespace; this is compatible with a v0.13 crash after teardown but before retention or release finalization, and replaying deprovision can purge the release while stranding tenant data; restore the complete matching pre-close snapshot and restart its v0.13 lineage in isolation, or require height-pinned chain plus provider-inventory proof that the lease is terminal before making an explicit manual data-disposition and authority-repair decision; then stop, rerun the read-only preflight, and take a fresh backup",
					ErrV013UnresolvedClose,
					leaseUUID,
					managedVolumeCountsByLease[leaseUUID],
				)
			}
		}
		legacyCallbackCohort := make([]ContainerInfo, 0, len(cohort))
		for _, container := range cohort {
			if container.ServiceName == "" {
				legacyCallbackCohort = append(legacyCallbackCohort, container)
			}
		}
		if len(legacyCallbackCohort) > 0 {
			if _, _, callbackErr := resolveLegacyContainerCallbackURLs(legacyCallbackCohort); callbackErr != nil {
				return fmt.Errorf(
					"validate v0.13 migration callback cohort for active release %s: %w",
					leaseUUID,
					callbackErr,
				)
			}
		}
		crashClass, crashItems, crashErr := inspectV013MigrationCrashCohort(&release, cohort)
		if crashErr != nil {
			return fmt.Errorf(
				"inspect interrupted v0.13 migration cohort for active release %s: %w",
				leaseUUID,
				crashErr,
			)
		}
		if crashClass != v013MigrationCrashNone &&
			containsV013MigrationGeneratedStack(cohort) {
			switch crashClass {
			case v013MigrationCrashBeforeRelease:
				authoritylessStack := slices.DeleteFunc(
					slices.Clone(cohort),
					func(container ContainerInfo) bool {
						return !isV013MigrationGeneratedStackContainer(container)
					},
				)
				containerIDs, idErr := sortedImmutableContainerIDs(authoritylessStack)
				if idErr != nil {
					return fmt.Errorf(
						"%w: active release %s has an exact pre-RecordMigration authorityless stack generation, but its immutable-ID diagnostic is unsafe: %w",
						ErrV013InterruptedMigration,
						leaseUUID,
						idErr,
					)
				}
				encodedContainerIDs, idErr := json.Marshal(containerIDs)
				if idErr != nil {
					return fmt.Errorf(
						"%w: encode immutable IDs for pre-RecordMigration stack generation %s: %w",
						ErrV013InterruptedMigration,
						leaseUUID,
						idErr,
					)
				}
				return fmt.Errorf(
					"%w: active release %s is the exact pre-RecordMigration shape: a complete dense rollback cohort and a semantically equal complete stack generation whose v0.13 labels omit backend/provider/callback authority; immutable_stack_container_ids=%s. The initializer will not inherit those fields. Keep a full stopped backup, re-inspect each exact immutable stack container ID, stop and remove only IDs that still match this authorityless generation, leave every -prev container, bind, and volume intact, and rerun the read-only preflight. After it passes, take a fresh complete backup, seal with adopt, and start the upgraded backend; startup migration then resumes from the proven rollback-only cohort",
					ErrV013InterruptedMigration,
					leaseUUID,
					encodedContainerIDs,
				)
			case v013MigrationCrashAfterRelease:
				return fmt.Errorf(
					"%w: active release %s is the exact post-RecordMigration shape: a committed authorityless stack generation plus a partial or complete rollback-remnant cohort; desired quantity and settlement authority cannot be reconstructed from remnants. Restore a known-good complete pre-migration snapshot or use a dedicated proof-bearing repair tool; do not remove a generation, infer quantity, or seal this lineage",
					ErrV013InterruptedMigration,
					leaseUUID,
				)
			default:
				return fmt.Errorf("%w: active release %s has an unsupported migration crash class",
					ErrV013InterruptedMigration, leaseUUID)
			}
		}
		authorityItems := crashItems
		switch {
		case len(release.Items) > 0:
			authorityItems = slices.Clone(release.Items)
		case crashClass == v013MigrationCrashNone:
			authorityItems, crashErr = deriveLegacyActiveReleaseItems(&release, cohort)
		case len(authorityItems) == 0:
			crashErr = errors.New("interrupted v0.13 migration produced no workload authority")
		}
		if crashErr != nil {
			return fmt.Errorf(
				"validate managed cohort for active v0.13 release %s: %w",
				leaseUUID,
				crashErr,
			)
		}
		authorityProfiles, profileErr := resolveResourceProfilesForConfig(cfg, authorityItems)
		if profileErr != nil {
			return fmt.Errorf(
				"resolve startup release authority for active v0.13 release %s: %w",
				leaseUUID,
				profileErr,
			)
		}
		var legacyRuntimeAuthority *shared.LegacyRuntimeAuthority
		if crashClass == v013MigrationCrashNone {
			callbackURL, lifecycleCallbackURL, callbackErr := resolveLegacyContainerCallbackURLs(cohort)
			if callbackErr != nil {
				return fmt.Errorf(
					"resolve startup runtime authority for active v0.13 release %s: %w",
					leaseUUID,
					callbackErr,
				)
			}
			identity := cohort[0]
			frozen, freezeErr := shared.NewLegacyRuntimeAuthority(
				identity.Tenant,
				identity.ProviderUUID,
				callbackURL,
				lifecycleCallbackURL,
			)
			if freezeErr != nil {
				return fmt.Errorf(
					"validate startup runtime authority for active v0.13 release %s: %w",
					leaseUUID,
					freezeErr,
				)
			}
			legacyRuntimeAuthority = &frozen
		}
		var capacityErr error
		switch {
		case len(release.Items) == 0:
			authorityClass := shared.LegacyActiveAuthorityWorkload
			if crashClass != v013MigrationCrashNone {
				authorityClass = shared.LegacyActiveAuthorityMigration
			}
			if legacyRuntimeAuthority != nil {
				capacityErr = releases.CheckLegacyActiveAuthorityAndRuntimeCapacity(
					leaseUUID,
					release,
					authorityItems,
					authorityProfiles,
					authorityClass,
					*legacyRuntimeAuthority,
				)
			} else {
				capacityErr = releases.CheckLegacyActiveAuthorityCapacity(
					leaseUUID,
					release,
					authorityItems,
					authorityProfiles,
					authorityClass,
				)
			}
		case len(release.ResourceProfiles) == 0:
			capacityErr = releases.CheckActiveResourceProfilesCapacity(
				leaseUUID,
				release,
				authorityProfiles,
			)
		}
		if capacityErr != nil {
			return fmt.Errorf(
				"active v0.13 release %s cannot fit its required startup authority backfill: %w",
				leaseUUID,
				capacityErr,
			)
		}
		if crashClass != v013MigrationCrashNone {
			interruptedMigrationLeases[leaseUUID] = struct{}{}
		}
	}
	volumeEvidenceContainers := slices.DeleteFunc(
		slices.Clone(containers),
		func(container ContainerInfo) bool {
			_, interrupted := interruptedMigrationLeases[container.LeaseUUID]
			return interrupted && isLegacyRollbackRemnant(container)
		},
	)
	evidenceVolumes, err := storageIdentityContainerVolumeEvidence(cfg, volumeEvidenceContainers)
	if err != nil {
		return err
	}

	reapingLeases := make(map[string]managedVolumeEvidenceAuthority)
	for _, retention := range retentions {
		expectedVolumeNames, expectedErr := managedVolumeEvidenceAuthorityForLease(
			retention.OriginalLeaseUUID,
			retention.Items,
		)
		if expectedErr != nil {
			return fmt.Errorf(
				"derive exact managed-volume identities for retention %s: %w",
				retention.OriginalLeaseUUID,
				expectedErr,
			)
		}
		if retention.Status == shared.RetentionStatusReaping {
			reapingLeases[retention.OriginalLeaseUUID] = expectedVolumeNames
		}
		if len(retention.ResourceProfiles) == 0 {
			for _, item := range retention.Items {
				if _, err := cfg.GetSKUProfile(item.SKU); err != nil {
					return fmt.Errorf(
						"retention %s cannot resolve v0.13 SKU %q; restore the matching v0.13 SKU mapping and profile before adoption: %w",
						retention.OriginalLeaseUUID,
						item.SKU,
						err,
					)
				}
			}
		}
		if len(retention.RetainedVolumeNames) > 0 && cfg.VolumeDataPath == "" {
			return fmt.Errorf("retention %s names stateful volumes but volume_data_path is empty",
				retention.OriginalLeaseUUID)
		}
		if len(retention.RetainedVolumeNames) > 0 {
			if err := requireExistingPathUnderRoot(cfg.VolumeDataPath, cfg.VolumeDataPath); err != nil {
				return fmt.Errorf("retention %s cannot attest configured volume root: %w",
					retention.OriginalLeaseUUID, err)
			}
		}
		seenRetentionVolumes := make(map[string]struct{}, len(retention.RetainedVolumeNames))
		for _, volumeName := range retention.RetainedVolumeNames {
			managedName, parseErr := parseManagedVolumeName(volumeName)
			if parseErr != nil {
				return fmt.Errorf("retention %s contains invalid managed volume name %q: %w",
					retention.OriginalLeaseUUID, volumeName, parseErr)
			}
			if !expectedVolumeNames.containsRetained(managedName) {
				return fmt.Errorf(
					"retention %s volume %q is not an exact retained identity for its source lease items",
					retention.OriginalLeaseUUID,
					volumeName,
				)
			}
			if _, duplicate := seenRetentionVolumes[volumeName]; duplicate {
				return fmt.Errorf("retention %s contains duplicate managed volume name %q",
					retention.OriginalLeaseUUID, volumeName)
			}
			seenRetentionVolumes[volumeName] = struct{}{}
			volumePath := filepath.Join(cfg.VolumeDataPath, volumeName)
			if _, statErr := os.Lstat(volumePath); statErr != nil {
				if retention.Status == shared.RetentionStatusReaping && errors.Is(statErr, os.ErrNotExist) {
					// v0.13 destroys one reaping volume at a time and leaves the
					// original tombstone unchanged until the complete batch and
					// final Delete succeed. A retryable partial reap therefore names
					// already-destroyed volumes legitimately. Missing names explain
					// no bytes; any surviving name below still must appear in the
					// managed-volume inventory, and every unlisted managed volume is
					// rejected by the reverse cross-check.
					continue
				}
				return fmt.Errorf("retention %s volume %q is not present under configured volume root: %w",
					retention.OriginalLeaseUUID, volumeName, statErr)
			}
			if err := requireExistingPathUnderRoot(cfg.VolumeDataPath, volumePath); err != nil {
				return fmt.Errorf("retention %s volume %q is not present under configured volume root: %w",
					retention.OriginalLeaseUUID, volumeName, err)
			}
			evidenceVolumes[volumeName] = struct{}{}
		}
	}
	// A v0.13 give-up tombstone intentionally has no stored destroy list. Its
	// runtime finalizer derives the abandoned footprint from the canonical and
	// retained namespaces belonging to OriginalLeaseUUID. Use that same strict
	// namespace proof here: it preserves resumable give-up/partial-reap state,
	// while the reverse cross-check below still rejects every volume outside an
	// exact reaping lease identity. Index by UUID and scan the managed inventory
	// once so an operator-controlled 100k-row journal cannot force O(R*V) work.
	for volumeName, managedName := range managedSet {
		leaseUUID := managedVolumeLeaseUUID(managedName)
		expectedVolumeNames, explained := reapingLeases[leaseUUID]
		if !explained {
			continue
		}
		if !expectedVolumeNames.containsEither(managedName) {
			return fmt.Errorf(
				"reaping retention %s matched managed volume %q outside its exact source item identities",
				leaseUUID,
				volumeName,
			)
		}
		volumePath := filepath.Join(cfg.VolumeDataPath, volumeName)
		if err := requireExistingPathUnderRoot(cfg.VolumeDataPath, volumePath); err != nil {
			return fmt.Errorf("reaping retention %s cannot attest managed volume %q: %w",
				leaseUUID, volumeName, err)
		}
		evidenceVolumes[volumeName] = struct{}{}
	}

	switch mode {
	case StorageIdentityInitializeNew:
		if len(containers) != 0 || len(retentions) != 0 || len(managedSet) != 0 || len(releases.ActiveLeaseUUIDs) != 0 {
			return fmt.Errorf("new storage identity requires an empty backend (containers=%d retentions=%d managed_volumes=%d); use adopt for a verified v0.13 lineage",
				len(containers), len(retentions), len(managedSet))
		}
	case StorageIdentityInitializeAdopt:
		if len(containers) == 0 && len(retentions) == 0 && len(managedSet) == 0 {
			return errors.New(
				"adopt storage identity found a drained v0.13 callback outbox but no managed " +
					"containers, retentions, or volumes; rerun with -initialize-storage-identity new " +
					"only after independently confirming this is the expected empty v0.13 substrate " +
					"(not lost state) and its legacy callback outbox was fully drained",
			)
		}
		for volumeName := range evidenceVolumes {
			if _, exists := managedSet[volumeName]; !exists {
				return fmt.Errorf("adoption evidence names managed volume %q that is absent from configured root", volumeName)
			}
		}
		for volumeName := range managedSet {
			if _, explained := evidenceVolumes[volumeName]; !explained {
				return fmt.Errorf("managed volume %q has no strict live-container or retention evidence", volumeName)
			}
		}
		seenLeases := make(map[string]struct{})
		for _, container := range containers {
			if _, seen := seenLeases[container.LeaseUUID]; seen {
				continue
			}
			seenLeases[container.LeaseUUID] = struct{}{}
			if _, active := releases.ActiveLeaseUUIDs[container.LeaseUUID]; !active {
				return fmt.Errorf("managed lease %s has no active v0.13 release authority", container.LeaseUUID)
			}
		}
		for _, leaseUUID := range slices.Sorted(maps.Keys(releases.ActiveLeaseUUIDs)) {
			if _, live := seenLeases[leaseUUID]; !live {
				return fmt.Errorf("active v0.13 release %s has no managed container cohort", leaseUUID)
			}
		}
	}
	return nil
}

// legacyReleaseMatchesInterruptedDeprovisionRetention recognizes the exact
// cross-journal shape v0.13 can leave after committing a retention/volume rename
// but before purging the prior active release. It is diagnostic authority only:
// preflight always refuses the shape and never edits either journal. The
// retention inspector has already validated identity, quantities, and manifest
// topology; this comparison additionally proves that the release and retention
// carry the same normalized manifest.
func legacyReleaseMatchesInterruptedDeprovisionRetention(
	release *shared.Release,
	retention *shared.RetentionEntry,
) (bool, error) {
	if release == nil || retention == nil || release.Status != "active" ||
		(retention.Status != shared.RetentionStatusActive &&
			retention.Status != shared.RetentionStatusReaping) ||
		retention.StackManifest == nil {
		return false, nil
	}
	if release.OperationID != "" || len(release.Items) != 0 ||
		len(release.ResourceProfiles) != 0 || release.LegacyMigration {
		return false, nil
	}
	releaseManifest, err := manifest.ParsePayload(release.Manifest)
	if err != nil {
		return false, fmt.Errorf("parse legacy active release manifest: %w", err)
	}
	if release.Image != "stack" {
		legacyService := releaseManifest.Services[manifest.DefaultServiceName]
		if len(releaseManifest.Services) != 1 || legacyService == nil ||
			release.Image != legacyService.Image {
			return false, nil
		}
	}
	items := append([]backend.LeaseItem(nil), retention.Items...)
	if err := backend.NormalizeProvisionRequest(&backend.ProvisionRequest{Items: items}); err != nil {
		return false, fmt.Errorf("normalize retention items: %w", err)
	}
	if topologyMatches := manifest.ValidateStackAgainstItems(releaseManifest, items) == nil; !topologyMatches {
		return false, nil
	}
	releaseJSON, err := json.Marshal(releaseManifest)
	if err != nil {
		return false, fmt.Errorf("marshal normalized release manifest: %w", err)
	}
	retentionJSON, err := json.Marshal(retention.StackManifest)
	if err != nil {
		return false, fmt.Errorf("marshal retention manifest: %w", err)
	}
	return bytes.Equal(releaseJSON, retentionJSON), nil
}

// managedVolumeEvidenceAuthority is the exact volume-name authority one
// retained lease's item topology can grant. Physical volume presence is
// legitimately a subset: v0.13 created a host volume only when an image VOLUME
// or writable-path probe needed one. Keeping quantities as bounds, rather than
// materializing every possible name, also keeps stopped-store inspection
// proportional to journal size when a record carries a large valid quantity.
type managedVolumeEvidenceAuthority struct {
	leaseUUID         string
	serviceQuantities map[string]int
	legacyAppQuantity int
}

func managedVolumeEvidenceAuthorityForLease(
	leaseUUID string,
	items []backend.LeaseItem,
) (managedVolumeEvidenceAuthority, error) {
	if !backend.IsCanonicalLeaseUUID(leaseUUID) {
		return managedVolumeEvidenceAuthority{}, fmt.Errorf("lease UUID %q is not canonical", leaseUUID)
	}
	normalized := slices.Clone(items)
	if err := backend.NormalizeProvisionRequest(&backend.ProvisionRequest{Items: normalized}); err != nil {
		return managedVolumeEvidenceAuthority{}, err
	}
	if _, err := backend.ValidateOperationQuantities(normalized); err != nil {
		return managedVolumeEvidenceAuthority{}, err
	}

	result := managedVolumeEvidenceAuthority{
		leaseUUID:         leaseUUID,
		serviceQuantities: make(map[string]int, len(normalized)),
	}
	for _, item := range normalized {
		if !isManagedVolumeServiceName(item.ServiceName) {
			return managedVolumeEvidenceAuthority{}, fmt.Errorf(
				"service name %q is outside the managed-volume grammar",
				item.ServiceName,
			)
		}
		if _, duplicate := result.serviceQuantities[item.ServiceName]; duplicate {
			return managedVolumeEvidenceAuthority{}, fmt.Errorf(
				"duplicate service name %q",
				item.ServiceName,
			)
		}
		result.serviceQuantities[item.ServiceName] = item.Quantity
	}
	if len(normalized) == 1 && normalized[0].ServiceName == manifest.DefaultServiceName {
		// This is the only topology the pre-service on-disk form could
		// represent. Multi-service names must carry their service explicitly.
		result.legacyAppQuantity = normalized[0].Quantity
	}
	return result, nil
}

type managedVolumeEvidenceIdentity struct {
	leaseUUID   string
	serviceName string
	instance    int
	retained    bool
	legacyV013  bool
}

// managedVolumeEvidenceIdentityFromName accepts only a parsed token. Its
// slicing and integer conversion are therefore projections of grammar already
// proved by parseManagedVolumeName, never a second permissive parser.
func managedVolumeEvidenceIdentityFromName(name managedVolumeName) managedVolumeEvidenceIdentity {
	value := name.value()
	retained := strings.HasPrefix(value, retainedVolumePrefix)
	remainder := strings.TrimPrefix(value, volumePrefix)
	if retained {
		remainder = strings.TrimPrefix(value, retainedVolumePrefix)
	}
	leaseUUID, suffix := remainder[:36], remainder[37:]
	serviceName, indexText := "", suffix
	legacyV013 := true
	if dash := strings.LastIndexByte(suffix, '-'); dash >= 0 {
		serviceName, indexText = suffix[:dash], suffix[dash+1:]
		legacyV013 = false
	}
	instance, _ := strconv.Atoi(indexText)
	return managedVolumeEvidenceIdentity{
		leaseUUID:   leaseUUID,
		serviceName: serviceName,
		instance:    instance,
		retained:    retained,
		legacyV013:  legacyV013,
	}
}

func (a managedVolumeEvidenceAuthority) containsRetained(name managedVolumeName) bool {
	identity := managedVolumeEvidenceIdentityFromName(name)
	return identity.retained && a.containsIdentity(identity)
}

func (a managedVolumeEvidenceAuthority) containsEither(name managedVolumeName) bool {
	return a.containsIdentity(managedVolumeEvidenceIdentityFromName(name))
}

func (a managedVolumeEvidenceAuthority) containsIdentity(identity managedVolumeEvidenceIdentity) bool {
	if identity.leaseUUID != a.leaseUUID {
		return false
	}
	if identity.legacyV013 {
		return identity.instance < a.legacyAppQuantity
	}
	quantity, exists := a.serviceQuantities[identity.serviceName]
	return exists && identity.instance < quantity
}

// managedVolumeLeaseUUID may accept only a parsed token. parseManagedVolumeName
// proved the fixed-width canonical UUID and the complete suffix grammar, so this
// extraction cannot accidentally turn a prefix collision into lease authority.
func managedVolumeLeaseUUID(volumeName managedVolumeName) string {
	return managedVolumeEvidenceIdentityFromName(volumeName).leaseUUID
}

func reapingLeaseUUIDFromVolumeName(volumeName string) (string, bool) {
	managedName, err := parseManagedVolumeName(volumeName)
	if err != nil {
		return "", false
	}
	return managedVolumeLeaseUUID(managedName), true
}

type stoppedV013RollbackCohortProof struct {
	providerUUID string
	containerIDs []string
}

func proveStoppedV013RollbackCohort(
	containers []ContainerInfo,
	expectedBackend string,
) (stoppedV013RollbackCohortProof, error) {
	if len(containers) == 0 {
		return stoppedV013RollbackCohortProof{}, errors.New("rollback cohort is empty")
	}
	containerIDs, err := sortedImmutableContainerIDs(containers)
	if err != nil {
		return stoppedV013RollbackCohortProof{}, fmt.Errorf("immutable IDs: %w", err)
	}
	identity := containers[0]
	if !backend.IsCanonicalLeaseUUID(identity.LeaseUUID) ||
		!backend.IsCanonicalLeaseUUID(identity.ProviderUUID) ||
		strings.TrimSpace(identity.Tenant) == "" ||
		strings.TrimSpace(identity.SKU) == "" ||
		strings.TrimSpace(identity.Image) == "" ||
		identity.BackendName != expectedBackend {
		return stoppedV013RollbackCohortProof{}, errors.New(
			"rollback cohort has incomplete or foreign lease/backend/provider/tenant/SKU/image identity",
		)
	}
	if _, _, err := resolveLegacyContainerCallbackURLs(containers); err != nil {
		return stoppedV013RollbackCohortProof{}, fmt.Errorf("callback identity: %w", err)
	}
	seenIndexes := make(map[int]string, len(containers))
	for _, container := range containers {
		if !isLegacyRollbackRemnant(container) {
			return stoppedV013RollbackCohortProof{}, fmt.Errorf(
				"container %q is not an exact v0.13 rollback name",
				container.ContainerID,
			)
		}
		if !isDockerNonRunningStatus(container.Status) {
			return stoppedV013RollbackCohortProof{}, fmt.Errorf(
				"container %q is not in an explicit non-running Docker state (state %q)",
				container.ContainerID,
				container.Status,
			)
		}
		if container.LeaseUUID != identity.LeaseUUID ||
			container.BackendName != identity.BackendName ||
			container.ProviderUUID != identity.ProviderUUID ||
			container.Tenant != identity.Tenant ||
			container.SKU != identity.SKU ||
			container.Image != identity.Image ||
			container.CustomDomain != identity.CustomDomain {
			return stoppedV013RollbackCohortProof{}, fmt.Errorf(
				"container %q has divergent lease/backend/provider/tenant/SKU/image/domain identity",
				container.ContainerID,
			)
		}
		if container.InstanceIndex < 0 {
			return stoppedV013RollbackCohortProof{}, fmt.Errorf(
				"container %q has negative instance index %d",
				container.ContainerID,
				container.InstanceIndex,
			)
		}
		if prior, duplicate := seenIndexes[container.InstanceIndex]; duplicate {
			return stoppedV013RollbackCohortProof{}, fmt.Errorf(
				"containers %q and %q duplicate instance index %d",
				prior,
				container.ContainerID,
				container.InstanceIndex,
			)
		}
		seenIndexes[container.InstanceIndex] = container.ContainerID
	}
	for index := range len(containers) {
		if _, exists := seenIndexes[index]; !exists {
			return stoppedV013RollbackCohortProof{}, fmt.Errorf(
				"rollback cohort has sparse indexes; index %d is absent",
				index,
			)
		}
	}
	return stoppedV013RollbackCohortProof{
		providerUUID: identity.ProviderUUID,
		containerIDs: containerIDs,
	}, nil
}

const (
	maxDiagnosticContainerIDBytes = 128
	maxDiagnosticBackendNameBytes = 512
)

// sortedImmutableContainerIDs renders only exact Docker object handles for an
// operator cleanup. Names are intentionally excluded because they can be
// reused. Both the cohort cardinality and every opaque identifier are bounded
// before they enter an error line or runbook transcript.
func sortedImmutableContainerIDs(containers []ContainerInfo) ([]string, error) {
	if len(containers) == 0 {
		return nil, errors.New("immutable container cohort is empty")
	}
	if len(containers) > backend.MaxOperationQuantity {
		return nil, fmt.Errorf(
			"immutable container cohort contains %d containers, safe bound is %d",
			len(containers),
			backend.MaxOperationQuantity,
		)
	}
	containerIDs := make([]string, 0, len(containers))
	seen := make(map[string]struct{}, len(containers))
	for _, container := range containers {
		containerID := container.ContainerID
		if len(containerID) == 0 || len(containerID) > maxDiagnosticContainerIDBytes {
			return nil, fmt.Errorf(
				"container ID length %d is outside 1..%d bytes",
				len(containerID),
				maxDiagnosticContainerIDBytes,
			)
		}
		for _, character := range containerID {
			if (character >= 'a' && character <= 'z') ||
				(character >= 'A' && character <= 'Z') ||
				(character >= '0' && character <= '9') ||
				character == '-' || character == '_' || character == '.' || character == ':' {
				continue
			}
			return nil, fmt.Errorf("container ID contains unsafe character %U", character)
		}
		if _, duplicate := seen[containerID]; duplicate {
			return nil, fmt.Errorf("duplicate immutable container ID %q", containerID)
		}
		seen[containerID] = struct{}{}
		containerIDs = append(containerIDs, containerID)
	}
	slices.Sort(containerIDs)
	return containerIDs, nil
}

func dockerStorageInitializationProfile(
	cfg Config,
	paths *dockerStorageInitializationPaths,
	substrateID string,
	mode StorageIdentityInitializationMode,
) (backendidentity.InitializationProfile, bool, error) {
	if paths == nil || paths.markers == nil {
		return "", false, errors.New("docker storage initialization paths are required")
	}
	profile, pending, err := paths.markers.PendingInitializationProfile(cfg.Name, substrateID)
	if err != nil {
		return "", false, err
	}
	if pending {
		if profile != backendidentity.InitializationProfileFresh &&
			profile != backendidentity.InitializationProfileExisting {
			return "", false, errors.New("pending backend initialization has no store profile")
		}
		if mode == StorageIdentityInitializeAdopt && profile != backendidentity.InitializationProfileExisting {
			return "", false, errors.New("adopt mode cannot resume a fresh backend initialization")
		}
		return profile, true, nil
	}
	callbackStore, err := shared.InspectBoundCallbackStoreReadOnly(paths.callbacks)
	if err != nil {
		return "", false, err
	}
	releases, err := shared.InspectBoundLegacyReleaseStoreReadOnly(paths.releases)
	if err != nil {
		return "", false, err
	}
	retentions, err := shared.InspectBoundRetentionStoreReadOnly(paths.retention)
	if err != nil {
		return "", false, err
	}
	if callbackStore.IdentityBound || releases.IdentityBound || retentions.IdentityBound {
		return "", false, errors.New("authoritative journal is already identity-bound; restore its marker pair instead of resealing")
	}
	present := 0
	for _, exists := range []bool{callbackStore.Exists, releases.Exists, retentions.Exists} {
		if exists {
			present++
		}
	}
	switch present {
	case 0:
		if mode == StorageIdentityInitializeAdopt {
			return "", false, errors.New("adopt storage identity requires complete existing v0.13 journals")
		}
		return backendidentity.InitializationProfileFresh, false, nil
	case 3:
		return backendidentity.InitializationProfileExisting, false, nil
	default:
		return "", false, fmt.Errorf("authoritative backend journals are incomplete (%d of 3 present)", present)
	}
}

func storageIdentityContainerVolumeEvidence(
	cfg Config,
	containers []ContainerInfo,
) (map[string]struct{}, error) {
	evidenceVolumes := make(map[string]struct{})
	for _, container := range containers {
		profile, profileErr := cfg.GetSKUProfile(container.SKU)
		if profileErr != nil {
			return nil, fmt.Errorf("resolve SKU for managed container %s before storage identity initialization: %w",
				container.ContainerID, profileErr)
		}
		stateful := profile.DiskMB > 0
		if stateful && cfg.VolumeDataPath == "" {
			return nil, fmt.Errorf("managed stateful container %s exists but volume_data_path is empty",
				container.ContainerID)
		}
		for _, mount := range container.Mounts {
			if mount.Type != "bind" {
				continue
			}
			if cfg.VolumeDataPath == "" {
				return nil, fmt.Errorf("managed diskless container %s has a bind mount but volume_data_path is empty",
					container.ContainerID)
			}
			source := mount.Source
			interruptedMigrationAlternate := false
			if err := requireExistingPathUnderRoot(cfg.VolumeDataPath, source); err != nil {
				// v0.13 can stop+rename a legacy container and then rename its
				// volume parent before crashing. Docker retains the old bind source
				// string on the stopped `-prev` container even though the bytes now
				// live at the deterministic app-aware name. Admit only that exact
				// one-component substitution after proving the alternate exists.
				alternate, ok := interruptedMigrationMountSource(cfg, container, mount)
				if !ok {
					return nil, fmt.Errorf("managed container %s mount %q is not owned by configured volume root: %w",
						container.ContainerID, source, err)
				}
				source = alternate
				interruptedMigrationAlternate = true
			}
			relative, relErr := filepath.Rel(cfg.VolumeDataPath, source)
			if relErr != nil || relative == "." || relative == ".." ||
				strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
				return nil, fmt.Errorf("managed container %s mount %q has no managed volume root",
					container.ContainerID, source)
			}
			volumeName := strings.Split(relative, string(filepath.Separator))[0]
			managedName, parseErr := parseManagedVolumeName(volumeName)
			if parseErr != nil {
				return nil, fmt.Errorf(
					"managed container %s mount %q has invalid managed volume identity %q: %w",
					container.ContainerID,
					source,
					volumeName,
					parseErr,
				)
			}
			expectedName := canonicalVolumeName(
				container.LeaseUUID,
				container.ServiceName,
				container.InstanceIndex,
			)
			switch {
			case interruptedMigrationAlternate:
				expectedName = canonicalVolumeName(
					container.LeaseUUID,
					manifest.DefaultServiceName,
					container.InstanceIndex,
				)
			case container.ServiceName == "":
				expectedName = fmt.Sprintf(
					"fred-%s-%d",
					container.LeaseUUID,
					container.InstanceIndex,
				)
			}
			expectedManagedName, expectedErr := parseManagedVolumeName(expectedName)
			if expectedErr != nil {
				return nil, fmt.Errorf(
					"managed container %s labels do not derive a valid managed volume identity %q: %w",
					container.ContainerID,
					expectedName,
					expectedErr,
				)
			}
			if managedName != expectedManagedName {
				return nil, fmt.Errorf(
					"managed container %s mount %q identifies volume %q, expected exact live identity %q from its lease, service, and instance labels",
					container.ContainerID,
					source,
					managedName.value(),
					expectedManagedName.value(),
				)
			}
			if err := requireManagedVolumeMountSource(
				cfg.VolumeDataPath,
				source,
				managedName,
				mount.Target,
			); err != nil {
				return nil, fmt.Errorf(
					"managed container %s mount %q does not prove volume %q: %w",
					container.ContainerID,
					source,
					managedName.value(),
					err,
				)
			}
			evidenceVolumes[managedName.value()] = struct{}{}
		}
	}
	return evidenceVolumes, nil
}

// requireManagedVolumeMountSource binds Docker's lexical mount declaration to
// the exact managed-volume directory it claims. Global-root confinement alone
// is insufficient: volume-A/data can be a symlink to volume-B/data and still
// resolve below volume_data_path. Adoption must reject that cross-volume
// redirection, and a source subtree that fred could not have emitted for the
// declared container target.
func requireManagedVolumeMountSource(
	volumeDataPath string,
	source string,
	volumeName managedVolumeName,
	target string,
) error {
	volumeRoot := volumeName.hostPath(volumeDataPath)
	relative, err := filepath.Rel(volumeRoot, source)
	if err != nil {
		return fmt.Errorf("derive source subtree below exact managed volume root: %w", err)
	}
	if relative == "." || relative == ".." || filepath.IsAbs(relative) ||
		strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("source subtree %q is outside the exact managed volume root", relative)
	}
	targetSubtree := sanitizeVolumePath(target)
	if targetSubtree == "" {
		return fmt.Errorf("container target %q has no valid managed-volume subtree", target)
	}
	writableSubtree := filepath.Join(writablePathSubdir, targetSubtree)
	if !managedVolumeMountSubtreeMatches(relative, targetSubtree) {
		return fmt.Errorf(
			"source subtree %q does not match target-derived subtree %q or writable-path subtree %q",
			relative,
			targetSubtree,
			writableSubtree,
		)
	}
	storageRoot, err := os.OpenRoot(volumeDataPath)
	if err != nil {
		return fmt.Errorf("open configured managed volume root: %w", err)
	}
	defer func() { _ = storageRoot.Close() }()
	exactVolumeRoot, err := openAttestedManagedVolumeRoot(storageRoot, volumeName)
	if err != nil {
		return fmt.Errorf("open exact managed volume root: %w", err)
	}
	defer func() { _ = exactVolumeRoot.Close() }()
	var prefix string
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		prefix = filepath.Join(prefix, component)
		info, statErr := exactVolumeRoot.Lstat(prefix)
		if statErr != nil {
			return fmt.Errorf("stat exact source subtree %q: %w", prefix, statErr)
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("exact source subtree component %q is not a real directory", prefix)
		}
	}
	return nil
}

func managedVolumeMountSubtreeMatches(relative, targetSubtree string) bool {
	return relative == targetSubtree ||
		relative == filepath.Join(writablePathSubdir, targetSubtree)
}

func interruptedMigrationMountSource(
	cfg Config,
	container ContainerInfo,
	mount ContainerMount,
) (string, bool) {
	if !isLegacyRollbackRemnant(container) || cfg.VolumeDataPath == "" {
		return "", false
	}
	relative, err := filepath.Rel(cfg.VolumeDataPath, mount.Source)
	if err != nil || relative == "." || relative == ".." || filepath.IsAbs(relative) ||
		strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", false
	}
	parts := strings.Split(relative, string(filepath.Separator))
	oldName := fmt.Sprintf("fred-%s-%d", container.LeaseUUID, container.InstanceIndex)
	if len(parts) == 0 || parts[0] != oldName {
		return "", false
	}
	if _, err := os.Lstat(filepath.Join(cfg.VolumeDataPath, oldName)); !errors.Is(err, os.ErrNotExist) {
		// The v0.13 boundary renamed the whole parent. An existing parent,
		// dangling symlink, permission error, or I/O error is a different and
		// therefore unproved shape even if the app-aware target also exists.
		return "", false
	}
	sanitizedTarget := sanitizeVolumePath(mount.Target)
	if sanitizedTarget == "" ||
		!managedVolumeMountSubtreeMatches(filepath.Join(parts[1:]...), sanitizedTarget) {
		return "", false
	}
	parts[0] = canonicalVolumeName(
		container.LeaseUUID,
		manifest.DefaultServiceName,
		container.InstanceIndex,
	)
	alternate := filepath.Join(append([]string{cfg.VolumeDataPath}, parts...)...)
	if err := requireExistingPathUnderRoot(cfg.VolumeDataPath, alternate); err != nil {
		return "", false
	}
	return alternate, true
}

func requireExistingPathUnderRoot(rootPath, candidatePath string) error {
	root, err := filepath.Abs(filepath.Clean(rootPath))
	if err != nil {
		return fmt.Errorf("resolve configured root: %w", err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return fmt.Errorf("resolve configured root symlinks: %w", err)
	}
	candidate, err := filepath.Abs(filepath.Clean(candidatePath))
	if err != nil {
		return fmt.Errorf("resolve candidate path: %w", err)
	}
	candidate, err = filepath.EvalSymlinks(candidate)
	if err != nil {
		return fmt.Errorf("resolve candidate path symlinks: %w", err)
	}
	relative, err := filepath.Rel(root, candidate)
	if err != nil {
		return fmt.Errorf("compare candidate to configured root: %w", err)
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) ||
		filepath.IsAbs(relative) {
		return fmt.Errorf("path %s escapes configured root %s", candidate, root)
	}
	return nil
}

func verifyConfiguredVolumeMount(cfg Config) error {
	if cfg.VolumeDataPath == "" {
		return nil
	}
	mounted, err := mountinfo.Mounted(cfg.VolumeMountPath)
	if err != nil {
		return fmt.Errorf("verify configured volume mount %q: %w", cfg.VolumeMountPath, err)
	}
	if !mounted {
		return fmt.Errorf("%w: configured volume_mount_path %q is not an active mount",
			backendidentity.ErrIdentityDrift, cfg.VolumeMountPath)
	}
	if err := requireExistingPathUnderRoot(cfg.VolumeMountPath, cfg.VolumeDataPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: configured volume_data_path disappeared: %w",
				backendidentity.ErrIdentityDrift, err)
		}
		return fmt.Errorf("volume_data_path is not on configured mount: %w", err)
	}
	return nil
}

func newBackend(
	ctx context.Context,
	cfg Config,
	logger *slog.Logger,
	identityResolver dockerStorageIdentityResolver,
) (*Backend, error) {
	if ctx == nil {
		return nil, errors.New("backend construction context is required")
	}
	if logger == nil {
		return nil, errors.New("backend logger is required")
	}
	if identityResolver == nil {
		return nil, errors.New("docker storage identity resolver is required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// Resolve the retention-partition source ONCE at construction: a malformed
	// source is a startup failure, never a close-time surprise. Validate already
	// parsed it (so this cannot fail post-Validate), but re-parse and store the
	// result rather than leave the field zero — an unpopulated partitionSource
	// silently disables the feature and makes the budgets-without-source
	// misconfig undetectable.
	partitionSource, err := shared.ParsePartitionSource(cfg.RetentionPartitionSource)
	if err != nil {
		return nil, fmt.Errorf("retention_partition_source: %w", err) // unreachable post-Validate; belt-and-braces
	}

	docker, err := NewDockerClient(cfg.DockerHost, cfg.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to create Docker client: %w", err)
	}
	volumes, err := newVolumeManager(
		cfg.VolumeDataPath, cfg.VolumeFilesystem, cfg.GetMinAvgFileBytes(), logger,
	)
	if err != nil {
		_ = docker.Close()
		return nil, fmt.Errorf("failed to create volume manager: %w", err)
	}
	if err := assertVolumeDestroyer(volumes); err != nil {
		_ = docker.Close()
		return nil, err
	}
	storage, err := identityResolver.resolve(ctx, cfg, docker, volumes)
	if err != nil {
		_ = docker.Close()
		return nil, err
	}
	if !storage.Valid() {
		_ = docker.Close()
		return nil, errors.New("docker storage identity resolver returned an invalid identity")
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
		logger.Warn("max_retained_disk_mb is set but max_retained_leases_per_tenant is 0 (unlimited): one tenant can fill the retained pool, degrading others to refuse-to-retain; set a per-tenant count cap or max_retained_disk_mb_per_tenant")
	}
	if retentionCapSetButDisabled(cfg) {
		logger.Warn("retention cap/limit configured (max_retained_disk_mb / max_retained_leases_per_tenant / max_retained_disk_mb_per_tenant / retention_tenant_budgets / retention_partition_source) but retain_on_close=false: the cap has no effect; enable retain_on_close or remove the cap")
	}
	if retentionPartitionSourceWithoutBudgets(cfg) {
		logger.Warn("retention_partition_source is set but retention_tenant_budgets is empty: every declared partition collapses to the default bucket (expected only during a staged rollout)")
	}
	if retentionPartitionBudgetWithoutSource(cfg) {
		logger.Warn("a retention_tenant_budgets entry enables partitions (max_partitions > 0) but retention_partition_source is unset: partition sub-caps are dead config; set retention_partition_source or drop max_partitions from the budget")
	}
	if retentionPartitionWindowCannotRoll(cfg) {
		logger.Warn("a retention budget's per_partition_max_disk_mb is below (per_partition_max_leases+1) x the largest stateful SKU: at worst-case lease sizes the disk sub-cap binds before the count window can roll, refusing (destroying) incoming closes in a full partition; raise per_partition_max_disk_mb or lower per_partition_max_leases")
	}

	httpClient := newCallbackHTTPClient(cfg, logger)
	stopCtx, stopCancel, terminalStorageAuthorityFailure, storeAuthorityGate, err :=
		newBackendStorageAuthorityLifetime()
	if err != nil {
		_ = docker.Close()
		return nil, fmt.Errorf("construct backend storage authority lifetime: %w", err)
	}
	constructionComplete := false
	defer func() {
		if !constructionComplete {
			stopCancel()
		}
	}()
	cbStore, err := shared.OpenIdentityBoundCallbackStore(shared.CallbackStoreConfig{
		DBPath:         cfg.CallbackDBPath,
		MaxAge:         cfg.CallbackMaxAge,
		OnCleanupPanic: func(any) { background.CleanupPanicsTotal.WithLabelValues("callback").Inc() },
	}, storage, storeAuthorityGate)
	if err != nil {
		_ = docker.Close()
		return nil, fmt.Errorf("failed to open callback store: %w", err)
	}

	diagStore, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{
		DBPath:         cfg.DiagnosticsDBPath,
		MaxAge:         cfg.DiagnosticsMaxAge,
		OnCleanupPanic: func(any) { background.CleanupPanicsTotal.WithLabelValues("diagnostics").Inc() },
	})
	if err != nil {
		_ = cbStore.Close()
		_ = docker.Close()
		return nil, fmt.Errorf("failed to open diagnostics store: %w", err)
	}

	releaseStore, err := shared.OpenIdentityBoundReleaseStore(shared.ReleaseStoreConfig{
		DBPath:         cfg.ReleasesDBPath,
		MaxAge:         cfg.ReleasesMaxAge,
		OnCleanupPanic: func(any) { background.CleanupPanicsTotal.WithLabelValues("releases").Inc() },
	}, storage, storeAuthorityGate)
	if err != nil {
		_ = cbStore.Close()
		_ = diagStore.Close()
		_ = docker.Close()
		return nil, fmt.Errorf("failed to open release store: %w", err)
	}

	retentionStore, err := shared.OpenIdentityBoundRetentionStore(shared.RetentionStoreConfig{
		DBPath: cfg.RetentionDBPath,
		OnReindex: func(count int, dur time.Duration, trigger string) {
			retentionIndexReindexTotal.WithLabelValues(trigger).Inc()
			logger.Info("retention index rebuilt", "records", count, "duration", dur, "trigger", trigger)
		},
	}, storage, storeAuthorityGate)
	if err != nil {
		_ = cbStore.Close()
		_ = diagStore.Close()
		_ = releaseStore.Close()
		_ = docker.Close()
		return nil, fmt.Errorf("failed to open retention store: %w", err)
	}

	composeSvc, err := newComposeService(cfg.DockerHost)
	if err != nil {
		_ = cbStore.Close()
		_ = diagStore.Close()
		_ = releaseStore.Close()
		_ = retentionStore.Close()
		_ = docker.Close()
		return nil, fmt.Errorf("init compose service: %w", err)
	}

	b := &Backend{
		cfg:                             cfg,
		docker:                          docker,
		compose:                         composeSvc,
		pool:                            pool,
		volumes:                         volumes,
		logger:                          logger.With("backend", cfg.Name),
		partitionSource:                 partitionSource,
		provisions:                      make(map[string]*provision),
		actors:                          make(map[string]*leasesm.LeaseActor),
		callbackStore:                   cbStore,
		operationIntents:                cbStore,
		diagnosticsStore:                diagStore,
		releaseStore:                    releaseStore,
		releaseCapacityPlanner:          releaseStore,
		retentionStore:                  retentionStore,
		orphanStreaks:                   make(map[string]int),
		storageIdentity:                 storage.ID(),
		storageAuthority:                storage,
		storeAuthorityGate:              storeAuthorityGate,
		terminalStorageAuthorityFailure: terminalStorageAuthorityFailure,
		stopCtx:                         stopCtx,
		stopCancel:                      stopCancel,
		// tenantNetworkStripes is a fixed-size array embedded in Backend;
		// the zero value is ready to use (N unlocked sync.Mutexes).
	}

	// Pre-initialize the orphan-skip counter series to 0 (ENG-370): the reason
	// set is closed and known, so alert queries see 0 instead of no-data before
	// the first skip event.
	for _, r := range orphanSkipReasons {
		retentionOrphanSkipsTotal.WithLabelValues(r).Add(0)
	}

	b.storageVerifier = productionDockerStorageIdentityVerifier{backend: b, authority: storage}
	b.mutations = storageMutationAdapters{backend: b}

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
		_ = retentionStore.Close()
		_ = docker.Close()
		return nil, fmt.Errorf("configure durable callback sender: %w", err)
	}
	b.callbackSender = callbackSender

	// Wire the substrate-agnostic seams the lease state machine consumes.
	// Each adapter is a thin pass-through to the existing Docker-specific
	// methods; they exist so the SM/actor can depend on leasesm.* without
	// reaching back through *Backend at the seam.
	b.inspector = &dockerInstanceInspector{docker: b.docker}
	b.gatherer = &dockerDiagnosticsGatherer{backend: b}
	b.provisionStore = &backendProvisionStore{backend: b}

	// Gate custom-domain HTTP-01 issuance on the domain being resolvable
	// (ENG-266): don't fire an ACME order while the name is still NXDOMAIN, or a
	// negative-cache entry delays the eventual issuance. A quorum of public
	// resolvers mirrors what the ACME CA sees. The gate intentionally does NOT
	// require the domain to resolve to this backend's host_address — that is the
	// private br1 service-plane IP, not the public ingress the tenant domain
	// points at, so a host-match check could never pass on the production
	// topology and would strand every custom domain after a container recreate
	// (ENG-618). Left nil when disabled, which dnsGateAllows treats as always-ready.
	if b.cfg.Ingress.Enabled && !b.cfg.Ingress.CustomDomainDNSCheckDisabled {
		resolvers := newResolvers(b.cfg.Ingress.dnsResolvers())
		quorum := b.cfg.Ingress.dnsQuorum(len(resolvers))
		b.customDomainDNSReady = func(ctx context.Context, domain string) bool {
			cctx, cancel := context.WithTimeout(ctx, customDomainDNSCheckTimeout)
			defer cancel()
			return customDomainReadyByQuorum(cctx, resolvers, domain, quorum)
		}
	}

	constructionComplete = true
	return b, nil
}

// Start initializes the backend, recovers state, and starts background tasks.
func (b *Backend) Start(ctx context.Context) error {
	initialCtx, cancelInitial := b.recoveryDockerReadContext(ctx)
	defer cancelInitial()
	if err := b.VerifyStorageIdentity(initialCtx); err != nil {
		return err
	}
	// Verify Docker connectivity
	if err := b.docker.Ping(initialCtx); err != nil {
		return fmt.Errorf("failed to connect to Docker: %w", err)
	}

	// Validate volume manager (filesystem support, permissions)
	if err := b.volumes.Validate(); err != nil {
		return fmt.Errorf("volume manager validation failed: %w", err)
	}

	// Every mutating/convergent startup phase shares one generous but finite
	// backend-lifecycle budget, including the interrupted-volume recovery that
	// must run before Docker inventory is safe to inspect. It is deliberately
	// independent of the caller's short readiness context: main cancels that
	// context as soon as Start returns, while an idempotent recovery may
	// legitimately take longer. stopCtx cancellation still ends the whole pass.
	startupCtx, cancelStartup := b.startupRecoveryContext()
	defer cancelStartup()

	// Manager-private mutation evidence is structurally safe to inspect but is
	// not a bind-ready tenant volume. New() has already opened the identity-bound
	// journals (and therefore won their bbolt writer locks), so this is the first
	// point at which recovery may mutate it without racing a second backend
	// process. The recovery operation is deliberately unconditional and
	// idempotent: treating an arbitrary read error from RequireNoInterruptedVolumeMutations
	// as a Boolean "work exists" signal would conflate timeout/inventory failure
	// with typed recovery evidence. Recover before operation intents: an
	// unpublished XFS stage carries no tenant bytes and must be cleared, while an
	// exact unmounted ZFS child must be mounted before Docker/inventory recovery
	// can classify its owner.
	// These are filesystem/quota-only phases: their local cap must not inherit a
	// configured container stop grace. Both remain children of startupCtx, so the
	// documented aggregate startup deadline cannot be reset or exceeded here.
	recoveryCtx, cancelRecovery := startupVolumeMutationContext(startupCtx)
	err := b.mutationAdapter().recoverInterruptedVolumeMutations(recoveryCtx)
	cancelRecovery()
	if err != nil {
		return fmt.Errorf("recover interrupted managed-volume mutations: %w", err)
	}
	volumeProofCtx, cancelVolumeProof := startupVolumeMutationContext(startupCtx)
	interruptedErr := b.volumes.RequireNoInterruptedVolumeMutations(volumeProofCtx)
	var volumeProofErr error
	if interruptedErr == nil {
		_, volumeProofErr = attestManagedVolumeInventory(volumeProofCtx, b.volumes)
	}
	cancelVolumeProof()
	if interruptedErr != nil {
		return fmt.Errorf("interrupted managed-volume mutation remains after recovery: %w", interruptedErr)
	}
	if volumeProofErr != nil {
		return fmt.Errorf("managed volume substrate validation failed: %w", volumeProofErr)
	}

	// ENG-360: warn loudly if the operator over-sized the disk pool relative to
	// physical capacity (the invariant the hard-quota-sum model depends on).
	b.warnIfOverProvisioned()

	// Check daemon capabilities for hardening configuration
	b.checkDaemonCapabilities(initialCtx)

	// Recover state under a generous but finite backend-lifecycle budget, NOT
	// the caller's short startup ctx: recoverState can
	// drive legacy migration whose health-wait takes up to MigrationReadyTimeout
	// (90s) and which spawns background `-prev` grace-cleanup goroutines that
	// outlive Start's return. main cancels the 30s startup ctx the instant Start
	// returns, so binding recovery/migration to it caps the ready-wait and leaks
	// `-prev` containers (ENG-592). This matches the retention steps below, which
	// run under startupCtx. The fast connectivity/capability checks above keep
	// the caller's ctx (also capped internally) so an unreachable daemon fails
	// fast, while a daemon that wedges later cannot block Start for process life.
	if err := b.recoverState(startupCtx); err != nil {
		return fmt.Errorf("failed to recover state: %w", err)
	}
	// Classify restore substrate before retention reconciliation is allowed to
	// tear down, rename, or finalize any source record. Ambiguous partial/mixed
	// evidence must remain byte-for-byte available for operator recovery.
	preflightCtx, cancelPreflight := b.startupPhaseContext(startupCtx)
	err = b.preflightOperationIntentRecovery(preflightCtx)
	cancelPreflight()
	if err != nil {
		callbackStoreErrorsTotal.Inc()
		return fmt.Errorf("preflight interrupted operations: %w", err)
	}

	// Reconcile crash-interrupted soft-deletes and restores. MUST run AFTER
	// recoverState (so b.provisions reflects live containers) and BEFORE
	// cleanupOrphanedVolumes (so any mid-rename canonical volume is moved back
	// into the fred-retained- namespace before the orphan reaper sees it).
	retentionCtx, cancelRetention := b.startupPhaseContext(startupCtx)
	retentionReconcileErr := b.reconcileRetentions(retentionCtx)
	cancelRetention()
	if retentionReconcileErr != nil {
		b.logger.Warn("retention reconciliation failed", "error", retentionReconcileErr)
	}

	// Resolve the write-ahead window only after ordinary state and any
	// crash-interrupted restore have converged, but before quota/orphan/reaping
	// cleanup can rename or erase evidence. Partial or mixed substrate keeps the
	// intent durable and fails startup/readiness closed.
	operationCtx, cancelOperations := b.startupPhaseContext(startupCtx)
	err = b.recoverOperationIntents(operationCtx, retentionReconcileErr)
	cancelOperations()
	if err != nil {
		callbackStoreErrorsTotal.Inc()
		return fmt.Errorf("recover interrupted operations: %w", err)
	}

	// Backfill per-volume quotas onto existing volumes. Volumes provisioned
	// before the daemon held CAP_SYS_ADMIN were created untagged/un-limited;
	// once the capability is granted, this re-applies enforcement without a
	// re-provision. Every expected present volume is attempted, then any failures
	// fail startup/readiness closed: serving while even one known tenant volume
	// may be uncapped would violate the resource authority recovered above. Runs
	// after reconcileRetentions so the fred-retained- namespace matches the
	// retention records (ENG-454).
	// Uses b.stopCtx like the recovery steps above: a one-time legacy migration
	// in recoverState can push these startup steps past the caller's short ctx
	// deadline (ENG-592).
	quotaCtx, cancelQuotas := b.startupPhaseContext(startupCtx)
	err = b.reconcileVolumeQuotas(quotaCtx)
	cancelQuotas()
	if err != nil {
		return fmt.Errorf("reconcile startup volume quotas: %w", err)
	}

	// Clean up orphaned volumes (created but no matching provision).
	// Must run after recoverState so the provision map is populated. On
	// b.stopCtx (see reconcileVolumeQuotas above) so a slow migration doesn't
	// leave this running under an already-expired startup ctx (ENG-592).
	orphanCtx, cancelOrphans := b.startupPhaseContext(startupCtx)
	err = b.cleanupOrphanedVolumes(orphanCtx)
	cancelOrphans()
	if err != nil {
		return fmt.Errorf("orphaned volume cleanup failed: %w", err)
	}

	// Boot-eager reap: destroy volumes that expired while fred was offline.
	// The periodic sweep handles ongoing reaping; this catches the gap between
	// the last reap and the restart.
	reapCtx, cancelReap := b.startupPhaseContext(startupCtx)
	_, err = b.reapExpiredRetentions(reapCtx)
	cancelReap()
	if err != nil {
		b.logger.Warn("retention boot reap failed", "error", err)
	}
	// Belt-and-suspenders: recoverState already rebuilt the projection and the
	// boot reap (now wired in Step 5) self-refreshes. Startup has no prior valid
	// retained projection to preserve, so this final rebuild is fail-closed: an
	// unreadable/corrupt footprint must not leave the zero-value pool projection
	// serving traffic and over-admitting against bytes already on disk.
	if err := b.refreshRetentionAccountingChecked(); err != nil {
		return fmt.Errorf("rebuild retained resource accounting before startup: %w", err)
	}
	b.logRetentionBudgetSanity()

	// Best-effort recovery phases above deliberately continue on ordinary
	// transient errors. Identity drift is different: every guarded mutator
	// latches it and cancels stopCtx. Never report a successfully started API
	// after such a phase failed closed, and never launch workers against the
	// replacement substrate.
	finalIdentityCtx, cancelFinalIdentity := b.recoveryDockerReadContext(startupCtx)
	err = b.requireStorageIdentity(finalIdentityCtx)
	cancelFinalIdentity()
	if err != nil {
		return fmt.Errorf("storage identity lost during startup recovery: %w", err)
	}
	b.callbackStore.StartMaintenance()
	b.releaseStore.StartMaintenance()
	b.startRetentionReaper()

	// Replay callbacks on the tracked lifecycle goroutine. A Fred outage can
	// consume the full delivery retry budget, so replay must not delay backend
	// readiness. Stop cancels the sender context and waits for this goroutine
	// before closing the callback store.
	b.wg.Go(b.storageIdentityWatchLoop)
	b.wg.Go(b.callbackSender.RunReplayLoop)

	// Start periodic reconciliation (using WaitGroup.Go for Go 1.25+)
	b.wg.Go(b.reconcileLoop)

	// Start real-time container event listener for instant crash detection.
	// reconcileLoop stays as safety net for missed events.
	b.wg.Go(b.containerEventLoop)

	// Sample actor inbox depth and stuck-seconds on a ticker for the
	// fred_docker_backend_lease_actor_* observability gauges. Prime the durable
	// close gauges synchronously so an already-pending startup finalizer is
	// visible before the first periodic sample.
	b.sampleCloseIntentMetrics(time.Now())
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
	if err := b.waitForShutdownDrain(); err != nil {
		// A worker that ignored cancellation may still be inside Docker or one
		// of the durable stores. Closing those dependencies under it turns an
		// already-ambiguous mutation into data loss or a panic. Leave them open;
		// docker-backend treats this typed error as a forced process exit.
		return err
	}
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

func (b *Backend) waitForShutdownDrain() error {
	b.shutdownWaitOnce.Do(func() {
		b.shutdownWaitDone = make(chan struct{})
		go func() {
			b.wg.Wait()
			close(b.shutdownWaitDone)
		}()
	})

	timeout := cmp.Or(b.shutdownDrainTimeout, defaultShutdownDrainTimeout)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-b.shutdownWaitDone:
		return nil
	case <-timer.C:
		return fmt.Errorf(
			"%w after %s; dependencies remain open and the process must terminate or Stop must be retried after drain",
			ErrShutdownDrainTimeout,
			timeout,
		)
	}
}

// Name returns the backend name.
func (b *Backend) Name() string {
	return b.cfg.Name
}

// InitializeStorageIdentity loads an already sealed physical-storage identity.
// It never creates or repairs marker state. New installations and the first
// upgraded start must run docker-backend's explicit one-shot identity
// initialization mode before normal construction opens any durable store.
func (b *Backend) InitializeStorageIdentity(ctx context.Context) error {
	return b.loadStorageIdentity(ctx)
}

func (b *Backend) loadStorageIdentity(ctx context.Context) error {
	if b == nil {
		return errors.New("docker backend is required")
	}
	if err := b.docker.Ping(ctx); err != nil {
		return fmt.Errorf("verify Docker connectivity before identity initialization: %w", err)
	}
	if err := verifyConfiguredVolumeMount(b.cfg); err != nil {
		return err
	}
	if err := b.volumes.Validate(); err != nil {
		return fmt.Errorf("validate volume substrate before identity initialization: %w", err)
	}
	if _, err := attestManagedVolumeInventory(ctx, b.volumes); err != nil {
		return fmt.Errorf("attest managed volume substrate before identity initialization: %w", err)
	}
	if pinner, ok := b.volumes.(identityRootPinner); ok {
		if err := pinner.PinIdentityRoot(); err != nil {
			return fmt.Errorf("pin volume substrate before identity initialization: %w", err)
		}
	}
	info, err := b.docker.DaemonInfo(ctx)
	if err != nil {
		return fmt.Errorf("read Docker daemon identity: %w", err)
	}
	if strings.TrimSpace(info.SystemID) == "" {
		return errors.New("docker daemon returned an empty system ID")
	}
	return b.loadStorageIdentityForSubstrate(info.SystemID)
}

// loadStorageIdentityForSubstrate publishes or loads the marker pair against a
// caller-attested immutable Docker SystemID. Explicit adoption uses this after
// comparing the daemon before and after all read-only evidence, avoiding a
// third un-compared DaemonInfo read at the seal boundary.
func (b *Backend) loadStorageIdentityForSubstrate(substrateID string) error {
	if strings.TrimSpace(substrateID) == "" {
		return errors.New("docker daemon returned an empty system ID")
	}
	markerPath, err := b.storageIdentityMarkerPath()
	if err != nil {
		return err
	}
	if err := shared.ValidateDistinctStorePaths(map[string]string{
		"callback": b.cfg.CallbackDBPath, "diagnostics": b.cfg.DiagnosticsDBPath,
		"releases": b.cfg.ReleasesDBPath, "retention": b.cfg.RetentionDBPath,
		"primary marker": markerPath, "anchor marker": b.storageIdentityAnchorPath(),
	}); err != nil {
		return err
	}
	authority, err := backendidentity.LoadVerifiedMarkerPair(
		markerPath, b.storageIdentityAnchorPath(), b.cfg.Name, substrateID,
	)
	if err != nil {
		return fmt.Errorf("load Docker backend storage identity: %w", err)
	}
	if pinner, ok := b.volumes.(identityRootPinner); ok {
		if err := pinner.VerifyIdentityRoot(); err != nil {
			return fmt.Errorf("revalidate volume substrate after loading identity: %w", err)
		}
	}
	if err := verifyDockerAuthoritativeStoreSet(b.cfg, authority); err != nil {
		return fmt.Errorf("verify Docker authoritative stores: %w", err)
	}
	b.storageAuthority = authority
	b.storageIdentity = authority.ID()
	return nil
}

func verifyDockerAuthoritativeStoreSet(cfg Config, storage backendidentity.VerifiedStorage) error {
	if err := shared.VerifyCallbackStoreStorage(cfg.CallbackDBPath, storage); err != nil {
		return err
	}
	if err := shared.VerifyReleaseStoreStorage(cfg.ReleasesDBPath, storage); err != nil {
		return err
	}
	return shared.VerifyRetentionStoreStorage(cfg.RetentionDBPath, storage)
}

func verifyBoundDockerAuthoritativeStoreSet(
	paths *dockerStorageInitializationPaths,
	storage backendidentity.VerifiedStorage,
) error {
	if paths == nil {
		return errors.New("docker storage initialization paths are required")
	}
	if err := shared.VerifyBoundCallbackStoreStorage(paths.callbacks, storage); err != nil {
		return err
	}
	if err := shared.VerifyBoundReleaseStoreStorage(paths.releases, storage); err != nil {
		return err
	}
	return shared.VerifyBoundRetentionStoreStorage(paths.retention, storage)
}

func (b *Backend) storageIdentityAnchorPath() string {
	return dockerStorageIdentityAnchorPath(b.cfg)
}

func (b *Backend) storageIdentityMarkerPath() (string, error) {
	return dockerStorageIdentityMarkerPath(b.cfg)
}

func dockerStorageIdentityAnchorPath(cfg Config) string {
	return filepath.Clean(cfg.CallbackDBPath) + ".storage-identity-anchor.json"
}

func dockerStorageIdentityMarkerPath(cfg Config) (string, error) {
	markerPath := filepath.Clean(cfg.CallbackDBPath) + ".storage-identity.json"
	if cfg.VolumeDataPath != "" {
		root := filepath.Clean(cfg.VolumeDataPath)
		rootInfo, statErr := os.Stat(root)
		if statErr != nil {
			return "", fmt.Errorf("stat backend volume data root before identity verification: %w", statErr)
		}
		if !rootInfo.IsDir() {
			return "", fmt.Errorf("backend volume data root is not a directory: %s", root)
		}
		markerPath = filepath.Join(root, backendidentity.MarkerFilename)
	}
	return markerPath, nil
}

// VerifyStorageIdentity re-attests the marker and current Docker daemon before
// any request can observe or mutate backend state. It never creates a missing
// marker, so a runtime remount/replacement fails closed.
func (b *Backend) VerifyStorageIdentity(ctx context.Context) error {
	if b == nil || b.storageVerifier == nil {
		return errors.New("docker backend storage identity is not initialized")
	}
	if err := b.terminalStorageAuthorityError(); err != nil {
		return err
	}
	if identity := b.storageVerifier.StorageIdentity(); !identity.Valid() || identity != b.storageIdentity {
		return errors.New("docker backend storage verifier returned a divergent or invalid identity")
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
		return errors.New("docker backend verified storage authority is invalid")
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
	if err := verifyConfiguredVolumeMount(b.cfg); err != nil {
		wrapped := fmt.Errorf("verify Docker storage mount: %w", err)
		if errors.Is(err, backendidentity.ErrIdentityDrift) {
			return b.latchIdentityVerificationFailureLocked(wrapped)
		}
		return wrapped
	}
	if pinner, ok := b.volumes.(identityRootPinner); ok {
		if err := pinner.VerifyIdentityRoot(); err != nil {
			wrapped := fmt.Errorf("verify Docker volume root identity: %w", err)
			if errors.Is(err, errVolumeRootIdentityDrift) {
				wrapped = fmt.Errorf("%w: %w", backendidentity.ErrIdentityDrift, wrapped)
				return b.latchIdentityVerificationFailureLocked(wrapped)
			}
			return wrapped
		}
	}
	info, err := b.docker.DaemonInfo(ctx)
	if err != nil {
		return fmt.Errorf("revalidate Docker daemon identity: %w", err)
	}
	if strings.TrimSpace(info.SystemID) == "" {
		return errors.New("docker daemon returned an empty system ID")
	}
	markerPath, err := b.storageIdentityMarkerPath()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return b.latchIdentityVerificationFailureLocked(fmt.Errorf(
				"%w: Docker storage identity root disappeared: %w",
				backendidentity.ErrIdentityDrift, err,
			))
		}
		return err
	}
	if err := backendidentity.VerifyMarkerPair(
		markerPath, b.storageIdentityAnchorPath(), b.cfg.Name, info.SystemID, b.storageIdentity,
	); err != nil {
		wrapped := fmt.Errorf("verify Docker backend storage identity: %w", err)
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
		"retention": func() error {
			if b.retentionStore == nil {
				return errors.New("retention store is missing")
			}
			return b.retentionStore.VerifyStorageIdentity(storage)
		},
	} {
		if err := verify(); err != nil {
			wrapped := fmt.Errorf("%w: verify Docker %s store identity: %w",
				backendidentity.ErrIdentityDrift, name, err)
			_ = b.latchIdentityVerificationFailureLocked(wrapped)
			return wrapped
		}
	}
	return nil
}

func (b *Backend) requireStorageIdentity(ctx context.Context) error {
	if b == nil {
		return errors.New("docker backend is required")
	}
	if b.storageVerifier == nil {
		return errors.New("docker backend storage verifier is required")
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

// StorageIdentity returns the initialized immutable storage identity. Its zero
// value means production initialization has not completed.
func (b *Backend) StorageIdentity() backendidentity.ID {
	if b == nil {
		return backendidentity.ID{}
	}
	return b.storageIdentity
}

// TerminalStorageAuthorityFailure reports the first backend-lifetime storage
// failure that requires a fresh process to re-open durable stores and run
// startup recovery. The receive-only channel yields at most one error and is
// never closed. A nil channel means this Backend was assembled by an isolated
// test rather than the production constructor and therefore disables the
// corresponding select case.
func (b *Backend) TerminalStorageAuthorityFailure() <-chan error {
	if b == nil {
		return nil
	}
	return b.terminalStorageAuthorityFailure
}

// Health checks that the Docker daemon is reachable AND the persistence stores
// are readable. Probing the bbolt stores (not just docker.Ping) means a
// locked/corrupt/read-only retention or release store surfaces as unhealthy
// instead of the backend reporting healthy while soft-delete/restore silently
// fail — the most data-loss-sensitive subsystem must not be the unmonitored
// one. (ENG-448 / F31)
func (b *Backend) Health(ctx context.Context) error {
	if err := b.requireStorageIdentity(ctx); err != nil {
		return fmt.Errorf("backend storage identity unhealthy: %w", err)
	}
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

// sendOperationCallback resolves the exact operation callback URL from the
// provisions map and delegates to sendOperationCallbackWithURL. Use this when
// the provision is still in the map.
func (b *Backend) sendOperationCallback(leaseUUID string, status backend.CallbackStatus, errMsg string) {
	b.provisionsMu.RLock()
	var callbackURL string
	if prov, ok := b.provisions[leaseUUID]; ok {
		callbackURL = prov.CallbackURL
	}
	b.provisionsMu.RUnlock()

	b.sendOperationCallbackWithURL(leaseUUID, callbackURL, status, errMsg)
}

// sendOperationCallbackWithURL dispatches an exact requested-operation
// completion using a caller-provided URL.
func (b *Backend) sendOperationCallbackWithURL(leaseUUID, callbackURL string, status backend.CallbackStatus, errMsg string) {
	// Truncate error to fit the on-chain rejection reason limit.
	if len(errMsg) > callbackMaxErrorLen {
		errMsg = errMsg[:callbackMaxErrorLen-3] + "..."
	}

	b.callbackSender.SendOperationCallback(leaseUUID, callbackURL, b.Name(), status, errMsg)
}

// sendLifecycleCallbackWithURL dispatches a typed, observation-only
// lifecycle callback. retained is true only for successful retained teardown.
func (b *Backend) sendLifecycleCallbackWithURL(leaseUUID, callbackURL string, status backend.CallbackStatus, errMsg string, retained bool) {
	// Truncate error to fit the on-chain rejection reason limit.
	if len(errMsg) > callbackMaxErrorLen {
		errMsg = errMsg[:callbackMaxErrorLen-3] + "..."
	}

	b.callbackSender.SendLifecycleCallback(leaseUUID, callbackURL, b.Name(), status, errMsg, retained)
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
