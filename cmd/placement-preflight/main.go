// Command placement-preflight creates the placement database of a provider
// that has never run. Explicit --initialize-fresh mode proves that the provider
// has no chain lease history and that every backend reports empty inventories,
// takes the operator's attestation that the provider and every backend are
// quiesced, and then publishes a new placement authority. It binds complete
// backend evidence to a height-pinned signer-free chain snapshot.
// --print-fresh-confirmation prints the attestation that mode expects and
// touches nothing. --inspect-releases is an offline, read-only scan of a
// stopped backend's releases.db.
//
// It does not convert a v0.13 placement database. The 0.14 line's
// placement-preflight does; a provider upgrades through the final 0.14 release.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/placementprobe"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

const (
	defaultProofTimeout       = 2 * time.Minute
	insecureChainConfirmation = "I ACCEPT UNAUTHENTICATED CHAIN EVIDENCE FOR LOCAL DEVELOPMENT"
	freshSuccessVerdict       = "INITIALIZED_FOR_CUTOVER"
)

var version = "dev"

// errPreflightInitialized marks every failure that happens after the fresh
// placement database was published.
var errPreflightInitialized = errors.New("placement preflight initialization published")

// durablePreflightFailure reports a failure after the fresh placement database
// was published. The database exists, so the run must not be treated as rolled
// back.
type durablePreflightFailure struct {
	stage         string
	cause         error
	reportingOnly bool
}

func (failure *durablePreflightFailure) Error() string {
	consequence := "the durable result must be inspected and must not be treated as rolled back"
	if failure.reportingOnly {
		consequence = "the database was durably written and closed; only command reporting is indeterminate"
	}
	return fmt.Sprintf(
		"INITIALIZED: placement authority changed before %s failed; %s. Keep providerd stopped, "+
			"do not retry blindly, and run placement-repair -classify with the same "+
			"providerd config immediately: %v",
		failure.stage, consequence, failure.cause,
	)
}

func (failure *durablePreflightFailure) Unwrap() []error {
	return []error{errPreflightInitialized, failure.cause}
}

func newDurablePreflightFailure(stage string, cause error) error {
	return &durablePreflightFailure{stage: stage, cause: cause}
}

func newDurablePreflightVerdictFailure(cause error) error {
	return &durablePreflightFailure{
		stage:         "complete cutover verdict reporting",
		cause:         cause,
		reportingOnly: true,
	}
}

type inventoryClient = placementprobe.Client

type providerLeaseSnapshot interface {
	placement.FreshChainSnapshot
	BlockingLeaseUUIDs() []string
}

type freshChainClient interface {
	SnapshotProviderLeases(
		context.Context,
		string,
	) (providerLeaseSnapshot, error)
	Close() error
}

type commandDependencies struct {
	loadConfig           func(string) (*config.Config, error)
	newFreshChainClient  func(chain.ReadOnlyClientConfig) (freshChainClient, error)
	initializeFreshStore func(context.Context, *placement.FreshInitializationPlan) error
}

type readOnlyFreshChainClient struct{ client *chain.ReadOnlyClient }

func (client readOnlyFreshChainClient) SnapshotProviderLeases(
	ctx context.Context,
	providerUUID string,
) (providerLeaseSnapshot, error) {
	snapshot, err := client.client.SnapshotProviderLeases(ctx, providerUUID)
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (client readOnlyFreshChainClient) Close() error { return client.client.Close() }

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		writeCommandError(os.Stderr, err)
		os.Exit(1)
	}
}

// writeCommandError renders the complete causal chain as one JSON-quoted
// physical line. Errors may contain remote response bodies, newlines, or
// terminal controls; none may manufacture an unprefixed operator verdict.
func writeCommandError(stderr io.Writer, err error) {
	encoded, marshalErr := json.Marshal(err.Error())
	if marshalErr != nil {
		encoded = []byte(`"error text could not be encoded"`)
	}
	_, _ = fmt.Fprintf(stderr, "placement-preflight: ERROR %s\n", encoded)
}

func run(
	ctx context.Context,
	args []string,
	stdout io.Writer,
	stderr io.Writer,
) error {
	return runWithDependencies(ctx, args, stdout, stderr, defaultCommandDependencies())
}

func runWithDependencies(
	ctx context.Context,
	args []string,
	stdout io.Writer,
	stderr io.Writer,
	dependencies commandDependencies,
) error {
	flags := flag.NewFlagSet("placement-preflight", flag.ContinueOnError)
	// Flag parsing includes the rejected value in its automatic diagnostic.
	// Suppress that unstructured write; main renders the returned error through
	// writeCommandError, which cannot be used to forge a verdict line.
	flags.SetOutput(io.Discard)
	configPath := flags.String("config", "", "path to the stopped providerd configuration (required except with -inspect-releases)")
	inspectReleases := flags.String("inspect-releases", "", "offline read-only scan of a stopped backend releases.db for current manifest admission policy violations; writes a JSON report")
	proofTimeout := flags.Duration(
		"proof-timeout",
		defaultProofTimeout,
		"remote inventory/chain proof and cancellable validation timeout; the fresh-initialization proof is hard-capped at two minutes; excludes file open/copy/fsync/commit/close and output",
	)
	initializeFresh := flags.Bool("initialize-fresh", false, "prove an unused provider/backend fleet and atomically create a new placement database")
	printFreshConfirmation := flags.Bool(
		"print-fresh-confirmation",
		false,
		"print the exact target-bound fresh-initialization acknowledgement and exit without remote access or mutation",
	)
	expectedBackends := flags.String(
		"expected-backends",
		"",
		"independently supplied exact backend-name roster as a JSON array (required with -initialize-fresh or -print-fresh-confirmation)",
	)
	confirmQuiesced := flags.String("confirm-quiesced", "", "exact quiescence acknowledgement required with -initialize-fresh")
	confirmInsecureChain := flags.String(
		"confirm-insecure-chain",
		"",
		"exact development-only acknowledgement required for unauthenticated chain gRPC",
	)
	showVersion := flags.Bool("version", false, "print version and exit")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			flags.SetOutput(stderr)
			flags.Usage()
			return nil
		}
		return err
	}
	flags.SetOutput(stderr)
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if *showVersion {
		if _, err := fmt.Fprintln(stdout, version); err != nil {
			return fmt.Errorf("write version: %w", err)
		}
		return nil
	}
	if *inspectReleases != "" {
		var incompatible string
		flags.Visit(func(f *flag.Flag) {
			if f.Name != "inspect-releases" {
				incompatible = f.Name
			}
		})
		if incompatible != "" {
			return fmt.Errorf("-inspect-releases cannot be combined with -%s", incompatible)
		}
		report, err := shared.InspectReleaseAdmissionReadOnly(*inspectReleases)
		if err != nil {
			return err
		}
		if err := json.NewEncoder(stdout).Encode(report); err != nil {
			return fmt.Errorf("write release admission report: %w", err)
		}
		return nil
	}
	if *configPath == "" {
		flags.Usage()
		return fmt.Errorf("-config is required")
	}
	if *proofTimeout <= 0 {
		return fmt.Errorf("-proof-timeout must be positive")
	}
	if !*initializeFresh && !*printFreshConfirmation {
		return fmt.Errorf(
			"a mode is required: -initialize-fresh, -print-fresh-confirmation, -inspect-releases or -version",
		)
	}
	if *initializeFresh && *printFreshConfirmation {
		return fmt.Errorf("-initialize-fresh and -print-fresh-confirmation are mutually exclusive")
	}
	if !*initializeFresh && *confirmQuiesced != "" {
		return fmt.Errorf("-confirm-quiesced is only valid with -initialize-fresh")
	}
	if *initializeFresh && *expectedBackends == "" {
		return fmt.Errorf("-expected-backends is required with -initialize-fresh")
	}
	if *printFreshConfirmation && *expectedBackends == "" {
		return fmt.Errorf("-expected-backends is required with -print-fresh-confirmation")
	}
	if *printFreshConfirmation && *confirmInsecureChain != "" {
		return fmt.Errorf("-confirm-insecure-chain is not valid with -print-fresh-confirmation because that mode makes no chain request")
	}

	cfg, err := dependencies.loadConfig(*configPath)
	if err != nil {
		return fmt.Errorf("load providerd config: %w", err)
	}
	if err := requireCanonicalProviderUUID(cfg.ProviderUUID); err != nil {
		return err
	}
	var mutationFleet placementprobe.AuthenticatedFleet
	if *initializeFresh {
		mutationFleet, err = placementprobe.NewAuthenticatedFleet(cfg)
		if err != nil {
			return err
		}
	}
	if *printFreshConfirmation {
		expectedRoster, err := parseExpectedBackendRoster(*expectedBackends)
		if err != nil {
			return err
		}
		target, err := placement.NewFreshInitializationTarget(
			cfg.PlacementStoreDBPath, cfg.ProviderUUID, expectedRoster,
		)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintln(stdout, target.Confirmation()); err != nil {
			return fmt.Errorf("write fresh-initialization confirmation: %w", err)
		}
		return nil
	}
	if err := requireAuthenticatedChainProofTransport(cfg, *confirmInsecureChain); err != nil {
		return err
	}
	expectedRoster, err := parseExpectedBackendRoster(*expectedBackends)
	if err != nil {
		return err
	}
	target, err := placement.NewFreshInitializationTarget(
		cfg.PlacementStoreDBPath, cfg.ProviderUUID, expectedRoster,
	)
	if err != nil {
		return err
	}
	quiescenceProof, err := placement.ConfirmFreshQuiescence(target, *confirmQuiesced)
	if err != nil {
		return err
	}
	return runFreshInitialization(
		ctx, *proofTimeout, cfg, mutationFleet, target, quiescenceProof, stdout, dependencies,
	)
}

func requireCanonicalProviderUUID(providerUUID string) error {
	parsed, err := uuid.Parse(providerUUID)
	if err != nil || parsed == uuid.Nil || parsed.String() != providerUUID {
		return fmt.Errorf(
			"%w: configured provider UUID %q is not canonical",
			placement.ErrProviderAuthorityMismatch,
			providerUUID,
		)
	}
	return nil
}

func requireAuthenticatedChainProofTransport(
	cfg *config.Config,
	insecureConfirmation string,
) error {
	if cfg == nil {
		return fmt.Errorf("provider config is required")
	}
	verifiedTLS := cfg.GRPCTLSEnabled && !cfg.GRPCTLSSkipVerify
	if verifiedTLS {
		if insecureConfirmation != "" {
			return fmt.Errorf(
				"-confirm-insecure-chain is only valid when chain gRPC is unauthenticated",
			)
		}
		return nil
	}
	if insecureConfirmation != insecureChainConfirmation {
		return fmt.Errorf(
			"unauthenticated chain evidence requires an explicit local-development operator "+
				"attestation; configure certificate-verified chain gRPC or set "+
				"-confirm-insecure-chain %q",
			insecureChainConfirmation,
		)
	}
	return nil
}

func defaultCommandDependencies() commandDependencies {
	return commandDependencies{
		loadConfig: config.Load,
		newFreshChainClient: func(cfg chain.ReadOnlyClientConfig) (freshChainClient, error) {
			client, err := chain.NewReadOnlyClient(cfg)
			if err != nil {
				return nil, err
			}
			return readOnlyFreshChainClient{client: client}, nil
		},
		initializeFreshStore: placement.InitializeFreshStoreContext,
	}
}

func runFreshInitialization(
	ctx context.Context,
	proofTimeout time.Duration,
	cfg *config.Config,
	fleet placementprobe.AuthenticatedFleet,
	target placement.FreshInitializationTarget,
	quiescenceProof placement.FreshQuiescenceProof,
	stdout io.Writer,
	dependencies commandDependencies,
) error {
	if err := requireFreshTargetAbsent(target.DatabasePath()); err != nil {
		return err
	}

	freshCtx, cancel := context.WithTimeout(ctx, proofTimeout)
	defer cancel()
	clients, err := fleet.NewClients()
	if err != nil {
		return err
	}
	inventories, err := collectInventories(freshCtx, clients)
	if err != nil {
		return err
	}
	backendNames := configuredBackendNames(cfg)
	backendProof, err := placement.NewFreshBackendProof(backendNames, inventories)
	if err != nil {
		return err
	}

	snapshot, err := snapshotProviderLeases(freshCtx, cfg, dependencies.newFreshChainClient)
	if err != nil {
		return err
	}
	chainProof, err := placement.NewFreshChainProof(snapshot)
	if err != nil {
		return err
	}
	if snapshot.ProviderUUID() != cfg.ProviderUUID {
		return fmt.Errorf(
			"%w: chain snapshot belongs to %q, configured provider is %q",
			placement.ErrProviderAuthorityMismatch,
			snapshot.ProviderUUID(),
			cfg.ProviderUUID,
		)
	}
	plan, err := placement.NewFreshInitializationPlan(
		freshCtx, target, chainProof, backendProof, quiescenceProof,
	)
	if err != nil {
		return err
	}
	if err := dependencies.initializeFreshStore(freshCtx, plan); err != nil {
		if errors.Is(err, placement.ErrFreshInitializationPublished) {
			return newDurablePreflightFailure("post-publication durability verification", err)
		}
		return err
	}

	var output bytes.Buffer
	if err := writeFreshTarget(&output, target); err != nil {
		return err
	}
	if err := writeBackendTopology(&output, backendNames); err != nil {
		return fmt.Errorf("render fresh initialization topology: %w", err)
	}
	if err := writeBackendStorageIdentities(&output, inventories); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(&output,
		"%s: fresh placement database initialized; chain_height=%d total_leases=0 backends=%d\n",
		freshSuccessVerdict, snapshot.BlockHeight(), len(backendNames),
	); err != nil {
		return fmt.Errorf("render fresh initialization verdict: %w", err)
	}
	if err := writeCompleteVerdict(stdout, output.Bytes()); err != nil {
		return newDurablePreflightVerdictFailure(err)
	}
	return nil
}

func requireFreshTargetAbsent(dbPath string) error {
	if _, err := os.Lstat(dbPath); err == nil {
		return fmt.Errorf("%w: %s", placement.ErrPlacementStoreExists, dbPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect placement database destination: %w", err)
	}
	return nil
}

func parseExpectedBackendRoster(encoded string) ([]string, error) {
	var backendNames []string
	if err := json.Unmarshal([]byte(encoded), &backendNames); err != nil {
		return nil, fmt.Errorf("-expected-backends must be one JSON string array: %w", err)
	}
	if backendNames == nil {
		return nil, fmt.Errorf("-expected-backends must be a concrete nonempty JSON array")
	}
	return backendNames, nil
}

func writeFreshTarget(
	output io.Writer,
	target placement.FreshInitializationTarget,
) error {
	targetPath, err := json.Marshal(target.DatabasePath())
	if err != nil {
		return fmt.Errorf("encode fresh placement target: %w", err)
	}
	providerUUID, err := json.Marshal(target.ProviderUUID())
	if err != nil {
		return fmt.Errorf("encode fresh placement provider: %w", err)
	}
	backendNames, err := json.Marshal(target.BackendNames())
	if err != nil {
		return fmt.Errorf("encode fresh placement roster: %w", err)
	}
	if _, err := fmt.Fprintf(
		output,
		"fresh_target %s\nfresh_provider %s\nexpected_backend_roster %s\n",
		targetPath,
		providerUUID,
		backendNames,
	); err != nil {
		return fmt.Errorf("render fresh placement target: %w", err)
	}
	return nil
}

func snapshotProviderLeases(
	ctx context.Context,
	cfg *config.Config,
	newClient func(chain.ReadOnlyClientConfig) (freshChainClient, error),
) (_ providerLeaseSnapshot, resultErr error) {
	client, err := newClient(chain.ReadOnlyClientConfig{
		Endpoint:       cfg.GRPCEndpoint,
		TLSEnabled:     cfg.GRPCTLSEnabled,
		TLSCAFile:      cfg.GRPCTLSCAFile,
		TLSSkipVerify:  cfg.GRPCTLSSkipVerify,
		QueryPageLimit: cfg.QueryPageLimit,
	})
	if err != nil {
		return nil, fmt.Errorf("open read-only chain client: %w", err)
	}
	if client == nil {
		return nil, fmt.Errorf("open read-only chain client: factory returned nil client")
	}
	defer func() {
		if closeErr := client.Close(); resultErr == nil && closeErr != nil {
			resultErr = fmt.Errorf("close read-only chain client: %w", closeErr)
		}
	}()
	snapshot, err := client.SnapshotProviderLeases(ctx, cfg.ProviderUUID)
	if err != nil {
		return nil, fmt.Errorf("snapshot provider leases: %w", err)
	}
	return snapshot, nil
}

func configuredBackendNames(cfg *config.Config) []string {
	backendNames := make([]string, 0, len(cfg.Backends))
	for _, backendConfig := range cfg.Backends {
		backendNames = append(backendNames, backendConfig.Name)
	}
	slices.Sort(backendNames)
	return backendNames
}

func writeBackendStorageIdentities(
	stdout io.Writer,
	inventories map[string]placement.BackendInventory,
) error {
	names := slices.Sorted(maps.Keys(inventories))
	for _, backendName := range names {
		encodedName, err := json.Marshal(backendName)
		if err != nil {
			return fmt.Errorf("encode preflight backend name: %w", err)
		}
		if _, err := fmt.Fprintf(stdout, "backend_storage_id %s=%s\n",
			encodedName, inventories[backendName].StorageIdentity); err != nil {
			return fmt.Errorf("write preflight storage identity: %w", err)
		}
	}
	return nil
}

func writeBackendTopology(stdout io.Writer, backendNames []string) error {
	encoded, err := json.Marshal(backendNames)
	if err != nil {
		return fmt.Errorf("encode backend topology: %w", err)
	}
	if _, err := fmt.Fprintf(stdout, "backend_topology %s\n", encoded); err != nil {
		return fmt.Errorf("write backend topology: %w", err)
	}
	return nil
}

// writeCompleteVerdict makes one writer call. The stable mode-specific verdict
// is rendered as the final line by callers, so a short write cannot leave
// complete authorization after only auxiliary output. A writer that reports an
// error after accepting every byte still delivered the complete verdict and is
// treated as successful.
func writeCompleteVerdict(stdout io.Writer, rendered []byte) error {
	written, err := stdout.Write(rendered)
	if written == len(rendered) {
		return nil
	}
	if err == nil {
		err = io.ErrShortWrite
	}
	return fmt.Errorf("write complete preflight verdict (%d/%d bytes): %w",
		written, len(rendered), err)
}

func collectInventories(
	ctx context.Context,
	clients []inventoryClient,
) (map[string]placement.BackendInventory, error) {
	collected, err := placementprobe.Collect(ctx, clients)
	if err != nil {
		return nil, err
	}
	inventories := make(map[string]placement.BackendInventory, len(collected))
	for backendName, inventory := range collected {
		provisions := make([]string, 0, len(inventory.Provisions))
		provisionProviders := make(map[string]string, len(inventory.Provisions))
		for _, provision := range inventory.Provisions {
			provisions = append(provisions, provision.LeaseUUID)
			provisionProviders[provision.LeaseUUID] = provision.ProviderUUID
		}
		retentions := make([]string, 0, len(inventory.Retentions))
		for _, retention := range inventory.Retentions {
			retentions = append(retentions, retention.LeaseUUID)
		}
		inventories[backendName] = placement.BackendInventory{
			StorageIdentity:        inventory.StorageIdentity,
			Provisions:             provisions,
			ProvisionProviderUUIDs: provisionProviders,
			Retentions:             retentions,
		}
	}
	return inventories, nil
}
