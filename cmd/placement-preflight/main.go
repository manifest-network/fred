// Command placement-preflight verifies that a stopped v0.13 placement database
// is eligible for Fred's one-time lifecycle and storage-identity migration. It
// binds complete backend evidence to a height-pinned signer-free chain snapshot.
// Its default mode is read-only. Explicit --prepare mode holds an exclusive
// lock, publishes a required backup, then seals the verified database atomically.
// Explicit --initialize-fresh mode instead proves that chain and every backend
// are quiescent and empty before publishing a new placement authority.
// Explicit --prove-terminal-orphan mode holds the stopped legacy database
// read-only and combines exact row absence with positive terminal chain evidence.
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

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/placementprobe"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

const (
	defaultProofTimeout       = 2 * time.Minute
	insecureChainConfirmation = "I ACCEPT UNAUTHENTICATED CHAIN EVIDENCE FOR LOCAL DEVELOPMENT"
	inspectSuccessVerdict     = "READY_TO_PREPARE"
	prepareSuccessVerdict     = "PREPARED_FOR_CUTOVER"
	freshSuccessVerdict       = "INITIALIZED_FOR_CUTOVER"
	terminalOrphanVerdict     = "TERMINAL_ORPHAN_PROVED"
)

var version = "dev"

type durablePreflightStatus uint8

const (
	preflightNotMutated durablePreflightStatus = iota
	preflightPreparationOutcomeUnknown
	preflightPrepared
	preflightInitialized
)

var (
	errPreflightPrepared       = errors.New("placement preflight preparation committed")
	errPreflightInitialized    = errors.New("placement preflight initialization published")
	errPreflightOutcomeUnknown = errors.New(
		"placement preflight preparation outcome is unknown",
	)
)

func (status durablePreflightStatus) label() string {
	switch status {
	case preflightPreparationOutcomeUnknown:
		return "OUTCOME UNKNOWN"
	case preflightPrepared:
		return "PREPARED"
	case preflightInitialized:
		return "INITIALIZED"
	default:
		return ""
	}
}

func (status durablePreflightStatus) sentinel() error {
	switch status {
	case preflightPreparationOutcomeUnknown:
		return errPreflightOutcomeUnknown
	case preflightPrepared:
		return errPreflightPrepared
	case preflightInitialized:
		return errPreflightInitialized
	default:
		return nil
	}
}

type durablePreflightFailure struct {
	status        durablePreflightStatus
	stage         string
	cause         error
	reportingOnly bool
}

func (failure *durablePreflightFailure) Error() string {
	if failure.status == preflightPreparationOutcomeUnknown {
		return fmt.Sprintf(
			"OUTCOME UNKNOWN: bbolt Commit returned an error before %s; the live placement "+
				"authority may or may not contain the preparation. Keep providerd stopped, "+
				"preserve both the live database and exact backup, do not retry or restore "+
				"blindly, and run placement-repair -classify with the same providerd config "+
				"immediately: %v",
			failure.stage,
			failure.cause,
		)
	}
	consequence := "the durable result must be inspected and must not be treated as rolled back"
	if failure.reportingOnly {
		consequence = "the database was durably written and closed; only command reporting is indeterminate"
	}
	return fmt.Sprintf(
		"%s: placement authority changed before %s failed; %s. Keep providerd stopped, "+
			"do not retry blindly, and run placement-repair -classify with the same "+
			"providerd config immediately: %v",
		failure.status.label(), failure.stage, consequence, failure.cause,
	)
}

func (failure *durablePreflightFailure) Unwrap() []error {
	return []error{failure.status.sentinel(), failure.cause}
}

func newDurablePreflightFailure(
	status durablePreflightStatus,
	stage string,
	cause error,
) error {
	return &durablePreflightFailure{status: status, stage: stage, cause: cause}
}

func newDurablePreflightVerdictFailure(
	status durablePreflightStatus,
	cause error,
) error {
	return &durablePreflightFailure{
		status:        status,
		stage:         "complete cutover verdict reporting",
		cause:         cause,
		reportingOnly: true,
	}
}

type inventoryClient = placementprobe.Client

type providerLeaseSnapshot interface {
	placement.ProviderLeaseMembershipSnapshot
	BlockingLeaseUUIDs() []string
}

type freshChainClient interface {
	SnapshotProviderLeases(
		context.Context,
		string,
	) (providerLeaseSnapshot, error)
	Close() error
}

type legacyUpgradeInspector interface {
	CheckContext(
		context.Context,
		string,
		[]string,
		map[string]placement.BackendInventory,
		placement.LegacyUpgradeChainProof,
	) (placement.LegacyUpgradePreflightSummary, error)
	ProveTerminalOrphanContext(
		context.Context,
		string,
		placement.TerminalOrphanChainProof,
	) (placement.TerminalOrphanProofSummary, error)
	Close() error
}

type legacyUpgradePreparer interface {
	AuthorizePreparation(
		context.Context,
		string,
		[]string,
		map[string]placement.BackendInventory,
		placement.LegacyUpgradeChainProof,
		*placement.ExactBackupTarget,
		string,
	) (placement.LegacyPreparationCapability, error)
	PrepareContext(
		context.Context,
		string,
		[]string,
		map[string]placement.BackendInventory,
		placement.LegacyUpgradeChainProof,
		placement.LegacyPreparationCapability,
	) (placement.LegacyUpgradePreflightSummary, error)
	Close() error
}

type preparedAuthorityInspector interface {
	VerifyLegacyPreparationPostcondition(
		string,
		[]string,
		map[string]placement.BackendInventory,
		placement.LegacyUpgradePreflightSummary,
	) error
	Close() error
}

type commandDependencies struct {
	loadConfig                 func(string) (*config.Config, error)
	newInventoryClients        func(*config.Config) ([]inventoryClient, error)
	newFreshChainClient        func(chain.ReadOnlyClientConfig) (freshChainClient, error)
	openLegacyUpgradeInspector func(string) (legacyUpgradeInspector, error)
	openLegacyUpgradePreparer  func(string) (legacyUpgradePreparer, error)
	openPreparedAuthority      func(string, string) (preparedAuthorityInspector, error)
	bindExactBackupTarget      func(string) (*placement.ExactBackupTarget, error)
	initializeFreshStore       func(context.Context, *placement.FreshInitializationPlan) error
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
) (runErr error) {
	flags := flag.NewFlagSet("placement-preflight", flag.ContinueOnError)
	// Flag parsing includes the rejected value in its automatic diagnostic.
	// Suppress that unstructured write; main renders the returned error through
	// writeCommandError, which cannot be used to forge a verdict line.
	flags.SetOutput(io.Discard)
	configPath := flags.String("config", "", "path to the stopped providerd configuration (required)")
	proofTimeout := flags.Duration(
		"proof-timeout",
		defaultProofTimeout,
		"remote inventory/chain proof and cancellable validation timeout; fresh/legacy capabilities are hard-capped at two minutes; excludes file open/copy/fsync/commit/close and output",
	)
	prepare := flags.Bool("prepare", false, "atomically back up and seal the verified v0.13 database for the identity-bound upgrade")
	backupPath := flags.String("backup", "", "new backup path required with -prepare (must not already exist)")
	attestDrained := flags.String(
		"attest-drained",
		"",
		"exact causal-drain acknowledgement required with -prepare",
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
	proveTerminalOrphan := flags.String(
		"prove-terminal-orphan",
		"",
		"prove that one canonical terminal lease has no v0.13 placement row (read-only; requires -expected-backend)",
	)
	expectedBackend := flags.String(
		"expected-backend",
		"",
		"exact configured backend expected to own the local stopped Docker remnant (required with -prove-terminal-orphan)",
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
	if *configPath == "" {
		flags.Usage()
		return fmt.Errorf("-config is required")
	}
	if *proofTimeout <= 0 {
		return fmt.Errorf("-proof-timeout must be positive")
	}
	terminalFlagSet := make(map[string]bool, 2)
	flags.Visit(func(parsed *flag.Flag) {
		if parsed.Name == "prove-terminal-orphan" || parsed.Name == "expected-backend" {
			terminalFlagSet[parsed.Name] = true
		}
	})
	proveTerminalOrphanSet := terminalFlagSet["prove-terminal-orphan"]
	expectedBackendSet := terminalFlagSet["expected-backend"]
	terminalOrphanMode := proveTerminalOrphanSet || expectedBackendSet
	if proveTerminalOrphanSet != expectedBackendSet {
		return fmt.Errorf("-prove-terminal-orphan and -expected-backend must be supplied together")
	}
	if terminalOrphanMode && (*proveTerminalOrphan == "" || *expectedBackend == "") {
		return fmt.Errorf("-prove-terminal-orphan and -expected-backend must both be non-empty")
	}
	if terminalOrphanMode && (*prepare || *initializeFresh || *printFreshConfirmation) {
		return fmt.Errorf(
			"-prove-terminal-orphan is mutually exclusive with -prepare, -initialize-fresh, and -print-fresh-confirmation",
		)
	}
	if terminalOrphanMode {
		parsedLeaseUUID, err := uuid.Parse(*proveTerminalOrphan)
		if err != nil || parsedLeaseUUID == uuid.Nil || parsedLeaseUUID.String() != *proveTerminalOrphan {
			return fmt.Errorf(
				"%w: -prove-terminal-orphan must be one canonical non-nil UUID",
				placement.ErrTerminalOrphanProof,
			)
		}
	}
	if *initializeFresh && *printFreshConfirmation {
		return fmt.Errorf("-initialize-fresh and -print-fresh-confirmation are mutually exclusive")
	}
	if *initializeFresh && *prepare {
		return fmt.Errorf("-initialize-fresh and -prepare are mutually exclusive")
	}
	if *printFreshConfirmation && *prepare {
		return fmt.Errorf("-print-fresh-confirmation and -prepare are mutually exclusive")
	}
	if *initializeFresh && *backupPath != "" {
		return fmt.Errorf("-backup cannot be used with -initialize-fresh")
	}
	if *printFreshConfirmation && *backupPath != "" {
		return fmt.Errorf("-backup cannot be used with -print-fresh-confirmation")
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
	if !*initializeFresh && !*printFreshConfirmation && *expectedBackends != "" {
		return fmt.Errorf("-expected-backends is only valid with -initialize-fresh or -print-fresh-confirmation")
	}
	if *printFreshConfirmation && *confirmInsecureChain != "" {
		return fmt.Errorf("-confirm-insecure-chain is not valid with -print-fresh-confirmation because that mode makes no chain request")
	}
	if *prepare && *backupPath == "" {
		return fmt.Errorf("-backup is required with -prepare")
	}
	if *prepare && *attestDrained != placement.LegacyPreparationDrainAttestation {
		return fmt.Errorf(
			"-attest-drained must exactly equal %q with -prepare",
			placement.LegacyPreparationDrainAttestation,
		)
	}
	if !*prepare && *backupPath != "" {
		return fmt.Errorf("-backup is only valid with -prepare")
	}
	if !*prepare && *attestDrained != "" {
		return fmt.Errorf("-attest-drained is only valid with -prepare")
	}

	cfg, err := dependencies.loadConfig(*configPath)
	if err != nil {
		return fmt.Errorf("load providerd config: %w", err)
	}
	if err := requireCanonicalProviderUUID(cfg.ProviderUUID); err != nil {
		return err
	}
	var boundBackupTarget *placement.ExactBackupTarget
	if *prepare {
		boundBackupTarget, err = dependencies.bindExactBackupTarget(*backupPath)
		if err != nil {
			return err
		}
		defer func() {
			if closeErr := boundBackupTarget.Close(); closeErr != nil {
				closeErr = fmt.Errorf("close bound exact backup target: %w", closeErr)
				if runErr == nil {
					runErr = closeErr
				} else {
					runErr = errors.Join(runErr, closeErr)
				}
			}
		}()
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
	if terminalOrphanMode {
		if !slices.Contains(configuredBackendNames(cfg), *expectedBackend) {
			return fmt.Errorf(
				"-expected-backend %q is not an exact configured backend name",
				*expectedBackend,
			)
		}
		return runTerminalOrphanProof(
			ctx,
			*proofTimeout,
			cfg,
			*proveTerminalOrphan,
			*expectedBackend,
			stdout,
			dependencies,
		)
	}
	if *initializeFresh {
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
			ctx, *proofTimeout, cfg, target, quiescenceProof, stdout, dependencies,
		)
	}
	var closePlacement func() error
	var inspector legacyUpgradeInspector
	var preparer legacyUpgradePreparer
	if *prepare {
		preparer, err = dependencies.openLegacyUpgradePreparer(cfg.PlacementStoreDBPath)
		if err != nil {
			return err
		}
		closePlacement = preparer.Close
	} else {
		inspector, err = dependencies.openLegacyUpgradeInspector(cfg.PlacementStoreDBPath)
		if err != nil {
			return err
		}
		closePlacement = inspector.Close
	}
	placementClosed := false
	closePlacementOnce := func() error {
		if placementClosed {
			return nil
		}
		placementClosed = true
		return closePlacement()
	}
	durableStatus := preflightNotMutated
	defer func() {
		closeErr := closePlacementOnce()
		if closeErr == nil {
			return
		}
		if durableStatus != preflightNotMutated {
			closeFailure := newDurablePreflightFailure(
				durableStatus, "database close verification", closeErr,
			)
			if runErr == nil {
				runErr = closeFailure
			} else {
				runErr = errors.Join(runErr, closeFailure)
			}
			return
		}
		if runErr == nil {
			runErr = fmt.Errorf("close placement database: %w", closeErr)
		} else {
			runErr = errors.Join(runErr, fmt.Errorf("close placement database: %w", closeErr))
		}
	}()

	clients, err := dependencies.newInventoryClients(cfg)
	if err != nil {
		return err
	}
	preflightCtx, cancel := context.WithTimeout(ctx, *proofTimeout)
	defer cancel()
	inventories, err := collectInventories(preflightCtx, clients)
	if err != nil {
		return err
	}
	chainSnapshot, err := snapshotProviderLeases(
		preflightCtx, cfg, dependencies.newFreshChainClient,
	)
	if err != nil {
		return err
	}
	chainProof, err := placement.NewLegacyUpgradeChainProof(chainSnapshot)
	if err != nil {
		return err
	}

	backendNames := configuredBackendNames(cfg)
	var summary placement.LegacyUpgradePreflightSummary
	if *prepare {
		capability, capabilityErr := preparer.AuthorizePreparation(
			preflightCtx,
			cfg.ProviderUUID,
			backendNames,
			inventories,
			chainProof,
			boundBackupTarget,
			*attestDrained,
		)
		if capabilityErr != nil {
			return capabilityErr
		}
		summary, err = preparer.PrepareContext(
			preflightCtx,
			cfg.ProviderUUID,
			backendNames,
			inventories,
			chainProof,
			capability,
		)
	} else {
		summary, err = inspector.CheckContext(
			preflightCtx, cfg.ProviderUUID, backendNames, inventories, chainProof,
		)
	}
	if err != nil {
		if errors.Is(err, placement.ErrExactBackupPublished) {
			return fmt.Errorf(
				"BACKUP PUBLISHED: exact legacy backup %q exists, but its final validation "+
					"failed before any preparation transaction committed. Preserve it, "+
					"rerun the read-only proof, and choose a new -backup path: %w",
				boundBackupTarget.Path(),
				err,
			)
		}
		if errors.Is(err, placement.ErrLegacyPreparationOutcomeUnknown) {
			durableStatus = preflightPreparationOutcomeUnknown
			return newDurablePreflightFailure(
				durableStatus, "commit outcome could be established", err,
			)
		}
		if errors.Is(err, placement.ErrLegacyPreparationCommitted) {
			durableStatus = preflightPrepared
			return newDurablePreflightFailure(
				durableStatus, "explicit database sync verification", err,
			)
		}
		return err
	}
	if *prepare {
		durableStatus = preflightPrepared
	}
	// A preparation verdict is actionable operator output. Close first, render
	// every auxiliary line before the mode-specific final verdict, then issue
	// exactly one writer call so partial output cannot authorize a cutover.
	if err := closePlacementOnce(); err != nil {
		if durableStatus != preflightNotMutated {
			return newDurablePreflightFailure(
				durableStatus, "database close verification", err,
			)
		}
		return fmt.Errorf("close placement database: %w", err)
	}
	if *prepare {
		reopened, openErr := dependencies.openPreparedAuthority(
			cfg.PlacementStoreDBPath, cfg.ProviderUUID,
		)
		if openErr != nil {
			return newDurablePreflightFailure(
				durableStatus, "database reopen physical/schema verification", openErr,
			)
		}
		verifyErr := reopened.VerifyLegacyPreparationPostcondition(
			cfg.ProviderUUID, backendNames, inventories, summary,
		)
		closeErr := reopened.Close()
		if verifyErr != nil {
			if closeErr != nil {
				verifyErr = errors.Join(verifyErr, fmt.Errorf("close reopened database: %w", closeErr))
			}
			return newDurablePreflightFailure(
				durableStatus, "reopened database semantic verification", verifyErr,
			)
		}
		if closeErr != nil {
			return newDurablePreflightFailure(
				durableStatus, "reopened database close verification", closeErr,
			)
		}
		if err := boundBackupTarget.VerifyPublished(); err != nil {
			return newDurablePreflightFailure(
				durableStatus, "final exact-backup authority verification", err,
			)
		}
	}
	verdict := "database remained read-only"
	verdictKind := inspectSuccessVerdict
	if *prepare {
		verdict = fmt.Sprintf("database prepared; exact legacy backup: %q", boundBackupTarget.Path())
		verdictKind = prepareSuccessVerdict
	}
	var output bytes.Buffer
	if err := writeBackendStorageIdentities(&output, inventories); err != nil {
		return err
	}
	if *prepare {
		if _, err := fmt.Fprintln(&output,
			"WARNING: old-binary rollback now requires restoring the backup; never restore it after upgraded side effects begin."); err != nil {
			return fmt.Errorf("write preparation warning: %w", err)
		}
	}
	if _, err := fmt.Fprintf(&output,
		"%s: v0.13 placement preflight verified %d rows against %d leases on %d backends; %s\n",
		verdictKind, summary.PlacementRows, summary.InventoryLeases,
		summary.ConfiguredBackends, verdict,
	); err != nil {
		return fmt.Errorf("render preflight verdict: %w", err)
	}
	if err := writeCompleteVerdict(stdout, output.Bytes()); err != nil {
		if durableStatus != preflightNotMutated {
			return newDurablePreflightVerdictFailure(durableStatus, err)
		}
		return fmt.Errorf("write preflight verdict: %w", err)
	}
	return nil
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
		loadConfig:          config.Load,
		newInventoryClients: newInventoryClients,
		openLegacyUpgradeInspector: func(path string) (legacyUpgradeInspector, error) {
			return placement.OpenLegacyUpgradeInspector(path)
		},
		openLegacyUpgradePreparer: func(path string) (legacyUpgradePreparer, error) {
			return placement.OpenLegacyUpgradePreparer(path)
		},
		openPreparedAuthority: func(path, providerUUID string) (preparedAuthorityInspector, error) {
			return placement.OpenPreparedAuthorityInspector(path, providerUUID)
		},
		bindExactBackupTarget: placement.BindExactBackupTarget,
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
	clients, err := dependencies.newInventoryClients(cfg)
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
			return newDurablePreflightFailure(
				preflightInitialized, "post-publication durability verification", err,
			)
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
		return newDurablePreflightVerdictFailure(preflightInitialized, err)
	}
	return nil
}

func runTerminalOrphanProof(
	ctx context.Context,
	proofTimeout time.Duration,
	cfg *config.Config,
	leaseUUID string,
	expectedBackend string,
	stdout io.Writer,
	dependencies commandDependencies,
) (runErr error) {
	inspector, err := dependencies.openLegacyUpgradeInspector(cfg.PlacementStoreDBPath)
	if err != nil {
		return err
	}
	closed := false
	closeInspector := func() error {
		if closed {
			return nil
		}
		closed = true
		return inspector.Close()
	}
	defer func() {
		if closeErr := closeInspector(); closeErr != nil {
			closeErr = fmt.Errorf("close placement database: %w", closeErr)
			if runErr == nil {
				runErr = closeErr
			} else {
				runErr = errors.Join(runErr, closeErr)
			}
		}
	}()

	proofCtx, cancel := context.WithTimeout(ctx, proofTimeout)
	defer cancel()
	snapshot, err := snapshotProviderLeases(
		proofCtx,
		cfg,
		dependencies.newFreshChainClient,
	)
	if err != nil {
		return err
	}
	chainProof, err := placement.NewTerminalOrphanChainProof(
		snapshot,
		cfg.ProviderUUID,
		leaseUUID,
	)
	if err != nil {
		return err
	}
	summary, err := inspector.ProveTerminalOrphanContext(
		proofCtx,
		expectedBackend,
		chainProof,
	)
	if err != nil {
		return err
	}

	// This is one necessary input to a separate exact-ID Docker cleanup, never
	// sufficient authority by itself. Close the stopped placement file before
	// rendering it, then publish the complete verdict through one writer call.
	if err := closeInspector(); err != nil {
		return fmt.Errorf("close placement database: %w", err)
	}
	encodedLeaseUUID, err := json.Marshal(summary.LeaseUUID)
	if err != nil {
		return fmt.Errorf("encode terminal orphan lease UUID: %w", err)
	}
	encodedBackend, err := json.Marshal(summary.ExpectedBackend)
	if err != nil {
		return fmt.Errorf("encode terminal orphan backend name: %w", err)
	}
	encodedProviderUUID, err := json.Marshal(summary.ProviderUUID)
	if err != nil {
		return fmt.Errorf("encode terminal orphan provider UUID: %w", err)
	}
	verdict := fmt.Appendf(
		nil,
		"%s: lease=%s backend=%s provider=%s chain_height=%d placement=absent\n",
		terminalOrphanVerdict,
		encodedLeaseUUID,
		encodedBackend,
		encodedProviderUUID,
		summary.ChainHeight,
	)
	if err := writeCompleteVerdict(stdout, verdict); err != nil {
		return fmt.Errorf("write terminal orphan verdict: %w", err)
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

func newInventoryClients(cfg *config.Config) ([]inventoryClient, error) {
	return placementprobe.NewClients(cfg)
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
		provisionItems := make(map[string][]backend.LeaseItem, len(inventory.Provisions))
		for _, provision := range inventory.Provisions {
			provisions = append(provisions, provision.LeaseUUID)
			provisionProviders[provision.LeaseUUID] = provision.ProviderUUID
			provisionItems[provision.LeaseUUID] = slices.Clone(provision.Items)
		}
		retentions := make([]string, 0, len(inventory.Retentions))
		for _, retention := range inventory.Retentions {
			retentions = append(retentions, retention.LeaseUUID)
		}
		inventories[backendName] = placement.BackendInventory{
			StorageIdentity:        inventory.StorageIdentity,
			Provisions:             provisions,
			ProvisionProviderUUIDs: provisionProviders,
			ProvisionItems:         provisionItems,
			Retentions:             retentions,
		}
	}
	return inventories, nil
}
