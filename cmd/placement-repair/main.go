// Command placement-repair provides read-only offline placement discovery and
// deliberately narrow mutation paths for one exact attempt or conflict. It
// never infers causal non-execution from inventory absence: mutation also
// requires an exact operator drain attestation and target-bound confirmation.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/placementprobe"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

const (
	defaultRepairTimeout = 2 * time.Minute
	drainedAttestation   = placement.DrainAttestationText
)

var version = "dev"

var (
	errRepairCommitted      = errors.New("placement repair mutation committed")
	errRepairOutcomeUnknown = errors.New("placement repair mutation outcome is unknown")
)

type repairPostconditionInspector interface {
	VerifyRefusalPostcondition(
		placement.AttemptRepairCandidate,
		placement.AttemptRepairResult,
	) error
	VerifyConflictResolutionPostcondition(
		placement.ConflictRepairCandidate,
		placement.ConflictRepairResult,
	) error
	Close() error
}

type commandDependencies struct {
	bindExactBackupTarget      func(string) (*placement.ExactBackupTarget, error)
	createExactBackup          func(*placement.AttemptRepair, *placement.ExactBackupTarget) error
	openPostconditionInspector func(string, string) (repairPostconditionInspector, error)
	inspectAuthorityFile       func(
		string,
		placement.AuthorityExpectation,
	) (placement.AuthorityReport, error)
}

func defaultCommandDependencies() commandDependencies {
	return commandDependencies{
		bindExactBackupTarget: placement.BindExactBackupTarget,
		createExactBackup: func(
			repair *placement.AttemptRepair,
			target *placement.ExactBackupTarget,
		) error {
			return repair.CreateExactBackup(target)
		},
		openPostconditionInspector: func(path, providerUUID string) (repairPostconditionInspector, error) {
			return placement.OpenRepairInspector(path, providerUUID)
		},
		inspectAuthorityFile: placement.InspectAuthorityFile,
	}
}

type committedRepairFailure struct {
	stage         string
	cause         error
	reportingOnly bool
}

func (failure *committedRepairFailure) Error() string {
	consequence := "the durable result must be inspected and must not be treated as rolled back"
	if failure.reportingOnly {
		consequence = "the database was synced and closed; only command reporting is indeterminate"
	}
	return fmt.Sprintf(
		"COMMITTED: the placement repair transaction succeeded before %s failed; %s. "+
			"Keep providerd stopped and "+
			"run placement-repair -inspect immediately before any retry or restore: %v",
		failure.stage,
		consequence,
		failure.cause,
	)
}

func (failure *committedRepairFailure) Unwrap() []error {
	return []error{errRepairCommitted, failure.cause}
}

func newCommittedRepairFailure(stage string, cause error) error {
	return &committedRepairFailure{stage: stage, cause: cause}
}

func newCommittedVerdictFailure(cause error) error {
	return &committedRepairFailure{
		stage:         "PASS verdict reporting",
		cause:         cause,
		reportingOnly: true,
	}
}

type outcomeUnknownRepairFailure struct {
	stage string
	cause error
}

func (failure *outcomeUnknownRepairFailure) Error() string {
	return fmt.Sprintf(
		"OUTCOME UNKNOWN: bbolt Commit returned an error while %s; the placement repair "+
			"may or may not be visible. Keep providerd stopped, preserve the live database "+
			"and exact backup, and run placement-repair -inspect immediately. Do not retry "+
			"or restore blindly: %v",
		failure.stage,
		failure.cause,
	)
}

func (failure *outcomeUnknownRepairFailure) Unwrap() []error {
	return []error{errRepairOutcomeUnknown, failure.cause}
}

func newOutcomeUnknownRepairFailure(stage string, cause error) error {
	return &outcomeUnknownRepairFailure{stage: stage, cause: cause}
}

type publishedRepairBackupError struct {
	path  string
	cause error
}

func (failure *publishedRepairBackupError) Error() string {
	return fmt.Sprintf(
		"BACKUP PUBLISHED: exact pre-repair backup %q exists, but a later proof or "+
			"pre-commit step failed and no repair mutation committed. Preserve it, rerun "+
			"dry-run, and choose a new -backup path after repeating the proof: %v",
		failure.path,
		failure.cause,
	)
}

func (failure *publishedRepairBackupError) Unwrap() []error {
	return []error{placement.ErrExactBackupPublished, failure.cause}
}

func publishedRepairBackupFailure(path string, cause error) error {
	return &publishedRepairBackupError{path: path, cause: cause}
}

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
	_, _ = fmt.Fprintf(stderr, "placement-repair: ERROR %s\n", encoded)
}

func run(
	ctx context.Context,
	args []string,
	stdout io.Writer,
	stderr io.Writer,
) (runErr error) {
	return runWithDependencies(
		ctx, args, stdout, stderr, defaultCommandDependencies(),
	)
}

func runWithDependencies(
	ctx context.Context,
	args []string,
	stdout io.Writer,
	stderr io.Writer,
	dependencies commandDependencies,
) (runErr error) {
	flags := flag.NewFlagSet("placement-repair", flag.ContinueOnError)
	// Flag parsing includes the rejected value in its automatic diagnostic.
	// Suppress that unstructured write; main renders the returned error through
	// writeCommandError, which cannot be used to forge a verdict line.
	flags.SetOutput(io.Discard)
	configPath := flags.String("config", "", "path to the stopped providerd configuration (required)")
	leaseUUID := flags.String("lease", "", "exact canonical lease UUID")
	backendName := flags.String("backend", "", "exact attempted or selected backend name")
	operationText := flags.String("operation-id", "", "exact canonical attempted operation UUIDv4")
	classifyAuthority := flags.Bool("classify", false, "classify the stopped authority across the v0.13 preparation boundary (offline)")
	listRecords := flags.Bool("list", false, "list every durable placement row read-only (offline)")
	inspectRecord := flags.Bool("inspect", false, "inspect the exact -lease row read-only (offline)")
	resolveConflict := flags.Bool("resolve-conflict", false, "resolve one exact durable conflict to -backend")
	timeout := flags.Duration(
		"timeout",
		defaultRepairTimeout,
		"inventory-evidence freshness deadline through mutation admission; does not cancel blocking backup I/O",
	)
	apply := flags.Bool("apply", false, "apply the exact repair; default is dry-run")
	backupPath := flags.String("backup", "", "new exact pre-mutation backup path required with -apply (must not already exist)")
	confirmation := flags.String("confirm", "", "exact tuple-bound confirmation value required with -apply")
	attestation := flags.String("attest-drained", "", "exact delayed-effects/callback drain attestation required with -apply")
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
	timeoutExplicit := false
	flags.Visit(func(visited *flag.Flag) {
		if visited.Name == "timeout" {
			timeoutExplicit = true
		}
	})
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
	modeCount := 0
	for _, selected := range []bool{*classifyAuthority, *listRecords, *inspectRecord, *resolveConflict} {
		if selected {
			modeCount++
		}
	}
	if modeCount > 1 {
		return fmt.Errorf("-classify, -list, -inspect, and -resolve-conflict are mutually exclusive")
	}
	if timeoutExplicit && *classifyAuthority {
		return fmt.Errorf("read-only -classify does not accept -timeout")
	}
	if timeoutExplicit && (*listRecords || *inspectRecord) {
		return fmt.Errorf("read-only -list/-inspect does not accept -timeout")
	}
	if *timeout <= 0 {
		return fmt.Errorf("-timeout must be positive")
	}
	if *apply && *backupPath == "" {
		return fmt.Errorf("-backup is required with -apply; no mutation was attempted")
	}
	if !*apply && *backupPath != "" {
		return fmt.Errorf("-backup requires -apply; dry-run remains nonmutating")
	}
	if !*apply && (*confirmation != "" || *attestation != "") {
		return fmt.Errorf("-confirm and -attest-drained require -apply; no mutation was attempted")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load providerd config: %w", err)
	}
	if err := requireCanonicalProviderUUID(cfg.ProviderUUID); err != nil {
		return err
	}
	var boundBackupTarget *placement.ExactBackupTarget
	if *apply {
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
	if *classifyAuthority {
		if *apply || *confirmation != "" || *attestation != "" ||
			*backupPath != "" || *leaseUUID != "" || *backendName != "" ||
			*operationText != "" || *resolveConflict {
			return fmt.Errorf("read-only -classify cannot be combined with record or mutation flags")
		}
		expectation, expectationErr := placement.NewAuthorityExpectation(
			cfg.ProviderUUID, backendNames(cfg),
		)
		if expectationErr != nil {
			return expectationErr
		}
		return runAuthorityClassification(
			ctx, cfg.PlacementStoreDBPath, expectation, stdout,
			dependencies.inspectAuthorityFile,
		)
	}
	if *listRecords || *inspectRecord {
		if *apply || *confirmation != "" || *attestation != "" ||
			*backupPath != "" || *backendName != "" || *operationText != "" || *resolveConflict {
			return fmt.Errorf("read-only -list/-inspect cannot be combined with mutation flags")
		}
		if *listRecords && *leaseUUID != "" {
			return fmt.Errorf("-list does not accept -lease; use -inspect")
		}
		if *inspectRecord && *leaseUUID == "" {
			return fmt.Errorf("-inspect requires -lease")
		}
		return runInspection(
			cfg.PlacementStoreDBPath, cfg.ProviderUUID, *leaseUUID, *listRecords, stdout,
		)
	}
	if *leaseUUID == "" || *backendName == "" {
		flags.Usage()
		return fmt.Errorf("-lease and -backend are required")
	}
	if *resolveConflict {
		if *operationText != "" {
			return fmt.Errorf("-resolve-conflict does not accept -operation-id")
		}
	} else if *operationText == "" {
		return fmt.Errorf("-operation-id is required for attempt refusal")
	}

	repair, err := placement.OpenAttemptRepair(cfg.PlacementStoreDBPath, cfg.ProviderUUID)
	if err != nil {
		return err
	}
	repairClosed := false
	closeRepair := func() error {
		if repairClosed {
			return nil
		}
		repairClosed = true
		return repair.Close()
	}
	mutationCommitted := false
	mutationOutcomeUnknown := false
	defer func() {
		closeErr := closeRepair()
		if closeErr == nil {
			return
		}
		if mutationCommitted {
			closeFailure := newCommittedRepairFailure("database close verification", closeErr)
			if runErr == nil {
				runErr = closeFailure
			} else {
				runErr = errors.Join(runErr, closeFailure)
			}
			return
		}
		if mutationOutcomeUnknown {
			closeFailure := newOutcomeUnknownRepairFailure(
				"closing the database after the indeterminate commit", closeErr,
			)
			if runErr == nil {
				runErr = closeFailure
			} else {
				runErr = errors.Join(runErr, closeFailure)
			}
			return
		}
		if runErr == nil {
			runErr = fmt.Errorf("close placement repair: %w", closeErr)
		}
	}()

	configuredBackends := backendNames(cfg)
	canonicalConfigured := slices.Sorted(slices.Values(configuredBackends))
	if !slices.Equal(canonicalConfigured, repair.BackendTopology()) {
		return fmt.Errorf(
			"provider config backend topology %q does not exactly match durable topology %q",
			canonicalConfigured, repair.BackendTopology(),
		)
	}

	if *resolveConflict {
		candidate, matchErr := repair.MatchConflict(*leaseUUID, *backendName)
		if matchErr != nil {
			return matchErr
		}
		clients, clientsErr := placementprobe.NewIdentityBoundClients(cfg, repair)
		if clientsErr != nil {
			return clientsErr
		}
		probeCtx, cancel := context.WithTimeout(ctx, *timeout)
		defer cancel()
		inventories, collectErr := placementprobe.Collect(probeCtx, clients)
		if collectErr != nil {
			return collectErr
		}
		facts, evidenceErr := placementprobe.RequireConflictRepairEvidence(
			configuredBackends, inventories, candidate,
		)
		if evidenceErr != nil {
			return evidenceErr
		}
		plan, evidenceErr := repair.PlanConflictRepairContext(probeCtx, candidate, facts)
		if evidenceErr != nil {
			return evidenceErr
		}
		if !*apply {
			if err := closeRepair(); err != nil {
				return fmt.Errorf("close dry-run placement repair: %w", err)
			}
			if _, err := fmt.Fprintf(stdout,
				"DRY RUN ONLY: conflict revision %d for lease %s has durable candidates %q; backend %q is the sole current positive owner in complete configured-backend inventory; database unchanged. Current inventory alone cannot prove a delayed request/effect or callback on another candidate cannot still occur.\nTo apply only after draining every candidate's delayed requests/effects and callback replay, use -apply -backup <new-no-overwrite-path> -confirm %q -attest-drained %q\n",
				plan.Revision(), plan.LeaseUUID(), plan.CandidateBackends(),
				plan.SelectedBackend(), plan.ConfirmationValue(), drainedAttestation,
			); err != nil {
				return fmt.Errorf("write conflict repair dry-run verdict: %w", err)
			}
			return nil
		}
		if *confirmation != plan.ConfirmationValue() {
			return fmt.Errorf("-confirm must exactly equal %q", plan.ConfirmationValue())
		}
		if *attestation != drainedAttestation {
			return fmt.Errorf("-attest-drained must exactly equal %q", drainedAttestation)
		}
		drain, attestErr := repair.AttestDrain(plan.ConfirmationValue(), *attestation)
		if attestErr != nil {
			return attestErr
		}
		if err := requireFreshRepairEvidence(probeCtx, "before exact backup"); err != nil {
			return err
		}
		if err := dependencies.createExactBackup(repair, boundBackupTarget); err != nil {
			if errors.Is(err, placement.ErrExactBackupPublished) {
				return publishedRepairBackupFailure(boundBackupTarget.Path(), err)
			}
			return err
		}
		if err := requireFreshRepairEvidenceAfterBackup(probeCtx, boundBackupTarget.Path()); err != nil {
			return err
		}
		finalProbe := func(finalCtx context.Context) (placement.RepairInventorySnapshot, error) {
			finalInventories, collectErr := placementprobe.Collect(finalCtx, clients)
			if collectErr != nil {
				return placement.RepairInventorySnapshot{}, collectErr
			}
			return placementprobe.RequireConflictRepairEvidence(
				configuredBackends, finalInventories, candidate,
			)
		}
		result, resolveErr := repair.ResolveConflictContext(probeCtx, plan, drain, finalProbe)
		if resolveErr != nil {
			if errors.Is(resolveErr, placement.ErrRepairMutationOutcomeUnknown) {
				mutationOutcomeUnknown = true
				return newOutcomeUnknownRepairFailure(
					"resolving the exact placement conflict", resolveErr,
				)
			}
			if errors.Is(resolveErr, placement.ErrRepairMutationCommitted) {
				mutationCommitted = true
				return newCommittedRepairFailure(
					"post-commit invariant verification", resolveErr,
				)
			}
			return publishedRepairBackupFailure(boundBackupTarget.Path(), resolveErr)
		}
		mutationCommitted = true
		if err := repair.Sync(); err != nil {
			return newCommittedRepairFailure("explicit database sync verification", err)
		}
		if err := closeRepair(); err != nil {
			return newCommittedRepairFailure("database close verification", err)
		}
		reopened, openErr := dependencies.openPostconditionInspector(
			cfg.PlacementStoreDBPath, cfg.ProviderUUID,
		)
		if openErr != nil {
			return newCommittedRepairFailure(
				"database reopen physical/schema verification", openErr,
			)
		}
		verifyErr := reopened.VerifyConflictResolutionPostcondition(candidate, result)
		reopenedCloseErr := reopened.Close()
		if verifyErr != nil {
			if reopenedCloseErr != nil {
				verifyErr = errors.Join(
					verifyErr,
					fmt.Errorf("close reopened database: %w", reopenedCloseErr),
				)
			}
			return newCommittedRepairFailure(
				"reopened database semantic verification", verifyErr,
			)
		}
		if reopenedCloseErr != nil {
			return newCommittedRepairFailure(
				"reopened database close verification", reopenedCloseErr,
			)
		}
		if err := boundBackupTarget.VerifyPublished(); err != nil {
			return newCommittedRepairFailure("final exact-backup authority verification", err)
		}
		ownerKind := "active provision"
		if result.Retained {
			ownerKind = "retained lease (lifecycle authority remains quarantined)"
		}
		if _, err := fmt.Fprintf(stdout,
			"PASS: resolved exact placement conflict revision %d for lease %s to backend %q from one %s; exact pre-mutation backup %q; database synced and closed\n",
			candidate.Revision(), candidate.LeaseUUID(), result.ConfirmedOwner, ownerKind,
			boundBackupTarget.Path(),
		); err != nil {
			return newCommittedVerdictFailure(err)
		}
		return nil
	}

	operationID, err := operation.ParseID(*operationText)
	if err != nil {
		return fmt.Errorf("parse -operation-id: %w", err)
	}
	candidate, err := repair.MatchAttempt(*leaseUUID, *backendName, operationID)
	if err != nil {
		return err
	}
	if *apply {
		if *confirmation != candidate.ConfirmationValue() {
			return fmt.Errorf("-confirm must exactly equal %q", candidate.ConfirmationValue())
		}
		if *attestation != drainedAttestation {
			return fmt.Errorf("-attest-drained must exactly equal %q", drainedAttestation)
		}
	}

	clients, err := placementprobe.NewIdentityBoundClients(cfg, repair)
	if err != nil {
		return err
	}
	probeCtx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	inventories, err := placementprobe.Collect(probeCtx, clients)
	if err != nil {
		return err
	}
	facts, err := placementprobe.VerifyRepairEvidence(configuredBackends, inventories, candidate)
	if err != nil {
		return err
	}
	evidence, err := repair.VerifyAttemptRepairEvidenceContext(probeCtx, candidate, facts)
	if err != nil {
		return err
	}

	if !*apply {
		if err := closeRepair(); err != nil {
			return fmt.Errorf("close dry-run placement repair: %w", err)
		}
		if _, err := fmt.Fprintf(stdout,
			"DRY RUN ONLY: exact attempt %s on backend %q matches lease %s; complete configured-backend inventory contains no evidence of the attempted generation or other disallowed target state; database unchanged. Inventory evidence does not prove a delayed effect cannot still occur.\nTo apply after draining delayed requests/effects and callback replay, use -apply -backup <new-no-overwrite-path> -confirm %q -attest-drained %q\n",
			candidate.OperationID(), candidate.Backend(), candidate.LeaseUUID(),
			candidate.ConfirmationValue(), drainedAttestation,
		); err != nil {
			return fmt.Errorf("write dry-run verdict: %w", err)
		}
		return nil
	}

	drain, err := repair.AttestDrain(candidate.ConfirmationValue(), *attestation)
	if err != nil {
		return err
	}
	if err := requireFreshRepairEvidence(probeCtx, "before exact backup"); err != nil {
		return err
	}
	if err := dependencies.createExactBackup(repair, boundBackupTarget); err != nil {
		if errors.Is(err, placement.ErrExactBackupPublished) {
			return publishedRepairBackupFailure(boundBackupTarget.Path(), err)
		}
		return err
	}
	if err := requireFreshRepairEvidenceAfterBackup(probeCtx, boundBackupTarget.Path()); err != nil {
		return err
	}
	finalProbe := func(finalCtx context.Context) (placement.RepairInventorySnapshot, error) {
		finalInventories, collectErr := placementprobe.Collect(finalCtx, clients)
		if collectErr != nil {
			return placement.RepairInventorySnapshot{}, collectErr
		}
		return placementprobe.VerifyRepairEvidence(
			configuredBackends, finalInventories, candidate,
		)
	}
	result, err := repair.RefuseContext(probeCtx, candidate, evidence, drain, finalProbe)
	if err != nil {
		if errors.Is(err, placement.ErrRepairMutationOutcomeUnknown) {
			mutationOutcomeUnknown = true
			return newOutcomeUnknownRepairFailure(
				"refusing the exact placement attempt", err,
			)
		}
		if errors.Is(err, placement.ErrRepairMutationCommitted) {
			mutationCommitted = true
			return newCommittedRepairFailure("post-commit invariant verification", err)
		}
		return publishedRepairBackupFailure(boundBackupTarget.Path(), err)
	}
	mutationCommitted = true
	if err := repair.Sync(); err != nil {
		return newCommittedRepairFailure("explicit database sync verification", err)
	}
	if err := closeRepair(); err != nil {
		return newCommittedRepairFailure("database close verification", err)
	}
	reopened, openErr := dependencies.openPostconditionInspector(
		cfg.PlacementStoreDBPath, cfg.ProviderUUID,
	)
	if openErr != nil {
		return newCommittedRepairFailure("database reopen physical/schema verification", openErr)
	}
	verifyErr := reopened.VerifyRefusalPostcondition(candidate, result)
	reopenedCloseErr := reopened.Close()
	if verifyErr != nil {
		if reopenedCloseErr != nil {
			verifyErr = errors.Join(
				verifyErr,
				fmt.Errorf("close reopened database: %w", reopenedCloseErr),
			)
		}
		return newCommittedRepairFailure("reopened database semantic verification", verifyErr)
	}
	if reopenedCloseErr != nil {
		return newCommittedRepairFailure("reopened database close verification", reopenedCloseErr)
	}
	if err := boundBackupTarget.VerifyPublished(); err != nil {
		return newCommittedRepairFailure("final exact-backup authority verification", err)
	}
	ownerVerdict := "no confirmed owner existed"
	if result.ConfirmedOwner != "" {
		ownerVerdict = fmt.Sprintf("confirmed owner %q was preserved", result.ConfirmedOwner)
	}
	if _, err := fmt.Fprintf(stdout,
		"PASS: refused exact placement attempt %s for lease %s on backend %q; %s; exact pre-mutation backup %q; database synced and closed\n",
		candidate.OperationID(), candidate.LeaseUUID(), candidate.Backend(), ownerVerdict,
		boundBackupTarget.Path(),
	); err != nil {
		return newCommittedVerdictFailure(err)
	}
	return nil
}

func runAuthorityClassification(
	ctx context.Context,
	dbPath string,
	expectation placement.AuthorityExpectation,
	stdout io.Writer,
	inspect func(
		string,
		placement.AuthorityExpectation,
	) (placement.AuthorityReport, error),
) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("placement authority classification canceled before inspection: %w", err)
	}
	report, inspectErr := inspect(dbPath, expectation)
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("placement authority classification canceled during inspection: %w", err)
	}
	if report.Classification != "" {
		encoded, err := placement.MarshalAuthorityReport(report)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf(
				"placement authority classification canceled before reporting: %w",
				err,
			)
		}
		written, writeErr := stdout.Write(encoded)
		if writeErr != nil {
			return fmt.Errorf("write placement authority classification: %w", writeErr)
		}
		if written != len(encoded) {
			return fmt.Errorf("write placement authority classification: %w", io.ErrShortWrite)
		}
	}
	if inspectErr != nil {
		return inspectErr
	}
	if !report.SafeForCutover() {
		return fmt.Errorf(
			"placement authority classification %q requires operator investigation",
			report.Classification,
		)
	}
	return nil
}

func requireFreshRepairEvidence(ctx context.Context, stage string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf(
			"repair inventory evidence expired %s; no backup or repair mutation was attempted: %w",
			stage,
			err,
		)
	}
	return nil
}

func requireFreshRepairEvidenceAfterBackup(ctx context.Context, backupPath string) error {
	if err := ctx.Err(); err != nil {
		return publishedRepairBackupFailure(
			backupPath,
			fmt.Errorf("repair inventory evidence expired after backup publication: %w", err),
		)
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

func runInspection(
	dbPath, providerUUID, leaseUUID string,
	listRecords bool,
	stdout io.Writer,
) (runErr error) {
	inspector, err := placement.OpenRepairInspector(dbPath, providerUUID)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := inspector.Close(); runErr == nil && closeErr != nil {
			runErr = fmt.Errorf("close placement repair inspector: %w", closeErr)
		}
	}()
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	if listRecords {
		output := struct {
			BackendTopology []string                 `json:"backend_topology"`
			Placements      []placement.RepairRecord `json:"placements"`
		}{
			BackendTopology: inspector.BackendTopology(),
			Placements:      inspector.List(),
		}
		if err := encoder.Encode(output); err != nil {
			return fmt.Errorf("write placement repair list: %w", err)
		}
		return nil
	}
	record, exists, err := inspector.Inspect(leaseUUID)
	if err != nil {
		return err
	}
	output := struct {
		BackendTopology []string               `json:"backend_topology"`
		Exists          bool                   `json:"exists"`
		Placement       placement.RepairRecord `json:"placement"`
	}{
		BackendTopology: inspector.BackendTopology(),
		Exists:          exists,
		Placement:       record,
	}
	if err := encoder.Encode(output); err != nil {
		return fmt.Errorf("write placement repair inspection: %w", err)
	}
	return nil
}

func backendNames(cfg *config.Config) []string {
	names := make([]string, 0, len(cfg.Backends))
	for _, backendConfig := range cfg.Backends {
		names = append(names, backendConfig.Name)
	}
	return names
}
