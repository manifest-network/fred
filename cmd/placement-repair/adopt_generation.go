package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/placementprobe"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// generationAdoptionRequest carries the validated flags and the open repair
// session into the generation adoption mode.
type generationAdoptionRequest struct {
	repair                 *placement.AttemptRepair
	closeRepair            func() error
	newRepairClients       func(backend.BackendStorageIdentityResolver) ([]placementprobe.Client, error)
	configuredBackends     []string
	databasePath           string
	providerUUID           string
	leaseUUID              string
	backendName            string
	timeout                time.Duration
	apply                  bool
	confirmation           string
	attestation            string
	backupTarget           *placement.ExactBackupTarget
	mutationCommitted      *bool
	mutationOutcomeUnknown *bool
}

// runGenerationAdoption replaces one lease's quarantined lifecycle generation
// with the one its confirmed backend reports. The dry run prints only
// generation fingerprints: lifecycle IDs are callback capabilities.
func runGenerationAdoption(
	ctx context.Context,
	request generationAdoptionRequest,
	stdout io.Writer,
	dependencies commandDependencies,
) error {
	candidate, err := request.repair.MatchGenerationQuarantine(request.leaseUUID, request.backendName)
	if err != nil {
		return err
	}
	clients, err := request.newRepairClients(request.repair)
	if err != nil {
		return err
	}
	probeCtx, cancel := context.WithTimeout(ctx, request.timeout)
	defer cancel()
	collect := func(collectCtx context.Context) (placement.RepairInventorySnapshot, error) {
		inventories, collectErr := placementprobe.Collect(collectCtx, clients)
		if collectErr != nil {
			return placement.RepairInventorySnapshot{}, collectErr
		}
		return placementprobe.RequireGenerationAdoptionEvidence(
			request.configuredBackends, inventories, candidate,
		)
	}
	facts, err := collect(probeCtx)
	if err != nil {
		return err
	}
	plan, err := request.repair.PlanGenerationAdoptionContext(probeCtx, candidate, facts)
	if err != nil {
		return err
	}
	if !request.apply {
		if err := request.closeRepair(); err != nil {
			return fmt.Errorf("close dry-run placement repair: %w", err)
		}
		if _, err := fmt.Fprintf(stdout,
			"DRY RUN ONLY: lease %s on backend %q is quarantined at lifecycle generation %s; the backend is its sole owner and reports generation %s for the same tenant; database unchanged. Inventory cannot prove the backend was not restored from an older snapshot or is not replaying an operation.\nTo apply only after confirming both, use -apply -backup <new-no-overwrite-path> -confirm %q -attest-generation %q\n",
			plan.LeaseUUID(), plan.Backend(), plan.StoredGenerationFingerprint(),
			plan.ObservedGenerationFingerprint(), plan.ConfirmationValue(),
			placement.GenerationAttestationText,
		); err != nil {
			return fmt.Errorf("write generation adoption dry-run verdict: %w", err)
		}
		return nil
	}
	if request.confirmation != plan.ConfirmationValue() {
		return fmt.Errorf("-confirm must exactly equal %q", plan.ConfirmationValue())
	}
	if request.attestation != placement.GenerationAttestationText {
		return fmt.Errorf("-attest-generation must exactly equal %q", placement.GenerationAttestationText)
	}
	attestation, err := request.repair.AttestGeneration(plan.ConfirmationValue(), request.attestation)
	if err != nil {
		return err
	}
	if err := requireFreshRepairEvidence(probeCtx, "before exact backup"); err != nil {
		return err
	}
	if err := dependencies.createExactBackup(request.repair, request.backupTarget); err != nil {
		if errors.Is(err, placement.ErrExactBackupPublished) {
			return publishedRepairBackupFailure(request.backupTarget.Path(), err)
		}
		return err
	}
	if err := requireFreshRepairEvidenceAfterBackup(probeCtx, request.backupTarget.Path()); err != nil {
		return err
	}
	result, adoptErr := request.repair.AdoptObservedGenerationContext(
		probeCtx, plan, attestation, placement.GenerationAdoptionProbe(collect),
	)
	if adoptErr != nil {
		if errors.Is(adoptErr, placement.ErrRepairMutationOutcomeUnknown) {
			*request.mutationOutcomeUnknown = true
			return newOutcomeUnknownRepairFailure("adopting the observed lifecycle generation", adoptErr)
		}
		if errors.Is(adoptErr, placement.ErrRepairMutationCommitted) {
			*request.mutationCommitted = true
			return newCommittedRepairFailure("post-commit invariant verification", adoptErr)
		}
		return publishedRepairBackupFailure(request.backupTarget.Path(), adoptErr)
	}
	*request.mutationCommitted = true
	if err := request.repair.Sync(); err != nil {
		return newCommittedRepairFailure("explicit database sync verification", err)
	}
	if err := request.closeRepair(); err != nil {
		return newCommittedRepairFailure("database close verification", err)
	}
	reopened, err := dependencies.openPostconditionInspector(request.databasePath, request.providerUUID)
	if err != nil {
		return newCommittedRepairFailure("database reopen physical/schema verification", err)
	}
	verifyErr := reopened.VerifyGenerationAdoptionPostcondition(candidate, result)
	reopenedCloseErr := reopened.Close()
	if verifyErr != nil {
		if reopenedCloseErr != nil {
			verifyErr = errors.Join(verifyErr, fmt.Errorf("close reopened database: %w", reopenedCloseErr))
		}
		return newCommittedRepairFailure("reopened database semantic verification", verifyErr)
	}
	if reopenedCloseErr != nil {
		return newCommittedRepairFailure("reopened database close verification", reopenedCloseErr)
	}
	if err := request.backupTarget.VerifyPublished(); err != nil {
		return newCommittedRepairFailure("final exact-backup authority verification", err)
	}
	if _, err := fmt.Fprintf(stdout,
		"PASS: lease %s on backend %q now carries lifecycle generation %s instead of %s; callbacks, restart, and update are authorized again; exact pre-mutation backup %q; database synced and closed\n",
		plan.LeaseUUID(), plan.Backend(), plan.ObservedGenerationFingerprint(),
		plan.StoredGenerationFingerprint(), request.backupTarget.Path(),
	); err != nil {
		return newCommittedVerdictFailure(err)
	}
	return nil
}
