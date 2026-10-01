package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/placementprobe"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// backendRetirementRequest carries the validated flags and the open repair
// session into the retirement mode.
type backendRetirementRequest struct {
	cfg                    *config.Config
	repair                 *placement.AttemptRepair
	closeRepair            func() error
	backendName            string
	storageIDText          string
	apply                  bool
	confirmation           string
	attestation            string
	backupTarget           *placement.ExactBackupTarget
	mutationCommitted      *bool
	mutationOutcomeUnknown *bool
}

// backendRetirementOutput is the dry run's single JSON object.
type backendRetirementOutput struct {
	placement.BackendRetirementFacts
	TargetProbe placementprobe.RetirementTargetLiveness `json:"target_probe"`
	AttestLost  string                                  `json:"attest_lost"`
	Confirm     string                                  `json:"confirm"`
}

func runBackendRetirement(
	ctx context.Context,
	request backendRetirementRequest,
	stdout io.Writer,
	dependencies commandDependencies,
) error {
	storageID, err := backendidentity.Parse(request.storageIDText)
	if err != nil {
		return fmt.Errorf("-storage-id: %w", err)
	}
	plan, err := request.repair.PlanBackendRetirement(request.backendName, storageID)
	if err != nil {
		return err
	}
	target, err := plan.ProbeTarget()
	if err != nil {
		return err
	}
	liveness, err := placementprobe.ProbeRetirementTarget(ctx, request.cfg, target)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	facts := plan.Facts()
	if !request.apply {
		if err := request.closeRepair(); err != nil {
			return fmt.Errorf("close dry-run placement repair: %w", err)
		}
		output := backendRetirementOutput{
			BackendRetirementFacts: facts,
			TargetProbe:            liveness,
			AttestLost:             placement.LostBackendAttestationText,
			Confirm:                plan.ConfirmationValue(),
		}
		if err := json.NewEncoder(stdout).Encode(output); err != nil {
			return fmt.Errorf("write backend retirement plan: %w", err)
		}
		return nil
	}
	if request.confirmation != plan.ConfirmationValue() {
		return fmt.Errorf("-confirm must exactly equal %q", plan.ConfirmationValue())
	}
	if request.attestation != placement.LostBackendAttestationText {
		return fmt.Errorf("-attest-lost must exactly equal %q", placement.LostBackendAttestationText)
	}
	if err := dependencies.createExactBackup(request.repair, request.backupTarget); err != nil {
		if errors.Is(err, placement.ErrExactBackupPublished) {
			return publishedRepairBackupFailure(request.backupTarget.Path(), err)
		}
		return err
	}
	result, retireErr := request.repair.RetireBackend(plan, request.attestation)
	if retireErr != nil {
		if errors.Is(retireErr, placement.ErrRepairMutationOutcomeUnknown) {
			*request.mutationOutcomeUnknown = true
			return newOutcomeUnknownRepairFailure("retiring the lost backend", retireErr)
		}
		if errors.Is(retireErr, placement.ErrRepairMutationCommitted) {
			*request.mutationCommitted = true
			return newCommittedRepairFailure("post-commit invariant verification", retireErr)
		}
		return publishedRepairBackupFailure(request.backupTarget.Path(), retireErr)
	}
	*request.mutationCommitted = true
	if err := request.repair.Sync(); err != nil {
		return newCommittedRepairFailure("explicit database sync verification", err)
	}
	if err := request.closeRepair(); err != nil {
		return newCommittedRepairFailure("database close verification", err)
	}
	reopened, err := dependencies.openPostconditionInspector(
		request.cfg.PlacementStoreDBPath, request.cfg.ProviderUUID,
	)
	if err != nil {
		return newCommittedRepairFailure("database reopen physical/schema verification", err)
	}
	verifyErr := reopened.VerifyBackendRetirementPostcondition(result)
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
		"PASS: retired backend %q (storage %s) from %q; %d lost leases will be closed or rejected on chain, "+
			"%d leases forgot the name; remove %q from the providerd config before starting it; "+
			"exact pre-mutation backup %q; database synced and closed\n",
		facts.Backend, facts.StorageID, facts.DatabasePath, len(facts.LostLeases), len(facts.StrippedLeases),
		facts.Backend, request.backupTarget.Path(),
	); err != nil {
		return newCommittedVerdictFailure(err)
	}
	return nil
}
