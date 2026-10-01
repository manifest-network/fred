package docker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
)

// AdoptionFindingClass is the closed set of v0.13 store shapes that storage
// identity adoption refuses.
type AdoptionFindingClass uint8

const (
	// The zero value is invalid and never reported.
	_ AdoptionFindingClass = iota
	AdoptionFindingUpgradedCallbackStore
	AdoptionFindingPendingCallbacks
	AdoptionFindingIncompleteJournals
	AdoptionFindingCallbackCohort
	AdoptionFindingContainerWithoutAuthority
	AdoptionFindingReleaseRetentionComparison
	AdoptionFindingInterruptedDeprovision
	AdoptionFindingUnresolvedClose
	AdoptionFindingReleaseItems
	AdoptionFindingReleaseSKU
	AdoptionFindingRuntimeAuthority
	AdoptionFindingReleaseCapacity
	AdoptionFindingContainerVolume
	AdoptionFindingRetentionIdentity
	AdoptionFindingRetentionSKU
	AdoptionFindingRetentionVolume
	AdoptionFindingReapingVolume
	AdoptionFindingNonEmptySubstrate
	AdoptionFindingEmptySubstrate
	AdoptionFindingMissingEvidenceVolume
	AdoptionFindingUnexplainedVolume
	AdoptionFindingLeaseWithoutRelease
	AdoptionFindingReleaseWithoutCohort
)

func (class AdoptionFindingClass) String() string {
	switch class {
	case AdoptionFindingUpgradedCallbackStore:
		return "upgraded_callback_store"
	case AdoptionFindingPendingCallbacks:
		return "pending_callbacks"
	case AdoptionFindingIncompleteJournals:
		return "incomplete_journals"
	case AdoptionFindingCallbackCohort:
		return "invalid_callback_cohort"
	case AdoptionFindingContainerWithoutAuthority:
		return "container_without_authority"
	case AdoptionFindingReleaseRetentionComparison:
		return "release_retention_comparison"
	case AdoptionFindingInterruptedDeprovision:
		return "interrupted_deprovision"
	case AdoptionFindingUnresolvedClose:
		return "unresolved_close"
	case AdoptionFindingReleaseItems:
		return "release_items_underivable"
	case AdoptionFindingReleaseSKU:
		return "release_sku_unresolvable"
	case AdoptionFindingRuntimeAuthority:
		return "runtime_authority_unresolvable"
	case AdoptionFindingReleaseCapacity:
		return "release_capacity"
	case AdoptionFindingContainerVolume:
		return "container_volume"
	case AdoptionFindingRetentionIdentity:
		return "retention_identity"
	case AdoptionFindingRetentionSKU:
		return "retention_sku_unresolvable"
	case AdoptionFindingRetentionVolume:
		return "retention_volume"
	case AdoptionFindingReapingVolume:
		return "reaping_volume"
	case AdoptionFindingNonEmptySubstrate:
		return "non_empty_substrate"
	case AdoptionFindingEmptySubstrate:
		return "empty_substrate"
	case AdoptionFindingMissingEvidenceVolume:
		return "missing_evidence_volume"
	case AdoptionFindingUnexplainedVolume:
		return "unexplained_volume"
	case AdoptionFindingLeaseWithoutRelease:
		return "lease_without_release"
	case AdoptionFindingReleaseWithoutCohort:
		return "release_without_cohort"
	default:
		return "invalid"
	}
}

// Remedy names the operator action each finding's refusal text already
// prescribes. It is advisory: "investigate" means no single remedy is safe
// to automate.
func (class AdoptionFindingClass) Remedy() string {
	switch class {
	case AdoptionFindingPendingCallbacks:
		return "drain_v0_13_callbacks"
	case AdoptionFindingInterruptedDeprovision:
		return "replay_v0_13_deprovision"
	case AdoptionFindingUnresolvedClose:
		return "restore_pre_close_snapshot_or_decide_disposition"
	case AdoptionFindingReleaseSKU, AdoptionFindingRetentionSKU:
		return "restore_v0_13_sku_mapping"
	case AdoptionFindingUpgradedCallbackStore:
		return "restore_sealed_marker_pair"
	default:
		return "investigate"
	}
}

// adoptionFindings receives every refused shape. The fail-fast sink stops
// verification at the first finding, exactly as the preflight and the
// initializer always have; the audit's sink records every finding.
type adoptionFindings interface {
	add(adoptionFinding) error
}

type adoptionFinding struct {
	class     AdoptionFindingClass
	leaseUUID string
	// subject names the container or volume the finding is about, if any.
	subject string
	err     error
}

type failFastAdoptionFindings struct{}

func (failFastAdoptionFindings) add(finding adoptionFinding) error { return finding.err }

type collectedAdoptionFindings struct{ findings []adoptionFinding }

func (collected *collectedAdoptionFindings) add(finding adoptionFinding) error {
	collected.findings = append(collected.findings, finding)
	return nil
}

// adoptionEvidence is what verification learned beyond its verdict.
type adoptionEvidence struct {
	pendingCallbacks   int
	activeReleaseItems []adoptionReleaseItems
}

type adoptionReleaseItems struct {
	leaseUUID string
	items     []backend.LeaseItem
}

// StorageIdentityAdoptionBlocked is the audit verdict when any shape refuses
// adoption.
const StorageIdentityAdoptionBlocked StorageIdentityAdoptionVerdict = "v0_13_storage_identity_adoption_blocked"

// StorageIdentityAdoptionAudit is the machine-readable result of the read-only
// adoption audit: every refused shape at once, in the preflight's order, so the
// first finding is exactly the error the preflight would report.
type StorageIdentityAdoptionAudit struct {
	Verdict          StorageIdentityAdoptionVerdict `json:"verdict"`
	Findings         []AdoptionAuditFinding         `json:"findings"`
	ActiveReleases   []AdoptionAuditRelease         `json:"active_releases"`
	PendingCallbacks int                            `json:"pending_callbacks"`
}

// AdoptionAuditFinding is one refused shape.
type AdoptionAuditFinding struct {
	Class     string `json:"class"`
	Remedy    string `json:"remedy"`
	LeaseUUID string `json:"lease_uuid,omitempty"`
	Subject   string `json:"subject,omitempty"`
	Message   string `json:"message"`
}

// AdoptionAuditRelease carries the v0.13 items adoption would freeze for one
// active release, so a caller can compare them with chain state at a pinned
// height.
type AdoptionAuditRelease struct {
	LeaseUUID string              `json:"lease_uuid"`
	Items     []backend.LeaseItem `json:"items"`
}

// AuditStorageIdentityAdoptionForConfig is the read-only counterpart of the
// adoption preflight that reports every refused shape instead of the first.
// Preconditions the preflight cannot continue past (unreadable journals or
// substrate, a sealed or resuming lineage, a substrate that changes during
// the audit) are errors, not findings.
func AuditStorageIdentityAdoptionForConfig(
	ctx context.Context,
	cfg Config,
	logger *slog.Logger,
) (StorageIdentityAdoptionAudit, error) {
	if ctx == nil {
		return StorageIdentityAdoptionAudit{}, errors.New("storage identity adoption audit context is required")
	}
	if logger == nil {
		return StorageIdentityAdoptionAudit{}, errors.New("storage identity adoption audit logger is required")
	}
	if err := cfg.Validate(); err != nil {
		return StorageIdentityAdoptionAudit{}, fmt.Errorf("invalid config: %w", err)
	}
	if err := verifyConfiguredVolumeMount(cfg); err != nil {
		return StorageIdentityAdoptionAudit{}, err
	}
	dockerClient, err := NewDockerClient(ctx, cfg.DockerHost, cfg.Name)
	if err != nil {
		return StorageIdentityAdoptionAudit{}, fmt.Errorf("create Docker client for storage identity audit: %w", err)
	}
	defer func() { _ = dockerClient.Close() }()
	volumes, err := newVolumeManager(
		cfg.VolumeDataPath, cfg.VolumeFilesystem, cfg.GetMinAvgFileBytes(), logger,
	)
	if err != nil {
		return StorageIdentityAdoptionAudit{}, fmt.Errorf("create volume manager for storage identity audit: %w", err)
	}
	return auditStorageIdentityAdoptionWithDependencies(ctx, cfg, dockerClient, volumes)
}

func auditStorageIdentityAdoptionWithDependencies(
	ctx context.Context,
	cfg Config,
	dockerClient storageIdentityProofClient,
	volumes storageIdentityProofVolumes,
) (StorageIdentityAdoptionAudit, error) {
	proof, err := acquireDockerStorageIdentityProof(ctx, cfg, dockerClient, volumes)
	if err != nil {
		return StorageIdentityAdoptionAudit{}, err
	}
	defer func() { _ = proof.Close() }()
	if err := proof.verifyStableSubstrate(ctx, dockerClient, volumes); err != nil {
		return StorageIdentityAdoptionAudit{}, err
	}
	if err := proof.paths.markers.VerifyAbsent(); err != nil {
		return StorageIdentityAdoptionAudit{}, fmt.Errorf("audit requires an unsealed v0.13 marker pair: %w", err)
	}
	profile, resuming, err := dockerStorageInitializationProfile(
		cfg, proof.paths, proof.initialDaemon.SystemID, StorageIdentityInitializeAdopt,
	)
	if err != nil {
		return StorageIdentityAdoptionAudit{}, err
	}
	if resuming {
		return StorageIdentityAdoptionAudit{}, errors.New("read-only adoption audit refuses a pending storage identity initialization")
	}
	if profile != backendidentity.InitializationProfileExisting {
		return StorageIdentityAdoptionAudit{}, errors.New("adoption audit requires a complete existing v0.13 lineage")
	}
	collected := &collectedAdoptionFindings{}
	evidence, err := collectStorageIdentityInitializationEvidence(
		ctx, cfg, dockerClient, volumes, StorageIdentityInitializeAdopt, profile, false, proof.paths, collected,
	)
	if err != nil {
		return StorageIdentityAdoptionAudit{}, err
	}
	if err := proof.verifyStableSubstrate(ctx, dockerClient, volumes); err != nil {
		return StorageIdentityAdoptionAudit{}, err
	}
	if err := proof.paths.markers.VerifyAbsent(); err != nil {
		return StorageIdentityAdoptionAudit{}, fmt.Errorf("marker pair changed during read-only adoption audit: %w", err)
	}
	return newStorageIdentityAdoptionAudit(collected.findings, evidence), nil
}

func newStorageIdentityAdoptionAudit(
	findings []adoptionFinding,
	evidence adoptionEvidence,
) StorageIdentityAdoptionAudit {
	audit := StorageIdentityAdoptionAudit{
		Verdict:          StorageIdentityAdoptionReady,
		Findings:         make([]AdoptionAuditFinding, 0, len(findings)),
		ActiveReleases:   make([]AdoptionAuditRelease, 0, len(evidence.activeReleaseItems)),
		PendingCallbacks: evidence.pendingCallbacks,
	}
	for _, finding := range findings {
		audit.Findings = append(audit.Findings, AdoptionAuditFinding{
			Class:     finding.class.String(),
			Remedy:    finding.class.Remedy(),
			LeaseUUID: finding.leaseUUID,
			Subject:   finding.subject,
			Message:   finding.err.Error(),
		})
	}
	if len(audit.Findings) != 0 {
		audit.Verdict = StorageIdentityAdoptionBlocked
	}
	for _, release := range evidence.activeReleaseItems {
		items := slices.Clone(release.items)
		if items == nil {
			items = []backend.LeaseItem{}
		}
		audit.ActiveReleases = append(audit.ActiveReleases, AdoptionAuditRelease{
			LeaseUUID: release.leaseUUID, Items: items,
		})
	}
	return audit
}
