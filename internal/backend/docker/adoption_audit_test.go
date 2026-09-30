package docker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// preflightWithDifferentialAudit runs the read-only audit, then the fail-fast
// preflight, on the same fixture and requires them to agree: no findings when
// the preflight is ready, the preflight's exact error as the audit's first
// finding, or the same error when neither can continue.
func preflightWithDifferentialAudit(
	t *testing.T,
	ctx context.Context,
	cfg Config,
	dockerClient storageIdentityProofClient,
	volumes storageIdentityProofVolumes,
) (StorageIdentityAdoptionVerdict, error) {
	t.Helper()
	audit, auditErr := auditStorageIdentityAdoptionWithDependencies(ctx, cfg, dockerClient, volumes)
	verdict, err := preflightStorageIdentityAdoptionWithDependencies(ctx, cfg, dockerClient, volumes)
	switch {
	case auditErr != nil:
		require.Error(t, err, "the audit failed where the preflight passed: %v", auditErr)
		assert.Equal(t, auditErr.Error(), err.Error())
	case err == nil:
		assert.Equal(t, StorageIdentityAdoptionReady, audit.Verdict)
		assert.Empty(t, audit.Findings)
	default:
		require.NotEmpty(t, audit.Findings, "the preflight refused a shape the audit did not report: %v", err)
		assert.Equal(t, err.Error(), audit.Findings[0].Message)
		assert.Equal(t, StorageIdentityAdoptionBlocked, audit.Verdict)
	}
	return verdict, err
}

func TestAdoptionFindingClassesAreClosedAndNamed(t *testing.T) {
	seen := map[string]AdoptionFindingClass{}
	for class := AdoptionFindingUpgradedCallbackStore; class <= AdoptionFindingReleaseWithoutCohort; class++ {
		name := class.String()
		require.NotEqual(t, "invalid", name, "class %d has no name", class)
		_, duplicate := seen[name]
		require.False(t, duplicate, "class name %q is reused", name)
		seen[name] = class
		assert.NotEmpty(t, class.Remedy())
	}
	var zero AdoptionFindingClass
	assert.Equal(t, "invalid", zero.String())
	assert.Equal(t, "invalid", (AdoptionFindingReleaseWithoutCohort + 1).String())
}

// TestAdoptionAuditReportsEveryIndependentShape builds a lineage that the
// preflight refuses several ways and requires the audit to report each one in
// a single pass, in the preflight's order, without writing anything.
func TestAdoptionAuditReportsEveryIndependentShape(t *testing.T) {
	cfg := storageIdentityLegacyRetentionTestConfig(t)
	writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
	writeLegacyAuthorityStores(t, cfg)
	before := snapshotStorageIdentityAuthorityFiles(t, cfg)

	const (
		orphanLease = "11111111-1111-4111-8111-111111111111"
		volumeLease = "22222222-2222-4222-8222-222222222222"
	)
	orphanVolumes := []string{
		canonicalVolumeName(volumeLease, "app", 0),
		canonicalVolumeName(volumeLease, "db", 0),
	}
	dockerClient := &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		DaemonInfoFn: func(context.Context) (DaemonSecurityInfo, error) {
			return DaemonSecurityInfo{SystemID: "daemon-system-a"}, nil
		},
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{{
				ContainerID: "container-orphan", LeaseUUID: orphanLease,
				SKU: "sku-unknown", ServiceName: "app",
			}}, nil
		},
	}
	volumes := &mockVolumeManager{ListFn: func() ([]string, error) { return orphanVolumes, nil }}

	audit, err := auditStorageIdentityAdoptionWithDependencies(t.Context(), cfg, dockerClient, volumes)
	require.NoError(t, err)
	assert.Equal(t, StorageIdentityAdoptionBlocked, audit.Verdict)
	var classes []string
	for _, finding := range audit.Findings {
		classes = append(classes, finding.Class)
		assert.NotEmpty(t, finding.Message)
		assert.NotEmpty(t, finding.Remedy)
	}
	for _, want := range []string{
		"container_without_authority", "container_volume", "unexplained_volume", "lease_without_release",
	} {
		assert.Contains(t, classes, want)
	}
	unexplained := 0
	for _, finding := range audit.Findings {
		if finding.Class == "unexplained_volume" {
			unexplained++
		}
	}
	assert.Equal(t, len(orphanVolumes), unexplained, "each unexplained volume is its own finding")

	_, preflightErr := preflightStorageIdentityAdoptionWithDependencies(t.Context(), cfg, dockerClient, volumes)
	require.Error(t, preflightErr)
	assert.Equal(t, preflightErr.Error(), audit.Findings[0].Message)
	assertNoStorageIdentityMarkers(t, cfg)
	assertStorageIdentityAuthorityUnchanged(t, cfg, before)
}
