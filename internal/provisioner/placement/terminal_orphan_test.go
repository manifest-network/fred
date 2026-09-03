package placement

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

type terminalOrphanTestSnapshot struct {
	preflightChainSnapshot
	blockingCount int
	blocking      []string
}

func (snapshot terminalOrphanTestSnapshot) BlockingLeaseCount() int {
	return snapshot.blockingCount
}

func (snapshot terminalOrphanTestSnapshot) BlockingLeaseUUIDs() []string {
	return append([]string(nil), snapshot.blocking...)
}

func terminalOrphanSnapshot(
	providerUUID string,
	leaseUUIDs []string,
	blocking ...string,
) terminalOrphanTestSnapshot {
	items := make(map[string][]backend.LeaseItem, len(leaseUUIDs))
	for _, leaseUUID := range leaseUUIDs {
		items[leaseUUID] = []backend.LeaseItem{{
			SKU: "sku-test", Quantity: 1, ServiceName: legacyDefaultServiceName,
		}}
	}
	return terminalOrphanTestSnapshot{
		preflightChainSnapshot: preflightChainSnapshot{
			providerUUID: providerUUID,
			leaseUUIDs:   append([]string(nil), leaseUUIDs...),
			leaseItems:   items,
		},
		blockingCount: len(blocking),
		blocking:      append([]string(nil), blocking...),
	}
}

func terminalOrphanProof(t *testing.T, target string) TerminalOrphanChainProof {
	t.Helper()
	proof, err := NewTerminalOrphanChainProof(
		terminalOrphanSnapshot(preflightProviderUUID, []string{target}),
		preflightProviderUUID,
		target,
	)
	require.NoError(t, err)
	require.True(t, proof.valid())
	return proof
}

func TestNewTerminalOrphanChainProofRequiresPositiveTerminalMembership(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		snapshot       terminalOrphanTestSnapshot
		providerUUID   string
		leaseUUID      string
		mutateSnapshot func(*terminalOrphanTestSnapshot)
		want           string
	}{
		"target absent": {
			snapshot:     terminalOrphanSnapshot(preflightProviderUUID, []string{preflightLeaseB}),
			providerUUID: preflightProviderUUID,
			leaseUUID:    preflightLeaseA,
			want:         "absent from the height-117 all-state chain snapshot",
		},
		"target blocking": {
			snapshot: terminalOrphanSnapshot(
				preflightProviderUUID, []string{preflightLeaseA}, preflightLeaseA,
			),
			providerUUID: preflightProviderUUID,
			leaseUUID:    preflightLeaseA,
			want:         "non-terminal or unknown",
		},
		"configured provider differs": {
			snapshot:     terminalOrphanSnapshot(preflightProviderUUID, []string{preflightLeaseA}),
			providerUUID: preflightLeaseB,
			leaseUUID:    preflightLeaseA,
			want:         "configured provider",
		},
		"blocking count differs": {
			snapshot:     terminalOrphanSnapshot(preflightProviderUUID, []string{preflightLeaseA}),
			providerUUID: preflightProviderUUID,
			leaseUUID:    preflightLeaseA,
			mutateSnapshot: func(snapshot *terminalOrphanTestSnapshot) {
				snapshot.blockingCount = 1
			},
			want: "blocking identities",
		},
		"blocking identity is not a member": {
			snapshot: terminalOrphanSnapshot(
				preflightProviderUUID, []string{preflightLeaseA}, preflightLeaseB,
			),
			providerUUID: preflightProviderUUID,
			leaseUUID:    preflightLeaseA,
			want:         "absent from all-state membership",
		},
		"duplicate blocking identity": {
			snapshot: terminalOrphanSnapshot(
				preflightProviderUUID,
				[]string{preflightLeaseA, preflightLeaseB},
				preflightLeaseB,
				preflightLeaseB,
			),
			providerUUID: preflightProviderUUID,
			leaseUUID:    preflightLeaseA,
			want:         "duplicate blocking lease",
		},
		"target is noncanonical": {
			snapshot:     terminalOrphanSnapshot(preflightProviderUUID, []string{preflightLeaseA}),
			providerUUID: preflightProviderUUID,
			leaseUUID:    "018F47A2-8B1C-7DEF-8123-456789ABCDEF",
			want:         "not canonical",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			snapshot := test.snapshot
			if test.mutateSnapshot != nil {
				test.mutateSnapshot(&snapshot)
			}
			proof, err := NewTerminalOrphanChainProof(
				snapshot,
				test.providerUUID,
				test.leaseUUID,
			)
			require.ErrorIs(t, err, ErrTerminalOrphanProof)
			require.ErrorContains(t, err, test.want)
			assert.False(t, proof.valid())
		})
	}
}

func TestLegacyUpgradeInspectorProvesOnlyAbsentTerminalPlacement(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, dbPath, map[string][]byte{
		preflightLeaseB: []byte(`{"backend":"backend-b","set_at":"2026-08-25T15:00:00Z"}`),
	})
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	inspector, err := OpenLegacyUpgradeInspector(dbPath)
	require.NoError(t, err)

	summary, err := inspector.ProveTerminalOrphanContext(
		t.Context(),
		"backend-a",
		terminalOrphanProof(t, preflightLeaseA),
	)
	require.NoError(t, err)
	assert.Equal(t, TerminalOrphanProofSummary{
		LeaseUUID:       preflightLeaseA,
		ExpectedBackend: "backend-a",
		ProviderUUID:    preflightProviderUUID,
		ChainHeight:     117,
	}, summary)
	require.NoError(t, inspector.Close())
	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, after, "terminal proof must not write the stopped authority")
}

func TestLegacyUpgradeInspectorClassifiesResidualTerminalPlacement(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		owner       string
		wantError   error
		wantMessage string
	}{
		"expected owner needs old cleanup": {
			owner:       "backend-a",
			wantError:   ErrTerminalOrphanResidualPlacement,
			wantMessage: "cleanupOrphanedPlacements",
		},
		"foreign owner is never cleanup authority": {
			owner:       "backend-b",
			wantError:   ErrTerminalOrphanProof,
			wantMessage: `residual v0.13 owner "backend-b", expected "backend-a"`,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dbPath := filepath.Join(t.TempDir(), "placements.db")
			writeRawRecords(t, dbPath, map[string][]byte{
				preflightLeaseA: []byte(test.owner),
			})
			inspector, err := OpenLegacyUpgradeInspector(dbPath)
			require.NoError(t, err)
			_, err = inspector.ProveTerminalOrphanContext(
				t.Context(),
				"backend-a",
				terminalOrphanProof(t, preflightLeaseA),
			)
			require.ErrorIs(t, err, test.wantError)
			require.ErrorContains(t, err, test.wantMessage)
			require.NoError(t, inspector.Close())
		})
	}
}

func TestLegacyUpgradeInspectorRejectsInvalidOrCurrentEpochAnywhere(t *testing.T) {
	t.Parallel()

	tests := map[string]map[string][]byte{
		"malformed unrelated legacy row": {
			preflightLeaseB: {0x00},
		},
		"revisioned target row": {
			preflightLeaseA: []byte(`{"revision":1,"confirmed_backend":"backend-a"}`),
		},
		"ambiguous duplicate legacy fields": {
			preflightLeaseB: []byte(`{"backend":"backend-b","backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
		},
	}
	for name, records := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dbPath := filepath.Join(t.TempDir(), "placements.db")
			writeRawRecords(t, dbPath, records)
			inspector, err := OpenLegacyUpgradeInspector(dbPath)
			require.NoError(t, err)
			_, err = inspector.ProveTerminalOrphanContext(
				t.Context(),
				"backend-a",
				terminalOrphanProof(t, preflightLeaseA),
			)
			require.ErrorIs(t, err, ErrTerminalOrphanProof)
			require.NoError(t, inspector.Close())
		})
	}
}

func TestLegacyUpgradeInspectorTerminalProofHonorsCancellation(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, dbPath, map[string][]byte{})
	inspector, err := OpenLegacyUpgradeInspector(dbPath)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = inspector.ProveTerminalOrphanContext(
		ctx,
		"backend-a",
		terminalOrphanProof(t, preflightLeaseA),
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
	require.NoError(t, inspector.Close())
}
