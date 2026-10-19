package shared

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
)

// v013ReleaseWire is the v0.13.1 Release struct copied verbatim from
// `git show v0.13.1:internal/backend/shared/releases.go`. v0.13 stored each
// history as json.Marshal([]Release), so these fixtures are the exact bytes the
// external writer produced, independent of today's legacyReleaseJSON decoder.
type v013ReleaseWire struct {
	Version   int            `json:"version"`
	Manifest  []byte         `json:"manifest"`
	Image     string         `json:"image"`
	Status    string         `json:"status"`
	CreatedAt time.Time      `json:"created_at"`
	Error     string         `json:"error,omitempty"`
	Reason    backend.Reason `json:"reason,omitempty"`
	Message   string         `json:"message,omitempty"`
}

func putRawReleaseValue(t *testing.T, dbPath, leaseUUID string, value []byte) {
	t.Helper()
	db, err := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket, createErr := tx.CreateBucketIfNotExists(releasesBucketName)
		if createErr != nil {
			return createErr
		}
		return bucket.Put([]byte(leaseUUID), value)
	}))
	require.NoError(t, db.Close())
}

func writeV013ReleaseHistory(t *testing.T, dbPath, leaseUUID string, history []v013ReleaseWire) {
	t.Helper()
	encoded, err := json.Marshal(history)
	require.NoError(t, err)
	putRawReleaseValue(t, dbPath, leaseUUID, encoded)
}

const (
	v013AdoptionLease = "11111111-1111-4111-8111-111111111111"
	v013StackA        = `{"services":{"app":{"image":"nginx:1.25","env":{"MODE":"prod"}}}}`
	v013StackB        = `{"services":{"app":{"image":"nginx:1.27","env":{"MODE":"prod"}}}}`
	// v013FlatA is v013StackA as a pre-stack flat payload; a v0.13 Restart row
	// re-encoded it as v013StackAReencoded (json.Marshal of the parsed stack).
	v013FlatA           = `{"image":"nginx:1.25","env":{"MODE":"prod"}}`
	v013StackAReencoded = `{"services":{"app":{"env":{"MODE":"prod"},"image":"nginx:1.25"}}}`
)

func v013Row(version int, status, image, manifest string) v013ReleaseWire {
	row := v013ReleaseWire{
		Version:   version,
		Image:     image,
		Status:    status,
		CreatedAt: time.Date(2026, 10, 16, 9, 0, 0, 0, time.UTC).Add(time.Duration(version) * time.Hour),
	}
	if manifest != "" {
		row.Manifest = []byte(manifest)
	}
	return row
}

// ENG-1639: every successful v0.13 Provision, including the re-provision that
// replaces a lease whose containers died at runtime or across a host reboot,
// appended an active row without superseding the older one. The dev rehearsal
// (ENG-1123, s059) produced the byte-equal pair for every lease after a host
// reset; readiness then refused the host with no supported way forward.
func TestStoppedAdoptionNormalizesV013DuplicateActiveHistories(t *testing.T) {
	for _, test := range []struct {
		name         string
		history      []v013ReleaseWire
		latestActive int
		want         []string
	}{
		{
			name: "re-provision after container loss leaves a byte-equal pair",
			history: []v013ReleaseWire{
				v013Row(1, "active", "stack", v013StackA),
				v013Row(2, "active", "stack", v013StackA),
			},
			latestActive: 2,
			want:         []string{"superseded", "active"},
		},
		{
			name: "repeated re-provisions leave three byte-equal actives",
			history: []v013ReleaseWire{
				v013Row(1, "active", "stack", v013StackA),
				v013Row(2, "active", "stack", v013StackA),
				v013Row(3, "active", "stack", v013StackA),
			},
			latestActive: 3,
			want:         []string{"superseded", "superseded", "active"},
		},
		{
			name: "re-provision after a restart replays the raw flat payload",
			history: []v013ReleaseWire{
				v013Row(1, "superseded", "stack", v013FlatA),
				v013Row(2, "active", "stack", v013StackAReencoded),
				v013Row(3, "active", "stack", v013FlatA),
			},
			latestActive: 3,
			want:         []string{"superseded", "superseded", "active"},
		},
		{
			name: "re-provision after a failed update deploys the payload update persisted",
			history: []v013ReleaseWire{
				v013Row(1, "active", "stack", v013StackA),
				func() v013ReleaseWire {
					row := v013Row(2, "failed", "stack", v013StackB)
					row.Reason, row.Message = backend.ReasonUpdateFailed, "update failed"
					return row
				}(),
				v013Row(3, "active", "stack", v013StackB),
			},
			latestActive: 3,
			want:         []string{"superseded", "failed", "active"},
		},
		{
			name: "re-provision after an unpersisted update replays the older payload",
			history: []v013ReleaseWire{
				v013Row(1, "superseded", "stack", v013StackA),
				v013Row(2, "active", "stack", v013StackB),
				v013Row(3, "active", "stack", v013StackA),
			},
			latestActive: 3,
			want:         []string{"superseded", "superseded", "active"},
		},
		{
			name: "re-provision after RecordMigration",
			history: []v013ReleaseWire{
				v013Row(1, "active", "nginx:1.25", v013FlatA),
				v013Row(2, "active", "stack", v013StackAReencoded),
				v013Row(3, "active", "stack", v013FlatA),
			},
			latestActive: 3,
			want:         []string{"superseded", "superseded", "active"},
		},
		{
			name: "deploying row stranded before the re-provision stays inert history",
			history: []v013ReleaseWire{
				v013Row(1, "active", "stack", v013StackA),
				v013Row(2, "deploying", "stack", v013StackB),
				v013Row(3, "active", "stack", v013StackB),
			},
			latestActive: 3,
			want:         []string{"superseded", "deploying", "active"},
		},
		{
			name: "re-provision duplicate followed by an equivalent deploying retry",
			history: []v013ReleaseWire{
				v013Row(1, "active", "stack", v013StackA),
				v013Row(2, "active", "stack", v013StackA),
				v013Row(3, "deploying", "stack", v013StackAReencoded),
			},
			latestActive: 2,
			want:         []string{"superseded", "active", "superseded"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "releases.db")
			writeV013ReleaseHistory(t, dbPath, v013AdoptionLease, test.history)

			_, err := InspectReleaseStoreReadOnly(dbPath)
			require.ErrorContains(t, err, "active records",
				"current runtime inspection must never accept duplicate active authority")

			// The cutover readiness gate (placement-preflight -inspect-releases).
			report, err := InspectReleaseAdmissionReadOnly(dbPath)
			require.NoError(t, err)
			require.Equal(t, 1, report.Histories)
			require.Equal(t, len(test.history), report.Releases)

			bound, err := BindAuthoritativeStorePath(dbPath)
			require.NoError(t, err)
			inspection, err := InspectBoundLegacyReleaseStoreReadOnly(bound)
			require.NoError(t, bound.Close())
			require.NoError(t, err)
			require.True(t, inspection.RequiresLegacyNormalization)
			require.Equal(t, test.latestActive, inspection.ActiveReleases[v013AdoptionLease].Version,
				"v0.13 LatestActive stays the authority")

			require.NoError(t, prepareExistingBoundStoreForTest(
				t, dbPath, PrepareBoundReleaseStoreStorage,
			))
			strict, err := InspectReleaseStoreReadOnly(dbPath)
			require.NoError(t, err)
			require.True(t, strict.IdentityBound)
			require.False(t, strict.RequiresLegacyNormalization)

			releases := readRawReleaseHistory(t, dbPath, v013AdoptionLease)
			require.Len(t, releases, len(test.history))
			statuses := make([]string, len(releases))
			for index, release := range releases {
				statuses[index] = release.Status
				assert.Equal(t, test.history[index].Version, release.Version)
				assert.Equal(t, test.history[index].Manifest, release.Manifest,
					"adoption changes statuses, never manifest bytes")
			}
			assert.Equal(t, test.want, statuses)
		})
	}
}

func TestStoppedAdoptionRefusesUnexplainedV013DuplicateActivesWithoutMutation(t *testing.T) {
	for _, test := range []struct {
		name    string
		history []v013ReleaseWire
		want    string
	}{
		{
			name: "latest active is not a stack image row",
			history: []v013ReleaseWire{
				v013Row(1, "active", "stack", v013StackA),
				v013Row(2, "active", "nginx:1.25", v013FlatA),
			},
			want: "latest active release 2",
		},
		{
			name: "older active image is not its flat manifest's image",
			history: []v013ReleaseWire{
				v013Row(1, "active", "redis:7", v013FlatA),
				v013Row(2, "active", "stack", v013StackA),
			},
			want: "older active release 1 image",
		},
		{
			name: "older stack manifest carries a concrete image name",
			history: []v013ReleaseWire{
				v013Row(1, "active", "nginx:1.25", v013StackA),
				v013Row(2, "active", "stack", v013StackA),
			},
			want: "older active release 1 image",
		},
		{
			name: "older active has no manifest",
			history: []v013ReleaseWire{
				v013Row(1, "active", "stack", ""),
				v013Row(2, "active", "stack", v013StackA),
			},
			want: "older active release 1 has no manifest",
		},
		{
			name: "duplicate actives with a differing deploying tail stay ambiguous",
			history: []v013ReleaseWire{
				v013Row(1, "active", "stack", v013StackA),
				v013Row(2, "active", "stack", v013StackA),
				v013Row(3, "deploying", "stack", v013StackB),
			},
			want: "differs from LatestActive",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "releases.db")
			writeV013ReleaseHistory(t, dbPath, v013AdoptionLease, test.history)
			requireStoppedAdoptionRefusedWithoutMutation(t, dbPath, test.want)
		})
	}
}

// The v0.13 recognizers accept only a v013ReleaseHistory, which only the v0.13
// array decoder mints. A current envelope with the very RecordMigration pair
// that adoption accepts from a v0.13 array is corrupt current authority.
func TestStoppedAdoptionNeverExtendsV013ToleranceToCurrentEnvelopes(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "releases.db")
	envelope, err := marshalReleaseHistory([]Release{
		{Version: 1, Manifest: []byte(v013FlatA), Image: "nginx:1.25", Status: "active"},
		{Version: 2, Manifest: []byte(v013StackAReencoded), Image: "stack", Status: "active"},
	})
	require.NoError(t, err)
	putRawReleaseValue(t, dbPath, v013AdoptionLease, envelope)

	bound, err := BindAuthoritativeStorePath(dbPath)
	require.NoError(t, err)
	_, inspectErr := InspectBoundLegacyReleaseStoreReadOnly(bound)
	require.NoError(t, bound.Close())
	require.ErrorContains(t, inspectErr, "release history has 2 active records")
	assert.NotContains(t, inspectErr.Error(), "v0.13",
		"a current envelope must never reach the v0.13 recognizers")

	_, err = InspectReleaseAdmissionReadOnly(dbPath)
	require.ErrorContains(t, err, "release history has 2 active records")
}

func requireStoppedAdoptionRefusedWithoutMutation(t *testing.T, dbPath, want string) {
	t.Helper()
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	_, err = InspectReleaseAdmissionReadOnly(dbPath)
	require.ErrorContains(t, err, want)
	bound, err := BindAuthoritativeStorePath(dbPath)
	require.NoError(t, err)
	_, inspectErr := InspectBoundLegacyReleaseStoreReadOnly(bound)
	require.NoError(t, bound.Close())
	require.ErrorContains(t, inspectErr, want)
	err = prepareExistingBoundStoreForTest(t, dbPath, PrepareBoundReleaseStoreStorage)
	require.ErrorContains(t, err, want)

	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, after,
		"a refused adoption must roll back both normalization and identity binding")
}
