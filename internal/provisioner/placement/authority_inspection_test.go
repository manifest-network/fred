package placement

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

const (
	authorityInspectionProvider = "550e8400-e29b-41d4-a716-446655440000"
	authorityInspectionLease    = "018f47a2-8b1c-7def-8123-456789abcdef"
	authorityInspectionStorage  = "550e8400-e29b-41d4-a716-446655440001"
)

func authorityInspectionExpectation(t *testing.T) AuthorityExpectation {
	t.Helper()
	expectation, err := NewAuthorityExpectation(
		authorityInspectionProvider, []string{"backend-a"},
	)
	require.NoError(t, err)
	return expectation
}

func TestNewAuthorityExpectation_IsCanonicalDetachedAndRequired(t *testing.T) {
	input := []string{"backend-b", "backend-a"}
	expectation, err := NewAuthorityExpectation(authorityInspectionProvider, input)
	require.NoError(t, err)
	input[0] = "changed"
	assert.Equal(t, []string{"backend-a", "backend-b"}, expectation.backendTopology)
	assert.True(t, expectation.valid())

	_, err = InspectAuthorityFile("/does/not/matter", AuthorityExpectation{})
	require.ErrorContains(t, err, "expectation is invalid")
}

func TestInspectAuthorityFile_AmbiguousPreparationOutcomes(t *testing.T) {
	t.Run("unchanged injected commit is pristine v0.13", func(t *testing.T) {
		path := writeAuthorityInspectionLegacyDB(t)
		before, err := os.ReadFile(path)
		require.NoError(t, err)

		commitErr := errors.New("synthetic commit outcome error before mutation")
		assert.Error(t, commitErr)
		after, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, before, after)

		report, err := InspectAuthorityFile(path, authorityInspectionExpectation(t))
		require.NoError(t, err)
		assert.Equal(t, AuthorityPristineV013, report.Classification)
		assert.True(t, report.SafeForCutover())
		assert.Equal(t, 1, report.Counts.PlacementRows)
		assert.Equal(t, 1, report.Counts.LegacyPlacementRows)
		assert.True(t, report.PhysicalCheckCompleted)
		assert.True(t, report.PhysicalCheckValid)
		assert.Equal(t, []string{"placements"}, report.TopLevelBuckets)
		require.Len(t, report.Rows, 1)
		assert.Equal(t, AuthorityRowFact{
			LeaseUUID: authorityInspectionLease, State: "confirmed",
			Backend: "backend-a", LifecycleVerdict: "not_present_v0_13",
		}, report.Rows[0])
		assert.Empty(t, report.Diagnostics)
	})

	t.Run("committed then synthetic outcome error is prepared current", func(t *testing.T) {
		path := writeAuthorityInspectionLegacyDB(t)
		db, err := bolt.Open(path, 0o600, nil)
		require.NoError(t, err)
		require.NoError(t, db.Update(prepareAuthorityInspectionTransaction))
		require.NoError(t, db.Close())
		commitErr := errors.New("synthetic error returned after durable commit")
		assert.Error(t, commitErr)

		report, err := InspectAuthorityFile(path, authorityInspectionExpectation(t))
		require.NoError(t, err)
		assert.Equal(t, AuthorityPreparedCurrent, report.Classification)
		assert.True(t, report.SafeForCutover())
		assert.Equal(t, uint64(topologyMetadataSchema), report.MetadataSchema)
		assert.Equal(t, 1, report.Counts.RevisionedPlacementRows)
		assert.Equal(t, 1, report.Counts.LifecycleRows)
		assert.Equal(t, []string{
			"placement_lifecycle_capabilities", "placement_metadata", "placements",
		}, report.TopLevelBuckets)
		assert.Equal(t, uint64(1), report.TopologyID)
		assert.Zero(t, report.BaselineTopologyID)
		assert.Zero(t, report.InventoryTopologyID)
		assert.Equal(t, []AuthorityStorageBinding{{
			Backend: "backend-a", StorageID: authorityInspectionStorage,
		}}, report.StorageBindings)
		require.Len(t, report.Rows, 1)
		assert.Equal(t, uint64(1), report.Rows[0].Revision)
		assert.Equal(t, "confirmed", report.Rows[0].State)
		assert.Equal(t, "backend-a", report.Rows[0].Backend)
		assert.Equal(t, "legacy_active", report.Rows[0].LifecycleVerdict)
		assert.Empty(t, report.Diagnostics)
	})
}

func TestInspectAuthorityFile_UnsafeClassifications(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*testing.T, string)
		want     AuthorityClassification
		wantCode string
	}{
		{
			name: "partial current bucket set",
			mutate: func(t *testing.T, path string) {
				mutateAuthorityInspectionDB(t, path, func(tx *bolt.Tx) error {
					_, err := tx.CreateBucket(lifecycleCapabilityBucketName)
					return err
				})
			},
			want:     AuthorityMixedOrIncomplete,
			wantCode: "mixed_bucket_set",
		},
		{
			name: "corrupt metadata",
			mutate: func(t *testing.T, path string) {
				prepareAuthorityInspectionDB(t, path)
				mutateAuthorityInspectionDB(t, path, func(tx *bolt.Tx) error {
					return tx.Bucket(metadataBucketName).Put(metadataStateKey, []byte(`{"schema":`))
				})
			},
			want:     AuthorityCorrupt,
			wantCode: "metadata_corrupt",
		},
		{
			name: "provider mismatch",
			mutate: func(t *testing.T, path string) {
				prepareAuthorityInspectionDB(t, path)
				mutateAuthorityInspectionDB(t, path, func(tx *bolt.Tx) error {
					metadata, err := loadTopologyMetadata(tx)
					if err != nil {
						return err
					}
					metadata.ProviderUUID = "1e1698c3-a922-460a-8296-70efdbc03032"
					return putTopologyMetadata(tx, metadata)
				})
			},
			want:     AuthorityMixedOrIncomplete,
			wantCode: "provider_mismatch",
		},
		{
			name: "topology mismatch",
			mutate: func(t *testing.T, path string) {
				prepareAuthorityInspectionDB(t, path)
				mutateAuthorityInspectionDB(t, path, func(tx *bolt.Tx) error {
					metadata, err := loadTopologyMetadata(tx)
					if err != nil {
						return err
					}
					metadata.Topology = []string{"backend-b"}
					metadata.TopologyFingerprint, err = topologyFingerprint(metadata.Topology)
					if err != nil {
						return err
					}
					metadata.KnownBackends = []string{"backend-a", "backend-b"}
					metadata.KnownBackendStorageIDs["backend-b"] =
						"1e1698c3-a922-460a-8296-70efdbc03032"
					return putTopologyMetadata(tx, metadata)
				})
			},
			want:     AuthorityMixedOrIncomplete,
			wantCode: "topology_mismatch",
		},
		{
			name: "storage identity missing",
			mutate: func(t *testing.T, path string) {
				prepareAuthorityInspectionDB(t, path)
				mutateAuthorityInspectionDB(t, path, func(tx *bolt.Tx) error {
					metadata, err := loadTopologyMetadata(tx)
					if err != nil {
						return err
					}
					metadata.KnownBackendStorageIDs = map[string]string{}
					encoded, err := json.Marshal(metadata)
					if err != nil {
						return err
					}
					return tx.Bucket(metadataBucketName).Put(metadataStateKey, encoded)
				})
			},
			want:     AuthorityMixedOrIncomplete,
			wantCode: "storage_identity_incomplete",
		},
		{
			name: "missing lifecycle row",
			mutate: func(t *testing.T, path string) {
				prepareAuthorityInspectionDB(t, path)
				mutateAuthorityInspectionDB(t, path, func(tx *bolt.Tx) error {
					return tx.Bucket(lifecycleCapabilityBucketName).Delete(
						[]byte(authorityInspectionLease),
					)
				})
			},
			want:     AuthorityMixedOrIncomplete,
			wantCode: "placement_lifecycle_missing",
		},
		{
			name: "mismatched lifecycle relationship",
			mutate: func(t *testing.T, path string) {
				prepareAuthorityInspectionDB(t, path)
				mutateAuthorityInspectionDB(t, path, func(tx *bolt.Tx) error {
					encoded, err := encodeLifecycleCapability(lifecycleCapability{
						backend: "backend-b",
					})
					if err != nil {
						return err
					}
					return tx.Bucket(lifecycleCapabilityBucketName).Put(
						[]byte(authorityInspectionLease), encoded,
					)
				})
			},
			want:     AuthorityMixedOrIncomplete,
			wantCode: "placement_lifecycle_mismatch",
		},
		{
			name: "corrupt lifecycle relationship",
			mutate: func(t *testing.T, path string) {
				prepareAuthorityInspectionDB(t, path)
				mutateAuthorityInspectionDB(t, path, func(tx *bolt.Tx) error {
					return tx.Bucket(lifecycleCapabilityBucketName).Put(
						[]byte(authorityInspectionLease), []byte(`{"schema":`),
					)
				})
			},
			want:     AuthorityCorrupt,
			wantCode: "undecodable_lifecycle_row",
		},
		{
			name: "oversized legacy placement value",
			mutate: func(t *testing.T, path string) {
				mutateAuthorityInspectionDB(t, path, func(tx *bolt.Tx) error {
					return tx.Bucket(bucketName).Put(
						[]byte(authorityInspectionLease),
						bytes.Repeat([]byte("x"), maxAuthorityRowValueBytes+1),
					)
				})
			},
			want:     AuthorityCorrupt,
			wantCode: "oversized_legacy_placement_row",
		},
		{
			name: "oversized current placement value",
			mutate: func(t *testing.T, path string) {
				prepareAuthorityInspectionDB(t, path)
				mutateAuthorityInspectionDB(t, path, func(tx *bolt.Tx) error {
					return tx.Bucket(bucketName).Put(
						[]byte(authorityInspectionLease),
						bytes.Repeat([]byte("x"), maxAuthorityRowValueBytes+1),
					)
				})
			},
			want:     AuthorityCorrupt,
			wantCode: "oversized_current_placement",
		},
		{
			name: "oversized lifecycle value",
			mutate: func(t *testing.T, path string) {
				prepareAuthorityInspectionDB(t, path)
				mutateAuthorityInspectionDB(t, path, func(tx *bolt.Tx) error {
					return tx.Bucket(lifecycleCapabilityBucketName).Put(
						[]byte(authorityInspectionLease),
						bytes.Repeat([]byte("x"), maxAuthorityLifecycleValueBytes+1),
					)
				})
			},
			want:     AuthorityCorrupt,
			wantCode: "oversized_lifecycle_row",
		},
		{
			name: "oversized metadata value",
			mutate: func(t *testing.T, path string) {
				prepareAuthorityInspectionDB(t, path)
				mutateAuthorityInspectionDB(t, path, func(tx *bolt.Tx) error {
					return tx.Bucket(metadataBucketName).Put(
						metadataStateKey,
						bytes.Repeat([]byte("x"), maxAuthorityMetadataValueBytes+1),
					)
				})
			},
			want:     AuthorityCorrupt,
			wantCode: "metadata_oversized",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := writeAuthorityInspectionLegacyDB(t)
			test.mutate(t, path)
			report, err := InspectAuthorityFile(path, authorityInspectionExpectation(t))
			require.NoError(t, err)
			assert.Equal(t, test.want, report.Classification)
			assert.False(t, report.SafeForCutover())
			assert.Contains(t, authorityDiagnosticCodes(report), test.wantCode)
		})
	}
}

func TestInspectAuthorityFile_TruncatedFileIsCorruptAndNeverReplaced(t *testing.T) {
	path := writeAuthorityInspectionLegacyDB(t)
	bytes, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Greater(t, len(bytes), os.Getpagesize())
	require.NoError(t, os.WriteFile(path, bytes[:os.Getpagesize()], 0o600))
	before, err := os.Stat(path)
	require.NoError(t, err)

	report, err := InspectAuthorityFile(path, authorityInspectionExpectation(t))
	require.NoError(t, err)
	assert.Equal(t, AuthorityCorrupt, report.Classification)
	assert.Contains(t, authorityDiagnosticCodes(report), "unreadable_bbolt")
	after, err := os.Stat(path)
	require.NoError(t, err)
	assert.True(t, os.SameFile(before, after))
	assert.Equal(t, before.Size(), after.Size())
}

func TestInspectAuthorityFile_LiveWriterLockIsEnvironmentalError(t *testing.T) {
	path := writeAuthorityInspectionLegacyDB(t)
	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	report, err := InspectAuthorityFile(path, authorityInspectionExpectation(t))
	require.Error(t, err)
	assert.ErrorIs(t, err, bolt.ErrTimeout)
	assert.Empty(t, report.Classification)
}

func TestInspectAuthorityFile_MissingPathIsEnvironmentalError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.db")
	report, err := InspectAuthorityFile(path, authorityInspectionExpectation(t))
	require.ErrorIs(t, err, os.ErrNotExist)
	assert.Empty(t, report.Classification)
}

func TestInspectAuthorityFile_PathReplacementCanNeverReturnSafe(t *testing.T) {
	path := writeAuthorityInspectionLegacyDB(t)
	replacement := writeAuthorityInspectionLegacyDB(t)
	displaced := filepath.Join(filepath.Dir(path), "displaced.db")

	report, err := inspectAuthorityFile(
		path,
		authorityInspectionExpectation(t),
		authorityInspectionHooks{afterOpen: func() {
			require.NoError(t, os.Rename(path, displaced))
			require.NoError(t, os.Rename(replacement, path))
		}},
	)
	require.ErrorContains(t, err, "path or inode changed")
	assert.Equal(t, AuthorityMixedOrIncomplete, report.Classification)
	assert.False(t, report.SafeForCutover())
	assert.Contains(t, authorityDiagnosticCodes(report), "environment_changed")
	_, statErr := os.Stat(displaced)
	require.NoError(t, statErr)
}

func TestAuthorityOpenErrorIsCorruption_PreservesEnvironmentalPathErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "short.db")
	require.NoError(t, os.WriteFile(path, []byte("short"), 0o600))
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.False(t, authorityOpenErrorIsCorruption(info, &os.PathError{
		Op: "open", Path: path, Err: os.ErrPermission,
	}))
	assert.True(t, authorityOpenErrorIsCorruption(info, bolt.ErrInvalid))
}

func TestInspectAuthorityFile_ReportNeverContainsDurableValues(t *testing.T) {
	path := writeAuthorityInspectionLegacyDB(t)
	prepareAuthorityInspectionDB(t, path)
	const secret = "https://callback.invalid/path?operation_id=secret-token"
	mutateAuthorityInspectionDB(t, path, func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Put(
			[]byte(authorityInspectionLease),
			[]byte(`{"backend":"backend-a","revision":1,"callback_url":"`+secret+`"`),
		)
	})

	report, err := InspectAuthorityFile(path, authorityInspectionExpectation(t))
	require.NoError(t, err)
	encoded, err := MarshalAuthorityReport(report)
	require.NoError(t, err)
	assert.Equal(t, AuthorityCorrupt, report.Classification)
	assert.NotContains(t, string(encoded), secret)
	assert.NotContains(t, string(encoded), "secret-token")
	assert.NotContains(t, string(encoded), "callback_url")
	assert.NotContains(t, string(encoded), "operation_id")
	assert.NotContains(t, string(encoded), "lifecycle_id")
}

func TestInspectAuthorityFile_RedactsValidTypedAttemptButReportsSafeState(t *testing.T) {
	path := writeAuthorityInspectionLegacyDB(t)
	prepareAuthorityInspectionDB(t, path)
	operationID, err := operation.ParseID("6ba7b810-9dad-41d1-80b4-00c04fd430c8")
	require.NoError(t, err)
	snapshot, err := NewBackendRequestSnapshot(
		"sensitive-tenant", authorityInspectionProvider,
		[]backend.LeaseItem{{SKU: "sensitive-sku", Quantity: 1}},
	)
	require.NoError(t, err)
	const callbackHost = "private-callback.example.invalid"
	callbackPair, err := NewCallbackPair(
		operationID,
		"https://"+callbackHost+"/callbacks/provision?operation_id="+operationID.String(),
		"https://"+callbackHost+"/callbacks/provision?lifecycle_id="+operationID.String(),
	)
	require.NoError(t, err)
	lifecycleID, err := lifecycleIDForOperation(operationID)
	require.NoError(t, err)
	mutateAuthorityInspectionDB(t, path, func(tx *bolt.Tx) error {
		encodedPlacement, encodeErr := encodePlacement(Placement{
			Attempt:                "backend-a",
			revision:               2,
			attemptOperationID:     operationID,
			attemptOperationKind:   operation.KindProvision,
			attemptRequestSnapshot: snapshot,
			attemptCallbackPair:    callbackPair,
		})
		if encodeErr != nil {
			return encodeErr
		}
		if err := tx.Bucket(bucketName).Put([]byte(authorityInspectionLease), encodedPlacement); err != nil {
			return err
		}
		encodedLifecycle, encodeErr := encodeLifecycleCapability(lifecycleCapability{
			attemptBackend: "backend-a",
			attemptID:      lifecycleID,
		})
		if encodeErr != nil {
			return encodeErr
		}
		return tx.Bucket(lifecycleCapabilityBucketName).Put(
			[]byte(authorityInspectionLease), encodedLifecycle,
		)
	})

	report, err := InspectAuthorityFile(path, authorityInspectionExpectation(t))
	require.NoError(t, err)
	assert.Equal(t, AuthorityPreparedCurrent, report.Classification)
	require.Len(t, report.Rows, 1)
	assert.Equal(t, "attempting", report.Rows[0].State)
	assert.Equal(t, "backend-a", report.Rows[0].Attempt)
	assert.Equal(t, "attempt_pending", report.Rows[0].LifecycleVerdict)
	encoded, err := json.Marshal(report)
	require.NoError(t, err)
	for _, secret := range []string{
		operationID.String(), callbackHost, "sensitive-tenant", "sensitive-sku",
	} {
		assert.NotContains(t, string(encoded), secret)
	}
}

func TestInspectAuthorityFile_ClassifiesPersistedUntrustedPositiveFact(t *testing.T) {
	tests := []struct {
		name               string
		placement          Placement
		topology           []string
		wantUntrusted      bool
		wantClassification AuthorityClassification
	}{
		{
			name: "sole rejected positive",
			placement: Placement{
				Backend:           "backend-a",
				Conflict:          true,
				ConflictBackends:  []string{"backend-a"},
				untrustedPositive: true,
				revision:          2,
			},
			topology:           []string{"backend-a"},
			wantUntrusted:      true,
			wantClassification: AuthorityPreparedCurrent,
		},
		{
			name: "ordinary conflict",
			placement: Placement{
				Backend:          "backend-a",
				Conflict:         true,
				ConflictBackends: []string{"backend-a", "backend-b"},
				revision:         2,
			},
			topology:           []string{"backend-a", "backend-b"},
			wantClassification: AuthorityPreparedCurrent,
		},
		{
			name: "legacy unknown-owner conflict",
			placement: Placement{
				Backend:               "backend-a",
				Conflict:              true,
				ConflictBackends:      []string{"backend-a"},
				ConflictOwnersUnknown: true,
				revision:              2,
			},
			topology:           []string{"backend-a"},
			wantClassification: AuthorityPreparedCurrent,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := writeAuthorityInspectionLegacyDB(t)
			prepareAuthorityInspectionDB(t, path)
			mutateAuthorityInspectionDB(t, path, func(tx *bolt.Tx) error {
				if len(test.topology) > 1 {
					metadata, err := loadTopologyMetadata(tx)
					if err != nil {
						return err
					}
					metadata.Topology = slices.Clone(test.topology)
					metadata.TopologyFingerprint, err = topologyFingerprint(test.topology)
					if err != nil {
						return err
					}
					metadata.KnownBackends = slices.Clone(test.topology)
					metadata.KnownBackendStorageIDs["backend-b"] =
						"6ba7b811-9dad-41d1-80b4-00c04fd430c8"
					if err := putTopologyMetadata(tx, metadata); err != nil {
						return err
					}
				}
				encoded, err := encodePlacement(test.placement)
				if err != nil {
					return err
				}
				return tx.Bucket(bucketName).Put(
					[]byte(authorityInspectionLease), encoded,
				)
			})
			expectation, err := NewAuthorityExpectation(
				authorityInspectionProvider, test.topology,
			)
			require.NoError(t, err)

			report, err := InspectAuthorityFile(path, expectation)
			require.NoError(t, err)
			assert.Equal(t, test.wantClassification, report.Classification)
			require.Len(t, report.Rows, 1)
			assert.Equal(t, test.wantUntrusted, report.Rows[0].UntrustedPositive)

			encoded, err := json.Marshal(report.Rows[0])
			require.NoError(t, err)
			assert.Contains(t, string(encoded), fmt.Sprintf(
				`"untrusted_positive":%t`, test.wantUntrusted,
			))
		})
	}
}

func TestInspectAuthorityFile_RowFactsAreDeterministicAndBounded(t *testing.T) {
	path := writeAuthorityInspectionLegacyDB(t)
	mutateAuthorityInspectionDB(t, path, func(tx *bolt.Tx) error {
		placements := tx.Bucket(bucketName)
		for index := 1; index <= maxAuthorityRows+5; index++ {
			leaseUUID := fmt.Sprintf("00000000-0000-4000-8000-%012d", index)
			if err := placements.Put([]byte(leaseUUID), []byte("backend-a")); err != nil {
				return err
			}
		}
		return nil
	})

	report, err := InspectAuthorityFile(path, authorityInspectionExpectation(t))
	require.NoError(t, err)
	assert.Equal(t, AuthorityPristineV013, report.Classification)
	assert.Len(t, report.Rows, maxAuthorityRows)
	assert.Equal(t, 6, report.RowsOmitted)
	assert.True(t, slices.IsSortedFunc(report.Rows, func(left, right AuthorityRowFact) int {
		return strings.Compare(left.LeaseUUID, right.LeaseUUID)
	}))
	firstJSON, err := json.Marshal(report)
	require.NoError(t, err)
	second, err := InspectAuthorityFile(path, authorityInspectionExpectation(t))
	require.NoError(t, err)
	secondJSON, err := json.Marshal(second)
	require.NoError(t, err)
	assert.Equal(t, firstJSON, secondJSON)
}

func TestInspectAuthorityFile_AllCollectionsAreBoundedAndDeterministic(t *testing.T) {
	path := writeAuthorityInspectionLegacyDB(t)
	prepareAuthorityInspectionDB(t, path)
	names := []string{"backend-a", strings.Repeat("long-backend-", 50)}
	for index := 0; len(names) < maxAuthorityCollectionEntries+7; index++ {
		names = append(names, fmt.Sprintf("backend-%03d", index))
	}
	slices.Sort(names)
	storageIDs := make(map[string]string, len(names))
	for index, backendName := range names {
		storageIDs[backendName] = fmt.Sprintf(
			"00000000-0000-4000-8000-%012x", index+1,
		)
	}
	mutateAuthorityInspectionDB(t, path, func(tx *bolt.Tx) error {
		fingerprint, err := topologyFingerprint(names)
		if err != nil {
			return err
		}
		return putTopologyMetadata(tx, topologyMetadata{
			Schema:                 topologyMetadataSchema,
			ProviderUUID:           authorityInspectionProvider,
			Topology:               names,
			TopologyFingerprint:    fingerprint,
			KnownBackends:          names,
			KnownBackendStorageIDs: storageIDs,
			TopologyID:             1,
			InventoryTopologyID:    1,
			EmptyInventoryBackends: names,
		})
	})
	expectation, err := NewAuthorityExpectation(authorityInspectionProvider, names)
	require.NoError(t, err,
		"the classifier must not invent a backend-name length limit")

	first, err := InspectAuthorityFile(path, expectation)
	require.NoError(t, err)
	assert.Equal(t, AuthorityPreparedCurrent, first.Classification)
	for collection, got := range map[string]int{
		"expected topology": len(first.ExpectedBackendTopology),
		"observed topology": len(first.ObservedBackendTopology),
		"known topology":    len(first.KnownBackendTopology),
		"storage bindings":  len(first.StorageBindings),
		"empty inventories": len(first.EmptyInventoryBackends),
	} {
		assert.LessOrEqual(t, got, maxAuthorityCollectionEntries, collection)
	}
	wantOmitted := len(names) - maxAuthorityCollectionEntries
	assert.Equal(t, wantOmitted, first.ExpectedBackendTopologyOmitted)
	assert.Equal(t, wantOmitted, first.ObservedBackendTopologyOmitted)
	assert.Equal(t, wantOmitted, first.KnownBackendTopologyOmitted)
	assert.Equal(t, wantOmitted, first.StorageBindingsOmitted)
	assert.Equal(t, wantOmitted, first.EmptyInventoryBackendsOmitted)

	firstJSON, err := MarshalAuthorityReport(first)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(firstJSON), MaxAuthorityReportBytes)
	second, err := InspectAuthorityFile(path, expectation)
	require.NoError(t, err)
	secondJSON, err := MarshalAuthorityReport(second)
	require.NoError(t, err)
	assert.Equal(t, firstJSON, secondJSON)
}

func TestAuthorityReportBoundsPerRowIdentitiesAndEncodedSize(t *testing.T) {
	assessment := newAuthorityAssessment(authorityInspectionExpectation(t))
	assessment.addReportableRow()
	conflicts := make([]string, 0, maxAuthorityConflictBackends+3)
	for index := 0; index < maxAuthorityConflictBackends+3; index++ {
		conflicts = append(conflicts, fmt.Sprintf("backend-%03d", index))
	}
	assessment.setRow(AuthorityRowFact{
		LeaseUUID:        authorityInspectionLease,
		State:            "conflict",
		Backend:          strings.Repeat("b", maxAuthorityRenderedIdentity+1),
		Attempt:          strings.Repeat("a", maxAuthorityRenderedIdentity+1),
		Conflict:         true,
		ConflictBackends: conflicts,
		LifecycleVerdict: "mismatched",
	})
	report := assessment.finish(AuthorityMixedOrIncomplete)
	require.Len(t, report.Rows, 1)
	assert.Len(t, report.Rows[0].ConflictBackends, maxAuthorityConflictBackends)
	assert.Equal(t, 3, report.Rows[0].ConflictBackendsOmitted)
	assert.Equal(t, 2, report.Rows[0].IdentityFieldsOmitted)
	assert.Empty(t, report.Rows[0].Backend)
	assert.Empty(t, report.Rows[0].Attempt)
	encoded, err := MarshalAuthorityReport(report)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(encoded), MaxAuthorityReportBytes)

	_, err = MarshalAuthorityReport(AuthorityReport{
		ExpectedProviderUUID: strings.Repeat("x", MaxAuthorityReportBytes),
	})
	require.ErrorContains(t, err, "output limit")
}

func TestInspectAuthorityFile_LongCanonicalBackendNameIsSafeButOmitted(t *testing.T) {
	path := writeAuthorityInspectionLegacyDB(t)
	longName := strings.Repeat("backend", 100)
	mutateAuthorityInspectionDB(t, path, func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Put(
			[]byte(authorityInspectionLease), []byte(longName),
		)
	})
	expectation, err := NewAuthorityExpectation(
		authorityInspectionProvider, []string{longName},
	)
	require.NoError(t, err)

	report, err := InspectAuthorityFile(path, expectation)
	require.NoError(t, err)
	assert.Equal(t, AuthorityPristineV013, report.Classification)
	assert.Equal(t, 1, report.ExpectedBackendTopologyOmitted)
	require.Len(t, report.Rows, 1)
	assert.Empty(t, report.Rows[0].Backend)
	assert.Equal(t, 1, report.Rows[0].IdentityFieldsOmitted)
}

func writeAuthorityInspectionLegacyDB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "placements.db")
	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		placements, err := tx.CreateBucket(bucketName)
		if err != nil {
			return err
		}
		return placements.Put(
			[]byte(authorityInspectionLease),
			[]byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
		)
	}))
	require.NoError(t, db.Close())
	return path
}

func prepareAuthorityInspectionDB(t *testing.T, path string) {
	t.Helper()
	mutateAuthorityInspectionDB(t, path, prepareAuthorityInspectionTransaction)
}

func prepareAuthorityInspectionTransaction(tx *bolt.Tx) error {
	epoch, err := captureLifecycleInitializationEpoch(tx)
	if err != nil {
		return err
	}
	if err := initializeMetadata(tx); err != nil {
		return err
	}
	fingerprint, err := topologyFingerprint([]string{"backend-a"})
	if err != nil {
		return err
	}
	if err := putTopologyMetadata(tx, topologyMetadata{
		Schema:                 topologyMetadataSchema,
		ProviderUUID:           authorityInspectionProvider,
		Topology:               []string{"backend-a"},
		TopologyFingerprint:    fingerprint,
		KnownBackends:          []string{"backend-a"},
		KnownBackendStorageIDs: map[string]string{"backend-a": authorityInspectionStorage},
		TopologyID:             1,
	}); err != nil {
		return err
	}
	if err := initializeLifecycleCapabilities(tx, epoch); err != nil {
		return err
	}
	return migrateLegacyConfirmedRevisions(tx)
}

func mutateAuthorityInspectionDB(
	t *testing.T,
	path string,
	mutate func(*bolt.Tx) error,
) {
	t.Helper()
	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(mutate))
	require.NoError(t, db.Close())
}

func authorityDiagnosticCodes(report AuthorityReport) []string {
	codes := make([]string, 0, len(report.Diagnostics))
	for _, diagnostic := range report.Diagnostics {
		codes = append(codes, diagnostic.Code)
	}
	return codes
}
