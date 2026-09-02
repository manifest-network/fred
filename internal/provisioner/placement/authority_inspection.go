package placement

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/backendname"
)

// AuthorityClassification is the schema-neutral result of inspecting one
// stopped placement authority file. Only PristineV013 and PreparedCurrent are
// safe answers to an indeterminate legacy-preparation commit.
type AuthorityClassification string

const (
	AuthorityPristineV013      AuthorityClassification = "pristine_v0_13"
	AuthorityPreparedCurrent   AuthorityClassification = "prepared_current"
	AuthorityMixedOrIncomplete AuthorityClassification = "mixed_or_incomplete"
	AuthorityCorrupt           AuthorityClassification = "corrupt"
)

const maxAuthorityDiagnostics = 16

const maxAuthorityRows = 128

const (
	maxAuthorityCollectionEntries   = 64
	maxAuthorityConflictBackends    = 4
	maxAuthorityRenderedIdentity    = 512
	maxAuthorityMetadataValueBytes  = 4 << 20
	maxAuthorityRowValueBytes       = 1 << 20
	maxAuthorityLifecycleValueBytes = 256 << 10

	// MaxAuthorityReportBytes is the hard encoded-size ceiling for the offline
	// schema classifier. The report is an emergency diagnostic for potentially
	// damaged local state, so no durable value may make its output unbounded.
	MaxAuthorityReportBytes = 1 << 20
)

// AuthorityExpectation is the constructor-validated provider and complete
// configured backend topology against which durable authority is classified.
// Its zero value is invalid.
type AuthorityExpectation struct {
	providerUUID    string
	backendTopology []string
}

// NewAuthorityExpectation validates and detaches the exact configured
// provider/topology expected by offline classification. The zero value cannot
// authorize an inspection.
func NewAuthorityExpectation(
	providerUUID string,
	backendTopology []string,
) (AuthorityExpectation, error) {
	if !canonicalLeaseUUID(providerUUID) {
		return AuthorityExpectation{}, fmt.Errorf(
			"%w: expected provider UUID is not canonical",
			ErrProviderAuthorityMismatch,
		)
	}
	canonicalTopology, err := canonicalBackendTopology(backendTopology)
	if err != nil {
		return AuthorityExpectation{}, err
	}
	return AuthorityExpectation{
		providerUUID:    providerUUID,
		backendTopology: canonicalTopology,
	}, nil
}

func (expectation AuthorityExpectation) valid() bool {
	if !canonicalLeaseUUID(expectation.providerUUID) ||
		len(expectation.backendTopology) == 0 ||
		!slices.IsSorted(expectation.backendTopology) ||
		!authorityBackendNamesSafe(expectation.backendTopology) {
		return false
	}
	canonical, err := canonicalBackendTopology(expectation.backendTopology)
	return err == nil && slices.Equal(canonical, expectation.backendTopology)
}

// AuthorityCounts contains only bounded, non-secret structural facts. In
// particular, the classifier never emits placement values, callback URLs,
// operation tokens, or lifecycle tokens.
type AuthorityCounts struct {
	TopLevelBuckets         int `json:"top_level_buckets"`
	PlacementRows           int `json:"placement_rows"`
	LegacyPlacementRows     int `json:"legacy_placement_rows"`
	RevisionedPlacementRows int `json:"revisioned_placement_rows"`
	UnusablePlacementRows   int `json:"unusable_placement_rows"`
	LifecycleRows           int `json:"lifecycle_rows"`
	UnusableLifecycleRows   int `json:"unusable_lifecycle_rows"`
	DetachedLifecycleRows   int `json:"detached_lifecycle_rows"`
}

// AuthorityDiagnostic is a fixed, non-secret explanation of an unsafe
// classification. Diagnostics are de-duplicated and bounded.
type AuthorityDiagnostic struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// AuthorityStorageBinding is one non-secret immutable backend storage
// identity from strictly decoded metadata.
type AuthorityStorageBinding struct {
	Backend   string `json:"backend"`
	StorageID string `json:"storage_id"`
}

// AuthorityRowFact is a bounded, redacted per-lease authority summary. Typed
// operation and lifecycle IDs, callback destinations, payloads, and raw values
// are deliberately absent.
type AuthorityRowFact struct {
	LeaseUUID               string   `json:"lease_uuid"`
	State                   string   `json:"state"`
	Revision                uint64   `json:"revision"`
	Backend                 string   `json:"backend,omitempty"`
	Attempt                 string   `json:"attempt,omitempty"`
	Conflict                bool     `json:"conflict,omitempty"`
	UntrustedPositive       bool     `json:"untrusted_positive"`
	ConflictBackends        []string `json:"conflict_backends,omitempty"`
	ConflictBackendsOmitted int      `json:"conflict_backends_omitted,omitempty"`
	IdentityFieldsOmitted   int      `json:"identity_fields_omitted,omitempty"`
	ConflictOwnersUnknown   bool     `json:"conflict_owners_unknown,omitempty"`
	LifecycleVerdict        string   `json:"lifecycle_verdict"`
}

// AuthorityReport describes the durable schema found at one stable path and
// inode. Observed provider/topology are safe identities copied only from
// strictly decoded current metadata.
type AuthorityReport struct {
	Classification                 AuthorityClassification   `json:"classification"`
	ExpectedProviderUUID           string                    `json:"expected_provider_uuid"`
	ExpectedBackendTopology        []string                  `json:"expected_backend_topology"`
	ExpectedBackendTopologyOmitted int                       `json:"expected_backend_topology_omitted,omitempty"`
	TopLevelBuckets                []string                  `json:"top_level_buckets"`
	UnexpectedBucketCount          int                       `json:"unexpected_bucket_count,omitempty"`
	PhysicalCheckCompleted         bool                      `json:"physical_check_completed"`
	PhysicalCheckValid             bool                      `json:"physical_check_valid"`
	ObservedProviderUUID           string                    `json:"observed_provider_uuid,omitempty"`
	ObservedBackendTopology        []string                  `json:"observed_backend_topology,omitempty"`
	ObservedBackendTopologyOmitted int                       `json:"observed_backend_topology_omitted,omitempty"`
	KnownBackendTopology           []string                  `json:"known_backend_topology,omitempty"`
	KnownBackendTopologyOmitted    int                       `json:"known_backend_topology_omitted,omitempty"`
	StorageBindings                []AuthorityStorageBinding `json:"storage_bindings,omitempty"`
	StorageBindingsOmitted         int                       `json:"storage_bindings_omitted,omitempty"`
	MetadataSchema                 uint64                    `json:"metadata_schema,omitempty"`
	TopologyID                     uint64                    `json:"topology_id,omitempty"`
	TopologyFingerprint            string                    `json:"topology_fingerprint,omitempty"`
	BaselineTopologyID             uint64                    `json:"baseline_topology_id,omitempty"`
	BaselineFingerprint            string                    `json:"baseline_fingerprint,omitempty"`
	InventoryTopologyID            uint64                    `json:"inventory_topology_id,omitempty"`
	EmptyInventoryBackends         []string                  `json:"empty_inventory_backends,omitempty"`
	EmptyInventoryBackendsOmitted  int                       `json:"empty_inventory_backends_omitted,omitempty"`
	Counts                         AuthorityCounts           `json:"counts"`
	Rows                           []AuthorityRowFact        `json:"rows,omitempty"`
	RowsOmitted                    int                       `json:"rows_omitted,omitempty"`
	Diagnostics                    []AuthorityDiagnostic     `json:"diagnostics,omitempty"`
	DiagnosticsOmitted             int                       `json:"diagnostics_omitted,omitempty"`
}

// SafeForCutover reports whether the stopped file is wholly on one side of the
// v0.13 preparation boundary. It says nothing about external drain evidence.
func (report AuthorityReport) SafeForCutover() bool {
	return report.Classification == AuthorityPristineV013 ||
		report.Classification == AuthorityPreparedCurrent
}

type authorityAssessment struct {
	report         AuthorityReport
	seen           map[string]struct{}
	rows           map[string]AuthorityRowFact
	reportableRows int
	mixed          bool
	corrupt        bool
}

func newAuthorityAssessment(expectation AuthorityExpectation) *authorityAssessment {
	expected, expectedOmitted := boundedAuthorityIdentities(expectation.backendTopology)
	return &authorityAssessment{
		report: AuthorityReport{
			ExpectedProviderUUID:           expectation.providerUUID,
			ExpectedBackendTopology:        expected,
			ExpectedBackendTopologyOmitted: expectedOmitted,
		},
		seen: make(map[string]struct{}),
		rows: make(map[string]AuthorityRowFact),
	}
}

func (assessment *authorityAssessment) add(
	corrupt bool,
	code, message string,
) {
	if corrupt {
		assessment.corrupt = true
	} else {
		assessment.mixed = true
	}
	if _, duplicate := assessment.seen[code]; duplicate {
		return
	}
	assessment.seen[code] = struct{}{}
	if len(assessment.report.Diagnostics) >= maxAuthorityDiagnostics {
		assessment.report.DiagnosticsOmitted++
		return
	}
	assessment.report.Diagnostics = append(
		assessment.report.Diagnostics,
		AuthorityDiagnostic{Code: code, Message: message},
	)
}

func (assessment *authorityAssessment) mixedFinding(code, message string) {
	assessment.add(false, code, message)
}

func (assessment *authorityAssessment) corruptFinding(code, message string) {
	assessment.add(true, code, message)
}

func (assessment *authorityAssessment) addReportableRow() {
	assessment.reportableRows++
}

// setRow retains the lexicographically first bounded set, independent of scan
// order. This keeps a corrupt local database from turning diagnostic output
// construction into unbounded memory growth.
func (assessment *authorityAssessment) setRow(row AuthorityRowFact) {
	if row.LeaseUUID == "" {
		return
	}
	row = boundedAuthorityRow(row)
	if _, exists := assessment.rows[row.LeaseUUID]; exists {
		assessment.rows[row.LeaseUUID] = row
		return
	}
	if len(assessment.rows) < maxAuthorityRows {
		assessment.rows[row.LeaseUUID] = row
		return
	}
	maxLeaseUUID := ""
	for leaseUUID := range assessment.rows {
		if leaseUUID > maxLeaseUUID {
			maxLeaseUUID = leaseUUID
		}
	}
	if row.LeaseUUID >= maxLeaseUUID {
		return
	}
	delete(assessment.rows, maxLeaseUUID)
	assessment.rows[row.LeaseUUID] = row
}

func (assessment *authorityAssessment) finish(defaultClass AuthorityClassification) AuthorityReport {
	switch {
	case assessment.corrupt:
		assessment.report.Classification = AuthorityCorrupt
	case assessment.mixed:
		assessment.report.Classification = AuthorityMixedOrIncomplete
	case assessment.report.Classification == "":
		assessment.report.Classification = defaultClass
	}
	rowKeys := make([]string, 0, len(assessment.rows))
	for leaseUUID := range assessment.rows {
		rowKeys = append(rowKeys, leaseUUID)
	}
	slices.Sort(rowKeys)
	assessment.report.RowsOmitted = max(0, assessment.reportableRows-len(rowKeys))
	assessment.report.Rows = make([]AuthorityRowFact, 0, len(rowKeys))
	for _, leaseUUID := range rowKeys {
		row := assessment.rows[leaseUUID]
		assessment.report.Rows = append(assessment.report.Rows, row)
	}
	return assessment.report
}

func boundedAuthorityRow(row AuthorityRowFact) AuthorityRowFact {
	if row.Backend != "" && len(row.Backend) > maxAuthorityRenderedIdentity {
		row.Backend = ""
		row.IdentityFieldsOmitted++
	}
	if row.Attempt != "" && len(row.Attempt) > maxAuthorityRenderedIdentity {
		row.Attempt = ""
		row.IdentityFieldsOmitted++
	}
	var omitted int
	row.ConflictBackends, omitted = boundedAuthorityIdentityValues(
		row.ConflictBackends,
		maxAuthorityConflictBackends,
	)
	row.ConflictBackendsOmitted += omitted
	return row
}

func boundedAuthorityIdentities(values []string) ([]string, int) {
	return boundedAuthorityIdentityValues(values, maxAuthorityCollectionEntries)
}

func boundedAuthorityIdentityValues(values []string, limit int) ([]string, int) {
	if len(values) == 0 {
		return nil, 0
	}
	bounded := make([]string, 0, min(len(values), limit))
	omitted := 0
	for _, value := range values {
		if len(value) > maxAuthorityRenderedIdentity || len(bounded) >= limit {
			omitted++
			continue
		}
		bounded = append(bounded, value)
	}
	return bounded, omitted
}

// MarshalAuthorityReport encodes one deterministic report and enforces the
// public output ceiling even if a future field misses its construction bound.
func MarshalAuthorityReport(report AuthorityReport) ([]byte, error) {
	encoded, err := json.Marshal(report)
	if err != nil {
		return nil, fmt.Errorf("encode placement authority report: %w", err)
	}
	if len(encoded)+1 > MaxAuthorityReportBytes {
		return nil, fmt.Errorf(
			"placement authority report exceeds %d-byte output limit",
			MaxAuthorityReportBytes,
		)
	}
	return append(encoded, '\n'), nil
}

// InspectAuthorityFile classifies an existing stopped placement database
// without creating, replacing, or mutating it. The path must remain bound to
// the same regular inode from the initial stat through close. Missing files,
// permission/lock failures, path replacement, and other environmental failures
// are returned as errors. Durable format or semantic damage is instead
// represented by a Corrupt or MixedOrIncomplete report.
func InspectAuthorityFile(
	path string,
	expectation AuthorityExpectation,
) (AuthorityReport, error) {
	return inspectAuthorityFile(path, expectation, authorityInspectionHooks{})
}

type authorityInspectionHooks struct {
	afterOpen func()
}

func inspectAuthorityFile(
	path string,
	expectation AuthorityExpectation,
	hooks authorityInspectionHooks,
) (report AuthorityReport, resultErr error) {
	if !expectation.valid() {
		return AuthorityReport{}, errors.New("placement authority expectation is invalid")
	}
	assessment := newAuthorityAssessment(expectation)

	authority, err := bindOfflinePlacementAuthority(path)
	if err != nil {
		return AuthorityReport{}, err
	}
	defer func() {
		if closeErr := authority.close(); closeErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("close placement authority parent: %w", closeErr))
		}
	}()
	initialInfo := authority.info
	if initialInfo.Size() == 0 {
		assessment.corruptFinding(
			"empty_file",
			"the placement authority file is empty",
		)
		return finishUnopenedAuthorityReport(
			authority, assessment.finish(AuthorityCorrupt),
		)
	}

	db, err := authority.openBolt(true)
	if err != nil {
		if errors.Is(err, bolt.ErrTimeout) {
			return AuthorityReport{}, fmt.Errorf(
				"lock stopped placement authority read-only (is providerd still running?): %w",
				err,
			)
		}
		if authorityOpenErrorIsCorruption(initialInfo, err) {
			assessment.corruptFinding(
				"unreadable_bbolt",
				"the file is not a readable bbolt placement authority",
			)
			return finishUnopenedAuthorityReport(
				authority, assessment.finish(AuthorityCorrupt),
			)
		}
		return AuthorityReport{}, fmt.Errorf("open placement authority read-only: %w", err)
	}
	if hooks.afterOpen != nil {
		hooks.afterOpen()
	}

	assessment.report.PhysicalCheckCompleted = true
	if err := verifyBoltPhysicalConsistency(db); err != nil {
		if errors.Is(err, ErrPhysicalConsistency) {
			assessment.corruptFinding(
				"physical_corruption",
				"bbolt physical consistency validation failed",
			)
		} else {
			_ = db.Close()
			return AuthorityReport{}, fmt.Errorf("validate placement authority physically: %w", err)
		}
	} else {
		assessment.report.PhysicalCheckValid = true
		if err := db.View(func(tx *bolt.Tx) error {
			inspectAuthorityTransaction(tx, expectation, assessment)
			return nil
		}); err != nil {
			assessment.corruptFinding(
				"logical_read_failure",
				"the placement authority could not be traversed safely",
			)
		}
	}

	report = assessment.finish(AuthorityMixedOrIncomplete)
	statErr := authority.verify()
	closeErr := db.Close()
	afterCloseErr := authority.verify()
	if statErr != nil || closeErr != nil || afterCloseErr != nil {
		var environmental []error
		if statErr != nil {
			environmental = append(environmental, fmt.Errorf("restat placement authority before close: %w", statErr))
		}
		if closeErr != nil {
			environmental = append(environmental, fmt.Errorf("close placement authority: %w", closeErr))
		}
		if afterCloseErr != nil {
			environmental = append(environmental, fmt.Errorf("restat placement authority after close: %w", afterCloseErr))
		}
		return authorityReportWithUnstableEnvironment(report), errors.Join(environmental...)
	}
	return report, nil
}

func finishUnopenedAuthorityReport(
	authority *offlinePlacementAuthority,
	report AuthorityReport,
) (AuthorityReport, error) {
	if err := authority.verify(); err != nil {
		return authorityReportWithUnstableEnvironment(report),
			fmt.Errorf("reverify unreadable placement authority: %w", err)
	}
	return report, nil
}

func authorityReportWithUnstableEnvironment(report AuthorityReport) AuthorityReport {
	if !report.SafeForCutover() {
		return report
	}
	report.Classification = AuthorityMixedOrIncomplete
	if len(report.Diagnostics) < maxAuthorityDiagnostics {
		report.Diagnostics = append(report.Diagnostics, AuthorityDiagnostic{
			Code:    "environment_changed",
			Message: "the inspected path or file environment changed before completion",
		})
	} else {
		report.DiagnosticsOmitted++
	}
	return report
}

func authorityOpenErrorIsCorruption(info os.FileInfo, err error) bool {
	var pathError *os.PathError
	if errors.As(err, &pathError) || errors.Is(err, os.ErrNotExist) ||
		errors.Is(err, os.ErrPermission) {
		return false
	}
	if errors.Is(err, bolt.ErrInvalid) || errors.Is(err, bolt.ErrVersionMismatch) ||
		errors.Is(err, bolt.ErrChecksum) {
		return true
	}
	// A valid bbolt file contains two meta pages. This also classifies bbolt's
	// unexported "file size too small" result without matching error text.
	return info != nil && info.Size() < int64(2*os.Getpagesize())
}

func inspectAuthorityTransaction(
	tx *bolt.Tx,
	expectation AuthorityExpectation,
	assessment *authorityAssessment,
) {
	var hasPlacements, hasLifecycle, hasMetadata bool
	if err := tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
		assessment.report.Counts.TopLevelBuckets++
		switch {
		case bytes.Equal(name, bucketName):
			hasPlacements = true
			assessment.report.TopLevelBuckets = append(
				assessment.report.TopLevelBuckets, string(name),
			)
		case bytes.Equal(name, lifecycleCapabilityBucketName):
			hasLifecycle = true
			assessment.report.TopLevelBuckets = append(
				assessment.report.TopLevelBuckets, string(name),
			)
		case bytes.Equal(name, metadataBucketName):
			hasMetadata = true
			assessment.report.TopLevelBuckets = append(
				assessment.report.TopLevelBuckets, string(name),
			)
		default:
			assessment.report.UnexpectedBucketCount++
		}
		return nil
	}); err != nil {
		assessment.corruptFinding(
			"top_level_scan_failed",
			"top-level authority buckets could not be traversed",
		)
		return
	}
	slices.Sort(assessment.report.TopLevelBuckets)

	legacyShape := assessment.report.Counts.TopLevelBuckets == 1 && hasPlacements
	currentShape := assessment.report.Counts.TopLevelBuckets == 3 &&
		hasPlacements && hasLifecycle && hasMetadata

	if !legacyShape && !currentShape {
		assessment.mixedFinding(
			"mixed_bucket_set",
			"top-level buckets do not form one complete supported authority schema",
		)
	}
	if !hasPlacements {
		assessment.mixedFinding(
			"placements_bucket_missing",
			"the placements bucket is missing",
		)
	}

	if legacyShape {
		assessment.report.Classification = AuthorityPristineV013
		inspectLegacyPlacementRows(tx.Bucket(bucketName), expectation, assessment)
		return
	}
	if currentShape {
		assessment.report.Classification = AuthorityPreparedCurrent
	}

	metadata, metadataDecoded := inspectCurrentMetadata(
		tx.Bucket(metadataBucketName), expectation, assessment,
	)
	placementBucket := tx.Bucket(bucketName)
	lifecycleBucket := tx.Bucket(lifecycleCapabilityBucketName)
	inspectCurrentPlacementRows(
		placementBucket, lifecycleBucket, metadata, metadataDecoded, assessment,
	)
	inspectCurrentLifecycleRows(
		lifecycleBucket, metadata, metadataDecoded, placementBucket, assessment,
	)
}

func inspectLegacyPlacementRows(
	bucket *bolt.Bucket,
	expectation AuthorityExpectation,
	assessment *authorityAssessment,
) {
	configured := stringSet(expectation.backendTopology)
	if err := bucket.ForEach(func(key, value []byte) error {
		assessment.report.Counts.PlacementRows++
		leaseUUID, canonicalKey := authorityLeaseKey(key)
		if value == nil || !canonicalKey {
			assessment.corruptFinding(
				"invalid_legacy_placement_row",
				"a v0.13 placement row has an invalid key or nested value",
			)
			return nil
		}
		assessment.addReportableRow()
		row := AuthorityRowFact{
			LeaseUUID:        leaseUUID,
			State:            "unusable",
			LifecycleVerdict: "not_present_v0_13",
		}
		if len(value) > maxAuthorityRowValueBytes {
			assessment.corruptFinding(
				"oversized_legacy_placement_row",
				"a v0.13 placement row exceeds the safe decoder bound",
			)
			assessment.setRow(row)
			return nil
		}
		if revision, object, malformed := authorityRevisionHeader(value); object {
			row.Revision = revision
			switch {
			case malformed:
				assessment.corruptFinding(
					"malformed_legacy_placement_json",
					"a v0.13 placement JSON row is malformed",
				)
				assessment.setRow(row)
				return nil
			case revision != 0:
				assessment.report.Counts.RevisionedPlacementRows++
				assessment.mixedFinding(
					"revisioned_row_in_legacy_shape",
					"a revisioned placement exists without the complete current schema",
				)
				row.State = "mixed_revisioned"
				assessment.setRow(row)
				return nil
			}
		}
		backendName, err := decodeV013PreflightPlacement(value)
		if err != nil {
			if json.Valid(value) {
				assessment.mixedFinding(
					"non_v013_row_in_legacy_shape",
					"a valid JSON placement is not a v0.13 confirmed-owner row",
				)
			} else {
				assessment.corruptFinding(
					"undecodable_legacy_placement",
					"a v0.13 placement row cannot be decoded safely",
				)
			}
			assessment.setRow(row)
			return nil
		}
		assessment.report.Counts.LegacyPlacementRows++
		row.State = "confirmed"
		if safeAuthorityBackendName(backendName) {
			row.Backend = backendName
		} else {
			assessment.corruptFinding(
				"legacy_backend_identity_invalid",
				"a v0.13 placement backend cannot be rendered safely",
			)
		}
		assessment.setRow(row)
		if _, ok := configured[backendName]; !ok {
			assessment.mixedFinding(
				"legacy_backend_outside_expected_topology",
				"a v0.13 placement names a backend outside the expected topology",
			)
		}
		return nil
	}); err != nil {
		assessment.corruptFinding(
			"legacy_placement_scan_failed",
			"v0.13 placement rows could not be traversed",
		)
	}
}

// authorityRevisionHeader reduces the parser error to the only distinction the
// classifier needs. Keeping an error out of the Bolt iterator's control flow
// also makes explicit that malformed row data is a reported finding, not an
// iterator failure.
func authorityRevisionHeader(value []byte) (revision uint64, object, malformed bool) {
	revision, object, err := preflightRevisionHeader(value)
	return revision, object, err != nil
}

func inspectCurrentMetadata(
	bucket *bolt.Bucket,
	expectation AuthorityExpectation,
	assessment *authorityAssessment,
) (topologyMetadata, bool) {
	if bucket == nil {
		assessment.mixedFinding(
			"metadata_bucket_missing",
			"current placement metadata is missing",
		)
		return topologyMetadata{}, false
	}
	var encoded []byte
	if err := bucket.ForEach(func(key, value []byte) error {
		if !bytes.Equal(key, metadataStateKey) {
			assessment.mixedFinding(
				"unexpected_metadata_entry",
				"the metadata bucket contains an unexpected entry",
			)
			return nil
		}
		if value == nil {
			assessment.corruptFinding(
				"metadata_state_nested",
				"the metadata state is a nested bucket",
			)
			return nil
		}
		if len(value) > maxAuthorityMetadataValueBytes {
			assessment.corruptFinding(
				"metadata_oversized",
				"current placement metadata exceeds the safe decoder bound",
			)
			return nil
		}
		encoded = slices.Clone(value)
		return nil
	}); err != nil {
		assessment.corruptFinding(
			"metadata_scan_failed",
			"current placement metadata could not be traversed",
		)
		return topologyMetadata{}, false
	}
	if len(encoded) == 0 {
		assessment.mixedFinding(
			"metadata_state_missing",
			"current placement metadata has no topology state",
		)
		return topologyMetadata{}, false
	}
	metadata, err := decodeTopologyMetadata(encoded)
	if err != nil {
		assessment.corruptFinding(
			"metadata_corrupt",
			"current placement metadata cannot be decoded strictly",
		)
		return topologyMetadata{}, false
	}
	assessment.report.MetadataSchema = metadata.Schema
	assessment.report.TopologyID = metadata.TopologyID
	assessment.report.BaselineTopologyID = metadata.BaselineTopologyID
	assessment.report.InventoryTopologyID = metadata.InventoryTopologyID
	metadataErr := validateTopologyMetadata(metadata)
	if metadataErr != nil {
		assessment.mixedFinding(
			"metadata_inconsistent",
			"current placement metadata is structurally inconsistent",
		)
	} else {
		assessment.report.TopologyFingerprint = metadata.TopologyFingerprint
		assessment.report.BaselineFingerprint = metadata.BaselineFingerprint
	}
	if canonicalLeaseUUID(metadata.ProviderUUID) {
		assessment.report.ObservedProviderUUID = metadata.ProviderUUID
	}
	if validateCanonicalBackendNames(metadata.Topology, false) == nil &&
		authorityBackendNamesSafe(metadata.Topology) {
		assessment.report.ObservedBackendTopology,
			assessment.report.ObservedBackendTopologyOmitted =
			boundedAuthorityIdentities(metadata.Topology)
	}
	knownTopologyValid := validateCanonicalBackendNames(metadata.KnownBackends, false) == nil &&
		authorityBackendNamesSafe(metadata.KnownBackends)
	if knownTopologyValid {
		assessment.report.KnownBackendTopology,
			assessment.report.KnownBackendTopologyOmitted =
			boundedAuthorityIdentities(metadata.KnownBackends)
	}
	if len(metadata.EmptyInventoryBackends) == 0 ||
		(validateCanonicalBackendNames(metadata.EmptyInventoryBackends, false) == nil &&
			authorityBackendNamesSafe(metadata.EmptyInventoryBackends)) {
		assessment.report.EmptyInventoryBackends,
			assessment.report.EmptyInventoryBackendsOmitted =
			boundedAuthorityIdentities(metadata.EmptyInventoryBackends)
	}
	if metadata.ProviderUUID != expectation.providerUUID {
		assessment.mixedFinding(
			"provider_mismatch",
			"durable provider authority differs from the expected provider",
		)
	}
	if !slices.Equal(metadata.Topology, expectation.backendTopology) {
		assessment.mixedFinding(
			"topology_mismatch",
			"durable active topology differs from the expected topology",
		)
	}
	inspectAuthorityStorageBindings(metadata, knownTopologyValid, assessment)
	return metadata, true
}

func inspectAuthorityStorageBindings(
	metadata topologyMetadata,
	knownTopologyValid bool,
	assessment *authorityAssessment,
) {
	if !knownTopologyValid {
		assessment.mixedFinding(
			"storage_identity_incomplete",
			"durable backend storage bindings are incomplete or inconsistent",
		)
		return
	}
	known := stringSet(metadata.KnownBackends)
	keys := make([]string, 0, len(metadata.KnownBackendStorageIDs))
	for backendName := range metadata.KnownBackendStorageIDs {
		keys = append(keys, backendName)
	}
	slices.Sort(keys)
	storageOwners := make(map[backendidentity.ID]string, len(keys))
	consistent := len(keys) == len(known)
	for _, backendName := range keys {
		encodedID := metadata.KnownBackendStorageIDs[backendName]
		if _, exists := known[backendName]; !exists {
			consistent = false
			continue
		}
		id, err := backendidentity.Parse(encodedID)
		if err != nil || id.String() != encodedID {
			consistent = false
			continue
		}
		if owner, duplicate := storageOwners[id]; duplicate && owner != backendName {
			consistent = false
			continue
		}
		storageOwners[id] = backendName
		if len(backendName) > maxAuthorityRenderedIdentity ||
			len(assessment.report.StorageBindings) >= maxAuthorityCollectionEntries {
			assessment.report.StorageBindingsOmitted++
		} else {
			assessment.report.StorageBindings = append(
				assessment.report.StorageBindings,
				AuthorityStorageBinding{Backend: backendName, StorageID: encodedID},
			)
		}
	}
	for backendName := range known {
		if metadata.KnownBackendStorageIDs[backendName] == "" {
			consistent = false
		}
	}
	if !consistent {
		assessment.mixedFinding(
			"storage_identity_incomplete",
			"durable backend storage bindings are incomplete or inconsistent",
		)
	}
}

func inspectCurrentPlacementRows(
	bucket *bolt.Bucket,
	lifecycleBucket *bolt.Bucket,
	metadata topologyMetadata,
	metadataDecoded bool,
	assessment *authorityAssessment,
) {
	if bucket == nil {
		return
	}
	knownBackends := stringSet(metadata.KnownBackends)
	if err := bucket.ForEach(func(key, value []byte) error {
		assessment.report.Counts.PlacementRows++
		leaseUUID, canonicalKey := authorityLeaseKey(key)
		if canonicalKey {
			assessment.addReportableRow()
		}
		if value == nil || !canonicalKey {
			assessment.corruptFinding(
				"invalid_current_placement_row",
				"a current placement row has an invalid key or nested value",
			)
			if canonicalKey {
				assessment.setRow(AuthorityRowFact{
					LeaseUUID: leaseUUID, State: "unusable", LifecycleVerdict: "unknown",
				})
			}
			return nil
		}
		if len(value) > maxAuthorityRowValueBytes {
			assessment.report.Counts.UnusablePlacementRows++
			assessment.corruptFinding(
				"oversized_current_placement",
				"a current placement row exceeds the safe decoder bound",
			)
			assessment.setRow(AuthorityRowFact{
				LeaseUUID: leaseUUID, State: "unusable", LifecycleVerdict: "unknown",
			})
			return nil
		}
		placement, decoded := decodeAuthorityPlacement(leaseUUID, value)
		if !decoded {
			assessment.report.Counts.UnusablePlacementRows++
			assessment.corruptFinding(
				"undecodable_current_placement",
				"a current placement row cannot be interpreted safely",
			)
			assessment.setRow(AuthorityRowFact{
				LeaseUUID: leaseUUID, State: "unusable", LifecycleVerdict: "unknown",
			})
			return nil
		}
		state := placement.State().String()
		if placement.Conflict {
			state = "conflict"
		}
		backendName := placement.Backend
		attemptName := placement.Attempt
		conflictBackends := slices.Clone(placement.ConflictBackends)
		for _, name := range placementBackendNames(placement) {
			if safeAuthorityBackendName(name) {
				continue
			}
			assessment.mixedFinding(
				"placement_backend_identity_invalid",
				"a placement backend cannot be rendered safely",
			)
			if name == backendName {
				backendName = ""
			}
			if name == attemptName {
				attemptName = ""
			}
			conflictBackends = nil
		}
		row := AuthorityRowFact{
			LeaseUUID:             leaseUUID,
			State:                 state,
			Revision:              placement.revision,
			Backend:               backendName,
			Attempt:               attemptName,
			Conflict:              placement.Conflict,
			UntrustedPositive:     placement.untrustedPositive,
			ConflictBackends:      conflictBackends,
			ConflictOwnersUnknown: placement.ConflictOwnersUnknown,
			LifecycleVerdict:      "missing",
		}
		inspectAuthorityLifecycleBindingForPlacement(
			leaseUUID, placement, lifecycleBucket, &row, assessment,
		)
		assessment.setRow(row)
		if placement.revision == 0 {
			assessment.report.Counts.LegacyPlacementRows++
			assessment.mixedFinding(
				"unrevisioned_current_placement",
				"a current-schema placement has no durable revision",
			)
		} else {
			assessment.report.Counts.RevisionedPlacementRows++
		}
		if metadataDecoded {
			for _, backendName := range placementBackendNames(placement) {
				if _, ok := knownBackends[backendName]; !ok {
					assessment.mixedFinding(
						"placement_backend_outside_durable_history",
						"a placement names a backend outside durable identity history",
					)
					break
				}
			}
		}
		return nil
	}); err != nil {
		assessment.corruptFinding(
			"current_placement_scan_failed",
			"current placement rows could not be traversed",
		)
	}
}

func inspectAuthorityLifecycleBindingForPlacement(
	leaseUUID string,
	placement Placement,
	lifecycleBucket *bolt.Bucket,
	row *AuthorityRowFact,
	assessment *authorityAssessment,
) {
	if lifecycleBucket == nil {
		return
	}
	encoded := lifecycleBucket.Get([]byte(leaseUUID))
	if encoded == nil {
		assessment.mixedFinding(
			"placement_lifecycle_missing",
			"a current placement has no lifecycle authority row",
		)
		return
	}
	if len(encoded) > maxAuthorityLifecycleValueBytes {
		assessment.corruptFinding(
			"oversized_lifecycle_row",
			"a lifecycle row exceeds the safe decoder bound",
		)
		row.LifecycleVerdict = "corrupt"
		return
	}
	capability, err := decodeLifecycleCapability(encoded)
	if err != nil {
		row.LifecycleVerdict = "corrupt"
		return
	}
	if capability.unusable {
		row.LifecycleVerdict = "unusable"
		return
	}
	if !lifecycleBindingMatches(placement, capability) {
		assessment.mixedFinding(
			"placement_lifecycle_mismatch",
			"placement and lifecycle authority do not describe one generation",
		)
		row.LifecycleVerdict = "mismatched"
		return
	}
	row.LifecycleVerdict = authorityLifecycleVerdict(capability, true)
}

// decodeAuthorityPlacement mirrors the production structural decoder without
// logging its error. Offline classification must never accidentally render a
// persisted callback URL or token through a decoder error path.
func decodeAuthorityPlacement(leaseUUID string, value []byte) (Placement, bool) {
	if len(value) > maxAuthorityRowValueBytes {
		return Placement{}, false
	}
	if len(value) > 0 && value[0] != '{' {
		if json.Valid(value) || !validLegacyBackendName(value) {
			return Placement{}, false
		}
		return Placement{Backend: string(value)}, true
	}
	if len(value) == 0 {
		return Placement{}, false
	}
	fields, err := decodeUniqueJSONObject(value)
	if err != nil {
		return Placement{}, false
	}
	var persisted record
	if err := json.Unmarshal(value, &persisted); err != nil {
		return Placement{}, false
	}
	operationID, operationErr := decodeOperationID(persisted.OperationID)
	operationKind, kindErr := decodeOperationKind(persisted.OperationKind)
	payloadFingerprint, fingerprintErr := decodePayloadFingerprint(persisted.PayloadHash)
	requestItemsRaw, requestItemsPresent := fields["request_items"]
	requestSnapshot, requestErr := decodeBackendRequestSnapshot(
		persisted.Tenant,
		persisted.ProviderUUID,
		persisted.RequestItems,
		requestItemsRaw,
		requestItemsPresent,
	)
	callbackPair, callbackErr := decodeCallbackPair(
		operationID,
		persisted.CallbackURL,
		persisted.LifecycleCallbackURL,
	)
	if operationErr != nil || kindErr != nil || fingerprintErr != nil ||
		requestErr != nil || callbackErr != nil {
		return Placement{}, false
	}
	placement := Placement{
		Backend:                       persisted.Backend,
		Attempt:                       persisted.Attempt,
		SetAt:                         persisted.SetAt,
		Conflict:                      persisted.Conflict,
		ConflictBackends:              normalizeBackendNames(persisted.ConflictBackends),
		ConflictOwnersUnknown:         persisted.ConflictOwnersUnknown,
		untrustedPositive:             persisted.UntrustedPositive,
		revision:                      persisted.Revision,
		attemptOperationID:            operationID,
		attemptOperationKind:          operationKind,
		attemptRestoreSourceLeaseUUID: persisted.RestoreSourceLeaseUUID,
		attemptPayloadFingerprint:     payloadFingerprint,
		attemptRequestSnapshot:        requestSnapshot,
		attemptCallbackPair:           callbackPair,
	}
	if placement.Conflict {
		if placement.untrustedPositive && len(placement.ConflictBackends) == 0 {
			return Placement{}, false
		}
		if !placement.untrustedPositive && len(placement.ConflictBackends) < 2 {
			placement.ConflictOwnersUnknown = true
		}
	} else {
		placement.ConflictBackends = nil
		placement.ConflictOwnersUnknown = false
		placement.untrustedPositive = false
	}
	if placement.Attempt == "" {
		if placement.attemptOperationID.Valid() || placement.attemptOperationKind.Valid() ||
			placement.attemptRestoreSourceLeaseUUID != "" ||
			placement.attemptPayloadFingerprint.Valid() ||
			placement.attemptRequestSnapshot.Valid() ||
			placement.attemptCallbackPair.ValidFor(placement.attemptOperationID) {
			if placement.Backend == "" || !validAttemptMetadata(
				leaseUUID,
				placement.attemptOperationID,
				placement.attemptOperationKind,
				placement.attemptRestoreSourceLeaseUUID,
				placement.attemptPayloadFingerprint,
				placement.attemptRequestSnapshot,
				placement.attemptCallbackPair,
			) {
				return Placement{}, false
			}
		}
	} else if !validAttemptMetadata(
		leaseUUID,
		placement.attemptOperationID,
		placement.attemptOperationKind,
		placement.attemptRestoreSourceLeaseUUID,
		placement.attemptPayloadFingerprint,
		placement.attemptRequestSnapshot,
		placement.attemptCallbackPair,
	) {
		return Placement{}, false
	}
	if placement.Backend == "" && placement.Attempt == "" && !placement.Conflict {
		return Placement{}, false
	}
	return placement, true
}

func inspectCurrentLifecycleRows(
	bucket *bolt.Bucket,
	metadata topologyMetadata,
	metadataDecoded bool,
	placementBucket *bolt.Bucket,
	assessment *authorityAssessment,
) {
	if bucket == nil {
		assessment.mixedFinding(
			"lifecycle_bucket_missing",
			"current lifecycle authority is missing",
		)
		return
	}
	knownBackends := stringSet(metadata.KnownBackends)
	if err := bucket.ForEach(func(key, value []byte) error {
		assessment.report.Counts.LifecycleRows++
		leaseUUID, canonicalLease := authorityLeaseKey(key)
		rawPlacementExists := canonicalLease && placementBucket != nil &&
			(placementBucket.Get(key) != nil || placementBucket.Bucket(key) != nil)
		if canonicalLease && !rawPlacementExists {
			assessment.addReportableRow()
			assessment.report.Counts.DetachedLifecycleRows++
		}
		if value == nil || !canonicalLease {
			assessment.corruptFinding(
				"invalid_lifecycle_row",
				"a lifecycle row has an invalid key or nested value",
			)
			if canonicalLease {
				row := assessment.rows[leaseUUID]
				if row.LeaseUUID == "" {
					row = AuthorityRowFact{LeaseUUID: leaseUUID, State: "absent"}
				}
				row.LifecycleVerdict = "corrupt"
				assessment.setRow(row)
			}
			return nil
		}
		if len(value) > maxAuthorityLifecycleValueBytes {
			assessment.report.Counts.UnusableLifecycleRows++
			assessment.corruptFinding(
				"oversized_lifecycle_row",
				"a lifecycle row exceeds the safe decoder bound",
			)
			row := assessment.rows[leaseUUID]
			if row.LeaseUUID == "" {
				row = AuthorityRowFact{LeaseUUID: leaseUUID, State: "absent"}
			}
			row.LifecycleVerdict = "corrupt"
			assessment.setRow(row)
			return nil
		}
		capability, err := decodeLifecycleCapability(value)
		if err != nil {
			assessment.corruptFinding(
				"undecodable_lifecycle_row",
				"a lifecycle row cannot be decoded strictly",
			)
			row := assessment.rows[leaseUUID]
			if row.LeaseUUID == "" {
				row = AuthorityRowFact{LeaseUUID: leaseUUID, State: "absent"}
			}
			row.LifecycleVerdict = "corrupt"
			assessment.setRow(row)
			return nil
		}
		if capability.unusable {
			assessment.report.Counts.UnusableLifecycleRows++
		}
		for _, backendName := range []string{capability.backend, capability.attemptBackend} {
			if backendName != "" && !safeAuthorityBackendName(backendName) {
				assessment.mixedFinding(
					"lifecycle_backend_identity_invalid",
					"a lifecycle backend cannot be rendered safely",
				)
			}
		}
		if metadataDecoded {
			for _, backendName := range []string{capability.backend, capability.attemptBackend} {
				if backendName == "" {
					continue
				}
				if _, ok := knownBackends[backendName]; !ok {
					assessment.mixedFinding(
						"lifecycle_backend_outside_durable_history",
						"lifecycle authority names a backend outside durable identity history",
					)
					break
				}
			}
		}
		if !rawPlacementExists {
			row := assessment.rows[leaseUUID]
			if row.LeaseUUID == "" {
				row = AuthorityRowFact{LeaseUUID: leaseUUID, State: "absent"}
			}
			if safeAuthorityBackendName(capability.backend) {
				row.Backend = capability.backend
			}
			if safeAuthorityBackendName(capability.attemptBackend) {
				row.Attempt = capability.attemptBackend
			}
			row.LifecycleVerdict = authorityLifecycleVerdict(capability, false)
			assessment.setRow(row)
			if capability.attemptBackend != "" {
				assessment.mixedFinding(
					"detached_lifecycle_attempt",
					"a lifecycle attempt marker has no placement row",
				)
			}
		}
		return nil
	}); err != nil {
		assessment.corruptFinding(
			"lifecycle_scan_failed",
			"lifecycle rows could not be traversed",
		)
		return
	}
}

func authorityLifecycleVerdict(
	capability lifecycleCapability,
	attached bool,
) string {
	if capability.unusable {
		return "unusable"
	}
	if capability.attemptBackend != "" {
		return "attempt_pending"
	}
	if capability.retired {
		if attached {
			return "retired"
		}
		return "detached_retired"
	}
	if capability.id.Valid() {
		if attached {
			return "typed_active"
		}
		return "typed_teardown"
	}
	if attached {
		return "legacy_active"
	}
	return "legacy_teardown"
}

func lifecycleBindingMatches(
	placement Placement,
	capability lifecycleCapability,
) bool {
	if placement.Backend != "" && capability.backend != placement.Backend {
		return false
	}
	if placement.Attempt == "" {
		if capability.attemptBackend != "" {
			return false
		}
		if placement.attemptOperationID.Valid() {
			wantID, err := lifecycleIDForOperation(placement.attemptOperationID)
			return err == nil && capability.backend == placement.Backend &&
				capability.id == wantID && !capability.retired
		}
		return true
	}
	wantID, err := lifecycleIDForOperation(placement.attemptOperationID)
	return err == nil && capability.attemptBackend == placement.Attempt &&
		capability.attemptID == wantID
}

func placementBackendNames(placement Placement) []string {
	names := make([]string, 0, 2+len(placement.ConflictBackends))
	if placement.Backend != "" {
		names = append(names, placement.Backend)
	}
	if placement.Attempt != "" {
		names = append(names, placement.Attempt)
	}
	names = append(names, placement.ConflictBackends...)
	return names
}

func stringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

// authorityLeaseKey refuses to materialize an attacker-controlled bbolt key
// as a Go string until its fixed canonical UUID width has been established.
// The subsequent canonical check rejects non-UUID bytes without retaining
// them in the bounded report.
func authorityLeaseKey(key []byte) (string, bool) {
	if len(key) != 36 {
		return "", false
	}
	leaseUUID := string(key)
	return leaseUUID, canonicalLeaseUUID(leaseUUID)
}

func safeAuthorityBackendName(value string) bool {
	return backendname.Validate(value) == nil
}

func authorityBackendNamesSafe(values []string) bool {
	for _, value := range values {
		if !safeAuthorityBackendName(value) {
			return false
		}
	}
	return true
}
