package shared

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
)

// ErrMaintenanceReleaseClaimRequired marks a raw release-store mutation that
// attempted to bypass the exact MaintenanceReleaseClaim API. Maintenance rows
// are causal authority and may only be appended or transitioned by the typed
// methods in this file.
var ErrMaintenanceReleaseClaimRequired = errors.New("maintenance release requires exact claim")

// ReleaseClaim is an opaque compare-and-swap capability for one exact durable
// release row. It is intentionally not constructible outside this package.
type ReleaseClaim struct {
	leaseUUID string
	version   int
	digest    [sha256.Size]byte
}

func (c ReleaseClaim) LeaseUUID() string         { return c.leaseUUID }
func (c ReleaseClaim) Version() int              { return c.version }
func (c ReleaseClaim) Digest() [sha256.Size]byte { return c.digest }
func (c ReleaseClaim) valid() bool {
	return backend.IsCanonicalLeaseUUID(c.leaseUUID) && c.version > 0 &&
		c.digest != ([sha256.Size]byte{})
}

// MaintenanceReleaseClaim is the exact target-generation capability returned
// by AppendMaintenance or recovery lookup. Its immutable digest deliberately
// excludes terminal state fields, so the same claim remains valid across the
// deploying -> active/failed transition and cannot match a different cohort.
type MaintenanceReleaseClaim struct {
	releaseClaim    ReleaseClaim
	maintenanceID   MaintenanceID
	immutableDigest [sha256.Size]byte
}

func (c MaintenanceReleaseClaim) LeaseUUID() string { return c.releaseClaim.leaseUUID }
func (c MaintenanceReleaseClaim) Version() int      { return c.releaseClaim.version }
func (c MaintenanceReleaseClaim) MaintenanceID() MaintenanceID {
	return c.maintenanceID
}
func (c MaintenanceReleaseClaim) Digest() [sha256.Size]byte { return c.immutableDigest }

func (c MaintenanceReleaseClaim) Valid() bool { return c.valid() }

func (c MaintenanceReleaseClaim) valid() bool {
	return c.releaseClaim.valid() && c.maintenanceID.Valid() &&
		c.immutableDigest != ([sha256.Size]byte{})
}

// ClaimLatestActive returns a deep-cloned active release and an opaque exact
// fence from the same bbolt snapshot.
func (s *ReleaseStore) ClaimLatestActive(leaseUUID string) (Release, ReleaseClaim, error) {
	if err := s.requireCanonicalLeaseUUID(leaseUUID); err != nil {
		return Release{}, ReleaseClaim{}, err
	}
	var release Release
	var claim ReleaseClaim
	err := s.view(func(tx *bolt.Tx) error {
		releases, err := readReleaseHistoryTx(tx, leaseUUID)
		if err != nil {
			return err
		}
		index := latestActiveReleaseIndex(releases)
		if index < 0 {
			return fmt.Errorf("active release for %s does not exist", leaseUUID)
		}
		release = cloneRelease(releases[index])
		encoded, err := json.Marshal(releases[index])
		if err != nil {
			return fmt.Errorf("marshal active release fence for %s: %w", leaseUUID, err)
		}
		claim = ReleaseClaim{
			leaseUUID: leaseUUID,
			version:   releases[index].Version,
			digest:    sha256.Sum256(encoded),
		}
		return nil
	})
	if err != nil {
		return Release{}, ReleaseClaim{}, err
	}
	return release, claim, nil
}

// CheckAppendMaintenanceCapacity proves that the exact source is still active
// and that both terminal forms of target fit before any substrate mutation.
func (s *ReleaseStore) CheckAppendMaintenanceCapacity(
	admission MaintenanceIntentAdmission,
) error {
	if err := validateMaintenanceIntentAdmission(admission); err != nil {
		return err
	}
	source, target, err := s.maintenanceIntentAuthority(admission.intent)
	if err != nil {
		return err
	}
	if err := validateMaintenanceAppendInput(source, target); err != nil {
		return err
	}
	return s.view(func(tx *bolt.Tx) error {
		_, _, err := planMaintenanceAppendTx(
			tx,
			source,
			target,
			releaseHistoryCapacityCutoff(s.maxAge, time.Now()),
			backend.MaxStoredReleaseHistoryBytes,
		)
		return err
	})
}

// AppendMaintenance appends the exact MaintenanceID-bearing deploying target
// only while the source claim still names the active release. The returned
// capability includes the store-assigned version and immutable target digest.
func (s *ReleaseStore) AppendMaintenance(
	appendClaim MaintenanceAppendClaim,
) (MaintenanceReleaseClaim, error) {
	if err := validateMaintenanceAppendClaim(appendClaim); err != nil {
		return MaintenanceReleaseClaim{}, err
	}
	source, target, err := s.maintenanceIntentAuthority(appendClaim.intent)
	if err != nil {
		return MaintenanceReleaseClaim{}, err
	}
	if err := validateMaintenanceAppendInput(source, target); err != nil {
		return MaintenanceReleaseClaim{}, err
	}
	var claim MaintenanceReleaseClaim
	err = s.update(func(tx *bolt.Tx) error {
		releases, candidate, err := planMaintenanceAppendTx(
			tx,
			source,
			target,
			releaseHistoryCapacityCutoff(s.maxAge, time.Now()),
			backend.MaxStoredReleaseHistoryBytes,
		)
		if err != nil {
			return err
		}
		encoded, err := encodeReleaseHistory(releases)
		if err != nil {
			return fmt.Errorf("encode maintenance release history: %w", err)
		}
		if err := tx.Bucket(releasesBucketName).Put([]byte(source.leaseUUID), encoded); err != nil {
			return err
		}
		claim, err = newMaintenanceReleaseClaim(source.leaseUUID, candidate)
		return err
	})
	return claim, err
}

func (s *ReleaseStore) maintenanceIntentAuthority(
	intent MaintenanceIntentClaim,
) (ReleaseClaim, Release, error) {
	if err := validateMaintenanceIntentClaim(intent); err != nil {
		return ReleaseClaim{}, Release{}, err
	}
	if s != nil && s.binding != nil && intent.BackendStorageID() != s.binding.storageID {
		return ReleaseClaim{}, Release{}, errors.New(
			"maintenance intent and release journal have different storage identities",
		)
	}
	return intent.SourceRelease(), intent.TargetRelease(), nil
}

// FindMaintenanceRelease finds by the unguessable maintenance identity rather
// than by tail position. It returns (zero, zero, false, nil) when no target was
// ever appended.
func (s *ReleaseStore) FindMaintenanceRelease(
	leaseUUID string,
	maintenanceID MaintenanceID,
) (Release, MaintenanceReleaseClaim, bool, error) {
	if err := s.requireCanonicalLeaseUUID(leaseUUID); err != nil {
		return Release{}, MaintenanceReleaseClaim{}, false, err
	}
	if !maintenanceID.Valid() {
		return Release{}, MaintenanceReleaseClaim{}, false, errors.New("maintenance ID must be a canonical UUIDv4")
	}
	var found Release
	err := s.view(func(tx *bolt.Tx) error {
		releases, err := readReleaseHistoryTx(tx, leaseUUID)
		if err != nil {
			return err
		}
		for _, release := range releases {
			if release.MaintenanceID == maintenanceID {
				found = cloneRelease(release)
				return nil
			}
		}
		return nil
	})
	if err != nil || found.MaintenanceID == "" {
		return Release{}, MaintenanceReleaseClaim{}, false, err
	}
	claim, err := newMaintenanceReleaseClaim(leaseUUID, found)
	if err != nil {
		return Release{}, MaintenanceReleaseClaim{}, false, err
	}
	return found, claim, true, nil
}

// ActivateMaintenance commits one exact replacement generation. Replaying the
// same claim after the transaction committed is idempotent.
func (s *ReleaseStore) ActivateMaintenance(claim MaintenanceReleaseClaim) error {
	return s.transitionMaintenance(claim, "active", "", "")
}

// FailMaintenance records a terminal failure against one exact target without
// touching a later release. Replaying the same failed claim is idempotent.
func (s *ReleaseStore) FailMaintenance(
	claim MaintenanceReleaseClaim,
	reason backend.Reason,
	message string,
) error {
	return s.transitionMaintenance(claim, "failed", reason, message)
}

func (s *ReleaseStore) transitionMaintenance(
	claim MaintenanceReleaseClaim,
	status string,
	reason backend.Reason,
	message string,
) error {
	if !claim.valid() {
		return errors.New("maintenance release claim has no durable capability")
	}
	if status != "active" && status != "failed" {
		return fmt.Errorf("invalid maintenance terminal status %q", status)
	}
	return s.update(func(tx *bolt.Tx) error {
		releases, err := readReleaseHistoryTx(tx, claim.LeaseUUID())
		if err != nil {
			return err
		}
		index := maintenanceReleaseIndex(releases, claim.MaintenanceID())
		if index < 0 {
			return fmt.Errorf("maintenance release %s no longer exists for lease %s",
				claim.MaintenanceID(), claim.LeaseUUID())
		}
		candidate := &releases[index]
		if candidate.Version != claim.Version() {
			return errors.New("maintenance release version changed before precise mutation")
		}
		digest, err := maintenanceReleaseDigest(*candidate)
		if err != nil {
			return err
		}
		if digest != claim.Digest() {
			return errors.New("maintenance release changed before precise mutation")
		}
		if candidate.Status == status {
			return nil
		}
		if candidate.Status != "deploying" {
			return fmt.Errorf("maintenance release is already terminal with status %q", candidate.Status)
		}
		if status == "active" {
			for i := range releases {
				if i != index && releases[i].Status == "active" {
					releases[i].Status = "superseded"
				}
			}
			candidate.Status = "active"
			candidate.Reason = ""
			candidate.Message = ""
			candidate.Error = ""
		} else {
			candidate.Status = "failed"
			candidate.Reason = reason
			candidate.Message = message
			candidate.Error = ""
		}
		if err := validateReleaseHistory(releases); err != nil {
			return fmt.Errorf("invalid maintenance terminal history: %w", err)
		}
		encoded, err := compactAndEncodeReleaseHistory(
			releases,
			releaseHistoryCapacityCutoff(s.maxAge, time.Now()),
			backend.MaxStoredReleaseHistoryBytes,
		)
		if err != nil && status == "failed" && errors.Is(err, ErrReleaseHistoryCapacity) &&
			(reason != "" || message != "") {
			candidate.Reason = ""
			candidate.Message = ""
			encoded, err = compactAndEncodeReleaseHistory(
				releases,
				releaseHistoryCapacityCutoff(s.maxAge, time.Now()),
				backend.MaxStoredReleaseHistoryBytes,
			)
		}
		if err != nil {
			return fmt.Errorf("encode maintenance terminal history: %w", err)
		}
		return tx.Bucket(releasesBucketName).Put([]byte(claim.LeaseUUID()), encoded)
	})
}

func validateMaintenanceAppendInput(source ReleaseClaim, target Release) error {
	if !source.valid() {
		return errors.New("maintenance source release claim has no durable capability")
	}
	if target.Version != 0 {
		return errors.New("maintenance target version must be store-assigned")
	}
	if target.Status != "deploying" {
		return errors.New("maintenance target must start deploying")
	}
	if !target.MaintenanceID.Valid() {
		return errors.New("maintenance target requires a canonical UUIDv4 maintenance ID")
	}
	if !target.OperationID.Valid() {
		return errors.New("maintenance target requires a typed operation lineage")
	}
	if err := validateStoredRelease(target); err != nil {
		return fmt.Errorf("invalid maintenance release: %w", err)
	}
	if len(target.Items) > 0 && len(target.ResourceProfiles) == 0 {
		return errors.New("invalid maintenance release: desired items require exact resource profiles")
	}
	return nil
}

func planMaintenanceAppendTx(
	tx *bolt.Tx,
	source ReleaseClaim,
	target Release,
	cutoff time.Time,
	limitBytes int,
) ([]Release, Release, error) {
	if source.LeaseUUID() == "" {
		return nil, Release{}, errors.New("maintenance source lease is empty")
	}
	bucket := tx.Bucket(releasesBucketName)
	if bucket == nil {
		return nil, Release{}, errors.New("releases bucket missing")
	}
	data := bucket.Get([]byte(source.LeaseUUID()))
	if data == nil {
		return nil, Release{}, fmt.Errorf("release history for %s does not exist", source.LeaseUUID())
	}
	var current []Release
	if err := json.Unmarshal(data, &current); err != nil {
		return nil, Release{}, fmt.Errorf("corrupted release data for %s: %w", source.LeaseUUID(), err)
	}
	if err := validateReleaseHistory(current); err != nil {
		return nil, Release{}, fmt.Errorf("invalid release data for %s: %w", source.LeaseUUID(), err)
	}
	sourceRelease, err := verifySourceRelease(current, source)
	if err != nil {
		return nil, Release{}, err
	}
	if sourceRelease.RuntimeAuthority == nil || target.RuntimeAuthority == nil {
		return nil, Release{}, errors.New("maintenance source and target require runtime authority")
	}
	if target.OperationID != sourceRelease.OperationID ||
		target.RuntimeAuthority.OperationID() != sourceRelease.RuntimeAuthority.OperationID() {
		return nil, Release{}, errors.New("maintenance target changes operation lineage")
	}
	if target.RuntimeAuthority.Tenant() != sourceRelease.RuntimeAuthority.Tenant() {
		return nil, Release{}, errors.New("maintenance target changes tenant authority")
	}
	if target.RuntimeAuthority.ProviderUUID() != sourceRelease.RuntimeAuthority.ProviderUUID() {
		return nil, Release{}, errors.New("maintenance target changes provider authority")
	}
	// Callback route bases are trusted configuration and may intentionally rotate
	// during maintenance. NewReleaseRuntimeAuthority has already proven both URL
	// capabilities carry this same typed operation/lifecycle UUID, so byte-equal
	// URLs are neither required nor a stronger identity fence here.
	if maintenanceReleaseIndex(current, target.MaintenanceID) >= 0 {
		return nil, Release{}, errors.New("maintenance target ID already exists")
	}
	planned, _, err := planMaintenanceAppendedReleaseHistory(
		data,
		source.LeaseUUID(),
		cloneRelease(target),
		false,
		cutoff,
		limitBytes,
	)
	if err != nil {
		return nil, Release{}, err
	}
	index := maintenanceReleaseIndex(planned, target.MaintenanceID)
	if index < 0 {
		return nil, Release{}, errors.New("maintenance target was not retained by append plan")
	}
	return planned, cloneRelease(planned[index]), nil
}

func readReleaseHistoryTx(tx *bolt.Tx, leaseUUID string) ([]Release, error) {
	bucket := tx.Bucket(releasesBucketName)
	if bucket == nil {
		return nil, errors.New("releases bucket missing")
	}
	data := bucket.Get([]byte(leaseUUID))
	if data == nil {
		return nil, fmt.Errorf("release history for %s does not exist", leaseUUID)
	}
	var releases []Release
	if err := json.Unmarshal(data, &releases); err != nil {
		return nil, fmt.Errorf("corrupted release data for %s: %w", leaseUUID, err)
	}
	if err := validateReleaseHistory(releases); err != nil {
		return nil, fmt.Errorf("invalid release data for %s: %w", leaseUUID, err)
	}
	return releases, nil
}

func verifySourceRelease(releases []Release, source ReleaseClaim) (Release, error) {
	for _, release := range releases {
		if release.Version != source.Version() {
			continue
		}
		encoded, err := json.Marshal(release)
		if err != nil {
			return Release{}, err
		}
		if sha256.Sum256(encoded) != source.Digest() || release.Status != "active" {
			return Release{}, errors.New("maintenance source release changed before admission")
		}
		return release, nil
	}
	return Release{}, errors.New("maintenance source release no longer exists")
}

func maintenanceReleaseIndex(releases []Release, maintenanceID MaintenanceID) int {
	for index := range releases {
		if releases[index].MaintenanceID == maintenanceID {
			return index
		}
	}
	return -1
}

func newMaintenanceReleaseClaim(
	leaseUUID string,
	release Release,
) (MaintenanceReleaseClaim, error) {
	digest, err := maintenanceReleaseDigest(release)
	if err != nil {
		return MaintenanceReleaseClaim{}, err
	}
	return MaintenanceReleaseClaim{
		releaseClaim:    ReleaseClaim{leaseUUID: leaseUUID, version: release.Version, digest: digest},
		maintenanceID:   release.MaintenanceID,
		immutableDigest: digest,
	}, nil
}

func maintenanceReleaseDigest(release Release) ([sha256.Size]byte, error) {
	release = cloneRelease(release)
	release.Status = ""
	release.Error = ""
	release.Reason = ""
	release.Message = ""
	encoded, err := json.Marshal(release)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("marshal maintenance release digest: %w", err)
	}
	return sha256.Sum256(encoded), nil
}
