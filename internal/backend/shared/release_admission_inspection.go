package shared

import (
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// ReleaseAdmissionFinding identifies a stored row accepted under an older
// manifest policy. This is advisory evidence, not a corrupt-history verdict.
// Status is the row's status as stopped adoption binds it: adoption may
// supersede an older v0.13 active row but never changes a version or manifest.
type ReleaseAdmissionFinding struct {
	LeaseUUID   string `json:"lease_uuid"`
	Version     int    `json:"version"`
	Status      string `json:"status"`
	PolicyError string `json:"policy_error"`
}

// ReleaseAdmissionInspection summarizes an offline scan of every release row,
// including failed and superseded rows, against current admission policy.
type ReleaseAdmissionInspection struct {
	Histories int                       `json:"histories"`
	Releases  int                       `json:"releases"`
	Findings  []ReleaseAdmissionFinding `json:"findings"`
}

// InspectReleaseAdmissionReadOnly opens an existing stopped release database
// read-only. Both v0.13 arrays and current versioned histories are supported.
// Structural corruption is an error; policy drift is reported per lease and
// release. No database, binding, normalization or background writer is created.
func InspectReleaseAdmissionReadOnly(dbPath string) (ReleaseAdmissionInspection, error) {
	report := ReleaseAdmissionInspection{Findings: []ReleaseAdmissionFinding{}}
	db, _, err := openExistingBoltDBFile(pathnameAuthoritativeStoreFile(dbPath), true, false)
	if err != nil {
		return report, fmt.Errorf("open release database read-only: %w", err)
	}
	inspectErr := db.View(func(tx *bolt.Tx) error {
		if err := validateReleaseRootBuckets(tx); err != nil {
			return err
		}
		bucket := tx.Bucket(releasesBucketName)
		if bucket == nil {
			return errors.New("releases bucket is missing")
		}
		budget := newStoppedAuthoritativeInspectionBudget()
		return bucket.ForEach(func(key, value []byte) error {
			if err := budget.observe(key, value); err != nil {
				return err
			}
			if !backend.IsCanonicalLeaseUUID(string(key)) {
				return fmt.Errorf("release history key with length %d is not a canonical lease UUID", len(key))
			}
			if value == nil {
				return fmt.Errorf("release history for lease %q is a nested bucket", key)
			}
			history, err := parseStoppedV013ReleaseHistory(value)
			if err != nil {
				return fmt.Errorf("release history for lease %q: %w", key, err)
			}
			report.Histories++
			for _, release := range history.strictRows {
				report.Releases++
				if len(release.Manifest) == 0 {
					continue // Legacy status-only rows contain no admitted manifest.
				}
				if _, err := manifest.ParsePayload(release.Manifest); err != nil {
					report.Findings = append(report.Findings, ReleaseAdmissionFinding{
						LeaseUUID: string(key), Version: release.Version,
						Status: release.Status, PolicyError: err.Error(),
					})
				}
			}
			return nil
		})
	})
	if err := errors.Join(inspectErr, db.Close()); err != nil {
		return report, fmt.Errorf("inspect release admission policy: %w", err)
	}
	return report, nil
}
