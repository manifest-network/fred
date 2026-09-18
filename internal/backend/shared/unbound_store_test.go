package shared

import (
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

// These package-local constructors exist only for tests that exercise corrupt
// or stopped pre-binding journals. Production code has no callable path that
// can open a live authority-bearing store without verified storage lineage.
func newUnboundCallbackStoreForTest(cfg CallbackStoreConfig) (*CallbackStore, error) {
	base, err := openBoltStore(boltStoreConfig{
		DBPath: cfg.DBPath, BucketName: callbackBucketName,
		MaxAge: cfg.MaxAge, Label: "callback",
	})
	if err != nil {
		return nil, err
	}
	store, err := finishCallbackStoreOpen(cfg, base, initializeUnboundCallbackSchemaForTest)
	if err != nil {
		return nil, err
	}
	store.StartMaintenance()
	return store, nil
}

func initializeUnboundCallbackSchemaForTest(tx *bolt.Tx) error {
	if err := requireDrainedLegacyCallbackBucket(tx); err != nil {
		return err
	}
	if err := validateCallbackRootBuckets(tx); err != nil {
		return err
	}
	present := countCallbackSchemaBuckets(tx)
	if present != 0 && present != len(callbackCurrentSchemaBuckets()) {
		return errors.New("callback journal contains a partial aggregate schema")
	}
	for _, bucketName := range callbackCurrentSchemaBuckets() {
		if _, err := tx.CreateBucketIfNotExists(bucketName); err != nil {
			return err
		}
	}
	return nil
}

func newUnboundReleaseStoreForTest(cfg ReleaseStoreConfig) (*ReleaseStore, error) {
	base, err := openBoltStore(boltStoreConfig{
		DBPath: cfg.DBPath, BucketName: releasesBucketName,
		MaxAge: cfg.MaxAge, Label: "releases",
	})
	if err != nil {
		return nil, err
	}
	store := &ReleaseStore{
		boltStore: base, cleanupInterval: cfg.CleanupInterval,
		onCleanupPanic: cfg.OnCleanupPanic,
	}
	store.StartMaintenance()
	return store, nil
}

func newUnboundRetentionStoreForTest(cfg RetentionStoreConfig) (*RetentionStore, error) {
	base, err := openBoltStore(boltStoreConfig{
		DBPath: cfg.DBPath, BucketName: retentionBucketName, Label: "retention",
	})
	if err != nil {
		return nil, err
	}
	store := &RetentionStore{boltStore: base, onReindex: cfg.OnReindex}
	start := time.Now()
	byTenant, byStatus, count, err := store.scanIndex()
	if err != nil {
		_ = base.Close()
		return nil, fmt.Errorf("failed to build retention index: %w", err)
	}
	store.byTenant, store.byStatus = byTenant, byStatus
	store.fireReindex(count, time.Since(start), "open")
	return store, nil
}
