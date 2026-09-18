package docker

// Historical storage/rollback fixtures retain these labels without advertising
// nonexistent runtime emitters in the production metric vocabulary.
const (
	destroySiteProvisionCleanup = "provision_cleanup"
	teardownOpRestoreRollback   = "restore_rollback"
	teardownOpRestorePrelude    = "restore_prelude"
)
