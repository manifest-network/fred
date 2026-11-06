package docker

// Historical storage/rollback fixtures retain these labels without advertising
// nonexistent runtime emitters in the production metric vocabulary. They are
// bound when a test uses them, never at package initialization, so a fresh
// test process exports only production's series.

func destroySiteProvisionCleanup() destroySite { return bindDestroySite("provision_cleanup") }

func teardownOpRestoreRollback() teardownOperation { return bindTeardownOperation("restore_rollback") }

func teardownOpRestorePrelude() teardownOperation { return bindTeardownOperation("restore_prelude") }
