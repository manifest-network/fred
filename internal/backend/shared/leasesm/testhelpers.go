package leasesm

import (
	"context"

	"github.com/manifest-network/fred/internal/backend"
)

// This file holds test helpers that must be reachable from substrate
// test packages (notably docker/*_test.go). Helpers private to leasesm
// itself live in `*_test.go` files (see mocks_test.go); helpers here
// are necessary cross-package exports for tests that exercise full
// substrate flows synchronously — they cannot use `_test.go`-scoped
// helpers because Go test-package visibility doesn't cross package
// boundaries.
//
// Per architect bounded-set rule (PR5b-2 deviation E-3 condition #3):
// these 3 ForTest helpers are the COMPLETE set authorized for the
// PR5b-2 docker provision-test corpus. Future tests that want ForTest
// access for other SM events (replace-flow, deprovision-flow, etc.)
// MUST flag for Felix-level approval and likely revisit the test
// pattern rather than auto-adding more helpers — the "ForTest" suffix
// is not a generic escape hatch.

// FireProvisionRequestedForTest fires the provision-requested SM event.
// Exported only for cross-package tests; not part of the public leasesm
// contract.
//
// Synchronously drives Ready→Provisioning (or Failed→Provisioning on
// retry). Tests use this to bypass the actor's inbox-dispatch path so
// they can assert on SM state immediately after the call returns. The
// substrate's docker.doProvisionAndFire test helper composes this
// before invoking docker.doProvision, then composes the matching
// Completed/Errored fire after.
func FireProvisionRequestedForTest(a *LeaseActor) {
	_ = a.sm.Fire(context.Background(), evProvisionRequested)
}

// FireProvisionCompletedForTest fires the provision-completed SM event
// carrying the result. Exported only for cross-package tests; not part
// of the public leasesm contract.
//
// Synchronously drives Provisioning→Ready and runs
// onEnterReadyFromProvision (Status flip, ContainerIDs update,
// callback emission). result.ContainerIDs / Manifest / StackManifest /
// ServiceContainers are written under the LeaseProvisionStore's
// UpdateFn during the entry action.
func FireProvisionCompletedForTest(a *LeaseActor, result ProvisionSuccessResult) {
	_ = a.sm.Fire(context.Background(), evProvisionCompleted, result)
}

// FireProvisionErroredForTest fires the provision-errored SM event
// carrying the error context. Exported only for cross-package tests;
// not part of the public leasesm contract.
//
// Primitive-arg signature (callbackErr/lastError/logs) deliberately
// avoids exporting the internal provisionErrorInfo struct — per
// architect E-3 condition #2 (audit transitive exports). The function
// constructs provisionErrorInfo internally and passes it to sm.Fire.
//
// Synchronously drives Provisioning→Failed and runs
// onEnterFailedFromProvision (Status flip, FailCount++, LastError
// update, persist diagnostics, callback emission).
func FireProvisionErroredForTest(a *LeaseActor, callbackErr string, reason backend.Reason, lastError string, logs map[string]string) {
	_ = a.sm.Fire(context.Background(), evProvisionErrored, provisionErrorInfo{
		callbackErr: callbackErr,
		reason:      reason,
		lastError:   lastError,
		logs:        logs,
	})
}
