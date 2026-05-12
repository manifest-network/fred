// Package k3s is the ENG-148 final-acceptance compile probe.
//
// Proves that the substrate-agnostic surface introduced by ENG-148
// (shared/leasesm, shared/manifest, shared/workbarrier, and the flat
// shared/ package) is implementable by a non-Docker substrate without
// reaching back into internal/backend/docker/.
//
// The probe uses compile-time interface satisfaction guards for the
// four SM/actor seams a substrate must implement (InstanceInspector,
// DiagnosticsGatherer, LeaseProvisionStore, SMMetrics) plus blank
// imports for the non-interface substrate surface so the dep graph
// reflects realistic K3s consumer use. Stub implementations are
// deliberately no-op — the goal is compile-time verification, not
// runtime behavior. Substantive K3s implementation (Pod creation,
// status watching, multi-cluster routing, callback delivery, stack
// support) is deferred to ENG-133.
package k3s

import (
	"context"

	_ "github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	_ "github.com/manifest-network/fred/internal/backend/shared/manifest"
	_ "github.com/manifest-network/fred/internal/backend/shared/workbarrier"
)

// --- Stub adapters ------------------------------------------------
// Each type below is a no-op shell whose only job is to satisfy a
// substrate-agnostic interface at compile time. ENG-133 replaces
// them with real K3s implementations.

type k3sInstanceInspector struct{}

func (k3sInstanceInspector) InspectInstance(ctx context.Context, instanceID string) (*leasesm.InstanceState, error) {
	// TODO(ENG-133): inspect pod status via k8s.io/client-go.
	return nil, nil
}

type k3sDiagnosticsGatherer struct{}

func (k3sDiagnosticsGatherer) GatherDiagnostics(ctx context.Context, instanceID string, state *leasesm.InstanceState) string {
	// TODO(ENG-133): collect pod events + container logs.
	return ""
}

type k3sProvisionStore struct{}

func (k3sProvisionStore) Get(leaseUUID string) (*leasesm.ProvisionState, bool) {
	// TODO(ENG-133): map-lookup under the K3s backend's mutex.
	return nil, false
}

func (k3sProvisionStore) UpdateFn(leaseUUID string, fn func(*leasesm.ProvisionState)) bool {
	// TODO(ENG-133): closure-under-lock per LeaseProvisionStore contract.
	return false
}

type k3sSMMetrics struct{}

func (k3sSMMetrics) SMTransition(source, dest, trigger string) {}
func (k3sSMMetrics) ActorCreated()                             {}
func (k3sSMMetrics) ActorPanic()                               {}
func (k3sSMMetrics) FailingRaceSkipped()                       {}
func (k3sSMMetrics) WorkerPanic(workerType string)             {}
func (k3sSMMetrics) TerminalEventDropped(event string)         {}
func (k3sSMMetrics) ActiveProvisionsInc()                      {}
func (k3sSMMetrics) ActiveProvisionsDec()                      {}

// --- Compile-time interface satisfaction guards -------------------
// These are the structural proof that the ENG-148 substrate-agnostic
// surface is implementable from a non-Docker package. If any fails to
// compile, the ENG-148 acceptance gate is not met.

var (
	_ leasesm.InstanceInspector   = (*k3sInstanceInspector)(nil)
	_ leasesm.DiagnosticsGatherer = (*k3sDiagnosticsGatherer)(nil)
	_ leasesm.LeaseProvisionStore = (*k3sProvisionStore)(nil)
	_ leasesm.SMMetrics           = (*k3sSMMetrics)(nil)
)
