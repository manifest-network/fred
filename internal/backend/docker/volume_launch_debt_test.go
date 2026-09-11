package docker

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

type volumeDispatchHarness struct {
	backend    *Backend
	compose    *mockComposeExecutor
	prepared   imageexec.PreparedProject
	settlement *shared.OperationSettlement
	execution  shared.OperationExecutionClaim
	q          *quiescedVolumes
	refusal    error
	run        func(context.Context, *quiescedVolumes) error
}

func newVolumeDispatchHarness(t *testing.T) *volumeDispatchHarness {
	t.Helper()
	callbacks, err := newBoundCallbackStoreForTest(t, shared.CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = callbacks.Close() })
	_, settlement := operationSettlementForCallbackTest(t, callbacks)
	value, ok := operationIntentTestAuthorities.Load(callbacks)
	require.True(t, ok)
	authority := value.(*operationIntentTestAuthority)
	b := &Backend{stopCtx: t.Context(), storageIdentity: authority.storage.ID(), storeAuthorityGate: authority.gate,
		storageVerifier: testDockerRuntimeStorageVerifier{id: authority.storage.ID()}}
	b.volumeLaunches, err = newVolumeLaunchCoordinator(callbacks)
	require.NoError(t, err)
	docker := &mockDockerClient{}
	compose := &mockComposeExecutor{}
	compose.bindImages(docker.imageAdmitter())
	image, err := docker.AdmitImage(t.Context(), "launch:fixture")
	require.NoError(t, err)
	prepared, err := compose.PrepareProject(&composetypes.Project{Name: "launch-fixture", Services: composetypes.Services{
		"app": {Image: image.Reference()},
	}}, map[string]imageexec.Image{"app": image})
	require.NoError(t, err)
	h := &volumeDispatchHarness{backend: b, compose: compose, prepared: prepared, settlement: settlement}
	// Inject a refusal through the bound authority seam, not through a caller
	// flag that purports to prove whether a launch was dispatched.
	authorize := func(ctx context.Context, operation string) (context.Context, func(), error) {
		if operation == shared.MaintenanceTargetLaunchStep && h.refusal != nil {
			return nil, nil, h.refusal
		}
		return b.authorizeStorageMutation(ctx, operation)
	}
	require.NoError(t, shared.BindOperationSubstrateExecutor(settlement, authorize, b.completeStorageMutation,
		func(runner substratemutation.Runner, subject shared.OperationPhysicalSubject) func(context.Context) error {
			m := &storageMutations{runner: runner, operationSubject: subject, leaseUUID: subject.LeaseUUID(),
				ops: storageMutationOperations{backend: b, compose: compose, docker: docker}}
			return func(ctx context.Context) error {
				var err error
				h.q, err = b.quiesceLaunchVolumes(ctx, m, nil, nil, nil)
				if err != nil {
					return err
				}
				// Deliberately retain the reservation beyond the callback to prove
				// that an expired Runner cannot allocate debt through a copied q.
				t.Cleanup(h.q.release)
				return h.run(ctx, h.q)
			}
		},
		func(ctx context.Context, run func(context.Context) error, _ shared.OperationPhysicalSubject) error {
			return run(ctx)
		},
		func(context.Context, shared.OperationPhysicalSubject) (shared.OperationPhysicalEvidence, error) {
			return shared.OperationPhysicalEvidence{}, errors.New("fixture retains the originating Started intent")
		},
	))
	candidate, err := settlement.NewOperationIntentCandidate(dockerOperationIntentSpec(t, authority.storage.ID()))
	require.NoError(t, err)
	admission, err := settlement.BeginOperationIntent(candidate)
	require.NoError(t, err)
	claim, ok := admission.CreatedClaim()
	require.True(t, ok)
	release, err := settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	h.execution, err = settlement.StartOperationExecution(release)
	require.NoError(t, err)
	return h
}

func (h *volumeDispatchHarness) execute(t *testing.T, run func(context.Context, *quiescedVolumes) error) shared.OperationExecutionOutcome {
	t.Helper()
	h.run = run
	return h.settlement.ExecuteOperation(t.Context(), h.execution)
}

func TestVolumeLaunchDebtRefusedDispatchAllocatesNothing(t *testing.T) {
	for _, scenario := range []string{"canceled", "pre-attestation", "zero runner"} {
		t.Run(scenario, func(t *testing.T) {
			h := newVolumeDispatchHarness(t)
			calls := 0
			h.compose.UpFn = func(context.Context, *composetypes.Project, composeUpOpts) error { calls++; return nil }
			h.execute(t, func(ctx context.Context, q *quiescedVolumes) error {
				switch scenario {
				case "canceled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				case "pre-attestation":
					h.refusal = errors.New("authority refused before launch")
				case "zero runner":
					copied, mutations := *q, *q.mutations
					mutations.runner = substratemutation.Runner{}
					copied.mutations = &mutations
					q = &copied
				}
				err := h.backend.volumeLaunches.compose(ctx, q, h.prepared, composeUpOpts{})
				require.Error(t, err)
				require.NoError(t, h.backend.volumeLaunches.checkNamespace(q.mutations.leaseUUID))
				return err
			})
			require.Zero(t, calls)
			err := h.backend.volumeLaunches.compose(t.Context(), h.q, h.prepared, composeUpOpts{})
			require.ErrorIs(t, err, substratemutation.ErrCapabilityUnavailable, "retained facade cannot allocate after its Runner expires")
			require.NoError(t, h.backend.volumeLaunches.checkNamespace(h.q.mutations.leaseUUID))
			require.Zero(t, calls)
		})
	}
}

func TestVolumeLaunchDebtDispatchOwnsWriteAheadAndExactCompletion(t *testing.T) {
	for _, scenario := range []string{"success", "reply lost", "earlier preparation issue"} {
		t.Run(scenario, func(t *testing.T) {
			h := newVolumeDispatchHarness(t)
			calls := 0
			h.compose.UpFn = func(context.Context, *composetypes.Project, composeUpOpts) error {
				calls++
				require.ErrorIs(t, h.backend.volumeLaunches.checkNamespace(h.q.mutations.leaseUUID), shared.ErrVolumeLaunchUnsettled,
					"durable launch debt must exist before the SDK effect")
				if scenario == "reply lost" {
					return errors.New("daemon reply lost after dispatch")
				}
				return nil
			}
			outcome := h.execute(t, func(ctx context.Context, q *quiescedVolumes) error {
				if scenario == "earlier preparation issue" {
					require.Error(t, q.mutations.runner.Prepare(ctx, "best-effort image preparation", func(context.Context) error {
						return errors.New("optional preparation unavailable")
					}))
				}
				err := h.backend.volumeLaunches.compose(ctx, q, h.prepared, composeUpOpts{})
				if scenario == "reply lost" {
					require.ErrorContains(t, err, "reply lost")
					require.ErrorIs(t, h.backend.volumeLaunches.checkNamespace(q.mutations.leaseUUID), shared.ErrVolumeLaunchUnsettled)
				} else {
					require.NoError(t, err)
					require.NoError(t, h.backend.volumeLaunches.checkNamespace(q.mutations.leaseUUID))
				}
				return err
			})
			require.Equal(t, 1, calls)
			require.IsType(t, shared.OperationExecutionAmbiguous{}, outcome,
				"settling launch debt does not erase the outer workflow's evidence requirements or issues")
		})
	}
}
