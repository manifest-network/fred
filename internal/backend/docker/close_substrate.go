package docker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

// closeSubstrate is an invocation-only capability for one exact opaque close
// subject. It cannot be retargeted after construction.
type closeSubstrate func(context.Context) error

func newCloseStorageMutations(
	runner substratemutation.Runner,
	subject shared.ClosePhysicalSubject,
	ops storageMutationOperations,
) *storageMutations {
	claim := subject.Intent()
	return &storageMutations{
		runner: runner, ops: ops, leaseUUID: subject.LeaseUUID(),
		tenant: claim.Tenant(), providerUUID: claim.ProviderUUID(),
		callbackURL: claim.CallbackURL(), lifecycleURL: claim.LifecycleCallbackURL(),
		allowedLease: map[string]struct{}{subject.LeaseUUID(): {}},
		cleanupOnly:  claim.CleanupOnly(),
	}
}

func buildCloseSubstrate(
	b *Backend,
	ops storageMutationOperations,
) func(substratemutation.Runner, shared.ClosePhysicalSubject) closeSubstrate {
	return func(runner substratemutation.Runner, subject shared.ClosePhysicalSubject) closeSubstrate {
		mutations := newCloseStorageMutations(runner, subject, ops)
		return func(ctx context.Context) error {
			return b.doClosePhysical(mutations, ctx, subject)
		}
	}
}

func runCloseSubstrate(
	ctx context.Context,
	capability closeSubstrate,
	_ shared.ClosePhysicalSubject,
) error {
	if capability == nil {
		return errors.New("close substrate capability is unavailable")
	}
	return capability(ctx)
}

// classifyClosePhysical is the sole close verdict function. The caller cannot
// supply retained/destroyed booleans: it inventories the complete exact lease
// namespace, then joins it with the current durable retention generation.
func (b *Backend) classifyClosePhysical(
	ctx context.Context,
	subject shared.ClosePhysicalSubject,
) (shared.ClosePhysicalEvidence, error) {
	if !subject.Valid() || subject.LeaseUUID() == "" {
		return shared.ClosePhysicalEvidence{}, errors.New("close physical subject is invalid")
	}
	containers, err := b.strictIdentityBoundOperationInventory(ctx)
	if err != nil {
		return shared.ClosePhysicalEvidence{}, fmt.Errorf("inventory close containers: %w", err)
	}
	for _, container := range containers {
		if container.LeaseUUID == subject.LeaseUUID() {
			return shared.NewCloseIncomplete(subject)
		}
	}

	volumes, err := b.volumes.ListForProof(ctx)
	if err != nil {
		return shared.ClosePhysicalEvidence{}, fmt.Errorf("inventory close volumes: %w", err)
	}
	canonicalPrefix := leaseVolumePrefix(subject.LeaseUUID())
	retainedPrefix := retainedVolumePrefix + subject.LeaseUUID() + "-"
	var canonical, retained []string
	for _, name := range volumes {
		switch {
		case strings.HasPrefix(name, canonicalPrefix):
			canonical = append(canonical, name)
		case strings.HasPrefix(name, retainedPrefix):
			retained = append(retained, name)
		}
	}
	if len(canonical) != 0 {
		return shared.NewCloseIncomplete(subject)
	}

	claim := subject.Intent()
	record, err := b.retentionStore.Get(subject.LeaseUUID())
	if err != nil {
		return shared.ClosePhysicalEvidence{}, fmt.Errorf("read close retention authority: %w", err)
	}
	if record == nil {
		if len(retained) != 0 {
			return shared.NewCloseIncomplete(subject)
		}
		return shared.NewCloseDestroyed(subject)
	}
	if !claim.RetainOnClose() || claim.CleanupOnly() ||
		record.Status != shared.RetentionStatusActive {
		return shared.NewCloseIncomplete(subject)
	}
	proof, err := b.closeSettlement.ProveRetention(claim)
	if err != nil {
		return shared.ClosePhysicalEvidence{}, fmt.Errorf("prove close retention authority: %w", err)
	}
	want := proof.RetainedVolumeNames()
	slices.Sort(want)
	slices.Sort(retained)
	if !slices.Equal(retained, want) {
		return shared.NewCloseIncomplete(subject)
	}
	return shared.NewCloseRetained(subject, proof)
}
