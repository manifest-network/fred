package placement

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/maintenanceid"
)

func recoverySchedulerEntries(confirmed, ordinary int) []maintenanceRecoveryEntry {
	id, err := maintenanceid.Parse(maintenanceIDA)
	if err != nil {
		panic(err)
	}
	entries := make([]maintenanceRecoveryEntry, 0, confirmed+ordinary)
	for index := range max(confirmed, ordinary) {
		if index < confirmed {
			entries = append(entries, maintenanceRecoveryEntry{candidate: maintenanceRecoveryCandidate{
				lease: fmt.Sprintf("confirmed-%04d", index), id: id, phase: maintenancePayloadConfirmed,
			}})
		}
		if index < ordinary {
			entries = append(entries, maintenanceRecoveryEntry{candidate: maintenanceRecoveryCandidate{
				lease: fmt.Sprintf("ordinary-%04d", index), id: id, phase: maintenanceDeliveryOutstanding,
			}})
		}
	}
	return entries
}

func TestMaintenanceSchedulerAlternatesDeadlineExhaustingClassesAndRotatesIndependently(t *testing.T) {
	entries := recoverySchedulerEntries(40, 40)
	scheduler := newMaintenanceRecoveryScheduler()
	seen := make(map[string]int)
	// Model a call consuming the whole deadline, or a busy selection followed
	// by cancellation. Neither outcome may pin the head of either class.
	for index := range 160 {
		ctx, cancel := context.WithCancel(t.Context())
		pass := scheduler.begin("backend-a", entries)
		entry, selected := pass.next(ctx)
		require.True(t, selected)
		class := "confirmed"
		if index%2 != 0 {
			class = "ordinary"
		}
		require.Equal(t, fmt.Sprintf("%s-%04d", class, (index/2)%40), entry.candidate.lease)
		seen[entry.candidate.lease]++
		cancel()
		_, selected = pass.next(ctx)
		require.False(t, selected)
	}
	require.Len(t, seen, 80)
	for _, opportunities := range seen {
		require.Equal(t, 2, opportunities)
	}
}

func TestMaintenanceSchedulerKeepsAllConfirmedEligibleBeyondOrdinaryBatch(t *testing.T) {
	scheduler := newMaintenanceRecoveryScheduler()
	entries := recoverySchedulerEntries(40, 40)
	pass := scheduler.begin("backend-a", entries)
	var confirmed, ordinary []string
	for {
		entry, selected := pass.next(t.Context())
		if !selected {
			break
		}
		if entry.candidate.phase == maintenancePayloadConfirmed {
			confirmed = append(confirmed, entry.candidate.lease)
		} else {
			ordinary = append(ordinary, entry.candidate.lease)
		}
	}
	require.Len(t, confirmed, 40, "callback completion cannot fall outside the ordinary batch")
	require.Len(t, ordinary, maxMaintenanceRecoveryCommandsPerBackendPass)
	next := scheduler.begin("backend-a", entries)
	entry, selected := next.next(t.Context())
	require.True(t, selected)
	require.Equal(t, "ordinary-0032", entry.candidate.lease, "ordinary rotation cannot inherit the confirmed cursor")
	entry, selected = next.next(t.Context())
	require.True(t, selected)
	require.Equal(t, "confirmed-0000", entry.candidate.lease)
}

func TestMaintenanceSchedulerCancellationAndBackendResetDoNotSkipOpportunities(t *testing.T) {
	entries := recoverySchedulerEntries(2, 2)
	scheduler := newMaintenanceRecoveryScheduler()
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, selected := scheduler.begin("backend-a", entries).next(canceled)
	require.False(t, selected)
	first, selected := scheduler.begin("backend-a", entries).next(t.Context())
	require.True(t, selected)
	require.Equal(t, "confirmed-0000", first.candidate.lease)
	independent, selected := scheduler.begin("backend-b", entries).next(t.Context())
	require.True(t, selected)
	require.Equal(t, first.candidate.lease, independent.candidate.lease)
	scheduler.retain(map[string][]maintenanceRecoveryEntry{"backend-a": entries})
	restarted, selected := scheduler.begin("backend-b", entries).next(t.Context())
	require.True(t, selected)
	require.Equal(t, first.candidate.lease, restarted.candidate.lease, "a drained/reopened lane starts fairly from fresh observations")
	next, selected := scheduler.begin("backend-a", entries).next(t.Context())
	require.True(t, selected)
	require.Equal(t, "ordinary-0000", next.candidate.lease, "another backend's reset cannot reset active rotation")
}

func TestMaintenanceSchedulerNewCompletionLeavesItsOldOrdinaryPosition(t *testing.T) {
	entries := recoverySchedulerEntries(0, 40)
	scheduler := newMaintenanceRecoveryScheduler()
	first := scheduler.begin("backend-a", entries)
	for range maxMaintenanceRecoveryCommandsPerBackendPass {
		_, selected := first.next(t.Context())
		require.True(t, selected)
	}
	// An exact callback changes only this candidate's class. Its old position
	// before the ordinary cursor must not delay its next completion opportunity.
	entries[0].candidate.phase = maintenancePayloadConfirmed
	entry, selected := scheduler.begin("backend-a", entries).next(t.Context())
	require.True(t, selected)
	require.Equal(t, "ordinary-0000", entry.candidate.lease)
	require.Equal(t, maintenancePayloadConfirmed, entry.candidate.phase)
}

// This isolates scheduler CPU/allocation cost at the current pending ceiling
// and an over-limit legacy reopen. It deliberately performs no payload decode
// or I/O; journal transaction/reopen costs need a separate storage benchmark.
func BenchmarkMaintenanceRecoveryScheduler(b *testing.B) {
	for _, count := range []int{1024, 8192} {
		b.Run(fmt.Sprintf("pending_%d", count), func(b *testing.B) {
			entries := recoverySchedulerEntries(count/2, count/2)
			scheduler := newMaintenanceRecoveryScheduler()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				pass := scheduler.begin("backend-a", entries)
				for {
					if _, selected := pass.next(b.Context()); !selected {
						break
					}
				}
			}
		})
	}
}
