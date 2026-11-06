package leasesm

import (
	"context"
	"testing"

	"github.com/qmuntal/stateless"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

// TestLeaseTransitionsAreTheEdgesTheMachineTakes pins that a substrate binding
// one series per Transitions() element holds the series of every transition
// the machine reports: each permitted edge, fired, reports exactly its own
// Transition.
func TestLeaseTransitionsAreTheEdgesTheMachineTakes(t *testing.T) {
	listed := Transitions()
	require.Len(t, listed, len(leaseTransitions))

	seen := make(map[transitionKey]bool, len(leaseTransitions))
	labels := make(map[Transition]bool, len(leaseTransitions))
	for i, edge := range leaseTransitions {
		key := transitionKey{from: edge.from, event: edge.event}
		require.False(t, seen[key], "edge %v --%v--> is declared twice", edge.from, edge.event)
		seen[key] = true
		transition := listed[i]
		require.False(t, labels[transition], "two edges share labels %+v", transition)
		labels[transition] = true
		for _, label := range []string{transition.From(), transition.To(), transition.Event()} {
			assert.NotEmpty(t, label)
		}

		if edge.guard != nil {
			got, ok := transitionOf(stateless.Transition{Source: edge.from, Destination: edge.to, Trigger: edge.event})
			require.True(t, ok)
			assert.Equal(t, transition, got)
			continue
		}
		machine := stateless.NewStateMachine(edge.from)
		permitLeaseTransitions(machine, nil)
		var reported []Transition
		machine.OnTransitioned(func(_ context.Context, tr stateless.Transition) {
			got, ok := transitionOf(tr)
			require.True(t, ok, "transition %v is not in leaseTransitions", tr)
			reported = append(reported, got)
		})
		require.NoError(t, machine.Fire(edge.event))
		assert.Equal(t, []Transition{transition}, reported)
	}

	_, ok := transitionOf(stateless.Transition{
		Source: backend.ProvisionStatusDeprovisioning, Destination: backend.ProvisionStatusReady,
		Trigger: evProvisionCompleted,
	})
	assert.False(t, ok, "an edge the machine does not permit has no Transition")
}

func TestReservedStateIsLabeledByName(t *testing.T) {
	for _, transition := range Transitions() {
		assert.NotEqual(t, "1", transition.From(), "Reserved must not render as its integer value")
	}
	assert.Contains(t, Transitions(), Transition{from: "reserved", to: "provisioning", event: "ProvisionRequested"})
}

func TestWorkerAndTerminalEventListsAreComplete(t *testing.T) {
	workers := map[string]bool{}
	for _, worker := range Workers() {
		require.NotEmpty(t, worker.Label())
		workers[worker.Label()] = true
	}
	assert.Equal(t, map[string]bool{"provision": true, "replace": true, "diag": true}, workers)

	events := map[string]bool{}
	for _, event := range TerminalEvents() {
		require.NotEmpty(t, event.Label())
		require.False(t, events[event.Label()], "duplicate label %q", event.Label())
		events[event.Label()] = true
	}
	assert.Len(t, events, 16)
	assert.Empty(t, TerminalEvent(0).Label(), "the zero value is no event")
}
