package docker

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/manifest-network/fred/internal/fstree"
)

// requireTreeRemovalSupport refuses to start a volume manager on a kernel
// too old for fstree. Before Linux 5.8 the kernel reports no mount IDs, and
// fstree then refuses every removal with ErrCrossDevice: every XFS deletion
// would be held as cross_device and every writable-path wipe would fail,
// while the backend looked healthy. Failing Start names the cause instead.
func requireTreeRemovalSupport(dataPath string) error {
	dir, err := os.Open(dataPath) //nolint:gosec // operator-configured volume root, validated at construction
	if err != nil {
		return fmt.Errorf("open volume root %s to check tree-removal support: %w", dataPath, err)
	}
	defer func() { _ = dir.Close() }()
	if err := fstree.CheckKernelSupport(dir); err != nil {
		return fmt.Errorf("volume root %s: tenant tree removal is unsupported on this kernel: %w", dataPath, err)
	}
	return nil
}

// Tenant directory trees are removed in two places, both through fstree
// (ENG-1117): the XFS deletion of a condemned volume (delete_stage) and the
// writable-path wipe of a live volume before it is reseeded (writable_path).
// Each is a treeRemovalSite, declared with its series in series.go.

// treeRemovalClass is the closed classification of one fstree removal result.
// classifyTreeRemoval is total over errors, and every consumer (the metric
// label, the XFS hold reason) maps from it, so the two cannot disagree about
// what a given failure was.
type treeRemovalClass uint8

const (
	treeRemovalRemoved treeRemovalClass = iota + 1
	treeRemovalDeadline
	treeRemovalCanceled
	treeRemovalCrossDevice
	treeRemovalCutRefused
	treeRemovalTreeChanged
	treeRemovalUndeletable
	treeRemovalFailed
)

// classifyTreeRemoval maps a removal result to its class. Context errors come
// first, because a stopped walk is progress, not a refusal. Every sentinel is
// fstree's own verdict: a cut-anchor detach that fails, even by refusing an
// anchor on another device, unwraps only to ErrCutRefused, never to the
// hook's error. Anything unrecognized (EIO, EMFILE, ...) is
// treeRemovalFailed.
func classifyTreeRemoval(err error) treeRemovalClass {
	switch {
	case err == nil:
		return treeRemovalRemoved
	case errors.Is(err, context.DeadlineExceeded):
		return treeRemovalDeadline
	case errors.Is(err, context.Canceled):
		return treeRemovalCanceled
	case errors.Is(err, fstree.ErrCrossDevice):
		return treeRemovalCrossDevice
	case errors.Is(err, fstree.ErrCutRefused):
		return treeRemovalCutRefused
	case errors.Is(err, fstree.ErrTreeChanged):
		return treeRemovalTreeChanged
	case errors.Is(err, fstree.ErrUndeletable):
		return treeRemovalUndeletable
	default:
		return treeRemovalFailed
	}
}

// Outcome labels for treeRemovalsTotal, bound per site in series.go.
const (
	treeRemovalOutcomeRemoved     = "removed"
	treeRemovalOutcomeCutRefused  = "cut_refused"
	treeRemovalOutcomeTreeChanged = "tree_changed"
	treeRemovalOutcomeCrossDevice = "cross_device"
	treeRemovalOutcomeUndeletable = "undeletable"
	treeRemovalOutcomeCanceled    = "canceled"
	treeRemovalOutcomeError       = "error"
)

// counter is a class's series among one site's outcomes. Every class has
// one: an unrecognized class counts as error.
func (c treeRemovalClass) counter(outcomes treeRemovalOutcomeCounters) prometheus.Counter {
	switch c {
	case treeRemovalRemoved:
		return outcomes.removed
	case treeRemovalDeadline, treeRemovalCanceled:
		return outcomes.canceled
	case treeRemovalCrossDevice:
		return outcomes.crossDevice
	case treeRemovalCutRefused:
		return outcomes.cutRefused
	case treeRemovalTreeChanged:
		return outcomes.treeChanged
	case treeRemovalUndeletable:
		return outcomes.undeletable
	default:
		return outcomes.error
	}
}

// observeTreeRemoval counts one removal at site: its outcome, and any cuts it
// made. A cut means the tree was deeper than fstree's ancestry bound.
func observeTreeRemoval(site treeRemovalSite, report fstree.RemoveReport, err error) {
	classifyTreeRemoval(err).counter(site.outcomes).Inc()
	if report.Cuts > 0 {
		site.cuts.Add(float64(report.Cuts))
	}
}
