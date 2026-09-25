package imagefetch

import (
	"errors"
	"math/bits"
)

const (
	maxNamespaceMemory = 128 << 20
	// Each observed header may retain a seen-map entry. Nodes additionally
	// own the namespace object, a child-map entry, and (for directories) a
	// child map with its minimum bucket. These conservative allowances include
	// allocator and map growth overhead; retained string bytes are separate.
	namespaceHeaderMemory namespaceCharge = 128
	namespaceNodeMemory   namespaceCharge = 384
)

// Unsigned charges cannot return credit through negative size arithmetic.
type namespaceCharge uint

// namespaceMemory owns a monotonic, per-image model-memory allowance. Entry
// churn, global headers, whiteouts and replacement nodes never refund it, so
// it also bounds parsing work independently of the decoded byte allowance.
// Only a successful claim permits the caller to retain the new map entry,
// node or string. This envelope is fixed across ordinary and saved recovery
// issuers; a smaller saved byte ceiling cannot shrink namespace authority.
// The single root and at most maxLayers empty seen-map headers are fixed
// overhead outside this envelope; all variable map entries are charged here.
type namespaceMemory struct{ remaining namespaceCharge }

func newNamespaceMemory() namespaceMemory {
	return namespaceMemory{remaining: maxNamespaceMemory}
}

func (m *namespaceMemory) claim(bytes namespaceCharge) error {
	if bytes > m.remaining {
		return errors.New("image namespace exceeds memory budget")
	}
	m.remaining -= bytes
	return nil
}

// Retained names are bounded by the retained path budget before this function.
// The next power of two covers Go's allocation-class rounding without storing
// a second full copy of every path in the resource-accounting model.
func namespaceStringMemory(length int) namespaceCharge {
	if length == 0 {
		return 0
	}
	return namespaceCharge(1) << bits.Len(uint(length-1))
}
