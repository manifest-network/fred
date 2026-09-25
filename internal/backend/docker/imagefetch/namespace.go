package imagefetch

import (
	"errors"
	"math/bits"

	"github.com/manifest-network/fred/internal/backend/shared/imagebudget"
)

const (
	minNamespaceMemory   = 128 << 20
	minRetainedPathBytes = 32 << 20
	minResolvedPathBytes = 64 << 20
	namespaceMemoryRatio = 32
	retainedPathRatio    = 128
	resolvedPathRatio    = 64
	// Each observed header may retain a seen-map entry. Nodes additionally
	// own the namespace object, a child-map entry, and (for directories) a
	// child map with its minimum bucket. These conservative allowances include
	// allocator and map growth overhead; retained string bytes are separate.
	namespaceHeaderMemory namespaceCharge = 128
	namespaceNodeMemory   namespaceCharge = 384
)

// Unsigned charges cannot return credit through negative size arithmetic.
type namespaceCharge uint64

// namespaceMemory owns a monotonic, per-image model-memory allowance. Entry
// churn, global headers, whiteouts and replacement nodes never refund it, so
// it also bounds parsing work independently of the decoded byte allowance.
// Only a successful claim permits the caller to retain the new map entry,
// node or string. Its dimensions derive from the typed verification ceiling;
// the fixed floors preserve previously admitted saved recovery envelopes.
// The single root and at most maxLayers empty seen-map headers are fixed
// overhead outside this envelope; all variable map entries are charged here.
type namespaceMemory struct {
	verification  imagebudget.VerificationBudget
	remaining     namespaceCharge
	pathBytes     int64
	resolvedBytes int64
}

func newNamespaceMemory(verification imagebudget.VerificationBudget) namespaceMemory {
	if !verification.Valid() {
		return namespaceMemory{}
	}
	memory := namespaceMemory{verification: verification}
	memory.remaining = memory.limit()
	return memory
}

func (m namespaceMemory) limit() namespaceCharge {
	if !m.verification.Valid() {
		return 0
	}
	return namespaceCharge(max(minNamespaceMemory, m.verification.Bytes()/namespaceMemoryRatio))
}

func (m namespaceMemory) retainedPathLimit() int64 {
	if !m.verification.Valid() {
		return 0
	}
	return max(minRetainedPathBytes, m.verification.Bytes()/retainedPathRatio)
}

func (m namespaceMemory) resolvedPathLimit() int64 {
	if !m.verification.Valid() {
		return 0
	}
	return max(minResolvedPathBytes, m.verification.Bytes()/resolvedPathRatio)
}

func (m *namespaceMemory) claimName(name string) error {
	if int64(len(name)) > m.retainedPathLimit()-m.pathBytes {
		return errors.New("image paths exceed retained path byte budget")
	}
	if err := m.claim(namespaceStringMemory(len(name))); err != nil {
		return err
	}
	m.pathBytes += int64(len(name))
	return nil
}

func (m *namespaceMemory) claimResolution(component string) error {
	if int64(len(component)+1) > m.resolvedPathLimit()-m.resolvedBytes {
		return errors.New("image symlink resolution exceeds path work budget")
	}
	m.resolvedBytes += int64(len(component) + 1)
	return nil
}

// recoveryBytes projects consumed namespace authority back into the existing
// durable verification dimension. Issued limits are floor(verification/ratio),
// so each multiplication is bounded by its valid issuer and cannot overflow.
// Usage within a fixed floor needs no extra byte authority on recovery.
func (m namespaceMemory) recoveryBytes() int64 {
	var required int64
	if used := m.limit() - m.remaining; used > minNamespaceMemory {
		required = int64(used) * namespaceMemoryRatio
	}
	if m.pathBytes > minRetainedPathBytes {
		required = max(required, m.pathBytes*retainedPathRatio)
	}
	if m.resolvedBytes > minResolvedPathBytes {
		required = max(required, m.resolvedBytes*resolvedPathRatio)
	}
	return required
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
