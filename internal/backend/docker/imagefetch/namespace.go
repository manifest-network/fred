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

	// Verification bytes may come from multi-terabyte legacy disk headroom.
	// They never grant more than 1 GiB model memory, 256 MiB retained paths
	// or 512 MiB resolution work to a single active preparation.
	maxNamespaceVerification = 32 << 30
	namespaceMemoryRatio     = 32
	retainedPathRatio        = 128
	resolvedPathRatio        = 64
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
	envelope      namespaceEnvelope
	remaining     namespaceCharge
	pathBytes     int64
	resolvedBytes int64
}

// namespaceEnvelope separates parser resources from disk-sized verification.
// Every new or recovered preparation receives the same construction ceiling;
// callers cannot accidentally turn host free disk into unbounded model memory.
type namespaceEnvelope struct {
	memory             namespaceCharge
	retained, resolved int64
}

func newNamespaceMemory(verification imagebudget.VerificationBudget) namespaceMemory {
	if !verification.Valid() {
		return namespaceMemory{}
	}
	bytes := min(verification.Bytes(), int64(maxNamespaceVerification))
	envelope := namespaceEnvelope{
		memory:   namespaceCharge(max(minNamespaceMemory, bytes/namespaceMemoryRatio)),
		retained: max(minRetainedPathBytes, bytes/retainedPathRatio),
		resolved: max(minResolvedPathBytes, bytes/resolvedPathRatio),
	}
	return namespaceMemory{envelope: envelope, remaining: envelope.memory}
}

func (m namespaceMemory) limit() namespaceCharge   { return m.envelope.memory }
func (m namespaceMemory) retainedPathLimit() int64 { return m.envelope.retained }
func (m namespaceMemory) resolvedPathLimit() int64 { return m.envelope.resolved }

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
// durable verification dimension. Issued limits are bounded by floor(verification/ratio),
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
