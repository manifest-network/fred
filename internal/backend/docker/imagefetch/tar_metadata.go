package imagefetch

import (
	"errors"
	"math"
)

// retainedTarMetadata separates parser-owned header segments from arbitrary
// decoded bytes after the tar terminator. Tar-split emits a JSON segment for
// every nonempty trailing Read, which can be one byte for gzip multistreams.
type retainedTarMetadata struct {
	parserBytes uint64
	tailBytes   uint64
}

const tarSplitTailByteAllocation = 128

func (m retainedTarMetadata) allowance(used uint64) (int64, error) {
	// No admitted image may exceed twice the largest verification allowance
	// (MaxInt64/8). Keeping this structural ceiling before multiplication also
	// leaves room for subsequent filesystem and archive accounting.
	const ceiling = uint64(math.MaxInt64 / 4)
	if used > ceiling || m.parserBytes > (ceiling-used)/2 {
		return 0, errors.New("retained image metadata exceeds import allocation budget")
	}
	prefix := 2 * m.parserBytes
	if m.tailBytes > (ceiling-used-prefix)/tarSplitTailByteAllocation {
		return 0, errors.New("retained image metadata exceeds import allocation budget")
	}
	// A segment's type, signed-width position, JSON framing and base64 for a
	// one-byte payload fit in 64 bytes. A 128-byte allowance covers the worst case
	// of one segment per byte plus gzip framing/expansion. Stream-level fixed
	// overhead is additionally covered by the per-layer metadata allowance.
	return int64(prefix + tarSplitTailByteAllocation*m.tailBytes), nil
}
