//go:build integration

package chain

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGenesisCopyIsSealedWithTheChain pins that preparation cannot outlive
// the start of the node through a copy of its handle. A copy that stayed
// open would edit a genesis file the running node no longer reads.
func TestGenesisCopyIsSealedWithTheChain(t *testing.T) {
	c := &Chain{}
	handle := &Genesis{chain: c}
	copied := *handle
	require.False(t, handle.sealed())
	require.False(t, copied.sealed())

	// What Start does once prepare has returned.
	c.started = true

	require.True(t, handle.sealed())
	require.True(t, copied.sealed())
}
