//go:build linux

package fstree

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/fstree/internal/at"
)

// The kernel running this test reports mount IDs (the suite needs Linux 5.8
// or later, as fstree does): the probe passes on a real directory.
func TestCheckKernelSupportPassesOnThisKernel(t *testing.T) {
	dir, err := os.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dir.Close() })
	require.NoError(t, CheckKernelSupport(dir))
	require.Error(t, CheckKernelSupport(nil))

	closed, err := os.Open(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, closed.Close())
	err = CheckKernelSupport(closed)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrUnsupportedKernel, "a closed directory is not an old kernel")
}

// The classification is total: only a missing mount ID is an unsupported
// kernel, and no failure ever reads as support.
func TestKernelSupportErrorIsTotal(t *testing.T) {
	require.NoError(t, kernelSupportError(nil))

	err := kernelSupportError(fmt.Errorf("statx: %w", at.ErrNoMountID))
	require.ErrorIs(t, err, ErrUnsupportedKernel)
	assert.Contains(t, err.Error(), "Linux 5.8")

	err = kernelSupportError(errors.New("EIO"))
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrUnsupportedKernel)
}
