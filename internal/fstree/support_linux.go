//go:build linux

package fstree

import (
	"errors"
	"fmt"
	"os"

	"github.com/manifest-network/fred/internal/fstree/internal/at"
)

// CheckKernelSupport reports whether fstree can work on this kernel by making
// the identity read it makes before touching any tree: a statx of dir that
// must carry a mount ID. A kernel without one (before Linux 5.8) yields
// ErrUnsupportedKernel; there, every RemoveBeneath and WalkBeneath would
// refuse with ErrCrossDevice. A caller that removes tenant trees checks this
// once at startup and refuses to run, instead of failing every removal later.
// dir is read only: its descriptor is borrowed for one fstat-class call.
func CheckKernelSupport(dir *os.File) error {
	if dir == nil {
		return errors.New("fstree: nil directory")
	}
	var statErr error
	if err := at.BorrowView(dir, func(v at.View) { _, statErr = v.Stat() }); err != nil {
		return parentError(err)
	}
	return kernelSupportError(statErr)
}

// kernelSupportError classifies the probe's stat. A missing mount ID is
// ErrUnsupportedKernel; any other failure is reported as itself, never as
// support.
func kernelSupportError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, at.ErrNoMountID):
		return fmt.Errorf("%w: %w", ErrUnsupportedKernel, err)
	default:
		return fmt.Errorf("fstree: stat the probed directory: %w", err)
	}
}
