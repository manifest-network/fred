//go:build linux

package fstree

import (
	"fmt"

	"github.com/manifest-network/fred/internal/fstree/internal/at"
)

// Name is one path component: 1 to 255 bytes, free of '/' and NUL, and
// neither "." nor "..". A *at syscall resolves it inside its directory
// descriptor and nowhere else, so a Name cannot reach above, or more than
// one level below, the directory it is used with.
//
// ParseName is the only way to build a usable Name. Its field is unexported,
// so a path, or any string that has not passed the rules, cannot be handed to
// RemoveBeneath or WalkBeneath. The zero Name is invalid, and both refuse it
// with ErrInvalidName.
type Name = at.Name

// ParseName returns name as a Name, or an error wrapping ErrInvalidName when
// it is not a single path component. The error carries at most 64 bytes of
// name.
//
// This file is the only one in fstree that makes a Name, here and in
// cutName; a guard test pins that, so a name the traversal read from a
// directory cannot be turned back into a Name and used elsewhere.
func ParseName(name string) (Name, error) {
	parsed, ok := at.ParseName(name)
	if !ok {
		return Name{}, fmt.Errorf("%w: %q", ErrInvalidName, shortName(name))
	}
	return parsed, nil
}

// cutName returns the name of the cut numbered n: cutPrefix and n as 16 hex
// digits.
func cutName(n uint64) (Name, error) {
	return ParseName(fmt.Sprintf("%s%016x", cutPrefix, n))
}
