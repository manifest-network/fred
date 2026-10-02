//go:build linux

package fstree

import "fmt"

// Name is one path component: 1 to 255 bytes, free of '/' and NUL, and
// neither "." nor "..". A *at syscall resolves it inside its directory
// descriptor and nowhere else, so a Name cannot reach above, or more than
// one level below, the directory it is used with.
//
// ParseName is the only way to build a usable Name. Its field is unexported,
// so a path, or any string that has not passed the rules, cannot be handed to
// RemoveBeneath or WalkBeneath. The zero Name is invalid, and both refuse it
// with ErrInvalidName.
type Name struct {
	s string
}

// ParseName returns name as a Name, or an error wrapping ErrInvalidName when
// it is not a single path component. The error carries at most 64 bytes of
// name.
func ParseName(name string) (Name, error) {
	if !validName(name) {
		return Name{}, fmt.Errorf("%w: %q", ErrInvalidName, shortName(name))
	}
	return Name{s: name}, nil
}

// String returns the component as given to ParseName, or "" for the zero
// Name.
func (n Name) String() string { return n.s }

// isDotName reports whether name is "." or "..".
func isDotName[T ~string | ~[]byte](name T) bool {
	return (len(name) == 1 && name[0] == '.') ||
		(len(name) == 2 && name[0] == '.' && name[1] == '.')
}

// validName reports whether name is one path component a *at syscall
// resolves inside its directory descriptor: 1 to NAME_MAX bytes, free of '/'
// and NUL, and neither "." nor "..". ParseName applies it to the names
// callers pass; the directory reader applies it to the names the kernel
// lists.
func validName[T ~string | ~[]byte](name T) bool {
	if len(name) == 0 || len(name) > maxNameLen || isDotName(name) {
		return false
	}
	for i := range len(name) {
		if name[i] == '/' || name[i] == 0 {
			return false
		}
	}
	return true
}
