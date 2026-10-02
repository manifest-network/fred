//go:build linux

package at

// MaxNameLen is NAME_MAX, the longest single path component Linux filesystems
// accept.
const MaxNameLen = 255

// Name is one path component: 1 to MaxNameLen bytes, free of '/' and NUL, and
// neither "." nor "..". A *at syscall resolves it inside its directory
// descriptor and nowhere else, so a Name cannot reach above, or more than one
// level below, the directory it is used with.
//
// ParseName is the only way to build a usable Name. Its field is unexported,
// so a path, or any string that has not passed the rules, cannot become one.
// The zero Name is invalid, and every method that takes a Name refuses it
// with ErrZeroName before any syscall.
type Name struct {
	s string
}

// ParseName returns name as a Name, and false when it is not a single path
// component.
func ParseName(name string) (Name, bool) {
	if !validName(name) {
		return Name{}, false
	}
	return Name{s: name}, true
}

// String returns the component as given to ParseName, or "" for the zero
// Name.
func (n Name) String() string { return n.s }

// Valid reports whether n is a single path component. Every Name ParseName
// returned is; the zero Name is not.
func (n Name) Valid() bool { return validName(n.s) }

// isDotName reports whether name is "." or "..".
func isDotName[T ~string | ~[]byte](name T) bool {
	return (len(name) == 1 && name[0] == '.') ||
		(len(name) == 2 && name[0] == '.' && name[1] == '.')
}

// validName reports whether name is one path component a *at syscall
// resolves inside its directory descriptor: 1 to MaxNameLen bytes, free of
// '/' and NUL, and neither "." nor "..". ParseName applies it to the names
// callers pass; the directory reader applies it to the names the kernel
// lists.
func validName[T ~string | ~[]byte](name T) bool {
	if len(name) == 0 || len(name) > MaxNameLen || isDotName(name) {
		return false
	}
	for i := range len(name) {
		if name[i] == '/' || name[i] == 0 {
			return false
		}
	}
	return true
}
