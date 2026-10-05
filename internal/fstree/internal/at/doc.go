// Package at is fstree's descriptor layer: the only code that turns an open
// directory and a name into a syscall.
//
// fstree's confinement rests on three properties of every name-relative
// syscall: the name is one path component, it is resolved against the
// directory it was found in, and the flags never follow a symlink. This
// package makes each of them a property of a type, so code outside it cannot
// get them wrong:
//
//   - Name is one path component, and ParseName is its only constructor.
//   - Dir owns an open directory descriptor. The descriptor is unexported, the
//     open flags are fixed, and Close poisons the Dir, so a stale Dir fails
//     with ErrClosed instead of acting on a reused descriptor number.
//     OpenParent is the only way to reach the parent directory.
//   - Listed is an entry a Dir listed, bound to that Dir: its methods act on
//     the entry inside that Dir and never take the directory it is in, and
//     nothing converts it to a Name, so a listed name cannot be resolved
//     against another directory.
//   - View and ListedView are the read-only side. They list, stat and open,
//     and have no method that changes the filesystem, so code that reaches
//     the tree only through them cannot change it.
//   - Borrowed lends a directory to foreign code for one call, through a
//     scoped Control in the manner of syscall.RawConn.
//   - Identity always carries a mount ID. A kernel that does not report one
//     yields ErrNoMountID, never an identity compared by device alone.
//
// The raw syscalls live in sys_linux.go, the only file under internal/fstree
// that calls into golang.org/x/sys/unix or syscall; a guard test in fstree
// pins that.
//
// The package is Linux-only.
package at
