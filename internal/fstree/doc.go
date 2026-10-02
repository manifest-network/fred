// Package fstree removes and walks directory trees whose shape fred does not
// control, such as the contents of a tenant volume, with resources that stay
// bounded however deep or wide the tree is.
//
// Go's os.RemoveAll and (*os.Root).RemoveAll keep one open directory and one
// stack frame per level of the tree, plus that level's batch of names, so
// their descriptor use and memory grow with the tree's depth. The two
// primitives here do not:
//
//   - RemoveBeneath removes one entry of a directory and, when the entry is a
//     directory, everything beneath it.
//   - WalkBeneath visits the same entries read-only, for audits.
//
// Both take the parent as an open directory and the entry as a Name, and act
// only through descriptor-relative syscalls. A Name is a single path
// component, and ParseName is the only way to build one, so a path cannot be
// passed where an entry is expected. They never build a path, so no
// path-length limit applies, and an error message carries at most 64 bytes of
// one entry name plus its depth.
//
// # Precondition
//
// RemoveBeneath requires that nothing else mutates the tree while it runs:
// the tree's writers are stopped. It detects a concurrent rename
// (ErrTreeChanged) but cannot prevent one. A directory moved out of the tree
// while RemoveBeneath holds it open is still emptied before the move is
// detected; every descriptor-based remover, GNU fts and Go's own included,
// shares this limitation.
//
// WalkBeneath tolerates concurrent writers. It is best effort: it may skip or
// repeat entries that change while it runs, and it fails with ErrTreeChanged
// when an ancestor moves or a directory it holds is removed. It never follows
// a symlink or crosses a mount; as with RemoveBeneath, a directory moved out
// of the tree while the walk holds it is still listed before the move is
// detected.
//
// # Algorithm
//
// Both walk iteratively. They hold a descriptor for the current directory and
// a stack of its ancestors' inode numbers. A descent opens a child with
// O_NOFOLLOW|O_DIRECTORY and requires the anchor's device and mount. An ascent
// opens ".." and requires the same device and mount plus the inode recorded on
// the way down, so a directory moved during the walk is detected instead of
// followed.
//
// RemoveBeneath reads each directory from its start and handles entries in
// listing order. It unlinks every entry it can and stops at the first
// non-empty child directory, which it enters. Once that child is empty, the
// next scan of the parent removes it. When entering the child would take the
// stack past 65,536 levels, RemoveBeneath instead moves the child into the
// anchor under a fresh ".fred-cut-" name (a cut) and removes it later from the
// top, so the stack never outgrows its bound. Immediately before the call's
// first cut, and never in a call that needs none, it runs the caller's
// RemoveOptions.BeforeFirstCut on the anchor; a failure there refuses the cut
// (ErrCutRefused). Last, it removes the anchor itself, but only while the
// parent's name still binds the directory it emptied.
//
// WalkBeneath resumes each directory at the getdents offset recorded on the
// way down, and returns ErrTooDeep where RemoveBeneath would cut.
//
// # Invariants
//
//   - I1 Confinement: apart from the entry itself, which is removed by name
//     through the parent's descriptor, every mutation is relative to the
//     anchor, to a descriptor reached from it by an O_NOFOLLOW|O_DIRECTORY
//     openat, or to a ".." whose device, mount and inode match the recorded
//     ancestor.
//   - I2 Symlinks are never followed: non-directories are unlinked by name,
//     every open uses O_NOFOLLOW, and the anchor's name is checked with
//     AT_SYMLINK_NOFOLLOW.
//   - I3 Mounts are never crossed: the anchor must share its parent's device
//     and mount, checked before anything in it is touched or BeforeFirstCut
//     can run, and every directory entered must share the anchor's. The
//     mount ID catches a bind mount of the same filesystem, which st_dev alone
//     misses; kernels older than Linux 5.8 do not report it, and there only
//     the device is compared. A mount point inside the tree cannot be removed
//     (EBUSY), so RemoveBeneath stops there with ErrUndeletable.
//   - I4 Progress: every iteration removes an entry, descends, ascends or
//     cuts. An iteration that does none of these is retried once and then
//     fails with ErrTreeChanged, so a listing that disagrees with lookups
//     cannot make it spin.
//   - I5 Idempotence: a rerun continues from what is on disk. Cut subtrees are
//     ordinary children of the anchor.
//   - I6 Failure keeps bytes: an error stops the call at once with a typed,
//     wrapped error, and nothing more is removed. A refused cut, whether the
//     rename or BeforeFirstCut failed, is such an error: what was removed
//     before the depth bound was reached stays removed, and nothing is cut.
//     Only an entry that vanished or changed type after it was listed is
//     passed over, for the next scan to see as it is now.
//
// # Resource bounds
//
//   - Descriptors: besides the caller's parent, at most three for
//     RemoveBeneath (the anchor, the current directory and one transient) and
//     two for WalkBeneath.
//   - Memory: 8 bytes per level of ancestry for RemoveBeneath and 16 for
//     WalkBeneath, for at most 65,536 levels, plus one 32 KiB getdents buffer
//     and one batch of at most 256 names.
//   - Stack: no recursion, so the Go stack does not grow with the tree.
//   - Syscalls: O(entries + cuts). After an ascent, a directory is read again
//     with a small buffer that doubles while entries keep being consumed, so
//     the kernel's work per read stays proportional to what is used.
//   - Cancellation: the context is checked before every entry, descent and
//     ascent. An interrupted call leaves a consistent tree that a rerun
//     finishes.
//
// The package is Linux-only.
package fstree
