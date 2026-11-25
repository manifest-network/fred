//go:build linux

// Command probe tries, the way a tenant would, to take its own data out of
// the XFS project its volume's quota counts, and reports what the kernel
// answered. There are two ways out: move a file to another project, or make
// a directory stop handing its project to what is created in it. The second
// can be asked for through two different commands.
//
// Each run issues exactly one changing command, so its answer is that
// command's. The chattr tool rewrites a file's flags before it changes the
// project, and has no letter for what a directory hands down.
//
// Usage:
//
//	probe project <file> <project id>  move the file (FS_IOC_FSSETXATTR)
//	probe attribute <directory>        stop handing down (FS_IOC_FSSETXATTR)
//	probe flag <directory>             stop handing down (FS_IOC_SETFLAGS)
//
// It prints "changed" and exits 0 when the kernel accepted the change, and
// prints the error and exits 1 otherwise. It exits 2 when it was not asked
// properly, or when the directory hands nothing down to begin with.
package main

import (
	"fmt"
	"os"
	"strconv"
	"syscall"
	"unsafe"
)

// The asm-generic encodings of the commands, as on amd64 and arm64.
const (
	fsIocFsGetXattr = 0x801c581f
	fsIocFsSetXattr = 0x401c5820
	fsIocGetFlags   = 0x80086601
	fsIocSetFlags   = 0x40086602
)

// What makes a directory hand its project down, as an attribute
// (FS_XFLAG_PROJINHERIT) and as a flag (FS_PROJINHERIT_FL).
const (
	attributeInherit = 0x00000200
	flagInherit      = 0x20000000
)

// fsxattr mirrors struct fsxattr of linux/fs.h.
type fsxattr struct {
	xflags     uint32
	extsize    uint32
	nextents   uint32
	projid     uint32
	cowextsize uint32
	pad        [8]byte
}

func main() { os.Exit(run()) }

func run() int {
	if len(os.Args) < 3 {
		return usage()
	}
	file, err := os.Open(os.Args[2])
	if err != nil {
		fmt.Println("open:", err)
		return 1
	}
	defer func() { _ = file.Close() }()

	switch {
	case os.Args[1] == "project" && len(os.Args) == 4:
		project, err := strconv.ParseUint(os.Args[3], 10, 32)
		if err != nil {
			fmt.Println("bad project id:", err)
			return 2
		}
		return changeAttributes(file, "change project:", func(attributes *fsxattr) bool {
			attributes.projid = uint32(project)
			return true
		})
	case os.Args[1] == "attribute" && len(os.Args) == 3:
		return changeAttributes(file, "clear attribute:", func(attributes *fsxattr) bool {
			inherits := attributes.xflags&attributeInherit != 0
			attributes.xflags &^= attributeInherit
			return inherits
		})
	case os.Args[1] == "flag" && len(os.Args) == 3:
		var flags uint32
		if errno := flagsIoctl(file.Fd(), fsIocGetFlags, &flags); errno != 0 {
			fmt.Println("read flags:", errno)
			return 1
		}
		if flags&flagInherit == 0 {
			return nothingToClear()
		}
		flags &^= flagInherit
		if errno := flagsIoctl(file.Fd(), fsIocSetFlags, &flags); errno != 0 {
			fmt.Println("clear flag:", errno)
			return 1
		}
		fmt.Println("changed")
		return 0
	default:
		return usage()
	}
}

// changeAttributes reads the file's attributes, lets change edit them, and
// writes them back. change returns false when there is nothing to change.
func changeAttributes(file *os.File, what string, change func(*fsxattr) bool) int {
	var attributes fsxattr
	if errno := attributesIoctl(file.Fd(), fsIocFsGetXattr, &attributes); errno != 0 {
		fmt.Println("read attributes:", errno)
		return 1
	}
	if !change(&attributes) {
		return nothingToClear()
	}
	if errno := attributesIoctl(file.Fd(), fsIocFsSetXattr, &attributes); errno != 0 {
		fmt.Println(what, errno)
		return 1
	}
	fmt.Println("changed")
	return 0
}

func usage() int {
	fmt.Println("usage: probe project <file> <project id> | probe attribute <directory> | probe flag <directory>")
	return 2
}

func nothingToClear() int {
	fmt.Println("the directory hands no project down")
	return 2
}

func attributesIoctl(fd, command uintptr, attributes *fsxattr) syscall.Errno {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, command, uintptr(unsafe.Pointer(attributes))) //nolint:gosec // G103: the ioctl takes a pointer to the attribute structure
	return errno
}

func flagsIoctl(fd, command uintptr, flags *uint32) syscall.Errno {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, command, uintptr(unsafe.Pointer(flags))) //nolint:gosec // G103: the ioctl takes a pointer to the flags
	return errno
}
