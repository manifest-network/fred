//go:build linux

// Command probe tries to move a file to another XFS project, the way a tenant
// would retag its own data, and reports what the kernel answered. It issues
// only FS_IOC_FSGETXATTR and FS_IOC_FSSETXATTR. The chattr tool also rewrites
// the file's flags first, which is refused by a different rule and would hide
// the answer this probe is after.
//
// Usage: probe <file> <project id>. It prints "changed" and exits 0 when the
// kernel accepted the change, and prints the error and exits 1 otherwise.
package main

import (
	"fmt"
	"os"
	"strconv"
	"syscall"
	"unsafe"
)

// The asm-generic encodings of the two commands, as on amd64 and arm64.
const (
	fsIocFsGetXattr = 0x801c581f
	fsIocFsSetXattr = 0x401c5820
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
	if len(os.Args) != 3 {
		fmt.Println("usage: probe <file> <project id>")
		return 2
	}
	project, err := strconv.ParseUint(os.Args[2], 10, 32)
	if err != nil {
		fmt.Println("bad project id:", err)
		return 2
	}
	file, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Println("open:", err)
		return 1
	}
	defer func() { _ = file.Close() }()

	var attributes fsxattr
	if errno := ioctl(file.Fd(), fsIocFsGetXattr, &attributes); errno != 0 {
		fmt.Println("read attributes:", errno)
		return 1
	}
	attributes.projid = uint32(project)
	if errno := ioctl(file.Fd(), fsIocFsSetXattr, &attributes); errno != 0 {
		fmt.Println("change project:", errno)
		return 1
	}
	fmt.Println("changed")
	return 0
}

func ioctl(fd, command uintptr, attributes *fsxattr) syscall.Errno {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, command, uintptr(unsafe.Pointer(attributes))) //nolint:gosec // G103: the ioctl takes a pointer to the attribute structure
	return errno
}
