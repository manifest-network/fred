//go:build linux

// xfs-projid-probe is a static, non-root workload for the ENG-1118 integration
// test. It reports actual ioctl errnos; only the parent test decides whether
// the calls should have succeeded under the selected container policy.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Linux fsxattr UAPI, shared by amd64 and arm64. Keep this independent of
// Fred's attribute reader so the probe exercises the kernel ABI directly.
type fsxattr struct {
	XFlags, ExtentSize, Nextents, ProjectID, CowExtSize uint32
	Padding                                             [8]byte
}

const (
	getXAttr    = 0x801c581f // _IOR('X', 31, struct fsxattr)
	setXAttr    = 0x401c5820 // _IOW('X', 32, struct fsxattr)
	projInherit = 0x00000200 // FS_XFLAG_PROJINHERIT
	flagInherit = 0x20000000 // FS_PROJINHERIT_FL
)

func ioctl(file *os.File, command uintptr, data unsafe.Pointer) unix.Errno {
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, file.Fd(), command, uintptr(data))
	runtime.KeepAlive(file)
	return errno
}

func probe(expected, foreign uint32) (map[string]int, error) {
	if os.Getuid() != 10000 || os.Geteuid() != 10000 {
		return nil, fmt.Errorf("probe must run as non-root owner 10000, got %d/%d", os.Getuid(), os.Geteuid())
	}
	status, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return nil, err
	}
	for _, field := range []string{"CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb", "NoNewPrivs", "Seccomp"} {
		want := "0000000000000000"
		if field == "NoNewPrivs" {
			want = "1"
		} else if field == "Seccomp" {
			want = "2"
		}
		if !strings.Contains(string(status), field+":\t"+want+"\n") {
			return nil, fmt.Errorf("expected %s=%s in process status:\n%s", field, want, status)
		}
	}
	results := make(map[string]int)
	for _, name := range []string{"file-zero", "file-foreign", "dir-fsxattr", "dir-setflags"} {
		path := filepath.Join("/data", name)
		isDir := strings.HasPrefix(name, "dir-")
		if isDir {
			err = os.Mkdir(path, 0o700)
		} else {
			err = os.WriteFile(path, []byte("tenant-owned data\n"), 0o600)
		}
		if err != nil {
			return nil, err
		}
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer func() { _ = file.Close() }()
		var stat unix.Stat_t
		if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
			return nil, err
		}
		if stat.Uid != uint32(os.Geteuid()) {
			return nil, fmt.Errorf("%s is not owned by the probe", name)
		}
		var attr fsxattr
		if errno := ioctl(file, getXAttr, unsafe.Pointer(&attr)); errno != 0 {
			return nil, fmt.Errorf("FSGETXATTR %s: %w", name, errno)
		}
		if attr.ProjectID != expected || (isDir && attr.XFlags&projInherit == 0) {
			return nil, fmt.Errorf("%s did not inherit project %d: %+v", name, expected, attr)
		}
		var errno unix.Errno
		switch name {
		case "file-zero":
			attr.ProjectID = 0
			errno = ioctl(file, setXAttr, unsafe.Pointer(&attr))
		case "file-foreign":
			attr.ProjectID = foreign
			errno = ioctl(file, setXAttr, unsafe.Pointer(&attr))
		case "dir-fsxattr":
			attr.XFlags &^= projInherit
			errno = ioctl(file, setXAttr, unsafe.Pointer(&attr))
		case "dir-setflags":
			var flags uint32
			if errno := ioctl(file, unix.FS_IOC_GETFLAGS, unsafe.Pointer(&flags)); errno != 0 {
				return nil, fmt.Errorf("GETFLAGS %s: %w", name, errno)
			}
			if flags&flagInherit == 0 {
				return nil, fmt.Errorf("%s has no FS_PROJINHERIT_FL", name)
			}
			flags &^= flagInherit
			errno = ioctl(file, unix.FS_IOC_SETFLAGS, unsafe.Pointer(&flags))
		}
		results[name] = int(errno)
		// Ordinary writes must still work, and subsequent inodes must inherit
		// the original project when the attempted inheritance change is denied.
		if isDir {
			if err := os.WriteFile(filepath.Join(path, "child"), []byte("new data\n"), 0o600); err != nil {
				return nil, err
			}
		}
	}
	return results, nil
}

func run() error {
	if len(os.Args) != 3 {
		return fmt.Errorf("usage: probe expected-project foreign-project")
	}
	expected, err := strconv.ParseUint(os.Args[1], 10, 32)
	if err != nil {
		return err
	}
	foreign, err := strconv.ParseUint(os.Args[2], 10, 32)
	if err != nil {
		return err
	}
	if expected == 0 || foreign == 0 || expected == foreign {
		return fmt.Errorf("probe needs two different nonzero projects")
	}
	results, err := probe(uint32(expected), uint32(foreign))
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(results)
}

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
