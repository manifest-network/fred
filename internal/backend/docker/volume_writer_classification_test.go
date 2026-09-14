package docker

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/manifest-network/fred/internal/fsidentity"
)

func unrelatedBindLeaves(t *testing.T) []string {
	t.Helper()
	root := t.TempDir()
	fifo := filepath.Join(root, "fifo")
	require.NoError(t, unix.Mkfifo(fifo, 0o600))
	// Unix socket addresses have a short kernel limit; testing.T's descriptive
	// directory names can exceed it even when all filesystem paths are valid.
	socketRoot, err := os.MkdirTemp("", "sock-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(socketRoot) })
	socket := filepath.Join(socketRoot, "socket")
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	link := filepath.Join(root, "link")
	require.NoError(t, os.Symlink(root, link))
	return []string{fifo, socket, "/dev/null", link}
}

func TestProtectedVolumeClassifiesNonDirectoryAndLinkedSources(t *testing.T) {
	root, err := fsidentity.OpenDirectory(t.TempDir())
	require.NoError(t, err)
	defer root.Close()
	q := &quiescedVolumes{volumes: map[string]protectedVolume{"volume": {root: root}}}
	for _, source := range unrelatedBindLeaves(t) {
		affected, err := q.affects(source)
		require.NoError(t, err, source)
		require.False(t, affected, source)
	}
	inside := filepath.Join(root.Path(), "fifo")
	require.NoError(t, unix.Mkfifo(inside, 0o600))
	links := t.TempDir()
	for label, destination := range map[string]string{
		"root": root.Path(), "inside": inside, "parent": filepath.Dir(root.Path()),
	} {
		link := filepath.Join(links, label)
		require.NoError(t, os.Symlink(destination, link))
		affected, err := q.affects(link)
		require.NoError(t, err)
		require.True(t, affected, "linked %s must remain protected", label)
	}
	affected, err := q.affects(filepath.Join(links, "root", "not-created-yet"))
	require.NoError(t, err)
	require.True(t, affected, "a missing child must not hide its linked protected parent")
	loop := filepath.Join(links, "loop")
	require.NoError(t, os.Symlink(loop, loop))
	_, err = q.affects(loop)
	require.Error(t, err, "unresolved directory authority must remain conservative")
}

func TestVolumeWriterUnrelatedSpecialBindsPermitProtectedLaunch(t *testing.T) {
	f := newWriterRetirementHarness(t)
	for index, source := range unrelatedBindLeaves(t) {
		f.h.inventory.containers = append(f.h.inventory.containers, ContainerInfo{
			ContainerID: fmt.Sprintf("foreign-sidecar-%d", index), Status: "exited",
			Mounts: []ContainerMount{{Type: "bind", Source: source}},
		})
	}
	f.execute(t)
	require.Equal(t, 2, f.stops, "retire only exact prior workload writers")
	require.Equal(t, 2, f.removes)
	require.Equal(t, 1, f.launches)
}

func TestVolumeWriterInventoryRecoversExactPluginMountpoint(t *testing.T) {
	for _, scenario := range []string{"resolved", "empty", "wrong name", "wrong driver", "unavailable"} {
		t.Run(scenario, func(t *testing.T) {
			mountpoint := t.TempDir()
			inspections := 0
			cli := newImageSecurityDockerClient(t, func(req *http.Request) (*http.Response, error) {
				switch {
				case strings.HasSuffix(req.URL.Path, "/containers/json"):
					require.Equal(t, "1", req.URL.Query().Get("all"))
					return imageSecurityResponse(http.StatusOK, `[{"Id":"sidecar","Mounts":[{"Type":"volume","Name":"remote","Driver":"plugin","RW":true},{"Type":"volume","Name":"remote","Driver":"plugin","RW":true}]}]`), nil
				case strings.HasSuffix(req.URL.Path, "/volumes/remote"):
					inspections++
					value := map[string]string{"Name": "remote", "Driver": "plugin", "Mountpoint": mountpoint}
					switch scenario {
					case "empty":
						value["Mountpoint"] = ""
					case "wrong name":
						value["Name"] = "other"
					case "wrong driver":
						value["Driver"] = "other"
					case "unavailable":
						return imageSecurityResponse(http.StatusServiceUnavailable, `{"message":"plugin unavailable"}`), nil
					}
					body, err := json.Marshal(value)
					require.NoError(t, err)
					return imageSecurityResponse(http.StatusOK, string(body)), nil
				default:
					return nil, fmt.Errorf("unexpected Docker read: %s", req.URL.Path)
				}
			})
			all, err := cli.ListVolumeWriters(t.Context())
			require.Equal(t, 1, inspections, "repeated exact volume has one bounded resolution")
			if scenario != "resolved" {
				require.Error(t, err)
				require.Empty(t, all)
				return
			}
			require.NoError(t, err)
			require.Len(t, all, 1)
			require.Len(t, all[0].Mounts, 2)
			for _, mount := range all[0].Mounts {
				require.Equal(t, mountpoint, mount.Source)
			}
		})
	}
}
