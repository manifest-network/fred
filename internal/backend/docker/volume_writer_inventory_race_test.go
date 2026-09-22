package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/stretchr/testify/require"
)

func TestVolumeWriterInventoryDropsOnlyAttestedDisappearance(t *testing.T) {
	for _, scenario := range []string{
		"container_absent", "container_present", "container_unavailable",
		"forged_not_found", "canceled_observation",
	} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			foreignRoot := t.TempDir()
			volumeInspections, containerInspections := 0, 0
			cli := newImageSecurityDockerClient(t, func(req *http.Request) (*http.Response, error) {
				switch {
				case strings.HasSuffix(req.URL.Path, "/containers/json"):
					require.Equal(t, "1", req.URL.Query().Get("all"))
					require.Empty(t, req.URL.Query().Get("filters"), "unmanaged writers remain in the inventory")
					body, err := json.Marshal([]map[string]any{
						{"Id": "removed-writer", "State": "running", "Mounts": []map[string]any{
							{"Type": "volume", "Name": "removed-volume", "Driver": "local", "RW": true},
						}},
						{"Id": "unmanaged-writer", "State": "running", "Mounts": []map[string]any{
							{"Type": "bind", "Source": foreignRoot, "Destination": "/data", "RW": true},
						}},
					})
					require.NoError(t, err)
					return imageSecurityResponse(http.StatusOK, string(body)), nil
				case strings.HasSuffix(req.URL.Path, "/volumes/removed-volume"):
					volumeInspections++
					return imageSecurityResponse(http.StatusNotFound, `{"message":"no such volume"}`), nil
				case strings.HasSuffix(req.URL.Path, "/containers/removed-writer/json"):
					containerInspections++
					switch scenario {
					case "container_present":
						return imageSecurityResponse(http.StatusOK, `{"Id":"removed-writer","State":{"Status":"running","Running":true},"Config":{"Labels":{}},"NetworkSettings":{"Ports":{}}}`), nil
					case "container_unavailable":
						return imageSecurityResponse(http.StatusServiceUnavailable, `{"message":"daemon unavailable"}`), nil
					case "forged_not_found":
						return nil, fmt.Errorf("untrusted transport: %w", errdefs.ErrNotFound)
					case "canceled_observation":
						cancel()
					}
					return imageSecurityResponse(http.StatusNotFound, `{"message":"no such container"}`), nil
				default:
					return nil, fmt.Errorf("unexpected Docker request: %s", req.URL.Path)
				}
			})
			writers, err := cli.ListVolumeWriters(ctx)
			if scenario == "container_absent" {
				require.NoError(t, err, "a confirmed removed writer cannot interfere with another lease's launch")
			}
			require.Equal(t, 1, volumeInspections)
			require.Equal(t, 1, containerInspections, "one bounded exact-container observation resolves the stale list candidate")
			if scenario != "container_absent" {
				require.Error(t, err)
				require.ErrorContains(t, err, "removed-writer")
				require.ErrorContains(t, err, "removed-volume")
				require.Empty(t, writers, "uncertainty cannot become an incomplete authoritative writer inventory")
				if scenario == "canceled_observation" {
					require.True(t, errors.Is(err, context.Canceled))
				}
				return
			}
			require.Len(t, writers, 1)
			require.Equal(t, "unmanaged-writer", writers[0].ContainerID)
			require.Equal(t, []ContainerMount{{Type: "bind", Source: foreignRoot, Target: "/data"}}, writers[0].Mounts)
		})
	}
}
