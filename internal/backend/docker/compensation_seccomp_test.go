package docker

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
)

// testdata/compensation-plan-315ed5a.json was written by the unchanged
// 315ed5a encoder: instance 0 has the pre-profile security options, instance
// 1 also names a stale profile. Plans persisted by deployed binaries must
// keep decoding, and must never replay a profile of their own.
func TestCompensationPlanFrom315ed5aCreatesWithTheCurrentProfile(t *testing.T) {
	encoded, err := os.ReadFile("testdata/compensation-plan-315ed5a.json")
	require.NoError(t, err)
	plan, err := decodeCompensationSourceSnapshot(encoded)
	require.NoError(t, err)
	require.Len(t, plan.Containers, 2)
	for _, snapshot := range plan.Containers {
		require.Equal(t, []string{"no-new-privileges:true"}, snapshot.Host.SecurityOpt)
		require.True(t, snapshot.Host.ReadonlyRootfs)
		require.Equal(t, []string{"ALL"}, []string(snapshot.Host.CapDrop))
	}

	var created [][]string
	docker := newImageSecurityDockerClient(t, func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/images/") {
			return imageSecurityResponse(http.StatusOK, platformSecurityJSON(testImageID, ocispec.MediaTypeImageManifest)), nil
		}
		require.True(t, strings.HasSuffix(req.URL.Path, "/containers/create"))
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		var request struct{ HostConfig container.HostConfig }
		require.NoError(t, json.Unmarshal(body, &request))
		created = append(created, request.HostConfig.SecurityOpt)
		return imageSecurityResponse(http.StatusCreated, fmt.Sprintf(`{"Id":"source-%d"}`, len(created))), nil
	})
	image, err := docker.AdmitImage(t.Context(), testImageID)
	require.NoError(t, err)
	project, err := docker.images.Compile(&composetypes.Project{Name: "fred-source-lease", Services: composetypes.Services{"web-1": {Image: image.Reference()}}}, map[string]imageexec.Image{"web-1": image})
	require.NoError(t, err)
	binding, err := project.Container("web-1")
	require.NoError(t, err)
	want := tenantSeccompSecurityOpt(t, "no-new-privileges:true")
	for _, snapshot := range plan.Containers {
		for range 2 { // a retry must not accumulate entries
			_, outcome := docker.createCompensationContainer(t.Context(), compensationContainer{
				Name: snapshot.Name, Binding: binding, Config: snapshot.Config, Host: snapshot.Host, Networks: snapshot.Networks,
			})
			require.True(t, outcome.settled)
			require.NoError(t, outcome.err)
		}
		require.Equal(t, []string{"no-new-privileges:true"}, snapshot.Host.SecurityOpt, "creation must not change the decoded plan")
	}
	require.Len(t, created, 4)
	for _, securityOpt := range created {
		require.Equal(t, want, securityOpt)
	}
}

func inspectedTenantContainer(t *testing.T, index int, securityOpt []string) container.InspectResponse {
	t.Helper()
	image := fixtureImageID("compensation-size")
	labels := map[string]string{
		LabelManaged: "true", LabelLeaseUUID: "0192f1a0-1111-4abc-8def-00000000e11a", LabelTenant: "manifest1tenant",
		LabelProviderUUID: nominalDockerProviderUUID, LabelSKU: "docker-small", LabelServiceName: "web",
		LabelInstanceIndex: fmt.Sprint(index), LabelFailCount: "0", LabelCreatedAt: time.Unix(int64(index), 0).UTC().Format(time.RFC3339),
		LabelBackendName: "docker", LabelImageReference: "registry.example/web:1",
		LabelCallbackURL: "https://fred.example/callbacks/provision?operation_id=x", LabelLifecycleCallbackURL: "https://fred.example/callbacks/lifecycle",
	}
	return container.InspectResponse{
		ContainerJSONBase: &container.ContainerJSONBase{
			Name: fmt.Sprintf("/fred-lease-web-%d", index), Image: image,
			HostConfig: &container.HostConfig{
				CapDrop: []string{"ALL"}, SecurityOpt: securityOpt, ReadonlyRootfs: true,
				Tmpfs:         map[string]string{"/tmp": "size=64M", "/run": "size=64M"},
				RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
				Resources:     container.Resources{NanoCPUs: 500_000_000, Memory: 512 << 20, MemorySwap: 512 << 20},
				Mounts:        []mount.Mount{{Type: mount.TypeBind, Source: fmt.Sprintf("/data/volumes/fred-lease-web-%d/data", index), Target: "/data"}},
			},
		},
		Config:          &container.Config{Image: image, Env: []string{"PORT=8080"}, Hostname: fmt.Sprintf("web-%d", index), Labels: labels},
		NetworkSettings: &container.NetworkSettings{Networks: map[string]*network.EndpointSettings{"fred-tenant": {Aliases: []string{"web"}}}},
	}
}

// Captured configuration keeps every other security option and drops a
// profile; the caller's inspected value is not modified.
func TestCompensationSnapshotDropsSeccompOptions(t *testing.T) {
	inspected := inspectedTenantContainer(t, 0, tenantSeccompSecurityOpt(t, "no-new-privileges:true", "label=disable", "seccomp:unconfined"))
	original := slices.Clone(inspected.HostConfig.SecurityOpt)
	record := snapshotCompensationContainer(inspected, nil)
	require.NotNil(t, record)
	require.Equal(t, []string{"no-new-privileges:true", "label=disable"}, record.Host.SecurityOpt)
	require.NotSame(t, inspected.HostConfig, record.Host)
	require.Equal(t, original, inspected.HostConfig.SecurityOpt)
	require.Nil(t, compensationHostConfig(nil))
	withoutOptions := compensationHostConfig(&container.HostConfig{ReadonlyRootfs: true})
	require.Nil(t, withoutOptions.SecurityOpt, "a host without options keeps its encoded form")
}

// Every running container carries an inline profile of about 13 KB. A plan
// that kept it per instance would exceed the 4 MiB maintenance journal entry
// long before the largest admitted lease; captured plans drop it.
func TestCompensationPlanForTheLargestLeaseFitsTheJournal(t *testing.T) {
	inline := tenantSeccompSecurityOpt(t, "no-new-privileges:true")
	plan := compensationSourcePlan{Version: 1}
	for index := range backend.MaxOperationQuantity {
		record := snapshotCompensationContainer(inspectedTenantContainer(t, index, inline), nil)
		require.NotNil(t, record)
		record.Platform = ocispec.Platform{OS: "linux", Architecture: "amd64"}
		plan.Containers = append(plan.Containers, *record)
	}
	encoded, err := encodeCompensationSourcePlan(plan)
	require.NoError(t, err)
	const journalEntryLimit = 4 << 20
	t.Logf("plan for %d instances: %d bytes", backend.MaxOperationQuantity, len(encoded))
	require.Less(t, len(encoded), journalEntryLimit-64<<10, "the plan must leave room for the journal record around it")
	decoded, err := decodeCompensationSourceSnapshot(encoded)
	require.NoError(t, err)
	require.Len(t, decoded.Containers, backend.MaxOperationQuantity)

	for index := range plan.Containers {
		plan.Containers[index].Host.SecurityOpt = inline
	}
	unstripped, err := encodeCompensationSourcePlan(plan)
	require.NoError(t, err)
	require.Greater(t, len(unstripped), journalEntryLimit, "without the strip the same plan would not fit")
}
