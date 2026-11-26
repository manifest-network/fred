package docker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/docker/docker/client"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// Exercise the real Docker response decoders together with durable release and
// maintenance validation. Pinning execution to an ID must not make the original
// manifest's image reference disappear during recovery, or weaken its equality
// checks. The transport refuses image lookups: recovery must not resolve a tag
// that may have moved since this container was created.
func TestPinnedImageInventoryPreservesReleaseAuthority(t *testing.T) {
	const reference = "registry.example/tenant/app:latest"
	imageID := "sha256:" + strings.Repeat("a", 64)
	maintenanceID := mustParseMaintenanceID(t, "6ba7b811-9dad-41d1-80b4-00c04fd430c8")
	operationID := mustDockerOperationID(releaseCohortOperationID)
	authority, err := shared.NewReleaseRuntimeAuthority(
		operationID, releaseCohortTenant, releaseCohortProviderUUID,
		releaseCohortCallbackURL, releaseCohortLifecycleURL,
	)
	require.NoError(t, err)
	release := shared.Release{
		Manifest: []byte(`{"services":{"app":{"image":"` + reference + `"}}}`),
		Image:    "stack", OperationID: operationID, MaintenanceID: maintenanceID,
		RuntimeAuthority: &authority,
		Items:            []backend.LeaseItem{{SKU: "docker-small", ServiceName: "app", Quantity: 1}},
	}
	originalManifest := bytes.Clone(release.Manifest)

	for _, tc := range []struct {
		name         string
		configImage  string
		binding      map[string]string
		inventoryErr bool
		cohortErr    bool
	}{
		{name: "legacy tag", configImage: reference},
		{
			name: "pinned image", configImage: imageID,
			binding: map[string]string{LabelImageReference: reference, LabelImageID: imageID},
		},
		{
			name: "different released reference", configImage: imageID,
			binding:   map[string]string{LabelImageReference: "registry.example/other/app:latest", LabelImageID: imageID},
			cohortErr: true,
		},
		{
			name: "binding to a different image", configImage: imageID,
			binding:      map[string]string{LabelImageReference: reference, LabelImageID: "sha256:" + strings.Repeat("b", 64)},
			inventoryErr: true,
		},
		{
			name: "incomplete binding", configImage: imageID,
			binding: map[string]string{LabelImageReference: reference}, inventoryErr: true,
		},
		{
			name: "labels on an unpinned legacy container", configImage: reference,
			binding: map[string]string{LabelImageReference: reference, LabelImageID: imageID}, inventoryErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			labels := map[string]string{
				LabelManaged: "true", LabelLeaseUUID: releaseCohortLeaseUUID,
				LabelTenant: releaseCohortTenant, LabelProviderUUID: releaseCohortProviderUUID,
				LabelBackendName: "docker", LabelSKU: "docker-small", LabelServiceName: "app",
				LabelInstanceIndex: "0", LabelMaintenanceID: maintenanceID.String(),
				LabelCallbackURL: releaseCohortCallbackURL, LabelLifecycleCallbackURL: releaseCohortLifecycleURL,
			}
			for key, value := range tc.binding {
				labels[key] = value
			}
			dockerAPI, err := client.NewClientWithOpts(
				client.WithHost("http://docker.invalid"), client.WithVersion("1.47"),
				client.WithHTTPClient(&http.Client{Transport: dockerReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
					var payload any
					switch req.URL.Path {
					case "/v1.47/containers/container-id/json":
						payload = map[string]any{
							"Id": "container-id", "Image": imageID,
							"Config": map[string]any{"Image": tc.configImage, "Labels": labels},
							"State":  map[string]any{"Status": "running"}, "NetworkSettings": map[string]any{},
						}
					case "/v1.47/containers/json":
						payload = []map[string]any{{
							"Id": "container-id", "Image": tc.configImage, "ImageID": imageID,
							"Labels": labels, "State": "running",
						}}
					default:
						return nil, fmt.Errorf("unexpected Docker request: %s %s", req.Method, req.URL.Path)
					}
					body, marshalErr := json.Marshal(payload)
					if marshalErr != nil {
						return nil, marshalErr
					}
					return &http.Response{
						StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}},
						Body: io.NopCloser(bytes.NewReader(body)), Request: req,
					}, nil
				})}),
			)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, dockerAPI.Close()) })
			d := &DockerClient{client: newDockerSDKView(dockerAPI), backendName: "docker"}
			for _, read := range []struct {
				name string
				fn   func() ([]ContainerInfo, error)
			}{
				{"inspect", func() ([]ContainerInfo, error) {
					info, inspectErr := d.InspectContainer(t.Context(), "container-id")
					if inspectErr != nil {
						return nil, inspectErr
					}
					return []ContainerInfo{*info}, nil
				}},
				{"list", func() ([]ContainerInfo, error) { return d.ListManagedContainers(t.Context()) }},
				{"strict list", func() ([]ContainerInfo, error) { return d.ListManagedContainersStrict(t.Context()) }},
			} {
				t.Run(read.name, func(t *testing.T) {
					cohort, readErr := read.fn()
					if tc.inventoryErr {
						require.Error(t, readErr)
						require.Empty(t, cohort, "invalid identity must not become a usable inventory")
						return
					}
					require.NoError(t, readErr)
					require.Len(t, cohort, 1)
					cohortErr := validateRecoveredReleaseCohort(&release, cohort)
					maintenanceErr := validateMaintenanceGenerationContainer(
						releaseCohortLeaseUUID, maintenanceID, "docker", release, cohort[0],
					)
					if tc.cohortErr {
						require.Error(t, cohortErr)
						require.Error(t, maintenanceErr)
					} else {
						require.NoError(t, cohortErr)
						require.NoError(t, maintenanceErr)
						require.Equal(t, reference, cohort[0].Image)
					}
					require.Equal(t, originalManifest, release.Manifest)
				})
			}
		})
	}
}
