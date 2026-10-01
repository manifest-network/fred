package docker

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

func TestDockerImagePinBackfillUsesExactRecoveredContent(t *testing.T) {
	for _, mode := range []string{"classic-legacy", "bound-labels", "missing-container", "foreign-tenant", "foreign-provider", "foreign-backend", "wrong-reference", "duplicate-instance", "different-images", "stale-projection", "partial-cohort"} {
		t.Run(mode, func(t *testing.T) {
			const lease = "f5ab7a6a-2222-4222-8222-222222222222"
			const ref = "example.invalid/app:legacy"
			const repoDigest = "example.invalid/app@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
			const wrongRepo = "foreign.invalid/app@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
			stack := &manifest.StackManifest{Services: map[string]*manifest.Manifest{"app": {Image: ref}}}
			b := newBackendForProvisionTest(t, &mockDockerClient{}, map[string]*provision{
				lease: {ProvisionState: leasesm.ProvisionState{
					LeaseUUID: lease, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
					Status: backend.ProvisionStatusReady, Quantity: 2,
					Items:         []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "app"}},
					StackManifest: stack, ContainerIDs: []string{"container-0", "container-1"},
					ServiceContainers: map[string][]string{"app": {"container-0", "container-1"}},
				}},
			})
			release := seedProvisionReleaseFromProjectionForBackendTest(t, b, lease)
			p := b.provisions[lease]
			p.ActiveReleaseVersion = release.Version
			if mode == "stale-projection" {
				p.ActiveReleaseVersion++
			}
			if mode == "partial-cohort" {
				p.ServiceContainers["app"] = []string{"container-0"}
			}
			actual := make(map[string]container.InspectResponse)
			for index, id := range []string{"container-0", "container-1"} {
				labels := map[string]string{
					LabelManaged: "true", LabelBackendName: b.cfg.Name, LabelLeaseUUID: lease,
					LabelTenant: p.Tenant, LabelProviderUUID: p.ProviderUUID,
					LabelServiceName: "app", LabelSKU: "docker-micro", LabelInstanceIndex: fmt.Sprint(index),
					LabelCallbackURL: p.CallbackURL, LabelLifecycleCallbackURL: p.LifecycleCallbackURL,
				}
				configImage := ref
				if mode == "bound-labels" {
					configImage = testImageID
					labels[LabelImageReference], labels[LabelImageID] = ref, testImageID
				}
				actual[id] = container.InspectResponse{ContainerJSONBase: &container.ContainerJSONBase{ID: id, Image: testImageID}, Config: &container.Config{Image: configImage, Labels: labels}}
			}
			second := actual["container-1"]
			switch mode {
			case "foreign-tenant":
				second.Config.Labels[LabelTenant] = "tenant-b"
			case "foreign-provider":
				second.Config.Labels[LabelProviderUUID] = "foreign-provider"
			case "foreign-backend":
				second.Config.Labels[LabelBackendName] = "foreign-backend"
			case "wrong-reference":
				second.Config.Image = "example.invalid/other:legacy"
			case "duplicate-instance":
				second.Config.Labels[LabelInstanceIndex] = "0"
			case "different-images":
				second.Image = "sha256:" + strings.Repeat("c", 64)
			}
			actual["container-1"] = second
			var reads int
			d := newImageSecurityDockerClient(t, func(req *http.Request) (*http.Response, error) {
				require.Equal(t, http.MethodGet, req.Method, "backfill must never create, pull, load or remove content")
				reads++
				for id, value := range actual {
					if strings.HasSuffix(req.URL.Path, "/containers/"+id+"/json") {
						if mode == "missing-container" && id == "container-1" {
							return imageSecurityResponse(404, `{"message":"gone"}`), nil
						}
						data, err := json.Marshal(value)
						require.NoError(t, err)
						return imageSecurityResponse(200, string(data)), nil
					}
				}
				for _, imageID := range []string{testImageID, "sha256:" + strings.Repeat("c", 64)} {
					if strings.HasSuffix(req.URL.Path, "/images/"+imageID+"/json") {
						return imageSecurityResponse(200, fmt.Sprintf(`{"Id":%q,"Os":"linux","Architecture":"amd64","Config":{},"RepoDigests":[%q,%q]}`, imageID, wrongRepo, repoDigest)), nil
					}
				}
				t.Fatalf("backfill inspected mutable or unrelated reference: %s", req.URL.Path)
				return nil, nil
			})
			d.backendName = b.cfg.Name
			pins, err := shared.NewImagePinJournal(b.callbackStore, b.releaseStore, b.retentionStore)
			require.NoError(t, err)
			owner, err := newImagePinBackfiller(b, d, pins)
			require.NoError(t, err)
			report, err := owner.Sweep(t.Context())
			require.NoError(t, err)
			pin, err := pins.Lookup(lease, release.Manifest, ref)
			require.NoError(t, err)
			if mode != "classic-legacy" && mode != "bound-labels" {
				require.Nil(t, pin)
				require.Equal(t, 1, report.UnresolvedLeases)
				inventory, err := pins.Collect(t.Context())
				require.NoError(t, err)
				require.False(t, inventory.Complete())
				return
			}
			require.NotNil(t, pin)
			require.Equal(t, testImageID, pin.ImageID)
			require.Equal(t, repoDigest, pin.PullDigest, "only the exact original repository may provide recovery evidence")
			require.Zero(t, pin.ImportBytes)
			require.Equal(t, shared.ImagePinBackfillReport{PinsAdded: 1}, report)
			initialReads := reads
			report, err = owner.Sweep(t.Context())
			require.NoError(t, err)
			require.Equal(t, shared.ImagePinBackfillReport{}, report)
			require.Equal(t, initialReads, reads, "existing pins must not re-resolve a tag or repeat physical reads")
		})
	}
}
