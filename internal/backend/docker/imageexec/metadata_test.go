package imageexec_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	dockerimage "github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
)

func TestImageAdmissionBoundsMetadataBeforeMinting(t *testing.T) {
	for _, scenario := range []string{"volume count", "volume path bytes", "volume aggregate bytes", "label count", "label bytes", "label key bytes"} {
		t.Run(scenario, func(t *testing.T) {
			response := classicImage()
			switch scenario {
			case "volume count":
				response.Config.Volumes = make(map[string]struct{})
				for index := range 17 {
					response.Config.Volumes[fmt.Sprintf("/data%d", index)] = struct{}{}
				}
			case "volume path bytes":
				response.Config.Volumes = map[string]struct{}{"/" + strings.Repeat("a", 4096): {}}
			case "volume aggregate bytes":
				response.Config.Volumes = make(map[string]struct{})
				for index := range 16 {
					response.Config.Volumes[fmt.Sprintf("/data%d/", index)+strings.Repeat("a", 1100)] = struct{}{}
				}
			case "label count":
				response.Config.Labels = make(map[string]string)
				for index := range 129 {
					response.Config.Labels[fmt.Sprintf("label%d", index)] = "value"
				}
			case "label bytes":
				response.Config.Labels = map[string]string{"label": strings.Repeat("v", 64<<10)}
			case "label key bytes":
				response.Config.Labels = map[string]string{strings.Repeat("k", imageexec.MaxImageLabelBytes+1): ""}
			}
			source := &fakeSource{version: "1.51", inspect: func(context.Context, string, ...client.ImageInspectOption) (dockerimage.InspectResponse, error) {
				return response, nil
			}}
			admitter, _ := newRuntime(t, source)
			_, err := admitter.Admit(t.Context(), "fixture:latest")
			require.Error(t, err, scenario)
		})
	}
}

func TestImageAdmissionRequiresCanonicalIndependentVolumeTargets(t *testing.T) {
	for _, paths := range [][]string{
		{"/"}, {"relative"}, {"/data/../proc"}, {"/data/"}, {"//data"},
		{"/proc"}, {"/proc/self"}, {"/sys/kernel"}, {"/dev/shm"}, {"/tmp"}, {"/run"},
		{"/_wp"}, {"/_wp/source"}, {"/data", "/data/child"}, {"/data\x00hidden"},
		{"/data", "/data-other", "/data/child"},
	} {
		t.Run(fmt.Sprintf("%q", paths), func(t *testing.T) {
			response := classicImage()
			response.Config.Volumes = make(map[string]struct{})
			for _, path := range paths {
				response.Config.Volumes[path] = struct{}{}
			}
			admitter, _ := newRuntime(t, &fakeSource{version: "1.51", inspect: func(context.Context, string, ...client.ImageInspectOption) (dockerimage.InspectResponse, error) {
				return response, nil
			}})
			_, err := admitter.Admit(t.Context(), "fixture:latest")
			require.Error(t, err)
		})
	}
}

func TestImageAdmissionAcceptsMetadataAtPublishedLimits(t *testing.T) {
	response := classicImage()
	response.Config.Volumes = make(map[string]struct{}, imageexec.MaxImageVolumes)
	for index := range imageexec.MaxImageVolumes {
		prefix := fmt.Sprintf("/data%02d/", index)
		response.Config.Volumes[prefix+strings.Repeat("a", imageexec.MaxImageVolumeBytes/imageexec.MaxImageVolumes-len(prefix))] = struct{}{}
	}
	response.Config.Labels = make(map[string]string, imageexec.MaxImageLabels)
	for index := range imageexec.MaxImageLabels {
		key := fmt.Sprintf("label%03d", index)
		response.Config.Labels[key] = strings.Repeat("v", imageexec.MaxImageLabelBytes/imageexec.MaxImageLabels-len(key))
	}
	admitter, _ := newRuntime(t, &fakeSource{version: "1.51", inspect: func(context.Context, string, ...client.ImageInspectOption) (dockerimage.InspectResponse, error) {
		return response, nil
	}})
	admitted, err := admitter.Admit(t.Context(), "fixture:latest")
	require.NoError(t, err)
	require.Len(t, admitted.Volumes(), imageexec.MaxImageVolumes)
}
