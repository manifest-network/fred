package docker

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

func TestCompensationCodecLargeValidServiceDoesNotChargeEveryReplica(t *testing.T) {
	environment := make(map[string]string)
	var effectiveEnv []string
	for index := range 96 {
		key := fmt.Sprintf("SETTING_%03d", index)
		value := strings.Repeat("v", 16<<10)
		environment[key] = value
		effectiveEnv = append(effectiveEnv, key+"="+value)
	}
	stack := &manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": {Image: "nginx", Env: environment}}}
	payload, err := json.Marshal(stack)
	require.NoError(t, err)
	require.Less(t, len(payload), 2<<20, "the source manifest fits the supported request size")
	parsed, err := manifest.ParsePayload(payload)
	require.NoError(t, err)
	require.NoError(t, manifest.ValidateStackAgainstItems(parsed, []backend.LeaseItem{{ServiceName: "web", SKU: "small", Quantity: 8}}))
	plan := compensationSourcePlan{Version: 1}
	for index := range 8 {
		plan.Containers = append(plan.Containers, compensationContainerRecord{
			Name: fmt.Sprintf("source-web-%d", index), ImageID: fixtureImageID("same source"), Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
			Config: &container.Config{Image: fixtureImageID("same source"), Hostname: fmt.Sprintf("host-%d", index), Env: slices.Clone(effectiveEnv),
				Labels: map[string]string{LabelManaged: "true", LabelServiceName: "web", LabelInstanceIndex: fmt.Sprint(index), "large-common-label": strings.Repeat("label", 32<<10)}},
			Host: &container.HostConfig{ReadonlyRootfs: true}, Networks: &network.NetworkingConfig{},
		})
	}
	unnormalized, err := json.Marshal(plan)
	require.NoError(t, err)
	require.Greater(t, len(unnormalized), 4<<20, "the prior repeated-snapshot encoding rejects this valid replica cohort")
	encoded, err := encodeCompensationSourcePlan(plan)
	require.NoError(t, err)
	require.Less(t, len(encoded), 4<<20)
	var stored storedCompensationPlan
	require.NoError(t, json.Unmarshal(encoded, &stored))
	require.Len(t, stored.Configs, 1)
	decoded, err := decodeCompensationSourceSnapshot(encoded)
	require.NoError(t, err)
	require.Equal(t, plan, decoded, "normalization must preserve every effective policy and instance override")
	require.NotSame(t, decoded.Containers[0].Config, decoded.Containers[1].Config)
	decoded.Containers[0].Config.Labels[LabelInstanceIndex] = "changed"
	require.Equal(t, "1", decoded.Containers[1].Config.Labels[LabelInstanceIndex])
	// The mutation boundary clones only the instance it dispatches.
	created := cloneCompensationConfig(*decoded.Containers[0].Config)
	created.Env[0] = "changed"
	require.Equal(t, effectiveEnv[0], decoded.Containers[1].Config.Env[0])
}

func smallNormalizedCompensationPlan(t *testing.T) storedCompensationPlan {
	t.Helper()
	plan := compensationSourcePlan{Version: 1}
	for index := range 4 {
		service := "web"
		if index >= 2 {
			service = "worker"
		}
		plan.Containers = append(plan.Containers, compensationContainerRecord{Name: fmt.Sprint(index), Config: &container.Config{
			Image: "same", Env: []string{"A=B"}, Labels: map[string]string{LabelServiceName: service, LabelInstanceIndex: fmt.Sprint(index), "service-label": strings.Repeat(service, 1024)},
		}})
	}
	encoded, err := encodeCompensationSourcePlan(plan)
	require.NoError(t, err)
	var stored storedCompensationPlan
	require.NoError(t, json.Unmarshal(encoded, &stored))
	require.Len(t, stored.LabelValues, 2, "large per-service labels must also be interned across replicas")
	decoded, err := decodeCompensationSourceSnapshot(encoded)
	require.NoError(t, err)
	require.Equal(t, plan, decoded)
	return stored
}

func TestCompensationCodecRejectsChangedContentAndReferenceConfusion(t *testing.T) {
	for name, corrupt := range map[string]func(*storedCompensationPlan){
		"config content": func(p *storedCompensationPlan) {
			for key, value := range p.Configs {
				value.User = "changed"
				p.Configs[key] = value
				break
			}
		},
		"missing config": func(p *storedCompensationPlan) { p.Containers[0].Config = fixtureImageID("unknown") },
		"label content": func(p *storedCompensationPlan) {
			for key := range p.LabelValues {
				p.LabelValues[key] = "changed"
				break
			}
		},
		"missing label": func(p *storedCompensationPlan) {
			p.Containers[0].LabelRefs["service-label"] = fixtureImageID("unknown")
		},
		"common override": func(p *storedCompensationPlan) {
			p.CommonLabels = map[string]string{"must-stay": "old"}
			p.Containers[0].Labels["must-stay"] = "new"
		},
		"unused content": func(p *storedCompensationPlan) {
			value := container.Config{Image: "unused"}
			key, _ := compensationConfigDigest(value)
			p.Configs[key] = value
		},
		"cross label encoding": func(p *storedCompensationPlan) {
			p.Containers[0].Labels = maps.Clone(p.Containers[0].Labels)
			p.Containers[0].Labels["service-label"] = "ambiguous"
		},
	} {
		t.Run(name, func(t *testing.T) {
			stored := smallNormalizedCompensationPlan(t)
			corrupt(&stored)
			encoded, err := json.Marshal(stored)
			require.NoError(t, err)
			_, err = decodeCompensationSourceSnapshot(encoded)
			require.Error(t, err)
		})
	}
}
